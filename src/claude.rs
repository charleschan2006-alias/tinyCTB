//! Claude Code backend: session discovery via `~/.claude/projects` JSONL
//! transcripts, event ingestion via hook spool files, and headless turns via
//! detached `claude -p` processes.

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use notify::Watcher;
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::config::{ClaudeConfig, DaemonConfig};
use crate::state::{
    bridge_turn_pending, clear_pending_outbound_events, get_setting_number, get_setting_text,
    reconcile_thread_snapshots, remote_mode_status_path, set_setting, set_setting_text,
    state_dir_path, thread_snapshot_json, upsert_thread_snapshot, BridgeThreadSnapshot,
    PendingPrompt,
};

const CLAUDE_VERSION_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_PREVIEW_CHARS: usize = 2000;
const MAX_HOOK_EVENT_BYTES: u64 = 10 * 1024 * 1024;
const MAX_SPOOL_EVENTS_PER_CYCLE: usize = 200;

// ---------------------------------------------------------------------------
// Paths

pub(crate) fn claude_projects_dir() -> Result<PathBuf> {
    if let Ok(dir) = env::var("TINYCTB_CLAUDE_PROJECTS_DIR") {
        if !dir.trim().is_empty() {
            return Ok(PathBuf::from(dir));
        }
    }
    let home = dirs::home_dir().context("home directory is not available")?;
    Ok(home.join(".claude").join("projects"))
}

pub(crate) fn claude_settings_path() -> Result<PathBuf> {
    if let Ok(path) = env::var("TINYCTB_CLAUDE_SETTINGS_PATH") {
        if !path.trim().is_empty() {
            return Ok(PathBuf::from(path));
        }
    }
    let home = dirs::home_dir().context("home directory is not available")?;
    Ok(home.join(".claude").join("settings.json"))
}

pub(crate) fn events_spool_dir() -> Result<PathBuf> {
    Ok(state_dir_path()?.join("events"))
}

// Only referenced from the non-test spawn path; the test build stubs spawning.
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn turn_logs_dir() -> Result<PathBuf> {
    Ok(state_dir_path()?.join("logs").join("turns"))
}

// ---------------------------------------------------------------------------
// Binary resolution

#[derive(Debug, Clone)]
pub(crate) struct ResolvedBinary {
    pub(crate) path: PathBuf,
    pub(crate) source: &'static str,
}

pub(crate) fn resolve_claude_binary() -> Result<ResolvedBinary> {
    let mut seen = BTreeSet::new();
    let mut candidates: Vec<ResolvedBinary> = Vec::new();

    // An explicit CLAUDE_BIN override is authoritative: falling back to other
    // candidates would silently mask a broken override.
    if let Ok(override_path) = env::var("CLAUDE_BIN") {
        if !override_path.trim().is_empty() {
            let path = PathBuf::from(override_path);
            if claude_candidate_is_usable(&path) {
                return Ok(ResolvedBinary {
                    path,
                    source: "override",
                });
            }
            bail!(
                "CLAUDE_BIN is set to {} but it is not a usable claude binary (`--version` must succeed)",
                path.display()
            );
        }
    }
    if let Some(home) = dirs::home_dir() {
        push_candidate(
            &mut candidates,
            &mut seen,
            home.join(".local/bin/claude"),
            "platform-known",
        );
    }
    if let Ok(path_claude) = which::which("claude") {
        push_candidate(&mut candidates, &mut seen, path_claude, "path");
    }

    for candidate in candidates {
        if claude_candidate_is_usable(&candidate.path) {
            return Ok(candidate);
        }
    }

    bail!(
        "Could not resolve claude executable. Set CLAUDE_BIN or install Claude Code so `claude --version` works in this environment"
    )
}

fn push_candidate(
    candidates: &mut Vec<ResolvedBinary>,
    seen: &mut BTreeSet<String>,
    path: PathBuf,
    source: &'static str,
) {
    let key = path.display().to_string();
    if seen.insert(key) {
        candidates.push(ResolvedBinary { path, source });
    }
}

fn claude_candidate_is_usable(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let Ok(metadata) = fs::metadata(path) else {
            return false;
        };
        if metadata.permissions().mode() & 0o111 == 0 {
            return false;
        }
    }
    let Ok(mut child) = Command::new(path)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return false;
    };
    let started = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) if started.elapsed() >= CLAUDE_VERSION_TIMEOUT => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Small utilities

pub(crate) fn normalized_message(message: Option<&str>) -> Option<String> {
    message
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn generate_session_uuid() -> Result<String> {
    let mut bytes = [0u8; 16];
    fs::File::open("/dev/urandom")
        .context("failed to open /dev/urandom")?
        .read_exact(&mut bytes)
        .context("failed to read random bytes")?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

fn truncate_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_string();
    }
    value.chars().take(limit).collect()
}

// ---------------------------------------------------------------------------
// Away mode (settings + status marker file)

pub(crate) fn get_away_mode(conn: &Connection) -> Result<Value> {
    let away_started_at = get_setting_number(conn, "away_started_at")?;
    let existing_session = get_setting_text(conn, "away_session_id")?;
    let away_session_id =
        existing_session.or_else(|| away_started_at.map(|value| value.to_string()));
    if let Some(session_id) = away_session_id.as_deref() {
        if get_setting_text(conn, "away_session_id")?.is_none() {
            set_setting_text(conn, "away_session_id", session_id)?;
        }
    }
    let state = json!({
        "ok": true,
        "away": get_setting_text(conn, "away")?.unwrap_or_default() == "true",
        "awayStartedAt": away_started_at,
        "awaySessionId": away_session_id
    });
    write_remote_mode_status_marker(&state)?;
    Ok(state)
}

pub(crate) fn set_away_mode(conn: &Connection, away: bool, now: u64) -> Result<Value> {
    set_setting_text(conn, "away", if away { "true" } else { "false" })?;
    let state = if away {
        set_setting(conn, "away_started_at", now)?;
        set_setting_text(conn, "away_session_id", &now.to_string())?;
        json!({
            "ok": true,
            "away": true,
            "awayStartedAt": now,
            "awaySessionId": now.to_string()
        })
    } else {
        let cleared_pending = clear_pending_outbound_events(conn)?;
        // The cleared backlog may include an undelivered sync-error
        // notification; re-arm the streak so the next away session can
        // notify about a still-persistent error.
        crate::daemon::end_sync_error_streak(conn)?;
        conn.execute(
            "DELETE FROM settings WHERE key IN ('away_started_at', 'away_session_id')",
            params![],
        )?;
        json!({
            "ok": true,
            "away": false,
            "awayStartedAt": Value::Null,
            "awaySessionId": Value::Null,
            "clearedPendingNotifications": cleared_pending
        })
    };
    write_remote_mode_status_marker(&state)?;
    Ok(state)
}

fn write_remote_mode_status_marker(state: &Value) -> Result<()> {
    #[cfg(test)]
    if env::var_os("TINYCTB_STATE_DIR").is_none() {
        return Ok(());
    }

    let marker = json!({
        "away": state.get("away").cloned().unwrap_or(Value::Bool(false)),
        "awayStartedAt": state.get("awayStartedAt").cloned().unwrap_or(Value::Null),
        "awaySessionId": state.get("awaySessionId").cloned().unwrap_or(Value::Null)
    });
    let content = serde_json::to_vec(&marker)?;
    let path = remote_mode_status_path()?;
    if fs::read(&path).ok().as_deref() == Some(content.as_slice()) {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, content)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Session transcript discovery and parsing

#[derive(Debug, Clone)]
pub(crate) struct SessionFileInfo {
    pub(crate) session_id: String,
    pub(crate) path: PathBuf,
    pub(crate) mtime_ms: u64,
}

fn file_mtime_ms(path: &Path) -> Option<u64> {
    fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|value| value.as_millis() as u64)
}

pub(crate) fn list_session_files(limit: u64) -> Result<Vec<SessionFileInfo>> {
    let root = claude_projects_dir()?;
    let mut sessions = Vec::new();
    let Ok(project_dirs) = fs::read_dir(&root) else {
        return Ok(sessions);
    };
    for project_dir in project_dirs.filter_map(Result::ok) {
        let project_path = project_dir.path();
        if !project_path.is_dir() {
            continue;
        }
        let Ok(entries) = fs::read_dir(&project_path) else {
            continue;
        };
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let Some(mtime_ms) = file_mtime_ms(&path) else {
                continue;
            };
            sessions.push(SessionFileInfo {
                session_id: stem.to_string(),
                path,
                mtime_ms,
            });
        }
    }
    sessions.sort_by_key(|session| std::cmp::Reverse(session.mtime_ms));
    sessions.truncate(limit as usize);
    Ok(sessions)
}

pub(crate) fn find_session_file(session_id: &str) -> Result<Option<SessionFileInfo>> {
    let root = claude_projects_dir()?;
    let file_name = format!("{session_id}.jsonl");
    let Ok(project_dirs) = fs::read_dir(&root) else {
        return Ok(None);
    };
    for project_dir in project_dirs.filter_map(Result::ok) {
        let candidate = project_dir.path().join(&file_name);
        if candidate.is_file() {
            let mtime_ms = file_mtime_ms(&candidate).unwrap_or(0);
            return Ok(Some(SessionFileInfo {
                session_id: session_id.to_string(),
                path: candidate,
                mtime_ms,
            }));
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TranscriptSummary {
    pub(crate) name: Option<String>,
    pub(crate) cwd: Option<String>,
    pub(crate) last_assistant_text: Option<String>,
    pub(crate) last_record_type: Option<String>,
    /// Human-readable summary of the tool call(s) in the most recent assistant
    /// record, cleared once a later user record (tool result / reply) arrives.
    /// This is what a permission prompt is actually about — the Notification
    /// hook payload itself only carries a one-line message without the tool
    /// arguments.
    pub(crate) pending_tool_use: Option<String>,
}

const MAX_TOOL_DETAIL_CHARS: usize = 500;

fn ask_user_question_summary(input: &Value) -> Option<String> {
    let questions = input.get("questions")?.as_array()?;
    let mut parts = Vec::new();
    for question in questions {
        let Some(text) = question.get("question").and_then(Value::as_str) else {
            continue;
        };
        let options = question
            .get("options")
            .and_then(Value::as_array)
            .map(|options| {
                options
                    .iter()
                    .filter_map(|option| option.get("label").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join(" / ")
            })
            .filter(|labels| !labels.is_empty());
        parts.push(match options {
            Some(labels) => format!("{text}\n▸ {labels}"),
            None => text.to_string(),
        });
    }
    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn compact_tool_use_summary(name: &str, input: &Value) -> String {
    let detail = match name {
        "Bash" => input
            .get("command")
            .and_then(Value::as_str)
            .map(str::to_string),
        "Edit" | "Write" | "Read" | "NotebookEdit" => input
            .get("file_path")
            .and_then(Value::as_str)
            .map(str::to_string),
        "AskUserQuestion" => ask_user_question_summary(input),
        _ => serde_json::to_string(input)
            .ok()
            .filter(|raw| raw != "{}" && raw != "null"),
    };
    match detail {
        Some(detail) if !detail.trim().is_empty() => {
            format!("{name}: {}", truncate_chars(detail.trim(), MAX_TOOL_DETAIL_CHARS))
        }
        _ => name.to_string(),
    }
}

fn tool_use_summary_from_content(content: Option<&Value>) -> Option<String> {
    let blocks = content?.as_array()?;
    let summaries = blocks
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
        .filter_map(|block| {
            let name = block.get("name").and_then(Value::as_str)?;
            Some(compact_tool_use_summary(
                name,
                block.get("input").unwrap_or(&Value::Null),
            ))
        })
        .collect::<Vec<_>>();
    (!summaries.is_empty()).then(|| summaries.join("\n"))
}

fn text_from_message_content(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => normalized_message(Some(text)),
        Value::Array(blocks) => {
            let joined = blocks
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            normalized_message(Some(&joined))
        }
        _ => None,
    }
}

/// Defensive parse of a Claude Code session transcript. The JSONL format is not
/// a stable API: unknown record types are skipped, sidechain (subagent) and
/// meta records are ignored.
pub(crate) fn parse_transcript_summary(path: &Path) -> Result<TranscriptSummary> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read session transcript at {}", path.display()))?;
    let mut summary = TranscriptSummary::default();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let record_type = record.get("type").and_then(Value::as_str).unwrap_or("");
        if record.get("isSidechain").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        match record_type {
            "ai-title" => {
                if let Some(title) = record.get("aiTitle").and_then(Value::as_str) {
                    summary.name = normalized_message(Some(title));
                }
            }
            "assistant" => {
                if let Some(cwd) = record.get("cwd").and_then(Value::as_str) {
                    summary.cwd = Some(cwd.to_string());
                }
                let content = record
                    .get("message")
                    .and_then(|message| message.get("content"));
                if let Some(text) = content.and_then(text_from_message_content) {
                    summary.last_assistant_text = Some(truncate_chars(&text, MAX_PREVIEW_CHARS));
                }
                summary.pending_tool_use = tool_use_summary_from_content(content);
                summary.last_record_type = Some("assistant".to_string());
            }
            "user" => {
                if record.get("isMeta").and_then(Value::as_bool) == Some(true) {
                    continue;
                }
                if let Some(cwd) = record.get("cwd").and_then(Value::as_str) {
                    summary.cwd = Some(cwd.to_string());
                }
                // A user record (tool result or reply) means the previous tool
                // call is no longer pending.
                summary.pending_tool_use = None;
                summary.last_record_type = Some("user".to_string());
            }
            _ => {}
        }
    }
    Ok(summary)
}

/// Extract a plain user/assistant message list from a transcript (for `show`).
pub(crate) fn parse_transcript_messages(path: &Path, limit: usize) -> Result<Vec<Value>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read session transcript at {}", path.display()))?;
    let mut messages = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if record.get("isSidechain").and_then(Value::as_bool) == Some(true)
            || record.get("isMeta").and_then(Value::as_bool) == Some(true)
        {
            continue;
        }
        let record_type = record.get("type").and_then(Value::as_str).unwrap_or("");
        if record_type != "user" && record_type != "assistant" {
            continue;
        }
        let Some(text) = record
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(text_from_message_content)
        else {
            continue;
        };
        messages.push(json!({
            "role": record_type,
            "text": text,
            "timestamp": record.get("timestamp").cloned().unwrap_or(Value::Null)
        }));
    }
    if messages.len() > limit {
        messages = messages.split_off(messages.len() - limit);
    }
    Ok(messages)
}

/// Metadata-only snapshot from a transcript scan. Never carries a completion
/// status or pending prompt of its own: those are owned by hook events, and the
/// caller preserves any existing DB state (see `sync_state_from_sessions`).
fn scan_snapshot(info: &SessionFileInfo) -> BridgeThreadSnapshot {
    let summary = parse_transcript_summary(&info.path).unwrap_or_default();
    let status_type = match summary.last_record_type.as_deref() {
        Some("user") => "active",
        _ => "idle",
    };
    BridgeThreadSnapshot {
        thread_id: info.session_id.clone(),
        name: summary.name,
        cwd: summary.cwd,
        updated_at: Some(info.mtime_ms),
        status_type: status_type.to_string(),
        status_flags: Vec::new(),
        last_turn_status: None,
        last_preview: summary.last_assistant_text,
        pending_prompt: None,
        event_uid: None,
    }
}

// ---------------------------------------------------------------------------
// Hook event spool

/// Reads one hook payload from stdin and writes it into the spool directory.
/// Invoked as `tinyctb hook-event` from Claude Code hooks; must be fast and
/// must not fail the hosting Claude session.
pub(crate) fn write_hook_event_from_reader<R: Read>(reader: &mut R, now: u64) -> Result<Value> {
    let mut raw = String::new();
    reader
        .by_ref()
        .take(MAX_HOOK_EVENT_BYTES)
        .read_to_string(&mut raw)
        .context("failed to read hook payload from stdin")?;
    let payload: Value =
        serde_json::from_str(raw.trim()).context("hook payload is not valid JSON")?;
    let event_name = payload
        .get("hook_event_name")
        .and_then(Value::as_str)
        .context("hook payload missing hook_event_name")?
        .to_string();
    let session_id = payload
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let spool = events_spool_dir()?;
    fs::create_dir_all(&spool)?;
    let envelope = json!({
        "receivedAt": now,
        "hookEventName": event_name,
        "sessionId": session_id,
        "payload": payload
    });
    let file_name = format!("{now:015}-{}-{}.json", std::process::id(), event_name);
    let final_path = spool.join(&file_name);
    let tmp_path = spool.join(format!(".{file_name}.tmp"));
    fs::write(&tmp_path, serde_json::to_vec(&envelope)?)?;
    fs::rename(&tmp_path, &final_path)?;
    Ok(json!({
        "ok": true,
        "action": "hook_event",
        "hookEventName": event_name,
        "sessionId": session_id,
        "spooled": final_path.display().to_string()
    }))
}

fn pending_prompt_from_notification(
    payload: &Value,
    received_at: u64,
    pending_tool_use: Option<&str>,
) -> PendingPrompt {
    let message = payload
        .get("message")
        .and_then(Value::as_str)
        .map(str::to_string);
    let lowered = message.as_deref().unwrap_or("").to_ascii_lowercase();
    let kind = if lowered.contains("permission") || lowered.contains("approval") {
        "approval"
    } else {
        "reply"
    };
    // The hook message alone ("Claude needs your permission to use Bash") says
    // which tool but not what it would do; the transcript-derived pending tool
    // summary carries the actual content the user must judge.
    let question = match (message, pending_tool_use) {
        (Some(message), Some(tool)) => Some(format!("{message}\n\n⚙️ {tool}")),
        (Some(message), None) => Some(message),
        (None, Some(tool)) => Some(format!("⚙️ {tool}")),
        (None, None) => None,
    };
    PendingPrompt {
        prompt_id: format!("notify:{received_at}"),
        kind: kind.to_string(),
        status: "pending".to_string(),
        question,
    }
}

/// Consume spooled hook events and turn them into per-session snapshots that
/// are allowed to generate notifications. Returns (snapshots, consumed_count).
pub(crate) fn ingest_spool_events(now: u64) -> Result<(Vec<BridgeThreadSnapshot>, usize)> {
    let spool = events_spool_dir()?;
    let Ok(entries) = fs::read_dir(&spool) else {
        return Ok((Vec::new(), 0));
    };
    let mut files = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension().and_then(|ext| ext.to_str()) == Some("json")
                && !path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("")
                    .starts_with('.')
        })
        .collect::<Vec<_>>();
    files.sort();
    files.truncate(MAX_SPOOL_EVENTS_PER_CYCLE);

    let mut consumed = 0usize;
    // Snapshots that already carry a completed answer and were about to be
    // overwritten by a later event in the same batch. Flushing them keeps
    // every answer: two concurrent replies can both finish within one poll
    // cycle, and a new turn can start right after an answer.
    let mut completed: Vec<BridgeThreadSnapshot> = Vec::new();
    let mut by_session: BTreeMap<String, BridgeThreadSnapshot> = BTreeMap::new();
    for path in files {
        // The spool file name ({receivedAt}-{pid}-{event}) is unique even for
        // hooks firing in the same millisecond; it becomes the event uid.
        let event_uid = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(str::to_string);
        let parsed: Option<Value> = fs::read_to_string(&path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok());
        // Consume the file regardless: a malformed spool entry must not wedge
        // the loop forever. This is loud in the daemon log via consumed count.
        let _ = fs::remove_file(&path);
        consumed += 1;
        let Some(envelope) = parsed else {
            eprintln!(
                "tinyctb: discarded malformed hook spool entry {}",
                path.display()
            );
            continue;
        };
        let event_name = envelope
            .get("hookEventName")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let session_id = envelope
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if session_id.is_empty() || session_id == "unknown" {
            continue;
        }
        let received_at = envelope
            .get("receivedAt")
            .and_then(Value::as_u64)
            .unwrap_or(now);
        let payload = envelope.get("payload").cloned().unwrap_or(Value::Null);
        let transcript_path = payload
            .get("transcript_path")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .filter(|path| path.is_file())
            .or_else(|| {
                find_session_file(&session_id)
                    .ok()
                    .flatten()
                    .map(|info| info.path)
            });
        let summary = transcript_path
            .as_deref()
            .and_then(|path| parse_transcript_summary(path).ok())
            .unwrap_or_default();
        let payload_cwd = payload
            .get("cwd")
            .and_then(Value::as_str)
            .map(str::to_string);

        // The Stop payload carries the final answer directly; the transcript
        // parse is the fallback (older Claude Code versions, missing field).
        let payload_answer = payload
            .get("last_assistant_message")
            .and_then(Value::as_str)
            .and_then(|text| normalized_message(Some(text)))
            .map(|text| truncate_chars(&text, MAX_PREVIEW_CHARS));

        let mut base = by_session.remove(&session_id);
        if matches!(event_name.as_str(), "Stop" | "Notification" | "SessionStart") {
            if let Some(previous) =
                base.take_if(|previous| previous.last_turn_status.as_deref() == Some("completed"))
            {
                completed.push(previous);
            }
        }
        let snapshot = match event_name.as_str() {
            "Stop" => BridgeThreadSnapshot {
                thread_id: session_id.clone(),
                name: summary.name.or_else(|| base.as_ref().and_then(|b| b.name.clone())),
                cwd: summary
                    .cwd
                    .or(payload_cwd)
                    .or_else(|| base.as_ref().and_then(|b| b.cwd.clone())),
                updated_at: Some(received_at),
                status_type: "idle".to_string(),
                status_flags: Vec::new(),
                last_turn_status: Some("completed".to_string()),
                last_preview: payload_answer.or(summary.last_assistant_text),
                pending_prompt: None,
                event_uid,
            },
            "Notification" => BridgeThreadSnapshot {
                thread_id: session_id.clone(),
                name: summary.name.or_else(|| base.as_ref().and_then(|b| b.name.clone())),
                cwd: summary
                    .cwd
                    .or(payload_cwd)
                    .or_else(|| base.as_ref().and_then(|b| b.cwd.clone())),
                updated_at: Some(received_at),
                status_type: "active".to_string(),
                status_flags: vec!["waitingOnUserInput".to_string()],
                last_turn_status: None,
                last_preview: summary.last_assistant_text,
                pending_prompt: Some(pending_prompt_from_notification(
                    &payload,
                    received_at,
                    summary.pending_tool_use.as_deref(),
                )),
                event_uid,
            },
            "SessionStart" => BridgeThreadSnapshot {
                thread_id: session_id.clone(),
                name: summary.name,
                cwd: summary.cwd.or(payload_cwd),
                updated_at: Some(received_at),
                status_type: "active".to_string(),
                status_flags: Vec::new(),
                last_turn_status: None,
                last_preview: summary.last_assistant_text,
                pending_prompt: None,
                event_uid,
            },
            _ => {
                if let Some(base) = base {
                    by_session.insert(session_id, base);
                }
                continue;
            }
        };
        by_session.insert(session_id, snapshot);
    }
    // Flushed completed snapshots first (chronologically earlier), then the
    // final per-session state, so later upserts win in the DB.
    completed.extend(by_session.into_values());
    Ok((completed, consumed))
}

// ---------------------------------------------------------------------------
// Sync

fn existing_thread_state(
    conn: &Connection,
    thread_id: &str,
) -> Result<(Option<String>, Option<PendingPrompt>)> {
    use rusqlite::OptionalExtension;
    let last_turn_status: Option<String> = conn
        .query_row(
            "SELECT last_turn_status FROM threads_cache WHERE thread_id = ?1",
            params![thread_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let pending: Option<PendingPrompt> = conn
        .query_row(
            "SELECT prompt_id, prompt_kind, prompt_status, question
             FROM pending_prompts WHERE thread_id = ?1",
            params![thread_id],
            |row| {
                Ok(PendingPrompt {
                    prompt_id: row.get(0)?,
                    kind: row.get(1)?,
                    status: row.get(2)?,
                    question: {
                        let question: String = row.get(3)?;
                        if question.is_empty() {
                            None
                        } else {
                            Some(question)
                        }
                    },
                })
            },
        )
        .optional()?;
    Ok((last_turn_status, pending))
}

/// One backend sync pass:
/// 1. hook spool events -> snapshots that may emit thread_waiting /
///    thread_completed notifications (via `reconcile_thread_snapshots`)
/// 2. transcript scan -> metadata refresh only (existing status and pending
///    prompt are preserved; no events are generated from scans)
/// 3. away-mode notifications are enqueued here, not in the daemon loop:
///    ingesting the spool consumes its files, so every caller (CLI listing
///    commands included) must persist the resulting notifications or they
///    would be silently lost.
pub(crate) fn sync_state_from_sessions(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    limit: u64,
    record_deliveries: bool,
) -> Result<Value> {
    let (hook_snapshots, consumed) = ingest_spool_events(now)?;
    let hook_thread_ids = hook_snapshots
        .iter()
        .map(|snapshot| snapshot.thread_id.clone())
        .collect::<BTreeSet<_>>();
    let reconcile =
        reconcile_thread_snapshots(conn, now, hook_snapshots, record_deliveries)?;

    let scan_limit = config
        .claude
        .as_ref()
        .map(|claude| claude.session_scan_limit)
        .unwrap_or(50)
        .min(limit.max(1));
    let mut threads = reconcile
        .get("threads")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for info in list_session_files(scan_limit)? {
        if hook_thread_ids.contains(&info.session_id) {
            continue;
        }
        let mut snapshot = scan_snapshot(&info);
        let (last_turn_status, pending) = existing_thread_state(conn, &info.session_id)?;
        snapshot.last_turn_status = last_turn_status;
        snapshot.pending_prompt = pending;
        upsert_thread_snapshot(conn, &snapshot, now)?;
        threads.push(thread_snapshot_json(&snapshot));
    }

    let mut result = json!({
        "synced": threads.len(),
        "threads": threads,
        "events": reconcile.get("events").cloned().unwrap_or_else(|| json!([])),
        "away": reconcile.get("away").cloned().unwrap_or(Value::Bool(false)),
        "spoolConsumed": consumed
    });
    let filter = parse_event_filter(Some(&config.events));
    let mut notifiable = Vec::new();
    for event in watch_events_from_sync_result(&result, None) {
        let event_type = event.get("type").and_then(Value::as_str);
        let passes_filter = filter.as_ref().map_or(true, |filter| {
            event_type
                .map(|event_type| filter.contains(event_type))
                .unwrap_or(false)
        });
        // The events filter is an away-notification preference. Events owed to
        // a bridge-initiated turn bypass it: a narrow filter (e.g. only
        // thread_waiting) must not swallow a promised answer — that would also
        // leave the marker unconsumed to mis-match a later completion.
        let bridge_owed = !passes_filter
            && matches!(event_type, Some("thread_completed") | Some("thread_waiting"))
            && match crate::event_thread_id(&event) {
                Some(thread_id) => bridge_turn_pending(conn, &thread_id, now)?,
                None => false,
            };
        if passes_filter || bridge_owed {
            notifiable.push(event);
        }
    }
    let enqueued =
        crate::daemon::enqueue_daemon_notification_events(conn, &notifiable, now)?;
    if let Some(object) = result.as_object_mut() {
        object.insert("enqueued".to_string(), json!(enqueued));
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Watch event helpers (shape kept from the codex bridge)

pub(crate) fn parse_event_filter(input: Option<&str>) -> Option<BTreeSet<String>> {
    input
        .map(|value| {
            value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect::<BTreeSet<_>>()
        })
        .filter(|set| !set.is_empty())
}

pub(crate) fn filter_watch_events(
    events: Vec<Value>,
    filter: Option<&BTreeSet<String>>,
) -> Vec<Value> {
    match filter {
        None => events,
        Some(filter) => events
            .into_iter()
            .filter(|event| {
                event
                    .get("type")
                    .and_then(Value::as_str)
                    .map(|kind| filter.contains(kind))
                    .unwrap_or(false)
            })
            .collect(),
    }
}

pub(crate) fn watch_thread_error_event(error: &anyhow::Error, now: u64) -> Value {
    let message = error.to_string();
    json!({
        "type": "thread_error",
        "message": message,
        // observedAt lets the away-window check pass (the error happened now).
        // The key is unique per occurrence; deduping a continuous failure
        // streak is the daemon's job (an active-error flag cleared by the
        // next successful sync), so the same error recurring after recovery
        // notifies again.
        "observedAt": now,
        "eventKey": format!(
            "sync-error-{now}-{}",
            crate::sha256_hex(message.as_bytes())
        )
    })
}

fn enrich_event_with_thread(event: Value, threads: &[Value]) -> Value {
    let Some(thread_id) = event.get("threadId").and_then(Value::as_str) else {
        return event;
    };
    // One batch can carry several snapshots of the same session (two answers
    // from concurrent replies), so pair the event with the snapshot it came
    // from — otherwise the second answer would render the first answer's
    // preview. The spool uid is exact even for same-millisecond hooks; the
    // updated_at comparison is only a fallback for uid-less events.
    let event_uid = event.get("eventUid").filter(|value| !value.is_null());
    let updated_at = event.get("updatedAt").filter(|value| !value.is_null());
    let matches_id = |thread: &&Value| -> bool {
        thread.get("threadId").and_then(Value::as_str) == Some(thread_id)
    };
    let Some(thread) = threads
        .iter()
        .find(|thread| {
            matches_id(thread)
                && match event_uid {
                    Some(uid) => thread.get("eventUid") == Some(uid),
                    None => {
                        updated_at.map_or(true, |updated| thread.get("updatedAt") == Some(updated))
                    }
                }
        })
        .or_else(|| threads.iter().find(matches_id))
    else {
        return event;
    };
    let mut enriched = event;
    if let Some(object) = enriched.as_object_mut() {
        object.insert("thread".to_string(), thread.clone());
    }
    enriched
}

pub(crate) fn watch_events_from_sync_result(
    sync_result: &Value,
    filter: Option<&BTreeSet<String>>,
) -> Vec<Value> {
    let threads = sync_result
        .get("threads")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let events = sync_result
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|event| enrich_event_with_thread(event, &threads))
        .collect::<Vec<_>>();
    filter_watch_events(events, filter)
}

// ---------------------------------------------------------------------------
// Headless turns (detached `claude -p` processes)

fn claude_config(config: &DaemonConfig) -> ClaudeConfig {
    config.claude.clone().unwrap_or_default()
}

fn headless_command_args(
    claude: &ClaudeConfig,
    prompt: &str,
    session: SessionRef<'_>,
) -> Vec<String> {
    let mut args = vec!["-p".to_string(), prompt.to_string()];
    match session {
        SessionRef::Resume(session_id) => {
            args.push("--resume".to_string());
            args.push(session_id.to_string());
        }
        SessionRef::New(session_id) => {
            args.push("--session-id".to_string());
            args.push(session_id.to_string());
        }
    }
    args.push("--output-format".to_string());
    args.push("json".to_string());
    args.push("--permission-mode".to_string());
    args.push(claude.permission_mode.clone());
    args
}

enum SessionRef<'a> {
    Resume(&'a str),
    New(&'a str),
}

#[cfg(test)]
pub(crate) mod test_spawn {
    use std::sync::Mutex;

    pub(crate) type SpawnRecord = (String, Vec<String>, Option<String>);

    pub(crate) static RECORDED: Mutex<Vec<SpawnRecord>> = Mutex::new(Vec::new());

    pub(crate) fn take() -> Vec<SpawnRecord> {
        std::mem::take(&mut RECORDED.lock().expect("test spawn lock"))
    }
}

#[cfg_attr(test, allow(clippy::needless_return))]
fn spawn_detached_headless(
    binary: &Path,
    args: &[String],
    cwd: Option<&str>,
    log_name: &str,
) -> Result<Option<u32>> {
    #[cfg(test)]
    {
        test_spawn::RECORDED.lock().expect("test spawn lock").push((
            binary.display().to_string(),
            args.to_vec(),
            cwd.map(str::to_string),
        ));
        let _ = log_name;
        return Ok(Some(0));
    }
    #[cfg(not(test))]
    {
        let logs_dir = turn_logs_dir()?;
        fs::create_dir_all(&logs_dir)?;
        let log_path = logs_dir.join(format!("{log_name}.log"));
        let log_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("failed to open turn log at {}", log_path.display()))?;
        let err_file = log_file.try_clone()?;
        let mut command = Command::new(binary);
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(err_file));
        if let Some(cwd) = cwd.filter(|value| Path::new(value).is_dir()) {
            command.current_dir(cwd);
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let child = command
            .spawn()
            .with_context(|| format!("failed to spawn {} for headless turn", binary.display()))?;
        Ok(Some(child.id()))
    }
}

/// Start a headless turn that continues an existing session. Returns
/// immediately; the answer is delivered later through the Stop hook event.
pub(crate) fn send_user_message(
    config: &DaemonConfig,
    session_id: &str,
    message: &str,
    cwd_hint: Option<&str>,
    now: u64,
) -> Result<Value> {
    let message =
        normalized_message(Some(message)).context("reply message cannot be empty")?;
    let claude = claude_config(config);
    let binary = resolve_claude_binary()?;
    let cwd = cwd_hint
        .map(str::to_string)
        .or_else(|| {
            find_session_file(session_id)
                .ok()
                .flatten()
                .and_then(|info| parse_transcript_summary(&info.path).ok())
                .and_then(|summary| summary.cwd)
        });
    let args = headless_command_args(&claude, &message, SessionRef::Resume(session_id));
    let pid = spawn_detached_headless(
        &binary.path,
        &args,
        cwd.as_deref(),
        &format!("{session_id}-{now}"),
    )?;
    Ok(json!({
        "ok": true,
        "action": "reply_started",
        "threadId": session_id,
        "message": message,
        "cwd": cwd,
        "claude": {
            "transport": "headless-cli",
            "binary": binary.path.display().to_string(),
            "binarySource": binary.source,
            "pid": pid,
            "permissionMode": claude.permission_mode
        },
        "delivery": {
            "mode": "hook_notification",
            "status": "turn_started"
        },
        "sentAt": now
    }))
}

/// Start a brand-new headless session in the given working directory. The
/// session id is generated locally and passed via `--session-id`, so the
/// caller can route replies immediately.
pub(crate) fn start_thread_in_cwd(
    config: &DaemonConfig,
    cwd: Option<&str>,
    message: Option<&str>,
    now: u64,
) -> Result<Value> {
    let message = normalized_message(message).context("new session prompt cannot be empty")?;
    let claude = claude_config(config);
    let binary = resolve_claude_binary()?;
    let session_id = generate_session_uuid()?;
    let args = headless_command_args(&claude, &message, SessionRef::New(&session_id));
    let pid = spawn_detached_headless(
        &binary.path,
        &args,
        cwd,
        &format!("{session_id}-{now}"),
    )?;
    Ok(json!({
        "ok": true,
        "action": "new",
        "threadId": session_id,
        "cwd": cwd,
        "message": message,
        "claude": {
            "transport": "headless-cli",
            "binary": binary.path.display().to_string(),
            "binarySource": binary.source,
            "pid": pid,
            "permissionMode": claude.permission_mode
        },
        "delivery": {
            "mode": "hook_notification",
            "status": "turn_started"
        },
        "sentAt": now
    }))
}

// ---------------------------------------------------------------------------
// Daemon wakeup watcher (spool dir + Claude projects dir)

pub(crate) struct ClaudeWatchReceiver {
    rx: std::sync::mpsc::Receiver<()>,
    _watcher: notify::RecommendedWatcher,
}

impl ClaudeWatchReceiver {
    pub(crate) fn recv_timeout(&self, timeout: Duration) {
        let _ = self.rx.recv_timeout(timeout);
        while self.rx.try_recv().is_ok() {}
    }
}

pub(crate) fn start_claude_watch_receiver() -> Result<ClaudeWatchReceiver> {
    let spool = events_spool_dir()?;
    fs::create_dir_all(&spool).ok();
    let projects = claude_projects_dir()?;
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::RecommendedWatcher::new(
        move |result: notify::Result<notify::Event>| {
            if result.is_ok() {
                let _ = tx.send(());
            }
        },
        notify::Config::default(),
    )?;

    let mut watched_any = false;
    if spool.exists() {
        watcher.watch(&spool, notify::RecursiveMode::NonRecursive)?;
        watched_any = true;
    }
    if projects.exists() {
        watcher.watch(&projects, notify::RecursiveMode::Recursive)?;
        watched_any = true;
    }
    if !watched_any {
        bail!("No Claude Code paths exist to watch");
    }
    Ok(ClaudeWatchReceiver {
        rx,
        _watcher: watcher,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::create_state_db_in_memory;
    use std::io::Write;

    struct TempDirGuard {
        path: PathBuf,
    }

    impl TempDirGuard {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("tinyctb-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn write_transcript(dir: &Path, session_id: &str, lines: &[Value]) -> PathBuf {
        let path = dir.join(format!("{session_id}.jsonl"));
        let mut file = fs::File::create(&path).expect("create transcript");
        for line in lines {
            writeln!(file, "{line}").expect("write transcript line");
        }
        path
    }

    #[test]
    fn generates_valid_uuid_v4() {
        let uuid = generate_session_uuid().expect("uuid");
        assert_eq!(uuid.len(), 36);
        assert_eq!(uuid.as_bytes()[14], b'4');
        let variant = uuid.as_bytes()[19];
        assert!(matches!(variant, b'8' | b'9' | b'a' | b'b'));
        assert_ne!(uuid, generate_session_uuid().expect("uuid2"));
    }

    #[test]
    fn transcript_summary_extracts_title_cwd_and_last_assistant_text() {
        let temp = TempDirGuard::new("transcript-summary");
        let path = write_transcript(
            &temp.path,
            "sess-1",
            &[
                json!({"type": "ai-title", "aiTitle": "Fix the parser", "sessionId": "sess-1"}),
                json!({"type": "user", "cwd": "/home/user/project", "message": {"role": "user", "content": "please fix"}}),
                json!({"type": "assistant", "cwd": "/home/user/project", "message": {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "hmm"},
                    {"type": "text", "text": "Working on it."}
                ]}}),
                json!({"type": "assistant", "isSidechain": true, "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "subagent noise"}
                ]}}),
                json!({"type": "assistant", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "Done: parser fixed."}
                ]}}),
                json!({"type": "unknown-future-record", "whatever": true}),
            ],
        );

        let summary = parse_transcript_summary(&path).expect("summary");
        assert_eq!(summary.name.as_deref(), Some("Fix the parser"));
        assert_eq!(summary.cwd.as_deref(), Some("/home/user/project"));
        assert_eq!(
            summary.last_assistant_text.as_deref(),
            Some("Done: parser fixed.")
        );
        assert_eq!(summary.last_record_type.as_deref(), Some("assistant"));
    }

    #[test]
    fn transcript_summary_tracks_pending_tool_use_until_answered() {
        let temp = TempDirGuard::new("transcript-pending-tool");
        let path = write_transcript(
            &temp.path,
            "sess-tool",
            &[
                json!({"type": "user", "message": {"role": "user", "content": "build it"}}),
                json!({"type": "assistant", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "Building now."},
                    {"type": "tool_use", "name": "Bash", "input": {"command": "cargo build --release"}}
                ]}}),
            ],
        );
        let summary = parse_transcript_summary(&path).expect("summary");
        assert_eq!(
            summary.pending_tool_use.as_deref(),
            Some("Bash: cargo build --release")
        );

        // Once the tool result arrives the call is no longer pending.
        let path = write_transcript(
            &temp.path,
            "sess-tool-done",
            &[
                json!({"type": "assistant", "message": {"role": "assistant", "content": [
                    {"type": "tool_use", "name": "Bash", "input": {"command": "cargo build --release"}}
                ]}}),
                json!({"type": "user", "message": {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "t1", "content": "ok"}
                ]}}),
            ],
        );
        let summary = parse_transcript_summary(&path).expect("summary");
        assert_eq!(summary.pending_tool_use, None);
    }

    #[test]
    fn transcript_summary_renders_ask_user_question_options() {
        let temp = TempDirGuard::new("transcript-ask-user");
        let path = write_transcript(
            &temp.path,
            "sess-ask",
            &[
                json!({"type": "assistant", "message": {"role": "assistant", "content": [
                    {"type": "tool_use", "name": "AskUserQuestion", "input": {"questions": [{
                        "question": "Which database should we use?",
                        "options": [{"label": "Postgres"}, {"label": "SQLite"}]
                    }]}}
                ]}}),
            ],
        );
        let summary = parse_transcript_summary(&path).expect("summary");
        let pending = summary.pending_tool_use.expect("pending tool");
        assert!(pending.contains("Which database should we use?"));
        assert!(pending.contains("Postgres / SQLite"));
    }

    #[test]
    fn permission_notification_includes_pending_tool_detail() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("spool-permission-detail");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects = temp.path.join("projects").join("-home-user-project");
        fs::create_dir_all(&projects).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            temp.path.join("projects").display().to_string(),
        );
        write_transcript(
            &projects,
            "sess-perm",
            &[
                json!({"type": "assistant", "cwd": "/home/user/project", "message": {"role": "assistant", "content": [
                    {"type": "tool_use", "name": "Bash", "input": {"command": "rm -rf build/"}}
                ]}}),
            ],
        );
        let mut notify_payload = std::io::Cursor::new(
            json!({
                "hook_event_name": "Notification",
                "session_id": "sess-perm",
                "cwd": "/home/user/project",
                "message": "Claude needs your permission to use Bash"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut notify_payload, 1000).expect("spool notify");

        let (snapshots, _) = ingest_spool_events(2000).expect("ingest");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        let waiting = snapshots
            .iter()
            .find(|snapshot| snapshot.thread_id == "sess-perm")
            .expect("permission snapshot");
        let prompt = waiting.pending_prompt.as_ref().expect("pending prompt");
        assert_eq!(prompt.kind, "approval");
        let question = prompt.question.as_deref().expect("question");
        assert!(question.contains("Claude needs your permission to use Bash"));
        assert!(
            question.contains("⚙️ Bash: rm -rf build/"),
            "notification body must show what the tool would do: {question}"
        );
    }

    #[test]
    fn transcript_messages_skip_meta_and_tool_records() {
        let temp = TempDirGuard::new("transcript-messages");
        let path = write_transcript(
            &temp.path,
            "sess-2",
            &[
                json!({"type": "user", "isMeta": true, "message": {"role": "user", "content": "meta noise"}}),
                json!({"type": "user", "message": {"role": "user", "content": "real question"}}),
                json!({"type": "user", "message": {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "t1", "content": "tool output"}
                ]}}),
                json!({"type": "assistant", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "real answer"}
                ]}}),
            ],
        );

        let messages = parse_transcript_messages(&path, 10).expect("messages");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["text"], "real question");
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["text"], "real answer");
    }

    #[test]
    fn spool_ingestion_builds_stop_and_notification_snapshots() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("spool-ingest");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects = temp.path.join("projects").join("-home-user-project");
        fs::create_dir_all(&projects).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            temp.path.join("projects").display().to_string(),
        );
        write_transcript(
            &projects,
            "sess-stop",
            &[
                json!({"type": "assistant", "cwd": "/home/user/project", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "final answer"}
                ]}}),
            ],
        );

        let mut stop_payload = std::io::Cursor::new(
            json!({
                "hook_event_name": "Stop",
                "session_id": "sess-stop",
                "cwd": "/home/user/project"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut stop_payload, 1000).expect("spool stop");
        let mut notify_payload = std::io::Cursor::new(
            json!({
                "hook_event_name": "Notification",
                "session_id": "sess-notify",
                "cwd": "/home/user/other",
                "message": "Claude needs your permission to use Bash"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut notify_payload, 1001).expect("spool notify");

        let (snapshots, consumed) = ingest_spool_events(2000).expect("ingest");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        assert_eq!(consumed, 2);
        assert_eq!(snapshots.len(), 2);
        let stop = snapshots
            .iter()
            .find(|snapshot| snapshot.thread_id == "sess-stop")
            .expect("stop snapshot");
        assert_eq!(stop.last_turn_status.as_deref(), Some("completed"));
        assert_eq!(stop.last_preview.as_deref(), Some("final answer"));
        assert!(stop.pending_prompt.is_none());
        let waiting = snapshots
            .iter()
            .find(|snapshot| snapshot.thread_id == "sess-notify")
            .expect("notify snapshot");
        let prompt = waiting.pending_prompt.as_ref().expect("pending prompt");
        assert_eq!(prompt.kind, "approval");
        assert_eq!(
            prompt.question.as_deref(),
            Some("Claude needs your permission to use Bash")
        );
    }

    #[test]
    fn sync_preserves_completed_status_across_metadata_scans() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("sync-preserve");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects_root = temp.path.join("projects");
        let project = projects_root.join("-home-user-project");
        fs::create_dir_all(&project).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            projects_root.display().to_string(),
        );
        write_transcript(
            &project,
            "sess-sync",
            &[
                json!({"type": "assistant", "cwd": "/home/user/project", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "answer one"}
                ]}}),
            ],
        );
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };

        // Away on so hook snapshots emit events.
        set_away_mode(&conn, true, 500).expect("away on");
        let mut stop_payload = std::io::Cursor::new(
            json!({
                "hook_event_name": "Stop",
                "session_id": "sess-sync",
                "cwd": "/home/user/project"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut stop_payload, 1000).expect("spool stop");

        let first = sync_state_from_sessions(&conn, &config, 2000, 50, true).expect("sync 1");
        let events = first["events"].as_array().expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "thread_completed");

        // Second sync: no hook events; the metadata scan must not clear the
        // completed status or emit a duplicate event.
        let second = sync_state_from_sessions(&conn, &config, 3000, 50, true).expect("sync 2");
        assert_eq!(second["events"].as_array().expect("events").len(), 0);
        let thread = second["threads"]
            .as_array()
            .expect("threads")
            .iter()
            .find(|thread| thread["threadId"] == "sess-sync")
            .expect("thread snapshot")
            .clone();
        assert_eq!(thread["lastTurnStatus"], "completed");

        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");
    }

    #[test]
    fn cli_sync_preserves_away_notifications_from_consumed_spool() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("cli-sync-enqueue");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects_root = temp.path.join("projects");
        let project = projects_root.join("-home-user-project");
        fs::create_dir_all(&project).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            projects_root.display().to_string(),
        );
        write_transcript(
            &project,
            "sess-cli",
            &[
                json!({"type": "assistant", "cwd": "/home/user/project", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "cli answer"}
                ]}}),
            ],
        );
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        set_away_mode(&conn, true, 500).expect("away on");
        let mut stop_payload = std::io::Cursor::new(
            json!({
                "hook_event_name": "Stop",
                "session_id": "sess-cli",
                "cwd": "/home/user/project"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut stop_payload, 1000).expect("spool stop");

        // A CLI listing command (record_deliveries = false) consumes the spool
        // file; the notification must survive as a pending outbound event so
        // the daemon can still deliver it.
        let result = sync_state_from_sessions(&conn, &config, 2000, 50, false).expect("cli sync");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        assert_eq!(result["enqueued"], 1);
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            1
        );
    }

    /// The full target scenario for bridge-initiated turns: user is NOT away,
    /// replied from Telegram (marker set), the headless turn's Stop hook lands
    /// in the spool. The answer must reach the outbox through the real
    /// ingest -> reconcile -> enqueue chain, and must survive /back.
    #[test]
    fn bridge_turn_answer_flows_from_stop_hook_when_not_away() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("bridge-turn-flow");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects_root = temp.path.join("projects");
        let project = projects_root.join("-home-user-project");
        fs::create_dir_all(&project).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            projects_root.display().to_string(),
        );
        write_transcript(
            &project,
            "sess-bridge",
            &[
                json!({"type": "assistant", "cwd": "/home/user/project", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "bridge answer"}
                ]}}),
            ],
        );
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        // Away is OFF; the Telegram reply marked the turn.
        crate::state::mark_bridge_turn(&conn, "sess-bridge", 500).expect("mark");
        let mut stop_payload = std::io::Cursor::new(
            json!({
                "hook_event_name": "Stop",
                "session_id": "sess-bridge",
                "cwd": "/home/user/project"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut stop_payload, 1000).expect("spool stop");

        // Daemon-path sync (record_deliveries = true), exactly what daemon_cycle runs.
        let result = sync_state_from_sessions(&conn, &config, 2000, 50, true).expect("sync");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        assert_eq!(
            result["enqueued"], 1,
            "bridge answer must be enqueued while the user is present: {result}"
        );
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            1
        );

        // /back clears only the away backlog, not the owed bridge answer.
        let disabled = set_away_mode(&conn, false, 2500).expect("back");
        assert_eq!(disabled["clearedPendingNotifications"], 0);
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending after /back"),
            1,
            "an undelivered bridge answer must survive /back"
        );
    }

    /// Two concurrent Telegram replies to one session can both finish before a
    /// single daemon poll: two Stop spool files, one sync, and BOTH answers
    /// must reach the outbox (with their own previews), fully consuming the
    /// two-count marker.
    #[test]
    fn two_stops_in_one_cycle_push_two_answers() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("two-stops-one-cycle");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects_root = temp.path.join("projects");
        let project = projects_root.join("-home-user-project");
        fs::create_dir_all(&project).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            projects_root.display().to_string(),
        );
        write_transcript(
            &project,
            "sess-two",
            &[
                json!({"type": "assistant", "cwd": "/home/user/project", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "latest transcript text"}
                ]}}),
            ],
        );
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        // Away OFF; two Telegram replies were sent before either finished.
        crate::state::mark_bridge_turn(&conn, "sess-two", 400).expect("mark 1");
        crate::state::mark_bridge_turn(&conn, "sess-two", 500).expect("mark 2");
        for (received_at, answer) in [(1000u64, "answer one"), (1100, "answer two")] {
            let mut stop_payload = std::io::Cursor::new(
                json!({
                    "hook_event_name": "Stop",
                    "session_id": "sess-two",
                    "cwd": "/home/user/project",
                    "last_assistant_message": answer
                })
                .to_string(),
            );
            write_hook_event_from_reader(&mut stop_payload, received_at).expect("spool stop");
        }

        let result = sync_state_from_sessions(&conn, &config, 2000, 50, true).expect("sync");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        assert_eq!(
            result["enqueued"], 2,
            "both answers must be enqueued: {result}"
        );
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            2
        );
        assert!(
            !crate::state::bridge_turn_pending(&conn, "sess-two", 2500).expect("marker"),
            "both outstanding turns must be consumed"
        );
        // Each event keeps its own answer text.
        let events = result["events"].as_array().expect("events");
        let previews = events
            .iter()
            .filter_map(|event| event.get("lastPreview").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert!(previews.contains(&"answer one"), "events: {events:?}");
        assert!(previews.contains(&"answer two"), "events: {events:?}");
    }

    /// Same-millisecond variant: two hook processes can finish in the same
    /// millisecond, so the spool files differ only by PID. Event keys must use
    /// the spool uid, not the timestamp, or one answer is deduped away.
    #[test]
    fn two_same_millisecond_stops_push_two_answers() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("same-ms-stops");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects_root = temp.path.join("projects");
        fs::create_dir_all(projects_root.join("-home-user-project")).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            projects_root.display().to_string(),
        );
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        crate::state::mark_bridge_turn(&conn, "sess-samems", 400).expect("mark 1");
        crate::state::mark_bridge_turn(&conn, "sess-samems", 500).expect("mark 2");
        // Two spool files with identical receivedAt, distinct (fake) PIDs —
        // exactly what write_hook_event_from_reader produces in two processes.
        let spool = events_spool_dir().expect("spool dir");
        fs::create_dir_all(&spool).expect("create spool");
        for (pid, answer) in [(111u32, "answer one"), (222, "answer two")] {
            let envelope = json!({
                "receivedAt": 1000,
                "hookEventName": "Stop",
                "sessionId": "sess-samems",
                "payload": {
                    "hook_event_name": "Stop",
                    "session_id": "sess-samems",
                    "cwd": "/home/user/project",
                    "last_assistant_message": answer
                }
            });
            fs::write(
                spool.join(format!("{:015}-{}-Stop.json", 1000, pid)),
                envelope.to_string(),
            )
            .expect("write spool file");
        }

        let result = sync_state_from_sessions(&conn, &config, 2000, 50, true).expect("sync");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        assert_eq!(
            result["enqueued"], 2,
            "same-millisecond answers must both be enqueued: {result}"
        );
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            2
        );
        assert!(
            !crate::state::bridge_turn_pending(&conn, "sess-samems", 2500).expect("marker"),
            "both outstanding turns must be consumed"
        );

        // End to end: each rendered Telegram message must carry its own
        // answer, not the other snapshot's preview (uid-exact enrichment).
        let enriched = watch_events_from_sync_result(&result, None);
        let completed = enriched
            .iter()
            .filter(|event| event["type"] == "thread_completed")
            .collect::<Vec<_>>();
        assert_eq!(completed.len(), 2, "events: {enriched:?}");
        for event in completed {
            let own = event["lastPreview"].as_str().expect("event preview");
            let other = if own == "answer one" {
                "answer two"
            } else {
                "answer one"
            };
            let prepared = crate::telegram::render::prepare_telegram_delivery("999", event)
                .expect("prepared delivery");
            let text = prepared.payloads[0]["text"].as_str().expect("text");
            assert!(text.contains(own), "missing own answer in: {text}");
            assert!(
                !text.contains(other),
                "message must not show the other snapshot's answer: {text}"
            );
        }
    }

    /// A narrow events filter (user only wants thread_waiting pushes) must not
    /// swallow the answer to a turn started from Telegram.
    #[test]
    fn narrow_events_filter_does_not_swallow_bridge_answers() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("narrow-filter-bridge");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects_root = temp.path.join("projects");
        let project = projects_root.join("-home-user-project");
        fs::create_dir_all(&project).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            projects_root.display().to_string(),
        );
        write_transcript(
            &project,
            "sess-filtered",
            &[
                json!({"type": "assistant", "cwd": "/home/user/project", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "filtered answer"}
                ]}}),
            ],
        );
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: "thread_waiting".to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        crate::state::mark_bridge_turn(&conn, "sess-filtered", 500).expect("mark");
        let mut stop_payload = std::io::Cursor::new(
            json!({
                "hook_event_name": "Stop",
                "session_id": "sess-filtered",
                "cwd": "/home/user/project"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut stop_payload, 1000).expect("spool stop");

        let result = sync_state_from_sessions(&conn, &config, 2000, 50, true).expect("sync");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        assert_eq!(
            result["enqueued"], 1,
            "bridge answer must bypass the events filter: {result}"
        );
        assert!(
            !crate::state::bridge_turn_pending(&conn, "sess-filtered", 2500).expect("marker"),
            "marker must be consumed so it cannot mis-match a later completion"
        );
    }

    #[test]
    fn headless_reply_spawns_resume_with_permission_mode() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        let _ = test_spawn::take();
        if resolve_claude_binary().is_err() {
            // Environment without a claude binary: skip (binary resolution is
            // validated separately by doctor).
            return;
        }

        let result = send_user_message(&config, "sess-reply", "continue please", None, 4000)
            .expect("send reply");
        assert_eq!(result["action"], "reply_started");
        assert_eq!(result["threadId"], "sess-reply");

        let spawned = test_spawn::take();
        assert_eq!(spawned.len(), 1);
        let (_, args, _) = &spawned[0];
        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"sess-reply".to_string()));
        assert!(args.contains(&"bypassPermissions".to_string()));
        assert!(args.contains(&"json".to_string()));
    }

    #[test]
    fn headless_new_session_generates_session_id() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        let _ = test_spawn::take();
        if resolve_claude_binary().is_err() {
            return;
        }

        let result =
            start_thread_in_cwd(&config, Some("/tmp"), Some("build the thing"), 5000)
                .expect("start thread");
        assert_eq!(result["action"], "new");
        let thread_id = result["threadId"].as_str().expect("threadId");
        assert_eq!(thread_id.len(), 36);

        let spawned = test_spawn::take();
        assert_eq!(spawned.len(), 1);
        let (_, args, cwd) = &spawned[0];
        assert!(args.contains(&"--session-id".to_string()));
        assert!(args.contains(&thread_id.to_string()));
        assert_eq!(cwd.as_deref(), Some("/tmp"));
    }
}
