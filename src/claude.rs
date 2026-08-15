//! Claude Code backend: session discovery via `~/.claude/projects` JSONL
//! transcripts, event ingestion via hook spool files, and headless turns via
//! detached `claude -p` processes.

use anyhow::{bail, Context, Result};
use notify::Watcher;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use crate::config::{ClaudeConfig, DaemonConfig};
use crate::state::{
    clear_pending_outbound_events, get_setting_number, get_setting_text,
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

/// Shared by the approval gate so a Telegram approval shows the same amount
/// of detail as a permission notification.
pub(crate) fn truncate_tool_detail(detail: &str) -> String {
    truncate_chars(detail, MAX_TOOL_DETAIL_CHARS)
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
            format!(
                "{name}: {}",
                truncate_chars(detail.trim(), MAX_TOOL_DETAIL_CHARS)
            )
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
    // Hooks are children of the Claude session process, so they inherit its
    // messaging socket. Recording it here is how the bridge learns, from the
    // session itself, where to deliver a reply while that session is live —
    // no pid guessing, no ambiguity.
    let messaging_socket = env::var("CLAUDE_CODE_MESSAGING_SOCKET")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    // Captured here so a later injection can prove the socket is still THIS
    // session's (the path embeds a pid and can be rebound after pid reuse).
    let (socket_inode, socket_boot_id) = messaging_socket
        .as_deref()
        .map(socket_identity)
        .unwrap_or((None, None));
    // For a Notification, freeze the transcript boundary AT THIS MOMENT:
    // everything the file gains afterwards is what happened since the dialog
    // appeared — the evidence a later scan uses to decide it was dealt with.
    // (Ingest can run minutes later; measuring there would be too late.)
    let transcript_bytes = (event_name == "Notification")
        .then(|| {
            payload
                .get("transcript_path")
                .and_then(Value::as_str)
                .and_then(|path| fs::metadata(path).ok())
                .map(|meta| meta.len())
        })
        .flatten();
    let envelope = json!({
        "receivedAt": now,
        "hookEventName": event_name,
        "sessionId": session_id,
        "messagingSocket": messaging_socket,
        "socketInode": socket_inode,
        "socketBootId": socket_boot_id,
        "transcriptBytes": transcript_bytes,
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

/// Was this prompt DEALT WITH, judged by what the transcript gained after
/// the byte boundary recorded when the notification fired?
///
/// Definite evidence only, with two review-caught traps excluded:
/// - SIDECHAIN entries (`isSidechain: true`) are a subagent talking to
///   itself in the same file — the main session can sit at a permission
///   dialog while a subagent streams, so they prove nothing;
/// - a `tool_result` cannot be attributed to THIS approval (the payload
///   carries no tool_use_id, and a parallel already-allowed tool finishing
///   first would masquerade as the answer), so results never clear an
///   approval. They do clear an idle prompt: tool activity means the turn
///   is running.
///
/// What clears what:
/// - approval: a MAIN-CHAIN assistant entry only. Every outcome of a
///   permission dialog — allow, deny, cancel — ends with the assistant
///   continuing on the main chain;
/// - reply (idle): main-chain assistant, a tool_result, or real user text.
///
/// Rows without a boundary (pre-upgrade) are left alone: only their turn's
/// Stop clears them, exactly the old behaviour.
fn prompt_resolved_in_transcript(prompt: &PendingPrompt, transcript: &Path) -> bool {
    let Some(boundary) = prompt.transcript_bytes else {
        return false;
    };
    let Ok(file) = fs::File::open(transcript) else {
        return false;
    };
    use std::io::{BufRead as _, Seek as _};
    let mut reader = std::io::BufReader::new(file);
    if reader.seek(std::io::SeekFrom::Start(boundary)).is_err() {
        return false;
    }
    // STREAM the whole tail, line by line, stopping at the first evidence.
    // A fixed byte window looked cheaper but was wrong twice over: sidechain
    // chatter can push the real main-chain evidence past any cap (the prompt
    // would then linger to its Stop forever), and per-line lossy conversion
    // confines UTF-8 damage to the one mangled line the JSON parse skips.
    let mut buf: Vec<u8> = Vec::new();
    loop {
        buf.clear();
        let Ok(read) = reader.read_until(b'\n', &mut buf) else {
            return false;
        };
        if read == 0 {
            return false; // EOF: no evidence yet
        }
        let line = String::from_utf8_lossy(&buf);
        let Ok(entry) = serde_json::from_str::<Value>(line.trim()) else {
            continue; // partial trailing line or non-JSON metadata
        };
        if entry.get("isSidechain").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        match entry.get("type").and_then(Value::as_str) {
            Some("assistant") => return true,
            Some("user") if prompt.kind != "approval" => {
                let content = entry.pointer("/message/content");
                let has_tool_result = content.and_then(Value::as_array).is_some_and(|blocks| {
                    blocks.iter().any(|block| {
                        block.get("type").and_then(Value::as_str) == Some("tool_result")
                    })
                });
                let has_text = match content {
                    Some(Value::String(text)) => !text.trim().is_empty(),
                    Some(Value::Array(blocks)) => blocks
                        .iter()
                        .any(|block| block.get("type").and_then(Value::as_str) == Some("text")),
                    _ => false,
                };
                if has_tool_result || has_text {
                    return true;
                }
            }
            _ => {}
        }
    }
}

/// Which notification types mean "a dialog is waiting on the user". The
/// others (auth_success, elicitation_complete/response, agent_completed)
/// announce things FINISHING — turning those into pending prompts is how
/// phantom waits used to be born.
fn notification_waiting_kind(payload: &Value) -> Option<&'static str> {
    // Text fallback shared by "no field" (older claude) and "unknown type"
    // (newer claude than us): a DENYLIST of known completions, because an
    // allowlist would silently drop every wait type invented after this
    // code was written.
    let text_kind = || {
        let lowered = payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        if lowered.contains("permission") || lowered.contains("approval") {
            "approval"
        } else {
            "reply"
        }
    };
    match payload.get("notification_type").and_then(Value::as_str) {
        Some("permission_prompt") => Some("approval"),
        Some("idle_prompt" | "agent_needs_input") => Some("reply"),
        Some("elicitation_dialog" | "elicitation_url_dialog") => Some("reply"),
        // Known completions: announcements, never waits.
        Some(
            "auth_success" | "elicitation_complete" | "elicitation_response" | "agent_completed",
        ) => None,
        Some(unknown) => {
            eprintln!(
                "tinyctb: unknown notification_type {unknown:?}; treating as a wait by message text"
            );
            Some(text_kind())
        }
        None => Some(text_kind()),
    }
}

fn pending_prompt_from_notification(
    payload: &Value,
    received_at: u64,
    pending_tool_use: Option<&str>,
    kind: &str,
    transcript_bytes: Option<u64>,
) -> PendingPrompt {
    let message = payload
        .get("message")
        .and_then(Value::as_str)
        .map(str::to_string);
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
        transcript_bytes,
    }
}

/// Consume spooled hook events and turn them into per-session snapshots that
/// are allowed to generate notifications. Also returns the session ->
/// messaging-socket mappings the hooks reported, so replies can be delivered
/// into a live session instead of forking it with a headless `--resume`.
/// Returns (snapshots, sockets, consumed_count).
/// Spool files in chronological order (the name starts with the timestamp),
/// skipping partial writes and non-events.
fn spool_event_files() -> Result<Vec<PathBuf>> {
    let spool = events_spool_dir()?;
    let Ok(entries) = fs::read_dir(&spool) else {
        return Ok(Vec::new());
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
    Ok(files)
}

#[derive(Debug, Clone)]
pub(crate) struct SessionSocket {
    pub(crate) path: String,
    pub(crate) inode: Option<u64>,
    pub(crate) boot_id: Option<String>,
}

fn session_socket_from_envelope(envelope: &Value) -> Option<SessionSocket> {
    let path = envelope
        .get("messagingSocket")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(SessionSocket {
        path: path.to_string(),
        inode: envelope.get("socketInode").and_then(Value::as_u64),
        boot_id: envelope
            .get("socketBootId")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// Read the session -> messaging-socket mappings out of the spool WITHOUT
/// consuming it. The daemon runs this before handling Telegram updates: a
/// reply that arrives in the same cycle as the session's first hook event
/// must already see the mapping, otherwise it would fall back to a headless
/// `--resume` and fork the very session we can now deliver into.
pub(crate) fn peek_session_sockets(conn: &Connection, now: u64) -> Result<usize> {
    // Same file selection as ingestion (sorted, .json only, no temp files,
    // bounded per cycle) so a large backlog cannot make this unbounded. The
    // spool name starts with the timestamp, so sorting is chronological and
    // the NEWEST mapping per session wins deterministically — read_dir order
    // would otherwise decide which socket a session ends up with.
    let mut files = spool_event_files()?;
    if files.len() > MAX_SPOOL_EVENTS_PER_CYCLE {
        files = files.split_off(files.len() - MAX_SPOOL_EVENTS_PER_CYCLE);
    }
    let mut latest: BTreeMap<String, SessionSocket> = BTreeMap::new();
    for path in files {
        let Some(envelope) = fs::read_to_string(&path)
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        else {
            continue;
        };
        let session_id = envelope
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if session_id.is_empty() || session_id == "unknown" {
            continue;
        }
        if let Some(socket) = session_socket_from_envelope(&envelope) {
            latest.insert(session_id.to_string(), socket);
        }
    }
    for (session_id, socket) in &latest {
        crate::state::record_session_messaging_socket(conn, session_id, socket, now)?;
    }
    Ok(latest.len())
}

pub(crate) fn ingest_spool_events(
    now: u64,
) -> Result<(
    Vec<BridgeThreadSnapshot>,
    BTreeMap<String, SessionSocket>,
    usize,
)> {
    let mut files = spool_event_files()?;
    files.truncate(MAX_SPOOL_EVENTS_PER_CYCLE);

    let mut consumed = 0usize;
    let mut sockets: BTreeMap<String, SessionSocket> = BTreeMap::new();
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
        if let Some(socket) = session_socket_from_envelope(&envelope) {
            sockets.insert(session_id.clone(), socket);
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
        if matches!(
            event_name.as_str(),
            "Stop" | "Notification" | "SessionStart"
        ) {
            if let Some(previous) =
                base.take_if(|previous| previous.last_turn_status.as_deref() == Some("completed"))
            {
                completed.push(previous);
            }
        }
        let snapshot = match event_name.as_str() {
            "Stop" => BridgeThreadSnapshot {
                thread_id: session_id.clone(),
                name: summary
                    .name
                    .or_else(|| base.as_ref().and_then(|b| b.name.clone())),
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
            "Notification" => {
                // Completion-style notifications (auth_success, elicitation
                // results, agent_completed) are NOT waits; turning them into
                // pending prompts is how phantom "waiting on you" rows were
                // born. They leave the session state untouched.
                let Some(kind) = notification_waiting_kind(&payload) else {
                    if let Some(base) = base {
                        by_session.insert(session_id, base);
                    }
                    continue;
                };
                let transcript_bytes = envelope.get("transcriptBytes").and_then(Value::as_u64);
                BridgeThreadSnapshot {
                    thread_id: session_id.clone(),
                    name: summary
                        .name
                        .or_else(|| base.as_ref().and_then(|b| b.name.clone())),
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
                        kind,
                        transcript_bytes,
                    )),
                    event_uid,
                }
            }
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
    Ok((completed, sockets, consumed))
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
            "SELECT prompt_id, prompt_kind, prompt_status, question, transcript_bytes
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
                    transcript_bytes: row.get::<_, Option<i64>>(4)?.map(|bytes| bytes as u64),
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
    let (hook_snapshots, sockets, consumed) = ingest_spool_events(now)?;
    for (session_id, socket) in &sockets {
        crate::state::record_session_messaging_socket(conn, session_id, socket, now)?;
    }
    let hook_thread_ids = hook_snapshots
        .iter()
        .map(|snapshot| snapshot.thread_id.clone())
        .collect::<BTreeSet<_>>();
    let reconcile = reconcile_thread_snapshots(conn, now, hook_snapshots, record_deliveries)?;

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
        // Without this check an answered dialog stayed "pending" until the
        // turn's Stop — for a long agentic turn that meant /threads pinning
        // a phantom "waiting on you" for hours (measured live 2026-08-15:
        // it talked a user into killing an active session).
        snapshot.pending_prompt =
            pending.filter(|prompt| !prompt_resolved_in_transcript(prompt, &info.path));
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
        // The events filter is an away-notification preference; it must not
        // swallow an answer owed to a message the user injected from
        // Telegram (the headless path bypasses it for the same reason).
        let owed = !passes_filter
            && event_type == Some("thread_completed")
            && match crate::event_thread_id(&event) {
                Some(thread_id) => crate::state::live_injection_pending(
                    conn,
                    &thread_id,
                    event.get("updatedAt").and_then(Value::as_u64),
                    now,
                )?,
                None => false,
            };
        if passes_filter || owed {
            notifiable.push(event);
        }
    }
    let enqueued = crate::daemon::enqueue_daemon_notification_events(conn, &notifiable, now)?;
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

/// Environment token stamped on every process the bridge spawns. Hooks run
/// as descendants of the claude process and inherit it, which gives the
/// headless approval gate a first-layer identity check that needs no
/// database: no token, no bridge turn — however broken tinyctb's own state
/// may be, a user's terminal session can never be misclassified.
pub(crate) const BRIDGE_TURN_ENV: &str = "TINYCTB_BRIDGE_TURN";

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

/// Live handles of spawned headless turns. The daemon reaps these every cycle
/// so finished children never linger as zombies — a zombie still answers
/// `kill -0`, which would make crash detection report "running" forever.
#[cfg(not(test))]
mod turn_children {
    use std::process::Child;
    use std::sync::Mutex;

    pub(super) static RUNNING: Mutex<Vec<Child>> = Mutex::new(Vec::new());
}

#[cfg(test)]
pub(crate) mod test_identity_persist {
    use std::sync::atomic::AtomicBool;

    /// Makes `persist_spawn_identity` fail, so tests can prove a turn whose
    /// identity cannot be recorded is terminated instead of left running.
    pub(crate) static FAIL: AtomicBool = AtomicBool::new(false);
}

#[cfg(test)]
pub(crate) mod test_settle_fail {
    use std::sync::atomic::AtomicBool;

    /// Makes `settle_failed_turn`'s database write fail, so tests can prove
    /// a settle error is reported instead of swallowed.
    pub(crate) static FAIL: AtomicBool = AtomicBool::new(false);
}

#[cfg(test)]
pub(crate) mod test_kill {
    use std::sync::Mutex;

    pub(crate) static KILLED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

    pub(crate) fn take() -> Vec<u32> {
        std::mem::take(&mut KILLED.lock().expect("test kill lock"))
    }
}

fn ps_value(pid: u32, field: &str) -> Option<String> {
    let output = Command::new("ps")
        .args(["-o", field, "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!value.is_empty()).then_some(value)
}

/// The RESOLVED executable path of a running process (`/proc/<pid>/exe`).
/// Diagnostics only (racy across a wrapper's `exec`, absent on macOS); it
/// takes no part in the restart-kill decision.
fn process_exe_path(pid: u32) -> Option<String> {
    fs::read_link(format!("/proc/{pid}/exe"))
        .ok()
        .map(|path| path.display().to_string())
}

/// starttime ticks from `/proc/<pid>/stat` (field 22). Invariant across
/// `exec` and unique per PID incarnation — the authoritative identity for
/// the restart-kill check. Linux only.
fn process_start_ticks(pid: u32) -> Option<String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // comm (field 2) may contain spaces/parens; fields resume after last ')'.
    let rest = stat.rsplit_once(')')?.1;
    rest.split_whitespace().nth(19).map(str::to_string)
}

/// This boot's unique id. starttime ticks restart from zero on reboot, so
/// ticks are only meaningful within one boot — the boot id scopes them.
fn current_boot_id() -> Option<String> {
    fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ProcessIdentity {
    pub(crate) lstart: Option<String>,
    pub(crate) pgid: Option<u32>,
    pub(crate) exe: Option<String>,
    pub(crate) start_ticks: Option<String>,
    pub(crate) boot_id: Option<String>,
}

/// Identity of a just-spawned turn, persisted so a restarted daemon can later
/// verify the PID still belongs to the turn before signalling it. Best
/// effort: missing components make verification refuse to kill.
pub(crate) fn capture_process_identity(pid: Option<u32>) -> ProcessIdentity {
    let Some(pid) = pid.filter(|pid| *pid > 0) else {
        return ProcessIdentity::default();
    };
    ProcessIdentity {
        lstart: ps_value(pid, "lstart="),
        pgid: ps_value(pid, "pgid=").and_then(|value| value.trim().parse::<u32>().ok()),
        exe: process_exe_path(pid),
        start_ticks: process_start_ticks(pid),
        boot_id: current_boot_id(),
    }
}

/// Identity check for the restart case (no Child handle). Requires the full
/// Linux identity chain of boot id (scopes ticks to one boot), process group,
/// and starttime ticks (exec-invariant, unique per PID incarnation: a
/// `CLAUDE_BIN` wrapper exec'ing the real program keeps its identity, while
/// a reused PID gets new ticks). Anything less fails closed — including all
/// of macOS, where /proc does not exist: there a timed-out turn after a
/// daemon restart is abandoned as 'expired' WITHOUT being signalled, because
/// killing on weak identity is worse than leaking a process.
pub(crate) fn verified_restart_identity(turn: &crate::state::BridgeTurn, pid: u32) -> bool {
    let (Some(stored_pgid), Some(stored_ticks), Some(stored_boot)) = (
        turn.pgid,
        turn.proc_start_ticks.as_deref(),
        turn.boot_id.as_deref(),
    ) else {
        return false;
    };
    let Some(current_pgid) =
        ps_value(pid, "pgid=").and_then(|value| value.trim().parse::<u32>().ok())
    else {
        return false;
    };
    if current_pgid != stored_pgid {
        return false;
    }
    let Some(current_boot) = current_boot_id() else {
        return false;
    };
    if current_boot != stored_boot.trim() {
        return false;
    }
    process_start_ticks(pid).as_deref() == Some(stored_ticks)
}

/// TERM the whole process group, give the main child a short grace to exit
/// cleanly, then KILL the group UNCONDITIONALLY — a grandchild that ignores
/// TERM must not survive just because the main process exited politely.
/// Returns whether the supplied main child was reaped (true when no handle
/// was supplied); an unreaped child must go back to the registry so a later
/// cycle can collect it.
pub(crate) fn terminate_process_group(pid: u32, child: Option<&mut std::process::Child>) -> bool {
    if pid == 0 {
        return true;
    }
    let group = format!("-{pid}");
    // Returns whether the signal was actually delivered; PATH, permission or
    // resource problems must degrade gracefully, never wedge the daemon.
    let signal_group = |signal: &str| -> bool {
        Command::new("kill")
            .args([signal, "--", &group])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    };
    let termed = signal_group("-TERM");
    match child {
        Some(child) => {
            if termed {
                let started = std::time::Instant::now();
                while started.elapsed() < Duration::from_secs(2) {
                    if matches!(child.try_wait(), Ok(Some(_))) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            if !signal_group("-KILL") {
                // External kill unavailable: at least SIGKILL the main child
                // directly through the handle (no PATH involved).
                let _ = child.kill();
            }
            // Bounded reap — never block the daemon indefinitely on a child
            // that could not be signalled.
            let started = std::time::Instant::now();
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => return true,
                    _ if started.elapsed() >= Duration::from_secs(2) => return false,
                    _ => std::thread::sleep(Duration::from_millis(100)),
                }
            }
        }
        None => {
            std::thread::sleep(Duration::from_millis(500));
            signal_group("-KILL");
            true
        }
    }
}

/// Terminate a timed-out headless turn. With a live handle the child is
/// killed and reaped directly; after a daemon restart the stored identity
/// must fully match, otherwise we refuse to signal (PID reuse).
pub(crate) fn kill_turn_process(turn: &crate::state::BridgeTurn) {
    #[cfg(test)]
    {
        test_kill::KILLED
            .lock()
            .expect("test kill lock")
            .push(turn.pid.unwrap_or(0));
    }
    #[cfg(not(test))]
    {
        let Some(pid) = turn.pid.filter(|pid| *pid > 0) else {
            return;
        };
        let mut running = turn_children::RUNNING.lock().expect("turn children lock");
        if let Some(index) = running.iter().position(|child| child.id() == pid) {
            let mut child = running.remove(index);
            drop(running);
            if !terminate_process_group(pid, Some(&mut child)) {
                // Not reaped within the bound: hand the child back so the
                // per-cycle reaper can collect it later instead of leaking a
                // forever-unreapable zombie.
                turn_children::RUNNING
                    .lock()
                    .expect("turn children lock")
                    .push(child);
            }
        } else {
            drop(running);
            if verified_restart_identity(turn, pid) {
                terminate_process_group(pid, None);
            }
        }
    }
}

/// Reap finished headless children. Returns (pid, exit_code) per finished
/// child; exit_code is None if the status is unavailable.
pub(crate) fn reap_finished_turn_processes() -> Vec<(u32, Option<i32>)> {
    #[cfg(test)]
    {
        Vec::new()
    }
    #[cfg(not(test))]
    {
        let mut running = turn_children::RUNNING.lock().expect("turn children lock");
        let mut finished = Vec::new();
        running.retain_mut(|child| match child.try_wait() {
            Ok(Some(status)) => {
                finished.push((child.id(), status.code()));
                false
            }
            Ok(None) => true,
            Err(_) => {
                finished.push((child.id(), None));
                false
            }
        });
        finished
    }
}

/// Spawn a headless turn with its `bridge_turns` row already on disk.
///
/// The order is the point: the row is inserted BEFORE the process exists.
/// Two consumers race the spawn — the headless approval gate admits a tool
/// call only if it finds a running row for the session, and the daemon's
/// crash detection assumes every child it reaps is registered. Both would
/// misread a turn whose registration was still in flight; a turn's first
/// tool call is only bounded below by Claude's startup time, which is not a
/// guarantee.
///
/// A spawn failure settles the row as `failed` immediately — a permanently
/// "running" row would keep admitting gate calls for a process that never
/// existed and read as a crash forever.
#[allow(clippy::too_many_arguments)]
fn spawn_registered_headless(
    conn: &rusqlite::Connection,
    binary: &Path,
    args: &[String],
    cwd: Option<&str>,
    turn_id: &str,
    thread_id: &str,
    log_path: &Path,
    now: u64,
) -> Result<Option<u32>> {
    crate::state::register_bridge_turn(
        conn,
        turn_id,
        thread_id,
        &log_path.display().to_string(),
        None,
        None,
        None,
        None,
        None,
        None,
        now,
    )?;
    let pid = match spawn_detached_headless(binary, args, cwd, turn_id) {
        Ok(pid) => pid,
        Err(err) => return Err(settle_failed_turn(conn, turn_id, now, err)),
    };
    // Identity arrives in a second write because it cannot exist before the
    // spawn — and losing that write is NOT survivable. With `pid` still NULL
    // the daemon's crash check calls the turn failed after its 10s grace,
    // the turn stops counting as running, and from then on a live process
    // runs outside the approval boundary with nobody able to reap it. So the
    // write is retried and verified, and a turn that cannot be recorded is
    // killed rather than left running unsupervised.
    let identity = capture_process_identity(pid);
    if let Err(err) = persist_spawn_identity(conn, turn_id, pid, &identity) {
        kill_spawned_child(pid);
        let err = err.context(
            "could not record the spawned turn's identity; the process was terminated \
             rather than left running outside the approval boundary",
        );
        return Err(settle_failed_turn(conn, turn_id, now, err));
    }
    Ok(pid)
}

/// Settle a turn whose spawn went wrong, and fold how THAT went into the
/// reported error — a dropped settle error would leave the row `running`
/// with nobody told. If settling itself keeps failing, the row does stay
/// `running` with a NULL pid; the daemon's 10s crash grace then flags the
/// turn failed on its own, loudly — delayed and noisier, but never silent.
fn settle_failed_turn(
    conn: &rusqlite::Connection,
    turn_id: &str,
    now: u64,
    cause: anyhow::Error,
) -> anyhow::Error {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 0..3 {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(50));
        }
        #[cfg(test)]
        if test_settle_fail::FAIL.load(std::sync::atomic::Ordering::SeqCst) {
            last_error = Some(anyhow::anyhow!("injected settle failure"));
            continue;
        }
        match crate::state::mark_bridge_turn_finished(conn, turn_id, "failed", now) {
            Ok(()) => return cause,
            Err(err) => last_error = Some(err),
        }
    }
    let settle_error = last_error.unwrap_or_else(|| anyhow::anyhow!("settle never ran"));
    cause.context(format!(
        "additionally, settling turn {turn_id} as failed also failed ({settle_error:#}); \
         the row is still 'running' with no pid, so the daemon's grace-period check \
         will flag it within seconds"
    ))
}

/// Retry the identity write and verify it touched exactly the one row the
/// registration created. Transient `SQLITE_BUSY` from the daemon's own
/// connection is the expected failure here; three spaced attempts outlast it.
fn persist_spawn_identity(
    conn: &rusqlite::Connection,
    turn_id: &str,
    pid: Option<u32>,
    identity: &ProcessIdentity,
) -> Result<()> {
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 0..3 {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(50));
        }
        #[cfg(test)]
        if test_identity_persist::FAIL.load(std::sync::atomic::Ordering::SeqCst) {
            last_error = Some(anyhow::anyhow!("injected identity persist failure"));
            continue;
        }
        match crate::state::record_bridge_turn_spawn(
            conn,
            turn_id,
            pid,
            identity.lstart.as_deref(),
            identity.exe.as_deref(),
            identity.pgid,
            identity.start_ticks.as_deref(),
            identity.boot_id.as_deref(),
        ) {
            Ok(1) => return Ok(()),
            Ok(0) => {
                last_error = Some(anyhow::anyhow!(
                    "identity update matched no RUNNING row for turn {turn_id} — the daemon \
                     may have already settled it as crashed"
                ))
            }
            Ok(rows) => {
                last_error = Some(anyhow::anyhow!(
                    "identity update touched {rows} rows for turn {turn_id}, expected exactly 1"
                ))
            }
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("identity update never ran")))
}

/// Kill a child this process just spawned (it is still in RUNNING). The
/// narrow cousin of `kill_turn_process`: no restart-identity check needed,
/// because the pid came straight from our own `spawn`.
fn kill_spawned_child(pid: Option<u32>) {
    #[cfg(test)]
    {
        test_kill::KILLED
            .lock()
            .expect("test kill lock")
            .push(pid.unwrap_or(0));
    }
    #[cfg(not(test))]
    {
        let Some(pid) = pid.filter(|pid| *pid > 0) else {
            return;
        };
        let mut running = turn_children::RUNNING.lock().expect("turn children lock");
        if let Some(index) = running.iter().position(|child| child.id() == pid) {
            let mut child = running.remove(index);
            drop(running);
            if !terminate_process_group(pid, Some(&mut child)) {
                // Not reaped within the bound: hand the child back so the
                // per-cycle reaper collects it later instead of leaking a
                // forever-unreapable zombie.
                turn_children::RUNNING
                    .lock()
                    .expect("turn children lock")
                    .push(child);
            }
        }
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
        // create_new: every turn owns its log exclusively — two turns writing
        // one file would interleave and corrupt result attribution.
        let log_file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&log_path)
            .with_context(|| format!("failed to create turn log at {}", log_path.display()))?;
        let err_file = log_file.try_clone()?;
        let mut command = Command::new(binary);
        command
            .args(args)
            // The turn token: the headless gate's first-layer identity.
            .env(BRIDGE_TURN_ENV, log_name)
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
        let pid = child.id();
        turn_children::RUNNING
            .lock()
            .expect("turn children lock")
            .push(child);
        Ok(Some(pid))
    }
}

/// Deliver a message straight into a LIVE Claude session over its unix
/// messaging socket (one JSON line, the interface Claude Code documents for
/// injection). This is what keeps a Telegram reply from forking the session:
/// a headless `--resume` would branch the transcript from whatever the state
/// was when it started, invisible to the terminal the user is sitting at.
/// Returns Ok(false) when the session has no live socket, so the caller can
/// fall back to the headless path.
/// Identity of a messaging socket at the moment a hook reported it. The
/// socket path is `cc-socks/<pid>.sock`, so a later session that reuses the
/// pid rebinds the SAME path — the inode is what distinguishes the two
/// (a rebind always creates a fresh socket), scoped by boot id because
/// tmpfs inode numbers restart with the machine.
pub(crate) fn socket_identity(socket_path: &str) -> (Option<u64>, Option<String>) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let Some(inode) = fs::metadata(socket_path).ok().map(|meta| meta.ino()) else {
            return (None, None);
        };
        // The inode alone is NOT enough: tmpfs hands the same inode number
        // straight back after unlink+rebind (proven by test). The owning
        // session is identified by the pid embedded in the socket name plus
        // that pid's exec-invariant start ticks; boot id scopes both.
        let owner_ticks = Path::new(socket_path)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u32>().ok())
            .and_then(process_start_ticks);
        // Fail closed when the owner cannot be identified (custom
        // --messaging-socket-path with no pid in the name): an inode is not
        // proof of anything here, so such sessions simply fall back to the
        // headless path instead of risking delivery into a stranger.
        let stamp = match (current_boot_id(), owner_ticks) {
            (Some(boot), Some(ticks)) => Some(format!("{boot}:{ticks}")),
            _ => None,
        };
        (Some(inode), stamp)
    }
    #[cfg(not(unix))]
    {
        let _ = socket_path;
        (None, None)
    }
}

/// Where a session's live end actually is. `Window` and `Background` are
/// both injectable (a reply lands in the session either way); the difference
/// is whether the user can SEE it — a background-hosted session counted as
/// "terminal" made the /threads census claim one more window than the screen
/// showed (observed live, 2026-08-14).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalPresence {
    /// A terminal the user has open.
    Window,
    /// Alive under Claude Code's background pty host — no visible window.
    Background,
    /// No verified live socket at all.
    Gone,
}

/// Same identity rule as `inject_into_live_session` (recorded inode + boot
/// id must still match the socket on disk, unverifiable counts as dead), but
/// without connecting — this is for display, and a connect could perturb the
/// session.
pub(crate) fn session_terminal_presence(
    conn: &rusqlite::Connection,
    thread_id: &str,
) -> Result<TerminalPresence> {
    // A database error is NOT `Gone`: `Gone` renders as "idle", and a state
    // read failure dressed up as idleness is exactly the silent degradation
    // the caller needs to display honestly.
    let Some(socket) = crate::state::session_messaging_socket(conn, thread_id)? else {
        return Ok(TerminalPresence::Gone);
    };
    if !Path::new(&socket.path).exists() {
        return Ok(TerminalPresence::Gone);
    }
    let (Some(expected_inode), Some(expected_boot)) = (socket.inode, socket.boot_id) else {
        return Ok(TerminalPresence::Gone);
    };
    let (current_inode, current_boot) = socket_identity(&socket.path);
    if current_inode != Some(expected_inode)
        || current_boot.as_deref() != Some(expected_boot.as_str())
    {
        return Ok(TerminalPresence::Gone);
    }
    // The socket name carries the owning pid; a session whose parent is the
    // background pty host has no window. Unreadable /proc degrades to
    // Window — the pid was just verified alive, and guessing "background"
    // would recreate the missing-window confusion in the other direction.
    let hosted = Path::new(&socket.path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.parse::<u32>().ok())
        .is_some_and(parent_is_bg_pty_host);
    if hosted {
        Ok(TerminalPresence::Background)
    } else {
        Ok(TerminalPresence::Window)
    }
}

fn parent_is_bg_pty_host(pid: u32) -> bool {
    let ppid = fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| {
            stat.rsplit_once(')')?
                .1
                .split_whitespace()
                .nth(1)?
                .parse::<u32>()
                .ok()
        });
    let Some(ppid) = ppid else {
        return false;
    };
    fs::read_to_string(format!("/proc/{ppid}/cmdline"))
        .map(|cmdline| cmdline.contains("bg-pty-host"))
        .unwrap_or(false)
}

const INJECT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Deliver a message straight into a LIVE Claude session over its unix
/// messaging socket (one JSON line, the interface Claude Code documents for
/// injection). Returns Ok(false) when the session is not live or is not the
/// one we recorded, so the caller falls back to the headless path.
///
/// `expected` is the (inode, boot_id) captured when the session reported the
/// socket; both must still match or we refuse — otherwise a rebound path
/// would send the user's message into a different session.
pub(crate) fn inject_into_live_session(
    socket_path: &str,
    expected: (Option<u64>, Option<String>),
    message: &str,
) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::net::UnixStream;

        if !Path::new(socket_path).exists() {
            return Ok(false);
        }
        // Fail closed on identity: an unverifiable socket is treated as not
        // live rather than risking delivery into someone else's session.
        let (Some(expected_inode), Some(expected_boot)) = (expected.0, expected.1) else {
            return Ok(false);
        };
        let (current_inode, current_boot) = socket_identity(socket_path);
        if current_inode != Some(expected_inode) || current_boot.as_deref() != Some(&expected_boot)
        {
            return Ok(false);
        }

        // `UnixStream::connect` blocks with no timeout: a socket whose owner
        // is alive but no longer accepting (full backlog) would wedge the
        // daemon. Connect on a worker thread with a deadline; on timeout the
        // receiver drops, the eventual stream is closed unwritten, so a late
        // connect can never duplicate the message.
        // A connect that outlives the deadline cannot be cancelled, so the
        // number of such threads is hard-capped: past the cap we report "not
        // live" and fall back to the headless path rather than accumulating
        // one stuck thread per reply.
        static IN_FLIGHT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        const MAX_IN_FLIGHT_CONNECTS: usize = 2;
        struct InFlightGuard;
        impl Drop for InFlightGuard {
            fn drop(&mut self) {
                IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        if IN_FLIGHT.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= MAX_IN_FLIGHT_CONNECTS {
            IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            eprintln!(
                "tinyctb: {socket_path} has {MAX_IN_FLIGHT_CONNECTS} connects still pending; treating as not live"
            );
            return Ok(false);
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let path = socket_path.to_string();
        std::thread::spawn(move || {
            let _guard = InFlightGuard;
            let _ = tx.send(UnixStream::connect(&path));
        });
        let mut stream = match rx.recv_timeout(INJECT_CONNECT_TIMEOUT) {
            Ok(Ok(stream)) => stream,
            // Stale socket file for an exited session, or a connect that did
            // not answer in time: not an error, just "not live".
            Ok(Err(_)) | Err(_) => return Ok(false),
        };
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .context("failed to set messaging socket write timeout")?;
        let payload = json!({
            "type": "user",
            "message": { "role": "user", "content": message }
        });
        let mut line = serde_json::to_vec(&payload)?;
        line.push(b'\n');
        stream
            .write_all(&line)
            .with_context(|| format!("failed to write to messaging socket {socket_path}"))?;
        stream.flush().ok();
        Ok(true)
    }
    #[cfg(not(unix))]
    {
        let _ = (socket_path, expected, message);
        Ok(false)
    }
}

/// Start a headless turn that continues an existing session. Returns
/// immediately; the answer is delivered later through the Stop hook event.
pub(crate) fn send_user_message(
    conn: &rusqlite::Connection,
    config: &DaemonConfig,
    session_id: &str,
    message: &str,
    cwd_hint: Option<&str>,
    now: u64,
) -> Result<Value> {
    let message = normalized_message(Some(message)).context("reply message cannot be empty")?;
    let claude = claude_config(config);
    let binary = resolve_claude_binary()?;
    let cwd = cwd_hint.map(str::to_string).or_else(|| {
        find_session_file(session_id)
            .ok()
            .flatten()
            .and_then(|info| parse_transcript_summary(&info.path).ok())
            .and_then(|summary| summary.cwd)
    });
    // The random suffix keeps turn ids unique even for two replies to the
    // same session in one update batch (which share `now`).
    let turn_id = format!("{session_id}-{now}-{}", &generate_session_uuid()?[..8]);
    let log_path = turn_logs_dir()?.join(format!("{turn_id}.log"));
    let args = headless_command_args(&claude, &message, SessionRef::Resume(session_id));
    let pid = spawn_registered_headless(
        conn,
        &binary.path,
        &args,
        cwd.as_deref(),
        &turn_id,
        session_id,
        &log_path,
        now,
    )?;
    let identity = capture_process_identity(pid);
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
            "procStart": identity.lstart,
            "pgid": identity.pgid,
            "procExe": identity.exe,
            "procStartTicks": identity.start_ticks,
            "procBootId": identity.boot_id,
            "permissionMode": claude.permission_mode,
            "turnId": turn_id,
            "logPath": log_path.display().to_string()
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
    conn: &rusqlite::Connection,
    config: &DaemonConfig,
    cwd: Option<&str>,
    message: Option<&str>,
    now: u64,
) -> Result<Value> {
    let message = normalized_message(message).context("new session prompt cannot be empty")?;
    let claude = claude_config(config);
    let binary = resolve_claude_binary()?;
    let session_id = generate_session_uuid()?;
    let turn_id = format!("{session_id}-{now}-{}", &generate_session_uuid()?[..8]);
    let log_path = turn_logs_dir()?.join(format!("{turn_id}.log"));
    let args = headless_command_args(&claude, &message, SessionRef::New(&session_id));
    let pid = spawn_registered_headless(
        conn,
        &binary.path,
        &args,
        cwd,
        &turn_id,
        &session_id,
        &log_path,
        now,
    )?;
    let identity = capture_process_identity(pid);
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
            "procStart": identity.lstart,
            "pgid": identity.pgid,
            "procExe": identity.exe,
            "procStartTicks": identity.start_ticks,
            "procBootId": identity.boot_id,
            "permissionMode": claude.permission_mode,
            "turnId": turn_id,
            "logPath": log_path.display().to_string()
        },
        "delivery": {
            "mode": "hook_notification",
            "status": "turn_started"
        },
        "sentAt": now
    }))
}

// ---------------------------------------------------------------------------
// Bridge turn results (read back from the turn's own log file)

#[derive(Debug, Clone)]
pub(crate) struct BridgeTurnResult {
    pub(crate) is_error: bool,
    pub(crate) text: String,
}

/// Parse the final `--output-format json` result out of a turn log. The log
/// mixes stderr lines with the single result JSON object, so scan from the
/// end for the first line that parses as a `"type":"result"` object. Returns
/// None while the turn is still running (no result line yet).
pub(crate) fn read_bridge_turn_result(log_path: &Path) -> Option<BridgeTurnResult> {
    let raw = fs::read_to_string(log_path).ok()?;
    for line in raw.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if record.get("type").and_then(Value::as_str) != Some("result") {
            continue;
        }
        let is_error = record
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || record.get("subtype").and_then(Value::as_str) != Some("success");
        let text = record
            .get("result")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                format!(
                    "(turn ended with subtype `{}` and no result text)",
                    record
                        .get("subtype")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                )
            });
        return Some(BridgeTurnResult { is_error, text });
    }
    None
}

/// Last chars of a turn log, for failure notices.
pub(crate) fn turn_log_tail(log_path: &Path, max_chars: usize) -> String {
    let Ok(raw) = fs::read_to_string(log_path) else {
        return String::new();
    };
    let trimmed = raw.trim();
    let count = trimmed.chars().count();
    if count <= max_chars {
        return trimmed.to_string();
    }
    trimmed.chars().skip(count - max_chars).collect()
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

        let (snapshots, _, _) = ingest_spool_events(2000).expect("ingest");
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

    /// The resolution rule, judged on transcript CONTENT after the recorded
    /// boundary — never on mtime. The decisive counterexample (review-
    /// caught): a background task notification is injected as user text
    /// while a permission dialog still waits; it advances mtime but must
    /// not count as the answer.
    #[test]
    fn prompt_resolution_needs_definite_evidence() {
        let dir = std::env::temp_dir().join(format!("tinyctb-resolve-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let transcript = dir.join("t.jsonl");
        let base = r#"{"type":"user","message":{"role":"user","content":"do the thing"}}
"#;
        fs::write(&transcript, base).expect("base");
        let boundary = base.len() as u64;
        let prompt = |kind: &str| crate::state::PendingPrompt {
            prompt_id: "notify:1000".to_string(),
            kind: kind.to_string(),
            status: "pending".to_string(),
            question: Some("Claude needs your permission".to_string()),
            transcript_bytes: Some(boundary),
        };

        // Nothing after the boundary: still waiting.
        assert!(!prompt_resolved_in_transcript(
            &prompt("approval"),
            &transcript
        ));

        // Injected task notification (user TEXT) while the dialog waits:
        // resolves an idle prompt (the session will process it) but must
        // NOT resolve a permission dialog.
        let with_task = format!(
            "{base}{}
",
            r#"{"type":"user","message":{"role":"user","content":"<task-notification>done</task-notification>"}}"#
        );
        fs::write(&transcript, &with_task).expect("task");
        assert!(
            !prompt_resolved_in_transcript(&prompt("approval"), &transcript),
            "mtime moved, but a permission dialog may still be on screen"
        );
        assert!(prompt_resolved_in_transcript(&prompt("reply"), &transcript));

        // A tool RESULT cannot be attributed to THIS approval — a parallel
        // already-allowed tool finishing first looks identical — so it must
        // NOT clear an approval. It does clear an idle prompt.
        let with_result = format!(
            "{with_task}{}
",
            r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}}"#
        );
        fs::write(&transcript, &with_result).expect("result");
        assert!(
            !prompt_resolved_in_transcript(&prompt("approval"), &transcript),
            "an unattributable result must not answer for the dialog"
        );
        assert!(prompt_resolved_in_transcript(&prompt("reply"), &transcript));

        // A SIDECHAIN assistant entry is a subagent talking in the same
        // file — the main session may still be sitting at the dialog.
        fs::write(
            &transcript,
            format!(
                "{base}{}
",
                r#"{"type":"assistant","isSidechain":true,"message":{"role":"assistant","content":[{"type":"text","text":"subagent"}]}}"#
            ),
        )
        .expect("sidechain");
        assert!(
            !prompt_resolved_in_transcript(&prompt("approval"), &transcript),
            "sidechain output is not the main turn moving"
        );
        assert!(
            !prompt_resolved_in_transcript(&prompt("reply"), &transcript),
            "not for idle prompts either"
        );

        // Evidence followed by bytes that are INVALID UTF-8 (as a capped
        // read can produce by cutting a multi-byte character): the valid
        // evidence before the damage must still count.
        let mut damaged = format!(
            "{base}{}
",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"onward"}]}}"#
        )
        .into_bytes();
        damaged.extend_from_slice(&[0xE4, 0xB8]); // truncated 中
        fs::write(&transcript, &damaged).expect("utf8 damage");
        assert!(
            prompt_resolved_in_transcript(&prompt("approval"), &transcript),
            "a mangled tail must not discard evidence that already arrived"
        );

        // Evidence BEYOND what any fixed byte window would cover: megabytes
        // of sidechain chatter first, the real main-chain continuation
        // after. A capped read never saw it and the prompt lingered to its
        // Stop — streaming must find it.
        let filler_line = format!(
            "{}
",
            r#"{"type":"assistant","isSidechain":true,"message":{"role":"assistant","content":[{"type":"text","text":"PADPADPADPADPADPADPADPADPADPADPADPADPADPADPADPADPADPADPADPADPADPADPADPAD"}]}}"#
        );
        let mut huge = String::with_capacity(6 * 1024 * 1024);
        huge.push_str(base);
        while huge.len() < 5 * 1024 * 1024 {
            huge.push_str(&filler_line);
        }
        huge.push_str(
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"finally"}]}}"#,
        );
        huge.push('\n');
        fs::write(&transcript, &huge).expect("huge transcript");
        assert!(
            prompt_resolved_in_transcript(&prompt("approval"), &transcript),
            "evidence past 4MiB of sidechain chatter must still be found"
        );

        // Assistant activity is definite for anything.
        fs::write(
            &transcript,
            format!(
                "{base}{}
",
                r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"onward"}]}}"#
            ),
        )
        .expect("assistant");
        assert!(prompt_resolved_in_transcript(
            &prompt("approval"),
            &transcript
        ));

        // No boundary recorded (pre-upgrade row): never auto-resolved.
        let legacy = crate::state::PendingPrompt {
            transcript_bytes: None,
            ..prompt("approval")
        };
        fs::write(&transcript, &with_result).expect("rewrite");
        assert!(!prompt_resolved_in_transcript(&legacy, &transcript));
        let _ = fs::remove_dir_all(&dir);
    }

    /// End to end through the scan path: a dialog the user already dealt
    /// with (tool result after the boundary) is cleared by the next sync,
    /// while one with only injected task text after it survives. This is
    /// the bug that pinned a phantom "waiting on you" to /threads for hours
    /// of a long agentic turn.
    #[test]
    fn sync_clears_prompts_the_user_already_answered() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let projects =
            std::env::temp_dir().join(format!("tinyctb-answered-prompt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&projects);
        let workspace = projects.join("-home-user-x");
        fs::create_dir_all(&workspace).expect("projects dir");
        std::env::set_var("TINYCTB_CLAUDE_PROJECTS_DIR", &projects);
        let conn = crate::state::create_state_db_in_memory().expect("db");

        let base = r#"{"type":"user","message":{"role":"user","content":"hi"}}
"#;
        let transcript = workspace.join("sess-answered.jsonl");
        fs::write(&transcript, base).expect("transcript");
        let boundary = base.len() as u64;

        let seed = |bytes: Option<u64>| {
            crate::state::upsert_thread_snapshot(
                &conn,
                &crate::state::BridgeThreadSnapshot {
                    thread_id: "sess-answered".to_string(),
                    name: None,
                    cwd: None,
                    updated_at: Some(1000),
                    status_type: "active".to_string(),
                    status_flags: vec![],
                    last_turn_status: None,
                    last_preview: None,
                    pending_prompt: Some(crate::state::PendingPrompt {
                        prompt_id: "notify:1000".to_string(),
                        kind: "approval".to_string(),
                        status: "pending".to_string(),
                        question: Some("Claude needs your permission".to_string()),
                        transcript_bytes: bytes,
                    }),
                    event_uid: None,
                },
                1000,
            )
            .expect("seed");
        };
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        let count = |conn: &rusqlite::Connection| -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM pending_prompts WHERE thread_id = 'sess-answered'",
                [],
                |row| row.get(0),
            )
            .expect("count")
        };

        // Only injected task text after the boundary: the dialog may still
        // be on screen — the row must survive the sync.
        seed(Some(boundary));
        fs::write(
            &transcript,
            format!(
                "{base}{}
",
                r#"{"type":"user","message":{"role":"user","content":"<task-notification>x</task-notification>"}}"#
            ),
        )
        .expect("task text");
        sync_state_from_sessions(&conn, &config, 5000, 10, false).expect("sync");
        assert_eq!(
            count(&conn),
            1,
            "task text alone must not clear an approval"
        );

        // The main chain continues (every dialog outcome ends here): sync
        // clears the row.
        fs::write(
            &transcript,
            format!(
                "{base}{}
",
                r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"continuing"}]}}"#
            ),
        )
        .expect("assistant continues");
        sync_state_from_sessions(&conn, &config, 6000, 10, false).expect("sync");
        assert_eq!(count(&conn), 0, "an answered dialog's row must be cleared");

        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");
        let _ = fs::remove_dir_all(&projects);
    }

    /// Completion-style notifications are announcements, not waits: none of
    /// them may mint a pending prompt. Only the dialog-ish types do — and
    /// permission_prompt types as approval with the transcript boundary
    /// recorded at hook time riding along.
    #[test]
    fn completion_notifications_do_not_become_waits() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("spool-types");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects = temp.path.join("projects").join("-home-user-x");
        fs::create_dir_all(&projects).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            temp.path.join("projects").display().to_string(),
        );
        let transcript = projects.join("sess-types.jsonl");
        fs::write(
            &transcript,
            r#"{"type":"user","message":{"role":"user","content":"hi"}}
"#,
        )
        .expect("transcript");
        let size = fs::metadata(&transcript).expect("meta").len();

        let spool = |ts: u64, ntype: &str, msg: &str| {
            let mut payload = std::io::Cursor::new(
                json!({
                    "hook_event_name": "Notification",
                    "session_id": "sess-types",
                    "transcript_path": transcript.display().to_string(),
                    "notification_type": ntype,
                    "message": msg
                })
                .to_string(),
            );
            write_hook_event_from_reader(&mut payload, ts).expect("spool");
        };

        // auth_success must vanish without minting a wait.
        spool(1000, "auth_success", "Authentication successful");
        let (snapshots, _, _) = ingest_spool_events(2000).expect("ingest");
        assert!(
            snapshots
                .iter()
                .all(|snapshot| snapshot.pending_prompt.is_none()),
            "a completion notification must not become a pending prompt: {snapshots:?}"
        );

        // permission_prompt DOES, as an approval, carrying the transcript
        // boundary the hook measured.
        spool(
            3000,
            "permission_prompt",
            "Claude needs your permission to use Bash",
        );
        let (snapshots, _, _) = ingest_spool_events(4000).expect("ingest");
        let prompt = snapshots
            .iter()
            .find_map(|snapshot| snapshot.pending_prompt.as_ref())
            .expect("a permission_prompt must become a pending prompt");
        assert_eq!(prompt.kind, "approval");
        assert_eq!(
            prompt.transcript_bytes,
            Some(size),
            "the hook-time transcript boundary must ride along"
        );

        // A wait type invented AFTER this code was written must not be
        // silently dropped: unknown types fall back to the message text.
        spool(
            5000,
            "brand_new_wait_type",
            "Claude needs your permission to use Frobnicator",
        );
        let (snapshots, _, _) = ingest_spool_events(6000).expect("ingest");
        let prompt = snapshots
            .iter()
            .find_map(|snapshot| snapshot.pending_prompt.as_ref())
            .expect("an unknown type must still be treated as a wait");
        assert_eq!(prompt.kind, "approval", "classified by its message text");

        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");
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

        let (snapshots, _, consumed) = ingest_spool_events(2000).expect("ingest");
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

    /// Two turns of one session can both finish before a single daemon poll:
    /// two Stop spool files, one sync, and BOTH away notifications must reach
    /// the outbox with their own previews (no by-session collapse).
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
        set_away_mode(&conn, true, 500).expect("away on");
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
        set_away_mode(&conn, true, 500).expect("away on");
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

    /// P1 regression: a spawned process whose identity write fails must be
    /// TERMINATED and its turn settled — not left running. With `pid` NULL
    /// the daemon's crash check would call the turn failed after 10 seconds,
    /// the turn would stop counting as running, and a live claude process
    /// would keep making tool calls outside the approval boundary.
    #[test]
    fn spawn_that_cannot_be_recorded_is_terminated() {
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
        let _ = test_kill::take();
        if resolve_claude_binary().is_err() {
            return;
        }
        let conn = test_state_conn("persist-fail");
        test_identity_persist::FAIL.store(true, std::sync::atomic::Ordering::SeqCst);
        let result = send_user_message(&conn, &config, "sess-persist", "go", None, 4000);
        test_identity_persist::FAIL.store(false, std::sync::atomic::Ordering::SeqCst);

        assert!(
            result.is_err(),
            "a turn the database cannot track must be reported, not returned as started"
        );
        assert_eq!(
            test_kill::take(),
            vec![0],
            "the spawned child must be terminated (test spawn pid = 0)"
        );
        let status: String = conn
            .query_row("SELECT status FROM bridge_turns", [], |row| row.get(0))
            .expect("turn row");
        assert_eq!(
            status, "failed",
            "the turn must be settled so nothing keeps waiting on it"
        );
    }

    /// The /threads liveness facts: a session with a running turn counts as
    /// headless-active, a settled one does not; a session with no recorded
    /// socket never counts as a live terminal.
    #[test]
    fn liveness_primitives_read_the_actual_state() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let conn = test_state_conn("liveness");
        crate::state::register_bridge_turn(
            &conn,
            "turn-l",
            "sess-l",
            "/tmp/l.log",
            None,
            None,
            None,
            None,
            None,
            None,
            900,
        )
        .expect("register");
        let running = |conn: &rusqlite::Connection| {
            crate::state::list_running_bridge_turns(conn)
                .expect("query")
                .iter()
                .any(|turn| turn.thread_id == "sess-l")
        };
        assert!(running(&conn));
        crate::state::mark_bridge_turn_finished(&conn, "turn-l", "done", 950).expect("finish");
        assert!(!running(&conn));
        assert_eq!(
            session_terminal_presence(&conn, "sess-l").expect("presence"),
            TerminalPresence::Gone,
            "no recorded socket must never read as a live terminal"
        );
    }

    /// The daemon-preemption interleaving: a slow spawn can outlive the 10s
    /// crash grace, in which case the daemon settles the turn as failed
    /// BEFORE the identity write runs. That write must then refuse — updating
    /// the settled row would report a turn the daemon has declared dead (and
    /// notified the user about) as successfully started.
    #[test]
    fn identity_persist_refuses_a_turn_the_daemon_already_settled() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let conn = test_state_conn("preempt");
        crate::state::register_bridge_turn(
            &conn,
            "turn-p",
            "sess-p",
            "/tmp/p.log",
            None,
            None,
            None,
            None,
            None,
            None,
            900,
        )
        .expect("register");
        // The daemon got there first.
        crate::state::mark_bridge_turn_finished(&conn, "turn-p", "failed", 950).expect("settle");

        let err = persist_spawn_identity(&conn, "turn-p", Some(0), &ProcessIdentity::default())
            .expect_err("a settled turn must not be recorded as running");
        assert!(
            format!("{err:#}").contains("no RUNNING row"),
            "the error must name the interleaving: {err:#}"
        );
        // And the settled row itself must be untouched by the attempts.
        let pid: Option<i64> = conn
            .query_row(
                "SELECT pid FROM bridge_turns WHERE turn_id = 'turn-p'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(pid, None, "the failed row must not gain a pid");
    }

    /// When BOTH the identity write and the failure settling break (a truly
    /// broken database), the error the caller gets must carry the settle
    /// failure too — and the contract for the leftover row is explicit: it
    /// stays `running` with a NULL pid, which the daemon's grace-period
    /// check flags on its own.
    #[test]
    fn settle_failure_is_reported_not_swallowed() {
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
        let _ = test_kill::take();
        if resolve_claude_binary().is_err() {
            return;
        }
        let conn = test_state_conn("settle-fail");
        test_identity_persist::FAIL.store(true, std::sync::atomic::Ordering::SeqCst);
        test_settle_fail::FAIL.store(true, std::sync::atomic::Ordering::SeqCst);
        let result = send_user_message(&conn, &config, "sess-settle", "go", None, 4000);
        test_identity_persist::FAIL.store(false, std::sync::atomic::Ordering::SeqCst);
        test_settle_fail::FAIL.store(false, std::sync::atomic::Ordering::SeqCst);

        let err = result.expect_err("both failures must surface as an error");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("settling turn") && chain.contains("also failed"),
            "the settle failure must be in the reported chain, not dropped: {chain}"
        );
        assert_eq!(
            test_kill::take(),
            vec![0],
            "the child must still be terminated"
        );
        let status: String = conn
            .query_row("SELECT status FROM bridge_turns", [], |row| row.get(0))
            .expect("row");
        assert_eq!(
            status, "running",
            "the documented leftover: a running/NULL-pid row for the daemon's grace check"
        );
    }

    /// A throwaway state DB for tests that spawn turns: the spawn registers
    /// the turn itself, so it needs somewhere to write.
    fn test_state_conn(name: &str) -> rusqlite::Connection {
        let path = std::env::temp_dir().join(format!(
            "tinyctb-claude-turn-{name}-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        crate::state::create_state_db(&path).expect("test state db")
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

        let conn = test_state_conn("reply");
        let result = send_user_message(&conn, &config, "sess-reply", "continue please", None, 4000)
            .expect("send reply");
        assert_eq!(result["action"], "reply_started");
        assert_eq!(result["threadId"], "sess-reply");
        // The spawn registered the turn itself — and the row must be a
        // RUNNING one, because the daemon's log-watching and the headless
        // gate's straggler check both key off that status.
        let status: String = conn
            .query_row(
                "SELECT status FROM bridge_turns WHERE thread_id = 'sess-reply'",
                [],
                |row| row.get(0),
            )
            .expect("turn row");
        assert_eq!(
            status, "running",
            "the spawned turn must register as running"
        );

        let spawned = test_spawn::take();
        assert_eq!(spawned.len(), 1);
        let (_, args, _) = &spawned[0];
        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"sess-reply".to_string()));
        assert!(args.contains(&"bypassPermissions".to_string()));
        assert!(args.contains(&"json".to_string()));
    }

    /// Two replies in one Telegram update batch share `now`; turn ids and
    /// log files must still be unique or the answers get interleaved.
    #[test]
    fn concurrent_replies_same_timestamp_get_unique_turn_ids_and_logs() {
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

        let conn = test_state_conn("uid");
        let first =
            send_user_message(&conn, &config, "sess-uid", "one", None, 4000).expect("first reply");
        let second =
            send_user_message(&conn, &config, "sess-uid", "two", None, 4000).expect("second reply");
        let first_turn = first
            .pointer("/claude/turnId")
            .and_then(Value::as_str)
            .expect("first turnId");
        let second_turn = second
            .pointer("/claude/turnId")
            .and_then(Value::as_str)
            .expect("second turnId");
        assert!(first_turn.starts_with("sess-uid-4000-"));
        assert!(second_turn.starts_with("sess-uid-4000-"));
        assert_ne!(first_turn, second_turn, "turn ids must be unique");
        assert_ne!(
            first.pointer("/claude/logPath"),
            second.pointer("/claude/logPath"),
            "each turn must own its log file"
        );
        let _ = test_spawn::take();
    }

    /// The captured identity must be the real resolved executable of the
    /// running process — this is what makes restart-kill verification safe.
    #[test]
    #[cfg(target_os = "linux")]
    fn capture_process_identity_resolves_own_executable() {
        let identity = capture_process_identity(Some(std::process::id()));
        assert!(identity.lstart.is_some(), "lstart must be captured");
        assert!(identity.pgid.is_some(), "pgid must be captured");
        assert!(
            identity.start_ticks.is_some(),
            "starttime ticks must be captured on Linux"
        );
        let exe = identity.exe.expect("exe path must be captured");
        assert!(
            exe.contains("tinyctb"),
            "resolved exe should be this test binary: {exe}"
        );
        assert!(
            std::path::Path::new(&exe).is_absolute(),
            "exe must be an absolute resolved path: {exe}"
        );
    }

    /// A CLAUDE_BIN wrapper that `exec`s the real program changes
    /// /proc/<pid>/exe after our capture; the starttime-ticks identity must
    /// keep recognizing the process so a restarted daemon can still kill it.
    /// The wrapper is held BEFORE its exec by a sync file, so the test
    /// deterministically captures the pre-exec incarnation, releases it, and
    /// observes the exe change.
    #[test]
    #[cfg(target_os = "linux")]
    fn restart_identity_survives_wrapper_exec() {
        use std::os::unix::process::CommandExt;
        let sync_dir =
            std::env::temp_dir().join(format!("tinyctb-exec-sync-{}", std::process::id()));
        let _ = fs::remove_dir_all(&sync_dir);
        fs::create_dir_all(&sync_dir).expect("sync dir");
        let go_file = sync_dir.join("go");
        let mut command = std::process::Command::new("sh");
        command.args([
            "-c",
            "until [ -e \"$0\" ]; do sleep 0.05; done; exec sleep 30",
            &go_file.display().to_string(),
        ]);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = command.spawn().expect("spawn wrapper");
        let pid = child.id();

        // Captured while the wrapper is provably still the shell.
        let identity = capture_process_identity(Some(pid));
        let pre_exec_exe = identity.exe.clone().expect("pre-exec exe");
        assert!(
            !pre_exec_exe.contains("sleep"),
            "capture must happen before exec: {pre_exec_exe}"
        );

        // Release the wrapper and wait for the exec to be observable.
        fs::write(&go_file, b"go").expect("write go file");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match process_exe_path(pid) {
                Some(exe) if exe.contains("sleep") => break,
                _ if std::time::Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("wrapper never exec'd sleep");
                }
                _ => std::thread::sleep(Duration::from_millis(50)),
            }
        }
        // The exe changed but the ticks did not: the incarnation persists.
        assert_eq!(
            process_start_ticks(pid),
            identity.start_ticks,
            "starttime ticks must be exec-invariant"
        );

        let turn = crate::state::BridgeTurn {
            turn_id: "wrapper-test".to_string(),
            thread_id: "wrapper-thread".to_string(),
            log_path: String::new(),
            pid: Some(pid),
            started_at: 0,
            exited: false,
            exit_code: None,
            pgid: identity.pgid,
            proc_start_ticks: identity.start_ticks,
            boot_id: identity.boot_id,
        };
        let recognized = verified_restart_identity(&turn, pid);
        // A record without a boot id must fail closed even for the right pid.
        let mut without_boot = turn.clone();
        without_boot.boot_id = None;
        let recognized_without_boot = verified_restart_identity(&without_boot, pid);
        let _ = child.kill();
        let _ = child.wait();
        let _ = fs::remove_dir_all(&sync_dir);
        assert!(
            recognized,
            "exec inside the wrapper must not break identity (ticks are exec-invariant)"
        );
        assert!(
            !recognized_without_boot,
            "legacy records without boot_id must fail closed"
        );
        // And a different incarnation must fail closed: our own pid has
        // different ticks/pgid than the recorded turn.
        assert!(!verified_restart_identity(&turn, std::process::id()));
    }

    /// Real process-group termination: the main process exits politely on
    /// TERM, a grandchild ignores TERM — the unconditional group KILL must
    /// still sweep it so the whole PGID disappears.
    #[test]
    #[cfg(unix)]
    fn terminate_process_group_sweeps_term_ignoring_grandchildren() {
        use std::os::unix::process::CommandExt;
        let mut command = std::process::Command::new("sh");
        command.args([
            "-c",
            // grandchild ignores TERM; main traps TERM and exits cleanly
            "sh -c 'trap \"\" TERM; sleep 30' & trap 'exit 0' TERM; sleep 30 & wait",
        ]);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = command.spawn().expect("spawn test group");
        let pid = child.id();
        std::thread::sleep(Duration::from_millis(300));
        let group_alive = |pid: u32| {
            std::process::Command::new("kill")
                .args(["-0", "--", &format!("-{pid}")])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
        };
        assert!(group_alive(pid), "test group should be running before kill");

        assert!(
            terminate_process_group(pid, Some(&mut child)),
            "main child should be reaped within the bound"
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while group_alive(pid) {
            assert!(
                std::time::Instant::now() < deadline,
                "process group survived TERM+KILL (a TERM-ignoring grandchild leaked)"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Hooks inherit CLAUDE_CODE_MESSAGING_SOCKET from the session process,
    /// which is how the bridge learns where a live session listens.
    #[test]
    fn hook_event_records_the_sessions_messaging_socket() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("hook-socket-capture");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        std::env::set_var(
            "CLAUDE_CODE_MESSAGING_SOCKET",
            "/run/user/1000/cc-socks/4242.sock",
        );
        let mut payload = std::io::Cursor::new(
            json!({"hook_event_name": "Stop", "session_id": "sess-sock"}).to_string(),
        );
        write_hook_event_from_reader(&mut payload, 1000).expect("spool");
        let (_, sockets, _) = ingest_spool_events(2000).expect("ingest");
        std::env::remove_var("CLAUDE_CODE_MESSAGING_SOCKET");
        std::env::remove_var("TINYCTB_STATE_DIR");

        assert_eq!(
            sockets.get("sess-sock").map(|s| s.path.as_str()),
            Some("/run/user/1000/cc-socks/4242.sock")
        );
    }

    /// End-to-end over a real unix socket: the injected line must arrive as
    /// the exact JSON shape Claude Code accepts for message injection.
    #[test]
    #[cfg(target_os = "linux")]
    fn injection_writes_one_json_line_to_a_live_socket() {
        use std::io::{BufRead, BufReader};
        use std::os::unix::net::UnixListener;

        let dir = std::env::temp_dir().join(format!("tinyctb-uds-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("socket dir");
        // Named after a live pid, like cc-socks/<pid>.sock, so the owning
        // process identity is derivable (unnamed paths fail closed).
        let path = dir.join(format!("{}.sock", std::process::id()));
        let listener = UnixListener::bind(&path).expect("bind");
        let accepted = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line).expect("read");
            line
        });

        let socket_path = path.display().to_string();
        let delivered =
            inject_into_live_session(&socket_path, socket_identity(&socket_path), "telegram：hi")
                .expect("inject");
        assert!(delivered, "a live socket must accept the message");

        let line = accepted.join().expect("join");
        let parsed: Value = serde_json::from_str(line.trim()).expect("valid json line");
        assert_eq!(parsed["type"], "user");
        assert_eq!(parsed["message"]["role"], "user");
        assert_eq!(parsed["message"]["content"], "telegram：hi");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A missing or stale socket is not an error: the caller falls back to a
    /// headless resume, which is correct for an idle or closed session.
    #[test]
    fn injection_reports_not_delivered_for_missing_or_stale_socket() {
        assert!(!inject_into_live_session(
            "/nonexistent/tinyctb.sock",
            (Some(1), Some("boot".into())),
            "hi"
        )
        .expect("missing"));

        let dir = std::env::temp_dir().join(format!("tinyctb-uds-stale-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let stale = dir.join("stale.sock");
        fs::write(&stale, b"not a socket").expect("write stale file");
        assert!(
            !inject_into_live_session(
                &stale.display().to_string(),
                socket_identity(&stale.display().to_string()),
                "hi"
            )
            .expect("stale"),
            "a leftover socket file must not be treated as a live session"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The socket path is `<pid>.sock`, so after the owning session exits the
    /// path can be rebound by a different session that reused the pid.
    /// Delivering there would put the user's message into the WRONG session,
    /// so identity is checked against the owning process, not the file.
    /// (An inode check alone is not enough: tmpfs reuses inode numbers
    /// immediately after unlink+rebind — asserted below.)
    #[test]
    #[cfg(target_os = "linux")]
    fn injection_refuses_when_the_owning_session_is_gone() {
        use std::os::unix::net::UnixListener;

        let dir = std::env::temp_dir().join(format!("tinyctb-uds-rebind-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");

        // A helper process stands in for the owning Claude session; the
        // socket is named after its pid, exactly like cc-socks/<pid>.sock.
        let mut owner = std::process::Command::new("sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn owner");
        let path = dir.join(format!("{}.sock", owner.id()));
        let socket_path = path.display().to_string();
        let listener = UnixListener::bind(&path).expect("bind");
        let recorded = socket_identity(&socket_path);
        assert!(
            recorded.1.is_some(),
            "the owning process identity must be captured"
        );
        // While the owner lives, delivery is allowed.
        assert!(inject_into_live_session(&socket_path, recorded.clone(), "hi").expect("live"));

        // Owner exits; the socket file survives and a new listener rebinds it.
        owner.kill().ok();
        owner.wait().ok();
        drop(listener);
        fs::remove_file(&path).expect("unlink");
        // NB: the inode may or may not be recycled here (observed both ways
        // on tmpfs), which is exactly why identity is anchored to the owning
        // process rather than to the socket file.
        let _rebound = UnixListener::bind(&path).expect("rebind");

        assert!(
            !inject_into_live_session(&socket_path, recorded, "hi").expect("rebound"),
            "must refuse once the session that reported this socket is gone"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// P1 regression: an answer owed to an injected message must reach
    /// Telegram even with away off — the user asked from their phone.
    #[test]
    fn injected_reply_answer_is_pushed_even_when_not_away() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("injected-answer-push");
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
        // Away is OFF; a Telegram message was injected into the live session.
        crate::state::record_live_injection(&conn, "sess-inj", 500).expect("record injection");
        let mut stop = std::io::Cursor::new(
            json!({
                "hook_event_name": "Stop",
                "session_id": "sess-inj",
                "cwd": "/home/user/project",
                "last_assistant_message": "answered from the live session"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut stop, 1000).expect("spool");

        let result = sync_state_from_sessions(&conn, &config, 2000, 50, true).expect("sync");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        assert_eq!(result["enqueued"], 1, "{result}");
        let origin: String = conn
            .query_row("SELECT origin FROM outbound_events", [], |row| row.get(0))
            .expect("outbound row");
        assert_eq!(origin, "bridge", "owed answers survive /back");
        assert!(
            !crate::state::live_injection_pending(&conn, "sess-inj", Some(1000), 2500)
                .expect("pending"),
            "the answer consumed the owed record"
        );
    }

    /// A socket path with no pid in its name (custom --messaging-socket-path)
    /// cannot be tied to an owning process, so injection must refuse rather
    /// than trust an inode that tmpfs may hand back after a rebind.
    #[test]
    #[cfg(unix)]
    fn injection_fails_closed_when_the_owner_cannot_be_identified() {
        use std::os::unix::net::UnixListener;

        let dir = std::env::temp_dir().join(format!("tinyctb-uds-anon-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("custom-name.sock");
        let socket_path = path.display().to_string();
        let _listener = UnixListener::bind(&path).expect("bind");

        let identity = socket_identity(&socket_path);
        assert!(identity.0.is_some(), "the inode is still observable");
        assert!(
            identity.1.is_none(),
            "an unidentifiable owner must produce no usable identity"
        );
        assert!(
            !inject_into_live_session(&socket_path, identity, "hi").expect("anon"),
            "must not inject into a socket whose owning session cannot be proven"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// P1 regression: a Stop that happened BEFORE the injection must not be
    /// pushed as its answer (and must not burn the debt) — only a completion
    /// that postdates the injection can settle it.
    #[test]
    fn older_completion_cannot_claim_a_later_injection() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("injection-ordering");
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
        // A Stop was already spooled BEFORE the Telegram message arrived.
        let mut stale = std::io::Cursor::new(
            json!({
                "hook_event_name": "Stop",
                "session_id": "sess-order",
                "last_assistant_message": "answer to something else"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut stale, 1000).expect("spool stale");
        // Then the reply is injected into the live session (away is OFF).
        crate::state::record_live_injection(&conn, "sess-order", 2000).expect("inject record");

        let first = sync_state_from_sessions(&conn, &config, 2500, 50, true).expect("sync 1");
        assert_eq!(
            first["enqueued"], 0,
            "a pre-injection Stop must not be pushed as the reply: {first}"
        );
        assert!(
            crate::state::live_injection_pending(&conn, "sess-order", Some(3000), 2500)
                .expect("pending"),
            "the debt must survive an older completion"
        );

        // The real answer arrives after the injection.
        let mut real = std::io::Cursor::new(
            json!({
                "hook_event_name": "Stop",
                "session_id": "sess-order",
                "last_assistant_message": "the actual reply"
            })
            .to_string(),
        );
        write_hook_event_from_reader(&mut real, 3000).expect("spool real");
        let second = sync_state_from_sessions(&conn, &config, 3500, 50, true).expect("sync 2");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        assert_eq!(second["enqueued"], 1, "{second}");
        let payload: String = conn
            .query_row(
                "SELECT payload_json FROM outbound_events WHERE origin = 'bridge'",
                [],
                |row| row.get(0),
            )
            .expect("bridge row");
        assert!(payload.contains("the actual reply"), "payload: {payload}");
        assert!(
            !crate::state::live_injection_pending(&conn, "sess-order", Some(4000), 3500)
                .expect("settled"),
            "the post-injection answer settles the debt"
        );
    }

    /// P1 regression: a reply arriving in the SAME daemon cycle as the
    /// session's first hook event must already find the socket mapping,
    /// otherwise it forks the session instead of being injected.
    #[test]
    fn socket_mapping_is_available_before_the_spool_is_consumed() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let temp = TempDirGuard::new("socket-peek-order");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        std::env::set_var(
            "CLAUDE_CODE_MESSAGING_SOCKET",
            "/run/user/1000/cc-socks/77.sock",
        );
        let conn = create_state_db_in_memory().expect("db");
        let mut payload = std::io::Cursor::new(
            json!({"hook_event_name": "SessionStart", "session_id": "sess-peek"}).to_string(),
        );
        write_hook_event_from_reader(&mut payload, 1000).expect("spool");
        std::env::remove_var("CLAUDE_CODE_MESSAGING_SOCKET");

        // Before any sync: the peek the daemon runs first must already know.
        assert_eq!(peek_session_sockets(&conn, 1500).expect("peek"), 1);
        let socket = crate::state::session_messaging_socket(&conn, "sess-peek")
            .expect("lookup")
            .expect("socket known before spool consumption");
        assert_eq!(socket.path, "/run/user/1000/cc-socks/77.sock");

        // Peek is non-destructive: the sync still gets the event.
        let (snapshots, _, consumed) = ingest_spool_events(2000).expect("ingest");
        std::env::remove_var("TINYCTB_STATE_DIR");
        assert_eq!(consumed, 1, "peek must not consume the spool");
        assert_eq!(snapshots.len(), 1);
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

        let conn = test_state_conn("new");
        let result =
            start_thread_in_cwd(&conn, &config, Some("/tmp"), Some("build the thing"), 5000)
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
