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

    if let Ok(override_path) = env::var("CLAUDE_BIN") {
        push_candidate(
            &mut candidates,
            &mut seen,
            PathBuf::from(override_path),
            "override",
        );
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
    sessions.sort_by(|a, b| b.mtime_ms.cmp(&a.mtime_ms));
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
                if let Some(text) = record
                    .get("message")
                    .and_then(|message| message.get("content"))
                    .and_then(text_from_message_content)
                {
                    summary.last_assistant_text = Some(truncate_chars(&text, MAX_PREVIEW_CHARS));
                }
                summary.last_record_type = Some("assistant".to_string());
            }
            "user" => {
                if record.get("isMeta").and_then(Value::as_bool) == Some(true) {
                    continue;
                }
                if let Some(cwd) = record.get("cwd").and_then(Value::as_str) {
                    summary.cwd = Some(cwd.to_string());
                }
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
    let file_name = format!("{now:015}-{}-{}.json", std::process::id(), &event_name);
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

fn pending_prompt_from_notification(payload: &Value, received_at: u64) -> PendingPrompt {
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
    PendingPrompt {
        prompt_id: format!("notify:{received_at}"),
        kind: kind.to_string(),
        status: "pending".to_string(),
        question: message,
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
    let mut by_session: BTreeMap<String, BridgeThreadSnapshot> = BTreeMap::new();
    for path in files {
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

        let base = by_session.remove(&session_id);
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
                last_preview: summary.last_assistant_text,
                pending_prompt: None,
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
                pending_prompt: Some(pending_prompt_from_notification(&payload, received_at)),
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
    Ok((by_session.into_values().collect(), consumed))
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

    Ok(json!({
        "synced": threads.len(),
        "threads": threads,
        "events": reconcile.get("events").cloned().unwrap_or_else(|| json!([])),
        "away": reconcile.get("away").cloned().unwrap_or(Value::Bool(false)),
        "spoolConsumed": consumed
    }))
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

pub(crate) fn watch_thread_error_event(error: &anyhow::Error) -> Value {
    json!({
        "type": "thread_error",
        "message": error.to_string()
    })
}

fn enrich_event_with_thread(event: Value, threads: &[Value]) -> Value {
    let Some(thread_id) = event.get("threadId").and_then(Value::as_str) else {
        return event;
    };
    let Some(thread) = threads
        .iter()
        .find(|thread| thread.get("threadId").and_then(Value::as_str) == Some(thread_id))
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

#[derive(Debug, Clone)]
pub(crate) struct ClaudeTransportInfo {
    pub(crate) transport: &'static str,
    pub(crate) pid: Option<u32>,
}

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

    pub(crate) static RECORDED: Mutex<Vec<(String, Vec<String>, Option<String>)>> =
        Mutex::new(Vec::new());

    pub(crate) fn take() -> Vec<(String, Vec<String>, Option<String>)> {
        std::mem::take(&mut RECORDED.lock().expect("test spawn lock"))
    }
}

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

pub(crate) fn transport_info(pid: Option<u32>) -> ClaudeTransportInfo {
    ClaudeTransportInfo {
        transport: "headless-cli",
        pid,
    }
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
    fn headless_reply_spawns_resume_with_permission_mode() {
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
