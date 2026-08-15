use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::projects::{canonicalize_project_cwd, derive_project_label};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PendingPrompt {
    pub(crate) prompt_id: String,
    pub(crate) kind: String,
    pub(crate) status: String,
    pub(crate) question: Option<String>,
    /// Transcript size (bytes) at the moment the notification FIRED,
    /// recorded by the hook itself. Everything after this boundary is what
    /// happened since the prompt appeared — the evidence for deciding it
    /// was dealt with. `None` on rows from before this column existed.
    pub(crate) transcript_bytes: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TelegramInboundLogContext<'a> {
    pub(crate) thread_id: Option<&'a str>,
    pub(crate) route_message_id: Option<i64>,
    pub(crate) result_action: Option<&'a str>,
    pub(crate) backend_transport: Option<&'a str>,
    pub(crate) backend_pid: Option<u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct BridgeThreadSnapshot {
    pub(crate) thread_id: String,
    pub(crate) name: Option<String>,
    pub(crate) cwd: Option<String>,
    pub(crate) updated_at: Option<u64>,
    pub(crate) status_type: String,
    pub(crate) status_flags: Vec<String>,
    pub(crate) last_turn_status: Option<String>,
    pub(crate) last_preview: Option<String>,
    pub(crate) pending_prompt: Option<PendingPrompt>,
    /// Unique id of the hook spool file this snapshot came from (None for
    /// transcript scans and synthetic snapshots). Event keys use it so two
    /// hooks that fire in the same millisecond stay distinct events.
    pub(crate) event_uid: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WaitingThread {
    #[serde(rename = "threadId")]
    pub(crate) thread_id: String,
    pub(crate) name: Option<String>,
    #[serde(rename = "displayName")]
    pub(crate) display_name: String,
    pub(crate) project: Option<String>,
    pub(crate) cwd: Option<String>,
    #[serde(rename = "updatedAt")]
    pub(crate) updated_at: Option<u64>,
    #[serde(rename = "statusType")]
    pub(crate) status_type: String,
    #[serde(rename = "statusFlags")]
    pub(crate) status_flags: Vec<String>,
    pub(crate) prompt: PendingPrompt,
    #[serde(rename = "lastPreview")]
    pub(crate) last_preview: Option<String>,
    pub(crate) label: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WaitingSummary {
    pub(crate) count: usize,
    #[serde(rename = "threadIds")]
    pub(crate) thread_ids: Vec<String>,
    pub(crate) labels: Vec<String>,
    #[serde(rename = "appliedFilters")]
    pub(crate) applied_filters: Value,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WaitingResult {
    pub(crate) summary: WaitingSummary,
    pub(crate) threads: Vec<WaitingThread>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct InboxItem {
    #[serde(rename = "threadId")]
    pub(crate) thread_id: String,
    pub(crate) name: Option<String>,
    #[serde(rename = "displayName")]
    pub(crate) display_name: String,
    pub(crate) project: Option<String>,
    pub(crate) cwd: Option<String>,
    #[serde(rename = "updatedAt")]
    pub(crate) updated_at: Option<u64>,
    #[serde(rename = "lastSeenAt")]
    pub(crate) last_seen_at: Option<u64>,
    #[serde(rename = "ageSeconds")]
    pub(crate) age_seconds: Option<u64>,
    #[serde(rename = "statusType")]
    pub(crate) status_type: String,
    #[serde(rename = "statusFlags")]
    pub(crate) status_flags: Vec<String>,
    #[serde(rename = "lastPreview")]
    pub(crate) last_preview: Option<String>,
    #[serde(rename = "promptKind")]
    pub(crate) prompt_kind: Option<String>,
    #[serde(rename = "promptStatus")]
    pub(crate) prompt_status: Option<String>,
    pub(crate) question: Option<String>,
    pub(crate) basis: String,
    #[serde(rename = "attentionReason")]
    pub(crate) attention_reason: String,
    #[serde(rename = "waitingOn")]
    pub(crate) waiting_on: String,
    #[serde(rename = "suggestedAction")]
    pub(crate) suggested_action: String,
    pub(crate) priority: String,
    #[serde(rename = "recentAction")]
    pub(crate) recent_action: Option<Value>,
    pub(crate) label: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct InboxSummary {
    pub(crate) total: usize,
    #[serde(rename = "needsAttention")]
    pub(crate) needs_attention: usize,
    #[serde(rename = "countsByReason")]
    pub(crate) counts_by_reason: Value,
    #[serde(rename = "appliedFilters")]
    pub(crate) applied_filters: Value,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct InboxResult {
    pub(crate) summary: InboxSummary,
    pub(crate) items: Vec<InboxItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedWorkspace {
    pub(crate) cwd: String,
    pub(crate) label: String,
    pub(crate) last_seen_at: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TelegramCommandRouteKind {
    NewThread,
}

impl TelegramCommandRouteKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NewThread => "new_thread",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "new_thread" => Some(Self::NewThread),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TelegramCallbackRoute {
    pub(crate) callback_id: String,
    pub(crate) chat_id: String,
    pub(crate) message_id: Option<i64>,
    pub(crate) thread_id: String,
    pub(crate) action: TelegramCallbackAction,
    pub(crate) approval_id: Option<String>,
    /// Set for question buttons: which pending question this answers, and
    /// the literal option text the button stands for.
    pub(crate) question_id: Option<String>,
    pub(crate) answer: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TelegramCallbackAction {
    /// One option of a question the session is blocked on.
    AnswerQuestion,
    Approve,
    /// Approve and stop asking for this tool in this session.
    ApproveSession,
    Deny,
}

impl TelegramCallbackAction {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AnswerQuestion => "answer_question",
            Self::Approve => "approve",
            Self::ApproveSession => "approve_session",
            Self::Deny => "deny",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "answer_question" => Some(Self::AnswerQuestion),
            "approve" => Some(Self::Approve),
            "approve_session" => Some(Self::ApproveSession),
            "deny" => Some(Self::Deny),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OutboxDeliverySummary {
    pub(crate) attempted: usize,
    pub(crate) delivered: usize,
    pub(crate) failed: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct HistoryAction {
    pub(crate) action_type: String,
    pub(crate) payload: Value,
    pub(crate) created_at: u64,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ArchiveSelection {
    pub(crate) targets: Vec<String>,
    pub(crate) using_filter_selection: bool,
}

fn timestamp_to_millis(value: u64) -> u64 {
    const UNIX_TIMESTAMP_MILLIS_THRESHOLD: u64 = 100_000_000_000;
    if value < UNIX_TIMESTAMP_MILLIS_THRESHOLD {
        value.saturating_mul(1000)
    } else {
        value
    }
}

fn timestamp_age_seconds(now: u64, then: u64) -> u64 {
    timestamp_to_millis(now).saturating_sub(timestamp_to_millis(then)) / 1000
}

fn compact_text_preview(input: Option<String>, limit: usize) -> Option<String> {
    let text = input?.trim().replace(char::is_whitespace, " ");
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return None;
    }
    let count = normalized.chars().count();
    if count <= limit {
        return Some(normalized);
    }
    let truncated = normalized.chars().take(limit).collect::<String>();
    Some(format!("{}…", truncated.trim_end()))
}

pub(crate) fn derive_thread_display_name(
    name: Option<&str>,
    project: Option<&str>,
    question: Option<&str>,
    thread_id: &str,
) -> String {
    if let Some(value) = name.map(str::trim).filter(|value| !value.is_empty()) {
        return value.to_string();
    }
    if let Some(value) = question.map(str::trim).filter(|value| !value.is_empty()) {
        return compact_text_preview(Some(value.to_string()), 80)
            .unwrap_or_else(|| value.to_string());
    }
    if let Some(project) = project.filter(|value| !value.is_empty()) {
        return format!("Untitled {project} thread");
    }
    let short_id = thread_id.chars().take(8).collect::<String>();
    format!("Untitled thread {short_id}")
}

fn classify_attention(snapshot: &BridgeThreadSnapshot) -> (&'static str, &'static str) {
    match snapshot
        .pending_prompt
        .as_ref()
        .map(|prompt| prompt.kind.as_str())
    {
        Some("approval") => ("pending_approval", "prompt_kind"),
        Some("reply") => ("needs_reply", "prompt_kind"),
        _ if snapshot.last_turn_status.as_deref() == Some("completed") => {
            ("completed", "last_turn_completed")
        }
        _ if snapshot.last_turn_status.as_deref() == Some("interrupted")
            && snapshot
                .last_preview
                .as_deref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false) =>
        {
            ("updated", "last_turn_interrupted")
        }
        _ => ("active", "fallback_active"),
    }
}

fn classify_waiting_on(reason: &str) -> &'static str {
    match reason {
        "pending_approval" | "needs_reply" => "me",
        "active" => "claude",
        "updated" => "none",
        _ => "none",
    }
}

fn classify_suggested_action(reason: &str) -> &'static str {
    match reason {
        "pending_approval" => "approve",
        "needs_reply" => "reply",
        "completed" => "archive",
        "updated" => "inspect",
        _ => "inspect",
    }
}

fn classify_priority(reason: &str) -> &'static str {
    match reason {
        "pending_approval" => "high",
        "needs_reply" | "active" | "updated" => "medium",
        _ => "low",
    }
}

pub(crate) fn score_inbox_item(item: &InboxItem) -> u128 {
    let priority_score = match item.priority.as_str() {
        "high" => 300_u128,
        "medium" => 200_u128,
        _ => 100_u128,
    };
    priority_score * 1_000_000_000_000 + item.updated_at.unwrap_or(0) as u128
}

pub(crate) fn classify_inbox_item(snapshot: &BridgeThreadSnapshot, now: u64) -> InboxItem {
    let (attention_reason, basis) = classify_attention(snapshot);
    let project = derive_project_label(snapshot.cwd.as_deref());
    let display_name = derive_thread_display_name(
        snapshot.name.as_deref(),
        project.as_deref(),
        snapshot
            .pending_prompt
            .as_ref()
            .and_then(|prompt| prompt.question.as_deref()),
        &snapshot.thread_id,
    );
    InboxItem {
        thread_id: snapshot.thread_id.clone(),
        name: snapshot.name.clone(),
        display_name: display_name.clone(),
        project: project.clone(),
        cwd: snapshot.cwd.clone(),
        updated_at: snapshot.updated_at,
        last_seen_at: None,
        age_seconds: snapshot
            .updated_at
            .map(|updated| timestamp_age_seconds(now, updated)),
        status_type: snapshot.status_type.clone(),
        status_flags: snapshot.status_flags.clone(),
        last_preview: compact_text_preview(snapshot.last_preview.clone(), 220),
        prompt_kind: snapshot
            .pending_prompt
            .as_ref()
            .map(|prompt| prompt.kind.clone()),
        prompt_status: snapshot
            .pending_prompt
            .as_ref()
            .map(|prompt| prompt.status.clone()),
        question: compact_text_preview(
            snapshot
                .pending_prompt
                .as_ref()
                .and_then(|prompt| prompt.question.clone()),
            160,
        ),
        basis: basis.to_string(),
        attention_reason: attention_reason.to_string(),
        waiting_on: classify_waiting_on(attention_reason).to_string(),
        suggested_action: classify_suggested_action(attention_reason).to_string(),
        priority: classify_priority(attention_reason).to_string(),
        recent_action: None,
        label: format!(
            "{} · {}",
            display_name,
            project
                .clone()
                .or_else(|| snapshot.cwd.clone())
                .unwrap_or_else(|| "unknown cwd".to_string())
        ),
    }
}

pub(crate) fn observed_workspaces_from_db(
    conn: &Connection,
    limit: u64,
) -> Result<Vec<ObservedWorkspace>> {
    let mut stmt = conn.prepare(
        "SELECT cwd, MAX(COALESCE(updated_at, last_seen_at, 0)) AS seen_at
         FROM threads_cache
         WHERE cwd IS NOT NULL AND TRIM(cwd) != ''
         GROUP BY cwd
         ORDER BY seen_at DESC
         LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![to_sql_i64(limit)?], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|(cwd, seen_at)| {
            let cwd = canonicalize_project_cwd(&cwd)?;
            Ok(ObservedWorkspace {
                label: derive_project_label(Some(&cwd)).unwrap_or_else(|| cwd.clone()),
                cwd,
                last_seen_at: optional_from_sql_i64(seen_at)?,
            })
        })
        .collect()
}

fn to_sql_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("timestamp out of range for sqlite i64")
}

fn from_sql_i64(value: i64) -> Result<u64> {
    u64::try_from(value).context("negative sqlite integer cannot be converted to u64")
}

fn optional_from_sql_i64(value: Option<i64>) -> Result<Option<u64>> {
    value.map(from_sql_i64).transpose()
}

pub(crate) fn create_state_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(5))
        .context("failed to set SQLite busy timeout")?;
    // Switching to WAL needs a brief exclusive lock, so a second process
    // opening the database at the same moment (the approval hook and the
    // daemon do exactly that) gets SQLITE_BUSY. The mode is persistent, so
    // only set it when it is not already WAL, and treat losing the race as
    // success — whoever won set the very mode we wanted.
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap_or_default();
    if !journal_mode.eq_ignore_ascii_case("wal") {
        // Losing this race is harmless: the database works in either journal
        // mode, and the connection that won has already set the one we want.
        // Failing here instead would break the approval hook whenever it
        // happens to start at the same instant as the daemon.
        if let Err(error) = conn.execute_batch("PRAGMA journal_mode=WAL;") {
            eprintln!("tinyctb: could not switch SQLite to WAL ({error}); continuing");
        }
    }
    conn.execute_batch("PRAGMA synchronous=NORMAL;")
        .context("failed to configure SQLite synchronous mode")?;
    init_state_db(&conn)?;
    Ok(conn)
}

pub(crate) fn prune_state_logs(conn: &Connection, now: u64) -> Result<usize> {
    let retention_ms: u64 = 30 * 24 * 60 * 60 * 1000;
    let cutoff = now.saturating_sub(retention_ms);
    let sql_cutoff = to_sql_i64(cutoff)?;
    let inbound = conn.execute(
        "DELETE FROM telegram_inbound_log WHERE processed_at < ?1",
        params![sql_cutoff],
    )?;
    let actions = conn.execute(
        "DELETE FROM actions_log WHERE created_at < ?1",
        params![sql_cutoff],
    )?;
    let injections = prune_live_injections(conn, now, retention_ms)?;
    // Dialog message ids are only useful while a reply to that dialog is
    // plausible; without pruning the table grows for the life of the install.
    let dialogs = conn.execute(
        "DELETE FROM dialog_messages WHERE created_at < ?1",
        params![sql_cutoff],
    )?;
    // Settled turns are history; running rows are load-bearing (liveness,
    // crash detection) and are never pruned regardless of age.
    let turns = conn.execute(
        "DELETE FROM bridge_turns
         WHERE status != 'running' AND COALESCE(completed_at, started_at) < ?1",
        params![sql_cutoff],
    )?;
    // Settled or long-expired prompts are history too — /threads scans these
    // tables on every call. The not-open guard is redundant for rows a whole
    // retention period old (their windows are minutes, not weeks) but keeps
    // the invariant provable: an OPEN prompt is never pruned, whatever its
    // age says.
    let approvals = conn.execute(
        "DELETE FROM pending_approvals
         WHERE created_at < ?1 AND (decision IS NOT NULL OR expires_at < ?1)",
        params![sql_cutoff],
    )?;
    let questions = conn.execute(
        "DELETE FROM pending_questions
         WHERE created_at < ?1 AND (answer IS NOT NULL OR expires_at < ?1)",
        params![sql_cutoff],
    )?;
    Ok(inbound + actions + injections + dialogs + turns + approvals + questions)
}

/// Injection debts are short-lived by design: settled ones are history and
/// unclaimed ones expire with the TTL. Without pruning the table grows for
/// the life of the install and every lookup scans more rows.
///
/// Ages are computed through `timestamp_to_millis` on BOTH sides, exactly
/// like claiming does — comparing a millisecond cutoff against a raw value
/// would delete brand-new second-granularity rows as if they were ancient.
fn prune_live_injections(conn: &Connection, now: u64, retention_ms: u64) -> Result<usize> {
    let now_ms = timestamp_to_millis(now);
    let rows: Vec<(i64, i64, Option<i64>)> = {
        let mut stmt = conn.prepare("SELECT id, injected_at, claimed_at FROM live_injections")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut removed = 0usize;
    for (id, injected_at, claimed_at) in rows {
        let expired = match claimed_at {
            Some(claimed_at) => {
                now_ms.saturating_sub(timestamp_to_millis(from_sql_i64(claimed_at)?)) > retention_ms
            }
            None => {
                now_ms.saturating_sub(timestamp_to_millis(from_sql_i64(injected_at)?))
                    > LIVE_INJECTION_TTL_MS
            }
        };
        if expired {
            removed += conn.execute("DELETE FROM live_injections WHERE id = ?1", params![id])?;
        }
    }
    Ok(removed)
}

#[cfg(test)]
pub(crate) fn create_state_db_in_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    init_state_db(&conn)?;
    Ok(conn)
}

pub(crate) fn init_state_db(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS threads_cache (
          thread_id TEXT PRIMARY KEY,
          name TEXT,
          cwd TEXT,
          source TEXT,
          status_type TEXT NOT NULL,
          status_flags_json TEXT NOT NULL,
          updated_at INTEGER,
          last_seen_at INTEGER NOT NULL,
          last_turn_status TEXT,
          last_preview TEXT,
          messaging_socket TEXT,
          socket_inode INTEGER,
          socket_boot_id TEXT
        );
        CREATE TABLE IF NOT EXISTS pending_prompts (
          thread_id TEXT PRIMARY KEY,
          prompt_id TEXT NOT NULL,
          prompt_kind TEXT NOT NULL,
          prompt_status TEXT NOT NULL,
          question TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS delivery_log (
          event_key TEXT PRIMARY KEY,
          thread_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          delivered_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS thread_events (
          event_key TEXT PRIMARY KEY,
          thread_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          observed_at INTEGER NOT NULL,
          payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS outbound_events (
          event_id TEXT PRIMARY KEY,
          event_type TEXT NOT NULL,
          thread_id TEXT,
          payload_json TEXT NOT NULL,
          status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          next_attempt_at INTEGER NOT NULL,
          last_error TEXT,
          created_at INTEGER NOT NULL,
          delivered_at INTEGER,
          origin TEXT NOT NULL DEFAULT 'away'
        );
        CREATE TABLE IF NOT EXISTS telegram_message_routes (
          chat_id TEXT NOT NULL,
          message_id INTEGER NOT NULL,
          thread_id TEXT NOT NULL,
          event_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          PRIMARY KEY(chat_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS telegram_callback_routes (
          callback_id TEXT PRIMARY KEY,
          chat_id TEXT NOT NULL,
          message_id INTEGER,
          thread_id TEXT NOT NULL,
          action TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          used_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS telegram_command_routes (
          chat_id TEXT NOT NULL,
          message_id INTEGER NOT NULL,
          command TEXT NOT NULL,
          payload_json TEXT,
          created_at INTEGER NOT NULL,
          used_at INTEGER,
          PRIMARY KEY(chat_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS transport_delivery_log (
          event_id TEXT NOT NULL,
          transport TEXT NOT NULL,
          result_json TEXT NOT NULL,
          delivered_at INTEGER NOT NULL,
          PRIMARY KEY(event_id, transport)
        );
        CREATE TABLE IF NOT EXISTS telegram_inbound_log (
          bot_id TEXT NOT NULL,
          update_id INTEGER NOT NULL,
          update_kind TEXT NOT NULL,
          result_json TEXT NOT NULL,
          processed_at INTEGER NOT NULL,
          PRIMARY KEY(bot_id, update_id)
        );
        CREATE TABLE IF NOT EXISTS actions_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          thread_id TEXT NOT NULL,
          action_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pending_approvals (
          approval_id TEXT PRIMARY KEY,
          thread_id TEXT NOT NULL,
          tool_name TEXT NOT NULL,
          summary TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          decision TEXT,
          decided_at INTEGER,
          expires_at INTEGER NOT NULL DEFAULT 0,
          message_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS pending_questions (
          question_id TEXT PRIMARY KEY,
          thread_id TEXT NOT NULL,
          question TEXT NOT NULL,
          options_json TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          expires_at INTEGER NOT NULL,
          message_id INTEGER,
          answer TEXT,
          answered_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS dialog_messages (
          chat_id TEXT NOT NULL,
          message_id INTEGER NOT NULL,
          kind TEXT NOT NULL,
          ref_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          PRIMARY KEY(chat_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS live_injections (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          thread_id TEXT NOT NULL,
          injected_at INTEGER NOT NULL,
          claimed_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS bridge_turns (
          turn_id TEXT PRIMARY KEY,
          thread_id TEXT NOT NULL,
          log_path TEXT NOT NULL,
          pid INTEGER,
          started_at INTEGER NOT NULL,
          status TEXT NOT NULL,
          completed_at INTEGER,
          exited INTEGER NOT NULL DEFAULT 0,
          exit_code INTEGER,
          proc_start TEXT,
          proc_exe TEXT,
          pgid INTEGER,
          proc_start_ticks TEXT,
          boot_id TEXT
        );
        ",
    )?;
    ensure_column(
        conn,
        "threads_cache",
        "last_seen_at",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(conn, "threads_cache", "last_turn_status", "TEXT")?;
    ensure_column(conn, "threads_cache", "last_preview", "TEXT")?;
    ensure_column(conn, "threads_cache", "messaging_socket", "TEXT")?;
    ensure_column(conn, "threads_cache", "socket_inode", "INTEGER")?;
    ensure_column(conn, "threads_cache", "socket_boot_id", "TEXT")?;
    ensure_column(conn, "telegram_command_routes", "payload_json", "TEXT")?;
    ensure_column(conn, "telegram_callback_routes", "approval_id", "TEXT")?;
    ensure_column(conn, "pending_questions", "multi_select", "INTEGER")?;
    ensure_column(conn, "pending_prompts", "transcript_bytes", "INTEGER")?;
    ensure_column(conn, "pending_approvals", "headless", "INTEGER")?;
    ensure_column(conn, "telegram_callback_routes", "question_id", "TEXT")?;
    ensure_column(conn, "telegram_callback_routes", "answer", "TEXT")?;
    ensure_column(
        conn,
        "pending_approvals",
        "expires_at",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(conn, "pending_approvals", "message_id", "INTEGER")?;
    ensure_column(
        conn,
        "outbound_events",
        "origin",
        "TEXT NOT NULL DEFAULT 'away'",
    )?;
    ensure_column(conn, "bridge_turns", "exited", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column(conn, "bridge_turns", "exit_code", "INTEGER")?;
    migrate_live_injections(conn)?;
    ensure_column(conn, "bridge_turns", "proc_start", "TEXT")?;
    ensure_column(conn, "bridge_turns", "proc_exe", "TEXT")?;
    ensure_column(conn, "bridge_turns", "pgid", "INTEGER")?;
    ensure_column(conn, "bridge_turns", "proc_start_ticks", "TEXT")?;
    ensure_column(conn, "bridge_turns", "boot_id", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "thread_id", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "route_message_id", "INTEGER")?;
    ensure_column(conn, "telegram_inbound_log", "result_action", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "backend_transport", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "backend_pid", "INTEGER")?;
    // Partial indexes for the /threads open-prompt scan: only unsettled rows
    // are indexed, so the index stays tiny however much settled history
    // accumulates (and pruning keeps that bounded too).
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_pending_questions_open
             ON pending_questions(expires_at, created_at) WHERE answer IS NULL;
         CREATE INDEX IF NOT EXISTS idx_pending_approvals_open
             ON pending_approvals(expires_at, created_at) WHERE decision IS NULL;",
    )?;
    Ok(())
}

/// The first shipped shape of `live_injections` counted owed answers per
/// thread (`thread_id/injected_at/owed`); accounting is now per injection so
/// an older completion cannot claim a newer one. `CREATE TABLE IF NOT
/// EXISTS` leaves an already-deployed table untouched, so rebuild it here —
/// otherwise every query fails with "no such column" on an upgraded install.
fn migrate_live_injections(conn: &Connection) -> Result<()> {
    let columns = table_columns(conn, "live_injections")?;
    if columns.is_empty() || columns.iter().any(|column| column == "claimed_at") {
        return Ok(());
    }
    // One transaction for read + DROP + CREATE + expansion: a crash midway
    // would otherwise leave the new (partially filled) table in place, and
    // the next startup would see `claimed_at` and consider the migration
    // done — silently losing the debts that were never re-inserted.
    let tx = conn.unchecked_transaction()?;
    let legacy: Vec<(String, i64, i64)> = {
        let mut stmt = tx.prepare("SELECT thread_id, injected_at, owed FROM live_injections")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    tx.execute_batch(
        "DROP TABLE live_injections;
         CREATE TABLE live_injections (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           thread_id TEXT NOT NULL,
           injected_at INTEGER NOT NULL,
           claimed_at INTEGER
         );",
    )?;
    // Each outstanding count becomes one unclaimed row so no owed answer is
    // silently dropped by the upgrade.
    for (thread_id, injected_at, owed) in legacy {
        for _ in 0..owed.max(0) {
            tx.execute(
                "INSERT INTO live_injections(thread_id, injected_at, claimed_at)
                 VALUES (?1, ?2, NULL)",
                params![thread_id, injected_at],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(columns)
}

fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    let columns = table_columns(conn, table)?;
    if columns.iter().any(|current| current == column) {
        return Ok(());
    }
    match conn.execute_batch(&format!(
        "ALTER TABLE {table} ADD COLUMN {column} {definition}"
    )) {
        Ok(()) => Ok(()),
        // The hook process and the daemon open the database independently and
        // can migrate at the same moment; losing that race means the column
        // now exists, which is exactly the desired end state.
        Err(error) if error.to_string().contains("duplicate column name") => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn state_dir_path() -> Result<PathBuf> {
    if let Ok(dir) = env::var("TINYCTB_STATE_DIR") {
        let dir = PathBuf::from(dir);
        fs::create_dir_all(&dir)?;
        return Ok(dir);
    }

    let home = env::var("HOME").context("HOME is not set")?;
    let dir = PathBuf::from(home).join(".tinyctb");
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub(crate) fn state_db_path() -> Result<PathBuf> {
    Ok(state_dir_path()?.join("state.db"))
}

pub(crate) fn remote_mode_status_path() -> Result<PathBuf> {
    Ok(state_dir_path()?.join("remote-mode.json"))
}

#[allow(dead_code)]
pub(crate) fn live_backend_status_path() -> Result<PathBuf> {
    Ok(state_dir_path()?.join("live-backend.json"))
}

#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) fn get_setting_number(conn: &Connection, key: &str) -> Result<Option<u64>> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM settings WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()?;
    Ok(raw.and_then(|value| value.parse::<u64>().ok()))
}

pub(crate) fn set_setting(conn: &Connection, key: &str, value: u64) -> Result<()> {
    conn.execute(
        "INSERT INTO settings(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value.to_string()],
    )?;
    Ok(())
}

pub(crate) fn set_setting_text(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO settings(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

pub(crate) fn get_setting_text(conn: &Connection, key: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT value FROM settings WHERE key = ?1",
        params![key],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub(crate) fn list_settings_with_prefix(
    conn: &Connection,
    prefix: &str,
) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT key, value FROM settings
         WHERE key LIKE ?1
         ORDER BY key",
    )?;
    let pattern = format!("{prefix}%");
    let rows = stmt.query_map(params![pattern], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

pub(crate) fn delete_setting(conn: &Connection, key: &str) -> Result<()> {
    conn.execute("DELETE FROM settings WHERE key = ?1", params![key])?;
    Ok(())
}

pub(crate) fn upsert_thread_snapshot(
    conn: &Connection,
    snapshot: &BridgeThreadSnapshot,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO threads_cache(
            thread_id, name, cwd, source, status_type, status_flags_json,
            updated_at, last_seen_at, last_turn_status, last_preview
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(thread_id) DO UPDATE SET
            name = excluded.name,
            cwd = excluded.cwd,
            source = excluded.source,
            status_type = excluded.status_type,
            status_flags_json = excluded.status_flags_json,
            updated_at = CASE
                WHEN threads_cache.updated_at IS NULL THEN excluded.updated_at
                WHEN excluded.updated_at IS NULL THEN threads_cache.updated_at
                WHEN excluded.updated_at > threads_cache.updated_at THEN excluded.updated_at
                ELSE threads_cache.updated_at
            END,
            last_seen_at = excluded.last_seen_at,
            last_turn_status = excluded.last_turn_status,
            last_preview = excluded.last_preview",
        params![
            snapshot.thread_id,
            snapshot.name,
            snapshot.cwd,
            "claude-code",
            snapshot.status_type,
            serde_json::to_string(&snapshot.status_flags)?,
            snapshot.updated_at.map(to_sql_i64).transpose()?,
            to_sql_i64(now)?,
            snapshot.last_turn_status,
            snapshot.last_preview,
        ],
    )?;

    match &snapshot.pending_prompt {
        Some(prompt) => {
            conn.execute(
                "INSERT INTO pending_prompts(thread_id, prompt_id, prompt_kind, prompt_status, question, created_at, transcript_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(thread_id) DO UPDATE SET
                    prompt_id = excluded.prompt_id,
                    prompt_kind = excluded.prompt_kind,
                    prompt_status = excluded.prompt_status,
                    question = excluded.question,
                    created_at = excluded.created_at,
                    transcript_bytes = excluded.transcript_bytes",
                params![
                    snapshot.thread_id,
                    prompt.prompt_id,
                    prompt.kind,
                    prompt.status,
                    prompt.question.clone().unwrap_or_default(),
                    to_sql_i64(now)?,
                    prompt.transcript_bytes.map(|bytes| bytes as i64),
                ],
            )?;
        }
        None => {
            conn.execute(
                "DELETE FROM pending_prompts WHERE thread_id = ?1",
                params![snapshot.thread_id],
            )?;
        }
    }
    Ok(())
}

/// Remember where a live session listens for injected messages. Reported by
/// the session's own hooks (they inherit CLAUDE_CODE_MESSAGING_SOCKET), so
/// this mapping is authoritative rather than guessed from process state.
pub(crate) fn record_session_messaging_socket(
    conn: &Connection,
    thread_id: &str,
    socket: &crate::claude::SessionSocket,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO threads_cache(
            thread_id, status_type, status_flags_json, last_seen_at,
            messaging_socket, socket_inode, socket_boot_id
         ) VALUES (?1, 'active', '[]', ?3, ?2, ?4, ?5)
         ON CONFLICT(thread_id) DO UPDATE SET
            messaging_socket = excluded.messaging_socket,
            socket_inode = excluded.socket_inode,
            socket_boot_id = excluded.socket_boot_id",
        params![
            thread_id,
            socket.path,
            to_sql_i64(now)?,
            socket.inode.map(|value| value as i64),
            socket.boot_id
        ],
    )?;
    Ok(())
}

/// The socket a live session reported, with the identity captured at that
/// moment so the caller can refuse a path that has since been rebound.
pub(crate) fn session_messaging_socket(
    conn: &Connection,
    thread_id: &str,
) -> Result<Option<crate::claude::SessionSocket>> {
    let row: Option<(Option<String>, Option<i64>, Option<String>)> = conn
        .query_row(
            "SELECT messaging_socket, socket_inode, socket_boot_id
             FROM threads_cache WHERE thread_id = ?1",
            params![thread_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    Ok(row.and_then(|(path, inode, boot_id)| {
        let path = path.filter(|value| !value.trim().is_empty())?;
        Some(crate::claude::SessionSocket {
            path,
            inode: inode.map(|value| value as u64),
            boot_id,
        })
    }))
}

/// A message injected into a live session owes an answer back to Telegram,
/// exactly like a headless bridge turn does. There is no per-turn log to
/// read here — the answer lands in the live session's own transcript — so
/// the owed answer is claimed by that session's next completion.
pub(crate) const LIVE_INJECTION_TTL_MS: u64 = 6 * 60 * 60 * 1000;

pub(crate) fn record_live_injection(conn: &Connection, thread_id: &str, now: u64) -> Result<()> {
    conn.execute(
        "INSERT INTO live_injections(thread_id, injected_at, claimed_at)
         VALUES (?1, ?2, NULL)",
        params![thread_id, to_sql_i64(now)?],
    )?;
    Ok(())
}

/// Id of the oldest unclaimed injection this completion may answer. A
/// completion that happened BEFORE the injection cannot be its answer — a
/// Stop already sitting in the spool would otherwise be pushed as the reply
/// and burn the debt, leaving the real answer unsent.
fn claimable_live_injection(
    conn: &Connection,
    thread_id: &str,
    event_at: Option<u64>,
    now: u64,
) -> Result<Option<i64>> {
    // No timestamp on the event: it cannot be proven to postdate the
    // injection, so it must not claim it.
    let Some(event_at) = event_at else {
        return Ok(None);
    };
    // Both sides go through the same normalization — hook timestamps and
    // stored timestamps can differ in unit, and comparing a normalized value
    // against a raw one silently lets an older completion win.
    let event_ms = timestamp_to_millis(event_at);
    let now_ms = timestamp_to_millis(now);
    let mut stmt = conn.prepare(
        "SELECT id, injected_at FROM live_injections
         WHERE thread_id = ?1 AND claimed_at IS NULL
         ORDER BY injected_at ASC, id ASC",
    )?;
    let rows = stmt
        .query_map(params![thread_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, injected_at) in rows {
        let injected_ms = timestamp_to_millis(from_sql_i64(injected_at)?);
        if now_ms.saturating_sub(injected_ms) > LIVE_INJECTION_TTL_MS {
            continue;
        }
        if injected_ms <= event_ms {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

pub(crate) fn live_injection_pending(
    conn: &Connection,
    thread_id: &str,
    event_at: Option<u64>,
    now: u64,
) -> Result<bool> {
    Ok(claimable_live_injection(conn, thread_id, event_at, now)?.is_some())
}

/// Mark the injection this completion answered as settled.
pub(crate) fn consume_live_injection(
    conn: &Connection,
    thread_id: &str,
    event_at: Option<u64>,
    now: u64,
) -> Result<()> {
    if let Some(id) = claimable_live_injection(conn, thread_id, event_at, now)? {
        conn.execute(
            "UPDATE live_injections SET claimed_at = ?2 WHERE id = ?1",
            params![id, to_sql_i64(now)?],
        )?;
    }
    Ok(())
}

pub(crate) fn record_action(
    conn: &Connection,
    thread_id: &str,
    action_type: &str,
    payload: Value,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO actions_log(thread_id, action_type, payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            thread_id,
            action_type,
            serde_json::to_string(&payload)?,
            to_sql_i64(now)?
        ],
    )?;
    Ok(())
}

pub(crate) fn get_thread_history(
    conn: &Connection,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<HistoryAction>> {
    let mut stmt = conn.prepare(
        "SELECT action_type, payload_json, created_at
         FROM actions_log
         WHERE thread_id = ?1
         ORDER BY id DESC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![thread_id, to_sql_i64(limit)?], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    let raw = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    raw.into_iter()
        .map(|(action_type, payload_json, created_at)| {
            Ok(HistoryAction {
                action_type,
                payload: serde_json::from_str::<Value>(&payload_json).unwrap_or(Value::Null),
                created_at: from_sql_i64(created_at)?,
            })
        })
        .collect()
}

pub(crate) fn recent_actions_json(
    conn: &Connection,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<Value>> {
    Ok(get_thread_history(conn, thread_id, limit)?
        .into_iter()
        .map(|action| {
            json!({
                "actionType": action.action_type,
                "createdAt": action.created_at,
                "payload": action.payload
            })
        })
        .collect())
}

pub(crate) fn thread_snapshot_json(snapshot: &BridgeThreadSnapshot) -> Value {
    json!({
        "threadId": snapshot.thread_id,
        "eventUid": snapshot.event_uid,
        "name": snapshot.name,
        "cwd": snapshot.cwd,
        "updatedAt": snapshot.updated_at,
        "statusType": snapshot.status_type,
        "statusFlags": snapshot.status_flags,
        "lastTurnStatus": snapshot.last_turn_status,
        "lastPreview": snapshot.last_preview,
        "pendingPrompt": snapshot.pending_prompt.as_ref().map(|prompt| json!({
            "promptId": prompt.prompt_id,
            "promptKind": prompt.kind,
            "promptStatus": prompt.status,
            "question": prompt.question
        }))
    })
}

pub(crate) fn should_emit_for_away_window(
    away_started_at: Option<u64>,
    updated_at: Option<u64>,
) -> bool {
    match away_started_at {
        None => true,
        Some(started_at) => updated_at
            .map(|value| timestamp_to_millis(value) >= timestamp_to_millis(started_at))
            .unwrap_or(false),
    }
}

fn record_delivery(
    conn: &Connection,
    event_key: &str,
    thread_id: &str,
    event_type: &str,
    delivered_at: u64,
) -> Result<bool> {
    let changed = conn.execute(
        "INSERT OR IGNORE INTO delivery_log(event_key, thread_id, event_type, delivered_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![event_key, thread_id, event_type, to_sql_i64(delivered_at)?],
    )?;
    Ok(changed > 0)
}

fn record_thread_event(
    conn: &Connection,
    event_key: &str,
    thread_id: &str,
    event_type: &str,
    observed_at: u64,
    payload: &Value,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO thread_events(event_key, thread_id, event_type, observed_at, payload_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            event_key,
            thread_id,
            event_type,
            to_sql_i64(observed_at)?,
            serde_json::to_string(payload)?
        ],
    )?;
    Ok(())
}

pub(crate) fn reconcile_thread_snapshots(
    conn: &Connection,
    now: u64,
    snapshots: Vec<BridgeThreadSnapshot>,
    record_deliveries: bool,
) -> Result<Value> {
    let away = get_setting_text(conn, "away")?.unwrap_or_default() == "true";
    let away_started_at = get_setting_number(conn, "away_started_at")?;
    let mut events = Vec::new();
    let mut threads = Vec::new();

    for snapshot in &snapshots {
        upsert_thread_snapshot(conn, snapshot, now)?;
        threads.push(thread_snapshot_json(snapshot));

        // Hook events drive away notifications only — EXCEPT for a session
        // that owes an answer to a message injected from Telegram. That
        // message went into this live session's queue, so its next
        // completion is the reply the user was promised and must go out
        // whatever the away switch says.
        let owes_injected_answer =
            live_injection_pending(conn, &snapshot.thread_id, snapshot.updated_at, now)?;
        if !owes_injected_answer
            && (!away || !should_emit_for_away_window(away_started_at, snapshot.updated_at))
        {
            continue;
        }

        // The hook spool uid (unique even for same-millisecond hooks)
        // disambiguates event keys; scan snapshots fall back to updated_at
        // but never reach here anyway (they carry no prompt/completion).
        let event_discriminator = snapshot.event_uid.clone().unwrap_or_else(|| {
            snapshot
                .updated_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string())
        });

        if let Some(prompt) = &snapshot.pending_prompt {
            let event_key = format!(
                "thread_waiting:{}:{}:{}",
                snapshot.thread_id, prompt.prompt_id, event_discriminator
            );
            let should_emit = !record_deliveries
                || record_delivery(conn, &event_key, &snapshot.thread_id, "thread_waiting", now)?;
            if should_emit {
                let event = json!({
                    "type": "thread_waiting",
                    "threadId": snapshot.thread_id,
                    "eventUid": snapshot.event_uid,
                    "promptKind": prompt.kind,
                    "updatedAt": snapshot.updated_at,
                    "lastPreview": snapshot.last_preview,
                    "eventKey": event_key,
                });
                record_thread_event(
                    conn,
                    &event_key,
                    &snapshot.thread_id,
                    "thread_waiting",
                    now,
                    &event,
                )?;
                events.push(event);
            }
        }

        if snapshot.pending_prompt.is_none()
            && snapshot.last_turn_status.as_deref() == Some("completed")
        {
            let event_key = format!(
                "thread_completed:{}:{}",
                snapshot.thread_id, event_discriminator
            );
            let should_emit = !record_deliveries
                || record_delivery(
                    conn,
                    &event_key,
                    &snapshot.thread_id,
                    "thread_completed",
                    now,
                )?;
            if should_emit {
                let event = json!({
                    "type": "thread_completed",
                    "threadId": snapshot.thread_id,
                    "eventUid": snapshot.event_uid,
                    "updatedAt": snapshot.updated_at,
                    "lastPreview": snapshot.last_preview,
                    "eventKey": event_key,
                });
                record_thread_event(
                    conn,
                    &event_key,
                    &snapshot.thread_id,
                    "thread_completed",
                    now,
                    &event,
                )?;
                events.push(event);
            }
        }
    }

    Ok(json!({
        "synced": snapshots.len(),
        "threads": threads,
        "events": events,
        "away": away
    }))
}

pub(crate) fn list_waiting_from_db(
    conn: &Connection,
    project_filter: Option<&str>,
    limit: u64,
) -> Result<WaitingResult> {
    let mut stmt = conn.prepare(
        "SELECT t.thread_id, t.name, t.cwd, t.updated_at, t.status_type, t.status_flags_json,
                t.last_preview, p.prompt_id, p.prompt_kind, p.prompt_status, p.question
         FROM pending_prompts p
         INNER JOIN threads_cache t ON t.thread_id = p.thread_id
         ORDER BY COALESCE(t.updated_at, 0) DESC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, String>(8)?,
            row.get::<_, String>(9)?,
            row.get::<_, String>(10)?,
        ))
    })?;
    let raw = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut threads = raw
        .into_iter()
        .map(
            |(
                thread_id,
                name,
                cwd,
                updated_at_raw,
                status_type,
                status_flags_json,
                last_preview,
                prompt_id,
                prompt_kind,
                prompt_status,
                question,
            )| {
                let prompt = PendingPrompt {
                    prompt_id,
                    kind: prompt_kind.clone(),
                    status: prompt_status,
                    question: Some(question.clone()),
                    transcript_bytes: None,
                };
                let project = derive_project_label(cwd.as_deref());
                let display_name = derive_thread_display_name(
                    name.as_deref(),
                    project.as_deref(),
                    Some(question.as_str()),
                    &thread_id,
                );
                let label = format!(
                    "{} · {} · {}",
                    display_name,
                    project
                        .clone()
                        .or(cwd.clone())
                        .unwrap_or_else(|| "unknown cwd".to_string()),
                    prompt_kind
                );
                Ok(WaitingThread {
                    thread_id,
                    name,
                    display_name,
                    project,
                    cwd,
                    updated_at: optional_from_sql_i64(updated_at_raw)?,
                    status_type,
                    status_flags: serde_json::from_str(&status_flags_json).unwrap_or_default(),
                    prompt,
                    last_preview,
                    label,
                })
            },
        )
        .collect::<Result<Vec<_>>>()?;
    if let Some(needle) = project_filter {
        let needle = needle.to_lowercase();
        threads.retain(|thread| {
            thread
                .cwd
                .as_deref()
                .unwrap_or_default()
                .to_lowercase()
                .contains(&needle)
        });
    }
    if limit > 0 {
        threads.truncate(limit as usize);
    }
    Ok(WaitingResult {
        summary: WaitingSummary {
            count: threads.len(),
            thread_ids: threads
                .iter()
                .map(|thread| thread.thread_id.clone())
                .collect(),
            labels: threads.iter().map(|thread| thread.label.clone()).collect(),
            applied_filters: json!({ "project": project_filter, "limit": limit }),
        },
        threads,
    })
}

pub(crate) fn list_recent_thread_snapshots_from_db(
    conn: &Connection,
    limit: u64,
) -> Result<Vec<BridgeThreadSnapshot>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut stmt = conn.prepare(
        "SELECT t.thread_id, t.name, t.cwd, t.updated_at, t.status_type, t.status_flags_json,
                t.last_turn_status, t.last_preview, p.prompt_id, p.prompt_kind, p.prompt_status,
                p.question
         FROM threads_cache t
         LEFT JOIN pending_prompts p ON p.thread_id = t.thread_id
         ORDER BY COALESCE(t.updated_at, t.last_seen_at, 0) DESC
         LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![to_sql_i64(limit)?], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<String>>(11)?,
        ))
    })?;
    let raw = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    raw.into_iter()
        .map(
            |(
                thread_id,
                name,
                cwd,
                updated_at_raw,
                status_type,
                status_flags_json,
                last_turn_status,
                last_preview,
                prompt_id,
                prompt_kind,
                prompt_status,
                question,
            )| {
                Ok(BridgeThreadSnapshot {
                    thread_id,
                    name,
                    cwd,
                    updated_at: optional_from_sql_i64(updated_at_raw)?,
                    status_type,
                    status_flags: serde_json::from_str(&status_flags_json).unwrap_or_default(),
                    last_turn_status,
                    last_preview,
                    pending_prompt: prompt_id.map(|prompt_id| PendingPrompt {
                        prompt_id,
                        kind: prompt_kind.unwrap_or_else(|| "reply".to_string()),
                        status: prompt_status.unwrap_or_else(|| "Needs input".to_string()),
                        question,
                        transcript_bytes: None,
                    }),
                    event_uid: None,
                })
            },
        )
        .collect()
}

pub(crate) fn list_inbox_from_db(
    conn: &Connection,
    now: u64,
    project_filter: Option<&str>,
    status_filter: Option<&str>,
    attention_filter: Option<&str>,
    waiting_on_filter: Option<&str>,
    limit: u64,
) -> Result<InboxResult> {
    let mut stmt = conn.prepare(
        "SELECT t.thread_id, t.name, t.cwd, t.updated_at, t.last_seen_at, t.status_type, t.status_flags_json,
                t.last_turn_status, t.last_preview, p.prompt_kind, p.prompt_status, p.question
         FROM threads_cache t
         LEFT JOIN pending_prompts p ON p.thread_id = t.thread_id
         ORDER BY COALESCE(t.updated_at, 0) DESC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<String>>(11)?,
        ))
    })?;
    let raw = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut items = raw
        .into_iter()
        .map(
            |(
                thread_id,
                name,
                cwd,
                updated_at_raw,
                last_seen_at_raw,
                status_type,
                status_flags_json,
                last_turn_status,
                last_preview,
                prompt_kind,
                prompt_status,
                question,
            )| {
                let status_flags =
                    serde_json::from_str::<Vec<String>>(&status_flags_json).unwrap_or_default();
                let pending_prompt = prompt_kind.clone().map(|kind| PendingPrompt {
                    prompt_id: format!("{}:{}", kind, thread_id),
                    kind,
                    status: prompt_status.unwrap_or_else(|| "Needs input".to_string()),
                    question,
                    transcript_bytes: None,
                });
                let snapshot = BridgeThreadSnapshot {
                    thread_id,
                    name,
                    cwd,
                    updated_at: optional_from_sql_i64(updated_at_raw)?,
                    status_type,
                    status_flags,
                    last_turn_status,
                    last_preview,
                    pending_prompt,
                    event_uid: None,
                };
                let mut item = classify_inbox_item(&snapshot, now);
                item.last_seen_at = Some(from_sql_i64(last_seen_at_raw)?);
                item.recent_action = recent_actions_json(conn, &item.thread_id, 1)?
                    .into_iter()
                    .next();
                Ok(item)
            },
        )
        .collect::<Result<Vec<_>>>()?;

    if let Some(needle) = project_filter {
        let needle = needle.to_lowercase();
        items.retain(|item| {
            item.cwd
                .as_deref()
                .unwrap_or_default()
                .to_lowercase()
                .contains(&needle)
        });
    }
    if let Some(status) = status_filter {
        items.retain(|item| item.status_type == status);
    }
    if let Some(attention) = attention_filter {
        items.retain(|item| item.attention_reason == attention);
    }
    if let Some(waiting_on) = waiting_on_filter {
        items.retain(|item| item.waiting_on == waiting_on);
    }

    items.sort_by_key(|item| std::cmp::Reverse(score_inbox_item(item)));
    if limit > 0 {
        items.truncate(limit as usize);
    }

    let pending_approval = items
        .iter()
        .filter(|item| item.attention_reason == "pending_approval")
        .count();
    let needs_reply = items
        .iter()
        .filter(|item| item.attention_reason == "needs_reply")
        .count();
    let active = items
        .iter()
        .filter(|item| item.attention_reason == "active")
        .count();
    let completed = items
        .iter()
        .filter(|item| item.attention_reason == "completed")
        .count();

    Ok(InboxResult {
        summary: InboxSummary {
            total: items.len(),
            needs_attention: items.iter().filter(|item| item.waiting_on == "me").count(),
            counts_by_reason: json!({
                "pendingApproval": pending_approval,
                "needsReply": needs_reply,
                "active": active,
                "completed": completed
            }),
            applied_filters: json!({
                "project": project_filter,
                "status": status_filter,
                "attention": attention_filter,
                "waitingOn": waiting_on_filter,
                "limit": limit
            }),
        },
        items,
    })
}

#[allow(dead_code)]
pub(crate) fn resolve_archive_targets(
    conn: &Connection,
    thread_ids: &[String],
    project: Option<&str>,
    status: Option<&str>,
    attention: Option<&str>,
    limit: u64,
    now: u64,
) -> Result<ArchiveSelection> {
    if !thread_ids.is_empty() {
        return Ok(ArchiveSelection {
            targets: thread_ids.to_vec(),
            using_filter_selection: false,
        });
    }

    let inbox = list_inbox_from_db(conn, now, project, status, attention, None, limit)?;
    Ok(ArchiveSelection {
        targets: inbox
            .items
            .into_iter()
            .map(|item| item.thread_id)
            .collect::<Vec<_>>(),
        using_filter_selection: true,
    })
}

#[allow(dead_code)]
pub(crate) fn archive_result(dry_run: bool, results: Vec<Value>) -> Value {
    json!({
        "ok": true,
        "action": "archive",
        "dryRun": dry_run,
        "results": results
    })
}

/// A turn started from the bridge (Telegram reply or /new). Its answer is
/// read from the turn's own log file (`claude -p --output-format json` writes
/// the result there), which binds the answer to exactly this turn — Stop
/// hooks cannot attribute an answer when the target session is also active
/// elsewhere (a busy agent session's own turn once got pushed as "the
/// answer" while the real one queued for 19 minutes and was then dropped).
#[derive(Debug, Clone)]
pub(crate) struct BridgeTurn {
    pub(crate) turn_id: String,
    pub(crate) thread_id: String,
    pub(crate) log_path: String,
    pub(crate) pid: Option<u32>,
    pub(crate) started_at: u64,
    /// The daemon reaped this child and it is definitely gone (authoritative,
    /// unlike `kill -0` which zombies and PID reuse can fool).
    pub(crate) exited: bool,
    pub(crate) exit_code: Option<i32>,
    /// Restart-kill identity (Linux only): process group + starttime ticks
    /// (/proc/<pid>/stat, exec-invariant, unique per PID incarnation) scoped
    /// by boot_id. All three must match or the kill is refused. The DB also
    /// stores proc_start/proc_exe for forensics, but they take no part in
    /// the decision (unreachable on macOS, racy across exec on Linux).
    pub(crate) pgid: Option<u32>,
    pub(crate) proc_start_ticks: Option<String>,
    pub(crate) boot_id: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn register_bridge_turn(
    conn: &Connection,
    turn_id: &str,
    thread_id: &str,
    log_path: &str,
    pid: Option<u32>,
    proc_start: Option<&str>,
    proc_exe: Option<&str>,
    pgid: Option<u32>,
    proc_start_ticks: Option<&str>,
    boot_id: Option<&str>,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO bridge_turns(turn_id, thread_id, log_path, pid, started_at, status, completed_at, exited, exit_code, proc_start, proc_exe, pgid, proc_start_ticks, boot_id)
         VALUES (?1, ?2, ?3, ?4, ?5, 'running', NULL, 0, NULL, ?6, ?7, ?8, ?9, ?10)",
        params![
            turn_id,
            thread_id,
            log_path,
            pid.map(i64::from),
            to_sql_i64(now)?,
            proc_start,
            proc_exe,
            pgid.map(i64::from),
            proc_start_ticks,
            boot_id
        ],
    )?;
    Ok(())
}

/// Threads whose hooks observed a still-pending terminal prompt, with the
/// prompt itself — for the /threads pool union. A session stuck at a
/// terminal dialog may be far older than the recent-cache window, and it is
/// exactly the session the user runs /threads to find.
pub(crate) fn threads_with_pending_terminal_prompts(
    conn: &Connection,
) -> Result<Vec<(String, PendingPrompt, u64)>> {
    let mut stmt = conn.prepare(
        "SELECT thread_id, prompt_id, prompt_kind, prompt_status, question, MAX(created_at)
         FROM pending_prompts WHERE prompt_status = 'pending' GROUP BY thread_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            PendingPrompt {
                prompt_id: row.get(1)?,
                kind: row.get(2)?,
                status: row.get(3)?,
                question: row.get(4)?,
                transcript_bytes: None,
            },
            row.get::<_, i64>(5)?,
        ))
    })?;
    let mut result = Vec::new();
    for row in rows {
        let (thread_id, prompt, created_at) = row?;
        result.push((thread_id, prompt, from_sql_i64(created_at)?));
    }
    Ok(result)
}

pub(crate) fn list_running_bridge_turns(conn: &Connection) -> Result<Vec<BridgeTurn>> {
    let mut stmt = conn.prepare(
        "SELECT turn_id, thread_id, log_path, pid, started_at, exited, exit_code,
                pgid, proc_start_ticks, boot_id
         FROM bridge_turns WHERE status = 'running' ORDER BY started_at ASC",
    )?;
    type BridgeTurnRow = (
        String,
        String,
        String,
        Option<i64>,
        i64,
        i64,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
    );
    let rows = stmt.query_map([], |row| {
        Ok::<BridgeTurnRow, rusqlite::Error>((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
            row.get(8)?,
            row.get(9)?,
        ))
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(
            |(
                turn_id,
                thread_id,
                log_path,
                pid,
                started_at,
                exited,
                exit_code,
                pgid,
                proc_start_ticks,
                boot_id,
            )| {
                Ok(BridgeTurn {
                    turn_id,
                    thread_id,
                    log_path,
                    pid: pid.and_then(|value| u32::try_from(value).ok()),
                    started_at: from_sql_i64(started_at)?,
                    exited: exited != 0,
                    exit_code: exit_code.and_then(|value| i32::try_from(value).ok()),
                    pgid: pgid.and_then(|value| u32::try_from(value).ok()),
                    proc_start_ticks,
                    boot_id,
                })
            },
        )
        .collect()
}

/// Record that a spawned turn process was reaped by the daemon.
pub(crate) fn record_bridge_turn_exit(
    conn: &Connection,
    pid: u32,
    exit_code: Option<i32>,
) -> Result<()> {
    conn.execute(
        "UPDATE bridge_turns SET exited = 1, exit_code = ?2
         WHERE pid = ?1 AND status = 'running'",
        params![i64::from(pid), exit_code],
    )?;
    Ok(())
}

/// Atomically claim a turn's failure transition. The daemon judges a turn
/// dead from an in-memory SNAPSHOT, and the most dangerous way that snapshot
/// goes stale is a turn read as `pid NULL` whose identity write lands before
/// the verdict is applied — settling it then would close a turn whose caller
/// was just told "started" and whose token now points at a failed row.
///
/// So the verdict is applied as a conditional UPDATE, and `pid_still_missing`
/// re-asserts the very evidence the verdict was based on. Zero rows = the
/// evidence no longer holds (or the turn is no longer running) — the caller
/// must drop the verdict, not announce it. A re-read instead of a CAS would
/// leave the same TOCTOU window this exists to close.
pub(crate) fn claim_bridge_turn_failure(
    conn: &Connection,
    turn_id: &str,
    status: &str,
    now: u64,
    pid_still_missing: bool,
) -> Result<bool> {
    let sql = if pid_still_missing {
        "UPDATE bridge_turns SET status = ?2, completed_at = ?3
         WHERE turn_id = ?1 AND status = 'running' AND pid IS NULL"
    } else {
        "UPDATE bridge_turns SET status = ?2, completed_at = ?3
         WHERE turn_id = ?1 AND status = 'running'"
    };
    let rows = conn.execute(sql, params![turn_id, status, to_sql_i64(now)?])?;
    Ok(rows > 0)
}

pub(crate) fn mark_bridge_turn_finished(
    conn: &Connection,
    turn_id: &str,
    status: &str,
    now: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE bridge_turns SET status = ?2, completed_at = ?3 WHERE turn_id = ?1",
        params![turn_id, status, to_sql_i64(now)?],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Remote approvals

/// Writes the away marker the approval gate reads on its fast path, without
/// needing a database — used by tests to put the gate in "away" mode.
#[cfg(test)]
pub(crate) fn write_away_marker_for_test(away: bool) -> Result<()> {
    let path = remote_mode_status_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, serde_json::to_vec(&json!({ "away": away }))?)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_pending_approval(
    conn: &Connection,
    approval_id: &str,
    thread_id: &str,
    tool_name: &str,
    summary: &str,
    headless: bool,
    now: u64,
    expires_at: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO pending_approvals(
            approval_id, thread_id, tool_name, summary, headless, created_at, decision,
            decided_at, expires_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7)",
        params![
            approval_id,
            thread_id,
            tool_name,
            summary,
            headless as i64,
            to_sql_i64(now)?,
            to_sql_i64(expires_at)?
        ],
    )?;
    Ok(())
}

/// Settles an approval nobody answered in time, as an ATOMIC transition:
/// either this call marks it expired (returns None), or a tap beat it to the
/// row by a hair and the decision that actually landed is returned so the
/// caller can honour it. Without this, the losing side would return "no
/// opinion" to the session while Telegram had already told the user their
/// answer was accepted.
pub(crate) fn expire_or_take_decision(
    conn: &Connection,
    approval_id: &str,
    now: u64,
) -> Result<Option<String>> {
    let changed = conn.execute(
        "UPDATE pending_approvals SET decision = 'expired', decided_at = ?2
         WHERE approval_id = ?1 AND decision IS NULL",
        params![approval_id, to_sql_i64(now)?],
    )?;
    if changed > 0 {
        return Ok(None);
    }
    // Lost the race (or the row is gone): report whatever is recorded, but
    // never treat our own "expired" marker as an answer.
    Ok(approval_decision(conn, approval_id)?.filter(|decision| decision != "expired"))
}

/// Remember which Telegram message carries an approval request, so a text
/// reply to it can be recognised as "you meant to answer this" instead of
/// being injected into the session as an ordinary message.
pub(crate) fn attach_approval_message_id(
    conn: &Connection,
    approval_id: &str,
    message_id: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE pending_approvals SET message_id = ?2 WHERE approval_id = ?1",
        params![approval_id, message_id],
    )?;
    Ok(())
}

/// Every chunk of a dialog message maps back to its dialog. Recorded per
/// chunk because a long question is split across several Telegram messages
/// and the user may reply to ANY of them; recognition must not depend on
/// which chunk they picked, nor on the dialog still being open — a reply to
/// a settled dialog is still a reply to that dialog, not chat for the
/// session.
pub(crate) fn record_dialog_message(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
    kind: &str,
    ref_id: &str,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO dialog_messages(chat_id, message_id, kind, ref_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![chat_id, message_id, kind, ref_id, to_sql_i64(now)?],
    )?;
    Ok(())
}

/// (kind, ref_id) of the dialog a message belongs to, whatever its state.
pub(crate) fn dialog_for_message(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
) -> Result<Option<(String, String)>> {
    conn.query_row(
        "SELECT kind, ref_id FROM dialog_messages WHERE chat_id = ?1 AND message_id = ?2",
        params![chat_id, message_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(Into::into)
}

pub(crate) fn approval_decision(conn: &Connection, approval_id: &str) -> Result<Option<String>> {
    let decision: Option<Option<String>> = conn
        .query_row(
            "SELECT decision FROM pending_approvals WHERE approval_id = ?1",
            params![approval_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(decision.flatten())
}

/// The outcome of tapping a button, so the toast can tell the truth instead
/// of claiming success for an answer that arrived too late.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApprovalAnswer {
    Recorded,
    AlreadyAnswered,
    Expired,
    Unknown,
}

/// Records the answer only if the approval is still unanswered AND still
/// within its deadline: the waiting hook gives up at that deadline, so an
/// answer after it cannot reach the session no matter what we store.
pub(crate) fn record_approval_decision(
    conn: &Connection,
    approval_id: &str,
    decision: &str,
    now: u64,
) -> Result<ApprovalAnswer> {
    let row: Option<(Option<String>, i64)> = conn
        .query_row(
            "SELECT decision, expires_at FROM pending_approvals WHERE approval_id = ?1",
            params![approval_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((existing, expires_at)) = row else {
        return Ok(ApprovalAnswer::Unknown);
    };
    // An expired record is settled too, but the user must be told which kind
    // of "settled" it is: the session has already fallen back to its own
    // prompt, so "already answered" would be misleading.
    match existing.as_deref() {
        Some("expired") => return Ok(ApprovalAnswer::Expired),
        Some(_) => return Ok(ApprovalAnswer::AlreadyAnswered),
        None => {}
    }
    let expires_at = from_sql_i64(expires_at)?;
    if expires_at > 0 && timestamp_to_millis(now) > timestamp_to_millis(expires_at) {
        // Past the deadline the waiting hook has already given up, so record
        // the expiry rather than an answer that can no longer be delivered.
        conn.execute(
            "UPDATE pending_approvals SET decision = 'expired', decided_at = ?2
             WHERE approval_id = ?1 AND decision IS NULL",
            params![approval_id, to_sql_i64(now)?],
        )?;
        return Ok(ApprovalAnswer::Expired);
    }
    let changed = conn.execute(
        "UPDATE pending_approvals SET decision = ?2, decided_at = ?3
         WHERE approval_id = ?1 AND decision IS NULL",
        params![approval_id, decision, to_sql_i64(now)?],
    )?;
    if changed > 0 {
        return Ok(ApprovalAnswer::Recorded);
    }
    // The row was settled between the read above and this update — most
    // likely by the waiting hook giving up. Classify from the state that
    // actually landed, or the user would be told "already handled" when the
    // truth is "timed out, your session is back at the terminal".
    settled_answer_kind(conn, approval_id)
}

/// How a row that could not be updated ended up settled.
fn settled_answer_kind(conn: &Connection, approval_id: &str) -> Result<ApprovalAnswer> {
    Ok(match approval_decision(conn, approval_id)?.as_deref() {
        Some("expired") => ApprovalAnswer::Expired,
        Some(_) => ApprovalAnswer::AlreadyAnswered,
        None => ApprovalAnswer::Unknown,
    })
}

pub(crate) fn pending_approval_row(
    conn: &Connection,
    approval_id: &str,
) -> Result<Option<(String, String, String)>> {
    conn.query_row(
        "SELECT thread_id, tool_name, summary FROM pending_approvals WHERE approval_id = ?1",
        params![approval_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()
    .map_err(Into::into)
}

// --- questions the session is blocked on -----------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_pending_question(
    conn: &Connection,
    question_id: &str,
    thread_id: &str,
    question: &str,
    options: &[String],
    multi_select: bool,
    now: u64,
    expires_at: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO pending_questions(
            question_id, thread_id, question, options_json, multi_select, created_at,
            expires_at, message_id, answer, answered_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, NULL)",
        params![
            question_id,
            thread_id,
            question,
            serde_json::to_string(options)?,
            multi_select as i64,
            to_sql_i64(now)?,
            to_sql_i64(expires_at)?
        ],
    )?;
    Ok(())
}

/// A prompt that is still WAITING on the user: its hook is blocked polling
/// the row this very moment, so a fresh set of answer buttons — on a
/// /threads message, say — feeds the same row and works exactly like the
/// original notification's buttons. One-answer semantics stay with the row,
/// not with any particular message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenPrompt {
    Approval {
        approval_id: String,
        summary: String,
        /// The GATE KIND at creation time, replayed verbatim: what a timeout
        /// does is a property of the approval, not of whatever the session
        /// happens to look like when /threads reoffers it.
        headless: bool,
    },
    Question {
        question_id: String,
        question: String,
        options: Vec<String>,
        multi_select: bool,
    },
}

/// Every thread's open prompt in two queries (not 2×N). An approval wins
/// over a question for the same thread — it blocks a tool call mid-flight.
/// Only unexpired, unanswered rows count: a settled prompt's buttons would
/// be refused anyway, and reoffering one would just advertise a dead window.
pub(crate) fn open_prompts(
    conn: &Connection,
    now: u64,
) -> Result<std::collections::HashMap<String, OpenPrompt>> {
    let mut prompts = std::collections::HashMap::new();
    // `>=`: the answer side treats `now == expires_at` as still answerable
    // (expiry there is `now > expires_at`), so the open side must agree —
    // an off-by-one here would refuse to reoffer a prompt that a tap could
    // still legitimately answer.
    let mut questions = conn.prepare(
        "SELECT thread_id, question_id, question, options_json, multi_select
         FROM pending_questions
         WHERE answer IS NULL AND expires_at >= ?1
         ORDER BY created_at ASC",
    )?;
    let rows = questions.query_map(params![to_sql_i64(now)?], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<i64>>(4)?,
        ))
    })?;
    for row in rows {
        let (thread_id, question_id, question, options_json, multi_select) = row?;
        let options = serde_json::from_str::<Vec<String>>(&options_json).unwrap_or_default();
        // ASC iteration + insert: the newest open question per thread wins.
        // A legacy NULL (row created before the column existed) is treated
        // as multi-select, i.e. NO one-tap buttons: guessing "single" could
        // submit one option of what was really a multi-select and silently
        // drop the rest. The reply path accepts both shapes either way.
        let multi_select = multi_select.map(|value| value != 0).unwrap_or(true);
        prompts.insert(
            thread_id,
            OpenPrompt::Question {
                question_id,
                question,
                options,
                multi_select,
            },
        );
    }
    let mut approvals = conn.prepare(
        "SELECT thread_id, approval_id, summary, headless
         FROM pending_approvals
         WHERE decision IS NULL AND expires_at >= ?1
         ORDER BY created_at ASC",
    )?;
    let rows = approvals.query_map(params![to_sql_i64(now)?], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<i64>>(3)?,
        ))
    })?;
    for row in rows {
        let (thread_id, approval_id, summary, headless) = row?;
        // A legacy NULL (row created before the column existed) claims
        // HEADLESS — the urgency-safe lie: "timeout denies" makes the user
        // answer promptly, and if the approval was really interactive the
        // terminal dialog still catches an ignored one. Claiming interactive
        // the other way would invite ignoring a task that dies on timeout.
        let headless = headless.map(|value| value != 0).unwrap_or(true);
        prompts.insert(
            thread_id,
            OpenPrompt::Approval {
                approval_id,
                summary,
                headless,
            },
        );
    }
    Ok(prompts)
}

/// Remember which Telegram message carries this question, so a text reply to
/// it can be recognised as the answer instead of being injected into the
/// session as a new user message.
pub(crate) fn attach_question_message_id(
    conn: &Connection,
    question_id: &str,
    message_id: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE pending_questions SET message_id = ?2 WHERE question_id = ?1",
        params![question_id, message_id],
    )?;
    Ok(())
}

pub(crate) fn question_answer(conn: &Connection, question_id: &str) -> Result<Option<String>> {
    let answer: Option<Option<String>> = conn
        .query_row(
            "SELECT answer FROM pending_questions WHERE question_id = ?1",
            params![question_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(answer.flatten())
}

/// Same one-answer-only contract as approvals: the first answer wins, and an
/// answer after the deadline is refused because the blocked hook has already
/// given up and the terminal dialog has taken over.
pub(crate) fn record_question_answer(
    conn: &Connection,
    question_id: &str,
    answer: &str,
    now: u64,
) -> Result<ApprovalAnswer> {
    let row: Option<(Option<String>, i64)> = conn
        .query_row(
            "SELECT answer, expires_at FROM pending_questions WHERE question_id = ?1",
            params![question_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((existing, expires_at)) = row else {
        return Ok(ApprovalAnswer::Unknown);
    };
    match existing.as_deref() {
        Some(QUESTION_EXPIRED) => return Ok(ApprovalAnswer::Expired),
        Some(_) => return Ok(ApprovalAnswer::AlreadyAnswered),
        None => {}
    }
    if timestamp_to_millis(now) > timestamp_to_millis(from_sql_i64(expires_at)?) {
        conn.execute(
            "UPDATE pending_questions SET answer = ?2, answered_at = ?3
             WHERE question_id = ?1 AND answer IS NULL",
            params![question_id, QUESTION_EXPIRED, to_sql_i64(now)?],
        )?;
        return Ok(ApprovalAnswer::Expired);
    }
    let changed = conn.execute(
        "UPDATE pending_questions SET answer = ?2, answered_at = ?3
         WHERE question_id = ?1 AND answer IS NULL",
        params![question_id, answer, to_sql_i64(now)?],
    )?;
    if changed > 0 {
        return Ok(ApprovalAnswer::Recorded);
    }
    Ok(match question_answer(conn, question_id)?.as_deref() {
        Some(QUESTION_EXPIRED) => ApprovalAnswer::Expired,
        Some(_) => ApprovalAnswer::AlreadyAnswered,
        None => ApprovalAnswer::Unknown,
    })
}

/// Sentinel stored in `answer` for a question nobody answered in time.
pub(crate) const QUESTION_EXPIRED: &str = "\u{0}expired";

/// Atomic counterpart of `expire_or_take_decision` for questions.
pub(crate) fn expire_or_take_answer(
    conn: &Connection,
    question_id: &str,
    now: u64,
) -> Result<Option<String>> {
    let changed = conn.execute(
        "UPDATE pending_questions SET answer = ?2, answered_at = ?3
         WHERE question_id = ?1 AND answer IS NULL",
        params![question_id, QUESTION_EXPIRED, to_sql_i64(now)?],
    )?;
    if changed > 0 {
        return Ok(None);
    }
    Ok(question_answer(conn, question_id)?.filter(|answer| answer != QUESTION_EXPIRED))
}

fn approval_auto_allow_key(thread_id: &str, tool_name: &str) -> String {
    format!("approval_auto_allow:{thread_id}:{tool_name}")
}

/// "Allow this tool for the rest of the session" — without it an agent doing
/// many Bash calls would need one Telegram tap per call.
pub(crate) fn set_approval_auto_allow(
    conn: &Connection,
    thread_id: &str,
    tool_name: &str,
    now: u64,
) -> Result<()> {
    set_setting_text(
        conn,
        &approval_auto_allow_key(thread_id, tool_name),
        &now.to_string(),
    )
}

/// Fill in the process identity of a pre-registered turn once the spawn has
/// happened. The row is created before the spawn (see
/// `spawn_registered_headless`); this second step only adds what cannot be
/// known before `spawn` returns.
///
/// Scoped to `status = 'running'` and returning the row count, so the caller
/// can tell BOTH failure shapes apart from success: a lost write, and a
/// daemon that already settled the turn in the meantime (a slow spawn can
/// outlive the 10s crash grace). Updating a settled row would misreport a
/// turn the daemon has declared dead as successfully recorded.
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_bridge_turn_spawn(
    conn: &Connection,
    turn_id: &str,
    pid: Option<u32>,
    proc_start: Option<&str>,
    proc_exe: Option<&str>,
    pgid: Option<u32>,
    proc_start_ticks: Option<&str>,
    boot_id: Option<&str>,
) -> Result<usize> {
    let rows = conn.execute(
        "UPDATE bridge_turns
         SET pid = ?2, proc_start = ?3, proc_exe = ?4, pgid = ?5,
             proc_start_ticks = ?6, boot_id = ?7
         WHERE turn_id = ?1 AND status = 'running'",
        params![
            turn_id,
            pid.map(i64::from),
            proc_start,
            proc_exe,
            pgid.map(i64::from),
            proc_start_ticks,
            boot_id
        ],
    )?;
    Ok(rows)
}

/// Status of one specific turn, by the id the turn token names.
pub(crate) fn bridge_turn_status(conn: &Connection, turn_id: &str) -> Result<Option<String>> {
    use rusqlite::OptionalExtension;
    Ok(conn
        .query_row(
            "SELECT status FROM bridge_turns WHERE turn_id = ?1",
            params![turn_id],
            |row| row.get(0),
        )
        .optional()?)
}

pub(crate) fn approval_auto_allowed(
    conn: &Connection,
    thread_id: &str,
    tool_name: &str,
) -> Result<bool> {
    Ok(get_setting_text(conn, &approval_auto_allow_key(thread_id, tool_name))?.is_some())
}

pub(crate) fn telegram_current_project_key(chat_id: &str, user_id: Option<&str>) -> String {
    format!(
        "telegram_current_project:{}:{}",
        chat_id,
        user_id.unwrap_or("*")
    )
}

pub(crate) fn get_telegram_current_project_id(
    conn: &Connection,
    chat_id: &str,
    user_id: Option<&str>,
) -> Result<Option<String>> {
    get_setting_text(conn, &telegram_current_project_key(chat_id, user_id))
}

pub(crate) fn set_telegram_current_project_id(
    conn: &Connection,
    chat_id: &str,
    user_id: Option<&str>,
    project_id: &str,
) -> Result<()> {
    set_setting_text(
        conn,
        &telegram_current_project_key(chat_id, user_id),
        project_id,
    )
}

#[allow(dead_code)]
pub(crate) fn unarchive_thread_result(
    conn: &Connection,
    thread_id: &str,
    dry_run: bool,
    now: u64,
    live_result: Option<Value>,
) -> Result<Value> {
    if dry_run {
        return Ok(json!({
            "ok": true,
            "action": "unarchive",
            "dryRun": true,
            "results": [{ "threadId": thread_id, "status": "would_unarchive" }]
        }));
    }

    let result = live_result.unwrap_or_else(|| json!({ "threadId": thread_id, "ok": true }));
    record_action(
        conn,
        thread_id,
        "unarchive",
        json!({ "result": result, "unarchivedAt": now }),
        now,
    )?;
    Ok(json!({
        "ok": true,
        "action": "unarchive",
        "dryRun": false,
        "results": [{ "threadId": thread_id, "status": "unarchived", "result": result }]
    }))
}

pub(crate) fn telegram_inbound_processed(
    conn: &Connection,
    bot_id: &str,
    update_id: i64,
) -> Result<bool> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT update_id FROM telegram_inbound_log WHERE bot_id = ?1 AND update_id = ?2",
            params![bot_id, update_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(exists.is_some())
}

pub(crate) fn record_telegram_inbound_processed(
    conn: &Connection,
    bot_id: &str,
    update_id: i64,
    update_kind: &str,
    result: &Value,
    context: TelegramInboundLogContext<'_>,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO telegram_inbound_log(
            bot_id,
            update_id,
            update_kind,
            result_json,
            processed_at,
            thread_id,
            route_message_id,
            result_action,
            backend_transport,
            backend_pid
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            bot_id,
            update_id,
            update_kind,
            serde_json::to_string(result)?,
            to_sql_i64(now)?,
            context.thread_id,
            context.route_message_id,
            context.result_action,
            context.backend_transport,
            context.backend_pid.map(i64::from),
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_telegram_message_route(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
    thread_id: &str,
    event_id: &str,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO telegram_message_routes(chat_id, message_id, thread_id, event_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![chat_id, message_id, thread_id, event_id, to_sql_i64(now)?],
    )?;
    Ok(())
}

pub(crate) fn insert_telegram_callback_route(
    conn: &Connection,
    route: &TelegramCallbackRoute,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO telegram_callback_routes(callback_id, chat_id, message_id, thread_id, action, created_at, used_at, approval_id, question_id, answer)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, ?9)",
        params![
            route.callback_id,
            route.chat_id,
            route.message_id,
            route.thread_id,
            route.action.as_str(),
            to_sql_i64(now)?,
            route.approval_id,
            route.question_id,
            route.answer
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_telegram_command_route(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
    kind: TelegramCommandRouteKind,
    payload: Option<&Value>,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO telegram_command_routes(chat_id, message_id, command, payload_json, created_at, used_at)
         VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
        params![
            chat_id,
            message_id,
            kind.as_str(),
            payload.map(serde_json::to_string).transpose()?,
            to_sql_i64(now)?
        ],
    )?;
    Ok(())
}

pub(crate) fn update_telegram_callback_message_id(
    conn: &Connection,
    callback_id: &str,
    message_id: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE telegram_callback_routes SET message_id = ?2 WHERE callback_id = ?1",
        params![callback_id, message_id],
    )?;
    Ok(())
}

pub(crate) fn mark_telegram_callback_route_used(
    conn: &Connection,
    callback_id: &str,
    now: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE telegram_callback_routes SET used_at = ?2 WHERE callback_id = ?1 AND used_at IS NULL",
        params![callback_id, to_sql_i64(now)?],
    )?;
    Ok(())
}

pub(crate) fn lookup_telegram_message_route(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
) -> Result<Option<String>> {
    conn.query_row(
        "SELECT thread_id FROM telegram_message_routes WHERE chat_id = ?1 AND message_id = ?2",
        params![chat_id, message_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub(crate) fn lookup_telegram_command_route(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
) -> Result<Option<(TelegramCommandRouteKind, Option<Value>)>> {
    let command = conn
        .query_row(
            "SELECT command, payload_json FROM telegram_command_routes
             WHERE chat_id = ?1 AND message_id = ?2 AND used_at IS NULL",
            params![chat_id, message_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    match command {
        Some((command, payload_json)) => {
            Ok(TelegramCommandRouteKind::from_str(&command).map(|kind| {
                (
                    kind,
                    payload_json.and_then(|raw| serde_json::from_str::<Value>(&raw).ok()),
                )
            }))
        }
        None => Ok(None),
    }
}

pub(crate) fn mark_telegram_command_route_used(
    conn: &Connection,
    chat_id: &str,
    message_id: i64,
    now: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE telegram_command_routes SET used_at = ?3 WHERE chat_id = ?1 AND message_id = ?2",
        params![chat_id, message_id, to_sql_i64(now)?],
    )?;
    Ok(())
}

/// `origin` records why the event was enqueued: "away" (away-mode
/// notification about a local session) or "bridge" (answer to a turn started
/// from Telegram). /back clears only the former.
pub(crate) fn enqueue_outbound_event(
    conn: &Connection,
    event: &Value,
    now: u64,
    origin: &str,
) -> Result<bool> {
    let event_id = crate::notification_event_id(event);
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("bridge_event");
    let thread_id = crate::event_thread_id(event);
    let inserted = conn.execute(
        "INSERT OR IGNORE INTO outbound_events(event_id, event_type, thread_id, payload_json, status, attempts, next_attempt_at, last_error, created_at, delivered_at, origin)
         VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5, NULL, ?5, NULL, ?6)",
        params![
            event_id,
            event_type,
            thread_id,
            serde_json::to_string(event)?,
            to_sql_i64(now)?,
            origin
        ],
    )?;
    Ok(inserted > 0)
}

pub(crate) fn pending_outbound_count(conn: &Connection) -> Result<u64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM outbound_events WHERE status != 'delivered'",
        [],
        |row| row.get(0),
    )?;
    from_sql_i64(count)
}

/// /back clears the away-notification backlog only. Answers to turns the user
/// started from Telegram (origin 'bridge') stay queued: they were explicitly
/// requested and must survive a delivery failure followed by /back.
pub(crate) fn clear_pending_outbound_events(conn: &Connection) -> Result<usize> {
    let deleted = conn.execute(
        "DELETE FROM outbound_events WHERE status != 'delivered' AND origin = 'away'",
        [],
    )?;
    Ok(deleted)
}

pub(crate) fn transport_delivery_exists(
    conn: &Connection,
    event_id: &str,
    transport: &str,
) -> Result<bool> {
    let exists: Option<String> = conn
        .query_row(
            "SELECT event_id FROM transport_delivery_log WHERE event_id = ?1 AND transport = ?2",
            params![event_id, transport],
            |row| row.get(0),
        )
        .optional()?;
    Ok(exists.is_some())
}

pub(crate) fn record_transport_delivery(
    conn: &Connection,
    event_id: &str,
    transport: &str,
    result: &Value,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO transport_delivery_log(event_id, transport, result_json, delivered_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            event_id,
            transport,
            serde_json::to_string(result)?,
            to_sql_i64(now)?
        ],
    )?;
    Ok(())
}

fn retry_delay_ms(attempts: u64) -> u64 {
    let exponent = attempts.saturating_sub(1).min(8);
    (1u64 << exponent) * 1000
}

pub(crate) fn deliver_due_outbound_events<F>(
    conn: &Connection,
    now: u64,
    limit: usize,
    deadline: Option<Instant>,
    mut sender: F,
) -> Result<OutboxDeliverySummary>
where
    F: FnMut(&Value) -> Result<Value>,
{
    let rows = {
        let mut stmt = conn.prepare(
            "SELECT event_id, payload_json, attempts
             FROM outbound_events
             WHERE status != 'delivered' AND next_attempt_at <= ?1
             ORDER BY created_at ASC, event_id ASC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![to_sql_i64(now)?, limit as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };

    let mut summary = OutboxDeliverySummary {
        attempted: 0,
        delivered: 0,
        failed: 0,
    };
    for (event_id, payload_json, attempts) in rows {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        summary.attempted += 1;
        let event: Value = serde_json::from_str(&payload_json)
            .with_context(|| format!("outbound event {event_id} contains invalid JSON"))?;
        match sender(&event) {
            Ok(_) => {
                conn.execute(
                    "UPDATE outbound_events
                     SET status = 'delivered', attempts = attempts + 1, delivered_at = ?2, last_error = NULL
                     WHERE event_id = ?1",
                    params![event_id, to_sql_i64(now)?],
                )?;
                summary.delivered += 1;
            }
            Err(error) => {
                let next_attempts = from_sql_i64(attempts)?.saturating_add(1);
                let next_attempt_at = now.saturating_add(retry_delay_ms(next_attempts));
                conn.execute(
                    "UPDATE outbound_events
                     SET status = 'failed', attempts = ?2, next_attempt_at = ?3, last_error = ?4
                     WHERE event_id = ?1",
                    params![
                        event_id,
                        to_sql_i64(next_attempts)?,
                        to_sql_i64(next_attempt_at)?,
                        format!("{error:#}")
                    ],
                )?;
                summary.failed += 1;
            }
        }
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use serde_json::json;

    use crate::telegram::{
        extract_telegram_callback_route, extract_telegram_command_prompt_reply,
        extract_telegram_reply_route, telegram_bot_id,
    };
    use crate::{importable_projects_from_observed, set_away_mode, TelegramConfig};

    fn snapshot_fixture(
        thread_id: &str,
        cwd: &str,
        updated_at: u64,
        status_type: &str,
        status_flags: Vec<&str>,
        last_turn_status: Option<&str>,
    ) -> BridgeThreadSnapshot {
        let status_flags_vec = status_flags
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let pending_prompt = if status_flags_vec
            .iter()
            .any(|flag| flag == "waitingOnApproval")
        {
            Some(PendingPrompt {
                prompt_id: format!("approval:{thread_id}"),
                kind: "approval".to_string(),
                status: "Needs approval".to_string(),
                question: Some(format!("preview for {thread_id}")),
                transcript_bytes: None,
            })
        } else if status_flags_vec
            .iter()
            .any(|flag| flag == "waitingOnUserInput" || flag == "waitingOnInput")
        {
            Some(PendingPrompt {
                prompt_id: format!("reply:{thread_id}"),
                kind: "reply".to_string(),
                status: "Needs input".to_string(),
                question: Some(format!("preview for {thread_id}")),
                transcript_bytes: None,
            })
        } else {
            None
        };
        BridgeThreadSnapshot {
            thread_id: thread_id.to_string(),
            name: None,
            cwd: Some(cwd.to_string()),
            updated_at: Some(updated_at),
            status_type: status_type.to_string(),
            status_flags: status_flags_vec.clone(),
            last_turn_status: last_turn_status.map(|value| value.to_string()),
            last_preview: Some(format!("preview for {thread_id}")),
            pending_prompt,
            event_uid: None,
        }
    }

    #[test]
    fn state_schema_matches_ts_bridge_contract() {
        let conn = create_state_db_in_memory().expect("db");
        let tables = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .expect("prepare")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("tables");
        assert!(tables.contains(&"delivery_log".to_string()));
        assert!(tables.contains(&"outbound_events".to_string()));

        let enabled = set_away_mode(&conn, true, 1234).expect("enable away");
        assert_eq!(enabled["away"], true);
        assert_eq!(
            get_setting_text(&conn, "away")
                .expect("away setting")
                .as_deref(),
            Some("true")
        );
        assert_eq!(
            get_setting_text(&conn, "away_mode").expect("legacy away setting"),
            None
        );
    }

    #[test]
    fn create_state_db_migrates_legacy_threads_cache_columns() {
        let path =
            std::env::temp_dir().join(format!("tinyctb-state-migrate-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).expect("open legacy db");
            conn.execute_batch(
                "
                CREATE TABLE threads_cache (
                    thread_id TEXT PRIMARY KEY,
                    name TEXT,
                    cwd TEXT,
                    source TEXT,
                    status_type TEXT NOT NULL,
                    status_flags_json TEXT NOT NULL,
                    updated_at INTEGER,
                    last_seen_at INTEGER NOT NULL
                );
                ",
            )
            .expect("create legacy table");
        }

        let conn = create_state_db(&path).expect("migrated db");
        let columns = table_columns(&conn, "threads_cache").expect("columns");
        assert!(columns.contains(&"last_turn_status".to_string()));
        assert!(columns.contains(&"last_preview".to_string()));
        let _ = std::fs::remove_file(path);
    }

    /// An install that already shipped the counting shape of
    /// `live_injections` must be rebuilt into per-injection rows — otherwise
    /// every query hits "no such column: claimed_at" after the upgrade.
    /// Pruning must normalize timestamps the same way claiming does: a
    /// second-granularity row that was just created must survive, and an old
    /// one must go, in BOTH units.
    #[test]
    fn prune_live_injections_handles_second_and_millisecond_timestamps() {
        let conn = create_state_db_in_memory().expect("db");
        let retention_ms: u64 = 30 * 24 * 60 * 60 * 1000;
        // "Now" as a realistic epoch in both units.
        let now_ms: u64 = 1_786_400_000_000;
        let now_secs: u64 = now_ms / 1000;

        let insert = |thread: &str, injected_at: u64, claimed_at: Option<u64>| {
            conn.execute(
                "INSERT INTO live_injections(thread_id, injected_at, claimed_at)
                 VALUES (?1, ?2, ?3)",
                params![thread, injected_at as i64, claimed_at.map(|v| v as i64)],
            )
            .expect("insert");
        };
        // Fresh, unclaimed — must survive in both units.
        insert("fresh_ms", now_ms - 60_000, None);
        insert("fresh_secs", now_secs - 60, None);
        // Just claimed — must survive in both units.
        insert("claimed_ms", now_ms - 60_000, Some(now_ms - 60_000));
        insert("claimed_secs", now_secs - 60, Some(now_secs - 60));
        // Unclaimed past the TTL, and claimed past retention — must go.
        insert("stale_ms", now_ms - LIVE_INJECTION_TTL_MS - 60_000, None);
        insert(
            "stale_secs",
            now_secs - (LIVE_INJECTION_TTL_MS / 1000) - 60,
            None,
        );
        insert(
            "old_claim_ms",
            now_ms - retention_ms - 120_000,
            Some(now_ms - retention_ms - 60_000),
        );

        let removed = prune_live_injections(&conn, now_ms, retention_ms).expect("prune");
        assert_eq!(removed, 3, "only the expired rows are pruned");

        let surviving: Vec<String> = conn
            .prepare("SELECT thread_id FROM live_injections ORDER BY thread_id")
            .expect("stmt")
            .query_map([], |row| row.get(0))
            .expect("rows")
            .collect::<rusqlite::Result<_>>()
            .expect("collect");
        assert_eq!(
            surviving,
            vec![
                "claimed_ms".to_string(),
                "claimed_secs".to_string(),
                "fresh_ms".to_string(),
                "fresh_secs".to_string()
            ],
            "a recent second-granularity row must not be mistaken for an ancient one"
        );
    }

    /// The dialog-message index must not grow for the life of the install.
    #[test]
    fn prune_removes_only_old_dialog_messages() {
        let conn = create_state_db_in_memory().expect("db");
        let retention_ms: u64 = 30 * 24 * 60 * 60 * 1000;
        let now: u64 = 1_786_400_000_000;
        record_dialog_message(
            &conn,
            "456",
            1,
            "question",
            "q-old",
            now - retention_ms - 60_000,
        )
        .expect("old");
        record_dialog_message(&conn, "456", 2, "approval", "ap-new", now - 60_000).expect("new");

        prune_state_logs(&conn, now).expect("prune");

        assert!(
            dialog_for_message(&conn, "456", 1)
                .expect("lookup")
                .is_none(),
            "an old dialog message is pruned"
        );
        assert_eq!(
            dialog_for_message(&conn, "456", 2).expect("lookup"),
            Some(("approval".to_string(), "ap-new".to_string())),
            "a recent one is kept so replies still resolve"
        );
    }

    #[test]
    fn create_state_db_migrates_legacy_live_injections_table() {
        let path = std::env::temp_dir().join(format!(
            "tinyctb-live-injections-migrate-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).expect("open legacy db");
            conn.execute_batch(
                "CREATE TABLE live_injections (
                    thread_id TEXT PRIMARY KEY,
                    injected_at INTEGER NOT NULL,
                    owed INTEGER NOT NULL
                 );
                 INSERT INTO live_injections(thread_id, injected_at, owed)
                 VALUES ('thr_owed', 1000, 2);",
            )
            .expect("seed legacy table");
        }

        let conn = create_state_db(&path).expect("migrated db");
        let columns = table_columns(&conn, "live_injections").expect("columns");
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"claimed_at".to_string()));
        assert!(!columns.contains(&"owed".to_string()));

        // Both outstanding answers survive as individual debts, and the new
        // API works against the migrated table.
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM live_injections WHERE thread_id = 'thr_owed'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(rows, 2, "each owed answer becomes one unclaimed row");
        assert!(
            live_injection_pending(&conn, "thr_owed", Some(2000), 2000).expect("pending"),
            "migrated debts remain claimable"
        );
        consume_live_injection(&conn, "thr_owed", Some(2000), 2000).expect("consume");
        assert!(
            live_injection_pending(&conn, "thr_owed", Some(2000), 2000).expect("second"),
            "only one debt is settled per completion"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn create_state_db_migrates_legacy_telegram_inbound_log_columns() {
        let path =
            std::env::temp_dir().join(format!("tinyctb-inbound-migrate-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).expect("open legacy db");
            conn.execute_batch(
                "
                CREATE TABLE telegram_inbound_log (
                    bot_id TEXT NOT NULL,
                    update_id INTEGER NOT NULL,
                    update_kind TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    processed_at INTEGER NOT NULL,
                    PRIMARY KEY(bot_id, update_id)
                );
                ",
            )
            .expect("create legacy inbound log");
        }

        let conn = create_state_db(&path).expect("migrated db");
        let columns = table_columns(&conn, "telegram_inbound_log").expect("columns");
        assert!(columns.contains(&"thread_id".to_string()));
        assert!(columns.contains(&"route_message_id".to_string()));
        assert!(columns.contains(&"result_action".to_string()));
        assert!(columns.contains(&"backend_transport".to_string()));
        assert!(columns.contains(&"backend_pid".to_string()));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn telegram_command_prompt_routes_map_reply_to_new_thread_prompt() {
        let conn = create_state_db_in_memory().expect("db");
        insert_telegram_command_route(
            &conn,
            "456",
            222,
            TelegramCommandRouteKind::NewThread,
            Some(&json!({ "projectId": "bridge" })),
            1000,
        )
        .expect("insert command route");

        let route = extract_telegram_command_prompt_reply(
            &conn,
            &json!({
                "chat": { "id": 456 },
                "from": { "id": 789 },
                "text": "Build the mobile proof report",
                "reply_to_message": { "message_id": 222 }
            }),
            &TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            },
        )
        .expect("extract command prompt reply")
        .expect("route");

        assert_eq!(route.kind, TelegramCommandRouteKind::NewThread);
        assert_eq!(route.message, "Build the mobile proof report");
        assert_eq!(route.project_id.as_deref(), Some("bridge"));
    }

    #[test]
    fn telegram_routes_map_message_replies_to_backend_threads() {
        let conn = create_state_db_in_memory().expect("db");
        insert_telegram_message_route(&conn, "456", 111, "thr_1", "event_1", 1000)
            .expect("insert route");

        let routed = extract_telegram_reply_route(
            &conn,
            &json!({
                "message_id": 222,
                "from": { "id": 789 },
                "chat": { "id": 456 },
                "text": "continue with the safer patch",
                "reply_to_message": { "message_id": 111 }
            }),
            &TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            },
        )
        .expect("route")
        .expect("reply should route");

        assert_eq!(routed.thread_id, "thr_1");
        assert_eq!(routed.message, "continue with the safer patch");
    }

    #[test]
    fn telegram_callback_routes_map_buttons_to_approvals() {
        let conn = create_state_db_in_memory().expect("db");
        insert_telegram_callback_route(
            &conn,
            &TelegramCallbackRoute {
                callback_id: "cb_1".to_string(),
                chat_id: "456".to_string(),
                message_id: None,
                thread_id: "thr_approval".to_string(),
                action: TelegramCallbackAction::Deny,
                approval_id: None,
                question_id: None,
                answer: None,
            },
            1000,
        )
        .expect("insert callback");

        let routed = extract_telegram_callback_route(
            &conn,
            &json!({
                "id": "callback-query-id",
                "from": { "id": 789 },
                "message": {
                    "message_id": 111,
                    "chat": { "id": 456 }
                },
                "data": "claude:cb_1"
            }),
            &TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            },
        )
        .expect("route");
        let crate::telegram::TelegramCallbackLookup::Route(routed) = routed else {
            panic!("callback should route");
        };

        assert_eq!(routed.thread_id, "thr_approval");
        assert_eq!(routed.action, TelegramCallbackAction::Deny);
        assert_eq!(routed.callback_query_id, "callback-query-id");
    }

    #[test]
    fn telegram_inbound_log_dedupes_processed_updates_per_bot() {
        let conn = create_state_db_in_memory().expect("db");
        let bot_id = telegram_bot_id("123:secret");

        assert!(
            !telegram_inbound_processed(&conn, &bot_id, 42).expect("lookup"),
            "update should not start processed"
        );
        record_telegram_inbound_processed(
            &conn,
            &bot_id,
            42,
            "message",
            &json!({ "threadId": "thr_1" }),
            TelegramInboundLogContext {
                thread_id: Some("thr_1"),
                route_message_id: Some(111),
                result_action: Some("telegram_reply"),
                backend_transport: Some("spawned_stdio"),
                backend_pid: Some(4242),
            },
            1000,
        )
        .expect("record inbound update");

        assert!(
            telegram_inbound_processed(&conn, &bot_id, 42).expect("lookup"),
            "recorded update should not be processed again"
        );
        assert!(
            !telegram_inbound_processed(&conn, &telegram_bot_id("456:other"), 42).expect("lookup"),
            "same update id from a different bot token hash must remain independent"
        );

        let row = conn
            .query_row(
                "SELECT thread_id, route_message_id, result_action, backend_transport, backend_pid
                 FROM telegram_inbound_log
                 WHERE bot_id = ?1 AND update_id = ?2",
                params![bot_id, 42],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .expect("structured inbound row");
        assert_eq!(row.0.as_deref(), Some("thr_1"));
        assert_eq!(row.1, Some(111));
        assert_eq!(row.2.as_deref(), Some("telegram_reply"));
        assert_eq!(row.3.as_deref(), Some("spawned_stdio"));
        assert_eq!(row.4, Some(4242));
    }

    #[test]
    fn persists_snapshots_and_actions_in_sqlite_state() {
        let conn = create_state_db_in_memory().expect("db");
        let waiting = snapshot_fixture(
            "thr_wait",
            "/tmp/project-wait",
            1200,
            "active",
            vec!["waitingOnUserInput"],
            Some("in_progress"),
        );
        let done = snapshot_fixture(
            "thr_done",
            "/tmp/project-done",
            1300,
            "notLoaded",
            vec![],
            Some("completed"),
        );

        upsert_thread_snapshot(&conn, &waiting, 2000).expect("upsert waiting");
        upsert_thread_snapshot(&conn, &done, 2000).expect("upsert done");
        record_action(
            &conn,
            "thr_wait",
            "reply",
            json!({"message": "On it"}),
            2100,
        )
        .expect("record action");

        let waiting_rows = list_waiting_from_db(&conn, None, 10).expect("waiting rows");
        assert_eq!(waiting_rows.threads.len(), 1);
        assert_eq!(waiting_rows.threads[0].thread_id, "thr_wait");
        assert_eq!(waiting_rows.threads[0].prompt.kind, "reply");

        let history = get_thread_history(&conn, "thr_wait", 10).expect("history");
        assert_eq!(history[0].action_type, "reply");
    }

    #[test]
    fn list_recent_thread_snapshots_orders_by_latest_activity() {
        let conn = create_state_db_in_memory().expect("db");
        let older = snapshot_fixture(
            "thr_old",
            "/tmp/project-old",
            1200,
            "notLoaded",
            vec![],
            Some("completed"),
        );
        let newer = snapshot_fixture(
            "thr_new",
            "/tmp/project-new",
            2400,
            "active",
            vec!["waitingOnUserInput"],
            Some("in_progress"),
        );

        upsert_thread_snapshot(&conn, &older, 1200).expect("upsert older");
        upsert_thread_snapshot(&conn, &newer, 2400).expect("upsert newer");

        let recent = list_recent_thread_snapshots_from_db(&conn, 1).expect("recent threads");

        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].thread_id, "thr_new");
        assert_eq!(
            recent[0]
                .pending_prompt
                .as_ref()
                .expect("pending prompt")
                .kind,
            "reply"
        );
    }

    #[test]
    fn project_current_selection_round_trips_per_identity() {
        let conn = create_state_db_in_memory().expect("db");
        set_telegram_current_project_id(&conn, "chat-1", Some("user-1"), "bridge")
            .expect("set current");

        assert_eq!(
            get_telegram_current_project_id(&conn, "chat-1", Some("user-1"))
                .expect("get current")
                .as_deref(),
            Some("bridge")
        );
        assert_eq!(
            get_telegram_current_project_id(&conn, "chat-1", Some("user-2"))
                .expect("get other")
                .as_deref(),
            None
        );
    }

    #[test]
    fn project_import_suggests_unique_ids_from_observed_workspaces() {
        let conn = create_state_db_in_memory().expect("db");
        let alpha = snapshot_fixture(
            "thr_alpha",
            "/Users/hanifcarroll/projects/client-a/app",
            2000,
            "active",
            vec![],
            Some("in_progress"),
        );
        let beta = snapshot_fixture(
            "thr_beta",
            "/Users/hanifcarroll/projects/client-b/app",
            3000,
            "active",
            vec![],
            Some("in_progress"),
        );
        upsert_thread_snapshot(&conn, &alpha, 2000).expect("upsert alpha");
        upsert_thread_snapshot(&conn, &beta, 3000).expect("upsert beta");

        let imported = importable_projects_from_observed(
            &observed_workspaces_from_db(&conn, 10).expect("observed"),
            &[],
        );

        assert_eq!(imported.len(), 2);
        assert_eq!(imported[0].id, "app");
        assert_eq!(imported[1].id, "app-2");
    }

    #[test]
    fn live_backend_status_path_uses_bridge_state_directory() {
        let _guard = test_env_lock().lock().expect("test env lock");
        let home =
            std::env::temp_dir().join(format!("tinyctb-live-state-path-{}", std::process::id()));
        let _ = fs::remove_dir_all(&home);
        fs::create_dir_all(&home).expect("create temp home");
        let previous_home = std::env::var("HOME").ok();
        let previous_state_dir = std::env::var("TINYCTB_STATE_DIR").ok();
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::set_var("HOME", &home);

        let path = live_backend_status_path().expect("live backend status path");

        if let Some(previous_home) = previous_home {
            std::env::set_var("HOME", previous_home);
        } else {
            std::env::remove_var("HOME");
        }
        if let Some(previous_state_dir) = previous_state_dir {
            std::env::set_var("TINYCTB_STATE_DIR", previous_state_dir);
        } else {
            std::env::remove_var("TINYCTB_STATE_DIR");
        }
        let _ = fs::remove_dir_all(&home);

        assert!(path.ends_with(".tinyctb/live-backend.json"));
    }

    #[test]
    fn prune_state_logs_removes_only_rows_older_than_retention() {
        let conn = create_state_db_in_memory().expect("db");
        let retention_ms: u64 = 30 * 24 * 60 * 60 * 1000;
        let now = retention_ms + 2000;
        record_telegram_inbound_processed(
            &conn,
            "bot",
            1,
            "message_ignored",
            &json!({}),
            TelegramInboundLogContext::default(),
            1000,
        )
        .expect("old inbound");
        record_telegram_inbound_processed(
            &conn,
            "bot",
            2,
            "message_ignored",
            &json!({}),
            TelegramInboundLogContext::default(),
            now,
        )
        .expect("recent inbound");
        conn.execute(
            "INSERT INTO actions_log(thread_id, action_type, payload_json, created_at)
             VALUES ('thr_1', 'x', '{}', 1000)",
            [],
        )
        .expect("old action");
        conn.execute(
            "INSERT INTO actions_log(thread_id, action_type, payload_json, created_at)
             VALUES ('thr_1', 'x', '{}', ?1)",
            params![to_sql_i64(now).expect("recent timestamp")],
        )
        .expect("recent action");

        let removed = prune_state_logs(&conn, now).expect("prune");
        assert_eq!(removed, 2);
        assert!(!telegram_inbound_processed(&conn, "bot", 1).expect("old inbound removed"));
        assert!(telegram_inbound_processed(&conn, "bot", 2).expect("recent inbound kept"));
        let recent_actions: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM actions_log WHERE created_at >= ?1",
                params![to_sql_i64(now).expect("cutoff")],
                |row| row.get(0),
            )
            .expect("recent actions count");
        assert_eq!(recent_actions, 1);
    }

    #[test]
    fn outbound_delivery_deadline_skips_pending_events() {
        let conn = create_state_db_in_memory().expect("db");
        enqueue_outbound_event(
            &conn,
            &json!({
                "type": "thread_waiting",
                "threadId": "thr_1",
                "updatedAt": 42
            }),
            1000,
            "away",
        )
        .expect("enqueue");

        let summary = deliver_due_outbound_events(
            &conn,
            2000,
            10,
            Some(Instant::now() - Duration::from_secs(1)),
            |_| Ok(json!({ "ok": true })),
        )
        .expect("deadline summary");

        assert_eq!(summary.attempted, 0);
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);
    }
}
