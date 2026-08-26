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
    /// The RAW `notification_type` from the hook payload (`idle_prompt`,
    /// `agent_needs_input`, ...). The `kind` field folds several of these
    /// into "reply" for rendering; consumers that must distinguish a mere
    /// idle reminder from a genuine question (idle-echo suppression) read
    /// this instead. `None` on rows predating the column or on prompts not
    /// born from a Notification hook.
    pub(crate) notification_type: Option<String>,
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
    // A stop operation only matters while its Telegram update could still be
    // redelivered — measured in minutes; a month is generous.
    let stop_ops = conn.execute(
        "DELETE FROM stop_operations WHERE created_at < ?1",
        params![sql_cutoff],
    )?;
    // Settled turns are history; running rows are load-bearing (liveness,
    // crash detection) and are never pruned regardless of age.
    let turns = conn.execute(
        "DELETE FROM bridge_turns
         WHERE status NOT IN ('running', 'stopping')
           AND cleanup_pending = 0
           AND COALESCE(completed_at, started_at) < ?1",
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
    // Delivered pushes are history; a row still awaiting delivery is only
    // kept while its retry schedule is live — one whose next attempt is
    // itself a whole retention period old is abandoned, not pending.
    let outbound = conn.execute(
        "DELETE FROM outbound_events
         WHERE created_at < ?1 AND (status = 'delivered' OR next_attempt_at < ?1)",
        params![sql_cutoff],
    )?;
    // The transport log is the idempotence ledger AND (since it carries the
    // authoritative send time) the delivery-order record. A row may only go
    // once its outbound event is gone: while that event still exists,
    // dropping the log entry would re-send a message the user already read.
    // Deleting outbound rows first, in this same function, is what makes the
    // NOT EXISTS below safe — an orphan here is a send whose event has
    // already aged out, so nothing can resurrect it.
    let transports = conn.execute(
        "DELETE FROM transport_delivery_log
         WHERE delivered_at < ?1
           AND NOT EXISTS (
             SELECT 1 FROM outbound_events
             WHERE outbound_events.event_id = transport_delivery_log.event_id
           )",
        params![sql_cutoff],
    )?;
    // Telegram routes exist so a reply/tap on an old message can find its
    // thread; past retention the message is unanswerable anyway.
    let routes = conn.execute(
        "DELETE FROM telegram_message_routes WHERE created_at < ?1",
        params![sql_cutoff],
    )?;
    let callbacks = conn.execute(
        "DELETE FROM telegram_callback_routes WHERE created_at < ?1",
        params![sql_cutoff],
    )?;
    let commands = conn.execute(
        "DELETE FROM telegram_command_routes WHERE created_at < ?1",
        params![sql_cutoff],
    )?;
    Ok(inbound
        + actions
        + injections
        + dialogs
        + stop_ops
        + turns
        + approvals
        + questions
        + outbound
        + transports
        + routes
        + callbacks
        + commands)
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
    // The clock the one-time backfill below clamps against. Taken here so a
    // database that cannot read a clock still opens.
    let migration_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(u64::MAX);
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
        -- One row per /stop COMMAND (keyed by its Telegram message), holding
        -- its FIRST interpretation — the resolved turns, or the terminal
        -- reply of an early exit (empty, ambiguous, too short). A
        -- redelivered update replays against this record instead of
        -- reinterpreting the command over whatever happens to be running by
        -- then: nothing-to-stop must never become stop-everything just
        -- because new turns started before the redelivery.
        CREATE TABLE IF NOT EXISTS stop_operations (
          operation_id TEXT PRIMARY KEY,
          kind TEXT NOT NULL DEFAULT 'turns',
          turns_json TEXT NOT NULL,
          reply TEXT,
          created_at INTEGER NOT NULL
        );
        ",
    )?;
    ensure_column(
        conn,
        "stop_operations",
        "kind",
        "TEXT NOT NULL DEFAULT 'turns'",
    )?;
    ensure_column(conn, "stop_operations", "reply", "TEXT")?;
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
    // WHERE the stored `updated_at` came from. Knowing only the arriving
    // value's provenance is not enough to decide whether it may replace what
    // is there: a measurement must be able to correct a GUESS downward, and
    // must not drag a real OBSERVATION backwards. Rows written before this
    // column existed default to 0 — treated as a guess, which is what the
    // damaged ones are; a sound one is simply re-established by the next
    // event, at most seconds away.
    // Provenance, kept in its OWN columns rather than as a label on one
    // shared value. A single winner remembers who wrote it and nothing else,
    // so the lower bound a real observation had established was lost the
    // moment a measurement replaced it: Observed(9000) then Measured(9500)
    // then Measured(8000) walked straight past 9000, which a hook had seen
    // happen. Two kinds of evidence, two columns, combined when read by
    // `EFFECTIVE_RECENCY_SQL` — the observation is a floor, the measurement
    // moves freely above it, and `updated_at` is only what v0.2.7 left.
    ensure_column(conn, "threads_cache", "last_record_at", "INTEGER")?;
    ensure_column(conn, "threads_cache", "last_observed_at", "INTEGER")?;
    // WHICH generation of the file a measurement read. "The newest reading
    // wins" only holds if commit order matches read order, and it does not:
    // one scan can read an old file, stall, and commit after a scan that
    // read a newer one — writing the older answer last. The generation goes
    // in with the measurement and is compared on the way, so a reading of an
    // older file cannot replace a reading of a newer one.
    // NOTE: two earlier revisions stored `last_record_generation` and
    // `last_record_inode` here, to order two readings inside the write. They
    // could not: a generation comparison rejected a replacement file that
    // carried an older mtime, and "a different inode always wins" let the
    // replaced file win by committing second. Ordering readings is now the
    // caller's job — it confirms the file is still the one it read — and the
    // columns are gone. Old databases keep them, unread.
    // Everything v0.2.7 wrote went into one column, scans and hooks alike,
    // so its provenance cannot be recovered from the row. Some of it can be
    // recovered from history — but only some, and only carefully:
    //
    //   * the floor is the time the hook SAW, which lives in the payload's
    //     `updatedAt`. `observed_at` is when reconcile happened to run, so a
    //     backlog processed today would have backfilled ten-day-old activity
    //     as today's — the very failure this release is about, rebuilt by
    //     its own migration.
    //   * `thread_events` holds only the events that were worth notifying
    //     about: an event skipped because away mode was off and nothing was
    //     owed left no row at all. Plenty of legacy threads therefore have
    //     no recoverable floor, and that is FINE — the floor only guards
    //     against a measurement read before the transcript was flushed, and
    //     the measurement itself is the ground truth for when a session
    //     last spoke.
    // Guarded on every side, because this runs while the database is being
    // OPENED: one malformed historical payload must not be able to stop
    // tinyctb from starting. `json_valid` before `json_extract`, an integer
    // type check rather than a CAST that would silently turn "soon" into 0,
    // and a clamp — an absurd future value written by anything at all would
    // otherwise become a permanent floor no measurement could ever correct.
    conn.execute(
        "UPDATE threads_cache
            SET last_observed_at = (
                SELECT MAX(json_extract(e.payload_json, '$.updatedAt'))
                  FROM thread_events e
                 WHERE e.thread_id = threads_cache.thread_id
                   AND json_valid(e.payload_json)
                   AND json_type(e.payload_json, '$.updatedAt') = 'integer'
                   AND json_extract(e.payload_json, '$.updatedAt') <= ?1
            )
          WHERE last_observed_at IS NULL
            AND EXISTS (
                SELECT 1 FROM thread_events e
                 WHERE e.thread_id = threads_cache.thread_id
                   AND json_valid(e.payload_json)
                   AND json_type(e.payload_json, '$.updatedAt') = 'integer'
                   AND json_extract(e.payload_json, '$.updatedAt') <= ?1
            )",
        params![to_sql_i64(migration_now)?],
    )?;
    ensure_column(conn, "telegram_command_routes", "payload_json", "TEXT")?;
    ensure_column(conn, "telegram_callback_routes", "approval_id", "TEXT")?;
    ensure_column(conn, "pending_questions", "multi_select", "INTEGER")?;
    ensure_column(conn, "pending_prompts", "transcript_bytes", "INTEGER")?;
    ensure_column(conn, "pending_prompts", "notification_type", "TEXT")?;
    // WHICH INSTANCE of a prompt this row is. The id is `notify:{received_at}`
    // at millisecond resolution, so two Notifications in one millisecond
    // share it — and a scan that resolved the first would then clear the
    // second by name. A revision comes from an AUTOINCREMENT rowid, which
    // SQLite promises never to reuse, so no two instances can collide.
    ensure_column(conn, "pending_prompts", "revision", "INTEGER")?;
    // WHEN the socket in this row was seen. Without it the mapping was
    // whatever the last writer said, and hooks do not arrive in order — an
    // overtaken one could point replies back at a socket the session had
    // already moved off.
    ensure_column(conn, "threads_cache", "socket_observed_at", "INTEGER")?;
    // SET when the sighting behind a route stopped being believable, CLEARED
    // when a real one replaces it. Not the same as "no sighting recorded":
    // every row written before that column existed has none, and those are
    // ordinary, not suspect.
    ensure_column(conn, "threads_cache", "socket_unverified_since", "INTEGER")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS prompt_revisions (id INTEGER PRIMARY KEY AUTOINCREMENT);",
    )?;
    // A column added to a live table starts NULL on every row already in it,
    // and in SQL NULL is not 0 — it is not equal to ANYTHING, including
    // itself. The compare-and-clear below matches on the revision, so a
    // prompt that predates this upgrade could never be retired: the scan
    // would confirm it was answered, ask to delete it, and match no row. A
    // session already closed has no hook left to clear it either, so it
    // would sit in `/threads` as a question nobody can dismiss.
    //
    // ZERO counts as absent too, and this is not hypothetical: on a database
    // an earlier build of this version already touched, the column carries
    // `NOT NULL DEFAULT 0`, so any writer that omits it — an older binary
    // still installed as the hook, say — lands on 0. That is worse than
    // NULL, because 0 MATCHES, and every such row matches every other one:
    // the compare-and-clear would retire whichever prompt happened to be
    // there. The counter starts at 1, so 0 can only ever mean "no instance
    // id was assigned".
    //
    // Give each of them a real one, drawn from the same counter the new ones
    // use, so no legacy row can collide with a future one or with another.
    let legacy: Vec<i64> = {
        let mut stmt = conn
            .prepare("SELECT rowid FROM pending_prompts WHERE revision IS NULL OR revision = 0")?;
        let rows = stmt.query_map([], |row| row.get(0))?;
        rows.collect::<rusqlite::Result<Vec<i64>>>()?
    };
    for rowid in legacy {
        let revision = next_prompt_revision(conn)?;
        conn.execute(
            "UPDATE pending_prompts SET revision = ?2 WHERE rowid = ?1",
            params![rowid, revision],
        )?;
    }
    ensure_column(conn, "outbound_events", "claimed_at", "INTEGER")?;
    ensure_column(conn, "outbound_events", "claim_token", "TEXT")?;
    ensure_column(
        conn,
        "outbound_events",
        "cancel_requested",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(conn, "pending_approvals", "headless", "INTEGER")?;
    ensure_column(conn, "pending_approvals", "turn_id", "TEXT")?;
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
    backfill_stop_receipt_fields(conn)?;
    ensure_column(conn, "bridge_turns", "proc_start", "TEXT")?;
    ensure_column(conn, "bridge_turns", "proc_exe", "TEXT")?;
    ensure_column(conn, "bridge_turns", "pgid", "INTEGER")?;
    ensure_column(conn, "bridge_turns", "proc_start_ticks", "TEXT")?;
    ensure_column(conn, "bridge_turns", "boot_id", "TEXT")?;
    // Re-kill backoff for `stopping` recovery: each attempt can hold the
    // daemon loop for ~2s of confirmation, so attempts are spaced out.
    ensure_column(
        conn,
        "bridge_turns",
        "stop_attempts",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "bridge_turns",
        "last_stop_attempt_at",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    // Supervision marker: this turn's process group is UNPROVEN-empty and
    // must keep being probed WHATEVER the status column says. Deliberately
    // its own column: it is written exactly when the status transition
    // itself is what keeps failing.
    ensure_column(
        conn,
        "bridge_turns",
        "cleanup_pending",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    // cgroup regime (v0.2.5): the turn's kernel-object ownership. NULL =
    // legacy killpg regime.
    ensure_column(conn, "bridge_turns", "cgroup_path", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "thread_id", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "route_message_id", "INTEGER")?;
    ensure_column(conn, "telegram_inbound_log", "result_action", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "backend_transport", "TEXT")?;
    ensure_column(conn, "telegram_inbound_log", "backend_pid", "INTEGER")?;
    // Partial indexes for the /threads open-prompt scan and the delivery
    // tick: only unsettled rows are indexed, so each index stays tiny however
    // much settled history accumulates (and pruning keeps that bounded too).
    // The outbound one matters most — the daemon counts and drains pending
    // outbound events on EVERY tick (10 Hz), and without it both queries
    // walk the entire delivered history each time (measured: ~90 ms/s of
    // CPU against a 1.2k-row table).
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_pending_questions_open
             ON pending_questions(expires_at, created_at) WHERE answer IS NULL;
         CREATE INDEX IF NOT EXISTS idx_pending_approvals_open
             ON pending_approvals(expires_at, created_at) WHERE decision IS NULL;
         DROP INDEX IF EXISTS idx_outbound_events_pending;
         CREATE INDEX IF NOT EXISTS idx_outbound_events_active
             ON outbound_events(next_attempt_at) WHERE status IN ('pending', 'failed');",
    )?;
    Ok(())
}

/// The first shipped shape of `live_injections` counted owed answers per
/// thread (`thread_id/injected_at/owed`); accounting is now per injection so
/// an older completion cannot claim a newer one. `CREATE TABLE IF NOT
/// EXISTS` leaves an already-deployed table untouched, so rebuild it here —
/// otherwise every query fails with "no such column" on an upgraded install.
/// Interim (unreleased) builds enqueued stop receipts before the structured
/// `stopTurn`/`stopPhase` fields existed, and the terminal withdrawal
/// matches on those fields EXACTLY — an unlabelled row would dodge it
/// forever. Backfilled here once, parsed from the RIGHT of the key (the
/// invocation id contains ':' itself, the trailing turn and phase do not).
/// Released versions never wrote such rows; the LIKE pattern is a literal
/// prefix, no interpolation.
fn backfill_stop_receipt_fields(conn: &Connection) -> Result<()> {
    let rows: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT event_id, json_extract(payload_json, '$.eventKey')
             FROM outbound_events
             WHERE json_valid(payload_json)
               AND json_extract(payload_json, '$.eventKey') LIKE 'stop-summary:%'
               AND json_extract(payload_json, '$.stopTurn') IS NULL",
        )?;
        let mapped = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        mapped.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (event_id, key) in rows {
        let mut parts = key.rsplitn(3, ':');
        let (Some(phase), Some(turn)) = (parts.next(), parts.next()) else {
            continue;
        };
        if !matches!(phase, "requested" | "outcome" | "final") {
            // An even older, phase-less key shape: nothing safe to infer.
            continue;
        }
        conn.execute(
            "UPDATE outbound_events
             SET payload_json = json_set(payload_json, '$.stopTurn', ?2, '$.stopPhase', ?3)
             WHERE event_id = ?1",
            params![event_id, turn, phase],
        )?;
    }
    Ok(())
}

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

/// Serialises tests that touch process-wide state (env vars, config paths).
/// Poison is deliberately recovered: the lock protects no invariant of its
/// own, so a test that panics while holding it must produce ONE red test —
/// not a cascade of `PoisonError` failures in every later test that locks.
#[cfg(test)]
pub(crate) fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Set (or clear) one environment variable, restoring whatever was there
/// before on drop. Panic-safe where a manual save/restore is not: an assert
/// firing between the mutation and the restore must not leak the rewritten
/// variable into every later test that shares it.
#[cfg(test)]
pub(crate) struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

#[cfg(test)]
impl EnvVarGuard {
    pub(crate) fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        EnvVarGuard { key, previous }
    }

    pub(crate) fn clear(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        std::env::remove_var(key);
        EnvVarGuard { key, previous }
    }
}

#[cfg(test)]
impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => std::env::set_var(self.key, previous),
            None => std::env::remove_var(self.key),
        }
    }
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

/// The next prompt instance identity. Never reused, even after deletes —
/// that is what AUTOINCREMENT buys, and why a plain MAX(id)+1 would not do.
fn next_prompt_revision(conn: &Connection) -> Result<i64> {
    conn.execute("INSERT INTO prompt_revisions DEFAULT VALUES", [])?;
    let revision = conn.last_insert_rowid();
    // An identity source, not a log.
    conn.execute(
        "DELETE FROM prompt_revisions WHERE id < ?1",
        params![revision],
    )?;
    Ok(revision)
}

/// How recent a thread is, from the evidence the row holds.
///
/// A measurement and an observation are different KINDS of evidence and are
/// stored apart, so the answer is composed here rather than fought over at
/// write time. The observation is a FLOOR — something was seen happening,
/// and no later reading of a file may claim it did not. Above that floor the
/// newest measurement wins, up or down. Only when there is neither does the
/// row fall back to what v0.2.7 left behind, and then to when it was last
/// seen at all.
pub(crate) const EFFECTIVE_RECENCY_SQL: &str = "COALESCE(
    NULLIF(
        MAX(COALESCE(t.last_record_at, 0), COALESCE(t.last_observed_at, 0)),
        0
    ),
    t.updated_at,
    t.last_seen_at,
    0
)";

/// What the database DID with a snapshot. A refusal is not a failure -- the
/// row is simply not this reading's to write any more -- but it is also not a
/// write, and a caller that cannot tell the two apart will go on to report a
/// snapshot the database deliberately threw away.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotWrite {
    /// The row was written.
    Applied,
    /// The file moved under the reading before the write could take the lock,
    /// so nothing was written.
    RejectedStale,
    /// A hook that arrived after a NEWER one had already been recorded. Its
    /// hook-owned state was left alone -- it does not describe the present --
    /// but it still happened, and effects that are statements about the PAST
    /// rather than about now may still be owed.
    Superseded,
}

/// Where an `updated_at` came from, ordered by nothing — these are KINDS of
/// evidence, and the rules between them are not a ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpdatedAt {
    /// A file mtime. Says when the file was touched, which is not when the
    /// session spoke: a backup or a copy makes it lie by days.
    ///
    /// A reading with no stamps still CLEARS the measurement the last one
    /// left behind: a transcript rewritten or truncated must not go on
    /// reporting a time that is no longer anywhere in it.
    ///
    /// Neither reading carries a file identity any more. Ordering two
    /// readings inside the write could not work — two inodes have no order
    /// between them — so the caller instead confirms, as late as it can,
    /// that the file is still the one it read.
    Guessed,
    /// Read off the transcript's own records. A reading of the file as it is
    /// now, so the NEWEST reading is the true one — including when it is
    /// earlier than what is stored, which is how a row a touched file pushed
    /// to today comes back down.
    ///
    Measured,
    /// A hook reporting what it saw. Real activity, and it only ever moves
    /// forward.
    Observed,
}

impl UpdatedAt {
    fn as_i64(self) -> i64 {
        match self {
            UpdatedAt::Guessed => 0,
            UpdatedAt::Measured => 1,
            UpdatedAt::Observed => 2,
        }
    }
}

pub(crate) fn upsert_thread_snapshot(
    conn: &Connection,
    snapshot: &BridgeThreadSnapshot,
    now: u64,
    source: UpdatedAt,
    // The prompt a SCAN read and found answered, if it found one. Its only
    // licence to clear the row: it may retire that prompt and no other.
    // The prompt INSTANCE a scan read and found answered, if it found one.
    // Its only licence to touch the prompt row: it may retire that instance
    // and no other.
    resolved_prompt_revision: Option<i64>,
    // Asked again once the write lock is held. Its answer decides whether
    // this write happens at all.
    still_current: Option<&dyn Fn() -> bool>,
    // The newest observation THIS cycle holds for this session, used only
    // when the stored high point turns out to be unusable. `None` from
    // callers that have no batch around them.
    cycle_floor: Option<u64>,
) -> Result<SnapshotWrite> {
    // ONE TRANSACTION, and an IMMEDIATE one: the row and the prompt are one
    // statement about a session, and a reader must never see half of it.
    //
    // It is also what makes the caller's freshness check mean anything. The
    // check used to run just before an unlocked write, so a second writer
    // could slip in between and be overwritten by an answer already known to
    // be stale. Taking the write lock FIRST and re-asking inside it closes
    // that: nobody else can commit while this decision is being made.
    let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
    if let Some(still_current) = still_current {
        if !still_current() {
            return Ok(SnapshotWrite::RejectedStale);
        }
    }
    let conn = &tx;
    // IS THIS OBSERVATION STILL THE PRESENT? Hooks are spooled as files and
    // read back a cycle later, and one that could not be read is KEPT for the
    // next cycle — deliberately, so a transient error never destroys a real
    // hook. The price is that hooks can arrive OUT OF ORDER: a Stop stamped
    // 2000 lands, then the Notification stamped 1000 that failed to read
    // before it. Only `last_observed_at` was a maximum, so the floor held
    // while everything it exists to protect — status, turn status, preview,
    // the prompt — was written straight over by the older hook, putting a
    // session back to `waiting` on a question it had already answered.
    //
    // The ruling is made HERE, under the write lock already held, so nothing
    // can land between deciding and acting on it.
    let (last_observed_at, last_record_at): (Option<i64>, Option<i64>) = conn
        .query_row(
            "SELECT last_observed_at, last_record_at FROM threads_cache WHERE thread_id = ?1",
            params![snapshot.thread_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .unwrap_or((None, None));
    // A WATERMARK IN THE FUTURE is not a watermark. Every rule below reads
    // the stored high point as "the newest thing that has happened", which
    // assumes the wall clock only ever moves forward. It does not: an NTP
    // correction steps it back, and one hook with a bad `receivedAt` plants a
    // stamp years ahead all by itself. Either way the stored high point then
    // sits above everything real, every genuine hook after it looks late,
    // and the session's status, prompt and routing freeze until the clock
    // catches up — which for a bad future stamp is never.
    //
    // So a high point ahead of the clock reading this transaction is not
    // evidence to be defended, it is damage to be repaired: the incoming
    // hook is accepted and the baseline is REBUILT from it rather than
    // maxed against the old one.
    let now_i64 = to_sql_i64(now)?;
    // A high point ahead of the clock is DISCARDED, not translated. Two
    // wrong repairs were tried on the way here and both had the same shape,
    // of answering "this number is unusable" with a different number:
    //
    //   * rebuild from whatever arrives next — but entries that failed to
    //     read are kept and retried, so the next arrival is as likely to be
    //     an old backlog entry as a fresh hook, and it would re-declare the
    //     past as the present;
    //   * bring it down to the clock — but a hook is stamped when it is
    //     WRITTEN and read a cycle later, so `received_at < now` is the
    //     normal case, and a floor at `now` swallows the very next genuine
    //     hook. If that was the session's last one, its old status and its
    //     old question stay forever.
    //
    // So the damaged value simply stops counting, and the floor is taken
    // from the evidence that is left. Two pieces, both real, neither of them
    // the processing clock:
    //
    //   * what this CYCLE is holding for this session — its newest hook, so
    //     an entry retried out of the backlog is measured against the batch
    //     it arrived with rather than against the first thing to be read;
    //   * the last record time MEASURED off the transcript, which no hook
    //     wrote and which a hook stamped before it cannot be describing.
    //
    // If neither exists there is nothing in the world to order this against,
    // and it is taken. That case is stated rather than hidden: a session
    // whose stored high point is damaged and whose repairing cycle holds
    // only one old entry, with no transcript reading on the row, lets that
    // entry set the new floor.
    let watermark_ahead_of_the_clock = last_observed_at.is_some_and(|seen| seen > now_i64);
    let effective_seen = if watermark_ahead_of_the_clock {
        let from_cycle = cycle_floor.map(to_sql_i64).transpose()?;
        let from_transcript = last_record_at.filter(|at| *at <= now_i64);
        from_cycle.max(from_transcript)
    } else {
        last_observed_at
    };
    let superseded = matches!(source, UpdatedAt::Observed)
        && match (snapshot.updated_at, effective_seen) {
            // STRICTLY behind, and that limit is deliberate. Two hooks
            // stamped the same millisecond cannot be ordered at all: the
            // spool file names them `{received_at}-{pid}-{event}`, and a pid
            // is not a sequence — sorting by it would dress an arbitrary
            // order up as causality. So a tie is treated as CURRENT, and the
            // choice is between two losses. Accepting a tie can push a
            // question that was in fact already answered. Refusing one can
            // drop a real question so it is never pushed at all, and nothing
            // later re-raises it.
            //
            // The asymmetry is only in how far each can be walked back, and
            // it is smaller than it looks. A wrong push cannot be recalled
            // at all. What the revision buys is narrower than "the next scan
            // fixes it": it guarantees that IF this session is scanned again,
            // the stale row can be cleared precisely, without retiring a
            // question that replaced it. It does not promise the scan
            // happens — the same cycle skips sessions a hook just spoke for,
            // and later cycles only look at the most recently touched, so a
            // quiet session can sit outside that pool for a long time. Even
            // so: a stale row that MIGHT be cleared beats a real question
            // that was never announced and that nothing will announce.
            (Some(at), Some(seen)) => to_sql_i64(at)? < seen,
            _ => false,
        };
    conn.execute(
        "INSERT INTO threads_cache(
            thread_id, name, cwd, source, status_type, status_flags_json,
            updated_at, last_seen_at, last_turn_status, last_preview,
            last_record_at, last_observed_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                   CASE WHEN ?11 = 1 THEN ?7 END,
                   CASE WHEN ?11 = 2 THEN ?7 END)
         ON CONFLICT(thread_id) DO UPDATE SET
            -- A MISSING value is silence, not an erasure. A hook without a
            -- transcript to hand carries neither name nor cwd, and writing
            -- its `None` straight in wiped what a scan had read off the file.
            --
            -- WHO MAY SPEAK ABOUT METADATA, though, is a second question, and
            -- COALESCE alone did not answer it: a lagging scan holding an OLD
            -- non-null name still wrote it over the hook\'s new one. A hook
            -- (2) reports its own payload and is always current. A MEASURED
            -- reading (1) carries the transcript\'s own record time, so it may
            -- speak when the file it read is not older than the last thing
            -- actually seen. A GUESS (0) carries only an mtime -- the very
            -- number this version exists to stop trusting -- so it may fill a
            -- gap and must never outrank an observation.
            name = CASE
                WHEN (?11 = 2 AND ?12 = 0)
                  OR (?11 = 1 AND excluded.updated_at IS NOT NULL
                      AND excluded.updated_at >= ?14)
                  OR (?11 = 0 AND ?14 = 0)
                    THEN COALESCE(excluded.name, threads_cache.name)
                ELSE threads_cache.name
            END,
            cwd = CASE
                WHEN (?11 = 2 AND ?12 = 0)
                  OR (?11 = 1 AND excluded.updated_at IS NOT NULL
                      AND excluded.updated_at >= ?14)
                  OR (?11 = 0 AND ?14 = 0)
                    THEN COALESCE(excluded.cwd, threads_cache.cwd)
                ELSE threads_cache.cwd
            END,
            source = excluded.source,
            -- HOOK-OWNED. A scan learns these by READING THE ROW and
            -- writing them straight back, which is not a fresh answer at
            -- all — it is whatever the row said when the scan started. A
            -- hook landing in between was then undone by a scan that never
            -- saw it, no file race required. Only an observation writes
            -- them; a reading leaves them exactly as they are.
            status_type = CASE
                WHEN ?11 = 2 AND ?12 = 0 THEN excluded.status_type
                ELSE threads_cache.status_type
            END,
            status_flags_json = CASE
                WHEN ?11 = 2 AND ?12 = 0 THEN excluded.status_flags_json
                ELSE threads_cache.status_flags_json
            END,
            -- `updated_at` is the FALLBACK now, not the answer: it is what
            -- v0.2.7 rows carry and what a session with no other evidence
            -- has. It keeps its old forward-only rule so a guess cannot drag
            -- it about; the two columns below are what ordering reads.
            updated_at = CASE
                WHEN excluded.updated_at IS NULL THEN threads_cache.updated_at
                WHEN threads_cache.updated_at IS NULL THEN excluded.updated_at
                WHEN excluded.updated_at > threads_cache.updated_at THEN excluded.updated_at
                ELSE threads_cache.updated_at
            END,
            -- MEASURED: a reading of the file as it is. The newest reading
            -- is the true one, earlier or later — that is how a row a
            -- touched file pushed to today comes back down.
            -- MEASURED sets the reading; GUESSED clears it. One reading of
            -- one file, committed as a unit — a transcript rewritten to
            -- carry no stamps must not go on reporting a time that is no
            -- longer in it.
            --
            -- No generation is compared here. Two inodes have no order
            -- between them, so a rule of always-accept-a-different-inode let
            -- a reading of the REPLACED file beat a reading of the file that
            -- replaced it, purely by committing second. The caller asks the
            -- only question that settles it -- is the file still the one I
            -- read -- immediately before this runs.
            last_record_at = CASE
                WHEN ?11 = 2 THEN threads_cache.last_record_at
                WHEN ?11 = 1 THEN excluded.updated_at
                ELSE NULL
            END,
            -- OBSERVED: something was seen happening. It only ever moves
            -- forward, and it is the floor no measurement may cross.
            last_observed_at = CASE
                -- Repairing. The floor the ruling above was made against
                -- must OUTLIVE this write, or the repair lasts exactly one
                -- statement: the first entry of the batch would write its own
                -- older stamp here, the damage would be gone, and every
                -- entry after it would be judged against that lower mark
                -- instead of the evidence — the second one walking straight
                -- past a floor the first was refused by.
                --
                -- So the floor is kept, and only an observation that was NOT
                -- refused (?12 = 0) may raise it.
                WHEN ?13 = 1
                    THEN NULLIF(
                        MAX(?14, CASE
                            WHEN ?11 = 2 AND ?12 = 0 THEN COALESCE(excluded.updated_at, 0)
                            ELSE 0
                        END),
                        0
                    )
                WHEN ?11 = 2
                 AND excluded.updated_at > COALESCE(threads_cache.last_observed_at, 0)
                    THEN excluded.updated_at
                ELSE threads_cache.last_observed_at
            END,
            last_seen_at = excluded.last_seen_at,
            last_turn_status = CASE
                WHEN ?11 = 2 AND ?12 = 0 THEN excluded.last_turn_status
                ELSE threads_cache.last_turn_status
            END,
            -- The preview follows the same evidence as the time it belongs
            -- to. A hook has the answer from its own payload and is always
            -- current. A READING is only current if the file it read is not
            -- older than the last thing actually SEEN — a Stop whose text has
            -- not reached the transcript yet would otherwise be replaced by
            -- the previous answer, read from a file that had not caught up.
            -- Same authority as the name above: a GUESS could otherwise put
            -- the previous answer back by nothing more than a `touch`.
            last_preview = CASE
                WHEN (?11 = 2 AND ?12 = 0)
                  OR (?11 = 1 AND excluded.updated_at IS NOT NULL
                      AND excluded.updated_at >= ?14)
                  OR (?11 = 0 AND ?14 = 0)
                    THEN COALESCE(excluded.last_preview, threads_cache.last_preview)
                ELSE threads_cache.last_preview
            END",
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
            source.as_i64(),
            i64::from(superseded),
            i64::from(watermark_ahead_of_the_clock),
            effective_seen.unwrap_or(0),
        ],
    )?;

    // The PROMPT ROW, and who may say what about it.
    //
    // A hook is authoritative: it saw the session ask, or saw it stop. A
    // scan is not — it can only report what it read out of a transcript, and
    // it learned the prompt itself by reading this very row a moment ago. A
    // scan writing `None` therefore said nothing about the present: it said
    // "the row had no prompt when I started", and deleting on the strength
    // of that erased a prompt a hook had written in between. No file race
    // was needed for it, only a scan and a hook in the same second.
    //
    // So a scan may only CLEAR the prompt it actually looked at, by name:
    // it read that prompt, checked the transcript, and found it answered.
    match (&snapshot.pending_prompt, source, superseded) {
        // A SUPERSEDED hook is not a statement about the present, and a
        // prompt is the loudest such statement there is: re-raising a
        // question the session has already answered puts a dead question
        // back in `/threads` with nothing left to clear it.
        (_, UpdatedAt::Observed, true) => {}
        // A SCAN carrying a prompt is saying "the one I read is still
        // unresolved" — a statement about what it READ, not about what is
        // there now. Writing it back put a prompt the hook had already
        // replaced straight over the new one.
        (Some(_), UpdatedAt::Measured | UpdatedAt::Guessed, _) => {}
        (Some(prompt), UpdatedAt::Observed, _) => {
            conn.execute(
                "INSERT INTO pending_prompts(thread_id, prompt_id, prompt_kind, prompt_status, question, created_at, transcript_bytes, notification_type, revision)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(thread_id) DO UPDATE SET
                    prompt_id = excluded.prompt_id,
                    prompt_kind = excluded.prompt_kind,
                    prompt_status = excluded.prompt_status,
                    question = excluded.question,
                    created_at = excluded.created_at,
                    transcript_bytes = excluded.transcript_bytes,
                    notification_type = excluded.notification_type,
                    -- Every write is a NEW instance, so a compare-and-clear
                    -- made against the previous one finds nothing.
                    revision = ?9",
                params![
                    snapshot.thread_id,
                    prompt.prompt_id,
                    prompt.kind,
                    prompt.status,
                    prompt.question.clone().unwrap_or_default(),
                    to_sql_i64(now)?,
                    prompt.transcript_bytes.map(|bytes| bytes as i64),
                    prompt.notification_type,
                    next_prompt_revision(conn)?,
                ],
            )?;
        }
        (None, UpdatedAt::Observed, _) => {
            // A hook saying "no prompt" is a fact about the session now.
            conn.execute(
                "DELETE FROM pending_prompts WHERE thread_id = ?1",
                params![snapshot.thread_id],
            )?;
        }
        (None, _, _) => {
            // A scan resolved the prompt it read. Compare-and-clear, so a
            // prompt written since — which this scan never saw — survives.
            // Compare-and-clear on the INSTANCE, not the name:
            // `notify:{received_at}` repeats within a millisecond, so
            // clearing by name could retire a prompt that had replaced the
            // one this scan actually looked at.
            if let Some(revision) = resolved_prompt_revision {
                conn.execute(
                    "DELETE FROM pending_prompts WHERE thread_id = ?1 AND revision = ?2",
                    params![snapshot.thread_id, revision],
                )?;
            }
        }
    }
    tx.commit()?;
    Ok(if superseded {
        SnapshotWrite::Superseded
    } else {
        SnapshotWrite::Applied
    })
}

/// Remember where a live session listens for injected messages. Reported by
/// the session's own hooks (they inherit CLAUDE_CODE_MESSAGING_SOCKET), so
/// this mapping is authoritative rather than guessed from process state.
/// Mark every route whose sighting sits ahead of the clock as UNVERIFIED.
///
/// Making the timestamp believable does not make the path believable, and an
/// earlier version of this did only that: it wrote `now` over the bad stamp,
/// the row stopped looking suspect on the next cycle, and the same unchecked
/// socket went straight back into service as if something had confirmed it.
/// Worse, a stamp of `now` then out-ranked the next genuine hook, which is
/// always stamped a little earlier than the cycle that reads it.
///
/// So the sighting is REMOVED rather than rewritten, and the row is marked.
/// The mark is what persists: it survives cycles, and only two things end it
/// — a real sighting arriving (`record_session_messaging_socket` clears it),
/// or the route being shown to be gone (`clear_resolved_socket_quarantines`).
/// While it stands, replies are held rather than routed or forked.
pub(crate) fn quarantine_future_socket_sightings(conn: &Connection, now: u64) -> Result<usize> {
    let now = to_sql_i64(now)?;
    let quarantined = conn.execute(
        "UPDATE threads_cache
            SET socket_unverified_since = ?1, socket_observed_at = NULL
          WHERE socket_observed_at > ?1",
        params![now],
    )?;
    if quarantined > 0 {
        eprintln!(
            "tinyctb: {quarantined} session(s) carried a socket sighting from ahead of the \
             clock; the route is now unverified and replies are held until something vouches \
             for it"
        );
    }
    Ok(quarantined)
}

/// Every route currently marked unverified, so the caller can check whether
/// each is still there.
pub(crate) fn unverified_socket_routes(
    conn: &Connection,
) -> Result<Vec<(String, crate::claude::SessionSocket)>> {
    let mut stmt = conn.prepare(
        "SELECT thread_id, messaging_socket, socket_inode, socket_boot_id
           FROM threads_cache
          WHERE socket_unverified_since IS NOT NULL
            AND messaging_socket IS NOT NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            crate::claude::SessionSocket {
                path: row.get::<_, String>(1)?,
                inode: row.get::<_, Option<i64>>(2)?.map(|value| value as u64),
                boot_id: row.get::<_, Option<String>>(3)?,
            },
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// The route answered to the identity recorded for it, which is the thing
/// that was ever actually in question. The doubt ends; the route stays.
pub(crate) fn vouch_for_socket_route(conn: &Connection, thread_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE threads_cache SET socket_unverified_since = NULL WHERE thread_id = ?1",
        params![thread_id],
    )?;
    Ok(())
}

/// The route is gone, so there is nothing left to be unsure about: drop it.
/// A reply for this session now falls back honestly instead of being held
/// forever waiting for a session that will never speak again.
pub(crate) fn forget_unverified_socket_route(conn: &Connection, thread_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE threads_cache
            SET messaging_socket = NULL, socket_inode = NULL, socket_boot_id = NULL,
                socket_observed_at = NULL, socket_unverified_since = NULL
          WHERE thread_id = ?1",
        params![thread_id],
    )?;
    Ok(())
}

pub(crate) fn record_session_messaging_socket(
    conn: &Connection,
    thread_id: &str,
    socket: &crate::claude::SessionSocket,
    // WHEN the hook that carried this socket was received. The mapping is
    // only as current as the observation behind it, and observations arrive
    // out of order: a hook kept over from a failed read comes back a cycle
    // late and would otherwise route replies to wherever it was pointing.
    observed_at: u64,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO threads_cache(
            thread_id, status_type, status_flags_json, last_seen_at,
            messaging_socket, socket_inode, socket_boot_id, socket_observed_at
         ) VALUES (?1, 'active', '[]', ?3, ?2, ?4, ?5, ?6)
         ON CONFLICT(thread_id) DO UPDATE SET
            messaging_socket = excluded.messaging_socket,
            socket_inode = excluded.socket_inode,
            socket_boot_id = excluded.socket_boot_id,
            socket_observed_at = excluded.socket_observed_at,
            -- Something vouched for the route: the doubt is over.
            socket_unverified_since = NULL
            -- The stored sighting is only usable as far as the clock. Above
            -- that it is damage, and MIN brings it back down for this
            -- comparison rather than letting it lock the routing out
            -- forever. It does NOT open the door to anything: an old backlog
            -- entry is still older than the clock and still loses. Only a
            -- sighting at least as new as now can take a corrupted slot.
         WHERE excluded.socket_observed_at
               >= MIN(COALESCE(threads_cache.socket_observed_at, 0), ?3)",
        params![
            thread_id,
            socket.path,
            to_sql_i64(now)?,
            socket.inode.map(|value| value as i64),
            socket.boot_id,
            to_sql_i64(observed_at)?
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
    Ok(session_messaging_route(conn, thread_id)?.map(|(socket, _)| socket))
}

/// A route as the row holds it: path, inode, boot id, and when the doubt
/// about it began, if it ever did.
type StoredSocketRoute = (Option<String>, Option<i64>, Option<String>, Option<i64>);

/// The route for a session, and whether anything currently vouches for it.
///
/// `true` means the sighting behind it stopped being believable and nothing
/// has settled it since. The route may well be right — it usually is — but a
/// caller may not treat its ABSENCE of a reply as proof the session is gone,
/// because spawning a second one is the mistake that cannot be taken back.
pub(crate) fn session_messaging_route(
    conn: &Connection,
    thread_id: &str,
) -> Result<Option<(crate::claude::SessionSocket, bool)>> {
    let row: Option<StoredSocketRoute> = conn
        .query_row(
            "SELECT messaging_socket, socket_inode, socket_boot_id, socket_unverified_since
             FROM threads_cache WHERE thread_id = ?1",
            params![thread_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    Ok(row.and_then(|(path, inode, boot_id, unverified_since)| {
        let path = path.filter(|value| !value.trim().is_empty())?;
        Some((
            crate::claude::SessionSocket {
                path,
                inode: inode.map(|value| value as u64),
                boot_id,
            },
            unverified_since.is_some(),
        ))
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
/// Settle one injection debt against this completion, and say whether THIS
/// caller is the one that settled it.
///
/// The answer is what decides bridge routing, so it cannot be a separate
/// question asked earlier: two completions that both read "a debt is
/// pending" each pushed themselves as the promised answer, and the user got
/// the same reply twice. The UPDATE carries its own precondition, so exactly
/// one caller comes away with `true` — the other falls back to the away
/// rules like any ordinary completion.
pub(crate) fn consume_live_injection(
    conn: &Connection,
    thread_id: &str,
    event_at: Option<u64>,
    now: u64,
) -> Result<bool> {
    let Some(id) = claimable_live_injection(conn, thread_id, event_at, now)? else {
        return Ok(false);
    };
    let changed = conn.execute(
        "UPDATE live_injections SET claimed_at = ?2
         WHERE id = ?1 AND claimed_at IS NULL",
        params![id, to_sql_i64(now)?],
    )?;
    Ok(changed > 0)
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
            "question": prompt.question,
            "notificationType": prompt.notification_type
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

pub(crate) fn record_delivery(
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
    // Kept for the call sites that still pass it: the delivery marker moved
    // to the outbound enqueue, so nothing here writes one any more.
    _record_deliveries: bool,
) -> Result<Value> {
    let away = get_setting_text(conn, "away")?.unwrap_or_default() == "true";
    let away_started_at = get_setting_number(conn, "away_started_at")?;
    let mut events = Vec::new();
    let mut threads = Vec::new();

    // The newest observation this cycle holds per session. It is what
    // re-establishes a damaged floor: not the clock, and not whichever entry
    // happened to be read first.
    let mut cycle_floor: std::collections::BTreeMap<&str, u64> = std::collections::BTreeMap::new();
    for snapshot in &snapshots {
        if let Some(at) = snapshot.updated_at {
            let seen = cycle_floor.entry(snapshot.thread_id.as_str()).or_insert(at);
            *seen = (*seen).max(at);
        }
    }

    for snapshot in &snapshots {
        // A hook OBSERVES; it never measures the transcript. It also hands
        // over no freshness check, so there is nothing here that CAN refuse
        // it -- said out loud rather than by discarding the answer.
        let write = upsert_thread_snapshot(
            conn,
            snapshot,
            now,
            UpdatedAt::Observed,
            None,
            None,
            cycle_floor.get(snapshot.thread_id.as_str()).copied(),
        )?;
        debug_assert_ne!(write, SnapshotWrite::RejectedStale);
        // A hook a NEWER one already overtook describes the past, not the
        // present. Its row was left alone; handing it back here would put
        // the same stale state in front of the reader by another door.
        let superseded = write == SnapshotWrite::Superseded;
        if !superseded {
            threads.push(thread_snapshot_json(snapshot));
        }

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

        // EFFECTS SPLIT BY WHAT THEY CLAIM. A waiting notification says "this
        // session is asking you something NOW" -- an overtaken hook cannot
        // say that, and saying it anyway is how an answered question comes
        // back to the phone. A completion says "this session finished" --
        // that happened, it is still true, and the person it was promised to
        // has not heard it yet, so a late one is still owed.
        if let Some(prompt) = snapshot.pending_prompt.as_ref().filter(|_| !superseded) {
            let event_key = format!(
                "thread_waiting:{}:{}:{}",
                snapshot.thread_id, prompt.prompt_id, event_discriminator
            );
            // NOT gated on a delivery marker here. The marker used to be
            // written in this function while the outbound row was written
            // much later; a failure in between left the marker standing and
            // silenced every retry, so the notification was lost for good.
            // The marker now commits WITH the outbound row, and this loop
            // simply reports what it saw.
            let should_emit = true;
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
            // Same rule as the waiting event above: the marker belongs with
            // the outbound row, not here.
            let should_emit = true;
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
    let mut stmt = conn.prepare(&format!(
        "SELECT t.thread_id, t.name, t.cwd, {recency} AS updated_at, t.status_type,
                t.status_flags_json,
                t.last_preview, p.prompt_id, p.prompt_kind, p.prompt_status, p.question
         FROM pending_prompts p
         INNER JOIN threads_cache t ON t.thread_id = p.thread_id
         ORDER BY {recency} DESC",
        recency = EFFECTIVE_RECENCY_SQL
    ))?;
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
                    notification_type: None,
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
    let mut stmt = conn.prepare(&format!(
        "SELECT t.thread_id, t.name, t.cwd, {recency} AS updated_at, t.status_type,
                t.status_flags_json,
                t.last_turn_status, t.last_preview, p.prompt_id, p.prompt_kind, p.prompt_status,
                p.question
         FROM threads_cache t
         LEFT JOIN pending_prompts p ON p.thread_id = t.thread_id
         ORDER BY {recency} DESC
         LIMIT ?1",
        recency = EFFECTIVE_RECENCY_SQL
    ))?;
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
                        notification_type: None,
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
    let mut stmt = conn.prepare(&format!(
        "SELECT t.thread_id, t.name, t.cwd, {recency} AS updated_at, t.last_seen_at, t.status_type,
                t.status_flags_json,
                t.last_turn_status, t.last_preview, p.prompt_kind, p.prompt_status, p.question
         FROM threads_cache t
         LEFT JOIN pending_prompts p ON p.thread_id = t.thread_id
         ORDER BY {recency} DESC",
        recency = EFFECTIVE_RECENCY_SQL
    ))?;
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
                    notification_type: None,
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
    /// cgroup regime: the turn's own cgroup directory, created BEFORE the
    /// spawn. Killing and emptiness-proof go through it; NULL rows use the
    /// legacy killpg machinery.
    pub(crate) cgroup_path: Option<String>,
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
    // `cleanup_pending`: a row registered WITHOUT a pid carries the cleanup
    // debt from birth — the successful identity write is what pays it off.
    // Pre-positioned here because a debt recorded only after a write
    // failure is a debt a failing database can refuse to record.
    conn.execute(
        "INSERT OR REPLACE INTO bridge_turns(turn_id, thread_id, log_path, pid, started_at, status, completed_at, exited, exit_code, proc_start, proc_exe, pgid, proc_start_ticks, boot_id, cleanup_pending)
         VALUES (?1, ?2, ?3, ?4, ?5, 'running', NULL, 0, NULL, ?6, ?7, ?8, ?9, ?10,
                 CASE WHEN ?4 IS NULL THEN 1 ELSE 0 END)",
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
        "SELECT thread_id, prompt_id, prompt_kind, prompt_status, question, notification_type, MAX(created_at)
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
                notification_type: row.get(5)?,
            },
            row.get::<_, i64>(6)?,
        ))
    })?;
    let mut result = Vec::new();
    for row in rows {
        let (thread_id, prompt, created_at) = row?;
        result.push((thread_id, prompt, from_sql_i64(created_at)?));
    }
    Ok(result)
}

/// Turns the daemon still owns: `running`, plus `stopping` — a turn whose
/// kill was requested but never confirmed is still out there, and dropping
/// it from this list is exactly how a live process stops being tracked.
pub(crate) fn list_running_bridge_turns(conn: &Connection) -> Result<Vec<BridgeTurn>> {
    list_bridge_turns_where(conn, "status IN ('running', 'stopping')")
}

/// Every turn the daemon must LOOK AT this cycle: the running set plus any
/// row still carrying the cleanup marker — including rows whose status a
/// racing writer already made terminal. A supervised group is supervised
/// whatever the status column says; a `running`-only scan was how a
/// marked-but-buried row escaped every future probe.
pub(crate) fn list_supervised_bridge_turns(conn: &Connection) -> Result<Vec<BridgeTurn>> {
    list_bridge_turns_where(
        conn,
        // ANY non-zero marker: the failure flavour (2) supervises exactly
        // like the stop flavour (1) — a `= 1` filter let a terminal row
        // with a failure marker slip out of every scan while its object
        // stayed populated and unprunable.
        "status IN ('running', 'stopping') OR cleanup_pending != 0",
    )
}

fn list_bridge_turns_where(conn: &Connection, filter: &str) -> Result<Vec<BridgeTurn>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT turn_id, thread_id, log_path, pid, started_at, exited, exit_code,
                pgid, proc_start_ticks, boot_id, cgroup_path
         FROM bridge_turns WHERE {filter}
         ORDER BY started_at ASC",
    ))?;
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
            row.get(10)?,
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
                cgroup_path,
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
                    cgroup_path,
                })
            },
        )
        .collect()
}

/// Record that a spawned turn process was reaped by the daemon. Keyed by
/// TURN ID: a pid key stamped `exited` onto every active row sharing a
/// recycled pid, and the innocent row was then settled as a crash.
pub(crate) fn record_bridge_turn_exit(
    conn: &Connection,
    turn_id: &str,
    exit_code: Option<i32>,
) -> Result<()> {
    conn.execute(
        "UPDATE bridge_turns SET exited = 1, exit_code = ?2
         WHERE turn_id = ?1 AND status IN ('running', 'stopping')",
        params![turn_id, exit_code],
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
/// A `/stop` command's FIRST interpretation — either the turns it resolved
/// to, or the terminal reply of an early exit.
pub(crate) struct StopOperation {
    pub(crate) kind: String,
    pub(crate) turn_ids: Vec<String>,
    pub(crate) reply: Option<String>,
}

/// Record a `/stop` command's interpretation the FIRST time it is made.
/// Idempotent by key so a racing re-insert cannot clobber the original.
pub(crate) fn record_stop_operation(
    conn: &Connection,
    operation_id: &str,
    kind: &str,
    turn_ids: &[String],
    reply: Option<&str>,
    now: u64,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO stop_operations(operation_id, kind, turns_json, reply, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            operation_id,
            kind,
            serde_json::to_string(turn_ids)?,
            reply,
            to_sql_i64(now)?
        ],
    )?;
    Ok(())
}

/// A previously seen `/stop` command's interpretation, or None if this
/// command id has never been recorded.
pub(crate) fn stop_operation(
    conn: &Connection,
    operation_id: &str,
) -> Result<Option<StopOperation>> {
    let row: Option<(String, String, Option<String>)> = conn
        .query_row(
            "SELECT kind, turns_json, reply FROM stop_operations WHERE operation_id = ?1",
            params![operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    match row {
        Some((kind, turns_json, reply)) => Ok(Some(StopOperation {
            kind,
            turn_ids: serde_json::from_str(&turns_json)?,
            reply,
        })),
        None => Ok(None),
    }
}

/// Withdraw every UNDELIVERED pre-final stop receipt for this turn
/// (`requested`/`outcome` phases, any invocation). Ordering rows by
/// timestamp cannot survive a retry: a `requested` that failed its first
/// send would retry AFTER the final receipt it causally precedes, telling
/// the user "正在终止" about a turn already reported stopped. Delivered
/// rows are history and stay; every invocation's story concludes with the
/// turn's terminal receipt.
pub(crate) fn withdraw_undelivered_stop_chatter(
    conn: &Connection,
    turn_id: &str,
    now: u64,
) -> Result<()> {
    let stale_cutoff = to_sql_i64(now.saturating_sub(CLAIM_LEASE_MS))?;
    // Matched on the STRUCTURED payload fields, exactly — parsing the event
    // key with LIKE made `_`/`%` inside a turn id act as wildcards and
    // could withdraw another turn's receipts. The same two-step shape as
    // `cancel_pending_push_inner`: delete what nobody holds, mark what a
    // live lease still might send.
    conn.execute(
        "DELETE FROM outbound_events
         WHERE delivered_at IS NULL
           AND status IN ('pending', 'failed')
           AND (claimed_at IS NULL OR claimed_at <= ?2)
           AND json_valid(payload_json)
           AND json_extract(payload_json, '$.stopTurn') = ?1
           AND json_extract(payload_json, '$.stopPhase') IN ('requested', 'outcome')
           AND NOT EXISTS (
             SELECT 1 FROM transport_delivery_log
             WHERE transport_delivery_log.event_id = outbound_events.event_id
           )",
        params![turn_id, stale_cutoff],
    )?;
    conn.execute(
        "UPDATE outbound_events SET cancel_requested = 1
         WHERE delivered_at IS NULL
           AND status IN ('pending', 'failed')
           AND json_valid(payload_json)
           AND json_extract(payload_json, '$.stopTurn') = ?1
           AND json_extract(payload_json, '$.stopPhase') IN ('requested', 'outcome')",
        params![turn_id],
    )?;
    Ok(())
}

/// One turn by id, whatever its status — replay needs to see settled rows.
pub(crate) fn bridge_turn_by_id(conn: &Connection, turn_id: &str) -> Result<Option<BridgeTurn>> {
    type Row = (
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
        Option<String>,
    );
    let row: Option<Row> = conn
        .query_row(
            "SELECT turn_id, thread_id, log_path, pid, started_at, exited, exit_code,
                    pgid, proc_start_ticks, boot_id, cgroup_path
             FROM bridge_turns WHERE turn_id = ?1",
            params![turn_id],
            |row| {
                Ok((
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
                    row.get(10)?,
                ))
            },
        )
        .optional()?;
    let Some((
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
        cgroup_path,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(BridgeTurn {
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
        cgroup_path,
    }))
}

/// Re-kill pacing for a `stopping` turn: (attempts so far, when the last
/// one was made).
pub(crate) fn stop_attempt_state(conn: &Connection, turn_id: &str) -> Result<(u32, u64)> {
    let row: Option<(i64, i64)> = conn
        .query_row(
            "SELECT stop_attempts, last_stop_attempt_at FROM bridge_turns WHERE turn_id = ?1",
            params![turn_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (attempts, last) = row.unwrap_or((0, 0));
    Ok((attempts.max(0) as u32, last.max(0) as u64))
}

pub(crate) fn record_stop_attempt(conn: &Connection, turn_id: &str, now: u64) -> Result<()> {
    conn.execute(
        "UPDATE bridge_turns SET stop_attempts = stop_attempts + 1, last_stop_attempt_at = ?2
         WHERE turn_id = ?1",
        params![turn_id, now as i64],
    )?;
    Ok(())
}

/// Durable supervision marker for a group that could not be proven empty
/// AND whose `stopping` transition would not commit. No status change — a
/// trigger or constraint on the status column cannot block this — but the
/// contract identity is written so the probe has something to ask about,
/// and the owned dialogs close in the same transaction.
pub(crate) fn mark_cleanup_pending(
    conn: &Connection,
    turn_id: &str,
    pid: Option<u32>,
    ticks: Option<&str>,
    boot_id: Option<&str>,
    now: u64,
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        // `cleanup_pending = 2`: this path is only ever a SPAWN FAILURE's
        // unwinding — the recovery loop must settle it as `failed`, not
        // report a stop the user never asked for.
        "UPDATE bridge_turns
         SET cleanup_pending = 2, pid = COALESCE(pid, ?2), pgid = COALESCE(pgid, ?2),
             proc_start_ticks = COALESCE(proc_start_ticks, ?3),
             boot_id = COALESCE(boot_id, ?4)
         WHERE turn_id = ?1",
        params![turn_id, pid.map(i64::from), ticks, boot_id],
    )?;
    settle_prompts_for_turn(&tx, turn_id, now)?;
    tx.commit()?;
    Ok(())
}

/// Active turns with NO ownership object — the explicitly opted-in legacy
/// regime. Teardown must refuse while any exist: the sweep cannot see
/// them, and deleting the ledger under them strands the processes.
pub(crate) fn count_active_legacy_turns(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        // Terminal rows still carrying a supervision marker count too:
        // their group was never proven empty, and without a cgroup path
        // the sweep cannot see them either.
        "SELECT COUNT(*) FROM bridge_turns
         WHERE (status IN ('running', 'stopping') OR cleanup_pending != 0)
           AND cgroup_path IS NULL",
        [],
        |row| row.get(0),
    )?)
}

/// Put a turn under the supervised loop's eye: its object (or group) is
/// not yet proven empty.
pub(crate) fn set_cleanup_marker(conn: &Connection, turn_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE bridge_turns SET cleanup_pending = 1 WHERE turn_id = ?1",
        params![turn_id],
    )?;
    Ok(())
}

/// The FAILURE flavour of supervision: the value records WHY the row is
/// being cleaned, so the recovery loop settles a spawn failure as `failed`
/// with a failure receipt — never as `stopped`, which is the user's word.
pub(crate) fn set_failure_cleanup_marker(conn: &Connection, turn_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE bridge_turns SET cleanup_pending = 2 WHERE turn_id = ?1",
        params![turn_id],
    )?;
    Ok(())
}

/// Settle a supervised (running/stopping) row into the given terminal —
/// which one is the CALLER's decision, made from the persisted intent.
pub(crate) fn settle_supervised_turn(
    conn: &Connection,
    turn_id: &str,
    terminal: &str,
    now: u64,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE bridge_turns SET status = ?2, completed_at = ?3
         WHERE turn_id = ?1 AND status IN ('running', 'stopping')",
        params![turn_id, terminal, to_sql_i64(now)?],
    )?;
    Ok(changed == 1)
}

/// Supervision is over: the group was proven empty.
pub(crate) fn clear_cleanup_pending(conn: &Connection, turn_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE bridge_turns SET cleanup_pending = 0 WHERE turn_id = ?1",
        params![turn_id],
    )?;
    Ok(())
}

/// Does this turn need the stopping-recovery treatment this cycle — either
/// an explicit `stopping` status, or the cleanup marker of a group nobody
/// has proven empty yet?
pub(crate) fn needs_stop_supervision(conn: &Connection, turn_id: &str) -> Result<bool> {
    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT status, cleanup_pending FROM bridge_turns WHERE turn_id = ?1",
            params![turn_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(matches!(row, Some((status, marker)) if status == "stopping" || marker != 0))
}

/// Settle a spawn-failure turn AND close everything it owns, atomically.
/// A `failed` turn leaves the daemon never sweeping it again — published
/// piecemeal, an approval the turn's first tool call managed to open would
/// sit answerable on the phone for up to a day.
pub(crate) fn settle_unwound_turn_failed(conn: &Connection, turn_id: &str, now: u64) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    // This settlement is only reached with PROOF in hand — the group
    // confirmed empty, or no process ever spawned — which is the one
    // circumstance allowed to override a `stopping` intent: the stop is
    // satisfied vacuously. Left unoverridden, a /stop racing a failed
    // spawn stranded a `stopping` row with NULL identity that probed
    // `Unknown` forever.
    let row: Option<(String, String)> = {
        use rusqlite::OptionalExtension as _;
        tx.query_row(
            "SELECT status, thread_id FROM bridge_turns WHERE turn_id = ?1",
            params![turn_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
    };
    tx.execute(
        "UPDATE bridge_turns
         SET status = 'failed', completed_at = ?2, cleanup_pending = 0
         WHERE turn_id = ?1 AND status IN ('running', 'stopping')",
        params![turn_id, to_sql_i64(now)?],
    )?;
    settle_prompts_for_turn(&tx, turn_id, now)?;
    // A raced /stop may have queued "正在终止" receipts; delivered after
    // this terminal they would promise progress on a settled turn. The
    // REPLACEMENT terminal receipt commits in this same transaction — a
    // withdrawal with no substitute left the user of an interrupted /stop
    // with no durable answer at all when the daemon crashed before any
    // later `final` could be written.
    withdraw_undelivered_stop_chatter(&tx, turn_id, now)?;
    if let Some((status, thread_id)) = row {
        if status == "stopping" {
            let label = crate::telegram::short_thread_id(&thread_id);
            let event = serde_json::json!({
                "type": "bridge_notice",
                "threadId": thread_id,
                "observedAt": now,
                "eventKey": format!("stop-settled:{turn_id}"),
                "message": format!("🧵 {label} — 已停止（清理确认：进程已结束）"),
            });
            enqueue_outbound_event(&tx, &event, now, "bridge")?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Record that a turn is being stopped, BEFORE any signal is sent.
///
/// The intent has to outlive a crash: without it, a daemon dying between the
/// kill and the settle leaves a `running` row that the next cycle reads as
/// an ordinary failure and reports as "exited without producing an answer" —
/// an error message for something the user chose to do.
pub(crate) fn mark_bridge_turn_stopping(conn: &Connection, turn_id: &str, now: u64) -> Result<()> {
    conn.execute(
        "UPDATE bridge_turns SET status = 'stopping', completed_at = ?2
         WHERE turn_id = ?1 AND status = 'running'",
        params![turn_id, to_sql_i64(now)?],
    )?;
    Ok(())
}

/// Promote a turn we proved dead from `stopping` to `stopped`. Returns
/// whether this call was the one that did it.
pub(crate) fn settle_stopping_turn(conn: &Connection, turn_id: &str, now: u64) -> Result<bool> {
    let rows = conn.execute(
        "UPDATE bridge_turns SET status = 'stopped', completed_at = ?2
         WHERE turn_id = ?1 AND status IN ('running', 'stopping')",
        params![turn_id, to_sql_i64(now)?],
    )?;
    Ok(rows > 0)
}

pub(crate) fn claim_bridge_turn_failure(
    conn: &Connection,
    turn_id: &str,
    status: &str,
    now: u64,
    pid_still_missing: bool,
) -> Result<bool> {
    let sql = if pid_still_missing {
        // `cleanup_pending = 0`: a row under cleanup supervision is NOT an
        // unexplained no-pid crash — it is a group nobody has proven empty,
        // and this no-evidence claim would bury its survivors 10 seconds
        // after the cleanup path itself refused to. `cgroup_path IS NULL`:
        // a cgroup-owned turn is never unexplained either — its object can
        // be PROBED, and burying it would strand live members.
        "UPDATE bridge_turns SET status = ?2, completed_at = ?3
         WHERE turn_id = ?1 AND status = 'running' AND pid IS NULL
           AND cleanup_pending = 0 AND cgroup_path IS NULL"
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
        // Never overwrite `stopping`. A turn whose log happened to produce an
        // answer just before the user stopped it is still a STOPPED turn:
        // letting `done`/`error` win here would erase the intent and, with
        // it, the daemon's obligation to keep confirming the process died.
        "UPDATE bridge_turns SET status = ?2, completed_at = ?3
         WHERE turn_id = ?1 AND status != 'stopping'",
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
    // Plain INSERT on purpose: a duplicate approval id must ERROR, never
    // silently re-open a decided row with a NULL decision. Publication
    // handles the legitimate duplicate (an interrupted gate re-run) by
    // checking first; anything else reaching a collision is a bug worth
    // hearing about.
    conn.execute(
        "INSERT INTO pending_approvals(
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

/// Where an approval stands for someone who is answering it NOW.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApprovalStanding {
    /// Inside its window and undecided: a button press would be recorded.
    Open,
    /// Past its deadline (whether or not the gate has stamped it yet).
    Expired,
    /// Already answered.
    Decided,
}

/// The same three-way split `record_approval_decision` applies to a tap,
/// computed without writing anything.
///
/// Reading `decision` alone misses the gap between the deadline passing and
/// the gate stamping `"expired"`: in that gap a tap is correctly told the
/// window is closed while a text reply used to be told to "press the
/// buttons" on a dialog that could no longer accept one.
pub(crate) fn approval_standing(
    conn: &Connection,
    approval_id: &str,
    now: u64,
) -> Result<Option<ApprovalStanding>> {
    let row: Option<(Option<String>, i64)> = conn
        .query_row(
            "SELECT decision, expires_at FROM pending_approvals WHERE approval_id = ?1",
            params![approval_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((decision, expires_at)) = row else {
        return Ok(None);
    };
    Ok(Some(match decision.as_deref() {
        Some("expired") => ApprovalStanding::Expired,
        Some(_) => ApprovalStanding::Decided,
        // Same deadline arithmetic as the recording path, so a tap and a
        // reply landing in the same millisecond cannot disagree.
        None => {
            let expires_at = from_sql_i64(expires_at)?;
            if expires_at > 0 && timestamp_to_millis(now) > timestamp_to_millis(expires_at) {
                ApprovalStanding::Expired
            } else {
                ApprovalStanding::Open
            }
        }
    }))
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

/// cgroup regime: bind the turn to the ownership object created for it —
/// in the SAME transaction as its registration, so the row never exists
/// without naming the kernel object that supervises it.
pub(crate) fn record_turn_cgroup(conn: &Connection, turn_id: &str, path: &str) -> Result<()> {
    conn.execute(
        // The birth debt is VOID for a cgroup-owned turn: the ownership
        // object supervises it from before the process exists, so there is
        // nothing an identity write still has to secure.
        "UPDATE bridge_turns SET cgroup_path = ?2, cleanup_pending = 0 WHERE turn_id = ?1",
        params![turn_id, path],
    )?;
    Ok(())
}

/// Record which bridge turn an approval belongs to.
///
/// Kept separate from `create_pending_approval` on purpose: only the
/// headless gate knows the turn, and threading an extra parameter through a
/// constructor with eighteen call sites buys nothing but churn.
pub(crate) fn record_approval_turn_owner(
    conn: &Connection,
    approval_id: &str,
    turn_id: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE pending_approvals SET turn_id = ?2 WHERE approval_id = ?1",
        params![approval_id, turn_id],
    )?;
    Ok(())
}

/// Close the approvals a stopped bridge turn owned, withdrawing their queued
/// buttons in the same breath. Returns how many were closed.
///
/// Ownership is by TURN, not by thread. Two things share a thread's
/// namespace and must not be swept up: an approval belonging to a CONCURRENT
/// turn of the same session, and questions — `AskUserQuestion` does not
/// exist in headless mode at all, so a question on this thread came from the
/// user's own terminal and is none of our business.
///
/// A row with no recorded owner FAILS OPEN (left untouched): closing
/// something we cannot prove belonged to this turn is the worse mistake.
pub(crate) fn settle_prompts_for_turn(conn: &Connection, turn_id: &str, now: u64) -> Result<usize> {
    // EVERY approval the turn owns, not just the unsettled ones. An approval
    // that already timed out on its own may still have an undelivered push
    // sitting on the retry schedule, and delivering that button after the
    // turn is gone is exactly the late-dead-button this exists to prevent.
    // Withdrawing the push and updating the decision are therefore separate
    // questions, asked separately.
    let rows: Vec<(String, Option<String>)> = {
        let mut stmt =
            conn.prepare("SELECT approval_id, decision FROM pending_approvals WHERE turn_id = ?1")?;
        let rows = stmt.query_map(params![turn_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut settled = 0usize;
    for (approval_id, decision) in &rows {
        if decision.is_none() {
            // Still open: settle it AND withdraw its push, atomically.
            settle_expired_and_cancel_push_inner(conn, SettleTarget::Approval(approval_id), now)?;
            settled += 1;
        } else {
            // Already decided; only the queued button is still a hazard.
            cancel_pending_push_inner(conn, &format!("approval:{approval_id}"), now)?;
        }
    }
    Ok(settled)
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
        // A successful FULL identity write pays off the birth debt: from
        // here the ordinary lifecycle owns the turn.
        "UPDATE bridge_turns
         SET pid = ?2, proc_start = ?3, proc_exe = ?4, pgid = ?5,
             proc_start_ticks = ?6, boot_id = ?7, cleanup_pending = 0
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

/// How long after a completion push its idle-reminder echo is still
/// recognisable. The reminder fires ~60s after the completion; retries and
/// delivery lag get generous margin, but a completion older than this can no
/// longer vouch that an identical wait is an echo rather than a new turn
/// that happens to end in the same words.
pub(crate) const IDLE_REMINDER_ECHO_WINDOW_MS: u64 = 5 * 60 * 1000;

/// The lastPreview of the completion push that would make an identical idle
/// reminder redundant — used to recognise the 60s reminder that repeats,
/// word for word, the completion push sent a minute earlier.
///
/// Returns Some only when the MOST RECENT delivered push for the thread is a
/// `thread_completed` delivered within the echo window. Both constraints are
/// causal, not cosmetic: if anything was delivered after the completion, or
/// the completion is old, an identical-looking wait is a NEW wait (e.g. a
/// `events="thread_waiting"` config never delivers completions at all, so a
/// prior delivered WAIT with the same text must never suppress the next
/// one). Any doubt returns None — fail open to noise, never to silence.
pub(crate) fn last_delivered_completion_preview(
    conn: &Connection,
    thread_id: &str,
    now: u64,
) -> Result<Option<String>> {
    // A delivery batch stamps every row with the same `delivered_at`, so the
    // timestamp alone cannot say which push the user saw LAST. The
    // tie-breakers replay the delivery loop's own order (`DELIVER_DUE_
    // OUTBOUND_SQL` walks created_at ASC, event_id ASC) — the row it sent
    // last is the maximum under that order, and if that row is not the
    // completion, something else reached the user after it.
    let row: Option<(String, String, i64)> = conn
        .query_row(
            "SELECT event_type, payload_json, delivered_at FROM outbound_events
             WHERE thread_id = ?1 AND delivered_at IS NOT NULL AND status = 'delivered'
             ORDER BY delivered_at DESC, created_at DESC, event_id DESC LIMIT 1",
            params![thread_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((event_type, payload, delivered_at)) = row else {
        return Ok(None);
    };
    if event_type != "thread_completed" {
        return Ok(None);
    }
    let delivered_at = from_sql_i64(delivered_at)?;
    if now.saturating_sub(delivered_at) > IDLE_REMINDER_ECHO_WINDOW_MS {
        return Ok(None);
    }
    Ok(serde_json::from_str::<serde_json::Value>(&payload)
        .ok()
        .and_then(|event| {
            event
                .get("lastPreview")
                .or_else(|| event.pointer("/thread/lastPreview"))
                .and_then(serde_json::Value::as_str)
                .map(|preview| preview.trim().to_string())
        }))
}

/// The two queries the daemon runs on EVERY tick. They are constants so the
/// EXPLAIN QUERY PLAN test pins the access path of the REAL query text: both
/// must keep the literal active-status term that matches the
/// partial index's WHERE clause, or SQLite silently falls back to walking
/// the full delivered history 2×/tick.
pub(crate) const PENDING_OUTBOUND_COUNT_SQL: &str =
    "SELECT COUNT(*) FROM outbound_events WHERE status IN ('pending', 'failed')";
/// How long a delivery claim is honoured before another cycle may take the
/// row back. A claim without an expiry is a permanent lock: a daemon killed
/// between claiming and sending would leave the row claimed forever, never
/// delivered — and because the batch query is `LIMIT 100`, a hundred such
/// orphans would starve every notification behind them for good.
/// DELIVERY SEMANTICS: **at-least-once**, chosen deliberately.
///
/// The send and the transport-log write cannot be one atomic step — the
/// first crosses the network, the second is local — so a daemon killed
/// between them leaves an undecidable window: Telegram accepted the message
/// but nothing here knows it. The recovery path re-sends, which can show the
/// user the same request twice.
///
/// The alternative (log first, then send = at-most-once) trades a visible
/// duplicate for a silent disappearance, and the whole point of this outbox
/// is that a request the user must answer never vanishes. A duplicate is
/// obvious and answerable; a lost approval looks exactly like a session that
/// hung. One-answer-per-row means the second copy is harmless: tapping it
/// gets "已经处理过了".
pub(crate) const CLAIM_LEASE_MS: u64 = 5 * 60 * 1000;

/// Rows a cycle may take: due, undelivered, and either unclaimed or holding
/// a claim whose lease has run out. Excluding LIVE claims keeps a batch of
/// in-flight rows from consuming the limit that later notifications need.
pub(crate) const DELIVER_DUE_OUTBOUND_SQL: &str = "SELECT event_id, payload_json, attempts
     FROM outbound_events
     WHERE status IN ('pending', 'failed') AND next_attempt_at <= ?1
       AND (claimed_at IS NULL OR claimed_at <= ?3)
     ORDER BY created_at ASC, event_id ASC
     LIMIT ?2";

pub(crate) fn pending_outbound_count(conn: &Connection) -> Result<u64> {
    let count: i64 = conn.query_row(PENDING_OUTBOUND_COUNT_SQL, [], |row| row.get(0))?;
    from_sql_i64(count)
}

/// /back clears the away-notification backlog only. Answers to turns the user
/// started from Telegram (origin 'bridge') stay queued: they were explicitly
/// requested and must survive a delivery failure followed by /back.
/// What settling a prompt from inside the gate actually found.
pub(crate) enum SettleOutcome {
    /// A Telegram answer had already landed. It stands — the phone showed
    /// the user "已接受" and the hook must honour exactly that.
    Answered(String),
    /// Nobody answered remotely; the request is now expired and any unsent
    /// push was withdrawn.
    Expired,
}

pub(crate) enum SettleTarget<'a> {
    Approval(&'a str),
    Question(&'a str),
}

/// Settle a prompt the gate has decided it no longer owns, and withdraw its
/// push — as ONE transaction, because the two halves contradict each other
/// if only one lands: a settled request whose button still ships, or a
/// withdrawn button for a request still waiting.
///
/// The linearization point against the daemon is the outbound row's claim.
/// Cancelling only touches rows that are unclaimed AND carry no transport
/// record: once delivery has claimed a row it may already be on the wire, so
/// the honest move is to leave it and let the callback answer "已超时".
/// Withdraw a queued push identified by its `eventKey`, honouring the claim
/// and transport-log rules: a row inside a live delivery lease cannot be
/// deleted (its owner may already be mid-send), and one that already reached
/// Telegram is left alone. Both cases fall back to recording the intent.
///
/// Separate from settling a decision on purpose: an approval that already
/// timed out still needs its button withdrawn, and asking both questions
/// together made the second one unreachable.
pub(crate) fn cancel_pending_push_inner(
    conn: &Connection,
    event_key: &str,
    now: u64,
) -> Result<()> {
    let stale_cutoff = to_sql_i64(now.saturating_sub(CLAIM_LEASE_MS))?;
    // Withdraw outright what nobody is holding.
    conn.execute(
        "DELETE FROM outbound_events
         WHERE delivered_at IS NULL
           AND status IN ('pending', 'failed')
           AND (claimed_at IS NULL OR claimed_at <= ?2)
           AND json_valid(payload_json)
           AND json_extract(payload_json, '$.eventKey') = ?1
           AND NOT EXISTS (
             SELECT 1 FROM transport_delivery_log
             WHERE transport_delivery_log.event_id = outbound_events.event_id
           )",
        params![event_key, stale_cutoff],
    )?;
    // A row inside a live lease cannot be deleted — its owner may already be
    // mid-send. But that owner can also be DEAD (claimed, then the daemon
    // was killed), and five minutes later the reclaim would post a button
    // for a request settled long ago. Record the intent instead: whoever
    // ends up holding the row honours it before sending.
    conn.execute(
        "UPDATE outbound_events SET cancel_requested = 1
         WHERE delivered_at IS NULL
           AND status IN ('pending', 'failed')
           AND json_valid(payload_json)
           AND json_extract(payload_json, '$.eventKey') = ?1",
        params![event_key],
    )?;
    Ok(())
}

pub(crate) fn settle_expired_and_cancel_push(
    conn: &Connection,
    target: SettleTarget<'_>,
    now: u64,
) -> Result<SettleOutcome> {
    let tx = conn.unchecked_transaction()?;
    let outcome = settle_expired_and_cancel_push_inner(conn, target, now)?;
    tx.commit()?;
    Ok(outcome)
}

/// The body of `settle_expired_and_cancel_push` WITHOUT its own transaction,
/// for callers already holding one — SQLite has no nested transactions, so
/// settling several rows atomically has to drive this directly.
pub(crate) fn settle_expired_and_cancel_push_inner(
    conn: &Connection,
    target: SettleTarget<'_>,
    now: u64,
) -> Result<SettleOutcome> {
    let (answer, event_key) = match target {
        SettleTarget::Approval(id) => (
            expire_or_take_decision(conn, id, now)?,
            format!("approval:{id}"),
        ),
        SettleTarget::Question(id) => (
            expire_or_take_answer(conn, id, now)?,
            format!("question:{id}"),
        ),
    };
    if let Some(answer) = answer {
        // A tap won the race. Keep its push exactly as it is.
        return Ok(SettleOutcome::Answered(answer));
    }
    cancel_pending_push_inner(conn, &event_key, now)?;
    Ok(SettleOutcome::Expired)
}

pub(crate) fn clear_pending_outbound_events(conn: &Connection) -> Result<usize> {
    let deleted = conn.execute(
        "DELETE FROM outbound_events WHERE status IN ('pending', 'failed') AND origin = 'away'",
        [],
    )?;
    Ok(deleted)
}

/// When this event already reached the transport, and at what time the SEND
/// actually happened. The timestamp is the authority on delivery order:
/// `outbound_events.delivered_at` is written after the send, so a crash
/// between the two leaves the outbound row to be stamped by whichever later
/// cycle notices — long after the user saw the message. Ordering pushes by
/// that later stamp would put a crash-orphaned completion *after* pushes the
/// user genuinely received later.
pub(crate) fn transport_delivered_at(
    conn: &Connection,
    event_id: &str,
    transport: &str,
) -> Result<Option<u64>> {
    let delivered_at: Option<i64> = conn
        .query_row(
            "SELECT delivered_at FROM transport_delivery_log WHERE event_id = ?1 AND transport = ?2",
            params![event_id, transport],
            |row| row.get(0),
        )
        .optional()?;
    match delivered_at {
        Some(value) => Ok(Some(from_sql_i64(value)?)),
        None => Ok(None),
    }
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
        let mut stmt = conn.prepare(DELIVER_DUE_OUTBOUND_SQL)?;
        let rows = stmt.query_map(
            params![
                to_sql_i64(now)?,
                limit as i64,
                to_sql_i64(now.saturating_sub(CLAIM_LEASE_MS))?
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?;
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
        // CLAIM the row before sending. The rows above were read in one
        // batch, so between that read and this send a gate may have settled
        // its prompt and withdrawn the push; without a claim the stale
        // in-memory payload would still go out as a dead button. The claim
        // and the gate's cancel contend on the same row, and whichever lands
        // first decides — that is the linearization point.
        // A malformed payload is QUARANTINED, never propagated. Returning
        // `?` here aborted the whole batch and left the bad row first in
        // line next cycle — one corrupt event silently froze every
        // notification behind it, forever.
        let event: Value = match serde_json::from_str(&payload_json) {
            Ok(event) => event,
            Err(error) => {
                eprintln!("tinyctb: quarantining unparseable outbound event {event_id}: {error}");
                conn.execute(
                    "UPDATE outbound_events
                     SET status = 'invalid', last_error = ?2,
                         claimed_at = NULL, claim_token = NULL
                     WHERE event_id = ?1",
                    params![event_id, format!("unparseable payload: {error}")],
                )?;
                summary.failed += 1;
                continue;
            }
        };
        // The claim is a LEASE with an owner token. The lease lets a later
        // cycle reclaim a row whose owner died mid-send; the token makes the
        // dead owner harmless if it ever wakes up, because every settling
        // update below insists on holding the same token.
        let token =
            crate::claude::generate_session_uuid().unwrap_or_else(|_| format!("{event_id}:{now}"));
        let claimed = conn.execute(
            "UPDATE outbound_events SET claimed_at = ?2, claim_token = ?3
             WHERE event_id = ?1
               AND delivered_at IS NULL
               AND status IN ('pending', 'failed')
               AND (claimed_at IS NULL OR claimed_at <= ?4)",
            params![
                event_id,
                to_sql_i64(now)?,
                token,
                to_sql_i64(now.saturating_sub(CLAIM_LEASE_MS))?
            ],
        )?;
        if claimed == 0 {
            continue; // withdrawn, already delivered, or in flight elsewhere
        }
        // Honour a cancellation recorded while this row was claimed by an
        // owner that then died. If the transport log says it never reached
        // Telegram, drop it; if it did, settle it as the delivery it was
        // rather than sending a second copy.
        let cancel_requested: i64 = conn
            .query_row(
                "SELECT cancel_requested FROM outbound_events WHERE event_id = ?1",
                params![event_id],
                |row| row.get(0),
            )
            .unwrap_or(0);
        if cancel_requested != 0 {
            match transport_delivered_at(conn, &event_id, "telegram")? {
                Some(sent_at) => {
                    conn.execute(
                        "UPDATE outbound_events
                         SET status = 'delivered', delivered_at = ?2, claimed_at = NULL,
                             claim_token = NULL
                         WHERE event_id = ?1 AND claim_token = ?3",
                        params![event_id, to_sql_i64(sent_at)?, token],
                    )?;
                }
                None => {
                    conn.execute(
                        "DELETE FROM outbound_events WHERE event_id = ?1 AND claim_token = ?2",
                        params![event_id, token],
                    )?;
                }
            }
            continue;
        }
        summary.attempted += 1;
        match sender(&event) {
            Ok(result) => {
                // A sender that recognised this event as ALREADY sent (its
                // transport log says so) reports when that send actually
                // happened. Stamping `now` instead would date a message the
                // user read before the crash to the recovery cycle, and
                // every "what did the user see last" question ordered by
                // this column would answer wrongly.
                let delivered_at = result
                    .get("deliveredAt")
                    .and_then(Value::as_u64)
                    .unwrap_or(now);
                conn.execute(
                    "UPDATE outbound_events
                     SET status = 'delivered', attempts = attempts + 1, delivered_at = ?2, last_error = NULL
                     WHERE event_id = ?1 AND claim_token = ?3",
                    params![event_id, to_sql_i64(delivered_at)?, token],
                )?;
                summary.delivered += 1;
            }
            Err(error) => {
                let next_attempts = from_sql_i64(attempts)?.saturating_add(1);
                let next_attempt_at = now.saturating_add(retry_delay_ms(next_attempts));
                // Release the claim: a failed send must be re-claimable
                // when its retry comes due, or the row would be stranded.
                conn.execute(
                    "UPDATE outbound_events
                     SET status = 'failed', attempts = ?2, next_attempt_at = ?3, last_error = ?4,
                         claimed_at = NULL, claim_token = NULL
                     WHERE event_id = ?1 AND claim_token = ?5",
                    params![
                        event_id,
                        to_sql_i64(next_attempts)?,
                        to_sql_i64(next_attempt_at)?,
                        format!("{error:#}"),
                        token
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

    /// What `/threads` actually orders by. Asserting on the raw column would
    /// have missed the point: the answer is composed from two kinds of
    /// evidence, and this is the composition.
    fn effective_recency(conn: &Connection, thread: &str) -> Option<i64> {
        conn.query_row(
            &format!("SELECT {EFFECTIVE_RECENCY_SQL} FROM threads_cache t WHERE t.thread_id = ?1"),
            params![thread],
            |row| row.get(0),
        )
        .optional()
        .expect("row")
    }

    /// A row that v0.2.7 pushed to "today" from a file mtime must be able to
    /// come back DOWN. Ordering by a value that only ever moves forward left
    /// every such row pinned at the top of `/threads` for good — the very
    /// thing this release exists to fix, and unreachable if the fix only
    /// applies to rows that do not exist yet.
    #[test]
    fn a_measurement_corrects_a_row_that_was_pushed_forward() {
        let conn = create_state_db_in_memory().expect("db");
        let snapshot = |at: u64| BridgeThreadSnapshot {
            thread_id: "sess-inflated".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(at),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        };
        let spoke_at = 1_000u64;
        let inflated = 9_000u64;

        // What v0.2.7 left behind: a guess from the file's mtime.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(inflated),
            inflated,
            UpdatedAt::Guessed,
            None,
            None,
            None,
        )
        .expect("inflated");
        assert_eq!(
            effective_recency(&conn, "sess-inflated"),
            Some(inflated as i64),
            "with nothing but a guess, the guess is all there is"
        );

        // A scan that MEASURED the transcript corrects it downward.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(spoke_at),
            9_500,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("measured");
        assert_eq!(
            effective_recency(&conn, "sess-inflated"),
            Some(spoke_at as i64),
            "a measurement is a reading of the file as it is, and it supersedes a guess"
        );

        // An OBSERVATION still only moves forward: a late hook may not drag
        // the row back.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(500),
            9_600,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("late observation");
        assert_eq!(
            effective_recency(&conn, "sess-inflated"),
            Some(spoke_at as i64),
            "an observation that arrives late is still an older observation"
        );
    }

    /// A v0.2.7 row holding a REAL Stop must not be dropped by the first
    /// measurement below it. That column mixed scans and hooks, so the row
    /// itself cannot say which it was — but history can, IF the right time
    /// is taken from it: `observed_at` is when reconcile happened to run,
    /// while the time the hook SAW is in the payload. A backlog processed
    /// today would otherwise have backfilled ten-day-old activity as today's
    /// — this release's own failure, rebuilt by its own migration.
    #[test]
    fn a_legacy_row_keeps_the_floor_its_events_prove() {
        let path = std::env::temp_dir().join(format!("tinyctb-legacy-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            // A database as v0.2.7 left it. The event was SEEN at 1000 and
            // RECONCILED at 9000 — a backlog, processed long afterwards.
            let conn = create_state_db(&path).expect("db");
            conn.execute_batch(
                "INSERT INTO threads_cache(thread_id, status_type, status_flags_json,
                                           updated_at, last_seen_at)
                 VALUES ('sess-legacy', 'idle', '[]', 9000, 9000);
                 INSERT INTO thread_events(event_key, thread_id, event_type, observed_at,
                                           payload_json)
                 VALUES ('thread_completed:sess-legacy:1', 'sess-legacy', 'thread_completed',
                         9000, '{\"type\":\"thread_completed\",\"updatedAt\":1000}');
                 -- A thread whose events were never worth notifying about, so
                 -- nothing was recorded for it at all.
                 INSERT INTO threads_cache(thread_id, status_type, status_flags_json,
                                           updated_at, last_seen_at)
                 VALUES ('sess-nofloor', 'idle', '[]', 9000, 9000);
                 UPDATE threads_cache SET last_observed_at = NULL;",
            )
            .expect("legacy shape");
        }

        // Opening it again runs the migration.
        let conn = create_state_db(&path).expect("reopen");
        let floor: Option<i64> = conn
            .query_row(
                "SELECT last_observed_at FROM threads_cache WHERE thread_id = 'sess-legacy'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(
            floor,
            Some(1_000),
            "the floor is when the hook SAW it, not when a backlog got round to it"
        );

        // A measurement above the floor is taken; the floor is not a ceiling.
        let snapshot = |thread: &str, at: u64| BridgeThreadSnapshot {
            thread_id: thread.to_string(),
            name: None,
            cwd: None,
            updated_at: Some(at),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        };
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot("sess-legacy", 2_000),
            9_500,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("first scan after the upgrade");
        assert_eq!(
            effective_recency(&conn, "sess-legacy"),
            Some(2_000),
            "the measurement is the ground truth for when the session last spoke"
        );

        // And below it, the floor holds.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot("sess-legacy", 500),
            9_600,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("a lower reading");
        assert_eq!(
            effective_recency(&conn, "sess-legacy"),
            Some(1_000),
            "an upgrade may not lose what a hook had already seen"
        );

        // A row with nothing to recover from gets no floor — and that is
        // the honest outcome, not a regression: the measurement is what says
        // when the session spoke, and the floor only guards against reading
        // a transcript before it was flushed.
        let floor: Option<i64> = conn
            .query_row(
                "SELECT last_observed_at FROM threads_cache WHERE thread_id = 'sess-nofloor'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(floor, None, "nothing was recorded, so nothing is invented");
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot("sess-nofloor", 1_000),
            9_700,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("scan");
        assert_eq!(
            effective_recency(&conn, "sess-nofloor"),
            Some(1_000),
            "and the measurement corrects it, which is the whole point"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// A scan may not undo a hook. It learns status, turn state and the
    /// prompt by READING THE ROW and writing them back, so what it writes is
    /// whatever the row said when it started — a hook landing in between was
    /// simply erased. No file race is needed for this: a scan and a hook in
    /// the same second is enough.
    #[test]
    fn a_scan_does_not_write_back_what_a_hook_has_since_changed() {
        let conn = create_state_db_in_memory().expect("db");
        let row = |status: &str, turn: Option<&str>, prompt: Option<&str>| BridgeThreadSnapshot {
            thread_id: "sess-authority".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(1_000),
            status_type: status.to_string(),
            status_flags: Vec::new(),
            last_turn_status: turn.map(str::to_string),
            last_preview: None,
            pending_prompt: prompt.map(|id| PendingPrompt {
                prompt_id: id.to_string(),
                kind: "reply".to_string(),
                status: "pending".to_string(),
                question: Some("在等你".to_string()),
                transcript_bytes: None,
                notification_type: None,
            }),
            event_uid: None,
        };
        let prompt_now = |conn: &Connection| -> Option<String> {
            conn.query_row(
                "SELECT prompt_id FROM pending_prompts WHERE thread_id = 'sess-authority'",
                [],
                |r| r.get(0),
            )
            .optional()
            .expect("row")
        };
        let status_now = |conn: &Connection| -> (String, Option<String>) {
            conn.query_row(
                "SELECT status_type, last_turn_status FROM threads_cache
                  WHERE thread_id = 'sess-authority'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("row")
        };

        // The state a scan read a moment ago: idle, finished, no prompt.
        let _ = upsert_thread_snapshot(
            &conn,
            &row("idle", Some("completed"), None),
            1_000,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("seed");

        // A hook lands: the session is asking something now.
        let _ = upsert_thread_snapshot(
            &conn,
            &row("active", None, Some("notify:new")),
            2_000,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("hook");
        assert_eq!(prompt_now(&conn).as_deref(), Some("notify:new"));

        // The scan, holding what it read BEFORE the hook, writes its answer.
        // It carries the prompt IT read — which is a statement about what it
        // saw, not about what is there now.
        let _ = upsert_thread_snapshot(
            &conn,
            &row("idle", Some("completed"), Some("notify:old")),
            2_100,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("scan");
        assert_eq!(
            prompt_now(&conn).as_deref(),
            Some("notify:new"),
            "a scan that never saw this prompt may not delete it"
        );
        assert_eq!(
            status_now(&conn),
            ("active".to_string(), None),
            "nor put back the status and turn state the hook has since changed"
        );

        // What a scan MAY do: retire the INSTANCE it actually read and found
        // answered. Not the name — `notify:{received_at}` repeats inside a
        // millisecond, so a name would clear whatever holds it now.
        let revision_now = |conn: &Connection| -> i64 {
            conn.query_row(
                "SELECT revision FROM pending_prompts WHERE thread_id = 'sess-authority'",
                [],
                |r| r.get(0),
            )
            .expect("row")
        };
        let live = revision_now(&conn);
        let _ = upsert_thread_snapshot(
            &conn,
            &row("idle", Some("completed"), None),
            2_200,
            UpdatedAt::Measured,
            Some(live - 1),
            None,
            None,
        )
        .expect("stale clear");
        assert_eq!(
            prompt_now(&conn).as_deref(),
            Some("notify:new"),
            "an instance this scan never read is not its to retire"
        );

        // Same NAME, new instance: a second Notification inside the same
        // millisecond. A scan holding the first instance must not touch it.
        let _ = upsert_thread_snapshot(
            &conn,
            &row("active", None, Some("notify:new")),
            2_250,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("same name, new instance");
        let replaced = revision_now(&conn);
        assert_ne!(
            replaced, live,
            "a rewrite is a new instance, never the old one"
        );
        let _ = upsert_thread_snapshot(
            &conn,
            &row("idle", Some("completed"), None),
            2_275,
            UpdatedAt::Measured,
            Some(live),
            None,
            None,
        )
        .expect("clear the instance that is gone");
        assert_eq!(
            prompt_now(&conn).as_deref(),
            Some("notify:new"),
            "the name is the same, the instance is not — and the instance is what counts"
        );

        let _ = upsert_thread_snapshot(
            &conn,
            &row("idle", Some("completed"), None),
            2_300,
            UpdatedAt::Measured,
            Some(replaced),
            None,
            None,
        )
        .expect("clear");
        assert_eq!(
            prompt_now(&conn),
            None,
            "the prompt it did read, and did find answered, it may retire"
        );
    }

    /// Metadata has owners too. A hook that has no transcript to hand
    /// carries neither name nor cwd — silence, not an erasure — and writing
    /// its `None` in wiped what a scan had read off the file. And a Stop
    /// whose answer has not reached the transcript yet must not have its
    /// preview replaced by the previous one, read from a file that had not
    /// caught up.
    #[test]
    fn metadata_has_owners_and_silence_erases_nothing() {
        let conn = create_state_db_in_memory().expect("db");
        let row = |name: Option<&str>, preview: Option<&str>, at: u64| BridgeThreadSnapshot {
            thread_id: "sess-meta".to_string(),
            name: name.map(str::to_string),
            cwd: name.map(|_| "/work".to_string()),
            updated_at: Some(at),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: preview.map(str::to_string),
            pending_prompt: None,
            event_uid: None,
        };
        let meta = |conn: &Connection| -> (Option<String>, Option<String>, Option<String>) {
            conn.query_row(
                "SELECT name, cwd, last_preview FROM threads_cache WHERE thread_id = 'sess-meta'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .expect("row")
        };

        // A scan read a name, a cwd and a preview off the transcript.
        let _ = upsert_thread_snapshot(
            &conn,
            &row(Some("项目"), Some("旧答复"), 1_000),
            1_000,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("scan");
        assert_eq!(
            meta(&conn),
            (
                Some("项目".into()),
                Some("/work".into()),
                Some("旧答复".into())
            )
        );

        // A hook with no transcript to hand: it knows nothing about the name.
        let _ = upsert_thread_snapshot(
            &conn,
            &row(None, Some("新答复"), 2_000),
            2_000,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("hook");
        assert_eq!(
            meta(&conn),
            (
                Some("项目".into()),
                Some("/work".into()),
                Some("新答复".into())
            ),
            "silence about the name is not an instruction to forget it"
        );

        // A scan of a transcript that has not caught up yet: its reading is
        // older than what was seen, so its preview is not the current one.
        let _ = upsert_thread_snapshot(
            &conn,
            &row(Some("项目"), Some("旧答复"), 1_500),
            2_100,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("lagging scan");
        assert_eq!(
            meta(&conn).2,
            Some("新答复".into()),
            "a file that had not caught up may not undo an answer that was seen"
        );

        // Once the transcript catches up, its reading is current again.
        let _ = upsert_thread_snapshot(
            &conn,
            &row(Some("项目"), Some("更新的答复"), 3_000),
            3_000,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("caught up");
        assert_eq!(meta(&conn).2, Some("更新的答复".into()));
    }

    /// A hook that could not be read is kept for the next cycle, so hooks
    /// can arrive OUT OF ORDER. The one that matters: a Stop lands, then the
    /// Notification that came before it. Only `last_observed_at` was a
    /// maximum, so the floor held while everything under it — status, turn
    /// status, preview, the prompt — was written straight back to the older
    /// hook's version, and an answered question reappeared in `/threads`
    /// with no hook left to clear it.
    ///
    /// The effects split by what they CLAIM, not by their age: an old
    /// "waiting" asserts something about now and must not be spoken; an old
    /// "completed" is a fact that happened and is still owed to whoever was
    /// promised it.
    #[test]
    fn a_late_hook_cannot_reopen_what_a_newer_one_closed() {
        let conn = create_state_db_in_memory().expect("db");
        set_setting_text(&conn, "away", "true").expect("away");
        let hook = |at: u64,
                    status: &str,
                    turn: Option<&str>,
                    preview: &str,
                    prompt: Option<&str>,
                    uid: &str| BridgeThreadSnapshot {
            thread_id: "sess-order".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(at),
            status_type: status.to_string(),
            status_flags: Vec::new(),
            last_turn_status: turn.map(str::to_string),
            last_preview: Some(preview.to_string()),
            pending_prompt: prompt.map(|id| PendingPrompt {
                prompt_id: id.to_string(),
                kind: "reply".to_string(),
                status: "pending".to_string(),
                question: Some("要不要继续？".to_string()),
                transcript_bytes: None,
                notification_type: None,
            }),
            event_uid: Some(uid.to_string()),
        };
        let row = |conn: &Connection| -> (String, Option<String>, Option<String>, i64) {
            conn.query_row(
                "SELECT status_type, last_turn_status, last_preview,
                        (SELECT COUNT(*) FROM pending_prompts WHERE thread_id = 'sess-order')
                 FROM threads_cache WHERE thread_id = 'sess-order'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .expect("row")
        };

        // The Stop lands first: the session finished and is asking nothing.
        let done = reconcile_thread_snapshots(
            &conn,
            2_000,
            vec![hook(
                2_000,
                "idle",
                Some("completed"),
                "新答复",
                None,
                "uid-stop",
            )],
            false,
        )
        .expect("stop");
        assert_eq!(
            row(&conn),
            (
                "idle".into(),
                Some("completed".into()),
                Some("新答复".into()),
                0
            )
        );
        assert_eq!(
            done.get("events").and_then(Value::as_array).map(Vec::len),
            Some(1),
            "the completion is announced"
        );

        // Now the Notification that PRECEDED it finally gets read.
        let late = reconcile_thread_snapshots(
            &conn,
            2_100,
            vec![hook(
                1_000,
                "active",
                None,
                "旧答复",
                Some("notify:1000"),
                "uid-notify",
            )],
            false,
        )
        .expect("late notification");
        assert_eq!(
            row(&conn),
            (
                "idle".into(),
                Some("completed".into()),
                Some("新答复".into()),
                0
            ),
            "an overtaken hook may not put the session back to what it was"
        );
        assert_eq!(
            late.get("events").and_then(Value::as_array).map(Vec::len),
            Some(0),
            "and must not ask a question the session has already answered"
        );
        assert_eq!(
            late.get("threads").and_then(Value::as_array).map(Vec::len),
            Some(0),
            "nor hand its stale snapshot back by another door"
        );

        // A late COMPLETION is a different claim: it happened, and the
        // person it was promised to still has not heard it.
        let late_done = reconcile_thread_snapshots(
            &conn,
            2_200,
            vec![hook(
                1_500,
                "idle",
                Some("completed"),
                "更早就完成了",
                None,
                "uid-stop-early",
            )],
            false,
        )
        .expect("late completion");
        assert_eq!(
            late_done
                .get("events")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1),
            "a completion that arrived late is still a completion that happened"
        );
        assert_eq!(
            row(&conn).2,
            Some("新答复".into()),
            "though it still may not restate the present"
        );
    }

    /// Two hooks stamped the SAME millisecond. There is no order to find:
    /// the spool names entries `{received_at}-{pid}-{event}`, and a pid is
    /// not a sequence, so sorting on it would dress an arbitrary order up as
    /// causality. This pins the policy instead of pretending — a tie counts
    /// as CURRENT, in both directions — and the reason is which loss is
    /// recoverable. Accepting a tie can push a question already answered,
    /// and the next scan retires the row. Refusing one can drop a real
    /// question that nothing later re-raises, and an away user never learns
    /// it was asked.
    #[test]
    fn hooks_stamped_the_same_millisecond_are_not_ordered_and_do_not_lose() {
        let hook = |thread: &str,
                    at: u64,
                    status: &str,
                    turn: Option<&str>,
                    prompt: Option<&str>,
                    uid: &str| BridgeThreadSnapshot {
            thread_id: thread.to_string(),
            name: None,
            cwd: None,
            updated_at: Some(at),
            status_type: status.to_string(),
            status_flags: Vec::new(),
            last_turn_status: turn.map(str::to_string),
            last_preview: None,
            pending_prompt: prompt.map(|id| PendingPrompt {
                prompt_id: id.to_string(),
                kind: "reply".to_string(),
                status: "pending".to_string(),
                question: Some("同毫秒".to_string()),
                transcript_bytes: None,
                notification_type: None,
            }),
            event_uid: Some(uid.to_string()),
        };
        let prompt_of = |conn: &Connection, thread: &str| -> Option<(String, i64)> {
            conn.query_row(
                "SELECT prompt_id, revision FROM pending_prompts WHERE thread_id = ?1",
                params![thread],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .expect("prompt")
        };

        // Stop first, then the Notification stamped the same millisecond.
        let conn = create_state_db_in_memory().expect("db");
        set_setting_text(&conn, "away", "true").expect("away");
        reconcile_thread_snapshots(
            &conn,
            1_000,
            vec![hook(
                "sess-tie-a",
                1_000,
                "idle",
                Some("completed"),
                None,
                "uid-1",
            )],
            false,
        )
        .expect("stop");
        let after = reconcile_thread_snapshots(
            &conn,
            1_100,
            vec![hook(
                "sess-tie-a",
                1_000,
                "active",
                None,
                Some("notify:1000"),
                "uid-2",
            )],
            false,
        )
        .expect("tied notification");
        let raised = prompt_of(&conn, "sess-tie-a").expect("a tie is treated as current");
        assert_eq!(raised.0, "notify:1000");
        assert!(
            raised.1 > 0,
            "and it carries a real instance id, so a scan that finds it answered can clear it"
        );
        assert_eq!(
            after.get("events").and_then(Value::as_array).map(Vec::len),
            Some(1),
            "the question reaches the phone rather than being silently dropped"
        );

        // The other direction, same millisecond: the Stop lands second.
        let conn = create_state_db_in_memory().expect("db");
        set_setting_text(&conn, "away", "true").expect("away");
        reconcile_thread_snapshots(
            &conn,
            1_000,
            vec![hook(
                "sess-tie-b",
                1_000,
                "active",
                None,
                Some("notify:1000"),
                "uid-3",
            )],
            false,
        )
        .expect("notification");
        let after = reconcile_thread_snapshots(
            &conn,
            1_100,
            vec![hook(
                "sess-tie-b",
                1_000,
                "idle",
                Some("completed"),
                None,
                "uid-4",
            )],
            false,
        )
        .expect("tied stop");
        assert_eq!(
            prompt_of(&conn, "sess-tie-b"),
            None,
            "a tie is current in this direction too: the Stop clears the prompt"
        );
        assert_eq!(
            after.get("events").and_then(Value::as_array).map(Vec::len),
            Some(1),
            "and the completion is announced"
        );
    }

    /// The high point is only meaningful while the clock moves forward. An
    /// NTP correction steps it back, and one bad `receivedAt` plants a stamp
    /// years ahead by itself — after which every real hook looks late and the
    /// session's status, prompt and routing freeze until the clock catches
    /// up, which for a future stamp is never. A high point ahead of the clock
    /// is damage, not evidence, and the baseline is rebuilt from what is
    /// actually arriving.
    #[test]
    fn a_high_point_ahead_of_the_clock_stops_counting_as_evidence() {
        let conn = create_state_db_in_memory().expect("db");
        let hook = |at: u64, status: &str| BridgeThreadSnapshot {
            thread_id: "sess-clock".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(at),
            status_type: status.to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: Some(status.to_string()),
            pending_prompt: None,
            event_uid: None,
        };
        let seen = |conn: &Connection| -> (String, Option<i64>) {
            conn.query_row(
                "SELECT status_type, last_observed_at FROM threads_cache
                 WHERE thread_id = 'sess-clock'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("row")
        };

        // A hook arrives with a stamp far ahead of everything — a bad clock
        // on the machine that wrote it, or an NTP step about to happen here.
        let _ = upsert_thread_snapshot(
            &conn,
            &hook(10_000, "active"),
            10_000,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("future hook");
        assert_eq!(seen(&conn), ("active".into(), Some(10_000)));

        // The clock is now 9_100, and a real hook is stamped 9_000 — BEFORE
        // the cycle that reads it, which is the normal case and the one a
        // floor of `now` would have swallowed. The stored high point is in
        // the future, so it cannot judge anything; the hook is taken and the
        // floor is rebuilt from it.
        let write = upsert_thread_snapshot(
            &conn,
            &hook(9_000, "idle"),
            9_100,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("hook after the step back");
        assert_eq!(
            write,
            SnapshotWrite::Applied,
            "a session must not freeze because the clock moved backwards"
        );
        assert_eq!(
            seen(&conn),
            ("idle".into(), Some(9_000)),
            "and the floor is REBUILT from what arrived, not maxed against the damage"
        );

        // Rebuilt, not merely disabled: an older hook is refused again.
        let write = upsert_thread_snapshot(
            &conn,
            &hook(8_000, "active"),
            9_200,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("older hook");
        assert_eq!(write, SnapshotWrite::Superseded);
        assert_eq!(
            seen(&conn),
            ("idle".into(), Some(9_000)),
            "the new baseline holds the line the old one used to"
        );
    }

    /// A clock that steps BACKWARDS is the case the repair exists for, and
    /// it only ever fires on the stored high point being ahead of the clock.
    /// Anything that holds the clock up — a `max` against the cycle's start,
    /// say — hides exactly that condition, and then every real hook after
    /// the correction is judged against a mark from before it.
    #[test]
    fn a_step_backwards_lets_the_session_move_again() {
        let conn = create_state_db_in_memory().expect("db");
        conn.execute(
            "INSERT INTO threads_cache(
                thread_id, status_type, status_flags_json, updated_at, last_seen_at,
                last_turn_status, last_preview, last_observed_at)
             VALUES ('sess-back', 'active', '[]', 9000, 9000, NULL, '旧答复', 9000)",
            [],
        )
        .expect("row");
        conn.execute(
            "INSERT INTO pending_prompts(
                thread_id, prompt_id, prompt_kind, prompt_status, question, created_at, revision)
             VALUES ('sess-back', 'notify:8000', 'reply', 'pending', '回拨前的问题', 8000, 1)",
            [],
        )
        .expect("prompt");

        // The clock is now 5000, and a real Stop arrives stamped 5100.
        let write = upsert_thread_snapshot(
            &conn,
            &BridgeThreadSnapshot {
                thread_id: "sess-back".to_string(),
                name: None,
                cwd: None,
                updated_at: Some(5_100),
                status_type: "idle".to_string(),
                status_flags: Vec::new(),
                last_turn_status: Some("completed".to_string()),
                last_preview: Some("新答复".to_string()),
                pending_prompt: None,
                event_uid: Some("uid-back".to_string()),
            },
            5_200,
            UpdatedAt::Observed,
            None,
            None,
            Some(5_100),
        )
        .expect("stop after the step back");

        assert_eq!(
            write,
            SnapshotWrite::Applied,
            "a session must not stay frozen at where an old clock left it"
        );
        let (status, turn, preview, observed, prompts): (
            String,
            Option<String>,
            Option<String>,
            Option<i64>,
            i64,
        ) = conn
            .query_row(
                "SELECT status_type, last_turn_status, last_preview, last_observed_at,
                        (SELECT COUNT(*) FROM pending_prompts WHERE thread_id = 'sess-back')
                 FROM threads_cache WHERE thread_id = 'sess-back'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .expect("row");
        assert_eq!(
            (
                status.as_str(),
                turn.as_deref(),
                preview.as_deref(),
                prompts
            ),
            ("idle", Some("completed"), Some("新答复"), 0),
            "the answer lands and the question it answered is cleared"
        );
        assert_eq!(
            observed,
            Some(5_100),
            "and the floor is rebuilt from the hook that actually arrived"
        );

        // The route moves with it, on a sighting stamped before the cycle
        // that reads it, as every real one is.
        record_session_messaging_socket(
            &conn,
            "sess-back",
            &crate::claude::SessionSocket {
                path: "/run/after.sock".to_string(),
                inode: None,
                boot_id: None,
            },
            5_100,
            5_200,
        )
        .expect("socket after the step back");
        let routed: Option<String> = conn
            .query_row(
                "SELECT messaging_socket FROM threads_cache WHERE thread_id = 'sess-back'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(routed.as_deref(), Some("/run/after.sock"));
    }

    /// The evidence a repair is made against has to last as long as the
    /// batch it is judging. It did not: the first entry wrote its own stamp
    /// over the damaged high point, so the damage was gone by the second
    /// statement and everything after it was measured against whatever that
    /// first entry happened to say — a floor two events had just been
    /// refused by, lowered by one of them.
    #[test]
    fn a_refused_event_cannot_lower_the_floor_it_was_refused_by() {
        let conn = create_state_db_in_memory().expect("db");
        let hook = |at: u64, status: &str, turn: Option<&str>, prompt: Option<&str>, uid: &str| {
            BridgeThreadSnapshot {
                thread_id: "sess-floor".to_string(),
                name: None,
                cwd: None,
                updated_at: Some(at),
                status_type: status.to_string(),
                status_flags: Vec::new(),
                last_turn_status: turn.map(str::to_string),
                last_preview: None,
                pending_prompt: prompt.map(|id| PendingPrompt {
                    prompt_id: id.to_string(),
                    kind: "reply".to_string(),
                    status: "pending".to_string(),
                    question: Some("旧问题".to_string()),
                    transcript_bytes: None,
                    notification_type: None,
                }),
                event_uid: Some(uid.to_string()),
            }
        };

        // A damaged high point, and a transcript reading that is real: the
        // session was demonstrably still talking at 2000.
        conn.execute(
            "INSERT INTO threads_cache(
                thread_id, status_type, status_flags_json, updated_at, last_seen_at,
                last_turn_status, last_preview, last_observed_at, last_record_at)
             VALUES ('sess-floor', 'active', '[]', 2000, 2000,
                     NULL, '现状', 9000, 2000)",
            [],
        )
        .expect("damaged row");

        // Both of these are older than the transcript already proves.
        let result = reconcile_thread_snapshots(
            &conn,
            2_500,
            vec![
                hook(1_000, "active", None, Some("notify:1000"), "uid-a"),
                hook(1_500, "idle", Some("completed"), None, "uid-b"),
            ],
            false,
        )
        .expect("batch");

        let (status, turn, preview, observed, prompts): (
            String,
            Option<String>,
            Option<String>,
            Option<i64>,
            i64,
        ) = conn
            .query_row(
                "SELECT status_type, last_turn_status, last_preview, last_observed_at,
                        (SELECT COUNT(*) FROM pending_prompts WHERE thread_id = 'sess-floor')
                 FROM threads_cache WHERE thread_id = 'sess-floor'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .expect("row");
        assert_eq!(
            (
                status.as_str(),
                turn.as_deref(),
                preview.as_deref(),
                prompts
            ),
            ("active", None, Some("现状"), 0),
            "neither entry is newer than what the transcript proves, so neither may speak"
        );
        assert!(
            observed.is_some_and(|at| at >= 2_000),
            "and the floor they were both refused by must still be standing, got {observed:?}"
        );
        assert_eq!(
            result
                .get("threads")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(0),
            "nor may either be handed back as the present"
        );
    }

    /// The repair must not become a second way in. A high point from the
    /// future makes the stored one unusable — and the next thing to arrive is
    /// as likely to be an entry kept over from a failed read as a fresh hook.
    /// Handing THAT the baseline would let the backlog re-declare the past as
    /// the present: the old question back on the phone, the old socket back
    /// on the reply route.
    #[test]
    fn a_backlog_entry_does_not_inherit_a_broken_baseline() {
        let conn = create_state_db_in_memory().expect("db");
        let old_hook = BridgeThreadSnapshot {
            thread_id: "sess-backlog".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(1_000),
            status_type: "active".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: Some("旧答复".to_string()),
            pending_prompt: Some(PendingPrompt {
                prompt_id: "notify:1000".to_string(),
                kind: "reply".to_string(),
                status: "pending".to_string(),
                question: Some("积压的旧问题".to_string()),
                transcript_bytes: None,
                notification_type: None,
            }),
            event_uid: Some("uid-backlog".to_string()),
        };

        // The row as an older build left it: a high point from the future,
        // and the session long since finished and quiet.
        conn.execute(
            "INSERT INTO threads_cache(
                thread_id, status_type, status_flags_json, updated_at, last_seen_at,
                last_turn_status, last_preview, last_observed_at,
                messaging_socket, socket_observed_at)
             VALUES ('sess-backlog', 'idle', '[]', 9_000_000, 9_000_000,
                     'completed', '新答复', 9_000_000, '/run/new.sock', 9_000_000)",
            [],
        )
        .expect("damaged row");

        // The cycle also holds this session's current hook — which is what a
        // retained entry comes back alongside — so the floor is rebuilt from
        // the batch's newest observation rather than from the entry that
        // happened to be read first.
        let write = upsert_thread_snapshot(
            &conn,
            &old_hook,
            1_100,
            UpdatedAt::Observed,
            None,
            None,
            Some(1_050),
        )
        .expect("backlog hook");
        assert_eq!(
            write,
            SnapshotWrite::Superseded,
            "a broken high point is not a licence for the backlog to speak"
        );
        let (status, turn, preview, prompts): (String, Option<String>, Option<String>, i64) = conn
            .query_row(
                "SELECT status_type, last_turn_status, last_preview,
                        (SELECT COUNT(*) FROM pending_prompts WHERE thread_id = 'sess-backlog')
                 FROM threads_cache WHERE thread_id = 'sess-backlog'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .expect("row");
        assert_eq!(
            (
                status.as_str(),
                turn.as_deref(),
                preview.as_deref(),
                prompts
            ),
            ("idle", Some("completed"), Some("新答复"), 0),
            "the old question must not come back and the old answer must not return"
        );

        // Nor may the backlog's socket take the reply route.
        record_session_messaging_socket(
            &conn,
            "sess-backlog",
            &crate::claude::SessionSocket {
                path: "/run/old.sock".to_string(),
                inode: None,
                boot_id: None,
            },
            1_000,
            1_100,
        )
        .expect("backlog socket");
        let routed: Option<String> = conn
            .query_row(
                "SELECT messaging_socket FROM threads_cache WHERE thread_id = 'sess-backlog'",
                [],
                |row| row.get(0),
            )
            .expect("socket");
        assert_eq!(
            routed.as_deref(),
            Some("/run/new.sock"),
            "a reply must not be sent where the session used to listen"
        );

        // The doubt is RECORDED, not papered over with a fresh timestamp.
        assert_eq!(
            quarantine_future_socket_sightings(&conn, 1_200).expect("quarantine"),
            1
        );
        let (route, seen, doubted): (Option<String>, Option<i64>, Option<i64>) = conn
            .query_row(
                "SELECT messaging_socket, socket_observed_at, socket_unverified_since
                   FROM threads_cache WHERE thread_id = 'sess-backlog'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .expect("row");
        assert_eq!(route.as_deref(), Some("/run/new.sock"));
        assert_eq!(
            seen, None,
            "the unbelievable sighting is removed, not rewritten"
        );
        assert!(doubted.is_some(), "and the doubt is what persists");
        assert_eq!(
            quarantine_future_socket_sightings(&conn, 1_300).expect("again"),
            0,
            "there is nothing left to mark, but the mark already made stands"
        );

        // A REAL hook ends it — stamped before the cycle that reads it, which
        // is the normal case and which a floor of `now` would have refused.
        record_session_messaging_socket(
            &conn,
            "sess-backlog",
            &crate::claude::SessionSocket {
                path: "/run/vouched.sock".to_string(),
                inode: None,
                boot_id: None,
            },
            1_350,
            1_400,
        )
        .expect("fresh sighting");
        let (route, doubted): (Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT messaging_socket, socket_unverified_since
                   FROM threads_cache WHERE thread_id = 'sess-backlog'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("row");
        assert_eq!(
            route.as_deref(),
            Some("/run/vouched.sock"),
            "a hook stamped before the cycle that reads it must still be able to speak"
        );
        assert_eq!(doubted, None, "and vouching for the route ends the doubt");
    }

    /// The same repair for the reply route: a frozen socket sends answers to
    /// where a session used to listen, for as long as the clock lags.
    #[test]
    fn a_socket_sighting_ahead_of_the_clock_is_rebuilt_too() {
        let conn = create_state_db_in_memory().expect("db");
        let socket = |name: &str| crate::claude::SessionSocket {
            path: format!("/run/user/1000/{name}.sock"),
            inode: None,
            boot_id: None,
        };
        let stored = |conn: &Connection| -> Option<String> {
            conn.query_row(
                "SELECT messaging_socket FROM threads_cache WHERE thread_id = 'sess-clock-sock'",
                [],
                |row| row.get(0),
            )
            .expect("row")
        };

        record_session_messaging_socket(
            &conn,
            "sess-clock-sock",
            &socket("future"),
            10_000,
            10_000,
        )
        .expect("future sighting");
        assert_eq!(stored(&conn).as_deref(), Some("/run/user/1000/future.sock"));

        record_session_messaging_socket(&conn, "sess-clock-sock", &socket("real"), 9_000, 9_000)
            .expect("after the step back");
        assert_eq!(
            stored(&conn).as_deref(),
            Some("/run/user/1000/real.sock"),
            "routing must not stay pinned to a sighting from a time that has not happened"
        );

        record_session_messaging_socket(&conn, "sess-clock-sock", &socket("older"), 8_000, 9_100)
            .expect("older sighting");
        assert_eq!(
            stored(&conn).as_deref(),
            Some("/run/user/1000/real.sock"),
            "and the rebuilt baseline still refuses what is genuinely behind it"
        );
    }

    /// `COALESCE` answers "is this value missing?" and nothing else. It does
    /// not ask whether the writer had any business speaking: a scan holding
    /// an OLD non-null name wrote it straight over the hook's new one, and a
    /// GUESS -- whose only clock is the file mtime this whole version exists
    /// to stop trusting -- could put the previous answer back with a `touch`.
    #[test]
    fn a_guess_cannot_outrank_an_observation() {
        let conn = create_state_db_in_memory().expect("db");
        let row = |name: &str, preview: &str, at: u64| BridgeThreadSnapshot {
            thread_id: "sess-rank".to_string(),
            name: Some(name.to_string()),
            cwd: Some(format!("/work/{name}")),
            updated_at: Some(at),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: Some(preview.to_string()),
            pending_prompt: None,
            event_uid: None,
        };
        let meta = |conn: &Connection| -> (String, String, String) {
            conn.query_row(
                "SELECT name, cwd, last_preview FROM threads_cache WHERE thread_id = 'sess-rank'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .expect("row")
        };

        // A transcript with no parseable stamps at all: only a guess is left,
        // and on a row nothing has ever been observed on it may fill the gap.
        let _ = upsert_thread_snapshot(
            &conn,
            &row("旧名", "旧答复", 1_000),
            1_000,
            UpdatedAt::Guessed,
            None,
            None,
            None,
        )
        .expect("guess fills a gap");
        assert_eq!(meta(&conn).2, "旧答复".to_string());

        // A Stop hook: real activity, with the answer in its own payload.
        let _ = upsert_thread_snapshot(
            &conn,
            &row("新名", "新答复", 2_000),
            2_000,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("hook");
        assert_eq!(
            meta(&conn),
            ("新名".into(), "/work/新名".into(), "新答复".into())
        );

        // The file is touched -- backed up, copied, opened by an editor. Its
        // mtime now says 9000 and its contents still say the old answer.
        let _ = upsert_thread_snapshot(
            &conn,
            &row("旧名", "旧答复", 9_000),
            9_000,
            UpdatedAt::Guessed,
            None,
            None,
            None,
        )
        .expect("guess after a touch");
        assert_eq!(
            meta(&conn),
            ("新名".into(), "/work/新名".into(), "新答复".into()),
            "an mtime is not evidence, and may not overrule something seen"
        );

        // A reading that DOES carry record stamps, but older ones: it is
        // talking about a file that has not caught up yet.
        let _ = upsert_thread_snapshot(
            &conn,
            &row("旧名", "旧答复", 1_500),
            2_100,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("lagging reading");
        assert_eq!(
            meta(&conn),
            ("新名".into(), "/work/新名".into(), "新答复".into()),
            "a non-null old value is still an old value"
        );

        // Once the transcript catches up, the reading is current again.
        let _ = upsert_thread_snapshot(
            &conn,
            &row("最新名", "更新的答复", 3_000),
            3_000,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("caught up");
        assert_eq!(
            meta(&conn),
            ("最新名".into(), "/work/最新名".into(), "更新的答复".into())
        );
    }

    /// The queries must RETURN the effective time, not merely order by it.
    /// Ordering with one value and handing back another meant a row could be
    /// selected as 08-12 and then arrive carrying "today" — and `/threads`
    /// sorts a second time on what it was handed, and shows it. The original
    /// failure was still reachable inside the pool the SQL had ordered
    /// correctly.
    #[test]
    fn the_listings_return_the_time_they_ordered_by() {
        let conn = create_state_db_in_memory().expect("db");
        let spoke_at = 1_000u64;
        let inflated = 9_000u64;
        let snapshot = BridgeThreadSnapshot {
            thread_id: "sess-listed".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(inflated),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: Some("completed".to_string()),
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        };
        // A v0.2.7 row: the guess is in `updated_at` and stays there.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot,
            inflated,
            UpdatedAt::Guessed,
            None,
            None,
            None,
        )
        .expect("guess");
        let _ = upsert_thread_snapshot(
            &conn,
            &BridgeThreadSnapshot {
                updated_at: Some(spoke_at),
                ..snapshot.clone()
            },
            9_500,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("measurement");
        let raw: Option<i64> = conn
            .query_row(
                "SELECT updated_at FROM threads_cache WHERE thread_id = 'sess-listed'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(
            raw,
            Some(inflated as i64),
            "the fixture is only meaningful while the legacy column still lies"
        );

        let listed = list_recent_thread_snapshots_from_db(&conn, 10).expect("recent");
        let found = listed
            .iter()
            .find(|item| item.thread_id == "sess-listed")
            .expect("listed");
        assert_eq!(
            found.updated_at,
            Some(spoke_at),
            "what comes back is what was ordered by — anything else re-sorts on a lie"
        );
    }

    /// The backfill runs while the database is being OPENED, so one bad
    /// historical payload must not be able to stop tinyctb from starting —
    /// and a value it cannot read must not be guessed at either.
    #[test]
    fn the_backfill_survives_whatever_history_holds() {
        let path = std::env::temp_dir().join(format!("tinyctb-badjson-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let conn = create_state_db(&path).expect("db");
            conn.execute_batch(
                "INSERT INTO threads_cache(thread_id, status_type, status_flags_json,
                                           updated_at, last_seen_at)
                 VALUES ('sess-bad', 'idle', '[]', 9000, 9000),
                        ('sess-text', 'idle', '[]', 9000, 9000),
                        ('sess-null', 'idle', '[]', 9000, 9000),
                        ('sess-future', 'idle', '[]', 9000, 9000);
                 INSERT INTO thread_events(event_key, thread_id, event_type, observed_at,
                                           payload_json)
                 VALUES ('a', 'sess-bad', 'thread_completed', 9000, '{ not json'),
                        ('b', 'sess-text', 'thread_completed', 9000,
                         '{\"updatedAt\":\"soon\"}'),
                        ('c', 'sess-null', 'thread_completed', 9000, '{\"updatedAt\":null}'),
                        ('d', 'sess-future', 'thread_completed', 9000,
                         '{\"updatedAt\":99999999999999}');
                 UPDATE threads_cache SET last_observed_at = NULL;",
            )
            .expect("history worth surviving");
        }

        // It opens at all — which is the first thing being asserted.
        let conn = create_state_db(&path).expect("a bad payload may not stop the daemon");
        for thread in ["sess-bad", "sess-text", "sess-null", "sess-future"] {
            let floor: Option<i64> = conn
                .query_row(
                    "SELECT last_observed_at FROM threads_cache WHERE thread_id = ?1",
                    params![thread],
                    |row| row.get(0),
                )
                .expect("row");
            assert_eq!(
                floor, None,
                "{thread}: a time that cannot be read is not a time — and a floor from the \
                 future would never be correctable"
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// A transcript rewritten to carry NO stamps must not keep reporting the
    /// time the last version had. "Measured first, guessed second" only held
    /// for a row's first write: a later guess did not clear the measurement,
    /// so a value no longer anywhere in the file went on being served, and
    /// the mtime it should have fallen back to was never used.
    #[test]
    fn a_reading_with_no_stamps_clears_the_measurement_it_replaces() {
        let conn = create_state_db_in_memory().expect("db");
        let snapshot = |at: u64| BridgeThreadSnapshot {
            thread_id: "sess-cleared".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(at),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        };

        // Generation 100 carried a stamp at 1000.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(1_000),
            1_000,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("measured");
        assert_eq!(effective_recency(&conn, "sess-cleared"), Some(1_000));

        // Generation 200 is a truncation: no stamps at all, mtime 9000.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(9_000),
            9_100,
            UpdatedAt::Guessed,
            None,
            None,
            None,
        )
        .expect("guessed");
        assert_eq!(
            effective_recency(&conn, "sess-cleared"),
            Some(9_000),
            "the measurement is not in the file any more, so it is not the answer any more"
        );
        let record: Option<i64> = conn
            .query_row(
                "SELECT last_record_at FROM threads_cache WHERE thread_id = 'sess-cleared'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(record, None, "and it is cleared, not merely outranked");
    }

    /// A file REPLACED by rename can carry an older mtime than the one it
    /// replaced. Comparing generations alone rejected every reading of the
    /// new file, for good — a permanent failure traded for a microsecond
    /// window. A different inode is a different file, and its reading is the
    /// current one whatever its mtime says.
    #[test]
    fn a_replaced_file_is_not_rejected_for_carrying_an_older_mtime() {
        let conn = create_state_db_in_memory().expect("db");
        let snapshot = |at: u64| BridgeThreadSnapshot {
            thread_id: "sess-renamed".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(at),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        };

        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(9_000),
            9_000,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("original file");
        // Replaced by rename: a different inode, an older mtime.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(1_000),
            9_100,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("replacement");
        assert_eq!(
            effective_recency(&conn, "sess-renamed"),
            Some(1_000),
            "a different file is not an older reading of the same one"
        );
    }

    /// The observation FLOOR survives a measurement that moved past it.
    /// Storing only "who wrote the current value" lost it: after
    /// Observed(9000) → Measured(9500), the row simply said "measured", so
    /// Measured(8000) walked straight past 9000 — a time a hook had SEEN
    /// happen. Two kinds of evidence, kept apart, composed when read.
    #[test]
    fn a_measurement_cannot_cross_below_an_observation_it_once_passed() {
        let conn = create_state_db_in_memory().expect("db");
        let snapshot = |at: u64| BridgeThreadSnapshot {
            thread_id: "sess-floor".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(at),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        };

        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(9_000),
            9_000,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("observed");
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(9_500),
            9_600,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("measured past it");
        assert_eq!(effective_recency(&conn, "sess-floor"), Some(9_500));

        // The third step is the one that used to walk past the floor.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(8_000),
            9_700,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("measured below it");
        assert_eq!(
            effective_recency(&conn, "sess-floor"),
            Some(9_000),
            "a reading may fall back to the floor, never below it — 9000 was SEEN"
        );
    }

    /// A measurement must not drag a real OBSERVATION backwards. Knowing
    /// only the arriving value's provenance cannot decide that: a Stop
    /// observed at 9000, then a transcript read at 8000, and the row wrote
    /// down real activity as older than it was — with a daemon and a CLI
    /// running together, an older scan could do it to a hook that had just
    /// landed.
    #[test]
    fn a_measurement_does_not_overwrite_a_real_observation() {
        let conn = create_state_db_in_memory().expect("db");
        let snapshot = |at: u64| BridgeThreadSnapshot {
            thread_id: "sess-observed".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(at),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        };
        let stored_at = |conn: &Connection| effective_recency(conn, "sess-observed");

        // A hook saw real activity at 9000.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(9_000),
            9_000,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("observation");
        // A scan then reads the transcript's last record as 8000.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(8_000),
            9_100,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("measurement");
        assert_eq!(
            stored_at(&conn),
            Some(9_000),
            "a reading of the file may not contradict something that was seen happening"
        );

        // Forward, it may: the session has spoken since.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(9_500),
            9_600,
            UpdatedAt::Measured,
            None,
            None,
            None,
        )
        .expect("newer measurement");
        assert_eq!(stored_at(&conn), Some(9_500));

        // A reading with no stamps clears the measurement and falls back to
        // the mtime — and the OBSERVATION floor still holds the row up.
        let _ = upsert_thread_snapshot(
            &conn,
            &snapshot(500),
            99_000,
            UpdatedAt::Guessed,
            None,
            None,
            None,
        )
        .expect("guess below the floor");
        assert_eq!(
            stored_at(&conn),
            Some(9_000),
            "a mtime below something that was SEEN cannot pull the row under it"
        );
    }
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
                notification_type: None,
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
                notification_type: None,
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
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
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

    /// An install upgrading from v0.2.1 has a `pending_prompts` table with
    /// no `notification_type` column and rows already in it. The migration
    /// must add the column, leave existing rows readable (NULL type → the
    /// daemon fails open and never suppresses them), and let new writes
    /// carry the type.
    #[test]
    fn create_state_db_migrates_legacy_pending_prompts_notification_type() {
        let path = std::env::temp_dir().join(format!(
            "tinyctb-prompt-type-migrate-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let conn = Connection::open(&path).expect("open legacy db");
            conn.execute_batch(
                "
                CREATE TABLE pending_prompts (
                    thread_id TEXT PRIMARY KEY,
                    prompt_id TEXT NOT NULL,
                    prompt_kind TEXT NOT NULL,
                    prompt_status TEXT NOT NULL,
                    question TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );
                INSERT INTO pending_prompts(thread_id, prompt_id, prompt_kind, prompt_status, question, created_at)
                VALUES ('thr_legacy', 'notify:1', 'reply', 'pending', '旧问题', 1000);
                ",
            )
            .expect("create legacy pending_prompts");
        }

        let conn = create_state_db(&path).expect("migrated db");
        let columns = table_columns(&conn, "pending_prompts").expect("columns");
        assert!(columns.contains(&"notification_type".to_string()));
        assert!(columns.contains(&"transcript_bytes".to_string()));

        let legacy: Option<String> = conn
            .query_row(
                "SELECT notification_type FROM pending_prompts WHERE thread_id = 'thr_legacy'",
                [],
                |row| row.get(0),
            )
            .expect("legacy row still readable");
        assert_eq!(
            legacy, None,
            "a pre-migration row has no known type — the daemon must fail open on it"
        );

        // A fresh write through the production upsert carries the type.
        let _ = upsert_thread_snapshot(
            &conn,
            &BridgeThreadSnapshot {
                thread_id: "thr_new".to_string(),
                name: None,
                cwd: None,
                updated_at: Some(2000),
                status_type: "idle".to_string(),
                status_flags: Vec::new(),
                last_turn_status: None,
                last_preview: None,
                pending_prompt: Some(PendingPrompt {
                    prompt_id: "notify:2".to_string(),
                    kind: "reply".to_string(),
                    status: "pending".to_string(),
                    question: Some("新问题".to_string()),
                    transcript_bytes: None,
                    notification_type: Some("idle_prompt".to_string()),
                }),
                event_uid: None,
            },
            2000,
            UpdatedAt::Observed,
            None,
            None,
            None,
        )
        .expect("upsert on migrated db");
        let fresh: Option<String> = conn
            .query_row(
                "SELECT notification_type FROM pending_prompts WHERE thread_id = 'thr_new'",
                [],
                |row| row.get(0),
            )
            .expect("fresh row");
        assert_eq!(fresh.as_deref(), Some("idle_prompt"));
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

        let _ =
            upsert_thread_snapshot(&conn, &waiting, 2000, UpdatedAt::Observed, None, None, None)
                .expect("upsert waiting");
        let _ = upsert_thread_snapshot(&conn, &done, 2000, UpdatedAt::Observed, None, None, None)
            .expect("upsert done");
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

        let _ = upsert_thread_snapshot(&conn, &older, 1200, UpdatedAt::Observed, None, None, None)
            .expect("upsert older");
        let _ = upsert_thread_snapshot(&conn, &newer, 2400, UpdatedAt::Observed, None, None, None)
            .expect("upsert newer");

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
        let _ = upsert_thread_snapshot(&conn, &alpha, 2000, UpdatedAt::Observed, None, None, None)
            .expect("upsert alpha");
        let _ = upsert_thread_snapshot(&conn, &beta, 3000, UpdatedAt::Observed, None, None, None)
            .expect("upsert beta");

        let imported = importable_projects_from_observed(
            &observed_workspaces_from_db(&conn, 10).expect("observed"),
            &[],
        );

        assert_eq!(imported.len(), 2);
        assert_eq!(imported[0].id, "app");
        assert_eq!(imported[1].id, "app-2");
    }

    /// One failing test must stay ONE failing test: the env lock recovers
    /// from poison, so a panic while holding it cannot cascade into every
    /// later test that locks. (The panic printed below is this test's own
    /// probe thread, not a failure.)
    #[test]
    fn a_panicking_lock_holder_does_not_poison_the_suite() {
        let poisoner = std::thread::spawn(|| {
            let _guard = test_env_lock();
            std::panic::panic_any("deliberate poison probe");
        });
        assert!(poisoner.join().is_err(), "the probe thread must panic");
        // Reverting the recovery to `.expect()` turns this acquisition into
        // the very cascade the recovery exists to prevent.
        let _guard = test_env_lock();
    }

    #[test]
    fn live_backend_status_path_uses_bridge_state_directory() {
        let _guard = test_env_lock();
        let home =
            std::env::temp_dir().join(format!("tinyctb-live-state-path-{}", std::process::id()));
        let _ = fs::remove_dir_all(&home);
        fs::create_dir_all(&home).expect("create temp home");
        // RAII, not manual save/restore: the expect() below must not be able
        // to leak a rewritten HOME into every later test.
        let _state_dir = EnvVarGuard::clear("TINYCTB_STATE_DIR");
        let _home = EnvVarGuard::set("HOME", &home);

        let path = live_backend_status_path().expect("live backend status path");

        assert!(path.ends_with(".tinyctb/live-backend.json"));
        let _ = fs::remove_dir_all(&home);
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
    fn outbound_tick_queries_use_the_pending_partial_index() {
        // The daemon runs these two queries on every tick; without the
        // partial index they walk the entire delivered history each time.
        // EXPLAIN QUERY PLAN pins the access path so a query edit that stops
        // matching the index WHERE clause fails loudly here instead of
        // silently costing ~10% of a core in production.
        let conn = create_state_db_in_memory().expect("db");
        for sql in [PENDING_OUTBOUND_COUNT_SQL, DELIVER_DUE_OUTBOUND_SQL] {
            let plan: Vec<String> = {
                let mut stmt = conn
                    .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                    .expect("explain");
                let dummy_params =
                    rusqlite::params_from_iter((0..stmt.parameter_count()).map(|_| 0i64));
                let rows = stmt
                    .query_map(dummy_params, |row| row.get::<_, String>(3))
                    .expect("plan rows");
                rows.collect::<rusqlite::Result<Vec<_>>>().expect("plan")
            };
            assert!(
                plan.iter()
                    .any(|step| step.contains("idx_outbound_events_active")),
                "query not served by the pending partial index: {sql}\nplan: {plan:?}"
            );
        }
    }

    /// Rows quarantined by an older build carry `delivered_at` without ever
    /// having been sent. They must not count as "what the user last saw",
    /// or an idle reminder gets suppressed on the strength of a message
    /// that never left the machine.
    #[test]
    fn a_quarantined_row_is_not_treated_as_the_last_delivery() {
        let conn = create_state_db_in_memory().expect("db");
        // Legacy shape: status 'invalid' AND a delivered_at stamp.
        conn.execute(
            "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at, delivered_at)
             VALUES ('evt_legacy', 'thread_waiting', 'sess', ?1, 'invalid', 0, 2000, 2500)",
            params![json!({"lastPreview": "从未送达"}).to_string()],
        )
        .expect("legacy row");
        conn.execute(
            "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at, delivered_at)
             VALUES ('evt_real', 'thread_completed', 'sess', ?1, 'delivered', 0, 1000, 1500)",
            params![json!({"lastPreview": "真的完成推送"}).to_string()],
        )
        .expect("real row");

        assert_eq!(
            last_delivered_completion_preview(&conn, "sess", 2600)
                .expect("query")
                .as_deref(),
            Some("真的完成推送"),
            "a never-sent quarantined row must not outrank the real delivery"
        );
    }

    #[test]
    fn completion_preview_respects_same_batch_delivery_order() {
        // One delivery batch stamps every row with the same delivered_at.
        // "Most recent" must follow the delivery loop's own order
        // (created_at ASC, event_id ASC): if the batch delivered something
        // AFTER the completion, the completion no longer vouches for an
        // identical-looking idle reminder.
        let conn = create_state_db_in_memory().expect("db");
        let insert = |id: &str, etype: &str, created_at: u64| {
            conn.execute(
                "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at, delivered_at)
                 VALUES (?1, ?2, 'sess-batch', ?3, 'delivered', 0, ?4, 1500)",
                params![
                    id,
                    etype,
                    json!({"lastPreview": "完成文本"}).to_string(),
                    to_sql_i64(created_at).expect("created")
                ],
            )
            .expect("insert delivered row");
        };
        // Completion first, then another push in the SAME batch: no vouch.
        insert("evt_completion", "thread_completed", 1000);
        insert("evt_later_answer", "bridge_event", 1100);
        assert_eq!(
            last_delivered_completion_preview(&conn, "sess-batch", 2000).expect("query"),
            None,
            "a same-batch push delivered after the completion must break the vouch"
        );

        // Completion genuinely last in the batch: vouches.
        conn.execute("DELETE FROM outbound_events", [])
            .expect("clear");
        insert("evt_earlier_answer", "bridge_event", 1000);
        insert("evt_completion", "thread_completed", 1100);
        assert_eq!(
            last_delivered_completion_preview(&conn, "sess-batch", 2000)
                .expect("query")
                .as_deref(),
            Some("完成文本"),
        );

        // Last tie-breaker: same delivered_at AND same created_at, so only
        // event_id separates them — exactly how the delivery loop broke the
        // tie when it sent them (created_at ASC, event_id ASC).
        conn.execute("DELETE FROM outbound_events", [])
            .expect("clear");
        insert("evt_a_completion", "thread_completed", 1000);
        insert("evt_b_answer", "bridge_event", 1000);
        assert_eq!(
            last_delivered_completion_preview(&conn, "sess-batch", 2000).expect("query"),
            None,
            "event_id ordering must place evt_b_answer after the completion"
        );

        conn.execute("DELETE FROM outbound_events", [])
            .expect("clear");
        insert("evt_a_answer", "bridge_event", 1000);
        insert("evt_b_completion", "thread_completed", 1000);
        assert_eq!(
            last_delivered_completion_preview(&conn, "sess-batch", 2000)
                .expect("query")
                .as_deref(),
            Some("完成文本"),
            "the completion sent last by event_id order still vouches"
        );
    }

    #[test]
    fn prune_state_logs_covers_outbound_and_telegram_routes() {
        let conn = create_state_db_in_memory().expect("db");
        let retention_ms: u64 = 30 * 24 * 60 * 60 * 1000;
        let now = retention_ms + 2000;
        let insert_outbound = |id: &str, status: &str, next_attempt_at: u64, created_at: u64| {
            conn.execute(
                "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
                 VALUES (?1, 'thread_waiting', 'thr_1', '{}', ?2, ?3, ?4)",
                params![
                    id,
                    status,
                    to_sql_i64(next_attempt_at).expect("next"),
                    to_sql_i64(created_at).expect("created")
                ],
            )
            .expect("insert outbound");
        };
        // Old delivered history goes; an old row still on a LIVE retry
        // schedule stays; one whose retry schedule is itself ancient is
        // abandoned and goes.
        insert_outbound("evt_old_delivered", "delivered", 1000, 1000);
        insert_outbound("evt_old_retrying", "failed", now, 1000);
        insert_outbound("evt_abandoned", "failed", 1000, 1000);
        insert_outbound("evt_recent_delivered", "delivered", now, now);
        for table in [
            "telegram_message_routes",
            "telegram_callback_routes",
            "telegram_command_routes",
        ] {
            for (tag, created_at) in [("old", 1000u64), ("recent", now)] {
                match table {
                    "telegram_message_routes" => conn.execute(
                        "INSERT INTO telegram_message_routes(chat_id, message_id, thread_id, event_id, created_at)
                         VALUES ('c', ?1, 'thr_1', 'evt', ?2)",
                        params![if tag == "old" { 1 } else { 2 }, to_sql_i64(created_at).expect("ts")],
                    ),
                    "telegram_callback_routes" => conn.execute(
                        "INSERT INTO telegram_callback_routes(callback_id, chat_id, thread_id, action, created_at)
                         VALUES (?1, 'c', 'thr_1', 'a', ?2)",
                        params![format!("cb_{tag}"), to_sql_i64(created_at).expect("ts")],
                    ),
                    _ => conn.execute(
                        "INSERT INTO telegram_command_routes(chat_id, message_id, command, created_at)
                         VALUES ('c', ?1, 'threads', ?2)",
                        params![if tag == "old" { 1 } else { 2 }, to_sql_i64(created_at).expect("ts")],
                    ),
                }
                .expect("insert route");
            }
        }

        // Transport log rows: one whose outbound event survives the prune
        // (must stay — it is the idempotence guard against re-sending), one
        // whose event is pruned in this very call (may go), and a recent one.
        for (event_id, delivered_at) in [
            ("evt_old_retrying", 1000u64),
            ("evt_old_delivered", 1000),
            ("evt_recent_delivered", now),
        ] {
            record_transport_delivery(
                &conn,
                event_id,
                "telegram",
                &json!({"ok": true}),
                delivered_at,
            )
            .expect("record transport");
        }

        let removed = prune_state_logs(&conn, now).expect("prune");
        // evt_old_delivered + evt_abandoned + one old row per route table
        // + the one orphaned transport row.
        assert_eq!(removed, 6);
        let transports: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT event_id FROM transport_delivery_log ORDER BY event_id")
                .expect("stmt");
            let rows = stmt
                .query_map([], |row| row.get(0))
                .expect("query")
                .collect::<rusqlite::Result<Vec<String>>>()
                .expect("rows");
            rows
        };
        assert_eq!(
            transports,
            vec!["evt_old_retrying", "evt_recent_delivered"],
            "a transport row whose outbound event still exists must never be pruned — \
             dropping it would re-send a message the user already read"
        );
        let surviving: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT event_id FROM outbound_events ORDER BY event_id")
                .expect("stmt");
            let rows = stmt
                .query_map([], |row| row.get(0))
                .expect("query")
                .collect::<rusqlite::Result<Vec<String>>>()
                .expect("rows");
            rows
        };
        assert_eq!(surviving, vec!["evt_old_retrying", "evt_recent_delivered"]);
        for table in [
            "telegram_message_routes",
            "telegram_callback_routes",
            "telegram_command_routes",
        ] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .expect("count");
            assert_eq!(count, 1, "{table} keeps only the recent row");
        }
    }

    /// The contract the gate depends on when it settles itself.
    /// An approval that already timed out on its own can still have an
    /// undelivered push on the retry schedule. `/stop` must withdraw that
    /// too — skipping settled rows meant the button shipped minutes after
    /// the turn it belonged to was gone.
    #[test]
    fn stopping_a_turn_withdraws_buttons_of_already_expired_approvals() {
        let conn = create_state_db_in_memory().expect("db");
        for (id, decision) in [("ap-open", None), ("ap-expired", Some("expired"))] {
            create_pending_approval(&conn, id, "sess", "Bash", "x", true, 1000, 9_000_000_000)
                .expect("approval");
            record_approval_turn_owner(&conn, id, "turn-1").expect("owner");
            if let Some(decision) = decision {
                conn.execute(
                    "UPDATE pending_approvals SET decision = ?2 WHERE approval_id = ?1",
                    params![id, decision],
                )
                .expect("settle");
            }
            // A push that failed once and is waiting to retry.
            conn.execute(
                "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
                 VALUES (?1, 'approval_request', 'sess', ?2, 'failed', 0, 1000)",
                params![id, json!({"eventKey": format!("approval:{id}")}).to_string()],
            )
            .expect("queue");
        }

        let settled = settle_prompts_for_turn(&conn, "turn-1", 5000).expect("settle turn");
        assert_eq!(settled, 1, "only the open approval needed a decision");
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(
            left, 0,
            "both buttons must be withdrawn — an expired approval's queued push \
             is just as capable of arriving late"
        );
    }

    /// One malformed payload must not take the whole cancellation down with
    /// it. Without the `json_valid` guard, `json_extract` errors and the
    /// transaction rolls back — after the process was already killed.
    #[test]
    fn a_corrupt_payload_does_not_abort_the_cancellation() {
        let conn = create_state_db_in_memory().expect("db");
        create_pending_approval(
            &conn,
            "ap-1",
            "sess",
            "Bash",
            "x",
            true,
            1000,
            9_000_000_000,
        )
        .expect("approval");
        record_approval_turn_owner(&conn, "ap-1", "turn-1").expect("owner");
        conn.execute(
            "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
             VALUES ('evt-mine', 'approval_request', 'sess', ?1, 'pending', 0, 1000)",
            params![json!({"eventKey": "approval:ap-1"}).to_string()],
        )
        .expect("queue mine");
        conn.execute(
            "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
             VALUES ('evt-corrupt', 'thread_waiting', 'other', '{not json', 'pending', 0, 1000)",
            [],
        )
        .expect("queue corrupt");

        settle_prompts_for_turn(&conn, "turn-1", 5000).expect("must not fail on a corrupt row");

        let mine: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events WHERE event_id = 'evt-mine'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(mine, 0, "the targeted button must still be withdrawn");
        let corrupt: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events WHERE event_id = 'evt-corrupt'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(corrupt, 1, "and the unrelated row must be left alone");
    }

    /// Whoever commits first wins, in BOTH orders, and either way no live
    /// button survives for a turn the user ended — measured through the
    /// PRODUCTION publication (approval + owner + callback routes + outbox
    /// button in one transaction), not just the approval row: a stop that
    /// committed first must find literally nothing to withdraw, and a
    /// publication that committed first must be swept in full.
    #[test]
    fn an_owned_approval_cannot_follow_or_outlive_a_stop() {
        let register = |conn: &Connection| {
            register_bridge_turn(
                conn,
                "turn-1",
                "sess",
                "/tmp/t.log",
                None,
                None,
                None,
                None,
                None,
                None,
                1000,
            )
            .expect("register owner");
        };

        // Order 1: the stop committed first — creation must refuse.
        let conn = create_state_db_in_memory().expect("db");
        register(&conn);
        mark_bridge_turn_stopping(&conn, "turn-1", 1500).expect("intent");
        let published = crate::approvals::publish_approval_request(
            &conn,
            "123",
            "ap-late",
            "sess",
            "Bash",
            "x",
            true,
            None,
            Some("turn-1"),
            None,
            2000,
            9_000_000_000,
        )
        .expect("publish");
        assert_eq!(
            published,
            crate::approvals::Publication::OwnerNotRunning,
            "a stopped owner must refuse new dialogs"
        );
        for table in [
            "pending_approvals",
            "telegram_callback_routes",
            "outbound_events",
        ] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .expect("count");
            assert_eq!(
                count, 0,
                "a refused publication must write NOTHING to {table}"
            );
        }

        // Order 2: creation committed first — the stop's sweep (the same
        // transaction shape `stop_bridge_turn` commits) expires it.
        let conn = create_state_db_in_memory().expect("db");
        register(&conn);
        let published = crate::approvals::publish_approval_request(
            &conn,
            "123",
            "ap-early",
            "sess",
            "Bash",
            "x",
            true,
            None,
            Some("turn-1"),
            None,
            1200,
            9_000_000_000,
        )
        .expect("publish");
        assert_eq!(published, crate::approvals::Publication::Published);
        let buttons: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(buttons, 1, "a publication must include its outbox button");
        let tx = conn.unchecked_transaction().expect("tx");
        mark_bridge_turn_stopping(&tx, "turn-1", 1500).expect("intent");
        settle_prompts_for_turn(&tx, "turn-1", 1500).expect("sweep");
        tx.commit().expect("commit");
        let decision: Option<String> = conn
            .query_row(
                "SELECT decision FROM pending_approvals WHERE approval_id = 'ap-early'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(
            decision.as_deref(),
            Some("expired"),
            "the stop must sweep the dialog the moment its intent commits"
        );
        let buttons: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events
                 WHERE json_extract(payload_json, '$.eventKey') = 'approval:ap-early'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(
            buttons, 0,
            "and the published button must be withdrawn with it"
        );
    }

    /// Republication of the SAME approval id (an interrupted gate re-run,
    /// a redelivered hook) must not reopen anything: an OPEN row keeps its
    /// routes and button untouched, and a decided row hands the decision
    /// back. REPLACE semantics here once reset a deny to NULL — no new
    /// message was pushed (the outbox key already existed), yet the stale
    /// sibling buttons came back to life and could flip the answer.
    #[test]
    fn republication_honours_what_already_happened() {
        let conn = create_state_db_in_memory().expect("db");
        register_bridge_turn(
            &conn,
            "turn-1",
            "sess",
            "/tmp/t.log",
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register owner");
        let publish = |now: u64| {
            crate::approvals::publish_approval_request(
                &conn,
                "123",
                "ap-1",
                "sess",
                "Bash",
                "x",
                true,
                None,
                Some("turn-1"),
                None,
                now,
                9_000_000_000,
            )
        };
        let counts = || -> (i64, i64, i64) {
            let count = |table: &str| -> i64 {
                conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .expect("count")
            };
            (
                count("pending_approvals"),
                count("telegram_callback_routes"),
                count("outbound_events"),
            )
        };

        assert_eq!(
            publish(1000).expect("publish"),
            crate::approvals::Publication::Published
        );
        let before = counts();
        assert_eq!(
            publish(2000).expect("republish"),
            crate::approvals::Publication::AlreadyPublished,
            "an open row is simply waited on"
        );
        assert_eq!(counts(), before, "and republication must write NOTHING");

        conn.execute(
            "UPDATE pending_approvals SET decision = 'deny', decided_at = 3000
             WHERE approval_id = 'ap-1'",
            [],
        )
        .expect("decide");
        assert_eq!(
            publish(4000).expect("decided"),
            crate::approvals::Publication::AlreadyDecided("deny".to_string()),
            "a decided row hands the decision back to be honoured"
        );
        let decision: Option<String> = conn
            .query_row(
                "SELECT decision FROM pending_approvals WHERE approval_id = 'ap-1'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(
            decision.as_deref(),
            Some("deny"),
            "republication must never reset a decision to NULL"
        );
        assert_eq!(counts(), before, "nor write anything while doing so");
    }

    /// Binding the ownership object voids the birth debt — structural
    /// supervision needs no identity write — and a bound row is also
    /// beyond the reach of the no-pid crash claim.
    #[test]
    fn binding_a_cgroup_voids_the_birth_debt_and_shields_the_row() {
        let conn = create_state_db_in_memory().expect("db");
        register_bridge_turn(
            &conn,
            "turn-1",
            "sess",
            "/tmp/t.log",
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        record_turn_cgroup(&conn, "turn-1", "/sys/fs/cgroup/x/turn-turn-1").expect("bind");

        let (marker, path): (i64, Option<String>) = conn
            .query_row(
                "SELECT cleanup_pending, cgroup_path FROM bridge_turns",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row");
        assert_eq!(marker, 0, "ownership is structural; the debt is void");
        assert!(path.is_some());
        assert!(
            !claim_bridge_turn_failure(&conn, "turn-1", "failed", 20_000, true).expect("claim"),
            "a bound row is never an unexplained no-pid crash"
        );
    }

    /// The no-evidence crash claim keeps its hands off supervised rows —
    /// isolated from the OTHER layers that usually shield them (routing,
    /// the coalesced pid): a hand-built row with pid still NULL and only
    /// the marker set must survive the claim.
    #[test]
    fn the_no_pid_claim_excludes_supervised_cleanups() {
        let conn = create_state_db_in_memory().expect("db");
        register_bridge_turn(
            &conn,
            "turn-1",
            "sess",
            "/tmp/t.log",
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        conn.execute(
            "UPDATE bridge_turns SET cleanup_pending = 1 WHERE turn_id = 'turn-1'",
            [],
        )
        .expect("marker");

        assert!(
            !claim_bridge_turn_failure(&conn, "turn-1", "failed", 20_000, true).expect("claim"),
            "a supervised row must not be claimable as a no-pid crash"
        );
        clear_cleanup_pending(&conn, "turn-1").expect("clear");
        assert!(
            claim_bridge_turn_failure(&conn, "turn-1", "failed", 20_000, true).expect("claim"),
            "and without supervision the ordinary claim applies again"
        );
    }

    /// A proof-carrying settlement (group empty, or never spawned) is the
    /// one thing allowed to override a `stopping` intent — the stop is
    /// satisfied vacuously. Left unoverridden, a /stop racing a failed
    /// spawn stranded a `stopping` row with NULL identity, probing
    /// `Unknown` forever.
    #[test]
    fn a_proof_carrying_settlement_overrides_a_stop_intent() {
        let conn = create_state_db_in_memory().expect("db");
        register_bridge_turn(
            &conn,
            "turn-1",
            "sess",
            "/tmp/t.log",
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        mark_bridge_turn_stopping(&conn, "turn-1", 1500).expect("stop wins");

        // A queued "正在终止" from the raced /stop…
        enqueue_outbound_event(
            &conn,
            &json!({
                "type": "bridge_notice", "threadId": "sess",
                "eventKey": "stop-summary:456:3:turn-1:requested",
                "observedAt": 1600, "message": "m",
                "stopTurn": "turn-1", "stopPhase": "requested"
            }),
            1600,
            "bridge",
        )
        .expect("stale receipt");

        settle_unwound_turn_failed(&conn, "turn-1", 2000).expect("settle");

        let (status, marker): (String, i64) = conn
            .query_row(
                "SELECT status, cleanup_pending FROM bridge_turns",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row");
        assert_eq!(status, "failed", "the vacuously satisfied stop settles");
        assert_eq!(marker, 0, "and no phantom supervision is left behind");
        // …is withdrawn AND replaced by a durable terminal receipt in the
        // SAME transaction: a withdrawal with no substitute left the user
        // with no stop answer at all if the daemon crashed here.
        let keys: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT json_extract(payload_json, '$.eventKey') FROM outbound_events")
                .expect("stmt");
            let rows = stmt
                .query_map([], |row| row.get(0))
                .expect("query")
                .collect::<rusqlite::Result<Vec<String>>>()
                .expect("rows");
            rows
        };
        assert_eq!(
            keys,
            vec!["stop-settled:turn-1".to_string()],
            "the stale receipt goes; the terminal answer arrives, atomically"
        );
    }

    /// Both spawn-cleanup settlements close what the turn owns in the SAME
    /// transaction. A `failed` turn is never swept by the daemon again, and
    /// a `stopping` one not before the next tick — either way, a button an
    /// early tool hook managed to publish would stay answerable and could
    /// hand a decision to a turn being unwound.
    #[test]
    fn spawn_cleanup_settlements_close_owned_dialogs_atomically() {
        for terminated in [true, false] {
            let conn = create_state_db_in_memory().expect("db");
            register_bridge_turn(
                &conn,
                "turn-1",
                "sess",
                "/tmp/t.log",
                None,
                None,
                None,
                None,
                None,
                None,
                1000,
            )
            .expect("register");
            create_pending_approval(
                &conn,
                "ap-1",
                "sess",
                "Bash",
                "x",
                true,
                1000,
                9_000_000_000,
            )
            .expect("approval");
            record_approval_turn_owner(&conn, "ap-1", "turn-1").expect("owner");
            enqueue_outbound_event(
                &conn,
                &json!({
                    "type": "approval_request", "threadId": "sess",
                    "eventKey": "approval:ap-1", "observedAt": 1000
                }),
                1000,
                "bridge",
            )
            .expect("button");

            let expected_status = if terminated {
                // A raced /stop's undelivered receipt must go with the
                // settlement — delivered later it would promise progress
                // on a settled turn.
                enqueue_outbound_event(
                    &conn,
                    &json!({
                        "type": "bridge_notice", "threadId": "sess",
                        "eventKey": "stop-summary:456:9:turn-1:requested",
                        "observedAt": 1500, "message": "m",
                        "stopTurn": "turn-1", "stopPhase": "requested"
                    }),
                    1500,
                    "bridge",
                )
                .expect("stale receipt");
                settle_unwound_turn_failed(&conn, "turn-1", 5000).expect("settle");
                let stale: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM outbound_events
                         WHERE json_extract(payload_json, '$.eventKey')
                               = 'stop-summary:456:9:turn-1:requested'",
                        [],
                        |row| row.get(0),
                    )
                    .expect("count");
                assert_eq!(stale, 0, "the stale stop receipt must be withdrawn");
                "failed"
            } else {
                // The unconfirmed unwinding keeps the row RUNNING under
                // the failure-cleanup marker — `stopping` is the user's
                // word — and still closes the dialogs atomically.
                mark_cleanup_pending(&conn, "turn-1", Some(4_210), None, None, 5000).expect("mark");
                "running"
            };

            let status: String = conn
                .query_row("SELECT status FROM bridge_turns", [], |row| row.get(0))
                .expect("status");
            assert_eq!(status, expected_status);
            let decision: Option<String> = conn
                .query_row(
                    "SELECT decision FROM pending_approvals WHERE approval_id = 'ap-1'",
                    [],
                    |row| row.get(0),
                )
                .expect("approval");
            assert_eq!(
                decision.as_deref(),
                Some("expired"),
                "{expected_status}: the owned dialog must close with the settlement"
            );
            let buttons: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM outbound_events
                     WHERE json_extract(payload_json, '$.eventKey') = 'approval:ap-1'",
                    [],
                    |row| row.get(0),
                )
                .expect("count");
            assert_eq!(
                buttons, 0,
                "{expected_status}: and its queued button must be withdrawn"
            );
        }
    }

    /// The backfill runs at DATABASE OPEN — proven through the real
    /// migration entry, not by calling the helper: an interim receipt is
    /// written, the file closed and reopened (the upgrade), and a second
    /// reopen must be idempotent.
    #[test]
    fn reopening_a_database_backfills_interim_receipts() {
        let path = std::env::temp_dir().join(format!("tinyctb-backfill-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let conn = create_state_db(&path).expect("create");
            enqueue_outbound_event(
                &conn,
                &json!({
                    "type": "bridge_notice", "threadId": "sess-z",
                    "eventKey": "stop-summary:456:8:turn-w:requested",
                    "observedAt": 1000, "message": "m"
                }),
                1000,
                "bridge",
            )
            .expect("enqueue");
        }
        let labelled = |conn: &Connection| -> Option<String> {
            conn.query_row(
                "SELECT json_extract(payload_json, '$.stopTurn') FROM outbound_events",
                [],
                |row| row.get(0),
            )
            .expect("row")
        };
        {
            let conn = create_state_db(&path).expect("reopen");
            assert_eq!(
                labelled(&conn).as_deref(),
                Some("turn-w"),
                "reopening must label the interim receipt through the migration entry"
            );
        }
        {
            let conn = create_state_db(&path).expect("second reopen");
            assert_eq!(labelled(&conn).as_deref(), Some("turn-w"));
            let rows: i64 = conn
                .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
                .expect("count");
            assert_eq!(rows, 1, "and a second reopen changes nothing");
        }
        let _ = std::fs::remove_file(&path);
    }

    /// Receipts written by interim builds lack the structured fields the
    /// terminal withdrawal matches on; the backfill labels them from the
    /// key so they cannot dodge withdrawal forever. A phase-less older key
    /// is left alone — nothing safe to infer from it.
    #[test]
    fn backfill_labels_interim_stop_receipts() {
        let conn = create_state_db_in_memory().expect("db");
        for key in [
            "stop-summary:456:7:turn-z:requested",
            "stop-summary:456:7:turn-z",
        ] {
            enqueue_outbound_event(
                &conn,
                &json!({
                    "type": "bridge_notice", "threadId": "sess-z",
                    "eventKey": key, "observedAt": 1000, "message": "m"
                }),
                1000,
                "bridge",
            )
            .expect("enqueue");
        }

        backfill_stop_receipt_fields(&conn).expect("backfill");

        let labelled: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT json_extract(payload_json, '$.stopTurn'),
                        json_extract(payload_json, '$.stopPhase')
                 FROM outbound_events
                 WHERE json_extract(payload_json, '$.eventKey')
                       = 'stop-summary:456:7:turn-z:requested'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row");
        assert_eq!(
            labelled,
            (Some("turn-z".to_string()), Some("requested".to_string())),
            "the interim receipt must be labelled from its key"
        );
        let untouched: Option<String> = conn
            .query_row(
                "SELECT json_extract(payload_json, '$.stopTurn') FROM outbound_events
                 WHERE json_extract(payload_json, '$.eventKey') = 'stop-summary:456:7:turn-z'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(untouched, None, "a phase-less key must be left alone");

        // And the withdrawal now reaches the labelled row.
        withdraw_undelivered_stop_chatter(&conn, "turn-z", 5000).expect("withdraw");
        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events
                 WHERE json_extract(payload_json, '$.eventKey')
                       = 'stop-summary:456:7:turn-z:requested'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(remaining, 0, "the backfilled receipt is withdrawable");
    }

    /// The terminal withdrawal pulls back only UNDELIVERED stale chatter:
    /// a receipt the user already received is history and must survive.
    #[test]
    fn stop_chatter_withdrawal_spares_delivered_history() {
        let conn = create_state_db_in_memory().expect("db");
        for (key, delivered) in [
            ("stop-summary:456:1:turn-x:requested", true),
            ("stop-summary:456:2:turn-x:requested", false),
        ] {
            enqueue_outbound_event(
                &conn,
                &json!({
                    "type": "bridge_notice", "threadId": "sess-x",
                    "eventKey": key, "observedAt": 1000, "message": "m",
                    "stopTurn": "turn-x", "stopPhase": "requested"
                }),
                1000,
                "bridge",
            )
            .expect("enqueue");
            if delivered {
                let event_id: String = conn
                    .query_row(
                        "SELECT event_id FROM outbound_events
                         WHERE json_extract(payload_json, '$.eventKey') = ?1",
                        params![key],
                        |row| row.get(0),
                    )
                    .expect("event id");
                record_transport_delivery(&conn, &event_id, "telegram", &json!({"ok": true}), 1)
                    .expect("delivery log");
            }
        }

        withdraw_undelivered_stop_chatter(&conn, "turn-x", 5000).expect("withdraw");

        let keys: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT json_extract(payload_json, '$.eventKey') FROM outbound_events")
                .expect("stmt");
            let rows = stmt
                .query_map([], |row| row.get(0))
                .expect("query")
                .collect::<rusqlite::Result<Vec<String>>>()
                .expect("rows");
            rows
        };
        assert_eq!(
            keys,
            vec!["stop-summary:456:1:turn-x:requested".to_string()],
            "delivered history stays; only the undelivered stale row is withdrawn"
        );
    }

    /// `_` and `%` inside a turn id must never act as wildcards: the
    /// withdrawal matches the STRUCTURED turn field exactly. Parsed out of
    /// the event key with LIKE, `turn_a` also matched `turnxa` and pulled
    /// back another turn's receipts.
    #[test]
    fn stop_chatter_withdrawal_is_not_a_wildcard_match() {
        let conn = create_state_db_in_memory().expect("db");
        for turn in ["turn_a", "turnxa"] {
            enqueue_outbound_event(
                &conn,
                &json!({
                    "type": "bridge_notice", "threadId": "sess-x",
                    "eventKey": format!("stop-summary:456:9:{turn}:requested"),
                    "observedAt": 1000, "message": "m",
                    "stopTurn": turn, "stopPhase": "requested"
                }),
                1000,
                "bridge",
            )
            .expect("enqueue");
        }

        withdraw_undelivered_stop_chatter(&conn, "turn_a", 5000).expect("withdraw");

        let keys: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT json_extract(payload_json, '$.eventKey') FROM outbound_events")
                .expect("stmt");
            let rows = stmt
                .query_map([], |row| row.get(0))
                .expect("query")
                .collect::<rusqlite::Result<Vec<String>>>()
                .expect("rows");
            rows
        };
        assert_eq!(
            keys,
            vec!["stop-summary:456:9:turnxa:requested".to_string()],
            "only the EXACT turn's receipt may be withdrawn"
        );
    }

    /// All-or-nothing, proven by sabotage: if the outbox insert fails, the
    /// approval row and its callback routes must vanish with it. Committed
    /// piecemeal, a broken outbox left an approval with no button —
    /// invisible on the phone, blocking the gate until its timeout.
    #[test]
    fn a_publication_that_cannot_queue_its_button_leaves_nothing() {
        let conn = create_state_db_in_memory().expect("db");
        register_bridge_turn(
            &conn,
            "turn-1",
            "sess",
            "/tmp/t.log",
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register owner");
        conn.execute_batch(
            "CREATE TRIGGER outbox_broken BEFORE INSERT ON outbound_events
             BEGIN SELECT RAISE(ABORT, 'outbox broken'); END;",
        )
        .expect("trigger");

        let result = crate::approvals::publish_approval_request(
            &conn,
            "123",
            "ap-1",
            "sess",
            "Bash",
            "x",
            true,
            None,
            Some("turn-1"),
            None,
            1000,
            9_000_000_000,
        );
        assert!(result.is_err(), "a broken outbox must fail the publication");
        for table in ["pending_approvals", "telegram_callback_routes"] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .expect("count");
            assert_eq!(
                count, 0,
                "{table} must roll back with the failed button — no half-published dialog"
            );
        }
    }

    /// The approval row and its owner must land together or not at all.
    /// Written as two commits, a `/stop` landing in between would see
    /// `turn_id` still NULL, correctly fail open, and leave a dead prompt
    /// that `/threads` would keep reoffering for up to a day.
    ///
    /// Two threads hammer the same window: one repeatedly creates
    /// approval+owner in a transaction, the other repeatedly reads. The
    /// reader must NEVER observe a row whose owner is missing.
    #[test]
    fn an_approval_is_never_visible_without_its_owner() {
        let path =
            std::env::temp_dir().join(format!("tinyctb-owner-race-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let writer_conn = create_state_db(&path).expect("db");
        let reader_conn = create_state_db(&path).expect("db");
        // The owner must be a RUNNING turn — creation now refuses owners in
        // any other state.
        register_bridge_turn(
            &writer_conn,
            "turn-1",
            "sess",
            "/tmp/t.log",
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register owner");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let reader_stop = std::sync::Arc::clone(&stop);
        let reader_barrier = std::sync::Arc::clone(&barrier);
        let reader = std::thread::spawn(move || {
            reader_barrier.wait();
            let mut orphans = 0usize;
            let mut seen = 0usize;
            while !reader_stop.load(std::sync::atomic::Ordering::Relaxed) {
                let rows: Vec<Option<String>> = {
                    let mut stmt = reader_conn
                        .prepare("SELECT turn_id FROM pending_approvals")
                        .expect("stmt");
                    let rows = stmt
                        .query_map([], |row| row.get(0))
                        .expect("query")
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .expect("rows");
                    rows
                };
                for owner in rows {
                    seen += 1;
                    if owner.is_none() {
                        orphans += 1;
                    }
                }
            }
            (seen, orphans)
        });

        barrier.wait();
        for index in 0..200 {
            let id = format!("ap-{index}");
            // THE PRODUCTION WRAPPER, not a hand-rolled transaction: a test
            // that builds its own atomicity proves nothing about the code
            // the gate actually runs.
            crate::approvals::publish_approval_request(
                &writer_conn,
                "123",
                &id,
                "sess",
                "Bash",
                "x",
                true,
                None,
                Some("turn-1"),
                None,
                1000,
                9_000_000_000,
            )
            .expect("publish");
            writer_conn
                .execute("DELETE FROM pending_approvals", [])
                .expect("clear");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let (seen, orphans) = reader.join().expect("reader");

        assert!(seen > 0, "the reader must actually have observed rows");
        assert_eq!(
            orphans, 0,
            "an approval was visible without its owner — /stop would then fail \
             open and leave a dead prompt behind"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn settling_honours_a_tap_and_only_withdraws_withdrawable_pushes() {
        let seed = |conn: &Connection, key: &str, claimed: Option<i64>, logged: bool| {
            conn.execute(
                "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at, claimed_at)
                 VALUES (?1, 'approval_request', 'thr', ?2, 'pending', 0, 0, ?3)",
                params![key, json!({"eventKey": key}).to_string(), claimed],
            )
            .expect("seed outbound");
            if logged {
                record_transport_delivery(conn, key, "telegram", &json!({"ok": true}), 1)
                    .expect("log");
            }
        };
        let open_approval = |conn: &Connection, id: &str| {
            create_pending_approval(conn, id, "thr", "Bash", "s", false, 1000, 9_000_000_000)
                .expect("approval");
        };

        // 1. A tap that landed first must survive: answer returned, push kept.
        let conn = create_state_db_in_memory().expect("db");
        open_approval(&conn, "a1");
        seed(&conn, "approval:a1", None, false);
        record_approval_decision(&conn, "a1", "allow", 1500).expect("tap");
        let outcome = settle_expired_and_cancel_push(&conn, SettleTarget::Approval("a1"), 2000)
            .expect("settle");
        assert!(
            matches!(outcome, SettleOutcome::Answered(ref d) if d == "allow"),
            "a tap the user already saw accepted must be returned, never discarded"
        );
        let kept: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(kept, 1, "the push behind an accepted answer stays");

        // 2. Nobody answered: expired, and the unclaimed push is withdrawn.
        let conn = create_state_db_in_memory().expect("db");
        open_approval(&conn, "a2");
        seed(&conn, "approval:a2", None, false);
        let outcome = settle_expired_and_cancel_push(&conn, SettleTarget::Approval("a2"), 2000)
            .expect("settle");
        assert!(matches!(outcome, SettleOutcome::Expired));
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(left, 0, "an unsent, unclaimed push must be withdrawn");

        // 3. A CLAIMED row is already delivery's business — it may be on the
        //    wire, so it is never yanked out from under the sender.
        let conn = create_state_db_in_memory().expect("db");
        open_approval(&conn, "a3");
        seed(&conn, "approval:a3", Some(1234), false);
        settle_expired_and_cancel_push(&conn, SettleTarget::Approval("a3"), 2000).expect("settle");
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(left, 1, "a claimed push must not be cancelled mid-flight");

        // 4. Same for one the transport log says already went out.
        let conn = create_state_db_in_memory().expect("db");
        open_approval(&conn, "a4");
        seed(&conn, "approval:a4", None, true);
        settle_expired_and_cancel_push(&conn, SettleTarget::Approval("a4"), 2000).expect("settle");
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(
            left, 1,
            "a push with a transport record has reached the user"
        );
    }

    /// The delivery loop reads its batch up front, so a push withdrawn after
    /// that read must still not go out. The per-row claim is what enforces
    /// it: this test cancels row two from inside row one's send.
    /// The retry schedule is the long tail of a dead button: a push whose
    /// first send failed sits in the outbox waiting to try again, and if the
    /// terminal settles the request in the meantime that retry would hand
    /// the user a button for something already decided. Settling must be
    /// able to withdraw a FAILED row, and the next cycle must send nothing.
    #[test]
    fn a_failed_push_settled_at_the_terminal_is_never_retried() {
        let conn = create_state_db_in_memory().expect("db");
        create_pending_approval(&conn, "ap1", "thr", "Bash", "s", false, 1000, 9_000_000_000)
            .expect("approval");
        enqueue_outbound_event(
            &conn,
            &json!({
                "type": "approval_request",
                "threadId": "thr",
                "eventKey": "approval:ap1",
                "updatedAt": 1000
            }),
            1000,
            "bridge",
        )
        .expect("enqueue");

        // First send fails: the row stays, claim released, retry scheduled.
        let summary = deliver_due_outbound_events(&conn, 1000, 10, None, |_| {
            Err(anyhow::anyhow!("telegram unreachable"))
        })
        .expect("first attempt");
        assert_eq!(summary.failed, 1);
        let (status, claimed): (String, Option<i64>) = conn
            .query_row(
                "SELECT status, claimed_at FROM outbound_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row");
        assert_eq!(status, "failed");
        assert_eq!(claimed, None, "a failed send must release its claim");

        // The terminal answers it; the gate settles and withdraws the push.
        let outcome = settle_expired_and_cancel_push(&conn, SettleTarget::Approval("ap1"), 2000)
            .expect("settle");
        assert!(matches!(outcome, SettleOutcome::Expired));

        // The retry cycle must find nothing to send.
        let sent = std::cell::Cell::new(0usize);
        let summary = deliver_due_outbound_events(&conn, 9_000_000, 10, None, |_| {
            sent.set(sent.get() + 1);
            Ok(json!({ "ok": true }))
        })
        .expect("retry cycle");
        assert_eq!(sent.get(), 0, "the withdrawn push must never be retried");
        assert_eq!(summary.attempted, 0);
    }

    /// The same recovery chain, but every step goes through PRODUCTION
    /// code: the claim is taken by a real delivery cycle whose sender panics
    /// the way a killed daemon would, the connection is dropped and the
    /// database reopened, and only then does the terminal settle and a later
    /// cycle reclaim. Hand-filling `claimed_at`/`claim_token` proved the
    /// query shapes; this proves the chain.
    #[test]
    fn the_crash_recovery_chain_holds_through_a_real_claim_and_reopen() {
        let path =
            std::env::temp_dir().join(format!("tinyctb-crash-chain-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let conn = create_state_db(&path).expect("db");
            create_pending_approval(&conn, "ap", "thr", "Bash", "s", false, 1000, 9_000_000_000)
                .expect("approval");
            enqueue_outbound_event(
                &conn,
                &json!({
                    "type": "approval_request",
                    "threadId": "thr",
                    "eventKey": "approval:ap",
                    "updatedAt": 1000
                }),
                1000,
                "bridge",
            )
            .expect("enqueue");

            // A real cycle claims the row, then the "daemon" dies mid-send:
            // the sender unwinds and the connection is dropped without any
            // settling update ever running.
            let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = deliver_due_outbound_events(&conn, 1000, 10, None, |_| {
                    panic!("daemon killed mid-send");
                });
            }));
            assert!(crashed.is_err(), "the sender must have died");
        } // connection dropped == process gone

        // Reopen, exactly like a restarted daemon.
        let conn = create_state_db(&path).expect("reopen");
        let (status, claimed, token): (String, Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT status, claimed_at, claim_token FROM outbound_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("row survives the crash");
        assert_eq!(status, "pending", "no settling update ran");
        assert!(
            claimed.is_some() && token.is_some(),
            "the claim is orphaned"
        );

        // The terminal answers it while the orphaned lease is still live.
        settle_expired_and_cancel_push(&conn, SettleTarget::Approval("ap"), 1500).expect("settle");

        // Lease lapses; a later real cycle reclaims and must send nothing.
        let sent = std::cell::Cell::new(0usize);
        deliver_due_outbound_events(&conn, CLAIM_LEASE_MS * 3, 10, None, |_| {
            sent.set(sent.get() + 1);
            Ok(json!({ "ok": true }))
        })
        .expect("recovery cycle");
        assert_eq!(
            sent.get(),
            0,
            "a request settled during the orphaned lease must never be posted"
        );
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(left, 0, "and the withdrawn push is gone");
        let _ = std::fs::remove_file(&path);
    }

    /// The narrow window Sol named: the daemon claims a row, then dies; the
    /// terminal settles the request while the lease is still live; five
    /// minutes later the reclaim would post a button for a settled call.
    /// The settle records its intent on the claimed row, and whoever picks
    /// the row up afterwards honours it instead of sending.
    #[test]
    fn a_settle_during_a_live_claim_is_honoured_when_the_row_is_reclaimed() {
        for logged in [false, true] {
            let conn = create_state_db_in_memory().expect("db");
            create_pending_approval(&conn, "ap", "thr", "Bash", "s", false, 1000, 9_000_000_000)
                .expect("approval");
            conn.execute(
                "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at, claimed_at, claim_token)
                 VALUES ('evt', 'approval_request', 'thr', ?1, 'pending', 0, 0, 1000, 'dead-owner')",
                params![json!({"eventKey": "approval:ap"}).to_string()],
            )
            .expect("seed claimed row");
            if logged {
                record_transport_delivery(&conn, "evt", "telegram", &json!({"ok": true}), 1500)
                    .expect("log");
            }

            // Terminal settles while the (dead) owner still holds the lease.
            settle_expired_and_cancel_push(&conn, SettleTarget::Approval("ap"), 2000)
                .expect("settle");
            let flagged: i64 = conn
                .query_row(
                    "SELECT cancel_requested FROM outbound_events WHERE event_id = 'evt'",
                    [],
                    |row| row.get(0),
                )
                .expect("flag");
            assert_eq!(flagged, 1, "the intent must survive on the claimed row");

            // Lease lapses; the row is reclaimed by a later cycle.
            let sent = std::cell::Cell::new(0usize);
            deliver_due_outbound_events(&conn, CLAIM_LEASE_MS * 3, 10, None, |_| {
                sent.set(sent.get() + 1);
                Ok(json!({ "ok": true }))
            })
            .expect("reclaim cycle");
            assert_eq!(
                sent.get(),
                0,
                "a settled request must never be posted on reclaim (logged={logged})"
            );

            let row: Option<(String, Option<i64>)> = conn
                .query_row(
                    "SELECT status, delivered_at FROM outbound_events WHERE event_id = 'evt'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .expect("row");
            if logged {
                let (status, delivered_at) = row.expect("a delivered row is kept as history");
                assert_eq!(status, "delivered");
                assert_eq!(
                    delivered_at,
                    Some(1500),
                    "recovery must record the time it ACTUALLY went out"
                );
            } else {
                assert!(row.is_none(), "an unsent, settled push is dropped");
            }
        }
    }

    /// One corrupt payload used to abort the whole batch and stay first in
    /// line, freezing every notification behind it forever. It must be
    /// quarantined and the rest of the queue must flow.
    #[test]
    fn a_malformed_event_is_quarantined_and_the_queue_keeps_moving() {
        let conn = create_state_db_in_memory().expect("db");
        conn.execute(
            "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
             VALUES ('evt_bad', 'approval_request', 'thr', '{not json', 'pending', 0, 1)",
            [],
        )
        .expect("seed bad");
        conn.execute(
            "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
             VALUES ('evt_good', 'thread_completed', 'thr', ?1, 'pending', 0, 2)",
            params![json!({"eventKey": "good"}).to_string()],
        )
        .expect("seed good");

        let sent = std::cell::RefCell::new(Vec::new());
        let summary = deliver_due_outbound_events(&conn, 1000, 10, None, |event| {
            sent.borrow_mut().push(
                event
                    .get("eventKey")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            );
            Ok(json!({ "ok": true }))
        })
        .expect("batch must not abort");

        assert_eq!(
            sent.into_inner(),
            vec!["good".to_string()],
            "the event behind the corrupt one must still go out"
        );
        assert_eq!(summary.delivered, 1);
        let status: String = conn
            .query_row(
                "SELECT status FROM outbound_events WHERE event_id = 'evt_bad'",
                [],
                |row| row.get(0),
            )
            .expect("bad row");
        assert_eq!(status, "invalid", "and the corrupt row must be terminal");
        // Terminal means terminal: the row must leave the ACTIVE set, or it
        // gets re-parsed every tick and 100 of them fill the batch limit.
        assert_eq!(
            pending_outbound_count(&conn).expect("pending"),
            0,
            "a quarantined row must not count as pending work"
        );
        let picked = std::cell::Cell::new(0usize);
        let summary = deliver_due_outbound_events(&conn, 2000, 10, None, |_| {
            picked.set(picked.get() + 1);
            Ok(json!({ "ok": true }))
        })
        .expect("second cycle");
        assert_eq!(picked.get(), 0, "nothing left to send");
        assert_eq!(
            summary.failed, 0,
            "and the corrupt row must not be re-parsed at all — a second \
             quarantine here means it is still in the queue"
        );
    }

    /// A daemon killed between claiming a row and sending it must not
    /// strand that row forever. The claim is a lease: once it expires the
    /// next cycle takes the row back and delivers it.
    #[test]
    fn a_claim_orphaned_by_a_crash_is_reclaimed_after_its_lease() {
        let seed = |conn: &Connection, id: &str, claimed_at: i64, logged: bool| {
            conn.execute(
                "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at, claimed_at, claim_token)
                 VALUES (?1, 'approval_request', 'thr', ?2, 'pending', 0, 0, ?3, 'dead-owner')",
                params![id, json!({"eventKey": id}).to_string(), claimed_at],
            )
            .expect("seed");
            if logged {
                record_transport_delivery(conn, id, "telegram", &json!({"ok": true}), 1)
                    .expect("log");
            }
        };
        let now = CLAIM_LEASE_MS * 3;

        // Lease expired, nothing was ever sent: reclaim and deliver.
        let conn = create_state_db_in_memory().expect("db");
        seed(&conn, "evt_orphan", 1, false);
        let summary =
            deliver_due_outbound_events(&conn, now, 10, None, |_| Ok(json!({"ok": true})))
                .expect("deliver");
        assert_eq!(
            summary.delivered, 1,
            "a crash-orphaned claim must be reclaimed once its lease lapses"
        );

        // Lease expired but the transport log says it DID reach Telegram:
        // still reclaimable, and the sender's own idempotence is what keeps
        // it from being sent twice — so the row settles without a re-send.
        let conn = create_state_db_in_memory().expect("db");
        seed(&conn, "evt_logged", 1, true);
        let sent = std::cell::Cell::new(0usize);
        deliver_due_outbound_events(&conn, now, 10, None, |event| {
            let id = event
                .get("eventKey")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if transport_delivered_at(&conn, id, "telegram")?.is_none() {
                sent.set(sent.get() + 1);
            }
            Ok(json!({"ok": true}))
        })
        .expect("deliver");
        assert_eq!(
            sent.get(),
            0,
            "a row already in the transport log must not be re-sent on reclaim"
        );

        // A LIVE claim is left alone.
        let conn = create_state_db_in_memory().expect("db");
        seed(&conn, "evt_live", to_sql_i64(now).expect("now"), false);
        let summary =
            deliver_due_outbound_events(&conn, now, 10, None, |_| Ok(json!({"ok": true})))
                .expect("deliver");
        assert_eq!(
            summary.attempted, 0,
            "a claim still inside its lease belongs to whoever holds it"
        );
    }

    /// A hundred corrupt rows must not fill the batch limit forever. This
    /// is what "terminal" has to mean: quarantined rows leave the active
    /// set, so the next real notification still gets through on the SAME
    /// cycle rather than queueing behind garbage that never drains.
    #[test]
    fn a_hundred_malformed_events_do_not_starve_the_queue() {
        let conn = create_state_db_in_memory().expect("db");
        for index in 0..100 {
            conn.execute(
                "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
                 VALUES (?1, 'approval_request', 'thr', '{not json', 'pending', 0, ?2)",
                params![format!("evt_bad_{index:03}"), index],
            )
            .expect("seed bad");
        }
        conn.execute(
            "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
             VALUES ('evt_good', 'thread_completed', 'thr', ?1, 'pending', 0, 999)",
            params![json!({"eventKey": "good"}).to_string()],
        )
        .expect("seed good");

        // First cycle: the limit is 100, so the good row is behind them all.
        let sent = std::cell::RefCell::new(Vec::new());
        deliver_due_outbound_events(&conn, 1000, 100, None, |event| {
            sent.borrow_mut().push(
                event
                    .get("eventKey")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            );
            Ok(json!({ "ok": true }))
        })
        .expect("first cycle");

        // Second cycle: with the bad rows terminal, the good one is now
        // first in line. If quarantine did not remove them from the active
        // set they would fill the limit again, forever.
        deliver_due_outbound_events(&conn, 2000, 100, None, |event| {
            sent.borrow_mut().push(
                event
                    .get("eventKey")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            );
            Ok(json!({ "ok": true }))
        })
        .expect("second cycle");

        assert_eq!(
            sent.into_inner(),
            vec!["good".to_string()],
            "the real notification must get out despite 100 corrupt rows"
        );
    }

    /// The batch is `LIMIT 100`. If in-flight rows counted against it, a
    /// hundred stuck claims would starve every later notification forever —
    /// which is exactly what an unbounded claim produced. Live claims are
    /// excluded from the query, so the fresh row still gets through.
    #[test]
    fn a_hundred_in_flight_claims_do_not_starve_the_queue() {
        let conn = create_state_db_in_memory().expect("db");
        let now = CLAIM_LEASE_MS * 3;
        for index in 0..100 {
            conn.execute(
                "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at, claimed_at, claim_token)
                 VALUES (?1, 'approval_request', 'thr', '{}', 'pending', 0, ?2, ?3, 'holder')",
                params![format!("evt_stuck_{index:03}"), index, to_sql_i64(now).expect("now")],
            )
            .expect("seed stuck");
        }
        conn.execute(
            "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
             VALUES ('evt_fresh', 'thread_completed', 'thr', ?1, 'pending', 0, 999)",
            params![json!({"eventKey": "fresh"}).to_string()],
        )
        .expect("seed fresh");

        let sent = std::cell::RefCell::new(Vec::new());
        deliver_due_outbound_events(&conn, now, 100, None, |event| {
            sent.borrow_mut().push(
                event
                    .get("eventKey")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            );
            Ok(json!({"ok": true}))
        })
        .expect("deliver");
        assert_eq!(
            sent.into_inner(),
            vec!["fresh".to_string()],
            "a queue full of in-flight claims must not block the next notification"
        );
    }

    #[test]
    fn a_push_withdrawn_after_the_batch_read_is_not_sent() {
        let conn = create_state_db_in_memory().expect("db");
        for (id, key) in [("evt_1", "approval:one"), ("evt_2", "approval:two")] {
            conn.execute(
                "INSERT INTO outbound_events(event_id, event_type, thread_id, payload_json, status, next_attempt_at, created_at)
                 VALUES (?1, 'approval_request', 'thr', ?2, 'pending', 0, 0)",
                params![id, json!({"eventKey": key, "type": "approval_request"}).to_string()],
            )
            .expect("seed");
        }
        let sent = std::cell::RefCell::new(Vec::new());
        let summary = deliver_due_outbound_events(&conn, 1000, 10, None, |event| {
            let key = event
                .get("eventKey")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if key == "approval:one" {
                // The gate settles the second request while we are busy.
                conn.execute("DELETE FROM outbound_events WHERE event_id = 'evt_2'", [])
                    .expect("withdraw");
            }
            sent.borrow_mut().push(key);
            Ok(json!({ "ok": true }))
        })
        .expect("deliver");

        assert_eq!(
            sent.into_inner(),
            vec!["approval:one".to_string()],
            "the withdrawn push must never reach the transport"
        );
        assert_eq!(summary.delivered, 1);
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
