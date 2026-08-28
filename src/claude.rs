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
    /// When the session last actually said something, from the stamp on the
    /// transcript's own last record.
    ///
    /// The file's mtime is not that. A transcript can be touched by anything
    /// — a backup, a copy, an editor — and the row then claimed the session
    /// had just spoken: a thread that went quiet on 08-12 sat at the top of
    /// `/threads` showing today's time, and nothing cleared it. `None` means
    /// the records carried no readable stamp, and only then is mtime used.
    pub(crate) last_record_at: Option<u64>,
    /// The earliest stamp this parse REFUSED for being ahead of the clock.
    ///
    /// Kept so the cache can tell when its own answer has expired: the file
    /// has not changed, but a stamp that was unbelievable at parse time
    /// becomes ordinary once the clock reaches it.
    pub(crate) earliest_refused_future_at: Option<u64>,
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
/// Process-wide cache for transcript summaries, keyed by path and validated
/// by the FULL fingerprint below — generation, size, inode and change time.
///
/// The daemon's full sync re-summarises up to 50 session transcripts every
/// ~1.5s, and an active session's transcript runs to tens of megabytes;
/// re-parsing unchanged files burned ~10% of a core, growing with
/// conversation length (measured 2026-08-16). CLI one-shot invocations
/// simply run with a cold cache.
///
/// It was once keyed by (mtime_ms, len), on the belief that any rewrite
/// changes one of the two. It does not: a same-length rewrite inside one
/// millisecond matches, and so does a rewrite whose mtime is put back
/// afterwards — which is what a backup or a restore does.
/// What makes one reading of a file different from another: modification
/// generation (nanoseconds), size, inode, and the inode's own change time.
///
/// The last one is why a restore cannot hide. Content written and the mtime
/// then put back leaves the first three identical — that is what a backup
/// tool does — and the stale summary was served forever. `ctime` is bumped
/// by the write and cannot be set back by `set_modified`, so it is the one
/// piece of evidence a restore does not control.
type FileFingerprint = (u128, u64, u64, i64);

type TranscriptSummaryCache =
    std::collections::HashMap<PathBuf, (FileFingerprint, TranscriptSummary)>;
static TRANSCRIPT_SUMMARY_CACHE: std::sync::Mutex<Option<TranscriptSummaryCache>> =
    std::sync::Mutex::new(None);

/// Cache growth bound: strictly more than the 50-session scan window, small
/// enough that a clear-and-rebuild (one sync's worth of parsing) is cheap.
const TRANSCRIPT_SUMMARY_CACHE_MAX: usize = 128;

/// Epoch milliseconds from the stamp Claude Code writes on every transcript
/// record: `2026-08-12T13:32:34.162Z`.
///
/// Strict and UTC-only on purpose. One known producer writes these, and a
/// lenient parser that guessed at other shapes would put a WRONG time into
/// the ordering — worse than the `None` that falls back to the file mtime.
pub(crate) fn transcript_timestamp_ms(raw: &str) -> Option<u64> {
    fn fixed_width_number(raw: &str, width: usize) -> Option<i64> {
        // Fixed width and digits only: `2026-8-2T1:2:3Z` is not the shape
        // this claims to read, and a `-` slipping into a component would
        // otherwise parse as a negative hour.
        if raw.len() != width || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        raw.parse().ok()
    }
    fn days_in_month(year: i64, month: i64) -> i64 {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
            2 => 28,
            _ => 0,
        }
    }

    let raw = raw.strip_suffix('Z')?;
    let (date, time) = raw.split_once('T')?;
    let (year, rest) = date.split_at(date.find('-')?);
    let year = fixed_width_number(year, 4)?;
    let mut date_parts = rest.strip_prefix('-')?.split('-');
    let month = fixed_width_number(date_parts.next()?, 2)?;
    let day = fixed_width_number(date_parts.next()?, 2)?;
    if date_parts.next().is_some() || !(1..=12).contains(&month) {
        return None;
    }
    // A full calendar check, not a 1..=31 wave-through: 2026-02-31 and a
    // non-leap 02-29 are not days, and a stamp that names one is not a time.
    if day < 1 || day > days_in_month(year, month) {
        return None;
    }
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (time, None),
    };
    let mut clock_parts = clock.split(':');
    let hour = fixed_width_number(clock_parts.next()?, 2)?;
    let minute = fixed_width_number(clock_parts.next()?, 2)?;
    let second = fixed_width_number(clock_parts.next()?, 2)?;
    // No leap second: this producer does not write them, and 23:59:60 would
    // land a millisecond ahead of the next day's first record.
    if clock_parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let millis: i64 = match fraction {
        // Three digits is what Claude Code writes; anything else is a shape
        // this parser does not claim to understand.
        Some(fraction) => fixed_width_number(fraction, 3)?,
        None => 0,
    };
    // Days from the civil calendar (Howard Hinnant's algorithm), which needs
    // no dependency and no leap-year special cases. Every step is checked:
    // a stamp from the year 999999 must return None, not wrap (or panic in a
    // debug build).
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era
        .checked_mul(146_097)?
        .checked_add(day_of_era)?
        .checked_sub(719_468)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(hour.checked_mul(3_600)?)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)?;
    let millis = seconds.checked_mul(1_000)?.checked_add(millis)?;
    u64::try_from(millis).ok()
}

/// A summary AND the identity of the file it was read from, or `None` for
/// that identity when the file changed underneath the read.
///
/// stat → read → stat. Taking the identity afterwards, separately, was not
/// the same thing: a reader could parse one file, stall, and then stamp its
/// answer with the identity of the file that replaced it — writing an old
/// reading under a new generation, where nothing downstream could tell.
/// When the two stats disagree the reading is simply dropped; the next cycle
/// reads again, microseconds later.
pub(crate) fn read_transcript_summary(
    path: &Path,
    now: u64,
) -> Result<(TranscriptSummary, Option<FileFingerprint>)> {
    let before = file_fingerprint(path);
    let summary = parse_transcript_summary(path, now)?;
    // Signalled between the read and the second stat, so a test can change
    // the file exactly inside the window this guards instead of racing it
    // with a sleep. Thread-local with an RAII guard: a process-wide seam
    // would couple every test in the suite to every other.
    #[cfg(test)]
    transcript_read_seam::signal();
    let after = file_fingerprint(path);
    let stable = match (before, after) {
        (Some(before), Some(after)) if before == after => Some(after),
        _ => None,
    };
    Ok((summary, stable))
}

/// Is the file still exactly the one a reading came from?
///
/// The last question before a reading is written down. Ordering two readings
/// inside the write could not answer it: two inodes have no order between
/// them, so a rule of always-accept-a-different-inode let a reading of the
/// REPLACED file beat the reading of the file that replaced it, purely by
/// committing second. There is no total order to be had in SQL — but there
/// is a fact on disk, and this asks it as late as it can.
fn file_unchanged_since(path: &Path, read_from: FileFingerprint) -> bool {
    // The same seam as the read, fired in the OTHER window this guards: the
    // gap between a reading that held still and the write it is about. That
    // gap has no other test hook -- everywhere else the reading and its
    // fingerprint are taken together, so they cannot disagree -- and a test
    // that tried to race it with a thread would be the flake it was meant to
    // catch. Thread-local and opt-in, so an unarmed test sees nothing.
    #[cfg(test)]
    transcript_read_seam::signal();
    file_fingerprint(path) == Some(read_from)
}

/// Signalled inside `read_transcript_summary`, between the read and the
/// second stat, so a test can act in exactly that window.
#[cfg(test)]
pub(crate) mod transcript_read_seam {
    thread_local! {
        static SEAM: std::cell::RefCell<Option<Box<dyn Fn()>>> =
            const { std::cell::RefCell::new(None) };
    }

    pub(crate) struct Armed;

    pub(crate) fn arm(action: impl Fn() + 'static) -> Armed {
        SEAM.with(|seam| seam.borrow_mut().replace(Box::new(action)));
        Armed
    }

    impl Drop for Armed {
        fn drop(&mut self) {
            SEAM.with(|seam| seam.borrow_mut().take());
        }
    }

    pub(crate) fn signal() {
        // Taken out for the call so a seam that reads the file itself cannot
        // re-enter this borrow.
        let action = SEAM.with(|seam| seam.borrow_mut().take());
        if let Some(action) = action {
            action();
            SEAM.with(|seam| seam.borrow_mut().replace(action));
        }
    }
}

fn file_fingerprint(path: &Path) -> Option<FileFingerprint> {
    let meta = fs::metadata(path).ok()?;
    let generation_ns = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos();
    #[cfg(unix)]
    let (inode, changed_ns) = {
        use std::os::unix::fs::MetadataExt as _;
        (meta.ino(), meta.ctime_nsec() + meta.ctime() * 1_000_000_000)
    };
    #[cfg(not(unix))]
    let (inode, changed_ns) = (0u64, 0i64);
    Some((generation_ns, meta.len(), inode, changed_ns))
}

pub(crate) fn parse_transcript_summary(path: &Path, now: u64) -> Result<TranscriptSummary> {
    // NANOSECONDS and the inode, not milliseconds alone. A rewrite of the
    // same length inside one millisecond — which is what an append-and-
    // truncate or a restore looks like — matched the old key exactly, and
    // the stale summary was served forever: the row could never be corrected
    // downward again, however the file changed.
    let fingerprint = file_fingerprint(path);
    if let Some(fingerprint) = fingerprint {
        let mut guard = TRANSCRIPT_SUMMARY_CACHE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((cached, summary)) = guard
            .get_or_insert_with(std::collections::HashMap::new)
            .get(path)
        {
            if *cached == fingerprint {
                // The FILE is unchanged — but this summary was built against
                // a clock, and the clock is not part of the key. Two ways it
                // goes stale on its own:
                //
                //   * a record refused for being ahead of the clock is
                //     legitimate once the clock reaches it;
                //   * a record accepted before a clock rollback is now in
                //     the future, and would be reported as fact.
                //
                // Either way the cached answer is no longer the answer.
                let refusal_now_believable = summary
                    .earliest_refused_future_at
                    .is_some_and(|refused| now >= refused);
                let accepted_now_in_future = summary.last_record_at.is_some_and(|at| at > now);
                if !refusal_now_believable && !accepted_now_in_future {
                    return Ok(summary.clone());
                }
            }
        }
    }
    let summary = parse_transcript_summary_uncached(path, now)?;
    if let Some(fingerprint) = fingerprint {
        let mut guard = TRANSCRIPT_SUMMARY_CACHE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cache = guard.get_or_insert_with(std::collections::HashMap::new);
        if cache.len() >= TRANSCRIPT_SUMMARY_CACHE_MAX {
            cache.clear();
        }
        cache.insert(path.to_path_buf(), (fingerprint, summary.clone()));
    }
    Ok(summary)
}

fn parse_transcript_summary_uncached(path: &Path, now: u64) -> Result<TranscriptSummary> {
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
        // WHEN this record was written, taken from the record itself. Every
        // record counts, not just the ones that change the summary: the
        // question is when the session last did anything at all.
        // Filtered PER RECORD, not clamped at the end. Taking the maximum
        // first and clamping afterwards let a single stamp from 2099 hide
        // every legitimate record behind it and read as "now" on every scan
        // for good.
        if let Some(at) = record
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(transcript_timestamp_ms)
        {
            if at <= now {
                summary.last_record_at = Some(match summary.last_record_at {
                    Some(previous) => previous.max(at),
                    None => at,
                });
            } else {
                // Remembered, not discarded: the cache needs to know when
                // this answer stops being the answer.
                summary.earliest_refused_future_at =
                    Some(match summary.earliest_refused_future_at {
                        Some(previous) => previous.min(at),
                        None => at,
                    });
            }
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
/// A scan snapshot, and the evidence behind its recency.
///
/// `None` for the evidence means the file changed while it was being read:
/// the answer belongs to no particular generation of it, so it is not filed
/// at all and the next cycle reads again.
fn scan_snapshot(
    info: &SessionFileInfo,
    now: u64,
) -> (
    BridgeThreadSnapshot,
    Option<(crate::state::UpdatedAt, FileFingerprint)>,
) {
    let (summary, fingerprint) = read_transcript_summary(&info.path, now).unwrap_or_default();
    let measured_at = summary.last_record_at;
    let status_type = match summary.last_record_type.as_deref() {
        Some("user") => "active",
        _ => "idle",
    };
    let snapshot = BridgeThreadSnapshot {
        thread_id: info.session_id.clone(),
        name: summary.name,
        cwd: summary.cwd,
        // MEASURED first, guessed second. The file's mtime says when the
        // file was touched, which is not when the session spoke — a stale
        // thread whose transcript was copied or backed up claimed today's
        // time and sat at the top of `/threads` for good.
        updated_at: Some(measured_at.unwrap_or(info.mtime_ms)),
        status_type: status_type.to_string(),
        status_flags: Vec::new(),
        last_turn_status: None,
        last_preview: summary.last_assistant_text,
        pending_prompt: None,
        event_uid: None,
    };
    // The whole answer for THIS generation, committed as one: a reading with
    // a stamp sets the measurement, a reading without one CLEARS the last
    // measurement and falls back to the mtime. Leaving the old measurement
    // in place meant "measured first, guessed second" only ever held for a
    // row's first write — a transcript truncated or rewritten to carry no
    // stamps kept reporting a time that was no longer in the file.
    let source = fingerprint.map(|fingerprint| {
        let kind = if measured_at.is_some() {
            crate::state::UpdatedAt::Measured
        } else {
            crate::state::UpdatedAt::Guessed
        };
        (kind, fingerprint)
    });
    (snapshot, source)
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
pub(crate) fn prompt_resolved_in_transcript(prompt: &PendingPrompt, transcript: &Path) -> bool {
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
        notification_type: payload
            .get("notification_type")
            .and_then(Value::as_str)
            .map(str::to_string),
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
    // A directory that does not exist yet is genuinely empty -- no hook has
    // ever fired. Anything else is NOT emptiness, it is ignorance, and the
    // two were reported identically: one transient EMFILE or permissions
    // blip made the queue look drained, and a caller acting on "no sockets"
    // routes a reply into a headless `--resume` that forks the session.
    let entries = match fs::read_dir(&spool) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(anyhow::Error::new(error)
                .context(format!("failed to list hook spool at {}", spool.display())))
        }
    };
    let mut files = spool_entry_paths(entries)
        .with_context(|| format!("failed to list hook spool at {}", spool.display()))?;
    files.sort();
    Ok(files)
}

/// The half of the listing that can be handed a failing iterator, so the rule
/// below can be tested without arranging a directory that breaks halfway
/// through — which no test can do reliably.
///
/// `read_dir` SUCCEEDING says only that the directory opened. Each step of the
/// walk can still fail on its own, and `filter_map(Result::ok)` dropped those
/// silently: the caller received a SHORTER list and could not tell it from a
/// complete one. That is the same fail-open as before, one level in — a
/// partial spool still reads as "this session has no socket".
fn spool_entry_paths<I>(entries: I) -> std::io::Result<Vec<PathBuf>>
where
    I: IntoIterator<Item = std::io::Result<fs::DirEntry>>,
{
    let mut files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let is_spool_entry = path.extension().and_then(|ext| ext.to_str()) == Some("json")
            && !path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("")
                .starts_with('.');
        if is_spool_entry {
            files.push(path);
        }
    }
    Ok(files)
}

/// WHEN a hook was received, or nothing.
///
/// Two answers are refused, and neither may fall back to the processing
/// clock. `now` is the NEWEST time there is, so handing it to an entry means
/// handing that entry authority over the session's status, its prompt and
/// its reply route — the strongest claim in the system, given away to the
/// entry that made the weakest case for it.
///
///   * AHEAD OF THE CLOCK. The stamp is written by the hook process and read
///     by the daemon a cycle later, and the two need not agree — a step
///     between them, or a machine that is simply wrong. A time that has not
///     happened is not a time. The file name is checked first, since it
///     carries the same instant written independently.
///   * MISSING, NULL, OR NOT A NUMBER. An entry that does not follow the
///     protocol has said nothing about when it arrived, and "said nothing"
///     is not "said now".
///
/// `None` means the entry cannot be placed in time. Callers keep it rather
/// than delete it, and refuse to answer socket questions while it is there:
/// its own hook may be the one that matters.
fn normalized_received_at(envelope: &Value, now: u64, path: &Path) -> Placement {
    let claimed = envelope.get("receivedAt").and_then(Value::as_u64);
    if let Some(claimed) = claimed.filter(|claimed| *claimed <= now) {
        return Placement::At(claimed);
    }
    // The SAME second chance for every unusable body, not just an impossible
    // one. Missing, null, a string, or a time that has not happened all mean
    // the same thing here — the body cannot say when this arrived — and the
    // name was written from that instant by the same process, in a form
    // nothing since has had a chance to edit.
    let from_name = received_at_from_spool_name(path);
    if let Some(from_name) = from_name.filter(|from_name| *from_name <= now) {
        eprintln!(
            "tinyctb: hook spool entry {} does not carry a usable `receivedAt` ({}); using \
             {from_name} from its name instead",
            path.display(),
            envelope
                .get("receivedAt")
                .map_or_else(|| "absent".to_string(), |value| value.to_string())
        );
        return Placement::At(from_name);
    }
    // BOTH stamps are structurally fine and both are ahead of the clock.
    // That is not an entry nobody can place — it is an entry nobody can
    // place YET, and the two were treated alike: a single clock correction
    // sent real hooks to the dead letter box for good, taking their
    // notifications and their socket with them. It waits instead.
    match claimed.or(from_name) {
        Some(due) => Placement::NotYet(due),
        None => {
            eprintln!(
                "tinyctb: hook spool entry {} cannot be placed in time by its body or its name",
                path.display()
            );
            Placement::Never
        }
    }
}

/// What could be learned about WHEN an entry arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Placement {
    /// A time the clock has already reached.
    At(u64),
    /// A well-formed time that has not arrived yet. The entry is real and
    /// will be ordinary as soon as the clock gets there — a rolled-back
    /// clock is the usual reason, and it rolls forward again.
    NotYet(u64),
    /// Neither the body nor the name says anything usable. No amount of
    /// waiting changes that.
    Never,
}

/// Where entries that are merely EARLY wait for the clock to reach them.
/// Out of the queue so they cannot hold its window, and checked at the top of
/// every cycle so nothing waits longer than it has to.
pub(crate) fn spool_future_dir() -> Result<PathBuf> {
    Ok(events_spool_dir()?.join("future"))
}

/// Where entries that cannot be placed in time go to stop being in the way.
///
/// LEAVING them was a slow poison. Each cycle reads the oldest bounded number
/// of entries, and an entry that is never placed is never released — so a
/// few hundred of them fill that window permanently, and every real hook
/// behind them waits forever while the cycle reports the same files consumed
/// over and over.
pub(crate) fn spool_dead_letter_dir() -> Result<PathBuf> {
    Ok(events_spool_dir()?.join("unplaceable"))
}

/// Bring back every entry whose time the clock has now reached.
pub(crate) fn requeue_due_future_entries(now: u64) -> Result<usize> {
    let dir = spool_future_dir()?;
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(anyhow::Error::new(error)
                .context(format!("failed to list parked hooks at {}", dir.display())))
        }
    };
    let spool = events_spool_dir()?;
    let mut returned = 0usize;
    for path in spool_entry_paths(entries)? {
        let due = received_at_from_spool_name(&path);
        // The name is the only stamp out here that can be trusted to sort,
        // and a parked entry keeps the name it came in with. One that cannot
        // be read at all is not going to become readable: it belongs with
        // the entries nothing can place.
        match due {
            Some(due) if due > now => continue,
            Some(_) => {}
            None => {
                set_aside_unplaceable_entry(&path)?;
                continue;
            }
        }
        let Some(name) = path.file_name() else {
            continue;
        };
        fs::rename(&path, spool.join(name)).with_context(|| {
            format!(
                "failed to return parked hook {} to the queue",
                path.display()
            )
        })?;
        returned += 1;
    }
    if returned > 0 {
        eprintln!(
            "tinyctb: returned {returned} parked hook(s) to the queue; the clock reached them"
        );
    }
    Ok(returned)
}

/// Park an entry that is only EARLY. It keeps its name, which is what says
/// when it is due.
fn park_future_entry(path: &Path, due: u64) -> Result<()> {
    let dir = spool_future_dir()?;
    fs::create_dir_all(&dir)?;
    let name = path
        .file_name()
        .map(|name| name.to_owned())
        .unwrap_or_else(|| std::ffi::OsString::from("entry.json"));
    let target = dir.join(name);
    fs::rename(path, &target).with_context(|| {
        format!(
            "failed to park early hook spool entry {} until {due}",
            path.display()
        )
    })?;
    eprintln!(
        "tinyctb: hook spool entry {} is stamped {due}, ahead of the clock; parked until the \
         clock reaches it",
        path.display()
    );
    Ok(())
}

/// Move an entry out of the queue without destroying it: it is a real hook,
/// it is simply not one anything here can order. It stays on disk, under a
/// name that says why, for whoever comes looking.
fn set_aside_unplaceable_entry(path: &Path) -> Result<()> {
    let dir = spool_dead_letter_dir()?;
    fs::create_dir_all(&dir)?;
    let name = path
        .file_name()
        .map(|name| name.to_owned())
        .unwrap_or_else(|| std::ffi::OsString::from("entry.json"));
    let target = dir.join(name);
    // Same filesystem, so a rename is atomic: the entry is never in both
    // places and never in neither.
    fs::rename(path, &target).with_context(|| {
        format!(
            "failed to set aside unplaceable hook spool entry {}",
            path.display()
        )
    })?;
    eprintln!(
        "tinyctb: moved unplaceable hook spool entry to {}; it is out of the queue and still on \
         disk",
        target.display()
    );
    Ok(())
}

/// The leading field of `{received_at:015}-{pid}-{event}.json`, and only when
/// it is exactly that: fifteen digits, nothing else. A loose parse would let
/// any file name in the spool dictate an ordering.
fn received_at_from_spool_name(path: &Path) -> Option<u64> {
    let stem = path.file_name().and_then(|name| name.to_str())?;
    let (digits, _) = stem.split_once('-')?;
    if digits.len() != 15 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
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
/// Settle every route whose doubt can be settled from the filesystem, and
/// report how many are left in question.
///
/// Waiting for "the next hook" to vouch for a route was a DEADLOCK, and it
/// closed on precisely the session that needed the reply most: one stopped
/// at a prompt produces no further hook until it is answered, its answer was
/// being held until a hook arrived, and holding was global — so one waiting
/// session stopped every reply in the bridge. A leftover socket file did the
/// same thing with nobody behind it at all.
///
/// The way out was already on the row. A recorded inode and boot id are what
/// was seen at the time; if the path still answers to both, this IS that
/// socket — the only thing that was ever in doubt was the timestamp beside
/// it. That settles it without anyone having to speak first. A path that is
/// gone, or that now answers to a different identity, settles it the other
/// way: the route is dropped and a reply falls back honestly.
///
/// What is left over is a route with no identity recorded to check against —
/// rows from before that was stored. Those stay in question, and the reply
/// path deals with each one where it is: it may try the socket, and it may
/// not fall back to spawning a second session if that fails.
pub(crate) fn clear_resolved_socket_quarantines(conn: &Connection) -> Result<usize> {
    let mut unresolved = 0usize;
    for (thread_id, socket) in crate::state::unverified_socket_routes(conn)? {
        let (Some(inode), Some(boot)) = (socket.inode, socket.boot_id.as_deref()) else {
            // Nothing recorded to check against. Presence alone proves only
            // that A socket is there, not that it is this one.
            if Path::new(&socket.path).exists() {
                unresolved += 1;
            } else {
                crate::state::forget_unverified_socket_route(conn, &thread_id)?;
            }
            continue;
        };
        let (current_inode, current_boot) = socket_identity(&socket.path);
        if Path::new(&socket.path).exists()
            && current_inode == Some(inode)
            && current_boot.as_deref() == Some(boot)
        {
            crate::state::vouch_for_socket_route(conn, &thread_id)?;
        } else {
            crate::state::forget_unverified_socket_route(conn, &thread_id)?;
        }
    }
    Ok(unresolved)
}

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
    let mut latest: BTreeMap<String, (SessionSocket, u64)> = BTreeMap::new();
    for path in files {
        // The same distinction ingestion makes, and for the same reason: a
        // read that failed may well succeed next cycle and says nothing
        // about the queue, while malformed JSON is a fact about that one
        // file. Collapsing them into `.ok()` turned a transient error into
        // the confident claim that a session has no socket.
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(error) => {
                return Err(anyhow::Error::new(error).context(format!(
                    "failed to read hook spool entry {} while looking for session sockets",
                    path.display()
                )))
            }
        };
        let Ok(envelope) = serde_json::from_str::<Value>(&raw) else {
            eprintln!(
                "tinyctb: hook spool entry {} is not valid JSON; ignoring it for socket lookup",
                path.display()
            );
            continue;
        };
        let session_id = envelope
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if session_id.is_empty() || session_id == "unknown" {
            continue;
        }
        // Fail CLOSED. This entry may be the one that moved the session's
        // socket, and an answer that silently leaves it out is the confident
        // "no mapping" that sends a reply into a headless fork. An entry that
        // is merely early is no different here: it is in the spool, it is
        // real, and nothing about it is known yet.
        let Placement::At(received_at) = normalized_received_at(&envelope, now, &path) else {
            return Err(anyhow::anyhow!(
                "hook spool entry {} cannot be placed in time; the socket picture is incomplete",
                path.display()
            ));
        };
        if let Some(socket) = session_socket_from_envelope(&envelope) {
            latest.insert(session_id.to_string(), (socket, received_at));
        }
    }
    for (session_id, (socket, received_at)) in &latest {
        crate::state::record_session_messaging_socket(conn, session_id, socket, *received_at, now)?;
    }
    Ok(latest.len())
}

/// Hook events read out of the spool, plus the FILES they came from.
///
/// The files are handed back rather than deleted here. A spool entry is the
/// only copy of a hook until everything it causes is durable — the row, the
/// derived events, and the outbound queue that turns them into a phone
/// notification. Deleting it at parse time made the delivery marker's
/// rollback pointless: the marker went back, and the input it would have
/// been re-derived from was already gone.
type SpoolIngest = (
    Vec<BridgeThreadSnapshot>,
    // The socket each session was last seen listening on, WITH the time that
    // sighting was made: the mapping is only as current as its observation.
    BTreeMap<String, (SessionSocket, u64)>,
    usize,
    // The entries this cycle is holding, to be released once its effects are
    // durable.
    Vec<PathBuf>,
);

pub(crate) fn ingest_spool_events(now: u64) -> Result<SpoolIngest> {
    // Whatever was only EARLY last time may be due now. The daemon does this
    // before its socket peek as well — an entry that comes due this cycle may
    // be a session's FIRST hook, and the reply lane runs first — but doing it
    // again here costs a `read_dir` and keeps every other caller correct.
    requeue_due_future_entries(now)?;
    let mut files = spool_event_files()?;
    files.truncate(MAX_SPOOL_EVENTS_PER_CYCLE);

    let mut consumed = 0usize;
    let mut set_aside = 0usize;
    let mut parked = 0usize;
    // Held until the cycle's effects are durable, then unlinked by the caller.
    let mut held: Vec<PathBuf> = Vec::new();
    let mut sockets: BTreeMap<String, (SessionSocket, u64)> = BTreeMap::new();
    // Snapshots about to be overwritten by a later event in the same batch,
    // whose EFFECT must survive the overwrite even though their state does
    // not. Two of them: an answer that already completed (two concurrent
    // replies can both finish within one poll cycle, and a new turn can
    // start right after an answer), and a question the replacing hook cannot
    // be shown to postdate.
    let mut carried: Vec<BridgeThreadSnapshot> = Vec::new();
    let mut by_session: BTreeMap<String, BridgeThreadSnapshot> = BTreeMap::new();
    for path in files {
        // The spool file name ({receivedAt}-{pid}-{event}) is unique even for
        // hooks firing in the same millisecond; it becomes the event uid.
        let event_uid = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(str::to_string);
        // An I/O failure and a parse failure are NOT the same thing. A
        // malformed entry can never succeed and must go, or it wedges the
        // loop forever; a read that failed — EMFILE, a permissions blip, a
        // mount going away — may well succeed next cycle, and deleting it
        // loses a real hook. Merging them meant one transient error was
        // enough to destroy the only copy.
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(error) => {
                eprintln!(
                    "tinyctb: could not read hook spool entry {} ({error}); keeping it for the \
                     next cycle",
                    path.display()
                );
                continue;
            }
        };
        consumed += 1;
        let Ok(envelope) = serde_json::from_str::<Value>(&raw) else {
            // Malformed: delete it NOW. It can never succeed, and re-reading
            // it every cycle would wedge the loop forever. A failure to
            // delete is loud — it means this entry will be parsed again.
            match fs::remove_file(&path) {
                Ok(()) => eprintln!(
                    "tinyctb: discarded malformed hook spool entry {}",
                    path.display()
                ),
                Err(error) => eprintln!(
                    "tinyctb: could not discard malformed hook spool entry {} ({error}); it will \
                     be parsed again next cycle",
                    path.display()
                ),
            }
            continue;
        };
        // Readable: this cycle is now responsible for it, and the file stays
        // until everything it causes has been committed.
        held.push(path.clone());
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
        // Kept, not consumed: the entry is real, and a later cycle — or a
        // human reading the log — may still be able to place it. Taking its
        // socket or its state on `now` would give the weakest evidence in
        // the spool the strongest authority in the system.
        let received_at = match normalized_received_at(&envelope, now, &path) {
            Placement::At(received_at) => received_at,
            // Off the release list — releasing means deleting — and out of
            // the queue, which is the part that matters: left where it was,
            // either of these would hold its place in the oldest-first
            // window and starve every real hook behind it.
            //
            // Read, but not CONSUMED either: nothing was taken from it, and
            // counting it reported the same files as processed cycle after
            // cycle while the queue behind them never moved. Counted only
            // once the move has actually happened — a failed move leaves the
            // entry exactly where it was doing the starving, which is not a
            // thing to report as success.
            placement => {
                let released = held.pop();
                debug_assert_eq!(released.as_ref(), Some(&path));
                consumed -= 1;
                match placement {
                    Placement::NotYet(due) => {
                        park_future_entry(&path, due)?;
                        parked += 1;
                    }
                    _ => {
                        set_aside_unplaceable_entry(&path)?;
                        set_aside += 1;
                    }
                }
                continue;
            }
        };
        if let Some(socket) = session_socket_from_envelope(&envelope) {
            sockets.insert(session_id.clone(), (socket, received_at));
        }
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
            .and_then(|path| parse_transcript_summary(path, now).ok())
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
            // What survives being replaced within one batch. A finished turn
            // always did: its completion happened and is still owed.
            //
            // A PENDING QUESTION now does too, when the hook about to replace
            // it is not strictly newer. The tie rule upstream decides which
            // snapshot the ROW ends up as; it never got a say here, because
            // this collapse ran first and simply dropped the question — so a
            // Stop and a Notification stamped the same millisecond came out
            // as the Stop alone, whichever of them actually came first. The
            // spool cannot break that tie (its file names carry a pid, not a
            // sequence), and of the two mistakes only one is unrecoverable:
            // an extra notification can be ignored, a question that was never
            // announced is never announced. So the question keeps its effect,
            // and the later snapshot still decides what the row says.
            if let Some(previous) = base.take_if(|previous| {
                previous.last_turn_status.as_deref() == Some("completed")
                    || (previous.pending_prompt.is_some()
                        && previous.updated_at.is_some_and(|at| at >= received_at))
            }) {
                carried.push(previous);
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
    // Carried snapshots first (chronologically earlier), then the
    // final per-session state, so later upserts win in the DB.
    carried.extend(by_session.into_values());
    if set_aside > 0 {
        eprintln!(
            "tinyctb: {set_aside} hook spool entr(ies) could not be placed in time and were set \
             aside; they are not counted as consumed"
        );
    }
    if parked > 0 {
        eprintln!(
            "tinyctb: {parked} hook spool entr(ies) are stamped ahead of the clock and are \
             waiting for it; they are not counted as consumed"
        );
    }
    Ok((carried, sockets, consumed, held))
}

// ---------------------------------------------------------------------------
// Sync

/// The row's hook-owned state as the scan finds it: the turn status it will
/// echo back into its result, and the prompt row with the INSTANCE identity a
/// resolution has to name.
type ExistingThreadState = (Option<String>, Option<(PendingPrompt, i64)>);

fn existing_thread_state(conn: &Connection, thread_id: &str) -> Result<ExistingThreadState> {
    use rusqlite::OptionalExtension;
    let last_turn_status: Option<String> = conn
        .query_row(
            "SELECT last_turn_status FROM threads_cache WHERE thread_id = ?1",
            params![thread_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let pending: Option<(PendingPrompt, i64)> = conn
        .query_row(
            "SELECT prompt_id, prompt_kind, prompt_status, question, transcript_bytes, notification_type, revision
             FROM pending_prompts WHERE thread_id = ?1",
            params![thread_id],
            |row| {
                Ok((
                    PendingPrompt {
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
                    notification_type: row.get(5)?,
                    },
                    row.get(6)?,
                ))
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
    let (hook_snapshots, sockets, consumed, held_spool_entries) = ingest_spool_events(now)?;
    for (session_id, (socket, received_at)) in &sockets {
        crate::state::record_session_messaging_socket(conn, session_id, socket, *received_at, now)?;
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
        let (mut snapshot, source) = scan_snapshot(&info, now);
        let Some((source, read_from)) = source else {
            // The file changed while it was being read, so this answer
            // belongs to no state of it. Filing it would put a reading of
            // one file under the identity of another.
            continue;
        };
        let (last_turn_status, pending) = existing_thread_state(conn, &info.session_id)?;
        snapshot.last_turn_status = last_turn_status;
        // Without this check an answered dialog stayed "pending" until the
        // turn's Stop — for a long agentic turn that meant /threads pinning
        // a phantom "waiting on you" for hours (measured live 2026-08-15:
        // it talked a user into killing an active session).
        //
        // What the scan may act on is THIS prompt and no other: it read it,
        // it checked the transcript, it found it answered. A prompt written
        // since is none of its business.
        let resolved = pending
            .as_ref()
            .filter(|(prompt, _)| prompt_resolved_in_transcript(prompt, &info.path))
            .map(|(_, revision)| *revision);
        snapshot.pending_prompt = pending
            .filter(|_| resolved.is_none())
            .map(|(prompt, _)| prompt);
        // Is the file STILL the one this answer came from? Ordering two
        // readings inside the write could not settle it — two inodes have no
        // order between them — so the question is asked of the disk, and
        // asked INSIDE the write transaction: between an unlocked check and
        // an unlocked write another writer could commit a newer reading and
        // have it overwritten by this one, already known to be stale.
        let path = info.path.clone();
        let still_current = move || file_unchanged_since(&path, read_from);
        // A REFUSED write is not a synced thread. The snapshot in hand
        // describes a file that has already moved; reporting it would hand
        // the caller, and the count, a reading the database just threw away.
        if upsert_thread_snapshot(
            conn,
            &snapshot,
            now,
            source,
            resolved,
            Some(&still_current),
            None,
        )? == crate::state::SnapshotWrite::Applied
        {
            threads.push(thread_snapshot_json(&snapshot));
        }
    }

    let mut result = json!({
        "synced": threads.len(),
        "threads": threads,
        // Not part of the synced state: snapshots a newer hook overtook,
        // carried solely so their own events enrich from the snapshot they
        // came from (see `reconcile_thread_snapshots`).
        "overtaken": reconcile.get("overtaken").cloned().unwrap_or_else(|| json!([])),
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
    // ONLY NOW. Until this point each spool entry is the one copy of its
    // hook: the row, the derived events and the outbound queue all had to
    // commit first. Deleting at parse time meant a failed enqueue rolled the
    // delivery marker back correctly and still lost the notification, because
    // the input it would have been re-derived from was already gone.
    for path in held_spool_entries {
        if let Err(error) = fs::remove_file(&path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "tinyctb: could not release hook spool entry {} ({error}); it will be read \
                     again next cycle",
                    path.display()
                );
            }
        }
    }
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

fn enrich_event_with_thread(event: Value, threads: &[Value], overtaken: &[Value]) -> Value {
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
    let found = match event_uid {
        // A uid names exactly one hook. Its snapshot may sit in the current
        // state or among the overtaken (a late completion whose row a newer
        // hook already owns) — and a same-session SIBLING is no substitute
        // in either case: rendered from one, a finished turn's notification
        // shows the next question as its answer. No match, no enrichment;
        // the event still carries its own preview.
        Some(uid) => threads
            .iter()
            .chain(overtaken.iter())
            .find(|thread| matches_id(thread) && thread.get("eventUid") == Some(uid)),
        None => threads
            .iter()
            .find(|thread| {
                matches_id(thread)
                    && updated_at.map_or(true, |updated| thread.get("updatedAt") == Some(updated))
            })
            .or_else(|| threads.iter().find(matches_id)),
    };
    let Some(thread) = found else {
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
    let overtaken = sync_result
        .get("overtaken")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let events = sync_result
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|event| enrich_event_with_thread(event, &threads, &overtaken))
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

/// Per-turn cgroup (v2) OWNERSHIP. The turn's process group is not guessed
/// from pids — it is a kernel object we created: membership is
/// `cgroup.procs`, killing is one write to `cgroup.kill` (atomic, the whole
/// subtree, immune to pid reuse), emptiness is `populated 0` in
/// `cgroup.events`, and the directory outlives daemon restarts, so
/// ownership survives them structurally. Everything the pid machinery had
/// to PROVE — anchors, incarnations, birth debts — is simply true here by
/// construction. On a host that cannot give us a subtree (no v2, no write
/// access), `create` returns None and the spawn stays on the killpg regime.
// The create/kill halves run only from cfg(not(test)) spawn/kill paths;
// tests exercise them directly against real cgroups.
#[cfg_attr(test, allow(dead_code))]
pub(crate) mod turn_cgroup {
    use std::fs;
    use std::io::ErrorKind;
    use std::path::{Path, PathBuf};

    /// Where THIS process lives in the unified hierarchy.
    fn own_cgroup_dir() -> Option<PathBuf> {
        let raw = fs::read_to_string("/proc/self/cgroup").ok()?;
        let rel = raw
            .lines()
            .find_map(|line| line.strip_prefix("0::"))?
            .trim();
        if rel.is_empty() || rel == "/" {
            return None;
        }
        Some(PathBuf::from(format!("/sys/fs/cgroup{rel}")))
    }

    /// The STABLE owner subtree for turn objects: the tinyctb SERVICE's
    /// cgroup, never the caller's. Parented under a terminal's scope (the
    /// CLI case) a turn would die with the terminal; under the service —
    /// whose unit sets KillMode=process precisely so restarts spare the
    /// subtrees — it survives daemon restarts. Cross-scope migration works
    /// because everything under user@.service is delegated to the user
    /// (verified on this host). Tests override the root explicitly.
    pub(crate) fn owner_root() -> Option<PathBuf> {
        if let Ok(root) = std::env::var("TINYCTB_CGROUP_ROOT") {
            let root = PathBuf::from(root);
            return root.is_dir().then_some(root);
        }
        // SAFETY: reads our own uid; touches nothing.
        let uid = unsafe { libc::getuid() };
        let service = PathBuf::from(format!(
            "/sys/fs/cgroup/user.slice/user-{uid}.slice/user@{uid}.service/app.slice/tinyctb.service"
        ));
        if service.is_dir() {
            return Some(service);
        }
        // No installed service (bare CLI use): our own subtree still gives
        // correct ownership for as long as the caller's scope lives.
        own_cgroup_dir().filter(|dir| dir.is_dir())
    }

    /// Create the turn's cgroup BEFORE anything is spawned. None means this
    /// host cannot provide one; the caller decides whether that is fatal.
    pub(crate) fn create(turn_id: &str) -> Option<PathBuf> {
        let dir = owner_root()?.join(format!("turn-{turn_id}"));
        match fs::create_dir(&dir) {
            Ok(()) => Some(dir),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => Some(dir),
            Err(_) => None,
        }
    }

    /// Validate a path RECORDED IN THE DATABASE before acting on it: it
    /// must live under the unified hierarchy and be named for exactly this
    /// turn. `cgroup.kill` on an arbitrary path would kill whatever lives
    /// there — a corrupted row must never be able to aim it.
    pub(crate) fn validated(path: &str, turn_id: &str) -> Option<PathBuf> {
        let dir = PathBuf::from(path);
        let expected = format!("turn-{turn_id}");
        if path.contains("..") {
            return None;
        }
        // Confined to the TRUSTED owner subtree, not merely the unified
        // hierarchy: a corrupted row must not aim `cgroup.kill` at some
        // other service's tree just because it lives under /sys/fs/cgroup.
        let root = owner_root()?;
        if !dir.starts_with(&root) {
            return None;
        }
        if dir.file_name()?.to_str()? != expected {
            return None;
        }
        Some(dir)
    }

    /// SIGKILL the whole subtree, atomically. Nothing to confirm here —
    /// `populated` is the confirmation.
    pub(crate) fn kill(dir: &Path) -> bool {
        fs::write(dir.join("cgroup.kill"), "1").is_ok()
    }

    /// Is anything still alive in the turn's cgroup? `Some(false)` is the
    /// PROOF the whole stopping machinery exists to obtain. A directory
    /// that no longer exists was settled and removed earlier — equally
    /// empty. An unreadable one proves nothing.
    pub(crate) fn populated(dir: &Path) -> Option<bool> {
        match fs::read_to_string(dir.join("cgroup.events")) {
            Ok(events) => Some(events.lines().any(|line| line.trim() == "populated 1")),
            Err(err) if err.kind() == ErrorKind::NotFound => Some(false),
            Err(_) => None,
        }
    }

    /// Bounded wait for a proof of emptiness.
    pub(crate) fn confirmed_empty(dir: &Path, within: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + within;
        loop {
            match populated(dir) {
                Some(false) => return true,
                Some(true) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(50))
                }
                _ => return false,
            }
        }
    }

    /// TEARDOWN: kill and remove EVERY turn object under the owner root.
    /// `reset` and `daemon uninstall` call this BEFORE deleting the ledger
    /// or the unit — with `KillMode=process` sparing the sub-trees, a
    /// teardown that skipped this would strand every running turn with no
    /// ledger and no supervisor. Returns the objects that could NOT be
    /// proven empty: the caller must ABORT on any, never delete the ledger
    /// out from under live work.
    pub(crate) fn sweep_all(within: std::time::Duration) -> anyhow::Result<SweepReport> {
        use anyhow::Context as _;
        let mut report = SweepReport::default();
        let Some(root) = owner_root() else {
            // A CONFIGURED root that is unusable is an error, not "no
            // objects": swallowing it would let a teardown proceed blind.
            if std::env::var_os("TINYCTB_CGROUP_ROOT").is_some() {
                anyhow::bail!("the configured cgroup root is not a usable directory");
            }
            return Ok(report);
        };
        let entries = fs::read_dir(&root).with_context(|| {
            format!("failed to enumerate turn objects under {}", root.display())
        })?;
        for entry in entries {
            let entry = entry
                .with_context(|| format!("failed to read an entry under {}", root.display()))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with("turn-") {
                continue;
            }
            let dir = entry.path();
            let _ = kill(&dir);
            // Proven empty AND actually removed, or it goes on the report:
            // "could not prove" and "could not remove" both block a
            // teardown — the contract is proof, not optimism.
            if confirmed_empty(&dir, within) && remove(&dir) {
                report.removed += 1;
            } else {
                report.stubborn.push(dir);
            }
        }
        Ok(report)
    }

    /// What a full sweep managed to prove.
    #[derive(Debug, Default)]
    pub(crate) struct SweepReport {
        pub(crate) removed: usize,
        pub(crate) stubborn: Vec<PathBuf>,
    }

    /// Remove an EMPTY turn cgroup; a populated one refuses (rmdir EBUSY),
    /// which is exactly the safety we want.
    pub(crate) fn remove(dir: &Path) -> bool {
        match fs::remove_dir(dir) {
            Ok(()) => true,
            Err(err) if err.kind() == ErrorKind::NotFound => true,
            Err(_) => false,
        }
    }
}

/// Live handles of spawned headless turns. The daemon reaps these every cycle
/// so finished children never linger as zombies — a zombie still answers
/// `kill -0`, which would make crash detection report "running" forever.
#[cfg_attr(test, allow(dead_code))]
mod turn_children {
    use std::process::Child;
    use std::sync::Mutex;

    /// A live handle BOUND to the turn that spawned it. The binding is the
    /// point: a stale `stopping` row can carry a pid the kernel has since
    /// recycled onto a NEWER turn, so matching registry entries by pid would
    /// hand that newer turn's process to the old turn's kill.
    pub(super) struct RunningTurn {
        pub(super) turn_id: String,
        pub(super) child: Child,
    }

    pub(super) static RUNNING: Mutex<Vec<RunningTurn>> = Mutex::new(Vec::new());

    /// Remove and return the handle for exactly this turn. Pid is not an
    /// accepted key: pids get recycled, turn ids do not.
    pub(super) fn take(turn_id: &str) -> Option<RunningTurn> {
        let mut running = RUNNING.lock().expect("turn children lock");
        let index = running.iter().position(|entry| entry.turn_id == turn_id)?;
        Some(running.remove(index))
    }

    pub(super) fn put_back(entry: RunningTurn) {
        RUNNING.lock().expect("turn children lock").push(entry);
    }
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
    use std::cell::{Cell, RefCell};

    // Thread-local ON PURPOSE. Every test drives the kill path on its own
    // thread, and these were once process-wide: parallel tests stole each
    // other's recorded pids via `take()` and raced over the forced outcome,
    // a flake that no "remember to hold the lock" convention survived.
    thread_local! {
        static KILLED: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };

        /// What `kill_turn_process` should report next. Real termination
        /// cannot be exercised in-process, but the CALLER's behaviour on
        /// each outcome is exactly what must not regress: settling a turn
        /// whose process was never confirmed dead drops a live process out
        /// of every later scan.
        static OUTCOME: Cell<Option<super::KillOutcome>> = const { Cell::new(None) };
    }

    pub(crate) fn record(pid: u32) {
        KILLED.with(|killed| killed.borrow_mut().push(pid));
    }

    pub(crate) fn forced_outcome() -> Option<super::KillOutcome> {
        OUTCOME.with(Cell::get)
    }

    pub(crate) fn take() -> Vec<u32> {
        KILLED.with(RefCell::take)
    }

    /// RAII: restores the outcome even if the test panics, so a forced
    /// verdict cannot leak into this thread's next assertion.
    pub(crate) struct OutcomeGuard;

    impl OutcomeGuard {
        pub(crate) fn set(outcome: super::KillOutcome) -> Self {
            OUTCOME.with(|cell| cell.set(Some(outcome)));
            OutcomeGuard
        }
    }

    impl Drop for OutcomeGuard {
        fn drop(&mut self) {
            OUTCOME.with(|cell| cell.set(None));
        }
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

/// What we can say about a process we signalled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Liveness {
    /// Provably over: the entry is gone, it is a zombie, or the pid now
    /// belongs to a different incarnation.
    Ended,
    /// Still there, same incarnation.
    Alive,
    /// Could not tell. NOT the same as ended — treating an unreadable
    /// `/proc` as death is how a live process gets recorded as stopped and
    /// then disappears from every later scan.
    Unknown,
}

/// Is the incarnation identified by `pid` + `expected_ticks` over?
///
/// `expected_ticks` is the value PERSISTED when the turn was spawned, never
/// a fresh sample: sampling after signalling can read `None` while the
/// process is merely momentarily unreadable, and a later `Some` would then
/// look like a generation change and be misread as death.
fn incarnation_liveness(pid: u32, expected_ticks: &str) -> Liveness {
    match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => liveness_from_stat(&stat, expected_ticks),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Liveness::Ended,
        // Any OTHER read failure is ignorance, not death. Reporting it as
        // ended is how a live process gets recorded as stopped.
        Err(_) => Liveness::Unknown,
    }
}

/// The parsing half, split out so every branch is reachable from a test
/// (a real `/proc` entry cannot be made malformed on demand).
fn liveness_from_stat(stat: &str, expected_ticks: &str) -> Liveness {
    let Some(rest) = stat.rsplit_once(')').map(|(_, rest)| rest) else {
        return Liveness::Unknown;
    };
    let mut fields = rest.split_whitespace();
    let Some(state) = fields.next() else {
        return Liveness::Unknown;
    };
    if state == "Z" {
        return Liveness::Ended; // dead, just not reaped yet
    }
    match fields.nth(18) {
        Some(ticks) if ticks == expected_ticks => Liveness::Alive,
        // A different incarnation owns this pid now: ours is over.
        Some(_) => Liveness::Ended,
        None => Liveness::Unknown,
    }
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
/// verify the PID still belongs to the turn before signalling it.
///
/// `pgid` is DETERMINISTIC, not observed: the spawn puts the child in its
/// own process group (`process_group(0)`), so the kernel guarantees
/// pgid == pid. Observing it through `ps` could fail, and a persisted NULL
/// pgid is the one hole the whole stopping machinery cannot recover from —
/// the daemon's group probe answers `Unknown` forever and a stopped turn
/// never settles.
pub(crate) fn capture_process_identity(pid: Option<u32>) -> ProcessIdentity {
    let Some(pid) = pid.filter(|pid| *pid > 0) else {
        return ProcessIdentity::default();
    };
    ProcessIdentity {
        lstart: ps_value(pid, "lstart="),
        #[cfg(unix)]
        pgid: Some(pid),
        #[cfg(not(unix))]
        pgid: ps_value(pid, "pgid=").and_then(|value| value.trim().parse::<u32>().ok()),
        exe: process_exe_path(pid),
        start_ticks: process_start_ticks(pid),
        boot_id: current_boot_id(),
    }
}

/// What a spawn MUST know about its child before the turn may live —
/// otherwise the turn can never be settled once the user stops it: the
/// daemon's recovery keys on `pgid`, and the Linux kill paths on
/// ticks + boot id. Returns why the identity is unusable, or None when it
/// is complete enough for this platform.
fn incomplete_spawn_identity(identity: &ProcessIdentity) -> Option<&'static str> {
    if identity.pgid.is_none() {
        return Some("no process group id");
    }
    #[cfg(target_os = "linux")]
    {
        if identity.start_ticks.is_none() {
            return Some("no starttime ticks from /proc");
        }
        if identity.boot_id.is_none() {
            return Some("no boot id");
        }
    }
    None
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

/// Has this child exited, WITHOUT reaping it? On Linux the answer comes
/// from `/proc` (an exited-but-unreaped leader reads state `Z`), which
/// leaves the leader unreaped — its pid, and with it the group id, stays
/// reserved until WE choose to reap. `try_wait` would answer the same
/// question by reaping, releasing the number for reuse while the group
/// KILL is still pending. Elsewhere `/proc` does not exist and `try_wait`
/// is the only probe there is.
fn leader_exited(child: &mut std::process::Child) -> bool {
    #[cfg(target_os = "linux")]
    {
        let pid = child.id();
        match fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => stat
                .rsplit_once(')')
                .and_then(|(_, rest)| rest.split_whitespace().next())
                .is_some_and(|state| matches!(state, "Z" | "X" | "x")),
            // A child we hold unreaped always has an entry; an unreadable
            // one merely runs the grace out, and the unconditional KILL
            // that follows is the backstop either way.
            Err(_) => false,
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        // No /proc means no way to observe the exit WITHOUT reaping, and
        // reaping here frees the pid while the group KILL is still pending.
        // So the grace simply runs its full course: a flat 2s latency on a
        // stop, in exchange for a KILL that provably targets the original
        // group on every platform.
        let _ = child;
        false
    }
}

/// TERM the whole process group, give the main child a short grace to exit
/// cleanly, then KILL the group UNCONDITIONALLY — a grandchild that ignores
/// TERM must not survive just because the main process exited politely.
/// Returns whether the supplied main child was reaped (true when no handle
/// was supplied); an unreaped child must go back to the registry so a later
/// cycle can collect it.
/// Send a signal to a whole process group IN PROCESS. The external `kill`
/// binary this replaced was an unbounded wait (its exit had to be
/// collected), depended on PATH, and widened the TERM→KILL window with a
/// fork+exec. Returns whether the kernel accepted delivery; ESRCH ("no
/// such group") is false — establishing "already gone" is the group
/// probe's job, not this one's.
fn signal_process_group(pgid: u32, signal: libc::c_int) -> bool {
    if pgid == 0 {
        return false;
    }
    // SAFETY: delivers a signal; no memory is touched.
    (unsafe { libc::killpg(pgid as libc::pid_t, signal) }) == 0
}

pub(crate) fn terminate_process_group(pid: u32, child: Option<&mut std::process::Child>) -> bool {
    if pid == 0 {
        return true;
    }
    let termed = signal_process_group(pid, libc::SIGTERM);
    match child {
        Some(child) => {
            if termed {
                // Grace WITHOUT reaping: the unreaped leader keeps its pid —
                // and with it the group id — reserved, so the KILL below
                // provably targets the original group. Reaping first would
                // free the number for reuse, and a recycled group id would
                // receive the KILL instead.
                let started = std::time::Instant::now();
                while started.elapsed() < Duration::from_secs(2) {
                    if leader_exited(child) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
            if !signal_process_group(pid, libc::SIGKILL) {
                // Group signalling unavailable (or the group already gone):
                // at least SIGKILL the main child through the handle.
                let _ = child.kill();
            }
            // Only now reap — bounded, never blocking the daemon on a child
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
            // Without a handle there is nobody to reap through, so the only
            // honest answer comes from OBSERVING the process — never from a
            // signal's exit code. `kill` returns ESRCH once the target is
            // already gone, which is success, not failure; and it returns
            // success for a process that merely has not exited yet.
            //
            // The caller supplies the ticks recorded at spawn time. This
            // path therefore reports `false` (Undetermined to the caller)
            // whenever it cannot prove the incarnation ended AND its group
            // emptied — a leader that dies while a grandchild keeps working
            // is not a stopped turn.
            unreachable!("the no-handle path goes through terminate_verified_group")
        }
    }
}

/// Is anything still in this process group?
///
/// `killpg(pgid, 0)` sends no signal; it only asks the kernel. That makes it
/// authoritative, and the only answer that survives what enumerating
/// `/proc` cannot: `hidepid=2` hides other users' processes, so a scan sees
/// an empty group that is not empty, and a descendant that dropped
/// privileges is invisible to a scan yet still reported by the kernel — as
/// `EPERM`, which means the group EXISTS.
///
/// `ESRCH` is the only answer that means empty. Everything else is "alive"
/// or "cannot tell", and both must stop us short of recording a turn as
/// stopped.
pub(crate) fn group_liveness(pgid: u32) -> Liveness {
    #[cfg(test)]
    if let Some(forced) = test_group_probe::injected() {
        return forced;
    }
    if pgid == 0 {
        return Liveness::Unknown; // 0 means "my own group" to killpg
    }
    // SAFETY: signal 0 performs the permission and existence checks and
    // delivers nothing.
    let result = unsafe { libc::killpg(pgid as libc::pid_t, 0) };
    let errno = (result != 0)
        .then(|| std::io::Error::last_os_error().raw_os_error())
        .flatten();
    liveness_from_killpg(result, errno)
}

/// The errno mapping, split out because the interesting case cannot be
/// produced on demand: whether `EPERM` reads as "alive" decides if a
/// descendant that dropped privileges can be mistaken for a dead group, and
/// no test can conjure a foreign group that this host refuses to signal.
fn liveness_from_killpg(result: i32, errno: Option<i32>) -> Liveness {
    if result == 0 {
        return Liveness::Alive; // exists and we may signal it
    }
    match errno {
        Some(libc::ESRCH) => Liveness::Ended,
        // The group EXISTS; we simply may not signal it — the exact shape of
        // a descendant that dropped privileges.
        Some(libc::EPERM) => Liveness::Alive,
        _ => Liveness::Unknown,
    }
}

#[cfg(test)]
pub(crate) mod test_group_probe {
    use super::Liveness;
    use std::cell::Cell;

    // Thread-local ON PURPOSE: a process-wide forced verdict leaked into
    // every other test that happened to probe a group at the same moment,
    // including the ones judging REAL processes.
    thread_local! {
        static FORCED: Cell<Option<Liveness>> = const { Cell::new(None) };
    }

    pub(crate) fn injected() -> Option<Liveness> {
        FORCED.with(Cell::get)
    }

    /// RAII so a forced verdict cannot outlive its test on this thread.
    pub(crate) struct ProbeGuard;

    impl ProbeGuard {
        pub(crate) fn set(liveness: Liveness) -> Self {
            FORCED.with(|cell| cell.set(Some(liveness)));
            ProbeGuard
        }
    }

    impl Drop for ProbeGuard {
        fn drop(&mut self) {
            FORCED.with(|cell| cell.set(None));
        }
    }
}

/// Confirm a REAPED leader's whole group is gone. With the full identity
/// the group is watched against the incarnation for a bounded window. With
/// no ticks (macOS: no /proc) the group can still be PROBED: the pid came
/// from our own just-reaped handle, and the probe signals nothing, so
/// `ESRCH` is a safe and sufficient proof — without this arm a reaped
/// macOS turn read `Undetermined` forever and never settled. No pgid at
/// all proves nothing.
fn confirm_reaped_leader(pid: u32, ticks: Option<&str>, pgid: Option<u32>) -> KillOutcome {
    match (ticks, pgid) {
        (Some(ticks), Some(pgid)) => confirm_group_gone(pid, pgid, ticks, Duration::from_secs(2)),
        (None, Some(pgid)) => match group_liveness(pgid) {
            Liveness::Ended => KillOutcome::Terminated,
            _ => KillOutcome::Undetermined,
        },
        (_, None) => KillOutcome::Undetermined,
    }
}

/// Both kill paths end here: a turn is only `Terminated` when the leader's
/// incarnation is over AND its process group is empty.
///
/// Reaping the leader is not enough. A descendant that ignores TERM, or one
/// running under a different uid that we cannot even signal, keeps doing
/// whatever the turn was doing — and recording the turn as `stopped` would
/// drop it out of every later scan.
fn confirm_group_gone(
    pid: u32,
    pgid: u32,
    expected_ticks: &str,
    deadline: Duration,
) -> KillOutcome {
    let until = std::time::Instant::now() + deadline;
    loop {
        let leader = incarnation_liveness(pid, expected_ticks);
        let group = group_liveness(pgid);
        match (leader, group) {
            (Liveness::Ended, Liveness::Ended) => return KillOutcome::Terminated,
            // Ignorance is never death.
            (Liveness::Unknown, _) | (_, Liveness::Unknown) => return KillOutcome::Undetermined,
            _ if std::time::Instant::now() >= until => return KillOutcome::Undetermined,
            _ => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

/// Terminate a turn we have no `Child` handle for (the daemon restarted
/// since it was spawned), reporting what we could actually establish.
fn terminate_verified_group(pid: u32, pgid: u32, expected_ticks: &str) -> KillOutcome {
    // The anchor is verified FIRST: the ticks must still match, or the pid
    // — and the group id derived from it — may already belong to unrelated
    // work. (The check-to-signal gap is microseconds — in-process killpg,
    // no fork/exec in between; closing it entirely
    // needs an ownership primitive like a per-turn cgroup, noted as the
    // long-term fix.)
    if !leader_anchors_group(pid, expected_ticks) {
        return KillOutcome::Undetermined;
    }
    // TERM and KILL back to back, with NO grace between them. This path
    // holds no handle, so nothing can keep the leader unreaped: init (or a
    // subreaper) collects it the moment it dies, the anchor evaporates, and
    // a KILL delayed by a grace window would then have to be refused —
    // leaving a TERM-ignoring grandchild alive and the turn `stopping`
    // forever. Graceful shutdown is the LIVE path's luxury: there the
    // unreaped handle pins the anchor for as long as the grace needs.
    let _ = signal_process_group(pgid, libc::SIGTERM);
    let _ = signal_process_group(pgid, libc::SIGKILL);
    confirm_group_gone(pid, pgid, expected_ticks, Duration::from_secs(2))
}

/// Does this pid still hold OUR incarnation's place — alive, or exited but
/// unreaped? An unreaped leader keeps the pid (and therefore the group id)
/// reserved by the kernel. Distinct from `incarnation_liveness`, which
/// calls a zombie `Ended`: true for "is the turn over", but exactly the
/// wrong question for "may the group id still be trusted".
fn leader_anchors_group(pid: u32, expected_ticks: &str) -> bool {
    process_start_ticks(pid).as_deref() == Some(expected_ticks)
}

/// What actually happened when we tried to end a turn.
///
/// The distinction matters because a caller may want to record the turn as
/// finished, and doing that for a process still running would leave a live
/// process that nothing tracks any more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KillOutcome {
    /// Signalled and reaped: the process is gone.
    Terminated,
    /// Signalled, but not reaped inside the bound. Probably dying, NOT
    /// proven dead.
    Undetermined,
    /// Nothing was signalled — no pid, or the recorded identity (boot id +
    /// pgid + start ticks) no longer matches, so signalling could hit an
    /// unrelated process that inherited the pid.
    Unverified,
}

/// Terminate a headless turn. With a live handle the child is killed and
/// reaped directly; after a daemon restart the stored identity must fully
/// match, otherwise we refuse to signal at all (PID reuse).
pub(crate) fn kill_turn_process(turn: &crate::state::BridgeTurn) -> KillOutcome {
    #[cfg(test)]
    {
        test_kill::record(turn.pid.unwrap_or(0));
        test_kill::forced_outcome().unwrap_or(KillOutcome::Terminated)
    }
    #[cfg(not(test))]
    {
        // cgroup regime FIRST: kill the OBJECT, reap our handle if we hold
        // one (a zombie leader keeps the cgroup populated until reaped),
        // and let `populated` be the confirmation. No anchors and no
        // incarnations — the object cannot be recycled out from under us.
        if let Some(dir) = turn
            .cgroup_path
            .as_deref()
            .and_then(|path| turn_cgroup::validated(path, &turn.turn_id))
        {
            let entry = turn_children::take(&turn.turn_id);
            let killed = turn_cgroup::kill(&dir);
            if let Some(mut entry) = entry {
                let started = std::time::Instant::now();
                let reaped = loop {
                    match entry.child.try_wait() {
                        Ok(Some(_)) | Err(_) => break true,
                        Ok(None) if started.elapsed() >= Duration::from_secs(2) => break false,
                        Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                    }
                };
                if !reaped {
                    turn_children::put_back(entry);
                }
            }
            if !killed {
                return KillOutcome::Undetermined;
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                match turn_cgroup::populated(&dir) {
                    Some(false) => return KillOutcome::Terminated,
                    Some(true) if std::time::Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(100))
                    }
                    _ => return KillOutcome::Undetermined,
                }
            }
        }
        // Legacy killpg regime below (no ownership object recorded).
        // The live registry answers by TURN ID, never by pid: `turn.pid` may
        // have been recycled onto a different, newer turn whose handle sits
        // in the registry, and matching by pid would hand THAT turn's
        // process to this turn's kill.
        if let Some(mut entry) = turn_children::take(&turn.turn_id) {
            let pid = entry.child.id();
            let reaped = terminate_process_group(pid, Some(&mut entry.child));
            // Reaping the leader proves only the leader is gone. The turn is
            // not stopped until its whole group is.
            let confirmed = if reaped {
                confirm_reaped_leader(pid, turn.proc_start_ticks.as_deref(), turn.pgid)
            } else {
                KillOutcome::Undetermined
            };
            if confirmed == KillOutcome::Terminated {
                KillOutcome::Terminated
            } else if reaped {
                KillOutcome::Undetermined
            } else {
                // Not reaped within the bound: hand the entry back so the
                // per-cycle reaper can collect it later instead of leaking a
                // forever-unreapable zombie.
                turn_children::put_back(entry);
                KillOutcome::Undetermined
            }
        } else {
            let Some(pid) = turn.pid.filter(|pid| *pid > 0) else {
                return KillOutcome::Unverified;
            };
            match (turn.proc_start_ticks.as_deref(), turn.pgid) {
                // The PERSISTED pgid, never `pid` as a stand-in: they happen
                // to match for a fresh setsid child, but signalling a guessed
                // group id is not something to do on a coincidence.
                (Some(ticks), Some(pgid)) if verified_restart_identity(turn, pid) => {
                    terminate_verified_group(pid, pgid, ticks)
                }
                // No persisted identity, or it no longer matches: the pid may
                // belong to something else entirely now.
                _ => KillOutcome::Unverified,
            }
        }
    }
}

/// Reap finished headless children. Returns (turn_id, exit_code) per
/// finished child; exit_code is None if the status is unavailable. The turn
/// id — not the pid — is the identity handed to the database: a pid can be
/// shared with an older stale row through recycling, and stamping exits by
/// pid marked both turns at once.
pub(crate) fn reap_finished_turn_processes() -> Vec<(String, Option<i32>)> {
    #[cfg(test)]
    {
        Vec::new()
    }
    #[cfg(not(test))]
    {
        let mut running = turn_children::RUNNING.lock().expect("turn children lock");
        let mut finished = Vec::new();
        running.retain_mut(|entry| match entry.child.try_wait() {
            Ok(Some(status)) => {
                finished.push((entry.turn_id.clone(), status.code()));
                false
            }
            Ok(None) => true,
            Err(_) => {
                finished.push((entry.turn_id.clone(), None));
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
    // cgroup regime: the OWNERSHIP object exists before the row, and the
    // row records it before the process exists — supervision is structural
    // from the first instant. Hosts that cannot provide a subtree record
    // nothing and stay on the killpg regime.
    // The SHARED spawn lock, held across create→register→spawn: a teardown
    // holding it exclusively is provably alone.
    #[cfg(not(test))]
    let _spawn_permit = crate::daemon::hold_spawn_lock_shared()?;
    #[cfg(test)]
    let cgroup: Option<std::path::PathBuf> = None;
    #[cfg(not(test))]
    let cgroup = turn_cgroup::create(turn_id);
    // Linux runs FAIL CLOSED: no ownership object, no spawn. The
    // structural guarantee ("inside its cgroup or never exists") would
    // otherwise silently evaporate exactly when delegation or permissions
    // break. The legacy killpg regime must be opted into explicitly.
    #[cfg(all(target_os = "linux", not(test)))]
    if cgroup.is_none()
        && std::env::var("TINYCTB_LEGACY_PROCESS_SUPERVISION")
            .ok()
            .as_deref()
            != Some("1")
    {
        anyhow::bail!(
            "no cgroup v2 subtree is available for turn supervision; refusing to spawn an              unsupervisable process. Reinstall the daemon unit (Delegate=yes) or set              TINYCTB_LEGACY_PROCESS_SUPERVISION=1 to accept the legacy killpg regime."
        );
    }
    let registered = (|| -> Result<()> {
        let tx = conn.unchecked_transaction()?;
        crate::state::register_bridge_turn(
            &tx,
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
        if let Some(dir) = &cgroup {
            crate::state::record_turn_cgroup(&tx, turn_id, &dir.display().to_string())?;
        }
        tx.commit()?;
        Ok(())
    })();
    if let Err(err) = registered {
        if let Some(dir) = &cgroup {
            let _ = turn_cgroup::remove(dir);
        }
        return Err(err);
    }
    let pid = match spawn_detached_headless(binary, args, cwd, turn_id, cgroup.as_deref()) {
        Ok(pid) => pid,
        Err(err) => {
            // The empty ownership object goes with the failed spawn; a
            // populated one (partial start) refuses removal and stays for
            // the daemon to sweep.
            if let Some(dir) = &cgroup {
                let _ = turn_cgroup::remove(dir);
            }
            return Err(settle_failed_turn(conn, turn_id, now, err));
        }
    };
    // Identity arrives in a second write because it cannot exist before the
    // spawn — and losing that write is NOT survivable. With `pid` still NULL
    // the daemon's crash check calls the turn failed after its 10s grace,
    // the turn stops counting as running, and from then on a live process
    // runs outside the approval boundary with nobody able to reap it. So the
    // write is retried and verified, and a turn that cannot be recorded is
    // killed rather than left running unsupervised.
    let identity = capture_process_identity(pid);
    // An incomplete identity is treated exactly like a failed identity
    // write: kill now, fail the spawn. A supervised process that /stop can
    // accept but never prove dead — pgid NULL probes `Unknown` forever —
    // is worse than no process. (The test build's stubbed spawn never
    // reaches real identity capture, hence the cfg.)
    #[cfg(not(test))]
    if let Some(missing) = incomplete_spawn_identity(&identity) {
        let cleanup = kill_spawned_child(
            turn_id,
            pid,
            identity.start_ticks.as_deref(),
            cgroup.as_deref(),
        );
        let err = anyhow::anyhow!(
            "the spawned turn's identity is incomplete ({missing}); the process was \
             terminated rather than left running beyond the reach of /stop"
        );
        return Err(settle_unwound_spawn(
            conn, turn_id, &identity, pid, cleanup, now, err,
        ));
    }
    if let Err(err) = persist_spawn_identity(conn, turn_id, pid, &identity) {
        let cleanup = kill_spawned_child(
            turn_id,
            pid,
            identity.start_ticks.as_deref(),
            cgroup.as_deref(),
        );
        let err = err.context(
            "could not record the spawned turn's identity; the process was terminated \
             rather than left running outside the approval boundary",
        );
        return Err(settle_unwound_spawn(
            conn, turn_id, &identity, pid, cleanup, now, err,
        ));
    }
    Ok(pid)
}

/// Settle a spawn that had to be unwound — by what the cleanup PROVED.
/// A group confirmed empty is `failed` history. An UNCONFIRMED one stays
/// `running` under the failure-cleanup marker (value 2) — never
/// `stopping`, which is the user's word — and the daemon's recovery loop
/// keeps probing until the group is provably gone, then settles it as
/// `failed` with a failure receipt.
///
/// The invariant with no exceptions: an unproven group NEVER reaches a
/// terminal status from here. A state that overstates liveness stays
/// visible and correctable; a terminal one that lies does not.
fn settle_unwound_spawn(
    conn: &rusqlite::Connection,
    turn_id: &str,
    identity: &ProcessIdentity,
    pid: Option<u32>,
    cleanup: KillOutcome,
    now: u64,
    cause: anyhow::Error,
) -> anyhow::Error {
    // FIRST, whatever the outcome and whatever the status column says:
    // patch the FULL identity in (COALESCE — never overwriting) and sweep
    // the dialogs. A `/stop` that won the race left a `stopping` row with
    // no pid/pgid the daemon could ever probe, and a recovery that saved
    // only pid/pgid could no longer re-signal after a daemon restart
    // (`verified_restart_identity` needs ticks + boot id).
    for attempt in 0..3 {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(50));
        }
        if crate::state::mark_cleanup_pending(
            conn,
            turn_id,
            pid,
            identity.start_ticks.as_deref(),
            identity.boot_id.as_deref(),
            now,
        )
        .is_ok()
        {
            break;
        }
    }
    if cleanup == KillOutcome::Terminated {
        return settle_failed_turn(conn, turn_id, now, cause);
    }
    // The row stays EXACTLY where the marker put it: `running` under the
    // failure-cleanup marker — or `stopping` only if a user's /stop won a
    // race, because that word is the USER's. Writing `stopping` from here
    // made the recovery loop report a spawn failure as "已停止"; instead
    // the daemon proves the group gone and settles it as `failed` with a
    // failure receipt.
    cause.context(format!(
        "the cleanup could not confirm turn {turn_id}'s process group empty; the turn \
         stays under the failure-cleanup marker until the daemon proves it gone and \
         settles it as failed"
    ))
}

/// Settle a turn whose spawn went wrong, and fold how THAT went into the
/// reported error — a dropped settle error would leave the row `running`
/// with nobody told. If settling itself keeps failing, the row stays
/// `running` under the failure-cleanup marker the exhaustion tail writes:
/// the recovery loop proves the (removed) object empty and settles it as
/// `failed` within a cycle — the 10-second no-pid claim deliberately
/// excludes supervised and cgroup-bound rows.
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
        // Settling sweeps the turn's dialogs in the SAME transaction: a
        // `failed` turn is outside every later daemon scan, so a button an
        // early tool hook managed to publish would otherwise stay
        // answerable for up to a day.
        match crate::state::settle_unwound_turn_failed(conn, turn_id, now) {
            Ok(()) => return cause,
            Err(err) => last_error = Some(err),
        }
    }
    let settle_error = last_error.unwrap_or_else(|| anyhow::anyhow!("settle never ran"));
    // Best-effort supervision marker: a cgroup-owned row is EXCLUDED from
    // the 10-second no-pid claim on purpose, so without this it would sit
    // `running` until the six-hour fiat. Marked, the recovery loop probes
    // its (already removed) object next tick — `populated` reads empty —
    // and settles it in seconds. If even this write fails, nothing durable
    // is writable at all, and the same brokenness stops every claim too.
    let marked = crate::state::set_failure_cleanup_marker(conn, turn_id).is_ok();
    cause.context(format!(
        "additionally, settling turn {turn_id} as failed also failed ({settle_error:#}); \
         the row is still 'running'{}",
        if marked {
            " under the cleanup marker — the recovery loop settles it within a cycle"
        } else {
            " and even the cleanup marker could not be written; the row stays visible \
             until writes recover"
        }
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

/// Kill a child this process just spawned (it is still in RUNNING), and
/// report what was PROVEN — at the group level, not the leader's. The
/// narrow cousin of `kill_turn_process`: no restart-identity check needed,
/// because the handle is looked up by the turn that spawned it. A reaped
/// leader alone is `Undetermined`: a TERM-proof or foreign-uid descendant
/// can outlive it, and the caller settles by this verdict.
fn kill_spawned_child(
    turn_id: &str,
    pid: Option<u32>,
    ticks: Option<&str>,
    cgroup: Option<&Path>,
) -> KillOutcome {
    #[cfg(test)]
    {
        let _ = (turn_id, ticks, cgroup);
        test_kill::record(pid.unwrap_or(0));
        test_kill::forced_outcome().unwrap_or(KillOutcome::Terminated)
    }
    #[cfg(not(test))]
    {
        let _ = pid;
        // cgroup regime: the object we just created is the authority — a
        // detached-hook descendant that changed its PGID is invisible to
        // killpg but not to `populated`.
        if let Some(dir) = cgroup {
            let entry = turn_children::take(turn_id);
            let killed = turn_cgroup::kill(dir);
            if let Some(mut entry) = entry {
                let started = std::time::Instant::now();
                let reaped = loop {
                    match entry.child.try_wait() {
                        Ok(Some(_)) | Err(_) => break true,
                        Ok(None) if started.elapsed() >= Duration::from_secs(2) => break false,
                        Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                    }
                };
                if !reaped {
                    turn_children::put_back(entry);
                }
            }
            if killed && turn_cgroup::confirmed_empty(dir, Duration::from_secs(2)) {
                return KillOutcome::Terminated;
            }
            return KillOutcome::Undetermined;
        }
        let Some(mut entry) = turn_children::take(turn_id) else {
            return KillOutcome::Unverified;
        };
        let child_pid = entry.child.id();
        if !terminate_process_group(child_pid, Some(&mut entry.child)) {
            // Not reaped within the bound: hand the entry back so the
            // per-cycle reaper collects it later instead of leaking a
            // forever-unreapable zombie.
            turn_children::put_back(entry);
            return KillOutcome::Undetermined;
        }
        // The group id is the spawn contract's pgid == pid; ticks come from
        // the identity capture when it produced them (macOS: the probe arm).
        confirm_reaped_leader(child_pid, ticks, Some(child_pid))
    }
}

#[cfg_attr(test, allow(clippy::needless_return))]
fn spawn_detached_headless(
    binary: &Path,
    args: &[String],
    cwd: Option<&str>,
    log_name: &str,
    cgroup: Option<&Path>,
) -> Result<Option<u32>> {
    #[cfg(test)]
    {
        test_spawn::RECORDED.lock().expect("test spawn lock").push((
            binary.display().to_string(),
            args.to_vec(),
            cwd.map(str::to_string),
        ));
        let _ = (log_name, cgroup);
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
        // cgroup regime: the child moves ITSELF into the turn's cgroup
        // between fork and exec. The fd is opened in the parent (pre_exec
        // must stay async-signal-safe: one raw write, no allocation), and
        // a failure to enter fails the spawn — the contract is "inside its
        // cgroup, or never exists at all".
        #[cfg(unix)]
        let _procs_keepalive = match cgroup {
            Some(dir) => {
                let procs = fs::OpenOptions::new()
                    .write(true)
                    .open(dir.join("cgroup.procs"))
                    .with_context(|| {
                        format!("failed to open cgroup.procs under {}", dir.display())
                    })?;
                {
                    use std::os::unix::io::AsRawFd;
                    use std::os::unix::process::CommandExt;
                    let fd = procs.as_raw_fd();
                    // SAFETY: the closure runs post-fork pre-exec and only
                    // performs a raw write on an inherited fd.
                    unsafe {
                        command.pre_exec(move || {
                            let buf = b"0\n";
                            if libc::write(fd, buf.as_ptr().cast(), buf.len()) < 0 {
                                return Err(std::io::Error::last_os_error());
                            }
                            Ok(())
                        });
                    }
                }
                Some(procs)
            }
            None => None,
        };
        #[cfg(not(unix))]
        let _ = cgroup;
        let child = command
            .spawn()
            .with_context(|| format!("failed to spawn {} for headless turn", binary.display()))?;
        let pid = child.id();
        turn_children::put_back(turn_children::RunningTurn {
            turn_id: log_name.to_string(),
            child,
        });
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
    /// Verified live socket, but WHICH of the two could not be read: no
    /// /proc entry for the owning pid, or a platform without /proc. A
    /// verified socket proves the session is alive and reachable; it proves
    /// nothing about a window, and this state exists so nothing downstream
    /// can quietly upgrade "unreadable" into "the user is looking at it".
    Unverified,
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
    // background pty host has no window. An unreadable /proc (or a platform
    // without one) is NOT a window: the identity check just proved the
    // socket was not swapped, which says nothing about what the user can
    // see, and publishing it as "🖥 终端活跃" put a terminal-fallback promise
    // on sessions nobody had looked at.
    let pid = Path::new(&socket.path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.parse::<u32>().ok());
    let Some(pid) = pid else {
        return Ok(TerminalPresence::Unverified);
    };
    Ok(match parent_is_bg_pty_host(pid) {
        Some(true) => TerminalPresence::Background,
        Some(false) => TerminalPresence::Window,
        None => TerminalPresence::Unverified,
    })
}

/// Signalled the instant the windowless probe starts, then stalls it. Lets a
/// test drop a transcript record into the exact window that separates a
/// boundary frozen before this probe from one frozen after it.
#[cfg(test)]
pub(crate) static WINDOWLESS_PROBE_SEAM: std::sync::Mutex<
    Option<std::sync::mpsc::Sender<std::sync::mpsc::Sender<()>>>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
fn windowless_probe_seam() {
    let sender = WINDOWLESS_PROBE_SEAM
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some(sender) = sender {
        // Signal, then WAIT for the test to confirm it has written — no
        // fixed sleep to be unlucky with under load.
        let (ack_tx, ack_rx) = std::sync::mpsc::channel();
        if sender.send(ack_tx).is_ok() {
            let _ = ack_rx.recv_timeout(Duration::from_secs(10));
        }
    }
}

/// The terminal device of the Claude Code session this hook belongs to, or
/// `None` when there is no usable one.
///
/// The route: `CLAUDE_CODE_MESSAGING_SOCKET` (inherited by every hook
/// process) is named after the claude process's pid, and that process holds
/// its pts on the standard fds. Verified 2026-08-22 under tmux: socket stem
/// -> pid -> `/proc/<pid>/fd/0..2` all resolved to the pane's `/dev/pts/N`.
/// The hook's own fds are useless here — Claude Code spawns hooks with
/// socket-backed stdio and no controlling terminal, so the session's tty is
/// only reachable through the claude process itself.
///
/// Where `/proc` does not exist (macOS) or gave nothing (all three fds
/// redirected), a constrained `ps -o tty=` on the same pid reports the
/// controlling terminal instead. `None` simply means "nothing to paint on",
/// never an error.
pub(crate) fn session_tty_path() -> Option<PathBuf> {
    // Tests NEVER fall through to the real probe: `cargo test` inherits the
    // developing session's own messaging socket, and a test without the seam
    // would resolve that session's pts and paint banners into the terminal
    // the developer is typing in. Unset seam = no tty, full stop.
    #[cfg(test)]
    return match env::var("TINYCTB_TEST_SESSION_TTY") {
        Ok(raw) if !raw.is_empty() => Some(PathBuf::from(raw)),
        _ => None,
    };
    #[cfg(not(test))]
    real_session_tty_path()
}

#[cfg(not(test))]
fn real_session_tty_path() -> Option<PathBuf> {
    let socket = env::var("CLAUDE_CODE_MESSAGING_SOCKET").ok()?;
    let pid = Path::new(&socket)
        .file_stem()?
        .to_str()?
        .parse::<u32>()
        .ok()?;
    proc_fd_tty(pid).or_else(|| ps_reported_tty(pid))
}

/// Linux fast path: fds 0..2 in order, first tty wins — no subprocess.
#[cfg(all(not(test), target_os = "linux"))]
fn proc_fd_tty(pid: u32) -> Option<PathBuf> {
    [0u32, 1, 2].iter().find_map(|fd| {
        let target = fs::read_link(format!("/proc/{pid}/fd/{fd}")).ok()?;
        target
            .to_str()
            .is_some_and(|path| path.starts_with("/dev/pts/") || path.starts_with("/dev/tty"))
            .then_some(target)
    })
}

#[cfg(all(not(test), not(target_os = "linux")))]
fn proc_fd_tty(_pid: u32) -> Option<PathBuf> {
    None
}

/// Portable fallback: the controlling terminal as `ps` reports it. macOS has
/// no `/proc`, so this is the ONLY route there; on Linux it covers a session
/// whose standard fds were all redirected but whose controlling tty exists.
/// The binary comes from a fixed trusted list — a PATH lookup could hand the
/// probe to any wrapper on the user's PATH — and the whole run is bounded by
/// `within_deadline` wiring `probe_child_line_blocking`: this is the hook's
/// hot path, and `Command::output()` with no timeout would freeze the gate
/// before its poll loop ever starts. The parsed path must also BE a
/// character device before anyone writes to it.
#[cfg(not(test))]
fn ps_reported_tty(pid: u32) -> Option<PathBuf> {
    // EVERY step lives inside the bounded worker — candidate `exists()`,
    // the spawn, the parse, and the char-device `metadata()`. The first
    // and last are filesystem touches, and a dead mount can park either
    // one forever: a bound that only covered the middle would leave the
    // gate frozen at the edges of the pipeline.
    within_deadline(PS_PROBE_BUDGET, move |deadline| {
        let binary = ["/bin/ps", "/usr/bin/ps"]
            .into_iter()
            .map(Path::new)
            .find(|path| path.exists())?;
        let args = ["-o", "tty=", "-p", &pid.to_string()].map(String::from);
        let line = probe_child_line_blocking(binary, &args, deadline)?;
        let path = parse_ps_tty(&line)?;
        is_char_device(&path).then_some(path)
    })
    .flatten()
}

/// The banner will WRITE to whatever this approves; a regular file (or a
/// fifo someone parked at a tty-looking path) must not pass.
#[cfg(not(test))]
fn is_char_device(path: &Path) -> bool {
    use std::os::unix::fs::FileTypeExt as _;
    fs::metadata(path).is_ok_and(|meta| meta.file_type().is_char_device())
}

/// Total budget for one `ps` probe: spawn, output, exit, all inclusive.
pub(crate) const PS_PROBE_BUDGET: Duration = Duration::from_millis(500);

/// Run `work` on a detached worker under a STRICT caller-visible deadline.
///
/// The point is what stays OFF the caller's thread: everything inside
/// `work` — filesystem probes included, since a dead mount can park
/// `exists()`, `metadata()` or `spawn()` forever with no error to catch.
/// The caller waits out the budget on a channel and walks away; a worker
/// that wakes late finds its deadline spent, does its own cleanup, and
/// sends into a dropped channel. Caller-visible time is the budget plus
/// channel scheduling — never a cleanup tax.
fn within_deadline<T: Send + 'static>(
    budget: Duration,
    work: impl FnOnce(std::time::Instant) -> T + Send + 'static,
) -> Option<T> {
    let deadline = std::time::Instant::now() + budget;
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(work(deadline));
    });
    rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        .ok()
}

/// Test-only composition of the two audited pieces — `within_deadline`
/// around `probe_child_line_blocking` — in exactly the shape
/// `ps_reported_tty` wires them (which inlines the pipeline so candidate
/// checks and device validation share the SAME deadline, and is itself
/// `cfg(not(test))` because tests must never probe the real session).
#[cfg(test)]
pub(crate) fn probe_child_line(binary: &Path, args: &[&str], budget: Duration) -> Option<String> {
    let binary = binary.to_path_buf();
    let args: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
    within_deadline(budget, move |deadline| {
        probe_child_line_blocking(&binary, &args, deadline)
    })
    .flatten()
}

/// Run a child expected to print one short line, with every step
/// deadline-checked — each has a real hang mode: the child may never exit
/// (budget → group kill + bounded reap, so no zombie is left behind), and
/// its stdout pipe may never reach EOF even after the child dies — a
/// wrapper that forked a grandchild leaves the write end open, which is why
/// the pipe is switched to `O_NONBLOCK` and read in the same deadline loop
/// instead of a blocking `read_to_string`. Output beyond 1 KiB is not "one
/// short line" and fails the probe outright.
fn probe_child_line_blocking(
    binary: &Path,
    args: &[String],
    deadline: std::time::Instant,
) -> Option<String> {
    // Test seam: a hung `spawn()` (binary on a dead filesystem) parks this
    // thread before any deadline check can run — unreachable from a real
    // test without a broken mount, so the stall is injected here. The
    // caller's recv_timeout, not this thread, is what stays bounded.
    #[cfg(test)]
    if let Ok(raw) = env::var("TINYCTB_TEST_PROBE_STALL_MS") {
        if let Ok(ms) = raw.parse::<u64>() {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }
    use std::io::Read as _;
    use std::os::unix::io::AsRawFd as _;
    use std::os::unix::process::CommandExt as _;
    let mut child = Command::new(binary)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        // Its own process group, so the timeout cleanup can kill the GROUP:
        // killing only the direct child orphans whatever it forked — and a
        // forked grandchild is exactly the thing that inherits our pipe and
        // holds EOF hostage. (Observed for real: the fake-ps test's `sh`
        // died while its `sleep 1000` lived on, three of them.)
        .process_group(0)
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    // A failed fcntl means the next read() may block FOREVER — that is not
    // "degraded", it is the exact hang this function exists to prevent, so
    // it aborts the probe. Both calls are checked.
    let nonblocking = unsafe {
        let fd = stdout.as_raw_fd();
        let flags = libc::fcntl(fd, libc::F_GETFL);
        flags != -1 && libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) != -1
    };
    if !nonblocking {
        abandon_probe(child);
        return None;
    }
    let mut collected = Vec::new();
    let mut reached_eof = false;
    loop {
        if std::time::Instant::now() >= deadline {
            abandon_probe(child);
            return None;
        }
        if !reached_eof {
            let mut buf = [0u8; 256];
            match stdout.read(&mut buf) {
                Ok(0) => reached_eof = true,
                Ok(count) => {
                    collected.extend_from_slice(&buf[..count]);
                    if collected.len() > 1024 {
                        abandon_probe(child);
                        return None;
                    }
                    continue;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => {
                    abandon_probe(child);
                    return None;
                }
            }
            // DELIBERATELY no try_wait() before EOF. Reaping an
            // already-exited leader here would free its PID — and with it
            // the process-GROUP anchor — while a descendant still holds
            // the pipe; a recycled pgid would aim the eventual group kill
            // at strangers. Left unreaped, the zombie pins the pgid until
            // `abandon_probe` sweeps the group or EOF makes reaping safe.
            continue;
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return None;
                }
                return String::from_utf8(collected).ok();
            }
            // EOF but still running: wait for the exit status the answer
            // deserves, or let the deadline end it.
            Ok(None) => std::thread::sleep(Duration::from_millis(5)),
            Err(_) => {
                abandon_probe(child);
                return None;
            }
        }
    }
}

/// End an overdue probe without ever waiting unbounded on the gate thread.
///
/// SIGKILL goes to the child's whole PROCESS GROUP (it is its own leader —
/// see the spawn), falling back to the pid if the group kill fails. The
/// reap is a short bounded `try_wait` loop: that covers every ordinary
/// death, and a child the kernel refuses to release (uninterruptible
/// sleep) is handed to a detached reaper thread instead — the thread can
/// block forever harmlessly, the GATE thread cannot. If the hook process
/// exits before the reaper collects, init inherits and reaps: the thread
/// is the belt, process exit is the braces. Either way no zombie survives
/// the hook.
fn abandon_probe(mut child: std::process::Child) {
    let pid = child.id() as i32;
    unsafe {
        if libc::kill(-pid, libc::SIGKILL) != 0 {
            let _ = child.kill();
        }
    }
    let grace = std::time::Instant::now() + Duration::from_millis(200);
    loop {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => return,
            Ok(None) => {
                if std::time::Instant::now() >= grace {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
    std::thread::spawn(move || {
        let _ = child.wait();
    });
}

/// `ps -o tty=` output -> a device path, or `None` for "no terminal".
/// Linux prints `pts/17`, macOS prints `ttys001`, a daemon prints `?`/`??`/
/// `-`.
///
/// Constrained on purpose — this string names a device the banner will
/// WRITE to, so parsing is a gate: one token, and the result is anchored
/// under `/dev` by CONSTRUCTION, component by component. (`PathBuf::join`
/// famously discards the base for an absolute right-hand side, so
/// `/tmp/victim` would have sailed straight through a join-based "anchor".)
/// An absolute token must itself start with `/dev/`; every remaining
/// component must be a plain name — no empties (`//`), no `.`, no `..`.
pub(crate) fn parse_ps_tty(raw: &str) -> Option<PathBuf> {
    let token = raw.trim();
    if token.is_empty() || token == "?" || token == "??" || token == "-" {
        return None;
    }
    if token.contains(char::is_whitespace) {
        return None;
    }
    let relative = match token.strip_prefix("/dev/") {
        Some(stripped) => stripped,
        None if token.starts_with('/') => return None,
        None => token,
    };
    let mut path = PathBuf::from("/dev");
    for component in relative.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return None;
        }
        path.push(component);
    }
    Some(path)
}

/// Does the session running THIS hook lack a terminal window? Hooks are
/// children of the session process and inherit its messaging socket, whose
/// file name is the session pid — so a gate can classify itself without any
/// database lookup or stale cache.
///
/// This matters because "no window" removes the fallback the approval gate
/// assumes exists. Handing a prompt back to a background session's terminal
/// puts the dialog somewhere nobody is looking: measured 2026-08-17, a
/// cchess background session sat blocked for 7h09m that way, and the only
/// person who could clear it had to walk to the machine.
///
/// The probe answers in THREE states, because it can genuinely fail: a
/// missing or malformed socket variable, an unreadable /proc entry, a
/// platform with no /proc at all. Those are `Unverified` — not "has a
/// window".
///
/// Callers split the two questions themselves. POLICY (how long to wait,
/// whether to paint a banner, whether a keyboard reclaim may take the
/// prompt) keys on `== Background` only, so an unreadable /proc keeps the
/// established behaviour and never invents a day-long wait. What the MESSAGE
/// says keys on all three, because telling the user their terminal is fine
/// must require having measured it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionWindow {
    /// A terminal window: an ordinary `claude` in a terminal emulator.
    Window,
    /// Alive under Claude Code's background pty host — no visible window.
    Background,
    /// The probe could not tell. Never claim either way from here.
    Unverified,
}

pub(crate) fn current_session_window() -> SessionWindow {
    // Test seam: this probe walks /proc and takes real time, which is
    // exactly why the transcript boundary must be frozen BEFORE it. A test
    // signals here and stalls, so an answer landing inside this window is
    // only visible if the boundary already predates it.
    #[cfg(test)]
    windowless_probe_seam();
    #[cfg(test)]
    if let Ok(raw) = env::var("TINYCTB_TEST_SESSION_WINDOWLESS") {
        return match raw.as_str() {
            "1" => SessionWindow::Background,
            "unverified" => SessionWindow::Unverified,
            _ => SessionWindow::Window,
        };
    }
    let Ok(socket) = env::var("CLAUDE_CODE_MESSAGING_SOCKET") else {
        return SessionWindow::Unverified;
    };
    let pid = Path::new(&socket)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.parse::<u32>().ok());
    let Some(pid) = pid else {
        return SessionWindow::Unverified;
    };
    match parent_is_bg_pty_host(pid) {
        Some(true) => SessionWindow::Background,
        Some(false) => SessionWindow::Window,
        None => SessionWindow::Unverified,
    }
}

/// `None` means the question could not be answered — /proc unreadable, or a
/// platform without it — as opposed to a confirmed "no, an ordinary parent".
fn parent_is_bg_pty_host(pid: u32) -> Option<bool> {
    let ppid = fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| {
            stat.rsplit_once(')')?
                .1
                .split_whitespace()
                .nth(1)?
                .parse::<u32>()
                .ok()
        })?;
    fs::read_to_string(format!("/proc/{ppid}/cmdline"))
        .ok()
        .map(|cmdline| cmdline.contains("bg-pty-host"))
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
            .and_then(|info| parse_transcript_summary(&info.path, now).ok())
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

/// What woke the daemon: a hook spool write (push-latency path — the next
/// cycle must run the full sync immediately) or ordinary transcript churn
/// in the projects dir (active sessions append constantly; forcing a full
/// sync per append made the loop spin at max speed — measured ~10% of a
/// core with one busy session).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchWake {
    Spool,
    Projects,
}

pub(crate) struct ClaudeWatchReceiver {
    rx: std::sync::mpsc::Receiver<WatchWake>,
    _watcher: notify::RecommendedWatcher,
}

impl ClaudeWatchReceiver {
    /// Waits out the full tick unless a SPOOL wake arrives — the signal that
    /// a hook just fired and the next cycle must sync immediately; returns
    /// true only then. Transcript churn (Projects wakes) is deliberately
    /// slept through: an active session streams dozens of transcript writes
    /// per second, and ending the wait on each one turned the poll interval
    /// into a fiction — the daemon ticked ~26×/s and burned ~10% of a core
    /// running fast lanes that transcript writes can never feed (measured).
    /// The periodic full sync picks transcript changes up on its own cadence.
    pub(crate) fn recv_timeout(&self, timeout: Duration) -> bool {
        wait_for_spool_wake(&self.rx, timeout)
    }
}

/// Bound on the post-wait drain. A watcher flooding the channel faster than
/// `try_recv` empties it could otherwise pin the loop here forever; anything
/// left behind is picked up by the next tick's wait (Projects wakes are
/// skipped there anyway, a leftover Spool wake ends it immediately).
const WATCH_DRAIN_LIMIT: usize = 4096;

fn wait_for_spool_wake(rx: &std::sync::mpsc::Receiver<WatchWake>, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    let mut spool_woken = false;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match rx.recv_timeout(remaining) {
            Ok(WatchWake::Spool) => {
                spool_woken = true;
                break;
            }
            Ok(WatchWake::Projects) => continue,
            Err(_) => break,
        }
    }
    // Coalesce everything already queued: a burst of hook events must
    // collapse into ONE spool-woken tick, not schedule a back-to-back full
    // sync per duplicate wake left sitting in the channel.
    for _ in 0..WATCH_DRAIN_LIMIT {
        match rx.try_recv() {
            Ok(WatchWake::Spool) => spool_woken = true,
            Ok(WatchWake::Projects) => {}
            Err(_) => break,
        }
    }
    spool_woken
}

pub(crate) fn start_claude_watch_receiver() -> Result<ClaudeWatchReceiver> {
    let spool = events_spool_dir()?;
    fs::create_dir_all(&spool).ok();
    let projects = claude_projects_dir()?;
    let (tx, rx) = std::sync::mpsc::channel();
    let spool_for_watcher = spool.clone();
    let mut watcher = notify::RecommendedWatcher::new(
        move |result: notify::Result<notify::Event>| {
            if let Ok(event) = result {
                let wake = if event
                    .paths
                    .iter()
                    .any(|path| path.starts_with(&spool_for_watcher))
                {
                    WatchWake::Spool
                } else {
                    WatchWake::Projects
                };
                let _ = tx.send(wake);
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

    /// The stamp parser, against the shape Claude Code actually writes and
    /// against everything it must refuse: a wrong time in the ordering is
    /// worse than the `None` that falls back to the file mtime.
    #[test]
    fn transcript_stamps_parse_strictly_and_only_in_utc() {
        assert_eq!(transcript_timestamp_ms("1970-01-01T00:00:00.000Z"), Some(0));
        assert_eq!(transcript_timestamp_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            transcript_timestamp_ms("1970-01-02T00:00:00Z"),
            Some(86_400_000)
        );
        // A leap day, and the day after it, walked by hand.
        assert_eq!(
            transcript_timestamp_ms("2024-03-01T00:00:00Z").unwrap()
                - transcript_timestamp_ms("2024-02-29T00:00:00Z").unwrap(),
            86_400_000
        );
        // The record that started this: 2026-08-12T13:32:34.162Z.
        let stamp = transcript_timestamp_ms("2026-08-12T13:32:34.162Z").expect("valid");
        assert_eq!(stamp % 1_000, 162, "millis are kept");
        let day = transcript_timestamp_ms("2026-08-12T00:00:00Z").expect("valid");
        assert_eq!(stamp - day, (13 * 3_600 + 32 * 60 + 34) * 1_000 + 162);
        // Ordering across a month boundary, which the civil-day arithmetic
        // is the only thing standing behind.
        assert!(
            transcript_timestamp_ms("2026-09-01T00:00:00Z")
                > transcript_timestamp_ms("2026-08-31T23:59:59Z")
        );
        // The leap day that DOES exist still parses, so the calendar check
        // rejects the impossible rather than everything unusual.
        assert!(transcript_timestamp_ms("2024-02-29T12:00:00Z").is_some());
        assert!(transcript_timestamp_ms("2000-02-29T12:00:00Z").is_some());
        assert!(transcript_timestamp_ms("1900-02-29T12:00:00Z").is_none());
        for refused in [
            "",
            "2026-08-12",
            // Dates that do not exist, which a 1..=31 range check waved
            // through and which would then order as real times.
            "2026-02-31T00:00:00Z",
            "2026-02-29T00:00:00Z",
            "2026-04-31T00:00:00Z",
            "2026-00-10T00:00:00Z",
            "2026-08-00T00:00:00Z",
            // Not fixed width, and signs inside components.
            "2026-8-2T1:2:3Z",
            "2026-08-12T-1:00:00Z",
            "2026-08-12T00:-1:00Z",
            "202-08-12T00:00:00Z",
            // A leap second lands ahead of the next day's first record.
            "2026-08-12T23:59:60Z",
            // Extreme years must return None, never wrap or panic.
            "999999-08-12T00:00:00Z",
            "0000-01-01T00:00:00Z",
            "2026-08-12T13:32:34",       // no zone: not ours to guess
            "2026-08-12T13:32:34+08:00", // an offset we do not convert
            "2026-08-12T13:32:34.1Z",    // not the three-digit shape
            "2026-08-12T13:32:34.162162Z",
            "2026-13-12T00:00:00Z", // month out of range
            "2026-08-32T00:00:00Z",
            "2026-08-12T24:00:00Z",
            "2026-08-12T13:32:34.abcZ",
            "yesterday",
        ] {
            assert_eq!(
                transcript_timestamp_ms(refused),
                None,
                "must refuse {refused:?} rather than guess"
            );
        }
    }

    /// A transient READ failure is not a malformed entry. Merging the two
    /// meant one permissions blip, one EMFILE, one mount going away, and the
    /// only copy of a real hook was deleted.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_spool_entry_is_kept_while_a_malformed_one_is_dropped() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-unread-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("events")).expect("spool dir");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);

        // Malformed: it can never succeed, so it goes.
        let malformed = root.join("events").join("1000-1-Stop.json");
        fs::write(&malformed, "{ not json").expect("write");
        let (_, _, consumed, held) = ingest_spool_events(2_000).expect("ingest");
        assert_eq!(consumed, 1);
        assert!(held.is_empty(), "a malformed entry is not held");
        assert!(!malformed.exists(), "and does not wedge the loop");

        // Unreadable: a real file with no read permission — `read_to_string`
        // fails with an I/O error rather than a parse error.
        let unreadable = root.join("events").join("2000-1-Stop.json");
        fs::write(
            &unreadable,
            json!({"hookEventName": "Stop", "sessionId": "sess-unread", "receivedAt": 2000})
                .to_string(),
        )
        .expect("write");
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000)).expect("chmod");
        }
        let (_, _, consumed, held) = ingest_spool_events(3_000).expect("ingest");
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o600));
        }
        assert_eq!(consumed, 0, "an unreadable entry is not consumed");
        assert!(held.is_empty());
        assert!(
            unreadable.exists(),
            "and is left for the next cycle rather than deleted"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// The same file, rewritten to the same LENGTH. That is what an
    /// append-and-truncate or a restore looks like, and against a key of
    /// (milliseconds, size) it matched exactly — the stale summary was then
    /// served forever, so the row could never be corrected again however the
    /// file changed. The earlier tests dodged this shape by using a
    /// different file per case; this one meets it head on.
    #[test]
    fn a_same_length_rewrite_is_not_served_from_the_cache() {
        let dir = std::env::temp_dir().join(format!("tinyctb-rewrite-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("rewritten.jsonl");
        let line = |stamp: &str| {
            format!(
                "{{\"type\":\"user\",\"timestamp\":\"{stamp}\",\"message\":{{\"role\":\"user\",\"content\":\"hi\"}}}}\n"
            )
        };
        let first = "2026-08-12T13:32:34.162Z";
        let second = "2026-08-11T09:00:00.000Z";
        assert_eq!(
            line(first).len(),
            line(second).len(),
            "the fixture is only meaningful if both writes are the same length"
        );
        let now = transcript_timestamp_ms("2026-08-20T00:00:00.000Z").expect("clock");

        // The two writes are pinned to timestamps HALF A MILLISECOND apart.
        // Waiting for the machine to be fast enough would make this a race;
        // stating the interval makes it the property under test — a key with
        // millisecond granularity cannot tell these apart, and one with
        // nanoseconds can.
        let base = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let stamp = |at: std::time::SystemTime| {
            let file = fs::File::options().write(true).open(&path).expect("open");
            file.set_modified(at).expect("set mtime");
        };

        fs::write(&path, line(first)).expect("write");
        stamp(base);
        assert_eq!(
            parse_transcript_summary(&path, now)
                .expect("parse")
                .last_record_at,
            transcript_timestamp_ms(first)
        );

        // Rewritten in place, same length, within the same millisecond.
        fs::write(&path, line(second)).expect("rewrite");
        stamp(base + Duration::from_micros(500));
        assert_eq!(
            parse_transcript_summary(&path, now)
                .expect("parse")
                .last_record_at,
            transcript_timestamp_ms(second),
            "a rewritten file is a different file to read, whatever its length"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The summary cache is keyed on the FILE, but the answer depends on the
    /// clock: a record refused for being ahead of it is ordinary once the
    /// clock arrives, and one accepted before a rollback is in the future
    /// afterwards. Neither may be served from a cache that cannot see it.
    #[test]
    fn the_summary_cache_does_not_outlive_the_clock_it_was_built_against() {
        let dir = std::env::temp_dir().join(format!("tinyctb-clockcache-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let at = transcript_timestamp_ms("2026-08-12T13:32:34.162Z").expect("stamp");
        let path = dir.join("later.jsonl");
        fs::write(
            &path,
            "{\"type\":\"user\",\"timestamp\":\"2026-08-12T13:32:34.162Z\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n",
        )
        .expect("transcript");

        // The clock is behind the record: refused, and remembered as refused.
        let early = parse_transcript_summary(&path, at - 1).expect("parse");
        assert_eq!(early.last_record_at, None);
        assert_eq!(early.earliest_refused_future_at, Some(at));

        // The clock catches up. The FILE has not changed, so the cache would
        // hand back the refusal — it must re-read instead.
        let later = parse_transcript_summary(&path, at + 1).expect("parse");
        assert_eq!(
            later.last_record_at,
            Some(at),
            "a record refused for being ahead of the clock counts once the clock arrives"
        );

        // And back the other way: after a rollback the accepted record is in
        // the future, and must not be served from the cache either.
        let rolled_back = parse_transcript_summary(&path, at - 1).expect("parse");
        assert_eq!(
            rolled_back.last_record_at, None,
            "a stamp this clock cannot believe is not reported as fact"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// One record from 2099 must not hide every legitimate one behind it.
    /// Taking the maximum first and clamping afterwards read as "now" on
    /// every scan, for good — the same permanent-top-of-the-list failure,
    /// arriving by a different door.
    #[test]
    fn a_single_future_record_does_not_hide_the_real_ones() {
        let dir = std::env::temp_dir().join(format!("tinyctb-future-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let spoke_at = transcript_timestamp_ms("2026-08-12T13:32:34.162Z").expect("stamp");
        let now = spoke_at + 60_000;
        let path = dir.join("mixed.jsonl");
        fs::write(
            &path,
            "{\"type\":\"user\",\"timestamp\":\"2026-08-12T13:32:34.162Z\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n\
             {\"type\":\"user\",\"timestamp\":\"2099-01-01T00:00:00.000Z\",\"message\":{\"role\":\"user\",\"content\":\"from the future\"}}\n",
        )
        .expect("transcript");

        let summary = parse_transcript_summary(&path, now).expect("parse");
        assert_eq!(
            summary.last_record_at,
            Some(spoke_at),
            "an unbelievable stamp is refused per record, so the real one still counts"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The tie policy, through the door it actually comes in. Testing it on
    /// `reconcile_thread_snapshots` alone missed that the batch is collapsed
    /// per session FIRST: a Notification and a Stop stamped the same
    /// millisecond came out as the Stop alone, so if the real order was
    /// Stop-then-question and the pid ordering read the other way, the
    /// question was never announced at all.
    #[test]
    fn a_question_and_an_answer_in_the_same_millisecond_both_get_out() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-tiebatch-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("events")).expect("spool");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);
        let projects = root.join("projects");
        fs::create_dir_all(&projects).expect("projects");
        let _projects_seam =
            crate::state::EnvVarGuard::set("TINYCTB_CLAUDE_PROJECTS_DIR", &projects);
        let conn = create_state_db_in_memory().expect("db");
        crate::state::set_setting_text(&conn, "away", "true").expect("away");

        // Same millisecond, and the pid field decides the order they are
        // read in — here putting the question first, which is exactly the
        // arrangement that used to lose it.
        fs::write(
            root.join("events")
                .join("000000001000000-111-Notification.json"),
            json!({
                "hookEventName": "Notification",
                "sessionId": "sess-tie",
                "receivedAt": 1_000u64,
                "payload": {"message": "Claude needs your permission to run ls"}
            })
            .to_string(),
        )
        .expect("notification");
        fs::write(
            root.join("events").join("000000001000000-222-Stop.json"),
            json!({
                "hookEventName": "Stop",
                "sessionId": "sess-tie",
                "receivedAt": 1_000u64,
                "payload": {"last_assistant_message": "做完了"}
            })
            .to_string(),
        )
        .expect("stop");

        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        let result = sync_state_from_sessions(&conn, &config, 1_100, 10, false).expect("sync");
        let kinds = result
            .get("events")
            .and_then(Value::as_array)
            .expect("events")
            .iter()
            .filter_map(|event| event.get("type").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert!(
            kinds.contains(&"thread_waiting"),
            "the question must reach the phone even when an answer shares its millisecond, \
             got {kinds:?}"
        );
        assert!(
            kinds.contains(&"thread_completed"),
            "and the answer must still go out too, got {kinds:?}"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// Setting an entry aside is the thing that unblocks the queue, so a
    /// failure to do it is a failure of the cycle. It was counted before the
    /// move and the error only printed: the poison stayed exactly where it
    /// was, starving everything behind it, while the sync reported the
    /// entries set aside and returned success.
    #[test]
    fn a_failed_set_aside_is_not_reported_as_success() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-noaside-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("events")).expect("spool");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);
        // An ordinary file sitting exactly where the dead letters go.
        fs::write(root.join("events").join("unplaceable"), "in the way").expect("blocker");
        let entry = root.join("events").join("no-stamp-Notification.json");
        fs::write(
            &entry,
            json!({
                "hookEventName": "Notification",
                "sessionId": "sess-stuck",
                "payload": {"message": "Claude needs your permission to run ls"}
            })
            .to_string(),
        )
        .expect("entry");

        assert!(
            ingest_spool_events(1_000).is_err(),
            "a queue that could not be cleared must not report a clean cycle"
        );
        assert!(
            entry.exists(),
            "and the entry is still there, which is precisely why it must be reported"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// The queue reads the oldest bounded number of entries each cycle. An
    /// entry that is never placed is never released, so enough of them hold
    /// that window permanently and every real hook behind them waits forever
    /// — while the cycle reports the same files consumed, over and over.
    #[test]
    fn unplaceable_entries_cannot_starve_the_hooks_behind_them() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-starve-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("events")).expect("spool");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);

        // A full window of entries that can be placed by neither body nor
        // name, and one real hook queued behind all of them.
        for index in 0..MAX_SPOOL_EVENTS_PER_CYCLE {
            fs::write(
                root.join("events")
                    .join(format!("not-a-stamp-{index:04}-Notification.json")),
                json!({
                    "hookEventName": "Notification",
                    "sessionId": "sess-noise",
                    "payload": {"message": "Claude needs your permission to run ls"}
                })
                .to_string(),
            )
            .expect("noise");
        }
        fs::write(
            root.join("events").join("zzzz-real-Stop.json"),
            json!({
                "hookEventName": "Stop",
                "sessionId": "sess-real",
                "receivedAt": 900u64,
                "payload": {"last_assistant_message": "做完了"}
            })
            .to_string(),
        )
        .expect("real hook");

        let (snapshots, _, consumed, _) = ingest_spool_events(1_000).expect("first cycle");
        assert_eq!(
            consumed, 0,
            "nothing was taken from any of them, so nothing may be reported as consumed"
        );
        assert!(
            snapshots.is_empty(),
            "the real hook is behind the window on this cycle"
        );

        // They are out of the queue, not destroyed.
        assert_eq!(
            fs::read_dir(spool_dead_letter_dir().expect("dir"))
                .expect("read dead letters")
                .count(),
            MAX_SPOOL_EVENTS_PER_CYCLE,
            "every one of them is still on disk, just not in the way"
        );

        // And the next cycle reaches what was behind them.
        let (snapshots, _, consumed, held) = ingest_spool_events(1_100).expect("second cycle");
        assert_eq!(consumed, 1);
        assert_eq!(
            snapshots
                .iter()
                .map(|snapshot| snapshot.thread_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sess-real"],
            "a real hook must not wait behind entries nobody can order"
        );
        assert_eq!(held.len(), 1);
        let _ = fs::remove_dir_all(&root);
    }

    /// An entry that follows no protocol still gets asked what time it is,
    /// and `unwrap_or(now)` answered for it — with the NEWEST time there is.
    /// That handed the weakest evidence in the spool the strongest authority
    /// in the system: permission to rewrite status, raise a prompt, and move
    /// the reply route.
    #[test]
    fn an_entry_that_cannot_be_placed_in_time_gets_no_authority() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-unplaced-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("events")).expect("spool");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);
        let conn = create_state_db_in_memory().expect("db");

        for (name, envelope) in [
            (
                "no-stamp-a-Notification.json",
                json!({
                    "hookEventName": "Notification",
                    "sessionId": "sess-noproto",
                    "messagingSocket": "/run/user/1000/wrong.sock",
                    "payload": {"message": "Claude needs your permission to run ls"}
                }),
            ),
            (
                "no-stamp-b-Notification.json",
                json!({
                    "hookEventName": "Notification",
                    "sessionId": "sess-noproto",
                    "receivedAt": "600",
                    "payload": {"message": "Claude needs your permission to run ls"}
                }),
            ),
            (
                "no-stamp-c-Notification.json",
                json!({
                    "hookEventName": "Notification",
                    "sessionId": "sess-noproto",
                    "receivedAt": Value::Null,
                    "payload": {"message": "Claude needs your permission to run ls"}
                }),
            ),
        ] {
            fs::write(root.join("events").join(name), envelope.to_string()).expect("entry");
        }

        // Fail CLOSED: one of these may be the entry that moved the socket,
        // so the socket picture cannot be reported as complete.
        assert!(
            peek_session_sockets(&conn, 1_000).is_err(),
            "an unplaceable entry makes the socket answer unsafe, not empty"
        );

        let (snapshots, sockets, consumed, held) = ingest_spool_events(1_000).expect("ingest");
        assert!(
            snapshots.is_empty(),
            "nothing that cannot be placed in time may speak about a session"
        );
        assert!(
            sockets.is_empty(),
            "and none of them may move a reply route"
        );
        assert!(
            held.is_empty(),
            "nor be released, which would delete a real hook nobody could read again"
        );
        assert_eq!(
            consumed, 0,
            "and nothing was taken from them, so nothing may be reported as consumed"
        );
        // Out of the queue, still on disk.
        for name in [
            "no-stamp-a-Notification.json",
            "no-stamp-b-Notification.json",
            "no-stamp-c-Notification.json",
        ] {
            assert!(
                !root.join("events").join(name).exists(),
                "{name} must not keep its place in the queue"
            );
            assert!(
                spool_dead_letter_dir().expect("dir").join(name).exists(),
                "{name} must still exist to be looked at"
            );
        }
        let _ = fs::remove_dir_all(&root);
    }

    /// The name carries the same instant, written by the same process, in a
    /// form nothing since could edit. When the body's stamp is impossible and
    /// the name's is not, the name is simply the better copy.
    #[test]
    fn a_future_stamp_falls_back_to_the_name_before_giving_up() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-byname-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("events")).expect("spool");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);
        fs::write(
            root.join("events").join("000000000000900-1-Stop.json"),
            json!({
                "hookEventName": "Stop",
                "sessionId": "sess-byname",
                "receivedAt": 9_000_000u64,
                "payload": {"last_assistant_message": "做完了"}
            })
            .to_string(),
        )
        .expect("entry");

        // And the same second chance for a body that says nothing at all:
        // missing is no more placeable than impossible, and the name is no
        // less of an answer in one case than the other.
        fs::write(
            root.join("events").join("000000000000800-1-Stop.json"),
            json!({
                "hookEventName": "Stop",
                "sessionId": "sess-noname",
                "payload": {"last_assistant_message": "也做完了"}
            })
            .to_string(),
        )
        .expect("entry with no stamp at all");

        let (snapshots, _, _, _) = ingest_spool_events(1_000).expect("ingest");
        let placed = snapshots
            .iter()
            .map(|snapshot| (snapshot.thread_id.as_str(), snapshot.updated_at))
            .collect::<Vec<_>>();
        assert!(
            placed.contains(&("sess-byname", Some(900))),
            "the name's stamp is believable and the body's is not: {placed:?}"
        );
        assert!(
            placed.contains(&("sess-noname", Some(800))),
            "a body that says nothing gets the same second chance: {placed:?}"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// ONE hook with a stamp from the future, and nothing after it. The
    /// repair that waits for a second event never runs, so the session would
    /// sit pinned at a time the clock has to reach — for a badly wrong stamp,
    /// never. The stamp is corrected at the door instead.
    #[test]
    fn a_lone_hook_from_the_future_leaves_no_future_watermark() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-futurehook-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("events")).expect("spool");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);
        let projects = root.join("projects");
        fs::create_dir_all(&projects).expect("projects");
        let _projects_seam =
            crate::state::EnvVarGuard::set("TINYCTB_CLAUDE_PROJECTS_DIR", &projects);
        let conn = create_state_db_in_memory().expect("db");

        fs::write(
            root.join("events").join("000000009000000-1-Stop.json"),
            json!({
                "hookEventName": "Stop",
                "sessionId": "sess-future",
                "receivedAt": 9_000_000u64,
                "messagingSocket": "/run/user/1000/future.sock",
                "payload": {"last_assistant_message": "来自未来"}
            })
            .to_string(),
        )
        .expect("hook");

        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        sync_state_from_sessions(&conn, &config, 1_000, 10, false).expect("sync");

        let stamps: Option<(Option<i64>, Option<i64>)> = conn
            .query_row(
                "SELECT last_observed_at, socket_observed_at FROM threads_cache
                 WHERE thread_id = 'sess-future'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })
            .expect("query");
        // The entry cannot be placed in time at all, so it writes nothing —
        // no state, no route, and above all no stamp from a moment that has
        // not happened, which would have frozen this session for as long as
        // the clock took to reach it.
        if let Some((observed, socket_seen)) = stamps {
            assert!(
                observed.is_none_or(|at| at <= 1_000),
                "a hook cannot pin a session at a moment that has not happened, got {observed:?}"
            );
            assert!(
                socket_seen.is_none_or(|at| at <= 1_000),
                "nor pin its reply routing there, got {socket_seen:?}"
            );
        }
        // Kept, not destroyed — and out of the queue, so it cannot hold its
        // place in the window forever.
        assert!(
            !root
                .join("events")
                .join("000000009000000-1-Stop.json")
                .exists(),
            "an entry nobody can place must not keep its place in the queue"
        );
        assert!(
            spool_future_dir()
                .expect("dir")
                .join("000000009000000-1-Stop.json")
                .exists(),
            "it is only EARLY, so it waits for the clock rather than being written off"
        );

        // And when the clock reaches it, it is an ordinary hook again.
        let (snapshots, _, consumed, _) =
            ingest_spool_events(9_000_001).expect("cycle with a caught-up clock");
        assert_eq!(consumed, 1);
        assert_eq!(
            snapshots
                .iter()
                .map(|snapshot| snapshot.thread_id.as_str())
                .collect::<Vec<_>>(),
            vec!["sess-future"],
            "a clock correction must not cost a real hook"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// `read_dir` succeeding says only that the directory OPENED. Each step
    /// of the walk can still fail on its own, and dropping those silently
    /// hands back a shorter list that no caller can tell from a complete
    /// one — the same fail-open as before, one level in.
    #[test]
    fn a_directory_entry_that_fails_mid_walk_is_not_silently_dropped() {
        let ok = |name: &str| -> std::io::Result<fs::DirEntry> {
            let dir = std::env::temp_dir().join(format!("tinyctb-walk-{}", std::process::id()));
            fs::create_dir_all(&dir).expect("dir");
            fs::write(dir.join(name), "{}").expect("file");
            fs::read_dir(&dir)
                .expect("read_dir")
                .filter_map(Result::ok)
                .find(|entry| entry.file_name() == std::ffi::OsStr::new(name))
                .map(Ok)
                .expect("entry")
        };

        let complete = spool_entry_paths(vec![ok("1000-1-Stop.json")]).expect("all readable");
        assert_eq!(complete.len(), 1);

        let partial = spool_entry_paths(vec![
            ok("1000-1-Stop.json"),
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        ]);
        assert!(
            partial.is_err(),
            "a walk that could not finish must not be handed back as a finished one"
        );
        let dir = std::env::temp_dir().join(format!("tinyctb-walk-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
    }

    /// "I could not look" is not "there is nothing there". The socket peek
    /// answers a question a Telegram reply is about to act on, and acting on
    /// a wrong "no socket" means a headless `--resume` that forks the
    /// session — which nothing later can undo. So a read that FAILED must
    /// surface as a failure. Malformed JSON is the other thing entirely: it
    /// is a settled fact about one file, it will never parse, and treating
    /// it as an outage would wedge replies forever.
    #[test]
    fn the_socket_peek_reports_ignorance_but_not_bad_json_as_failure() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-peekfail-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let conn = create_state_db_in_memory().expect("db");

        // 1. The spool cannot be listed at all — here because something else
        //    is sitting where the directory belongs.
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);
        fs::write(root.join("events"), "not a directory").expect("blocker");
        let listed = peek_session_sockets(&conn, 1_000);
        assert!(
            listed.is_err(),
            "an unlistable spool must not be reported as an empty one"
        );
        fs::remove_file(root.join("events")).expect("unblock");

        // 2. An entry that cannot be READ. Same rule: it may well read next
        //    cycle, and until then the socket picture is unknown.
        fs::create_dir_all(root.join("events")).expect("spool dir");
        let unreadable = root.join("events").join("1000-1-Notification.json");
        fs::write(&unreadable, "{}").expect("entry");
        let mut perms = fs::metadata(&unreadable).expect("meta").permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o000);
        fs::set_permissions(&unreadable, perms).expect("chmod");
        let read_failed = peek_session_sockets(&conn, 1_000);
        let unreadable_is_an_error = read_failed.is_err();
        let mut perms = fs::metadata(&unreadable).expect("meta").permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o600);
        fs::set_permissions(&unreadable, perms).expect("restore");
        assert!(
            unreadable_is_an_error,
            "an unreadable entry leaves the socket picture unknown, and must say so"
        );

        // 3. Malformed JSON: readable, hopeless, and NOT an outage.
        fs::write(&unreadable, "{ this is not json").expect("garbage");
        assert_eq!(
            peek_session_sockets(&conn, 1_000).expect("bad json is not a failure"),
            0,
            "a file that will never parse must not stall replies forever"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// The mapping is only as current as the observation behind it. Hooks
    /// arrive out of order, so a kept-over entry can name a socket the
    /// session has already moved off — and routing a reply there is the
    /// same fork this feature exists to prevent.
    #[test]
    fn a_late_hook_cannot_move_the_socket_back() {
        let conn = create_state_db_in_memory().expect("db");
        let socket = |name: &str| SessionSocket {
            path: format!("/run/user/1000/{name}.sock"),
            inode: None,
            boot_id: None,
        };
        let stored = |conn: &Connection| -> Option<String> {
            conn.query_row(
                "SELECT messaging_socket FROM threads_cache WHERE thread_id = 'sess-move'",
                [],
                |row| row.get(0),
            )
            .expect("row")
        };

        crate::state::record_session_messaging_socket(
            &conn,
            "sess-move",
            &socket("new"),
            2_000,
            2_000,
        )
        .expect("current");
        assert_eq!(stored(&conn).as_deref(), Some("/run/user/1000/new.sock"));

        crate::state::record_session_messaging_socket(
            &conn,
            "sess-move",
            &socket("old"),
            1_000,
            2_100,
        )
        .expect("late");
        assert_eq!(
            stored(&conn).as_deref(),
            Some("/run/user/1000/new.sock"),
            "an overtaken hook may not point replies at where the session used to listen"
        );

        crate::state::record_session_messaging_socket(
            &conn,
            "sess-move",
            &socket("newer"),
            3_000,
            3_000,
        )
        .expect("newer");
        assert_eq!(
            stored(&conn).as_deref(),
            Some("/run/user/1000/newer.sock"),
            "and a genuinely newer sighting still moves it"
        );
    }

    /// The spool entry is the ONE copy of a hook until everything it causes
    /// is durable. Deleting it at parse time made the delivery marker's
    /// rollback pointless: the marker went back and the input it would have
    /// been re-derived from was already gone.
    #[test]
    fn a_spool_entry_outlives_the_parse_and_dies_only_after_the_enqueue() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-hold-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("events")).expect("spool dir");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);
        let entry = root.join("events").join("1000-1-Stop.json");
        fs::write(
            &entry,
            json!({"hookEventName": "Stop", "sessionId": "sess-hold", "receivedAt": 1000})
                .to_string(),
        )
        .expect("spool entry");

        let (snapshots, _, consumed, held) = ingest_spool_events(2_000).expect("ingest");
        assert_eq!(consumed, 1);
        assert_eq!(snapshots.len(), 1, "the hook is read");
        assert!(
            entry.exists(),
            "and its file is still there — nothing it causes is durable yet"
        );
        assert_eq!(held, vec![entry.clone()], "the cycle is holding it");

        // A malformed entry is the exception: it can never succeed, and
        // re-reading it every cycle would wedge the loop.
        let malformed = root.join("events").join("2000-1-Stop.json");
        fs::write(&malformed, "{ not json").expect("write");
        let (_, _, consumed, held) = ingest_spool_events(3_000).expect("ingest");
        assert_eq!(consumed, 2);
        assert!(!malformed.exists(), "malformed goes at once");
        assert_eq!(held, vec![entry], "and only the readable one is held");
        let _ = fs::remove_dir_all(&root);
    }

    /// A reading is only worth filing if it belongs to a KNOWN state of the
    /// file. Taking the identity separately, after the parse, let a reader
    /// stamp one file's contents with the identity of the file that replaced
    /// it — an old answer recorded under a new generation, where nothing
    /// downstream could tell.
    ///
    /// The window is entered on purpose, through a seam, rather than raced
    /// with a sleep: "parsing 120k lines takes longer than 5ms" is a bet on
    /// the machine, not a synchronisation.
    #[test]
    fn a_reading_that_the_file_moved_under_is_not_filed() {
        let dir = std::env::temp_dir().join(format!("tinyctb-stable-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("stable.jsonl");
        let line = |stamp: &str| {
            format!(
                "{{\"type\":\"user\",\"timestamp\":\"{stamp}\",\"message\":{{\"role\":\"user\",\"content\":\"hi\"}}}}\n"
            )
        };
        fs::write(&path, line("2026-08-12T13:32:34.162Z")).expect("transcript");
        let now = u64::MAX;

        // Settled: the reading knows exactly which file it came from.
        let (summary, fingerprint) = read_transcript_summary(&path, now).expect("read");
        assert_eq!(
            summary.last_record_at,
            transcript_timestamp_ms("2026-08-12T13:32:34.162Z")
        );
        assert_eq!(
            fingerprint,
            file_fingerprint(&path),
            "and it is the identity of the file as it stands"
        );

        // Moved under the read, precisely in the window: no identity, so the
        // caller has nothing to file it under.
        let churning = dir.join("churning.jsonl");
        fs::write(&churning, line("2026-08-12T13:32:34.162Z")).expect("transcript");
        let writer_path = churning.clone();
        let _armed = transcript_read_seam::arm(move || {
            fs::write(&writer_path, line("2026-08-13T00:00:00.000Z")).expect("rewrite");
        });
        let (_, fingerprint) = read_transcript_summary(&churning, now).expect("read");
        assert_eq!(
            fingerprint, None,
            "a file that moved under the read has no state this answer belongs to"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// End to end: a session whose transcript moves under the read must not
    /// have ANYTHING written for it — not a row, not a time, not a prompt.
    /// The seam enters the window exactly, so this is a statement about the
    /// code rather than about how fast the machine is.
    #[test]
    fn a_session_whose_file_moved_under_the_read_is_not_written_at_all() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-nowrite-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let projects = root.join("projects");
        fs::create_dir_all(projects.join("-home-user-x")).expect("projects dir");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);
        let _projects_seam =
            crate::state::EnvVarGuard::set("TINYCTB_CLAUDE_PROJECTS_DIR", &projects);
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };

        let transcript = projects.join("-home-user-x").join("sess-moving.jsonl");
        let line = |stamp: &str| {
            format!(
                "{{\"type\":\"user\",\"timestamp\":\"{stamp}\",\"message\":{{\"role\":\"user\",\"content\":\"hi\"}}}}\n"
            )
        };
        fs::write(&transcript, line("2026-08-12T13:32:34.162Z")).expect("transcript");

        // Rewritten inside the window, every time it is read.
        let churn = transcript.clone();
        let _armed = transcript_read_seam::arm(move || {
            let _ = fs::write(&churn, line("2026-08-13T00:00:00.000Z"));
        });
        let result = sync_state_from_sessions(&conn, &config, 9_000, 10, false).expect("sync");

        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM threads_cache WHERE thread_id = 'sess-moving'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(
            rows, 0,
            "a reading of a file that would not hold still is filed nowhere"
        );
        // And the ANSWER must say so too. Writing nothing while reporting a
        // sync handed the caller the very snapshot the database had just
        // refused, and counted it -- an empty table behind a receipt.
        assert_eq!(
            result.get("synced").and_then(Value::as_u64),
            Some(0),
            "a refused write is not a synced thread"
        );
        assert_eq!(
            result
                .get("threads")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(0),
            "and the refused snapshot is not handed back"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// A reading that held still, and a file that moved before the write
    /// could take the lock. Nothing is written -- and the ANSWER must agree:
    /// returning `Ok(())` from the refusal left the caller unable to tell a
    /// write from a refusal, so it went on to hand back the snapshot the
    /// database had just thrown away and to count it as synced.
    #[test]
    fn a_write_refused_at_the_last_moment_is_not_counted_as_synced() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-lastmoment-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let projects = root.join("projects");
        fs::create_dir_all(projects.join("-home-user-x")).expect("projects dir");
        let _state = crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root);
        let _projects_seam =
            crate::state::EnvVarGuard::set("TINYCTB_CLAUDE_PROJECTS_DIR", &projects);
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };

        let transcript = projects.join("-home-user-x").join("sess-late.jsonl");
        let line = |stamp: &str| {
            format!(
                "{{\"type\":\"user\",\"timestamp\":\"{stamp}\",\"message\":{{\"role\":\"user\",\"content\":\"hi\"}}}}\n"
            )
        };
        fs::write(&transcript, line("2026-08-12T13:32:34.162Z")).expect("transcript");

        // The seam fires twice per session: once inside the read, once inside
        // the freshness check. Let the READ hold still, then move the file in
        // the gap the check exists to close.
        let churn = transcript.clone();
        let signals = std::cell::Cell::new(0u32);
        let _armed = transcript_read_seam::arm(move || {
            signals.set(signals.get() + 1);
            if signals.get() == 2 {
                let _ = fs::write(&churn, line("2026-08-13T00:00:00.000Z"));
            }
        });
        let result = sync_state_from_sessions(&conn, &config, 9_000, 10, false).expect("sync");

        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM threads_cache WHERE thread_id = 'sess-late'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(rows, 0, "the file moved, so the reading was not written");
        assert_eq!(
            result.get("synced").and_then(Value::as_u64),
            Some(0),
            "a refused write is not a synced thread"
        );
        assert_eq!(
            result
                .get("threads")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(0),
            "and the refused snapshot is not handed back to the caller"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// A prompt that was already open when the upgrade landed. The revision
    /// column is added to a live table, so every row already in it starts
    /// NULL -- and NULL is not equal to anything in SQL, including the 0 a
    /// reader might substitute for it. Without a backfill the scan's
    /// compare-and-clear matches no row, so an answered question can never be
    /// retired; if the session has already ended there is no hook left to
    /// clear it either, and it sits in `/threads` forever.
    #[test]
    fn a_prompt_open_across_the_upgrade_can_still_be_retired() {
        // Two shapes a live database can be in when this version arrives.
        // The column may be missing entirely, in which case ALTER TABLE
        // leaves NULL; or it may already be there carrying `NOT NULL
        // DEFAULT 0`, left by an earlier build, in which case every writer
        // that omits it lands on the SAME value -- and 0, unlike NULL, does
        // match, so one clear would retire whichever prompt it found.
        let shapes: [(&str, &str); 2] = [
            ("absent", ""),
            ("default-zero", ", revision INTEGER NOT NULL DEFAULT 0"),
        ];
        for (label, revision_column) in shapes {
            let path = std::env::temp_dir().join(format!(
                "tinyctb-legacy-revision-{label}-{}.db",
                std::process::id()
            ));
            let _ = fs::remove_file(&path);
            {
                let conn = Connection::open(&path).expect("open legacy db");
                conn.execute_batch(&format!(
                    "
                    CREATE TABLE pending_prompts (
                        thread_id TEXT PRIMARY KEY,
                        prompt_id TEXT NOT NULL,
                        prompt_kind TEXT NOT NULL,
                        prompt_status TEXT NOT NULL,
                        question TEXT NOT NULL,
                        created_at INTEGER NOT NULL{revision_column}
                    );
                    INSERT INTO pending_prompts(
                        thread_id, prompt_id, prompt_kind, prompt_status, question, created_at)
                    VALUES ('thr_a', 'notify:1', 'reply', 'pending', '升级前的问题', 1000),
                           ('thr_b', 'notify:1', 'reply', 'pending', '同一毫秒的另一个', 1000);
                    "
                ))
                .expect("legacy pending_prompts");
            }
            let conn = crate::state::create_state_db(&path).expect("migrated db");

            // The scan reads each prompt the way production does.
            let mut revisions = Vec::new();
            for thread in ["thr_a", "thr_b"] {
                let (_, pending) = existing_thread_state(&conn, thread).expect("read prompt");
                let (prompt, revision) = pending.expect("the legacy prompt survives the migration");
                assert_eq!(prompt.prompt_id, "notify:1");
                assert!(
                    revision > 0,
                    "{label}: a row from before the upgrade needs a real instance id, got {revision}"
                );
                revisions.push(revision);
            }
            assert_ne!(
                revisions[0], revisions[1],
                "{label}: two legacy prompts sharing an id is the collision this guards"
            );

            // Having confirmed thr_a was answered, the scan retires THAT
            // instance -- and only that one.
            let _ = crate::state::upsert_thread_snapshot(
                &conn,
                &crate::state::BridgeThreadSnapshot {
                    thread_id: "thr_a".to_string(),
                    name: None,
                    cwd: None,
                    updated_at: Some(2_000),
                    status_type: "idle".to_string(),
                    status_flags: Vec::new(),
                    last_turn_status: None,
                    last_preview: None,
                    pending_prompt: None,
                    event_uid: None,
                },
                2_000,
                crate::state::UpdatedAt::Measured,
                Some(revisions[0]),
                None,
                None,
            )
            .expect("clear");

            let left: Vec<String> = {
                let mut stmt = conn
                    .prepare("SELECT thread_id FROM pending_prompts ORDER BY thread_id")
                    .expect("prepare");
                let rows = stmt.query_map([], |row| row.get(0)).expect("query");
                rows.collect::<rusqlite::Result<Vec<String>>>()
                    .expect("rows")
            };
            assert_eq!(
                left,
                vec!["thr_b".to_string()],
                "{label}: the answered prompt must clear, and only it"
            );
            let _ = fs::remove_file(&path);
        }
    }

    /// The last question before a reading is written down: is the file still
    /// the one it came from?
    #[test]
    fn a_reading_is_only_written_while_its_file_is_unchanged() {
        let dir = std::env::temp_dir().join(format!("tinyctb-fresh-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("fresh.jsonl");
        fs::write(&path, "{}\n").expect("write");

        let read_from = file_fingerprint(&path).expect("stat");
        assert!(
            file_unchanged_since(&path, read_from),
            "nothing happened, so the reading still stands"
        );

        fs::write(&path, "{}{}\n").expect("rewrite");
        assert!(
            !file_unchanged_since(&path, read_from),
            "the file moved on, so the reading is about something else now"
        );

        let _ = fs::remove_file(&path);
        assert!(
            !file_unchanged_since(&path, read_from),
            "and a file that is gone is certainly not the one that was read"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The bug this release exists for. A session that last spoke on the
    /// 12th, whose transcript file was TOUCHED today — a backup, a copy,
    /// anything — used to report today's time and sit at the top of
    /// `/threads` permanently, with no way to clear it.
    #[test]
    fn a_touched_transcript_does_not_make_an_old_session_look_recent() {
        let dir = std::env::temp_dir().join(format!("tinyctb-recency-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let spoke_at = transcript_timestamp_ms("2026-08-12T13:32:34.162Z").expect("stamp");
        let touched_now = spoke_at + 14 * 24 * 60 * 60 * 1000;
        // A FILE PER CASE. One file rewritten three times is the same path
        // at the same length, and the summary cache is keyed on exactly
        // that — rewrites inside one millisecond read back the previous
        // parse, and the test then measures the cache instead of the code.
        let scan = |name: &str, line: &str| {
            let path = dir.join(format!("{name}.jsonl"));
            fs::write(&path, line).expect("transcript");
            scan_snapshot(
                &SessionFileInfo {
                    session_id: name.to_string(),
                    path,
                    mtime_ms: touched_now,
                },
                touched_now,
            )
        };

        assert_eq!(
            scan(
                "spoke-on-the-12th",
                "{\"type\":\"user\",\"timestamp\":\"2026-08-12T13:32:34.162Z\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n"
            )
            .0
            .updated_at,
            Some(spoke_at),
            "the row must say when the SESSION spoke, not when the file was touched"
        );

        // A stamp from the future cannot outrank the clock: a wrong time in
        // the ordering is exactly what this release is fixing.
        assert_eq!(
            scan(
                "claims-the-future",
                "{\"type\":\"user\",\"timestamp\":\"2099-01-01T00:00:00.000Z\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n"
            )
            .0
            .updated_at,
            Some(touched_now),
            "a stamp this clock cannot believe is clamped, never trusted"
        );

        // No readable stamp at all: the mtime is the only evidence there is.
        assert_eq!(
            scan(
                "no-stamp-at-all",
                "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n"
            )
            .0
            .updated_at,
            Some(touched_now),
            "without a stamp the mtime is all there is, and it is used"
        );
        let _ = fs::remove_dir_all(&dir);
    }
    use crate::state::create_state_db_in_memory;
    use std::io::Write;

    /// The probe itself, with the test seam OUT of the way — otherwise a
    /// test only proves the wiring downstream of it (which is exactly how an
    /// early revert of this mapping stayed green). Every way of not knowing
    /// must answer `Unverified`; only a readable parent may answer `Window`.
    #[test]
    fn the_window_probe_answers_unverified_for_everything_it_cannot_read() {
        let _guard = crate::state::test_env_lock();
        // Guards, one per case and scoped: an assertion failing below must
        // not leak a rewritten socket variable into every later test. (They
        // are also NOT reassigned into one binding — dropping the old guard
        // after the new one is created would restore the old value on top.)
        let _seam = crate::state::EnvVarGuard::clear("TINYCTB_TEST_SESSION_WINDOWLESS");

        {
            let _socket = crate::state::EnvVarGuard::clear("CLAUDE_CODE_MESSAGING_SOCKET");
            assert_eq!(
                current_session_window(),
                SessionWindow::Unverified,
                "no socket variable: nothing was measured"
            );
        }
        {
            let _socket = crate::state::EnvVarGuard::set(
                "CLAUDE_CODE_MESSAGING_SOCKET",
                "/run/cc-socks/not-a-pid.sock",
            );
            assert_eq!(
                current_session_window(),
                SessionWindow::Unverified,
                "a socket name that carries no pid measures nothing either"
            );
        }
        {
            // A pid whose /proc entry cannot be read (and every pid at all,
            // on a system without /proc) is the same shrug.
            let _socket = crate::state::EnvVarGuard::set(
                "CLAUDE_CODE_MESSAGING_SOCKET",
                "/run/cc-socks/4294967294.sock",
            );
            assert_eq!(
                current_session_window(),
                SessionWindow::Unverified,
                "an unreadable process is not a terminal window"
            );
        }
        // The one case that may claim a window: our own live pid, whose
        // parent is the test runner rather than the background pty host.
        #[cfg(target_os = "linux")]
        {
            let _socket = crate::state::EnvVarGuard::set(
                "CLAUDE_CODE_MESSAGING_SOCKET",
                format!("/run/cc-socks/{}.sock", std::process::id()),
            );
            assert_eq!(
                current_session_window(),
                SessionWindow::Window,
                "a readable, ordinary parent is a real measurement"
            );
            assert_ne!(
                current_session_window(),
                SessionWindow::Background,
                "and the policy view (== Background) agrees"
            );
        }
    }

    /// `ps -o tty=` output is about to name a device the banner WRITES to,
    /// so parsing is a gate, not a convenience: only a single clean token
    /// becomes a path, and only ever under `/dev`.
    #[test]
    fn ps_tty_output_parses_only_clean_device_tokens() {
        assert_eq!(
            parse_ps_tty("pts/17\n"),
            Some(PathBuf::from("/dev/pts/17")),
            "Linux form"
        );
        assert_eq!(
            parse_ps_tty("ttys001\n"),
            Some(PathBuf::from("/dev/ttys001")),
            "macOS form"
        );
        assert_eq!(
            parse_ps_tty("/dev/pts/9"),
            Some(PathBuf::from("/dev/pts/9")),
            "already-absolute form stays under /dev"
        );
        for no_terminal in ["?", "??", "-", "", "   \n"] {
            assert_eq!(parse_ps_tty(no_terminal), None, "{no_terminal:?}");
        }
        assert_eq!(parse_ps_tty("pts/3 extra"), None, "one token only");
        assert_eq!(parse_ps_tty("../etc/passwd"), None, "no escaping /dev");
        // `PathBuf::join` discards the base for an absolute right-hand side,
        // so an anchor built on join once let these straight through. The
        // anchor is by construction now — these must all die at the parser.
        assert_eq!(parse_ps_tty("/tmp/victim"), None, "absolute escape");
        assert_eq!(parse_ps_tty("/etc/passwd"), None, "absolute escape");
        assert_eq!(parse_ps_tty("/dev//pts/3"), None, "empty component");
        assert_eq!(parse_ps_tty("pts//3"), None, "empty component");
        assert_eq!(parse_ps_tty("pts/./3"), None, "dot component");
        assert_eq!(parse_ps_tty("pts/../3"), None, "dot-dot component");
        assert_eq!(parse_ps_tty("/dev/../etc/passwd"), None, "dot-dot escape");
    }

    /// Sol's pgid-recycling scenario: the LEADER exits at once while its
    /// forked grandchild (same group, holding our stdout pipe) lives on.
    /// Before EOF the leader must stay UNREAPED — the zombie pins the
    /// PID/PGID anchor — so the timeout's group kill still lands on the
    /// right group and sweeps the grandchild. A revision that reaps the
    /// leader early frees the anchor, the group kill dies with ESRCH, and
    /// the grandchild leaks: this test is what goes red then.
    // target_os = "linux", not just unix: the zombie-anchor assertion
    // reads /proc/<pid>/stat, which macOS does not have.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_dead_leader_keeps_its_group_anchor_until_the_sweep() {
        let _guard = crate::state::test_env_lock();
        std::env::remove_var("TINYCTB_TEST_PROBE_STALL_MS");
        use std::os::unix::fs::PermissionsExt as _;
        let dir = std::env::temp_dir().join(format!("tinyctb-deadleader-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let grandchild_file = dir.join("grandchild.pid");
        let shell_file = dir.join("shell.pid");
        let script = dir.join("ps");
        // No `wait`: the shell exits immediately, leaving only the
        // grandchild — which inherited our pipe, so EOF never comes.
        fs::write(
            &script,
            format!(
                "#!/bin/sh\nsleep 1000 &\necho $! > {}\necho $$ > {}\nexit 0\n",
                grandchild_file.display(),
                shell_file.display()
            ),
        )
        .expect("script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

        let (tx, rx) = std::sync::mpsc::channel();
        let probe_script = script.clone();
        std::thread::spawn(move || {
            let _ = tx.send(probe_child_line(&probe_script, &[], PS_PROBE_BUDGET));
        });

        let read_pid = |path: &std::path::Path| -> i32 {
            fs::read_to_string(path)
                .expect("pid recorded")
                .trim()
                .parse()
                .expect("pid parses")
        };
        // Mid-probe, after the shell has exited (a handful of ms) but well
        // before the 500ms deadline: the leader must still be visible as a
        // ZOMBIE — the unreaped corpse IS the PID/PGID anchor. An early
        // `try_wait` regression reaps it here and the /proc entry is gone
        // long before the deadline, which is exactly what this catches.
        std::thread::sleep(Duration::from_millis(150));
        let shell = read_pid(&shell_file);
        let grandchild = read_pid(&grandchild_file);
        let stat = fs::read_to_string(format!("/proc/{shell}/stat"))
            .expect("the exited leader must remain visible (unreaped) before EOF");
        let state = stat
            .rsplit_once(')')
            .and_then(|(_, tail)| tail.split_whitespace().next())
            .expect("stat state field");
        assert_eq!(
            state, "Z",
            "the pre-EOF leader must be an unreaped zombie holding the group anchor"
        );

        let line = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the probe must give up within its budget");
        assert_eq!(line, None, "no EOF, no answer");
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let fully_gone = |pid: i32| {
                let outcome = unsafe { libc::kill(pid, 0) };
                outcome == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            };
            if fully_gone(shell) && fully_gone(grandchild) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "group sweep missed: shell {shell} and/or grandchild {grandchild} \
                 still alive 3s after the probe gave up"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// A `spawn()` parked on a hung filesystem never reaches any deadline
    /// check — bounding it is the CALLER's recv, not the worker. The stall
    /// seam injects that park; with the probe back on the caller's thread
    /// this takes the full stall and goes red on the elapsed bound.
    #[cfg(unix)]
    #[test]
    fn a_hung_spawn_cannot_freeze_the_probe_caller() {
        let _guard = crate::state::test_env_lock();
        std::env::set_var("TINYCTB_TEST_PROBE_STALL_MS", "3000");
        let started = std::time::Instant::now();
        let line = probe_child_line(Path::new("/bin/sh"), &["-c", "echo pts/1"], PS_PROBE_BUDGET);
        let took = started.elapsed();
        std::env::remove_var("TINYCTB_TEST_PROBE_STALL_MS");
        assert_eq!(
            line, None,
            "a probe that cannot spawn in time has no answer"
        );
        assert!(
            took <= PS_PROBE_BUDGET + Duration::from_millis(180),
            "caller froze for {took:?} behind a hung spawn"
        );
    }

    /// The bounded probe against a happy child: output collected to EOF,
    /// exit observed, line returned.
    #[cfg(unix)]
    #[test]
    fn the_ps_probe_returns_a_fast_child_output() {
        let _guard = crate::state::test_env_lock();
        std::env::remove_var("TINYCTB_TEST_PROBE_STALL_MS");
        let line = probe_child_line(
            Path::new("/bin/sh"),
            &["-c", "echo pts/5"],
            Duration::from_secs(5),
        );
        assert_eq!(line.as_deref().map(str::trim), Some("pts/5"));
    }

    /// Sol's P1 scenario: `ps` (or whatever wrapper answered to that name)
    /// never exits. The probe must give up within its budget, and the child
    /// must be KILLED AND REAPED — a gate that leaves a zombie per question
    /// is trading one leak for another.
    #[cfg(unix)]
    #[test]
    fn a_never_exiting_ps_cannot_hang_tty_discovery() {
        let _guard = crate::state::test_env_lock();
        std::env::remove_var("TINYCTB_TEST_PROBE_STALL_MS");
        use std::os::unix::fs::PermissionsExt as _;
        let dir = std::env::temp_dir().join(format!("tinyctb-fakeps-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("fake ps dir");
        let pid_file = dir.join("child.pid");
        let grandchild_file = dir.join("grandchild.pid");
        let script = dir.join("ps");
        // The shell FORKS a sleeping grandchild that inherits our stdout
        // pipe — the exact "grandchild holds EOF hostage" shape — then
        // waits on it. An earlier revision killed only the shell and
        // leaked the sleep (three of them observed on the host); both pids
        // are recorded so both deaths can be ASSERTED, not assumed.
        fs::write(
            &script,
            format!(
                "#!/bin/sh\nsleep 1000 &\necho $! > {}\necho $$ > {}\nwait\n",
                grandchild_file.display(),
                pid_file.display()
            ),
        )
        .expect("fake ps");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

        // Worker + recv_timeout: an unbounded-probe regression never
        // returns, so an elapsed assertion after the call would never run —
        // this recv is what goes red instead of a hung test binary.
        let (tx, rx) = std::sync::mpsc::channel();
        let probe_script = script.clone();
        std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let line = probe_child_line(&probe_script, &[], PS_PROBE_BUDGET);
            let _ = tx.send((line, started.elapsed()));
        });
        let (line, took) = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the probe must give up within its budget");
        assert_eq!(line, None, "a wedged probe must fail, not pretend");
        // The budget is the CALLER-VISIBLE total: cleanup runs past the
        // caller's return on the worker thread. The margin (180ms) is
        // deliberately smaller than the 200ms reap grace, so a regression
        // that pays the cleanup tax on the caller's clock (~700ms) fails
        // here — while honest channel/scheduling jitter does not.
        assert!(
            took <= PS_PROBE_BUDGET + Duration::from_millis(180),
            "probe took {took:?}; the budget must be a strict caller-visible total"
        );
        // Both processes must be GONE: kill(pid, 0) refusing with ESRCH is
        // the kernel saying the pid no longer exists — killed and reaped (a
        // zombie still answers signal 0). The grandchild's reap runs
        // through init after the group kill, which is asynchronous, so the
        // check polls briefly instead of racing it.
        let read_pid = |path: &std::path::Path| -> i32 {
            fs::read_to_string(path)
                .expect("pid recorded")
                .trim()
                .parse()
                .expect("pid parses")
        };
        let shell = read_pid(&pid_file);
        let grandchild = read_pid(&grandchild_file);
        let both_gone_deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let fully_gone = |pid: i32| {
                let outcome = unsafe { libc::kill(pid, 0) };
                outcome == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            };
            if fully_gone(shell) && fully_gone(grandchild) {
                break;
            }
            assert!(
                std::time::Instant::now() < both_gone_deadline,
                "leaked process: shell {shell} and/or grandchild {grandchild} still alive \
                 3s after the probe gave up"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// The live registry hands a child back by TURN ID, never by pid. A
    /// stale `stopping` row can carry a pid the kernel recycled onto a newer
    /// turn; a pid-keyed lookup handed that newer turn's process to the old
    /// turn's kill.
    #[cfg(unix)]
    #[test]
    fn the_registry_matches_turns_not_pids() {
        let child = std::process::Command::new("sleep")
            .arg("300")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn victim");
        let pid = child.id();
        turn_children::put_back(turn_children::RunningTurn {
            turn_id: "turn-registry-owner".to_string(),
            child,
        });
        assert!(
            turn_children::take("turn-registry-intruder").is_none(),
            "a different turn must never receive this handle, whatever its pid claims"
        );
        let mut entry =
            turn_children::take("turn-registry-owner").expect("the owner gets its handle");
        assert_eq!(entry.child.id(), pid, "and it is the same process");
        let _ = entry.child.kill();
        let _ = entry.child.wait();
    }

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
    fn transcript_summary_cache_sees_appends() {
        // The summary cache keys on (mtime, size); an append changes the size
        // even when it lands within the same millisecond, so a cached entry
        // must never mask new transcript content. This is the direction that
        // would rot silently: a hit that SHOULD miss shows a stale preview.
        let temp = TempDirGuard::new("tinyctb-summary-cache");
        let path = write_transcript(
            &temp.path,
            "sess-cache",
            &[
                json!({"type": "assistant", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "first answer"}
                ]}}),
            ],
        );
        let first = parse_transcript_summary(&path, u64::MAX).expect("first parse");
        assert_eq!(first.last_assistant_text.as_deref(), Some("first answer"));

        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("append");
        writeln!(
            file,
            "{}",
            json!({"type": "assistant", "message": {"role": "assistant", "content": [
                {"type": "text", "text": "second answer"}
            ]}})
        )
        .expect("append line");

        let second = parse_transcript_summary(&path, u64::MAX).expect("second parse");
        assert_eq!(second.last_assistant_text.as_deref(), Some("second answer"));
    }

    /// A restore must not be able to hide a rewrite. This test used to
    /// assert the opposite — rewrite the content, put the mtime back, and
    /// expect the OLD text — which made a stale cache the contract rather
    /// than a bug. Putting the mtime back is exactly what a backup tool
    /// does, and the row could then never be corrected again.
    #[test]
    fn a_rewrite_with_a_restored_mtime_is_still_a_rewrite() {
        let temp = TempDirGuard::new("tinyctb-summary-cache-hit");
        let path = write_transcript(
            &temp.path,
            "sess-cache-hit",
            &[
                json!({"type": "assistant", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "cache hit one"}
                ]}}),
            ],
        );
        let first = parse_transcript_summary(&path, u64::MAX).expect("first parse");
        assert_eq!(first.last_assistant_text.as_deref(), Some("cache hit one"));

        let saved_mtime = fs::metadata(&path)
            .expect("meta")
            .modified()
            .expect("mtime");
        let raw = fs::read_to_string(&path).expect("read");
        let swapped = raw.replace("cache hit one", "cache hit two");
        assert_ne!(raw, swapped);
        assert_eq!(raw.len(), swapped.len(), "rewrite must preserve length");
        fs::write(&path, swapped).expect("rewrite");
        let file = fs::File::options().write(true).open(&path).expect("open");
        file.set_modified(saved_mtime).expect("restore mtime");
        drop(file);

        let second = parse_transcript_summary(&path, u64::MAX).expect("second parse");
        assert_eq!(
            second.last_assistant_text.as_deref(),
            Some("cache hit two"),
            "same mtime, same length, same inode — and still a different file to read"
        );
    }

    /// The cache must still SERVE hits, or it is just a slow path with extra
    /// steps. Proven by looking at the cache itself: chmod and rename both
    /// change the inode's own change time, which the fingerprint now reads,
    /// so there is no way left to make a file look untouched from outside.
    #[test]
    fn an_untouched_transcript_is_answered_from_the_cache() {
        let temp = TempDirGuard::new("tinyctb-summary-cache-served");
        let path = write_transcript(
            &temp.path,
            "sess-cache-served",
            &[
                json!({"type": "assistant", "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "only answer"}
                ]}}),
            ],
        );
        let cached_summary = || -> Option<TranscriptSummary> {
            let guard = TRANSCRIPT_SUMMARY_CACHE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard
                .as_ref()
                .and_then(|cache| cache.get(&path))
                .map(|(_, summary)| summary.clone())
        };

        assert_eq!(
            parse_transcript_summary(&path, u64::MAX)
                .expect("first parse")
                .last_assistant_text
                .as_deref(),
            Some("only answer")
        );
        assert_eq!(
            cached_summary()
                .expect("the parse must be remembered")
                .last_assistant_text
                .as_deref(),
            Some("only answer"),
            "a parse that is not remembered is not a cache"
        );

        // A second call with the file untouched answers the same, and the
        // entry is still the one that first parse put there.
        assert_eq!(
            parse_transcript_summary(&path, u64::MAX)
                .expect("second parse")
                .last_assistant_text
                .as_deref(),
            Some("only answer")
        );
        assert_eq!(
            cached_summary()
                .expect("still remembered")
                .last_assistant_text
                .as_deref(),
            Some("only answer")
        );
    }

    #[test]
    fn spool_wake_burst_coalesces_into_one_tick() {
        let (tx, rx) = std::sync::mpsc::channel();
        for _ in 0..5 {
            tx.send(WatchWake::Spool).expect("send");
        }
        tx.send(WatchWake::Projects).expect("send");
        assert!(wait_for_spool_wake(&rx, Duration::from_millis(200)));
        // The whole burst was consumed by that one tick — a duplicate wake
        // must not schedule a second back-to-back forced sync.
        assert!(!wait_for_spool_wake(&rx, Duration::from_millis(0)));
    }

    #[test]
    fn projects_churn_does_not_end_the_wait() {
        let (tx, rx) = std::sync::mpsc::channel();
        let producer = std::thread::spawn(move || {
            for _ in 0..30 {
                let _ = tx.send(WatchWake::Projects);
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        let start = std::time::Instant::now();
        let woken = wait_for_spool_wake(&rx, Duration::from_millis(80));
        assert!(!woken, "transcript churn must never report a spool wake");
        assert!(
            start.elapsed() >= Duration::from_millis(80),
            "projects wakes must not shorten the tick"
        );
        producer.join().expect("producer");
    }

    #[test]
    fn spool_wake_ends_the_wait_early() {
        let (tx, rx) = std::sync::mpsc::channel();
        let producer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let _ = tx.send(WatchWake::Spool);
        });
        let start = std::time::Instant::now();
        assert!(wait_for_spool_wake(&rx, Duration::from_secs(10)));
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "a spool wake must cut the wait short"
        );
        producer.join().expect("producer");
    }

    #[test]
    fn queued_spool_wake_survives_a_zero_timeout_drain() {
        let (tx, rx) = std::sync::mpsc::channel();
        tx.send(WatchWake::Projects).expect("send");
        tx.send(WatchWake::Spool).expect("send");
        assert!(wait_for_spool_wake(&rx, Duration::from_millis(0)));
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

        let summary = parse_transcript_summary(&path, u64::MAX).expect("summary");
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
        let summary = parse_transcript_summary(&path, u64::MAX).expect("summary");
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
        let summary = parse_transcript_summary(&path, u64::MAX).expect("summary");
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
        let summary = parse_transcript_summary(&path, u64::MAX).expect("summary");
        let pending = summary.pending_tool_use.expect("pending tool");
        assert!(pending.contains("Which database should we use?"));
        assert!(pending.contains("Postgres / SQLite"));
    }

    #[test]
    fn permission_notification_includes_pending_tool_detail() {
        let _guard = crate::state::test_env_lock();
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

        let (snapshots, _, _, _) = ingest_spool_events(2000).expect("ingest");
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
            notification_type: None,
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
        let _guard = crate::state::test_env_lock();
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
            let _ = crate::state::upsert_thread_snapshot(
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
                        notification_type: None,
                    }),
                    event_uid: None,
                },
                1000,
                crate::state::UpdatedAt::Observed,
                None,
                None,
                None,
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
        let _guard = crate::state::test_env_lock();
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
        let (snapshots, _, _, _) = ingest_spool_events(2000).expect("ingest");
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
        let (snapshots, _, _, _) = ingest_spool_events(4000).expect("ingest");
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
        let (snapshots, _, _, _) = ingest_spool_events(6000).expect("ingest");
        let prompt = snapshots
            .iter()
            .find_map(|snapshot| snapshot.pending_prompt.as_ref())
            .expect("an unknown type must still be treated as a wait");
        assert_eq!(prompt.kind, "approval", "classified by its message text");

        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");
    }

    /// End-to-end for the field that decides whether a wait may be
    /// suppressed: Notification hook → spool file → ingest → DB row →
    /// thread JSON → the daemon's suppression check. Every hop must carry
    /// the RAW notification_type; a break anywhere makes the daemon read
    /// None and fail open (noisy but safe) or, if the type were folded on
    /// the way, silently eat a real question.
    #[test]
    fn notification_type_survives_the_whole_hook_to_daemon_path() {
        let _guard = crate::state::test_env_lock();
        let temp = TempDirGuard::new("notification-type-e2e");
        std::env::set_var("TINYCTB_STATE_DIR", &temp.path);
        let projects = temp.path.join("projects").join("-home-user-project");
        fs::create_dir_all(&projects).expect("projects dir");
        std::env::set_var(
            "TINYCTB_CLAUDE_PROJECTS_DIR",
            temp.path.join("projects").display().to_string(),
        );
        let conn = crate::state::create_state_db_in_memory().expect("db");
        // Wait events are only emitted while away — that is the mode in
        // which suppression can happen at all.
        set_away_mode(&conn, true, 500).expect("away on");
        let config = crate::config::DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: "thread_waiting,thread_completed".to_string(),
            telegram: None,
            claude: None,
            projects: vec![],
        };

        // Two waits that fold to the SAME kind ("reply") but mean opposite
        // things for suppression.
        for (index, (session, notification_type)) in [
            ("sess-idle", "idle_prompt"),
            ("sess-question", "agent_needs_input"),
        ]
        .into_iter()
        .enumerate()
        {
            write_transcript(
                &projects,
                session,
                &[json!({"type": "assistant", "cwd": "/home/user/project",
                "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "相同的结束语"}
                ]}})],
            );
            let mut payload = std::io::Cursor::new(
                json!({
                    "hook_event_name": "Notification",
                    "session_id": session,
                    "cwd": "/home/user/project",
                    "notification_type": notification_type,
                    "message": "Claude is waiting for your input"
                })
                .to_string(),
            );
            // Distinct receive times: the spool file name is derived from
            // this timestamp, so a shared one would overwrite the first hook.
            write_hook_event_from_reader(&mut payload, 1000 + index as u64).expect("spool");
        }

        let result = sync_state_from_sessions(&conn, &config, 2000, 50, true).expect("sync");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        // Hop 1: the DB row kept the raw type (folded kind is still "reply").
        for (session, notification_type) in [
            ("sess-idle", "idle_prompt"),
            ("sess-question", "agent_needs_input"),
        ] {
            let (_, prompt) = existing_thread_state(&conn, session).expect("state");
            let (prompt, _) =
                prompt.unwrap_or_else(|| panic!("{session} must have a pending prompt"));
            assert_eq!(
                prompt.kind, "reply",
                "{session} folds to reply for rendering"
            );
            assert_eq!(
                prompt.notification_type.as_deref(),
                Some(notification_type),
                "{session} must keep its raw notification_type in the DB"
            );
        }

        // Hop 2: the events the daemon actually consumes — same call it
        // makes, so the thread object is attached exactly as in production.
        let events = watch_events_from_sync_result(&result, None);
        for (session, notification_type) in [
            ("sess-idle", "idle_prompt"),
            ("sess-question", "agent_needs_input"),
        ] {
            let event = events
                .iter()
                .find(|event| {
                    event.get("type").and_then(Value::as_str) == Some("thread_waiting")
                        && event.get("threadId").and_then(Value::as_str) == Some(session)
                })
                .unwrap_or_else(|| panic!("{session} must produce a thread_waiting event"));
            assert_eq!(
                event
                    .pointer("/thread/pendingPrompt/notificationType")
                    .and_then(Value::as_str),
                Some(notification_type),
                "{session}: the daemon reads this exact pointer to decide suppression"
            );
        }
    }

    #[test]
    fn spool_ingestion_builds_stop_and_notification_snapshots() {
        let _guard = crate::state::test_env_lock();
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

        let (snapshots, _, consumed, _) = ingest_spool_events(2000).expect("ingest");
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
        let _guard = crate::state::test_env_lock();
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
        let _guard = crate::state::test_env_lock();
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
        let _guard = crate::state::test_env_lock();
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
        let _guard = crate::state::test_env_lock();
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

    /// A completion the floor already overtook (a delayed spool entry, a
    /// dead-letter replay) is still owed — and it must render ITS OWN
    /// answer. Before the `overtaken` carry, its snapshot was dropped from
    /// `threads`, enrichment fell back to the fresh same-session snapshot,
    /// and the "finished" notification showed the next question instead.
    #[test]
    fn a_late_completion_renders_its_own_answer() {
        let _guard = crate::state::test_env_lock();
        let temp = TempDirGuard::new("late-completion");
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
        let spool = events_spool_dir().expect("spool dir");
        fs::create_dir_all(&spool).expect("create spool");
        let write_hook = |received_at: u64, pid: u32, event: &str, payload: Value| {
            let envelope = json!({
                "receivedAt": received_at,
                "hookEventName": event,
                "sessionId": "sess-late",
                "payload": payload
            });
            fs::write(
                spool.join(format!("{received_at:015}-{pid}-{event}.json")),
                envelope.to_string(),
            )
            .expect("write spool file");
        };

        // Cycle N: a hook at 5000 lands normally; the observed floor moves
        // past 4000.
        write_hook(
            5_000,
            999,
            "SessionStart",
            json!({
                "hook_event_name": "SessionStart",
                "session_id": "sess-late",
                "cwd": "/home/user/project"
            }),
        );
        sync_state_from_sessions(&conn, &config, 5_000, 50, true).expect("cycle n");

        // Cycle M: the delayed completion finally arrives, sharing the batch
        // with a fresh hook of the same session.
        write_hook(
            4_000,
            111,
            "Stop",
            json!({
                "hook_event_name": "Stop",
                "session_id": "sess-late",
                "cwd": "/home/user/project",
                "last_assistant_message": "the real delayed answer"
            }),
        );
        write_hook(
            6_000,
            333,
            "Notification",
            json!({
                "hook_event_name": "Notification",
                "session_id": "sess-late",
                "cwd": "/home/user/project",
                "message": "the next question",
                "notification_type": "agent_needs_input"
            }),
        );
        let result = sync_state_from_sessions(&conn, &config, 6_000, 50, true).expect("cycle m");
        std::env::remove_var("TINYCTB_STATE_DIR");
        std::env::remove_var("TINYCTB_CLAUDE_PROJECTS_DIR");

        let enriched = watch_events_from_sync_result(&result, None);
        let completed = enriched
            .iter()
            .find(|event| event["type"] == "thread_completed")
            .expect("the late completion is still owed");
        // Paired with the snapshot it came from, not the fresh sibling.
        assert_eq!(
            completed
                .pointer("/thread/eventUid")
                .and_then(Value::as_str),
            Some("000000000004000-111-Stop"),
            "event: {completed}"
        );
        let prepared = crate::telegram::render::prepare_telegram_delivery("999", completed)
            .expect("prepared delivery");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.contains("the real delayed answer"), "text: {text}");
        assert!(
            !text.contains("the next question"),
            "a finished turn must not render the fresh question as its answer: {text}"
        );
    }

    /// A uid names exactly one hook. When neither the current nor the
    /// overtaken snapshots carry it, the event stays unenriched — a
    /// same-session sibling is no substitute.
    #[test]
    fn an_event_without_its_snapshot_stays_unenriched() {
        let sync = json!({
            "threads": [
                { "threadId": "s-1", "eventUid": "sibling", "lastPreview": "sibling preview" }
            ],
            "overtaken": [],
            "events": [{
                "type": "thread_completed",
                "threadId": "s-1",
                "eventUid": "mine",
                "lastPreview": "my answer",
                "eventKey": "k-1"
            }]
        });
        let events = watch_events_from_sync_result(&sync, None);
        assert_eq!(events.len(), 1);
        assert!(
            events[0].get("thread").is_none(),
            "a sibling snapshot is no substitute: {}",
            events[0]
        );
    }

    /// P1 regression: a spawned process whose identity write fails must be
    /// TERMINATED and its turn settled — not left running. With `pid` NULL
    /// the daemon's crash check would call the turn failed after 10 seconds,
    /// the turn would stop counting as running, and a live claude process
    /// would keep making tool calls outside the approval boundary.
    #[test]
    fn spawn_that_cannot_be_recorded_is_terminated() {
        let _guard = crate::state::test_env_lock();
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

    /// The last link of the failure-cleanup chain: when the unwinding kill
    /// cannot PROVE the group empty (a TERM-proof or foreign-uid descendant
    /// may have outlived the reaped leader), the turn must go `stopping`
    /// with its contract pgid written in — NOT `failed`, which moves the
    /// survivors out of every scan the daemon will ever make.
    #[test]
    fn an_unconfirmed_spawn_cleanup_stays_scannable() {
        let _guard = crate::state::test_env_lock();
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
        let conn = test_state_conn("cleanup-unconfirmed");
        let _outcome = test_kill::OutcomeGuard::set(KillOutcome::Undetermined);
        test_identity_persist::FAIL.store(true, std::sync::atomic::Ordering::SeqCst);
        let result = send_user_message(&conn, &config, "sess-cleanup", "go", None, 4000);
        test_identity_persist::FAIL.store(false, std::sync::atomic::Ordering::SeqCst);

        assert!(result.is_err(), "the spawn must still be reported failed");
        assert_eq!(
            test_kill::take(),
            vec![0],
            "the child must still be signalled (test spawn pid = 0)"
        );
        let (status, pgid): (String, Option<i64>) = conn
            .query_row("SELECT status, pgid FROM bridge_turns", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .expect("turn row");
        assert_eq!(
            status, "running",
            "an unproven cleanup stays RUNNING under the failure marker — `stopping` \
             is the user's word, and `failed` buries survivors forever"
        );
        assert_eq!(
            pgid,
            Some(0),
            "and the contract pgid must be written so the recovery loop can probe"
        );
    }

    /// A spawn whose EVERY settlement write failed leaves `running` plus a
    /// bound (already removed) object — deliberately outside the no-pid
    /// claim. The exhaustion tail marks supervision, and the recovery loop
    /// settles it ONE TICK later off the object's proven emptiness; the
    /// six-hour fiat is not the convergence path.
    #[test]
    fn an_unsettleable_spawn_converges_through_the_marker() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-ladder-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let _root_guard = crate::state::EnvVarGuard::set("TINYCTB_CGROUP_ROOT", &root);
        let conn = create_state_db_in_memory().expect("db");
        crate::state::register_bridge_turn(
            &conn,
            "turn-1",
            "sess-ladder",
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
        // The object was created and then removed by the failed spawn's
        // cleanup: the recorded path points at nothing — which IS proof.
        let gone = root.join("turn-turn-1");
        crate::state::record_turn_cgroup(&conn, "turn-1", gone.to_str().expect("utf8"))
            .expect("bind");

        test_settle_fail::FAIL.store(true, std::sync::atomic::Ordering::SeqCst);
        let _err = settle_failed_turn(&conn, "turn-1", 5000, anyhow::anyhow!("spawn failed"));
        test_settle_fail::FAIL.store(false, std::sync::atomic::Ordering::SeqCst);

        let (status, marker): (String, i64) = conn
            .query_row(
                "SELECT status, cleanup_pending FROM bridge_turns",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row");
        assert_eq!(status, "running", "nothing settled — every write failed");
        assert_eq!(
            marker, 2,
            "but the FAILURE-flavoured marker made it through"
        );

        let config = DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: None,
            claude: Some(ClaudeConfig::default()),
            projects: vec![],
        };
        crate::daemon::process_bridge_turns(&conn, &config, 7_000).expect("cycle");

        let (status, marker): (String, i64) = conn
            .query_row(
                "SELECT status, cleanup_pending FROM bridge_turns",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row");
        assert_eq!(
            status, "failed",
            "a spawn failure's cleanup settles as FAILED — `stopped` is the user's word"
        );
        assert_eq!(marker, 0, "and supervision is released");
        let receipt: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events
                 WHERE json_extract(payload_json, '$.eventKey') = 'cleanup-settled:turn-1'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(receipt, 1, "and the receipt says failure, not stop");
        let _ = fs::remove_dir_all(&root);
    }

    /// Teardown must report what it cannot PROVE empty — the caller
    /// aborts on any — while clean objects are removed. The stuck object
    /// is a fake with a hand-written `populated 1`, which is exactly what
    /// the prober reads.
    #[test]
    fn teardown_sweep_reports_what_it_cannot_prove_empty() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-sweeproot-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let _root_guard = crate::state::EnvVarGuard::set("TINYCTB_CGROUP_ROOT", &root);

        let clean = root.join("turn-clean");
        fs::create_dir(&clean).expect("clean");
        let stuck = root.join("turn-stuck");
        fs::create_dir(&stuck).expect("stuck");
        fs::write(stuck.join("cgroup.events"), "populated 1\nfrozen 0\n").expect("events");

        let report = turn_cgroup::sweep_all(Duration::from_millis(50)).expect("sweep runs");

        assert!(
            report.stubborn.contains(&stuck),
            "the unprovable object must be REPORTED, not skipped: {report:?}"
        );
        // (On this FAKE root the kill probe creates a regular file, which
        // blocks remove_dir — so under the STRICT contract even the clean
        // object lands on the report rather than being silently dropped.
        // Real removal is covered by the real-kernel-object tests.)
        assert!(
            report.stubborn.contains(&clean),
            "an object that could not be removed is reported too: {report:?}"
        );
        assert!(
            stuck.exists(),
            "the unprovable one is left for the caller to refuse on"
        );
        let _ = fs::remove_dir_all(&root);

        // A configured-but-unusable root is an ERROR, never "no objects".
        let _bad = crate::state::EnvVarGuard::set(
            "TINYCTB_CGROUP_ROOT",
            "/nonexistent/definitely-not-a-dir",
        );
        assert!(
            turn_cgroup::sweep_all(Duration::from_millis(10)).is_err(),
            "a broken root must block the teardown"
        );
    }

    /// A database path may only aim `cgroup.kill` after validation: the
    /// unified-hierarchy prefix, the exact `turn-<id>` name, and no `..` —
    /// a corrupted row must never kill whatever lives elsewhere.
    #[test]
    fn cgroup_paths_from_the_database_are_validated_before_use() {
        let _guard = crate::state::test_env_lock();
        let _root =
            crate::state::EnvVarGuard::set("TINYCTB_CGROUP_ROOT", "/sys/fs/cgroup/user.slice");
        assert!(turn_cgroup::validated("/sys/fs/cgroup/user.slice/turn-abc", "abc").is_some());
        assert!(
            turn_cgroup::validated("/sys/fs/cgroup/user.slice/turn-abc", "other").is_none(),
            "the name must match exactly this turn"
        );
        assert!(
            turn_cgroup::validated("/sys/fs/cgroup/system.slice/turn-abc", "abc").is_none(),
            "outside the trusted owner subtree is refused even under the hierarchy"
        );
        assert!(
            turn_cgroup::validated("/tmp/turn-abc", "abc").is_none(),
            "outside the hierarchy is refused"
        );
        assert!(
            turn_cgroup::validated("/sys/fs/cgroup/user.slice/../etc/turn-abc", "abc").is_none(),
            "traversal is refused"
        );
        assert!(
            turn_cgroup::validated("/sys/fs/cgroup/user.slice", "abc").is_none(),
            "a non-turn directory is refused"
        );
    }

    /// The primitive itself, on a real kernel object: create, membership
    /// via the same fork-time entry the spawn uses, atomic subtree kill of
    /// a TERM-proof member, emptiness proof, removal.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_turn_cgroup_kills_and_proves_the_whole_tree() {
        // Serialised: `owner_root` reads the overridable env var.
        let _guard = crate::state::test_env_lock();
        let turn_id = format!("cgprim-{}", std::process::id());
        let Some(dir) = turn_cgroup::create(&turn_id) else {
            eprintln!("host cannot provide a cgroup subtree; skipping");
            return;
        };
        assert_eq!(
            turn_cgroup::populated(&dir),
            Some(false),
            "a fresh object is empty"
        );

        // A TERM-ignoring member: only the subtree KILL can end it.
        let procs = fs::OpenOptions::new()
            .write(true)
            .open(dir.join("cgroup.procs"))
            .expect("open cgroup.procs");
        let mut member = {
            use std::os::unix::io::AsRawFd;
            use std::os::unix::process::CommandExt;
            let fd = procs.as_raw_fd();
            let mut command = std::process::Command::new("sh");
            command
                .args(["-c", "trap '' TERM; sleep 300"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            // SAFETY: raw write on an inherited fd.
            unsafe {
                command.pre_exec(move || {
                    let buf = b"0\n";
                    if libc::write(fd, buf.as_ptr().cast(), buf.len()) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            command.spawn().expect("spawn member")
        };
        drop(procs);
        assert_eq!(
            turn_cgroup::populated(&dir),
            Some(true),
            "the member must be inside the object"
        );

        assert!(turn_cgroup::kill(&dir), "the subtree kill must be accepted");
        let _ = member.wait();
        let deadline = std::time::Instant::now();
        while turn_cgroup::populated(&dir) != Some(false) {
            assert!(
                deadline.elapsed() < Duration::from_secs(5),
                "a killed subtree must read empty"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(turn_cgroup::remove(&dir), "an empty object removes");
        assert_eq!(
            turn_cgroup::populated(&dir),
            Some(false),
            "and a removed object reads empty forever"
        );
    }

    /// The invariant with no exceptions: an unproven group never reaches a
    /// terminal status. With the stopping write itself sabotaged, the row
    /// must be left `running` — visible and correctable — not settled as
    /// `failed`, which would bury the possible survivors all over again.
    #[test]
    fn a_cleanup_that_cannot_even_mark_stopping_never_buries_the_turn() {
        let _guard = crate::state::test_env_lock();
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
        let conn = test_state_conn("cleanup-noterminal");
        conn.execute_batch(
            "CREATE TRIGGER stopping_broken BEFORE UPDATE OF status ON bridge_turns
             WHEN NEW.status = 'stopping'
             BEGIN SELECT RAISE(ABORT, 'stopping broken'); END;",
        )
        .expect("trigger");
        let _outcome = test_kill::OutcomeGuard::set(KillOutcome::Undetermined);
        test_identity_persist::FAIL.store(true, std::sync::atomic::Ordering::SeqCst);
        let result = send_user_message(&conn, &config, "sess-noterm", "go", None, 4000);
        test_identity_persist::FAIL.store(false, std::sync::atomic::Ordering::SeqCst);

        assert!(result.is_err(), "the spawn must still be reported failed");
        let row = |conn: &rusqlite::Connection| -> (String, i64) {
            conn.query_row(
                "SELECT status, cleanup_pending FROM bridge_turns",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("turn row")
        };
        let (status, marker) = row(&conn);
        assert_eq!(
            status, "running",
            "no proof of an empty group and no stopping write → NO terminal status"
        );
        assert_eq!(marker, 2, "the durable failure-cleanup marker must be set");

        // The review's counterexample continued: 10 seconds later the
        // daemon's no-pid crash claim used to bury exactly this row. The
        // marker must hold it open — same status, still supervised.
        crate::daemon::process_bridge_turns(&conn, &config, 4000 + 10_001).expect("daemon cycle");
        let (status, marker) = row(&conn);
        assert_eq!(
            status, "running",
            "the 10-second no-evidence claim must NOT bury a supervised group"
        );
        assert_eq!(marker, 2, "and supervision must continue");
    }

    /// The review's worst case: BOTH recovery writes fail, the database
    /// then recovers, and 10 seconds pass. The debt recorded at
    /// REGISTRATION — before anything could fail — must still hold the
    /// no-pid claim off.
    #[test]
    fn a_recovered_database_still_cannot_bury_the_debtor() {
        let _guard = crate::state::test_env_lock();
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
        let conn = test_state_conn("db-recovers");
        conn.execute_batch(
            "CREATE TRIGGER stopping_broken BEFORE UPDATE OF status ON bridge_turns
             WHEN NEW.status = 'stopping'
             BEGIN SELECT RAISE(ABORT, 'stopping broken'); END;
             CREATE TRIGGER marker_broken BEFORE UPDATE OF cleanup_pending ON bridge_turns
             BEGIN SELECT RAISE(ABORT, 'marker broken'); END;",
        )
        .expect("triggers");
        let _outcome = test_kill::OutcomeGuard::set(KillOutcome::Undetermined);
        test_identity_persist::FAIL.store(true, std::sync::atomic::Ordering::SeqCst);
        let result = send_user_message(&conn, &config, "sess-recover", "go", None, 4000);
        test_identity_persist::FAIL.store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(result.is_err(), "the spawn must be reported failed");

        // The database "recovers"...
        conn.execute_batch("DROP TRIGGER stopping_broken; DROP TRIGGER marker_broken;")
            .expect("drop triggers");
        // ...and 10 seconds later the no-pid claim comes around.
        crate::daemon::process_bridge_turns(&conn, &config, 4000 + 10_001).expect("cycle");

        let (status, marker): (String, i64) = conn
            .query_row(
                "SELECT status, cleanup_pending FROM bridge_turns",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row");
        assert_eq!(
            status, "running",
            "the birth debt must hold the no-evidence claim off even after recovery"
        );
        assert_eq!(
            marker, 1,
            "the debt was recorded at registration, not best-effort"
        );
    }

    /// A `/stop` that wins the race before the identity lands leaves a
    /// `stopping` row with nothing to probe. The unwinding must patch the
    /// FULL identity in whatever the status says — under both cleanup
    /// verdicts — and a REOPENED database must still hold everything a
    /// restart-path re-kill needs (ticks + boot id, not just pid/pgid).
    #[test]
    fn unwinding_patches_identity_into_a_stopping_row_under_both_verdicts() {
        let _guard = crate::state::test_env_lock();
        let path =
            std::env::temp_dir().join(format!("tinyctb-unwind-identity-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let identity = ProcessIdentity {
            lstart: None,
            pgid: Some(4_400),
            exe: None,
            start_ticks: Some("777".to_string()),
            boot_id: Some("boot-x".to_string()),
        };
        // Undetermined: the stop intent survives (the group is unproven,
        // the daemon keeps sweeping). Terminated: the settlement CARRIES
        // PROOF and overrides the stop — satisfied vacuously, no phantom
        // `stopping + NULL identity` left behind.
        for (verdict, expect_status, expect_marker) in [
            (KillOutcome::Undetermined, "stopping", 2_i64),
            (KillOutcome::Terminated, "failed", 0_i64),
        ] {
            let conn = crate::state::create_state_db(&path).expect("db");
            conn.execute("DELETE FROM bridge_turns", []).expect("reset");
            crate::state::register_bridge_turn(
                &conn,
                "turn-1",
                "sess-1",
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
            // The /stop wins the race before the identity write.
            crate::state::mark_bridge_turn_stopping(&conn, "turn-1", 1500).expect("stop wins");

            let _ = settle_unwound_spawn(
                &conn,
                "turn-1",
                &identity,
                Some(4_400),
                verdict,
                2000,
                anyhow::anyhow!("identity write failed"),
            );

            // Reopen: what a restarted daemon would find, with no live handle.
            drop(conn);
            let conn = crate::state::create_state_db(&path).expect("reopen");
            type Row = (
                String,
                Option<i64>,
                Option<i64>,
                Option<String>,
                Option<String>,
                i64,
            );
            let (status, pid, pgid, ticks, boot, marker): Row = conn
                .query_row(
                    "SELECT status, pid, pgid, proc_start_ticks, boot_id, cleanup_pending
                     FROM bridge_turns",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .expect("row");
            assert_eq!(
                status, expect_status,
                "{verdict:?}: unproven keeps the intent; proof settles it vacuously"
            );
            assert_eq!(pid, Some(4_400), "{verdict:?}: pid patched");
            assert_eq!(pgid, Some(4_400), "{verdict:?}: pgid patched");
            assert_eq!(
                ticks.as_deref(),
                Some("777"),
                "{verdict:?}: ticks patched — a restart-path re-kill is impossible without them"
            );
            assert_eq!(
                boot.as_deref(),
                Some("boot-x"),
                "{verdict:?}: boot id patched"
            );
            assert_eq!(marker, expect_marker, "{verdict:?}: marker state");
            let supervised = crate::state::list_supervised_bridge_turns(&conn)
                .expect("supervised")
                .iter()
                .any(|turn| turn.turn_id == "turn-1");
            assert_eq!(
                supervised,
                expect_marker != 0,
                "{verdict:?}: an unproven group stays visible to the reopened daemon; \
                 a proven-settled one is history"
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// The /threads liveness facts: a session with a running turn counts as
    /// headless-active, a settled one does not; a session with no recorded
    /// socket never counts as a live terminal.
    #[test]
    fn liveness_primitives_read_the_actual_state() {
        let _guard = crate::state::test_env_lock();
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

    /// A verified socket proves the session is ALIVE and reachable. It does
    /// not prove anyone can see it: the window question is answered by
    /// reading the owner's parent, and that read can fail (a parent owned by
    /// another user, a parent that just exited, a platform with no /proc).
    /// Publishing those as "🖥 终端活跃" is what put a terminal-fallback
    /// promise on sessions nobody had looked at.
    ///
    /// pid 1 is the reliable shape of that failure: its own stat is
    /// readable (so identity verifies), while its parent is pid 0, which has
    /// no /proc entry at all.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_session_whose_parent_cannot_be_read_is_not_published_as_a_window() {
        let _guard = crate::state::test_env_lock();
        let conn = test_state_conn("presence-unverified");
        let dir = std::env::temp_dir().join(format!("tinyctb-presence-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        // A plain file stands in for the socket: presence only stats it (an
        // AF_UNIX bind would fail in sandboxes that forbid it).
        let unreadable_parent = dir.join("1.sock");
        fs::write(&unreadable_parent, "").expect("socket stand-in");
        let path = unreadable_parent.display().to_string();
        let (inode, boot_id) = socket_identity(&path);
        assert!(
            inode.is_some() && boot_id.is_some(),
            "the fixture must present a VERIFIABLE identity, or the test \
             proves nothing about the window question"
        );
        crate::state::record_session_messaging_socket(
            &conn,
            "sess-unverified",
            &SessionSocket {
                path,
                inode,
                boot_id,
            },
            1000,
            1000,
        )
        .expect("record");
        assert_eq!(
            session_terminal_presence(&conn, "sess-unverified").expect("presence"),
            TerminalPresence::Unverified,
            "an unreadable parent is not a terminal window"
        );

        // The mirror: our own pid, whose parent is the test runner.
        let readable_parent = dir.join(format!("{}.sock", std::process::id()));
        fs::write(&readable_parent, "").expect("socket stand-in");
        let path = readable_parent.display().to_string();
        let (inode, boot_id) = socket_identity(&path);
        crate::state::record_session_messaging_socket(
            &conn,
            "sess-window",
            &SessionSocket {
                path,
                inode,
                boot_id,
            },
            1000,
            1000,
        )
        .expect("record");
        assert_eq!(
            session_terminal_presence(&conn, "sess-window").expect("presence"),
            TerminalPresence::Window,
            "a readable, ordinary parent is a real measurement"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The daemon-preemption interleaving: a slow spawn can outlive the 10s
    /// crash grace, in which case the daemon settles the turn as failed
    /// BEFORE the identity write runs. That write must then refuse — updating
    /// the settled row would report a turn the daemon has declared dead (and
    /// notified the user about) as successfully started.
    #[test]
    fn identity_persist_refuses_a_turn_the_daemon_already_settled() {
        let _guard = crate::state::test_env_lock();
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
        let _guard = crate::state::test_env_lock();
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
        let _guard = crate::state::test_env_lock();
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
        let _guard = crate::state::test_env_lock();
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
            cgroup_path: None,
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
    /// The restart path judges by OBSERVATION, not by a signal's exit code:
    /// `kill` returns ESRCH for a target that is already gone (success, not
    /// failure) and returns success for one that has not exited yet. A pid
    /// with no `/proc` entry and an empty group is provably over.
    ///
    /// Linux-only: every judgement here comes from `/proc`.
    #[cfg(target_os = "linux")]
    #[test]
    fn restart_termination_of_a_vanished_group_is_provably_over() {
        // A pid that cannot exist, with ticks that therefore never match.
        // Nothing can be proven about a group that was never there, and the
        // honest answer is Undetermined rather than "terminated".
        let outcome = terminate_verified_group(4_294_000_000, 4_294_000_000, "999999");
        assert_eq!(
            outcome,
            KillOutcome::Undetermined,
            "an out-of-range group id cannot be proven empty — Undetermined, not stopped"
        );
    }

    /// A leader that exited politely while a grandchild ignores TERM: the
    /// UNREAPED leader still anchors the group id, so the group KILL is
    /// provably aimed at the original group — and it is the only thing that
    /// ever sweeps that grandchild through the restart path. Refusing to
    /// signal over a zombie leader left exactly this shape running
    /// forever. The test HOLDS the leader unreaped, so the anchor is
    /// guaranteed for the whole call; no reaper race decides the verdict.
    #[cfg(target_os = "linux")]
    #[test]
    fn an_anchored_zombie_leader_still_gets_its_group_swept() {
        use std::os::unix::process::CommandExt as _;
        let dir = std::env::temp_dir().join(format!("tinyctb-anchor-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let pid_file = dir.join("grandchild.pid");
        // The leader spawns a TERM-ignoring grandchild and exits at once.
        // Null stdio, or the surviving grandchild holds the test harness's
        // captured output pipe open for its whole sleep — a red run then
        // reads as a hang instead of a failure.
        let mut leader = std::process::Command::new("sh")
            .args([
                "-c",
                &format!(
                    "sh -c 'trap \"\" TERM; echo $$ > {}; sleep 300' & exit 0",
                    pid_file.display()
                ),
            ])
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn leader");
        let leader_pid = leader.id();
        // Readable for zombies too — the entry stays until WE reap.
        let ticks = process_start_ticks(leader_pid).expect("leader ticks");
        let deadline = std::time::Instant::now();
        let grandchild: u32 = loop {
            if let Some(pid) = fs::read_to_string(&pid_file)
                .ok()
                .and_then(|raw| raw.trim().parse().ok())
            {
                break pid;
            }
            assert!(
                deadline.elapsed() < Duration::from_secs(5),
                "the grandchild must announce itself"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        // Dead-or-gone probe that also accepts an unreaped zombie: if the
        // orphan lands under a subreaper that never waits, the entry stays —
        // as a zombie, which is just as dead.
        let swept = |pid: u32| match fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => true,
            Ok(stat) => stat
                .rsplit_once(')')
                .and_then(|(_, rest)| rest.split_whitespace().next())
                .is_some_and(|state| matches!(state, "Z" | "X" | "x")),
        };
        assert!(
            !swept(grandchild),
            "the grandchild must be running before the kill"
        );

        let outcome = terminate_verified_group(leader_pid, leader_pid, &ticks);

        // It ignored TERM, so only the group KILL can have removed it.
        let deadline = std::time::Instant::now();
        while !swept(grandchild) {
            assert!(
                deadline.elapsed() < Duration::from_secs(5),
                "the TERM-ignoring grandchild must be swept by the group KILL"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        // With the leader held unreaped the group can never read empty, and
        // claiming "terminated" without that proof is the exact bug class
        // this module exists to prevent.
        assert_eq!(
            outcome,
            KillOutcome::Undetermined,
            "an unreaped leader keeps the group unproven"
        );
        let _ = leader.wait();
        let _ = fs::remove_dir_all(&dir);
    }

    /// The production shape that used to jam forever: through the restart
    /// path nothing pins the anchor — init reaps the leader the moment it
    /// dies — while a grandchild in the same group ignores TERM. A KILL
    /// delayed behind a grace window found the anchor gone, refused, and
    /// left the turn `stopping` with the grandchild running. Sweeping the
    /// whole group while the anchor is verified must complete the stop.
    #[cfg(target_os = "linux")]
    #[test]
    fn restart_termination_completes_despite_a_reaped_leader() {
        use std::os::unix::process::CommandExt as _;
        let dir = std::env::temp_dir().join(format!("tinyctb-reaped-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let pid_file = dir.join("grandchild.pid");
        // The leader stays alive (plain sleep tail — no TERM trap) while its
        // grandchild ignores TERM; a standby reaper plays init's part and
        // collects the leader the instant it dies.
        let child = std::process::Command::new("sh")
            .args([
                "-c",
                &format!(
                    "sh -c 'trap \"\" TERM; echo $$ > {}; sleep 300' & exec sleep 300",
                    pid_file.display()
                ),
            ])
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn leader");
        let pid = child.id();
        let ticks = process_start_ticks(pid).expect("leader ticks");
        let (reaped_tx, reaped_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut child = child;
            let _ = child.wait();
            let _ = reaped_tx.send(());
        });
        let deadline = std::time::Instant::now();
        while !pid_file.exists() {
            assert!(
                deadline.elapsed() < Duration::from_secs(5),
                "the grandchild must announce itself"
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        let outcome = terminate_verified_group(pid, pid, &ticks);

        assert!(
            reaped_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "the standby reaper must have collected the leader"
        );
        assert_eq!(
            outcome,
            KillOutcome::Terminated,
            "a reaped leader plus a TERM-ignoring grandchild must still be a COMPLETED stop"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The real shape: a live process in its own group, killed through the
    /// restart path, confirmed by observation rather than by exit codes.
    #[cfg(target_os = "linux")]
    #[test]
    fn restart_termination_confirms_a_real_process_died() {
        use std::os::unix::process::CommandExt as _;
        // Its OWN process group, like a real headless turn.
        let child = std::process::Command::new("sleep")
            .arg("300")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn victim");
        let pid = child.id();
        let ticks = process_start_ticks(pid).expect("victim must be observable");

        // Something must REAP it, or the zombie keeps the group non-empty
        // and killpg keeps answering "exists". In production that is the
        // daemon's own reaper, or init after a restart; here it is a thread.
        // Detached, with a channel timeout: a join would hang the whole
        // suite if the kill ever failed to land, where a bounded wait fails
        // THIS test instead.
        let (reaped_tx, reaped_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut child = child;
            let _ = child.wait();
            let _ = reaped_tx.send(());
        });

        let outcome = terminate_verified_group(pid, pid, &ticks);
        assert!(
            reaped_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "the reaper must have collected the victim"
        );
        assert_eq!(
            outcome,
            KillOutcome::Terminated,
            "a process that really dies and is reaped must be reported as terminated"
        );
    }

    /// Every branch of the parse, including the ones a real `/proc` entry
    /// cannot be coaxed into producing. A malformed or truncated entry is
    /// UNKNOWN, never "ended" — the difference is whether a still-running
    /// turn gets recorded as stopped and then vanishes from every scan.
    #[test]
    fn liveness_parsing_separates_unknown_from_ended() {
        // Field 3 is state, field 22 overall is starttime — i.e. index 18
        // after the state token.
        let stat = |state: &str, ticks: &str| {
            let filler = std::iter::repeat("0")
                .take(18)
                .collect::<Vec<_>>()
                .join(" ");
            format!("1234 (claude) {state} {filler} {ticks} rest")
        };
        assert_eq!(
            liveness_from_stat(&stat("S", "555"), "555"),
            Liveness::Alive
        );
        assert_eq!(
            liveness_from_stat(&stat("S", "777"), "555"),
            Liveness::Ended,
            "different ticks mean the pid was recycled"
        );
        assert_eq!(
            liveness_from_stat(&stat("Z", "555"), "555"),
            Liveness::Ended,
            "a zombie is dead, just not reaped"
        );
        assert_eq!(
            liveness_from_stat("garbage with no paren", "555"),
            Liveness::Unknown,
            "an unparseable entry proves nothing"
        );
        assert_eq!(
            liveness_from_stat("1234 (claude) S 1 2 3", "555"),
            Liveness::Unknown,
            "a truncated entry proves nothing either"
        );
    }

    /// The group probe's contract. `ESRCH` is the ONLY answer that means
    /// empty: `EPERM` says the group exists but belongs to someone else —
    /// exactly a descendant that dropped privileges — and reading that as
    /// "gone" is how a live turn gets recorded as stopped.
    #[cfg(target_os = "linux")]
    #[test]
    fn group_probe_only_calls_esrch_empty() {
        use std::os::unix::process::CommandExt as _;
        let mut member = std::process::Command::new("sleep")
            .arg("300")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn member");
        let pgid = member.id();
        assert_eq!(group_liveness(pgid), Liveness::Alive);
        let _ = member.kill();
        let _ = member.wait();
        assert_eq!(
            group_liveness(pgid),
            Liveness::Ended,
            "once reaped, killpg reports ESRCH and the group is genuinely empty"
        );
        // pgid 0 means "my own group" to killpg — never a question we can
        // answer about someone else's turn.
        assert_eq!(group_liveness(0), Liveness::Unknown);
    }

    /// The errno mapping, which is where the dangerous case lives. `ESRCH`
    /// is the ONLY answer meaning empty; `EPERM` says the group exists and
    /// merely belongs to someone else — the shape of a descendant that
    /// dropped privileges, and reading it as gone is how a turn that is
    /// still working gets recorded as stopped. (This host answers 0 even for
    /// init's group, so the branch cannot be produced with a real process.)
    #[test]
    fn killpg_errno_mapping_only_calls_esrch_empty() {
        assert_eq!(liveness_from_killpg(0, None), Liveness::Alive);
        assert_eq!(
            liveness_from_killpg(-1, Some(libc::ESRCH)),
            Liveness::Ended,
            "no such group is the one and only proof of emptiness"
        );
        assert_eq!(
            liveness_from_killpg(-1, Some(libc::EPERM)),
            Liveness::Alive,
            "a group we may not signal EXISTS — never read that as empty"
        );
        assert_eq!(
            liveness_from_killpg(-1, Some(libc::EINVAL)),
            Liveness::Unknown
        );
        assert_eq!(liveness_from_killpg(-1, None), Liveness::Unknown);
    }

    /// The forced-verdict seam drives the production entry point, so each
    /// verdict's handling is exercised rather than assumed.
    #[test]
    fn injected_group_verdicts_reach_the_production_probe() {
        for verdict in [Liveness::Alive, Liveness::Ended, Liveness::Unknown] {
            let _guard = test_group_probe::ProbeGuard::set(verdict);
            assert_eq!(group_liveness(4_242), verdict);
        }
    }

    /// Without incarnation ticks (macOS has no /proc) a reaped leader's
    /// group can still be PROBED, and ESRCH is sufficient proof — the pid
    /// came from our own just-reaped handle and the probe signals nothing.
    /// Without this arm the turn read Undetermined forever and never
    /// settled. A live group stays unproven; no pgid proves nothing.
    #[test]
    fn a_reaped_leader_without_ticks_confirms_by_group_probe() {
        let ended = test_group_probe::ProbeGuard::set(Liveness::Ended);
        assert_eq!(
            confirm_reaped_leader(4_242, None, Some(4_242)),
            KillOutcome::Terminated,
            "an empty group after a reaped leader is a completed stop"
        );
        drop(ended);
        let alive = test_group_probe::ProbeGuard::set(Liveness::Alive);
        assert_eq!(
            confirm_reaped_leader(4_242, None, Some(4_242)),
            KillOutcome::Undetermined,
            "a populated group stays unproven"
        );
        drop(alive);
        assert_eq!(
            confirm_reaped_leader(4_242, None, None),
            KillOutcome::Undetermined,
            "no pgid proves nothing"
        );
    }

    /// The pgid is ASSIGNED, not observed: the spawn puts the child in its
    /// own group, so pgid == pid by kernel guarantee. An observation (ps)
    /// can fail — here, for a pid that does not exist — and a NULL pgid is
    /// the one hole the stopping machinery cannot recover from.
    #[cfg(unix)]
    #[test]
    fn captured_identity_pins_the_pgid_deterministically() {
        let identity = capture_process_identity(Some(4_150_000));
        assert_eq!(
            identity.pgid,
            Some(4_150_000),
            "pgid must come from the spawn contract, not from a fallible probe"
        );
    }

    /// A spawn without the identity the stopping machinery keys on must be
    /// refused: a persisted NULL pgid probes Unknown forever (the turn can
    /// never settle once stopped), and on Linux missing ticks or boot id
    /// strands every restart-path kill.
    #[test]
    fn spawn_identity_must_be_complete_for_this_platform() {
        let complete = ProcessIdentity {
            lstart: None,
            pgid: Some(1_234),
            exe: None,
            start_ticks: Some("555".to_string()),
            boot_id: Some("boot".to_string()),
        };
        assert_eq!(incomplete_spawn_identity(&complete), None);
        let mut no_pgid = complete.clone();
        no_pgid.pgid = None;
        assert!(
            incomplete_spawn_identity(&no_pgid).is_some(),
            "pgid is mandatory on every platform"
        );
        #[cfg(target_os = "linux")]
        {
            let mut no_ticks = complete.clone();
            no_ticks.start_ticks = None;
            assert!(
                incomplete_spawn_identity(&no_ticks).is_some(),
                "Linux requires starttime ticks"
            );
            let mut no_boot = complete;
            no_boot.boot_id = None;
            assert!(
                incomplete_spawn_identity(&no_boot).is_some(),
                "Linux requires the boot id"
            );
        }
    }

    /// A live leader with an empty group, or a dead leader with a live one,
    /// are BOTH "not proven stopped". Requiring only the leader let a turn
    /// be settled while a descendant kept doing its work.
    #[cfg(target_os = "linux")]
    #[test]
    fn confirmation_requires_both_the_leader_and_the_group() {
        use std::os::unix::process::CommandExt as _;
        let mut member = std::process::Command::new("sleep")
            .arg("300")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn member");
        let pgid = member.id();

        // Leader gone (a pid that cannot exist), group still populated.
        let outcome = confirm_group_gone(
            4_294_000_000,
            pgid,
            "irrelevant",
            Duration::from_millis(200),
        );
        let _ = member.kill();
        let _ = member.wait();
        assert_eq!(
            outcome,
            KillOutcome::Undetermined,
            "a live group member means the turn is not stopped, whatever the leader did"
        );
    }

    /// A live process must never read as ended, and a pid whose ticks no
    /// longer match must never read as alive.
    #[cfg(target_os = "linux")]
    #[test]
    fn liveness_distinguishes_alive_ended_and_recycled() {
        let mine = std::process::id();
        let ticks = process_start_ticks(mine).expect("own ticks");
        assert_eq!(incarnation_liveness(mine, &ticks), Liveness::Alive);
        assert_eq!(
            incarnation_liveness(mine, "0"),
            Liveness::Ended,
            "different ticks mean the pid was recycled — our incarnation is over"
        );
        assert_eq!(
            incarnation_liveness(4_294_000_000, &ticks),
            Liveness::Ended,
            "a pid with no entry is gone"
        );
        // Our REAL process group (not our pid — a test binary is rarely a
        // group leader) must read as populated.
        let own_pgid = fs::read_to_string("/proc/self/stat")
            .ok()
            .and_then(|stat| {
                stat.rsplit_once(')')
                    // ppid, pgrp, session… — pgrp is the second after state,
                    // i.e. index 2 counting the state token itself.
                    .and_then(|(_, rest)| rest.split_whitespace().nth(2))
                    .and_then(|value| value.parse::<u32>().ok())
            })
            .expect("own pgid");
        assert_eq!(
            group_liveness(own_pgid),
            Liveness::Alive,
            "our own group is certainly not empty"
        );
        // NOTE: a pgid outside the kernel's pid range answers EINVAL, not
        // ESRCH — "that is not a valid group" is ignorance, not emptiness.
        // Emptiness is proven with a real group in `group_probe_only_calls_esrch_empty`.
        assert_eq!(
            group_liveness(4_294_000_000),
            Liveness::Unknown,
            "an out-of-range group id proves nothing either way"
        );
    }

    /// The grace probe must observe the leader's exit WITHOUT reaping it:
    /// unreaped, the leader keeps its pid — and the group id — reserved, so
    /// the unconditional group KILL that follows provably hits the original
    /// group. A probe that reaps (`try_wait`) frees the number first and
    /// hands the KILL to whoever inherits it.
    ///
    /// The discriminating evidence is the `/proc` entry, NOT the `Child`
    /// handle: `try_wait` caches a consumed exit status, so a probe that
    /// wrongly reaped would still answer politely through the handle — but
    /// it cannot put the zombie's `/proc` entry back.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_grace_probe_detects_exit_without_reaping() {
        use std::os::unix::process::CommandExt as _;
        let mut child = std::process::Command::new("sleep")
            .arg("300")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn victim");
        assert!(
            !leader_exited(&mut child),
            "a live leader has not exited yet"
        );
        child.kill().expect("kill victim");
        let started = std::time::Instant::now();
        while !leader_exited(&mut child) {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the exit must become observable"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let stat = fs::read_to_string(format!("/proc/{}/stat", child.id()))
            .expect("an unreaped leader must keep its /proc entry — the probe must not reap");
        assert!(
            stat.rsplit_once(')')
                .map(|(_, rest)| rest.trim_start().starts_with('Z'))
                .unwrap_or(false),
            "the observed exit must still be an unreaped zombie: {stat}"
        );
        let _ = child.wait();
    }

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
        let _guard = crate::state::test_env_lock();
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
        let (_, sockets, _, _) = ingest_spool_events(2000).expect("ingest");
        std::env::remove_var("CLAUDE_CODE_MESSAGING_SOCKET");
        std::env::remove_var("TINYCTB_STATE_DIR");

        assert_eq!(
            sockets.get("sess-sock").map(|(s, _)| s.path.as_str()),
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
        let _guard = crate::state::test_env_lock();
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
        let _guard = crate::state::test_env_lock();
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
        let _guard = crate::state::test_env_lock();
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
        let (snapshots, _, consumed, _) = ingest_spool_events(2000).expect("ingest");
        std::env::remove_var("TINYCTB_STATE_DIR");
        assert_eq!(consumed, 1, "peek must not consume the spool");
        assert_eq!(snapshots.len(), 1);
    }

    #[test]
    fn headless_new_session_generates_session_id() {
        let _guard = crate::state::test_env_lock();
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
