//! Remote approvals: answering Claude's permission prompts from Telegram.
//!
//! Two gates, split on `bypassPermissions`, because the sessions they serve
//! have nothing in common downstream.
//!
//! **Interactive** sessions are gated from `PermissionRequest` — the event
//! Claude Code raises right before it would show a permission prompt. That
//! timing is the whole point: the gate engages only for calls that genuinely
//! need an answer, so it never has to guess which tools are risky or
//! re-implement the `permissions.allow` rules.
//!
//! **Headless** turns — the ones the bridge starts for Telegram — get none of
//! that. `PermissionRequest` does not fire in `-p` mode, and the alternative
//! is worse than it sounds: with `--permission-mode default` a headless call
//! that would prompt is refused by the sandbox instead, which is why bridge
//! turns run under `bypassPermissions` in the first place. Measured, all
//! three, in docs/approvals.md. So they are gated from `PreToolUse`, which
//! does fire, with a matcher confining it to the tools that change something.
//!
//! The session's messaging socket cannot answer a permission prompt — it
//! accepts user messages only — so a blocking hook is the mechanism.
//!
//! Safety rules, all load-bearing:
//! - a timeout NEVER allows;
//! - what a timeout DOES do depends on what is downstream. An interactive
//!   session gets "no opinion" and its own terminal dialog. A headless turn
//!   has no dialog to fall back to and `bypassPermissions` behind it, so
//!   there "no opinion" would mean "run it" — silence has to deny outright;
//! - errors follow the same asymmetry: the interactive gate degrades to "no
//!   opinion" (the dialog catches it), the headless gate fails CLOSED once a
//!   running bridge turn is established (nothing would catch it);
//! - away gates the interactive side only. Telegram starts headless turns
//!   with away off too (`/new`, a Reply while at the keyboard), and those
//!   turns have no terminal regardless of where the user is sitting;
//! - a headless gate engages only for a session with a RUNNING bridge turn.
//!   A terminal `--dangerously-skip-permissions` session — even one that ran
//!   a Telegram turn in the past — is left exactly as the user configured it.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::io::Read;
use std::time::Duration;

use crate::claude::{generate_session_uuid, truncate_tool_detail};
use crate::config::load_daemon_config;
use crate::state::{
    approval_auto_allowed, approval_decision, create_state_db, enqueue_outbound_event,
    insert_telegram_callback_route, remote_mode_status_path, set_approval_auto_allow,
    state_db_path, TelegramCallbackAction, TelegramCallbackRoute,
};

/// How often a blocked gate re-reads its answer row and the away marker.
/// Measured 2026-08-17 on a waiting gate: 500ms cost 0.17 ms/s of CPU,
/// 100ms costs 1.50 ms/s — and the process only exists while an approval is
/// actually pending, so even an hour-long wait totals a few seconds of CPU.
/// Worth it for a Telegram tap and `/back` landing in a tenth of a second
/// instead of half of one. (What renders during the block differs by hook:
/// the PERMISSION dialog runs concurrently with `PermissionRequest` —
/// re-verified 2026-08-28 under a pty capture — while `PreToolUse` shows
/// only a spinner, verified 2026-08-22. The terminal-side answer path each
/// gate has follows from that split.)
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long a gate waits when its session has NO terminal window (Claude
/// Code's background pty host). While AWAY IS ON, nobody is assumed to be
/// at any screen, and a background session's dialog is the easiest of all
/// to lose track of (measured 2026-08-17: 7h09m of a blocked cchess
/// session) — so its Telegram window stays open for a day rather than an
/// hour. `/back` is unaffected: it hands background gates over immediately
/// like everyone else's, and the dialog then renders on the bg pty where
/// the parent session's task panel surfaces it.
///
/// Not literally forever: the hook it runs inside has a timeout, so an
/// unbounded wait would be a fiction the harness overrules. A day is the
/// most `approvalTimeoutSeconds` can be configured to, and the hook timeout
/// (see `hooks::approval_hook_timeout_seconds`) is provisioned to outlast
/// exactly this.
pub(crate) const WINDOWLESS_APPROVAL_WAIT: Duration = Duration::from_secs(86_400);

// PRESENCE DETECTION IS GONE — deliberately, and it must not come back.
// Keyboard activity used to hand a waiting prompt to the terminal within a
// poll tick ("the machine in front of them wins over the phone in their
// pocket"). The user overruled that design outright (2026-08-27): "我需要
// 双向推送，不管人是否在电脑前，只要 away 开着，就双向推送。谁先抢答算谁的。"
// And the same day's production log showed why: pressing Yes on one terminal
// dialog was itself the keystroke that killed the NEXT approval's phone
// buttons seconds after they were delivered. Away mode is the one
// declaration; desktop activity decides nothing. The daemon's keyboard
// listener still maintains its activity file, unconsumed — removing the
// producer rides the xinput-orphan cleanup (v0.2.9 backlog).

/// Left only so `reset` can sweep files an older build wrote.
pub(crate) const PRESENCE_STATE_FILE: &str = "presence-probe.json";
pub(crate) const PRESENCE_LOCK_FILE: &str = "presence-probe.lock";

/// How much of the transcript tail is scanned at gate start for the pending
/// tool_use this approval is about. The record is normally the LAST line;
/// the margin covers a burst of parallel calls and sidechain noise.
const TERMINAL_ANSWER_TAIL_BYTES: u64 = 512 * 1024;

const TERMINAL_ANSWER_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Watches the transcript for THE tool_result that settles THIS approval.
///
/// The terminal's permission dialog renders CONCURRENTLY with a blocking
/// `PermissionRequest` hook (re-verified 2026-08-28 under a pty capture: a
/// 60s sleeping hook, dialog on screen at 40s with `hook.end` never
/// written), so the user can answer at the terminal while the phone holds
/// live buttons. Without this watch the gate never learns that — it holds,
/// and RENEWS, dead buttons for a command that already ran (the original
/// 2026-08-17 measurement: 21 orphaned gates, 58 stale pushes in one
/// afternoon; renewal would now multiply that hourly).
///
/// An earlier watcher was removed for a false positive: it fired on ANY
/// main-chain assistant record, so an actively-talking session closed a
/// fresh approval within seconds (measured 2026-08-27: a 1-hour window
/// dead at 6 seconds). This one is precise — it fires only when a
/// tool_result arrives for a tool_use whose name AND input match the gated
/// call, pending at (or appearing after) the moment the gate started.
struct TerminalAnswerWatch {
    transcript: Option<std::path::PathBuf>,
    tool_name: String,
    tool_input: Value,
    /// When the gate started (wall clock, ms). A matching tool_use only
    /// counts as THIS call if its record timestamp is within
    /// `TERMINAL_ANSWER_MATCH_WINDOW_MS` of this — the discriminator that
    /// keeps a HISTORIC identical call in the tail from being adopted (and
    /// its long-settled result from folding a fresh gate).
    gate_now_ms: u64,
    /// The one tool_use this gate is about. Adopted once — the newest
    /// recent pending match at seed, or the first recent match to appear in
    /// the increments — and never re-bound after that: re-binding to a
    /// later overlapping identical call was how gate A ended up folding on
    /// call B's result.
    tracked_id: Option<String>,
    /// Sticky: the tracked tool_use got its tool_result. Consulted (and the
    /// gate folded) on the next poll.
    answered: bool,
    /// Next byte to read. Failure handling is fail-closed throughout: a
    /// failed SEED read disables the watch loudly, a rotation/truncation
    /// resyncs to the end, and a transient incremental read error just
    /// skips that poll — the watch never reads bytes it cannot vouch for.
    offset: u64,
    identity: Option<(u64, u64)>,
    partial: Vec<u8>,
    last_checked: std::time::Instant,
}

#[cfg(unix)]
fn watch_file_identity(meta: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt as _;
    (meta.dev(), meta.ino())
}

#[cfg(not(unix))]
fn watch_file_identity(_meta: &std::fs::Metadata) -> (u64, u64) {
    (0, 0)
}

/// How far back a matching tool_use may be stamped and still count as the
/// call this gate is about. Generous: the hook fires within milliseconds of
/// the tool_use record; two minutes absorbs clock skew and slow flushes
/// while still excluding yesterday's identical command.
const TERMINAL_ANSWER_MATCH_WINDOW_MS: u64 = 120_000;

/// What one bounded seed read yields: (file length, identity, complete
/// lines, trailing partial line).
type SeedRead = (u64, (u64, u64), Vec<Vec<u8>>, Vec<u8>);

impl TerminalAnswerWatch {
    fn new(payload: &Value, gate_now_ms: u64) -> Self {
        let tool_name = payload
            .get("tool_name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let tool_input = payload.get("tool_input").cloned().unwrap_or(Value::Null);
        let mut watch = Self {
            transcript: payload
                .get("transcript_path")
                .and_then(Value::as_str)
                .map(std::path::PathBuf::from)
                .filter(|path| path.is_file()),
            tool_name,
            tool_input,
            gate_now_ms,
            tracked_id: None,
            answered: false,
            offset: 0,
            identity: None,
            partial: Vec::new(),
            last_checked: std::time::Instant::now(),
        };
        // Seed from the tail. The read is BOUNDED to the length just
        // measured: `read_to_end` would swallow bytes appended during the
        // read while `offset` stayed at the old length, so those bytes
        // would be consumed once here and then re-read as garbage.
        //
        // The seed BINDS ONLY A PENDING candidate. A recent matching call
        // that already has its result is ambiguous — it could be this call
        // answered impossibly fast, or simply the previous identical call —
        // and binding it once folded a fresh gate on a 30-second-old
        // answer. An unbound seed loses only the vanishing case where the
        // dialog was answered before this constructor ran; the reviewer
        // ruled that the acceptable residue.
        if let Some(path) = watch.transcript.clone() {
            use std::io::{Read as _, Seek as _};
            let seeded = (|| -> std::io::Result<SeedRead> {
                let mut file = std::fs::File::open(&path)?;
                let meta = file.metadata()?;
                let len = meta.len();
                let start = len.saturating_sub(TERMINAL_ANSWER_TAIL_BYTES);
                let mut tail = vec![0u8; (len - start) as usize];
                file.seek(std::io::SeekFrom::Start(start))?;
                file.read_exact(&mut tail)?;
                let mut skip_first = start > 0;
                let mut complete = Vec::new();
                let mut partial = Vec::new();
                for line in tail.split_inclusive(|byte| *byte == b'\n') {
                    if skip_first {
                        skip_first = false;
                        continue; // partial first line
                    }
                    if line.ends_with(b"\n") {
                        complete.push(line.to_vec());
                    } else {
                        partial = line.to_vec();
                    }
                }
                Ok((len, watch_file_identity(&meta), complete, partial))
            })();
            match seeded {
                Ok((len, identity, complete, partial)) => {
                    // Candidates in tail order: (id, answered-yet).
                    let mut candidates: Vec<(String, bool)> = Vec::new();
                    for line in &complete {
                        watch.scan_seed_line(line, &mut candidates);
                    }
                    // Newest PENDING match wins; an answered one binds nothing.
                    watch.tracked_id = candidates
                        .iter()
                        .rev()
                        .find(|(_, answered)| !answered)
                        .map(|(id, _)| id.clone());
                    watch.partial = partial;
                    watch.offset = len;
                    watch.identity = Some(identity);
                }
                Err(error) => {
                    // LOUD and fail-closed: pretending to watch a transcript
                    // that could not be read would silently swallow the
                    // terminal answer; the log line is the difference
                    // between "no fold happened" and "the watch was blind".
                    eprintln!(
                        "tinyctb approval-gate: transcript seed read failed for {} ({error}); \
                         terminal-answer detection is OFF for this gate",
                        path.display()
                    );
                    watch.transcript = None;
                }
            }
        }
        watch
    }

    /// Seed-phase line scan: collect matching tool_use candidates and mark
    /// the ones whose tool_result is already present. No binding happens
    /// here — the caller decides from the full picture.
    fn scan_seed_line(&self, line: &[u8], candidates: &mut Vec<(String, bool)>) {
        let text = String::from_utf8_lossy(line);
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }
        let Ok(entry) = serde_json::from_str::<Value>(trimmed) else {
            return;
        };
        if entry.get("isSidechain").and_then(Value::as_bool) == Some(true) {
            return;
        }
        let record_ts_ms = entry
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(crate::claude::transcript_timestamp_ms);
        let Some(content) = entry.pointer("/message/content").and_then(Value::as_array) else {
            return;
        };
        for item in content {
            match item.get("type").and_then(Value::as_str) {
                Some("tool_use") => {
                    if self.matches_this_call(item, record_ts_ms) {
                        if let Some(id) = item.get("id").and_then(Value::as_str) {
                            if !candidates.iter().any(|(known, _)| known == id) {
                                candidates.push((id.to_string(), false));
                            }
                        }
                    }
                }
                Some("tool_result") => {
                    if let Some(id) = item.get("tool_use_id").and_then(Value::as_str) {
                        for (known, answered) in candidates.iter_mut() {
                            if known == id {
                                *answered = true;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Is this tool_use item the call this gate is about? Name, input AND
    /// recency must all agree.
    fn matches_this_call(&self, item: &Value, record_ts_ms: Option<u64>) -> bool {
        if item.get("name").and_then(Value::as_str) != Some(self.tool_name.as_str())
            || item.get("input") != Some(&self.tool_input)
        {
            return false;
        }
        // No readable stamp: not adoptable. A guard that cannot date the
        // record must not bind the gate's fate to it.
        let Some(ts) = record_ts_ms else {
            return false;
        };
        ts.abs_diff(self.gate_now_ms) <= TERMINAL_ANSWER_MATCH_WINDOW_MS
    }

    /// Feed one incremental transcript line. Adoption is once-only: the
    /// first recent match binds an unbound gate, and the binding is final —
    /// a later overlapping identical call must not steal a bound gate.
    fn consume_line(&mut self, line: &[u8]) {
        let text = String::from_utf8_lossy(line);
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }
        let Ok(entry) = serde_json::from_str::<Value>(trimmed) else {
            return;
        };
        if entry.get("isSidechain").and_then(Value::as_bool) == Some(true) {
            return;
        }
        let record_ts_ms = entry
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(crate::claude::transcript_timestamp_ms);
        let Some(content) = entry.pointer("/message/content").and_then(Value::as_array) else {
            return;
        };
        for item in content {
            match item.get("type").and_then(Value::as_str) {
                Some("tool_use") => {
                    if self.matches_this_call(item, record_ts_ms) {
                        if let Some(id) = item.get("id").and_then(Value::as_str) {
                            if self.tracked_id.is_none() {
                                self.tracked_id = Some(id.to_string());
                                self.answered = false;
                            }
                        }
                    }
                }
                Some("tool_result") => {
                    if let (Some(id), Some(tracked)) = (
                        item.get("tool_use_id").and_then(Value::as_str),
                        self.tracked_id.as_deref(),
                    ) {
                        if id == tracked {
                            self.answered = true;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Sticky once true: the tracked tool_use has its tool_result — set by
    /// `consume_line` DIRECTLY in the increments (the seed only collects
    /// candidates and binds a pending one; it never sets sticky state, so a
    /// finished historic call cannot fold a fresh gate). Never inferred
    /// from before/after set comparisons: a tool_use and its result
    /// arriving in one batch left those empty-to-empty and the fold was
    /// missed forever.
    fn answered_in_terminal(&mut self) -> bool {
        if self.answered {
            return true;
        }
        let Some(path) = self.transcript.clone() else {
            return false;
        };
        if self.last_checked.elapsed() < TERMINAL_ANSWER_CHECK_INTERVAL {
            return false;
        }
        self.last_checked = std::time::Instant::now();

        use std::io::{Read as _, Seek as _};
        let Ok(mut file) = std::fs::File::open(&path) else {
            return false;
        };
        let Ok(meta) = file.metadata() else {
            return false;
        };
        let len = meta.len();
        let identity = Some(watch_file_identity(&meta));
        if identity != self.identity || len < self.offset {
            // Rotated or truncated: resync to the end. Bytes written during
            // the gap are unseen — fail-closed, the watch just stops
            // vouching for that window.
            self.offset = len;
            self.identity = identity;
            self.partial.clear();
            return false;
        }
        if len == self.offset {
            return false;
        }
        if file.seek(std::io::SeekFrom::Start(self.offset)).is_err() {
            return false;
        }
        let mut fresh = Vec::new();
        let Ok(read) = file.read_to_end(&mut fresh) else {
            return false;
        };
        self.offset += read as u64;
        let mut buffer = std::mem::take(&mut self.partial);
        buffer.extend_from_slice(&fresh);
        let mut consumed = 0usize;
        let mut complete = Vec::new();
        for line in buffer.split_inclusive(|byte| *byte == b'\n') {
            if !line.ends_with(b"\n") {
                break;
            }
            consumed += line.len();
            complete.push(line.to_vec());
        }
        self.partial = buffer[consumed..].to_vec();
        for line in complete {
            self.consume_line(&line);
        }
        self.answered
    }
}

/// "No opinion": the normal permission flow decides. This is the answer for
/// every path that is not an explicit remote allow/deny.
fn no_opinion() -> Value {
    json!({})
}

/// Which hook is asking. Both gates run the same request/answer path and
/// differ only at the two edges: whose tool calls they engage for, and what
/// silence means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateKind {
    /// A session the user launched. `PermissionRequest` fires only when a
    /// decision is genuinely needed, and an unanswered request falls through
    /// to the terminal dialog that was going to appear anyway.
    Interactive,
    /// A headless turn the bridge started for Telegram. `PermissionRequest`
    /// does not fire in `-p` mode at all, so the gate hangs off `PreToolUse`;
    /// and because the turn runs under `bypassPermissions` with no terminal
    /// behind it, silence must DENY — falling through would run the call.
    Headless,
}

impl GateKind {
    fn allow(self, reason: &str) -> Value {
        match self {
            GateKind::Interactive => json!({
                "hookSpecificOutput": {
                    "hookEventName": "PermissionRequest",
                    "decision": { "behavior": "allow" },
                    "permissionDecisionReason": reason
                }
            }),
            GateKind::Headless => json!({
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "allow",
                    "permissionDecisionReason": reason
                }
            }),
        }
    }

    fn deny(self, message: &str) -> Value {
        match self {
            GateKind::Interactive => json!({
                "hookSpecificOutput": {
                    "hookEventName": "PermissionRequest",
                    "decision": { "behavior": "deny", "message": message }
                }
            }),
            GateKind::Headless => json!({
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": message
                }
            }),
        }
    }
}

/// Cheap away check that avoids opening the database: while the user is at
/// the keyboard the gate must cost almost nothing. The marker file is
/// written whenever away mode changes.
fn away_mode_active() -> bool {
    let Ok(path) = remote_mode_status_path() else {
        return false;
    };
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|value| value.get("away").and_then(Value::as_bool))
        .unwrap_or(false)
}

/// UserPromptSubmit hook: while the user is AWAY, teach the session how to
/// ask them things. An interactive session must use the AskUserQuestion
/// TOOL (Telegram renders it as buttons); prose questions at the end of a
/// turn arrive as a plain notification nobody can tap. A headless turn has
/// no such tool at all (measured 2026-08-12) — it gets the opposite
/// instruction: list the options at the end and wait for the reply.
/// At the keyboard (away off) this prints nothing and costs nothing.
pub(crate) fn run_prompt_context<R: Read>(reader: &mut R) -> Value {
    // Drain stdin so the hook pipe closes cleanly; the payload itself is
    // not needed — the decision keys on away mode and the turn token.
    let mut raw = String::new();
    let _ = reader.take(1024 * 1024).read_to_string(&mut raw);
    if !away_mode_active() {
        return json!({});
    }
    let headless = std::env::var(crate::claude::BRIDGE_TURN_ENV)
        .map(|token| !token.is_empty())
        .unwrap_or(false);
    let context = if headless {
        "用户不在电脑前，正通过 Telegram 手机遥控本会话。本环境没有 AskUserQuestion 工具：需要用户决策时不要猜测——把问题放在回答结尾，清晰列出编号选项，用户会通过 Telegram 回复作答后你再继续。"
    } else {
        "用户不在电脑前，正通过 Telegram 手机遥控本会话。需要用户做选择、确认或决策时，必须调用 AskUserQuestion 工具提问（手机上会渲染成可点按钮），不要只在正文里问——正文里的提问在手机上没有按钮可点。是/否类确认同样请用该工具。"
    };
    json!({
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": context
        }
    })
}

pub(crate) fn run_approval_gate<R: Read>(reader: &mut R, now: u64) -> Result<Value> {
    let payload = read_hook_payload(reader, "PermissionRequest")?;
    gate_tool_call(&payload, GateKind::Interactive, now)
}

/// Remote approvals for a headless turn — the calls the interactive gate can
/// never see.
///
/// Three measured facts force this second entry point (see docs/approvals.md):
/// `PermissionRequest` does not fire in `-p` mode; `--permission-mode default`
/// there does not prompt but has the sandbox refuse outright, which is why
/// bridge turns run under `bypassPermissions`; and `PreToolUse` *does* fire,
/// carrying the same `tool_name` / `tool_input` / `tool_use_id` payload.
///
/// So the very turns the user starts from their phone — the ones they are
/// least able to watch — were the only ones running with nothing in the way.
///
/// Infallible by design, and the error direction is the opposite of the
/// interactive gate's. There, an error degrading to "no opinion" hands the
/// call to the terminal dialog — safe. Here, "no opinion" IS execution: the
/// turn runs under `bypassPermissions` and nothing else will ask. So any
/// internal failure denies, loudly, with the error in the reason. The blast
/// radius of failing closed is confined by the checks that precede the
/// fallible work: a call that is not `bypassPermissions`, or that carries no
/// turn token in its environment, has already returned "no opinion" before
/// anything that can break (database, config, approvals, Telegram) is
/// touched.
pub(crate) fn run_headless_approval_gate<R: Read>(reader: &mut R, now: u64) -> Value {
    let attempt = read_hook_payload(reader, "PreToolUse")
        .and_then(|payload| gate_tool_call(&payload, GateKind::Headless, now));
    match attempt {
        Ok(value) => value,
        Err(err) => {
            // Loud on both channels: stderr for the hook debug log, and the
            // deny reason for the transcript the user will read.
            eprintln!("tinyctb headless-approval-gate failed closed: {err:#}");
            GateKind::Headless.deny(&format!(
                "tinyctb could not process this approval ({err:#}), so the call was \
                 blocked — a headless turn has no other check. Do not retry; tell \
                 the user what you were about to do and stop."
            ))
        }
    }
}

/// Timestamp for SETTLE stamps. The gate's `now` parameter is frozen at
/// hook start and reused across the whole window — fine for identifiers and
/// deadlines, but stamping a settle that happens minutes later with the
/// START time collapses the two instants: the 2026-08-22 forensics read a
/// hand-back as "expired the same millisecond it was born". The stamp is
/// `now` plus the MONOTONIC time this gate has actually been running — the
/// wall clock never re-enters, so a clock that jumps backwards mid-wait
/// cannot reorder the stamps — and the floor of 1ms keeps the ordering
/// strict even for a settle inside the first millisecond.
fn settle_stamp_ms(gate_started: u64, running_for: Duration) -> u64 {
    gate_started.saturating_add((running_for.as_millis() as u64).max(1))
}

fn read_hook_payload<R: Read>(reader: &mut R, event: &str) -> Result<Value> {
    let mut raw = String::new();
    reader
        .take(1024 * 1024)
        .read_to_string(&mut raw)
        .with_context(|| format!("failed to read {event} payload"))?;
    serde_json::from_str(raw.trim()).with_context(|| format!("{event} payload is not valid JSON"))
}

fn gate_tool_call(payload: &Value, kind: GateKind, now: u64) -> Result<Value> {
    // Away gates the INTERACTIVE side only: at the keyboard, the terminal
    // dialog is right there and remote approval would be noise. A headless
    // turn has no terminal wherever the user is sitting — Telegram can start
    // one with away off (`/new`, or a Reply while present) — so its gate
    // must not depend on away at all.
    //
    // Away is the ONE declaration, for BACKGROUND sessions too. The window
    // probe answers "how is this session hosted", not "is anyone watching":
    // a task forked into the daemon's bg-pty-host stays "windowless" forever
    // even while its parent session sits in an attended terminal whose task
    // panel surfaces and answers its dialogs. Exempting such sessions from
    // the away shortcut (the old rule) meant /back still pushed their
    // questions to the phone — measured 2026-08-28, reported by the user as
    // "back should behave as if tinyCTB were not running". With away off,
    // EVERY interactive gate steps aside; a background task then waits in
    // its own pty exactly as it would without tinyctb.
    //
    // The terminal-answer boundary is frozen FIRST — before the windowless
    // `/proc` walk, before config and the database. Everything after this
    // line takes time in which the concurrent terminal dialog can be
    // answered, and an answer landing before the boundary would be
    // invisible to the watch forever.
    let mut terminal_watch =
        (kind == GateKind::Interactive).then(|| TerminalAnswerWatch::new(payload, now));
    let session_window =
        (kind == GateKind::Interactive).then(crate::claude::current_session_window);
    let windowless = session_window == Some(crate::claude::SessionWindow::Background);
    if kind == GateKind::Interactive && !away_mode_active() {
        return Ok(no_opinion());
    }
    let tool_name = payload
        .get("tool_name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if tool_name.is_empty() {
        return match kind {
            GateKind::Interactive => Ok(no_opinion()),
            // The matcher restricts this hook to gated tools; a payload with
            // no tool name is malformed, and malformed fails closed here.
            GateKind::Headless => Err(anyhow::anyhow!("PreToolUse payload has no tool_name")),
        };
    }
    // `bypassPermissions` splits the two gates cleanly. An interactive
    // session in that mode raises no permission prompt, so the interactive
    // gate would never hear about the call anyway; a headless turn is in
    // that mode by construction.
    let bypassing =
        payload.get("permission_mode").and_then(Value::as_str) == Some("bypassPermissions");
    if bypassing != (kind == GateKind::Headless) {
        return Ok(no_opinion());
    }
    let thread_id = payload
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if thread_id.is_empty() {
        return match kind {
            GateKind::Interactive => Ok(no_opinion()),
            GateKind::Headless => Err(anyhow::anyhow!("PreToolUse payload has no session_id")),
        };
    }
    // The headless gate's admission check comes BEFORE config or any approval
    // work, deliberately early, and its first layer touches NO fallible
    // state at all: the turn token the bridge stamps into the environment of
    // every process it spawns (hooks inherit it through the claude process).
    //
    // - No token -> not a bridge process. A terminal
    //   `--dangerously-skip-permissions` session is waved past here even
    //   when tinyctb's own state directory is unreadable — an environment
    //   read cannot fail the way a database open can, so "bridge fails
    //   closed" can never spill onto a session the bridge does not own.
    // - The token names one turn, and it exists only inside that turn's
    //   process tree, so a session that ran a Telegram turn last week cannot
    //   be haunted by it either.
    // - Token holders ARE bridge processes: everything from here on fails
    //   closed. The database is consulted second, to check the named turn is
    //   still running — a straggler call from a turn the daemon has already
    //   settled (timed out, killed) gets a deny, not a fresh approval.
    // The spawn registers the turn before the process exists, so there is no
    // first-call race to lose.
    let mut admitted_conn = None;
    if kind == GateKind::Headless {
        let token = std::env::var(crate::claude::BRIDGE_TURN_ENV).unwrap_or_default();
        if token.is_empty() {
            return Ok(no_opinion());
        }
        let conn = create_state_db(&state_db_path()?)?;
        match crate::state::bridge_turn_status(&conn, &token)?.as_deref() {
            Some("running") => {}
            status => {
                return Ok(kind.deny(&format!(
                    "this bridge turn is {}, so its approval window is closed and \
                     {tool_name} was not run. Do not retry; stop.",
                    status.unwrap_or("unknown to the bridge")
                )))
            }
        }
        admitted_conn = Some(conn);
    }
    let config = match load_daemon_config() {
        Ok(config) => config,
        Err(err) => {
            return match kind {
                // Unconfigured bridge: stay out of the way entirely.
                GateKind::Interactive => Ok(no_opinion()),
                // A running bridge turn with unreadable config cannot be
                // waved through — that would execute the call unchecked.
                GateKind::Headless => {
                    Err(err.context("cannot load config while gating a headless call"))
                }
            };
        }
    };
    let claude = config.claude.clone().unwrap_or_default();
    let gated = match kind {
        // The event already means "this call needs an answer", so an empty
        // list gates everything that asks. A non-empty list is an explicit
        // narrowing for people who only want to be bothered about some tools.
        GateKind::Interactive => {
            claude.approval_tools.is_empty()
                || claude
                    .approval_tools
                    .iter()
                    .any(|gated| gated == &tool_name)
        }
        // `PreToolUse` fires for EVERY call, so the list is the whole filter
        // and an empty one is the off switch — the opposite reading, for the
        // opposite reason. See the config field's doc comment.
        GateKind::Headless => claude
            .headless_approval_tools
            .iter()
            .any(|gated| gated == &tool_name),
    };
    if !gated {
        return Ok(no_opinion());
    }
    let Some(telegram) = config.telegram.clone() else {
        return Ok(no_opinion());
    };

    // --- database-backed path ---------------------------------------------
    let conn = match admitted_conn {
        Some(conn) => conn,
        None => create_state_db(&state_db_path()?)?,
    };
    // Session-scoped auto-allow first: the user already granted this, and
    // nothing downstream may resurrect a prompt they paid a tap to silence.
    if approval_auto_allowed(&conn, &thread_id, &tool_name)? {
        // The session grant is standing, but its CONSUMPTION still passes
        // the final owner re-check: a `/stop` that already committed must
        // not have tools riding a pre-stop grant.
        if kind == GateKind::Headless && !consume_headless_authorization(&conn)? {
            return Ok(kind.deny(&format!(
                "this bridge turn was stopped, so the session grant for {tool_name} \
                 no longer applies and it was not run. Do not retry; stop."
            )));
        }
        return Ok(kind.allow(&format!(
            "{tool_name} was approved for this session from Telegram"
        )));
    }
    // While away is on, EVERY gated call gets its buttons pushed to
    // Telegram, and they stay live for the whole hold — desktop activity
    // decides nothing (see the presence-removal note at the top of this
    // file), and the terminal can answer its own concurrent dialog at any
    // time (the watch above folds the phone side when it does). Away OFF
    // means the terminal owns it outright, background sessions included —
    // the away shortcut at the top of this function already returned.

    // Monotonic gate age for settle stamps — see `settle_stamp_ms`.
    let gate_clock = std::time::Instant::now();
    // A stable id exists only for `PreToolUse` (headless): a real
    // `PermissionRequest` payload carries no tool_use_id, so an interactive
    // gate mints a fresh UUID on every run.
    let approval_id = payload
        .get("tool_use_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| generate_session_uuid().unwrap_or_else(|_| now.to_string()));
    let summary = tool_call_summary(&tool_name, payload.get("tool_input"));
    let wait = if windowless {
        WINDOWLESS_APPROVAL_WAIT
    } else {
        Duration::from_secs(claude.approval_timeout_seconds.clamp(5, 86_400))
    };
    let owning_turn = std::env::var(crate::claude::BRIDGE_TURN_ENV).ok();
    // A windowed interactive gate whose hour runs out while away is still on
    // has NOT been answered — it has been ignored by a phone whose buttons
    // then died. Going quiet at that point wedged a session for seven hours
    // (measured 2026-08-27: dialog raised 03:07, buttons dead 04:07, user
    // back 11:18, zero messages in between). So the window RENEWS: each
    // round settles atomically — a tap that landed wins — and an unanswered
    // round while away is still on publishes a fresh request with live
    // buttons, which doubles as the periodic "this session is still blocked"
    // reminder that was missing. The overall budget matches the installed
    // hook timeout; past it the hook dies anyway and silence is honest.
    let renewal_deadline = std::time::Instant::now() + WINDOWLESS_APPROVAL_WAIT;
    // The hold can now last hours (renewals). The permission dialog itself
    // renders concurrently with this hook and stays answerable, but WHY the
    // session also has buttons on a phone deserves a line of its own — same
    // law, same paint machinery as the question gate.
    let banner = QuestionBanner::for_session(windowless);
    let mut round: u32 = 0;
    loop {
        // Round 0 keeps the bare id (the tool_use_id, so a hook re-run is
        // idempotent); every renewal gets a fresh `:rN` so its Telegram
        // message carries live buttons while the lapsed rounds report as
        // expired.
        let round_id = if round == 0 {
            approval_id.clone()
        } else {
            format!("{approval_id}:r{round}")
        };
        let round_summary = if round == 0 {
            summary.clone()
        } else {
            format!("{summary}\n（第 {} 次提醒，此前的按钮已过期）", round + 1)
        };
        let round_now = settle_stamp_ms(now, gate_clock.elapsed());
        match publish_approval_request(
            &conn,
            &telegram.chat_id,
            &round_id,
            &thread_id,
            &tool_name,
            &round_summary,
            kind == GateKind::Headless,
            session_window,
            owning_turn.as_deref(),
            payload.get("cwd").and_then(Value::as_str),
            round_now,
            round_now + wait.as_millis() as u64,
        )? {
            // Fresh, or already open from an interrupted run of this same
            // gate: either way the row is live and the wait below owns it.
            Publication::Published | Publication::AlreadyPublished => {}
            // Republished after a settled row: hand the decision to the
            // per-kind interpreter — a real answer is honoured (never
            // re-asked: reopening is how a stale sibling button once flipped
            // a deny into an allow), and `expired` resolves the way a
            // timeout does (headless: deny; interactive: the terminal's own
            // prompt). Only a re-run can land here, and only a HEADLESS
            // re-run can in practice: `PreToolUse` carries a stable
            // tool_use_id, while a real `PermissionRequest` payload has
            // none, so an interactive gate mints a fresh UUID every run and
            // never collides. (An earlier revision "advanced the renewal
            // chain" here instead — built on the wrong premise that
            // interactive re-runs share an id, and worse, its bail-out
            // returned `{}` even for headless, which bypassPermissions reads
            // as run-it.)
            Publication::AlreadyDecided(decision) => {
                return apply_decision(&conn, kind, &thread_id, &tool_name, &decision, now);
            }
            // The admission check saw `running`, but `/stop` committed in
            // between: publication re-verifies the owner INSIDE its
            // transaction, and a refusal means the stop won.
            Publication::OwnerNotRunning => {
                return Ok(kind.deny(&format!(
                    "this bridge turn is being stopped, so its approval window is closed and \
                     {tool_name} was not run. Do not retry; stop."
                )));
            }
        }
        if round == 0 {
            banner.paint_note(&format!(
                "🔐 {tool_name} 等待审批：手机按钮或本终端对话框均可作答，先答先得"
            ));
        } else {
            banner.paint_note(&format!("🔁 审批续期第 {} 轮，手机按钮已更新", round + 1));
        }
        match wait_out_approval_window(
            &conn,
            kind,
            &thread_id,
            &tool_name,
            &round_id,
            wait,
            renewal_deadline,
            now,
            &gate_clock,
            terminal_watch.as_mut(),
        )? {
            WindowOutcome::Settled(value, via) => {
                banner.paint_note(match via {
                    SettledVia::Decision => "✔ 审批已有决定（手机按钮或已录决定）",
                    SettledVia::HandBack => "↩ 已交还本终端，对话框即将弹出",
                    SettledVia::TerminalAnswer => "✔ 本终端对话框已作答，手机按钮已收回",
                });
                return Ok(value);
            }
            // This round ran out with nobody answering. Renewal is for the
            // one configuration that would otherwise go silent while its
            // terminal dialog blocks unseen: a WINDOWED INTERACTIVE session
            // with away still on. Everything else keeps its single window —
            // windowless already waits the whole budget, a headless timeout
            // must deny, and away-off means the terminal owns the prompt.
            WindowOutcome::Unanswered => {
                let renewable = kind == GateKind::Interactive
                    && !windowless
                    && away_mode_active()
                    && std::time::Instant::now() < renewal_deadline;
                if !renewable {
                    banner.paint_note("⌛ 手机未作答，交还本终端对话框");
                    return Ok(match kind {
                        // An interactive session still has its own prompt to
                        // fall back on, which is exactly what "no opinion"
                        // hands it.
                        GateKind::Interactive => no_opinion(),
                        // A headless turn (proved running by the admission
                        // check) has no dialog behind it and
                        // `bypassPermissions` underneath: falling through
                        // would RUN the call. Silence has to deny outright.
                        GateKind::Headless => kind.deny(&format!(
                            "Nobody approved this from Telegram in time, so {tool_name} was \
                             not run. Do not retry it; tell the user what you were about to \
                             do and stop."
                        )),
                    });
                }
                round += 1;
            }
        }
    }
}

/// What one approval window came to.
enum WindowOutcome {
    /// The gate is finished. The value is the hook's reply; `via` names the
    /// exit so the banner can say what actually happened.
    Settled(Value, SettledVia),
    /// The window expired with no answer anywhere. The caller decides
    /// whether that ends the gate or renews the request.
    Unanswered,
}

/// Which exit finished the gate — three genuinely different stories the
/// terminal banner must not conflate.
enum SettledVia {
    /// A recorded decision (a phone tap, or a decision another actor wrote).
    Decision,
    /// `/back`: the user reclaimed the terminal; the dialog appears next.
    HandBack,
    /// The user answered the CONCURRENT terminal dialog; the phone side was
    /// folded.
    TerminalAnswer,
}

/// Poll out one approval window. Every early exit settles atomically — a tap
/// that already landed always wins over the exit that raced it.
#[allow(clippy::too_many_arguments)]
fn wait_out_approval_window(
    conn: &rusqlite::Connection,
    kind: GateKind,
    thread_id: &str,
    tool_name: &str,
    approval_id: &str,
    wait: Duration,
    renewal_deadline: std::time::Instant,
    now: u64,
    gate_clock: &std::time::Instant,
    mut terminal_watch: Option<&mut TerminalAnswerWatch>,
) -> Result<WindowOutcome> {
    let deadline = (std::time::Instant::now() + wait).min(renewal_deadline);
    while std::time::Instant::now() < deadline {
        std::thread::sleep(POLL_INTERVAL);
        if let Some(decision) = approval_decision(conn, approval_id)? {
            if decision != "expired" {
                return apply_decision(conn, kind, thread_id, tool_name, &decision, now)
                    .map(|value| WindowOutcome::Settled(value, SettledVia::Decision));
            }
            // Someone else closed this window — for a headless turn that is
            // `/stop` sweeping the dialogs the turn owns. Polling out the
            // remaining window (up to a day) would hold the tool blocked
            // for a turn the user already ended; the honest ending is the
            // same one a timeout reaches, now.
            if kind == GateKind::Headless {
                return Ok(WindowOutcome::Settled(
                    kind.deny(&format!(
                        "this approval was closed by the bridge (the turn is being \
                         stopped), so {tool_name} was not run. Do not retry; stop."
                    )),
                    SettledVia::Decision,
                ));
            }
        }
        // The ONE hand-back: the user declared themselves back (`/back`
        // turned away off). Nothing else releases the phone — the user's
        // law (2026-08-27): "只要 away 开着，就双向推送，谁先抢答算谁的。"
        // Keyboard activity used to hand the prompt over here, and it
        // backfired in production the same day: the user pressed Yes on one
        // terminal dialog, and that very keystroke killed the NEXT
        // approval's phone buttons seconds after they were pushed. Desktop
        // activity is not a declaration; away is. Settling stays atomic —
        // a tap that already landed still wins.
        //
        // Background sessions hand back the same way: their dialog renders
        // on the bg pty and the parent session's task panel surfaces it —
        // away off means the terminal side owns everything (measured
        // 2026-08-28: exempting them here kept pushing questions to the
        // phone after /back).
        if kind == GateKind::Interactive && !away_mode_active() {
            // Settle AND withdraw the queued push in one step: approval
            // pushes ride origin="bridge", which the /back sweep leaves
            // alone by design, so an unsent retry would still reach the
            // phone after away turned off — the very silence /back declares.
            return match crate::state::settle_expired_and_cancel_push(
                conn,
                approval_id,
                settle_stamp_ms(now, gate_clock.elapsed()),
            )? {
                crate::state::SettleOutcome::Answered => {
                    let decision = crate::state::approval_decision(conn, approval_id)?
                        .unwrap_or_else(|| "expired".to_string());
                    apply_decision(conn, kind, thread_id, tool_name, &decision, now)
                        .map(|value| WindowOutcome::Settled(value, SettledVia::Decision))
                }
                crate::state::SettleOutcome::Expired => {
                    Ok(WindowOutcome::Settled(no_opinion(), SettledVia::HandBack))
                }
            };
        }
        // The user answered the CONCURRENT terminal dialog (it renders while
        // this hook blocks — re-verified 2026-08-28). The call is decided;
        // holding (and renewing) its phone buttons would ship dead buttons
        // for a command that already ran. Settling is atomic with any
        // in-flight tap, and cancelling the unsent push rides the same
        // transaction. INTERACTIVE ONLY: under `bypassPermissions` a
        // headless turn reads `{}` as "nobody objected" and runs the tool.
        if kind == GateKind::Interactive
            && terminal_watch
                .as_deref_mut()
                .is_some_and(TerminalAnswerWatch::answered_in_terminal)
        {
            return match crate::state::settle_expired_and_cancel_push(
                conn,
                approval_id,
                settle_stamp_ms(now, gate_clock.elapsed()),
            )? {
                crate::state::SettleOutcome::Answered => {
                    let decision = crate::state::approval_decision(conn, approval_id)?
                        .unwrap_or_else(|| "expired".to_string());
                    apply_decision(conn, kind, thread_id, tool_name, &decision, now)
                        .map(|value| WindowOutcome::Settled(value, SettledVia::Decision))
                }
                crate::state::SettleOutcome::Expired => Ok(WindowOutcome::Settled(
                    no_opinion(),
                    SettledVia::TerminalAnswer,
                )),
            };
        }
    }
    // The window ran out. Settling the record and taking a decision is one
    // atomic step: if a tap landed in the instant between the last poll and
    // here, that answer wins and must be honoured — otherwise Telegram would
    // show "已允许" while the session quietly fell back to its own prompt.
    match crate::state::expire_or_take_decision(
        conn,
        approval_id,
        settle_stamp_ms(now, gate_clock.elapsed()),
    )? {
        Some(decision) => apply_decision(conn, kind, thread_id, tool_name, &decision, now)
            .map(|value| WindowOutcome::Settled(value, SettledVia::Decision)),
        // Nobody answered anywhere. Whether that ends the gate or renews the
        // request is the caller's ruling, not this window's.
        None => Ok(WindowOutcome::Unanswered),
    }
}

/// `AskUserQuestion` asked from Telegram. Registered as a `PreToolUse` hook
/// with `matcher: "AskUserQuestion"`, because no hook can take over the
/// question dialog itself and `PermissionRequest` never fires for it (asking
/// is not a permission-gated action).
///
/// The answer goes back through the tool's own contract — `allow` plus an
/// `updatedInput` carrying an `answers` map — so the call completes with the
/// answer as its result. The `allow` grants nothing: asking has no side
/// effect, and permission still requires an approval button.
///
/// The tool's own dialog offers buttons AND a free-text box, so Telegram
/// mirrors both: one button per option, plus "reply with text" for anything
/// else — a custom answer, a skip, an ordering like `3,1,2`, or the
/// comma-separated form a multi-select question needs.
pub(crate) fn run_question_gate<R: Read>(reader: &mut R, now: u64) -> Result<Value> {
    let mut raw = String::new();
    reader
        .take(1024 * 1024)
        .read_to_string(&mut raw)
        .context("failed to read PreToolUse payload")?;
    let payload: Value =
        serde_json::from_str(raw.trim()).context("PreToolUse payload is not valid JSON")?;

    // Same rule as the approval gate: away is the one declaration, for
    // background sessions too (see the note there — the window probe
    // measures hosting, not attendance).
    let session_window = crate::claude::current_session_window();
    let windowless = session_window == crate::claude::SessionWindow::Background;
    if !away_mode_active() {
        return Ok(no_opinion());
    }
    if payload.get("tool_name").and_then(Value::as_str) != Some("AskUserQuestion") {
        return Ok(no_opinion());
    }
    if payload.get("permission_mode").and_then(Value::as_str) == Some("bypassPermissions") {
        return Ok(no_opinion());
    }
    let Ok(config) = load_daemon_config() else {
        return Ok(no_opinion());
    };
    let Some(telegram) = config.telegram.clone() else {
        return Ok(no_opinion());
    };
    let thread_id = payload
        .get("session_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if thread_id.is_empty() {
        return Ok(no_opinion());
    }
    // Several questions in one call would need several round trips; that is
    // not worth the complexity here, so leave those to the terminal dialog.
    let questions = payload
        .pointer("/tool_input/questions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if questions.len() != 1 {
        return Ok(no_opinion());
    }
    // The answers map is keyed by the question text EXACTLY as the tool sent
    // it; a trimmed key would not match and the tool would still consider the
    // question unanswered. Trimming is for display only.
    let question_raw = questions[0]
        .get("question")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let question_text = question_raw.trim().to_string();
    if question_text.is_empty() {
        return Ok(no_opinion());
    }
    // A multi-select question cannot be answered by a single tap — the first
    // button press would submit and lose the rest. Those are answered by a
    // comma-separated reply instead (which is the shape the tool wants).
    let multi_select = questions[0]
        .get("multiSelect")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let options = questions[0]
        .get("options")
        .and_then(Value::as_array)
        .map(|options| {
            options
                .iter()
                .filter_map(|option| option.get("label").and_then(Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let claude = config.claude.clone().unwrap_or_default();
    // A windowless session gets the long window for the same reason the
    // approval gate does: there is no terminal dialog to fall back to, so
    // expiring early hides the question instead of relocating it.
    let wait = if windowless {
        WINDOWLESS_APPROVAL_WAIT
    } else {
        Duration::from_secs(claude.approval_timeout_seconds.clamp(5, 86_400))
    };
    // Same rule as the approval gate: away on means the question is pushed
    // to Telegram and its buttons stay live for the whole hold — desktop
    // activity decides nothing. Away off means the terminal owns it,
    // background sessions included (the away shortcut at the top of this
    // function already returned).

    // Monotonic gate age for settle stamps — see `settle_stamp_ms`.
    let gate_clock = std::time::Instant::now();
    let conn = create_state_db(&state_db_path()?)?;
    let question_id = format!("q{}", generate_session_uuid()?.replace('-', ""));
    crate::state::create_pending_question(
        &conn,
        &question_id,
        &thread_id,
        &question_text,
        &options,
        multi_select,
        now,
        now + wait.as_millis() as u64,
    )?;

    let buttons = if multi_select {
        Vec::new()
    } else {
        question_answer_buttons(
            &conn,
            &telegram.chat_id,
            &thread_id,
            &question_id,
            &options,
            now,
        )?
    };

    let cwd = payload.get("cwd").and_then(Value::as_str);
    let body = question_body(&question_text, &options, multi_select);
    let event = json!({
        "type": "question_request",
        "threadId": thread_id,
        "questionId": question_id,
        "observedAt": now,
        "eventKey": format!("question:{question_id}"),
        "lastPreview": body,
        "buttons": buttons,
        // Measured here, first-hand, and carried to the phone: a background
        // session's terminal cannot show this question, so the message must
        // not let the reader assume a dialog is waiting for them at home.
        // A failed probe says so rather than defaulting to "window" — the
        // wait still follows the old policy, but the wording must not.
        "terminalVisibility": terminal_visibility(session_window),
        "thread": {
            "threadId": thread_id,
            "cwd": cwd,
            "project": crate::projects::derive_project_label(cwd),
            "lastPreview": body
        }
    });
    enqueue_outbound_event(&conn, &event, now, "bridge")?;
    // The push exists; now make the wait VISIBLE where the user's law says
    // it must be — in the terminal. `PreToolUse` (unlike the permission
    // hook) renders nothing while it blocks — spinner only, verified
    // 2026-08-22 — so the banner is the only trace a person at the screen
    // gets that anything is waiting, and /back is their answer path.
    let banner = QuestionBanner::for_session(windowless);
    banner.paint_question(&question_text, &options, multi_select);
    let phone_answered = |answer: &str| {
        let resolved = resolve_answer(answer, &options);
        banner.paint_note(&format!("✔ 已由手机作答：{resolved}"));
        answered(&questions, &question_raw, &resolved)
    };
    let deadline = std::time::Instant::now() + wait;
    while std::time::Instant::now() < deadline {
        std::thread::sleep(POLL_INTERVAL);
        if let Some(answer) = crate::state::question_answer(&conn, &question_id)? {
            if answer != crate::state::QUESTION_EXPIRED {
                return Ok(phone_answered(&answer));
            }
        }
        // The ONE hand-back: `/back` turned away off. A per-session
        // keypress reclaim was tried and is OFF THE TABLE for lack of a
        // signal: the pts atime does not move on real keyboard reads
        // (devpts probe, 2026-08-28) and typed bytes never queue in the
        // kernel (FIONREAD probe, same day — the TUI drains stdin even
        // while the hook blocks). The one signal that exists is the desktop
        // keyboard, and a global keystroke killing every session's buttons
        // is the exact bug the user outlawed. The exit honours any answer
        // that raced in first.
        if !away_mode_active() {
            return match crate::state::settle_expired_question_and_cancel_push(
                &conn,
                &question_id,
                settle_stamp_ms(now, gate_clock.elapsed()),
            )? {
                Some(answer) => Ok(phone_answered(&answer)),
                None => {
                    banner.paint_note("↩ 已交还本终端，对话框即将弹出");
                    Ok(no_opinion())
                }
            };
        }
    }
    match crate::state::settle_expired_question_and_cancel_push(
        &conn,
        &question_id,
        settle_stamp_ms(now, gate_clock.elapsed()),
    )? {
        Some(answer) => Ok(phone_answered(&answer)),
        None => {
            banner.paint_note("⌛ 手机未作答，交还本终端对话框");
            Ok(no_opinion())
        }
    }
}

/// Best-effort visibility for the blocked question window, written straight
/// onto the session's terminal device.
///
/// While this gate blocks, Claude Code renders NOTHING for the pending
/// question — not the dialog, not even the tool name, just a spinner
/// (verified 2026-08-22 under tmux with a sleeping `PreToolUse` hook; the
/// blank mahjong-training terminal that prompted this design was the same
/// observation in production). The TUI only repaints its spinner line in
/// place during the block, so bytes written to the pts stay on screen for
/// the whole window; they are not tracked scrollback, and the native dialog
/// chews them once the hook exits — acceptable, because by then the dialog
/// itself is the visible thing.
///
/// Every write is best-effort and LOUD on failure, but never fatal: the
/// Telegram window must survive a tty-less world (SSH, redirected fds), and
/// a paint failure that silently killed the gate would trade a cosmetic gap
/// for a lost question.
struct QuestionBanner {
    tty: Option<std::path::PathBuf>,
}

impl QuestionBanner {
    fn for_session(windowless: bool) -> Self {
        // A windowless session has no terminal anyone watches; painting its
        // hidden pty would be writing to nobody and the Telegram window is
        // already the only real dialog.
        if windowless {
            return Self { tty: None };
        }
        let tty = crate::claude::session_tty_path();
        if tty.is_none() {
            eprintln!(
                "tinyctb question-gate: session tty not found; the terminal stays blank while \
                 the question waits on Telegram"
            );
        }
        Self { tty }
    }

    fn write(&self, text: &str) {
        let Some(tty) = &self.tty else { return };
        if let Err(err) = write_bounded(tty, text.as_bytes(), BANNER_WRITE_BUDGET) {
            eprintln!(
                "tinyctb question-gate: banner write to {} gave up: {err}",
                tty.display()
            );
        }
    }

    /// The banner proper: what is being asked, the options with the letter
    /// codes a typed reply may use, and how to summon the real dialog. `\r\n`
    /// throughout — the TUI may hold the tty in raw mode where a bare `\n`
    /// staircases. Question and option text is model-authored and passes
    /// through `terminal_safe` first — see that function for why.
    fn paint_question(&self, question: &str, options: &[String], multi_select: bool) {
        let mut lines = format!(
            "\r\n\x1b[1;36m┃ tinyCTB · 有提问待作答（已推送手机）\x1b[0m\r\n\
             \x1b[36m┃ {}\x1b[0m\r\n",
            clip(&terminal_safe(question), 160)
        );
        if !options.is_empty() {
            let listed = options
                .iter()
                .enumerate()
                .map(|(index, label)| {
                    format!(
                        "[{}] {}",
                        (b'A' + (index as u8 % 26)) as char,
                        clip(&terminal_safe(label), 40)
                    )
                })
                .collect::<Vec<_>>()
                .join("  ");
            lines.push_str(&format!("\x1b[36m┃ {listed}\x1b[0m\r\n"));
        }
        if multi_select {
            lines.push_str("\x1b[36m┃ 多选：手机端以逗号分隔回复\x1b[0m\r\n");
        }
        lines.push_str("\x1b[2m┃ 手机作答，或 /back 收回到本终端作答\x1b[0m\r\n");
        self.write(&lines);
    }

    /// One-line epilogue: the phone answered, or the window is being handed
    /// back. The durable record is the `systemMessage` receipt in the hook's
    /// reply — this line only keeps the just-painted banner from ending on a
    /// still-waiting promise. The note may embed a phone-authored answer, so
    /// the whole line is sanitized; the fixed prefix carries no controls and
    /// the ANSI wrapper is added AFTER, so it survives untouched.
    fn paint_note(&self, note: &str) {
        self.write(&format!(
            "\x1b[1;36m┃ tinyCTB · {}\x1b[0m\r\n",
            terminal_safe(note)
        ));
    }
}

/// Total time one banner write may spend before giving up. Generous for a
/// live terminal (a full banner is a few hundred bytes against a multi-KiB
/// tty output queue) and small against the gate's real job: even a wedged
/// tty costs half a second, not the approval window.
const BANNER_WRITE_BUDGET: Duration = Duration::from_millis(500);

/// Bounded, non-blocking tty write. A plain blocking `write_all` here can
/// hang FOREVER — ^S/XOFF flow control, a stopped tmux client, or a full
/// output queue all park the writer in the kernel with no error to catch —
/// and "best-effort" that never returns would freeze the gate itself: an
/// initial banner would stall the poll loop before it starts (the phone
/// answer never consumed), a receipt line would stall the answer's return
/// (phone shows answered, tool never proceeds). `O_NONBLOCK` turns those
/// stalls into `WouldBlock`, which is retried only inside `budget`; whatever
/// does not fit is dropped, reported, and the gate moves on. `O_NOCTTY`
/// keeps the open from ever adopting the tty as this process's controlling
/// terminal.
#[cfg(unix)]
fn write_bounded(tty: &std::path::Path, bytes: &[u8], budget: Duration) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let device = std::fs::OpenOptions::new()
        .append(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY)
        .open(tty)?;
    write_all_within(device, bytes, budget)
}

/// The budget is STRICT and checked at the top of every lap — not only on
/// `WouldBlock`. The other two loop states have their own unbounded modes:
/// an `Interrupted` storm (a signal-happy process) would otherwise spin
/// forever, and a tty dribbling one byte per write would pass every
/// per-error check while never finishing. `Ok(0)` is an error, not quiet
/// success — pretending a zero-length write "worked" silently drops the
/// rest of the banner.
fn write_all_within<W: std::io::Write>(
    mut device: W,
    bytes: &[u8],
    budget: Duration,
) -> std::io::Result<()> {
    let deadline = std::time::Instant::now() + budget;
    let mut written = 0usize;
    while written < bytes.len() {
        if std::time::Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("tty not draining; dropped {} bytes", bytes.len() - written),
            ));
        }
        match device.write(&bytes[written..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    format!("tty accepted 0 bytes; dropped {}", bytes.len() - written),
                ))
            }
            Ok(count) => written += count,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn write_bounded(_tty: &std::path::Path, _bytes: &[u8], _budget: Duration) -> std::io::Result<()> {
    Ok(())
}

/// Strip everything that could steer a terminal out of model-authored text.
///
/// Question and option text comes from the model — which may have absorbed
/// untrusted repo or web content — and a JSON-legal string carries ESC, BEL,
/// CR or C1 bytes just fine. Embedded raw, those become CSI screen wipes,
/// OSC title or clipboard writes, or CR overprints of the surrounding
/// banner. CR/LF/TAB become a plain space (word boundaries survive); every
/// other C0 control, DEL, and the C1 range are dropped outright — killing
/// the ESC that introduces any escape sequence while leaving its printable
/// tail as harmless visible text. The banner's own fixed ANSI is added
/// around the sanitized text afterwards, never through it.
fn terminal_safe(text: &str) -> String {
    text.chars()
        .filter_map(|ch| match ch {
            '\r' | '\n' | '\t' => Some(' '),
            ch if (ch as u32) < 0x20 || ch == '\u{7F}' => None,
            ch if ('\u{80}'..='\u{9F}').contains(&ch) => None,
            ch => Some(ch),
        })
        .collect()
}

/// Truncation for banner display only, on char boundaries — a byte cut
/// through the middle of the CJK text these banners usually carry would
/// panic `format!`.
fn clip(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut clipped: String = text.chars().take(max_chars).collect();
    clipped.push('…');
    clipped
}

/// Hand the user's choice back through the tool's own contract: the question
/// tool accepts an `answers` map (question text -> answer string), so the
/// call is allowed to complete WITH the answer already filled in. The model
/// then receives it as a tool result rather than having to infer it from a
/// refusal message.
///
/// The `allow` here is not a permission grant — asking a question has no side
/// effect. Granting permission still requires an approval button.
/// Let the user answer with option letters ("A", "a,c") as well as full
/// labels — the letters are what the message shows, so typing them is the
/// obvious thing to do. Multi-part answers come back comma-separated, the
/// shape the tool documents for multi-select.
///
/// Everything else must survive **byte for byte**. Two ways an earlier
/// version corrupted answers, both guarded below: a tapped label that itself
/// contains a comma (`Washington, D.C.`) got split and rejoined without the
/// space, and a sentence that merely opens with a letter (`A, but only
/// locally`) had that letter swapped for an option label.
fn resolve_answer(answer: &str, options: &[String]) -> String {
    let trimmed = answer.trim();
    // A tapped button sends its label verbatim, and a typed answer may equally
    // well be one. Labels are free text, commas included, so recognise them
    // before any splitting happens.
    if options.iter().any(|option| option == trimmed) {
        return trimmed.to_string();
    }
    let parts = trimmed
        .split([',', '，', '、'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    // Expand letter codes only when the answer is *nothing but* codes.
    // Rewriting one token of a sentence would put words in the user's mouth.
    let resolved = parts
        .iter()
        .map(|part| letter_option(part, options))
        .collect::<Option<Vec<_>>>();
    match resolved {
        Some(labels) if !labels.is_empty() => labels.join(","),
        _ => trimmed.to_string(),
    }
}

/// `"a"` / `"C"` -> the option at that position, when there is one.
fn letter_option(part: &str, options: &[String]) -> Option<String> {
    let mut chars = part.chars();
    let letter = match (chars.next(), chars.next()) {
        (Some(letter), None) if letter.is_ascii_alphabetic() => letter,
        _ => return None,
    };
    let index = usize::from(letter.to_ascii_uppercase() as u8 - b'A');
    options.get(index).cloned()
}

fn answered(questions: &[Value], question_text: &str, answer: &str) -> Value {
    json!({
        // The durable in-terminal receipt: `systemMessage` travels through
        // Claude Code's own rendering (and the transcript), unlike the tty
        // banner, which the post-hook repaint chews. Between the two, a
        // person at the screen can always reconstruct what was asked and
        // who answered it.
        // Display copies are sanitized; `updatedInput` below is NOT — the
        // answers map must reach the tool byte-exact or the question counts
        // as unanswered.
        "systemMessage": format!("tinyCTB：已由手机作答「{}」", terminal_safe(answer)),
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "permissionDecisionReason": "Answered from Telegram",
            "updatedInput": {
                "questions": questions,
                "answers": { question_text: answer }
            }
        }
    })
}

/// Turn a recorded answer into the hook's reply. Kept in one place so the
/// polling loop and the timeout race resolve an answer identically.
/// The FINAL consumption of any authorization by a HEADLESS gate:
/// re-verifies, in one IMMEDIATE transaction, that the owning turn is
/// still `running`. `/stop` commits `stopping` and its dialog sweep
/// atomically, so whichever transaction commits second sees the other — a
/// stop that landed first turns this into a deny; one that lands after
/// has its group kill already aimed at whatever the tool starts. The
/// admission check alone was a single look at entry; a session auto-allow
/// or an already-recorded `allow` could otherwise ride past a stop that
/// committed while the gate was waiting.
fn consume_headless_authorization(conn: &rusqlite::Connection) -> Result<bool> {
    let Some(turn_id) = std::env::var(crate::claude::BRIDGE_TURN_ENV)
        .ok()
        .filter(|token| !token.is_empty())
    else {
        // No owner token: not a bridge turn; nothing to re-verify.
        return Ok(true);
    };
    let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
    let row: Option<(String, i64)> = {
        use rusqlite::OptionalExtension as _;
        tx.query_row(
            "SELECT status, cleanup_pending FROM bridge_turns WHERE turn_id = ?1",
            rusqlite::params![turn_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
    };
    tx.commit()?;
    // `running` alone is not enough: a row still in birth debt, or one
    // whose cleanup marker landed while the stopping CAS has not yet, must
    // not have tools riding through the window. The identity handshake at
    // admission pays a healthy newborn's debt BEFORE any consumption, so
    // this never blocks the ordinary path.
    Ok(matches!(row, Some((status, marker)) if status == "running" && marker == 0))
}

fn apply_decision(
    conn: &rusqlite::Connection,
    kind: GateKind,
    thread_id: &str,
    tool_name: &str,
    decision: &str,
    now: u64,
) -> Result<Value> {
    // Every allow-shaped decision for a headless turn passes the final
    // owner re-check, whatever path delivered it here (a fresh tap, a
    // pre-recorded decision, a replayed publication).
    if kind == GateKind::Headless
        && matches!(decision, "allow" | "allow_session")
        && !consume_headless_authorization(conn)?
    {
        return Ok(kind.deny(&format!(
            "this bridge turn was stopped before the authorization for {tool_name} \
             could be used, so it was not run. Do not retry; stop."
        )));
    }
    match decision {
        "allow" => Ok(kind.allow("Approved from Telegram")),
        "allow_session" => {
            set_approval_auto_allow(conn, thread_id, tool_name, now)?;
            Ok(kind.allow(&format!(
                "Approved from Telegram; {tool_name} is allowed for this session"
            )))
        }
        "deny" => Ok(kind.deny("Denied from Telegram")),
        // Unknown value: refuse to guess. What "not guessing" means differs
        // per gate — the interactive session falls back to its terminal
        // prompt, while for a headless turn an empty reply would BE the
        // guess (it executes), so only deny refuses anything there.
        _ => match kind {
            GateKind::Interactive => Ok(no_opinion()),
            GateKind::Headless => Ok(kind.deny(&format!(
                "the recorded decision {decision:?} is not one this gate recognises, \
                 so {tool_name} was not run"
            ))),
        },
    }
}

/// Fresh answer buttons for an OPEN approval, with their callback routes
/// registered. Shared by the gate's original push and by /threads reoffers:
/// the blocked hook polls the ROW, so any registered button that writes the
/// row answers it — whichever message the user happens to have in front of
/// them. One-answer-per-approval is enforced by the row, not the message.
/// Publish an approval request ATOMICALLY: the owner re-verification, the
/// approval row, its callback routes, and the outbox button commit — or
/// vanish — together. Published piecemeal, a `/stop` that landed between
/// the row and the button found nothing to withdraw yet, and the stopped
/// turn still received a live-looking button.
///
/// IMMEDIATE, so the owner check and every write serialise against the
/// stop's intent transaction: whichever commits second sees the other's
/// work in full — a stop after this commit sweeps everything published
/// here; a stop before it makes this refuse.
///
/// It is also idempotent by APPROVAL ID: the same `tool_use_id`
/// republished (an interrupted gate re-run, a redelivered hook) must not
/// reopen a decided prompt — REPLACE semantics here once reset a recorded
/// `deny` to NULL, pushed no new message (the outbox key already existed),
/// and left a stale sibling button able to flip the answer to allow. An
/// existing OPEN row keeps its routes and button untouched; an existing
/// decision is handed back to be honoured, never re-asked.
///
/// `headless` doubles as the event's blocking flag: whether silence will
/// deny is part of the request, not a footnote. `session_window` is the same
/// kind of fact for an INTERACTIVE gate: its terminal fallback may exist but
/// be invisible, so the message must not promise one. `None` = a headless
/// turn, which never probes; the field is then OMITTED rather than guessed,
/// because `headless` already says there is no terminal behind this at all.
#[allow(clippy::too_many_arguments)]
pub(crate) fn publish_approval_request(
    conn: &rusqlite::Connection,
    chat_id: &str,
    approval_id: &str,
    thread_id: &str,
    tool_name: &str,
    summary: &str,
    headless: bool,
    session_window: Option<crate::claude::SessionWindow>,
    owning_turn: Option<&str>,
    cwd: Option<&str>,
    now: u64,
    expires_at: u64,
) -> Result<Publication> {
    use rusqlite::OptionalExtension as _;
    let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)?;
    if let Some(turn_id) = owning_turn {
        let status: Option<String> = tx
            .query_row(
                "SELECT status FROM bridge_turns WHERE turn_id = ?1",
                rusqlite::params![turn_id],
                |row| row.get(0),
            )
            .optional()?;
        if status.as_deref() != Some("running") {
            return Ok(Publication::OwnerNotRunning);
        }
    }
    let existing: Option<(Option<String>, i64)> = tx
        .query_row(
            "SELECT decision, expires_at FROM pending_approvals WHERE approval_id = ?1",
            rusqlite::params![approval_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((decision, expires_at)) = existing {
        return Ok(match decision {
            Some(decision) => Publication::AlreadyDecided(decision),
            // Un-decided, but only LIVE if its deadline has not passed. An
            // interrupted gate leaves its row NULL with a lapsed expires_at;
            // waiting on that row again would hold the tool a full window
            // behind buttons whose taps already report "已过期". Settle it
            // here, inside the same transaction, and let the caller resolve
            // it the way any expired row resolves.
            None if (expires_at as u64) < now => {
                // Settling and withdrawing its queued push are ONE step (the
                // shared helper): the raw UPDATE that stood here settled the
                // row but left the interrupted run's push on the retry
                // schedule, so buttons already known to be dead could still
                // ship minutes later.
                let outcome =
                    crate::state::settle_expired_and_cancel_push_inner(&tx, approval_id, now)?;
                let decision = match outcome {
                    // No answer can have landed — the row was NULL a moment
                    // ago inside this same IMMEDIATE transaction — but if
                    // one somehow did, it is the truth to hand back.
                    crate::state::SettleOutcome::Answered => {
                        crate::state::approval_decision(&tx, approval_id)?
                            .unwrap_or_else(|| "expired".to_string())
                    }
                    crate::state::SettleOutcome::Expired => "expired".to_string(),
                };
                tx.commit()?;
                return Ok(Publication::AlreadyDecided(decision));
            }
            None => Publication::AlreadyPublished,
        });
    }
    crate::state::create_pending_approval(
        &tx,
        approval_id,
        thread_id,
        tool_name,
        summary,
        headless,
        now,
        expires_at,
    )?;
    if let Some(turn_id) = owning_turn {
        crate::state::record_approval_turn_owner(&tx, approval_id, turn_id)?;
    }
    // Buttons register their callback routes as rows, which is exactly why
    // they must share this transaction.
    let buttons = approval_answer_buttons(&tx, chat_id, thread_id, approval_id, now)?;
    let mut event = json!({
        "type": "approval_request",
        "threadId": thread_id,
        "approvalId": approval_id,
        "toolName": tool_name,
        "observedAt": now,
        "eventKey": format!("approval:{approval_id}"),
        "lastPreview": summary,
        "buttons": buttons,
        "headless": headless,
        "thread": {
            "threadId": thread_id,
            "cwd": cwd,
            "project": crate::projects::derive_project_label(cwd),
            "lastPreview": summary
        }
    });
    if let Some(window) = session_window {
        event["terminalVisibility"] = json!(terminal_visibility(window));
    }
    // origin "bridge": an approval request is something the user asked for
    // by going away, and it must survive /back's away-backlog cleanup.
    enqueue_outbound_event(&tx, &event, now, "bridge")?;
    tx.commit()?;
    Ok(Publication::Published)
}

/// A gate's own reading of its session, in the vocabulary the renderer
/// speaks. Straight through — including the shrug: the probe walks /proc and
/// can fail, and "could not tell" must not be rounded to "has a window".
fn terminal_visibility(window: crate::claude::SessionWindow) -> &'static str {
    match window {
        crate::claude::SessionWindow::Background => {
            crate::telegram::render::TERMINAL_VISIBILITY_BACKGROUND
        }
        crate::claude::SessionWindow::Window => crate::telegram::render::TERMINAL_VISIBILITY_WINDOW,
        crate::claude::SessionWindow::Unverified => {
            crate::telegram::render::TERMINAL_VISIBILITY_UNVERIFIED
        }
    }
}

/// What publishing an approval request actually did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Publication {
    /// Row, owner, routes and outbox button all committed together.
    Published,
    /// The same approval id is already OPEN — its routes and button stand;
    /// nothing was touched, and the caller should simply wait on it.
    AlreadyPublished,
    /// The same approval id was already decided. The decision must be
    /// honoured, never re-asked.
    AlreadyDecided(String),
    /// The owner turn is no longer `running` — `/stop` got there first.
    /// Nothing was written.
    OwnerNotRunning,
}

pub(crate) fn approval_answer_buttons(
    conn: &rusqlite::Connection,
    chat_id: &str,
    thread_id: &str,
    approval_id: &str,
    now: u64,
) -> Result<Vec<Value>> {
    let mut buttons = Vec::new();
    for (action, label) in [
        (TelegramCallbackAction::Approve, "✅ 允许"),
        (TelegramCallbackAction::ApproveSession, "🔁 本会话都允许"),
        (TelegramCallbackAction::Deny, "❌ 拒绝"),
    ] {
        let callback_id = format!(
            "ap{}",
            generate_session_uuid()?
                .replace('-', "")
                .chars()
                .take(16)
                .collect::<String>()
        );
        insert_telegram_callback_route(
            conn,
            &TelegramCallbackRoute {
                callback_id: callback_id.clone(),
                chat_id: chat_id.to_string(),
                message_id: None,
                thread_id: thread_id.to_string(),
                action,
                approval_id: Some(approval_id.to_string()),
                question_id: None,
                answer: None,
            },
            now,
        )?;
        buttons
            .push(json!({ "text": label, "callbackId": callback_id, "action": action.as_str() }));
    }
    Ok(buttons)
}

/// Fresh option buttons for an OPEN question (single-select only — a
/// multi-select tap would submit one choice and drop the rest). Same shared
/// contract as `approval_answer_buttons`.
pub(crate) fn question_answer_buttons(
    conn: &rusqlite::Connection,
    chat_id: &str,
    thread_id: &str,
    question_id: &str,
    options: &[String],
    now: u64,
) -> Result<Vec<Value>> {
    let mut buttons = Vec::new();
    for (index, label) in options.iter().enumerate().take(8) {
        let callback_id = format!(
            "qa{}",
            generate_session_uuid()?
                .replace('-', "")
                .chars()
                .take(16)
                .collect::<String>()
        );
        insert_telegram_callback_route(
            conn,
            &TelegramCallbackRoute {
                callback_id: callback_id.clone(),
                chat_id: chat_id.to_string(),
                message_id: None,
                thread_id: thread_id.to_string(),
                action: TelegramCallbackAction::AnswerQuestion,
                approval_id: None,
                question_id: Some(question_id.to_string()),
                answer: Some(label.clone()),
            },
            now,
        )?;
        // Colour makes a row read as a button; the letter makes options
        // distinguishable at a glance (and is what a text reply can name).
        // Unicode has no full A–Z coloured-letter set — 🅰️🅱️ stop at B and
        // regional indicators (🇦🇧🇨) render flat on some clients — so a
        // saturated dot supplies the colour and the letter follows it.
        const MARKERS: [&str; 8] = ["🔴", "🟠", "🟡", "🟢", "🔵", "🟣", "🟤", "⚫"];
        let marker = MARKERS[index.min(MARKERS.len() - 1)];
        let letter = (b'A' + index as u8) as char;
        buttons.push(json!({
            "text": format!("{marker}{letter} {}", truncate_tool_detail(label)),
            "callbackId": callback_id,
            "action": TelegramCallbackAction::AnswerQuestion.as_str(),
            "answer": label
        }));
    }
    Ok(buttons)
}

/// The question message's body. When the options ARE the buttons, listing
/// them in the body too makes the buttons read as a duplicate block of
/// text — only the no-button paths (multi-select) spell the options out.
pub(crate) fn question_body(question: &str, options: &[String], multi_select: bool) -> String {
    if multi_select && !options.is_empty() {
        let listed = options
            .iter()
            .enumerate()
            .map(|(index, label)| format!("{}. {label}", (b'A' + index as u8) as char))
            .collect::<Vec<_>>()
            .join("\n");
        format!("{question}\n\n{listed}\n\n（多选）回复本消息，逗号分隔，例如 A,C")
    } else {
        question.to_string()
    }
}

/// One line describing what is about to happen, so the Telegram message shows
/// the actual command or file rather than just a tool name.
pub(crate) fn tool_call_summary(tool_name: &str, tool_input: Option<&Value>) -> String {
    let Some(input) = tool_input else {
        return tool_name.to_string();
    };
    let detail = match tool_name {
        "Bash" => input
            .get("command")
            .and_then(Value::as_str)
            .map(str::to_string),
        "Write" | "Edit" | "NotebookEdit" | "Read" => input
            .get("file_path")
            .and_then(Value::as_str)
            .map(str::to_string),
        "WebFetch" => input.get("url").and_then(Value::as_str).map(str::to_string),
        _ => serde_json::to_string(input)
            .ok()
            .filter(|raw| raw != "{}" && raw != "null"),
    };
    match detail {
        Some(detail) if !detail.trim().is_empty() => {
            format!("{tool_name}: {}", truncate_tool_detail(detail.trim()))
        }
        _ => tool_name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The final consumption of an allow re-checks the owner: the SAME
    /// recorded decision that was honoured while the turn ran must turn
    /// into a deny once `/stop` has committed — a session grant or a
    /// pre-recorded allow must not ride past the stop.
    #[test]
    fn an_allow_is_not_consumable_after_the_stop_committed() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::register_bridge_turn(
            &conn,
            "turn-g",
            "sess-g",
            "/tmp/t.log",
            Some(4_500),
            None,
            None,
            Some(4_500),
            None,
            None,
            1000,
        )
        .expect("register");
        let _token = crate::state::EnvVarGuard::set(crate::claude::BRIDGE_TURN_ENV, "turn-g");

        let allowed = apply_decision(&conn, GateKind::Headless, "sess-g", "Bash", "allow", 2000)
            .expect("apply");
        assert_eq!(
            allowed["hookSpecificOutput"]["permissionDecision"], "allow",
            "while the turn runs, the allow is honoured: {allowed}"
        );

        crate::state::mark_bridge_turn_stopping(&conn, "turn-g", 3000).expect("stop");
        let denied = apply_decision(&conn, GateKind::Headless, "sess-g", "Bash", "allow", 4000)
            .expect("apply");
        assert_eq!(
            denied["hookSpecificOutput"]["permissionDecision"], "deny",
            "after the stop committed, the same allow must deny: {denied}"
        );
        let session = apply_decision(
            &conn,
            GateKind::Headless,
            "sess-g",
            "Bash",
            "allow_session",
            5000,
        )
        .expect("apply");
        assert_eq!(
            session["hookSpecificOutput"]["permissionDecision"], "deny",
            "a session grant must not ride past the stop either: {session}"
        );
    }

    /// The consumption predicate demands a PAID debt, not just `running`:
    /// an unresolved birth-debt row must not have tools riding through,
    /// and the handshake — which pays the debt — is what re-opens the way.
    #[test]
    fn an_allow_waits_for_the_identity_debt() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::register_bridge_turn(
            &conn,
            "turn-d",
            "sess-d",
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
        let _token = crate::state::EnvVarGuard::set(crate::claude::BRIDGE_TURN_ENV, "turn-d");

        let denied = apply_decision(&conn, GateKind::Headless, "sess-d", "Bash", "allow", 2000)
            .expect("apply");
        assert_eq!(
            denied["hookSpecificOutput"]["permissionDecision"], "deny",
            "an unpaid birth debt must not be ridden through: {denied}"
        );

        crate::state::record_bridge_turn_spawn(
            &conn,
            "turn-d",
            Some(4_600),
            None,
            None,
            Some(4_600),
            Some("777"),
            Some("boot-y"),
        )
        .expect("identity write");
        let allowed = apply_decision(&conn, GateKind::Headless, "sess-d", "Bash", "allow", 3000)
            .expect("apply");
        assert_eq!(
            allowed["hookSpecificOutput"]["permissionDecision"], "allow",
            "the identity write pays the debt and re-opens the way: {allowed}"
        );
    }
    use crate::config::{ClaudeConfig, DaemonConfig, TelegramConfig};
    use crate::state::{record_approval_decision, write_away_marker_for_test};
    use std::fs;
    use std::path::PathBuf;

    struct GateEnv {
        root: PathBuf,
        /// Held, not read: dropping these restores the environment.
        #[allow(dead_code)]
        env: Vec<crate::state::EnvVarGuard>,
    }

    impl GateEnv {
        fn new(name: &str, away: bool, timeout_seconds: u64) -> Self {
            Self::with_tools(name, away, timeout_seconds, Vec::new())
        }

        fn with_tools(
            name: &str,
            away: bool,
            timeout_seconds: u64,
            approval_tools: Vec<String>,
        ) -> Self {
            let root =
                std::env::temp_dir().join(format!("tinyctb-gate-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("gate dir");
            // EVERY environment variable this fixture touches is held by a
            // guard, so a panicking assertion cannot leak one into the next
            // test — the manual save/restore this replaced only covered the
            // state dir, and the seams it merely cleared were left cleared.
            let env = vec![
                crate::state::EnvVarGuard::set("TINYCTB_STATE_DIR", &root),
                // A leftover turn token from a previous test would make this
                // one look like a bridge process; every test starts tokenless.
                crate::state::EnvVarGuard::clear(crate::claude::BRIDGE_TURN_ENV),
                // The window probe is PINNED, never left to the host: a test
                // process inherits the developer's own CLAUDE_CODE_MESSAGING_
                // SOCKET, so an unset seam reads whatever session happens to
                // be running the suite — "window" on my machine, "unverified"
                // in a clean CI shell. Every gate test starts as a session
                // that HAS a terminal window, and says so explicitly.
                crate::state::EnvVarGuard::set("TINYCTB_TEST_SESSION_WINDOWLESS", "0"),
                // And without a fake session tty: banner painting is opt-in
                // per test, and a leftover path would spray banners into a
                // dead temp file (or worse, a reused one another test
                // asserts on).
                crate::state::EnvVarGuard::clear("TINYCTB_TEST_SESSION_TTY"),
            ];
            crate::config::write_daemon_config(&DaemonConfig {
                version: 1,
                bridge_command: "tinyctb".to_string(),
                events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
                telegram: Some(TelegramConfig {
                    bot_token: "123:secret".to_string(),
                    chat_id: "456".to_string(),
                    allowed_user_id: None,
                }),
                claude: Some(ClaudeConfig {
                    approval_timeout_seconds: timeout_seconds,
                    approval_tools,
                    ..ClaudeConfig::default()
                }),
                projects: Vec::new(),
            })
            .expect("config");
            write_away_marker_for_test(away).expect("away marker");
            Self { root, env }
        }
    }

    impl Drop for GateEnv {
        fn drop(&mut self) {
            // `env` restores itself; only the directory needs sweeping.
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn gate(payload: Value) -> Value {
        let mut reader = std::io::Cursor::new(payload.to_string());
        run_approval_gate(&mut reader, 1000).expect("gate")
    }

    /// `gate`, but a hang is a clean FAILED instead of a wedged suite. Used
    /// by the renewal tests, whose revert-verification mutations can turn
    /// the renewal loop into an infinite spin — measured 2026-08-27: one
    /// such mutation held three overlapping verification runs and a full
    /// suite hostage for hours. The spinning thread is leaked on timeout;
    /// libtest's process exit sweeps it.
    fn gate_with_timeout(payload: Value, limit: Duration) -> Value {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(gate(payload));
        });
        rx.recv_timeout(limit)
            .expect("the gate must return within the harness limit, not spin")
    }

    /// The real `PermissionRequest` input shape: no `tool_use_id` (that is a
    /// PreToolUse field), and it carries permission suggestions instead.
    fn bash_payload() -> Value {
        json!({
            "hook_event_name": "PermissionRequest",
            "session_id": "sess-gate",
            "transcript_path": "/home/user/.claude/projects/x/sess-gate.jsonl",
            "tool_name": "Bash",
            "permission_mode": "default",
            "cwd": "/home/user/project",
            "tool_input": { "command": "rm -rf build/" },
            "permission_suggestions": []
        })
    }

    #[test]
    fn gate_stays_out_of_the_way_when_not_away() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("not-away", false, 5);
        assert_eq!(gate(bash_payload()), json!({}), "no opinion while present");
    }

    #[test]
    fn gate_skips_bridge_initiated_turns() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("bypass", true, 5);
        let mut payload = bash_payload();
        payload["permission_mode"] = json!("bypassPermissions");
        assert_eq!(
            gate(payload),
            json!({}),
            "a Telegram-started turn must not block on its own approval"
        );
    }

    /// With an explicit narrowing list, a tool outside it must be waved
    /// through IMMEDIATELY — not merely end up returning `{}` after sitting
    /// out the whole approval timeout, which looks identical from the
    /// return value alone.
    #[test]
    fn gate_ignores_tools_outside_the_configured_list() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::with_tools("ungated-tool", true, 30, vec!["Bash".to_string()]);
        let mut payload = bash_payload();
        payload["tool_name"] = json!("Read");

        let started = std::time::Instant::now();
        assert_eq!(gate(payload), json!({}));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "a narrowed-out tool must not wait for an answer"
        );

        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let approvals: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_approvals", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(approvals, 0, "and must not create an approval");
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            0,
            "nor push anything to Telegram"
        );
    }

    fn headless_gate(payload: Value) -> Value {
        let mut reader = std::io::Cursor::new(payload.to_string());
        run_headless_approval_gate(&mut reader, 1000)
    }

    /// The real `PreToolUse` shape, copied from a payload captured off a live
    /// `claude -p` run (docs/approvals.md records the experiment).
    fn headless_payload() -> Value {
        json!({
            "hook_event_name": "PreToolUse",
            "session_id": "sess-headless",
            "transcript_path": "/home/user/.claude/projects/x/sess-headless.jsonl",
            "tool_name": "Bash",
            "tool_use_id": "toolu_headless_1",
            "permission_mode": "bypassPermissions",
            "cwd": "/home/user/project",
            "prompt_id": "p1",
            "tool_input": { "command": "rm -rf build/" }
        })
    }

    /// Register the turn AND assume its identity: the env token is what the
    /// gate trusts first, the row is what it verifies second.
    fn enter_bridge_turn(conn: &rusqlite::Connection, thread_id: &str) {
        register_turn_for(conn, thread_id);
        std::env::set_var(crate::claude::BRIDGE_TURN_ENV, "turn-1");
    }

    fn register_turn_for(conn: &rusqlite::Connection, thread_id: &str) {
        // With a pid: this helper models an ESTABLISHED running turn, whose
        // identity write (or cgroup binding) already paid the birth debt —
        // a debtor row is deliberately denied consumption, and has its own
        // tests.
        crate::state::register_bridge_turn(
            conn,
            "turn-1",
            thread_id,
            "/tmp/turn.log",
            Some(4_700),
            None,
            None,
            Some(4_700),
            None,
            None,
            900,
        )
        .expect("register bridge turn");
    }

    /// The safety rule that had no owner until now. An interactive gate can
    /// answer "no opinion" and let the terminal dialog handle it; a headless
    /// turn has no terminal, runs under `bypassPermissions`, and would take
    /// "no opinion" as permission to proceed. So silence must deny.
    #[test]
    fn headless_gate_denies_when_nobody_answers() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-timeout", true, 5); // clamped minimum
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        drop(conn);

        let started = std::time::Instant::now();
        let result = headless_gate(headless_payload());
        assert!(
            started.elapsed() >= Duration::from_secs(4),
            "the gate must actually wait for an answer"
        );
        let out = &result["hookSpecificOutput"];
        assert_eq!(out["hookEventName"], "PreToolUse", "{result}");
        assert_eq!(out["permissionDecision"], "deny", "{result}");
        assert!(
            out["permissionDecisionReason"]
                .as_str()
                .expect("reason")
                .contains("not run"),
            "the model must be told the call did not happen: {result}"
        );

        // And the request that went out says silence will stop the task, so
        // ignoring it is an informed choice.
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let payload_json: String = conn
            .query_row("SELECT payload_json FROM outbound_events", [], |row| {
                row.get(0)
            })
            .expect("outbound row");
        let event: Value = serde_json::from_str(&payload_json).expect("json");
        assert_eq!(event["headless"], true, "{event}");
        assert!(payload_json.contains("rm -rf build/"), "{payload_json}");
    }

    /// The same `bypassPermissions` payload from a session the USER started
    /// (`--dangerously-skip-permissions` in a terminal) is not a bridge turn.
    /// The gate must step aside IMMEDIATELY — no approval row, no Telegram
    /// push, no wait. The old shape of this bug: the gate created an approval
    /// first and checked membership last, so a terminal bypass session got a
    /// Telegram request promising a fallback dialog that bypass mode does not
    /// have, and sat out the full timeout.
    #[test]
    fn headless_gate_leaves_an_unregistered_session_its_terminal() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-unregistered", true, 30);
        let started = std::time::Instant::now();
        let result = headless_gate(headless_payload());
        assert_eq!(
            result,
            json!({}),
            "no bridge turn behind it: do not deny on the user's behalf"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "and do not make the user's own session wait"
        );
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let approvals: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_approvals", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(approvals, 0, "no approval may be created before admission");
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            0,
            "and nothing may be pushed to Telegram"
        );
    }

    /// A token whose turn the daemon has already settled (timed out and
    /// killed, say) marks a straggler process. Its calls get an immediate
    /// deny — not a fresh approval with buttons pointing at a closed window,
    /// and certainly not a pass.
    #[test]
    fn headless_gate_denies_stragglers_of_a_settled_turn() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-straggler", true, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        crate::state::mark_bridge_turn_finished(&conn, "turn-1", "expired", 950).expect("settle");
        drop(conn);

        let started = std::time::Instant::now();
        let result = headless_gate(headless_payload());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "a straggler must be refused immediately, not offered an approval"
        );
        let out = &result["hookSpecificOutput"];
        assert_eq!(out["permissionDecision"], "deny", "{result}");
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let approvals: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_approvals", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(approvals, 0, "no approval row for a closed window");
    }

    /// The first admission layer must not be the database. A user's own
    /// bypass-mode terminal session (no turn token) has to sail through even
    /// when tinyctb's state directory is a smoking crater — if admission
    /// opened SQLite first, that breakage would surface as fail-closed
    /// denials inside sessions the bridge does not own.
    #[test]
    fn headless_gate_spares_tokenless_sessions_even_with_broken_state() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-broken-state", true, 30);
        // Point the state dir at a regular FILE: any database open under it
        // fails. The away marker is unreadable too, which must not matter —
        // the headless gate does not consult away at all.
        let blocker =
            std::env::temp_dir().join(format!("tinyctb-state-blocker-{}", std::process::id()));
        fs::write(&blocker, "not a directory").expect("blocker file");
        std::env::set_var("TINYCTB_STATE_DIR", &blocker);

        let result = headless_gate(headless_payload());
        let _ = fs::remove_file(&blocker);
        assert_eq!(
            result,
            json!({}),
            "a tokenless session must never see the bridge's own failures"
        );
    }

    /// An unknown persisted decision must not become execution. For the
    /// interactive gate "refuse to guess" falls back to the terminal prompt;
    /// for a headless turn the empty object IS the guess, so only deny works.
    #[test]
    fn unknown_decision_resolves_per_gate() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("unknown-decision", true, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let interactive = apply_decision(
            &conn,
            GateKind::Interactive,
            "sess",
            "Bash",
            "garbled",
            1000,
        )
        .expect("interactive");
        assert_eq!(interactive, json!({}), "terminal prompt catches it");
        let headless = apply_decision(&conn, GateKind::Headless, "sess", "Bash", "garbled", 1000)
            .expect("headless");
        assert_eq!(
            headless["hookSpecificOutput"]["permissionDecision"], "deny",
            "nothing catches it downstream: {headless}"
        );
    }

    /// Away gates the interactive side only. Telegram starts headless turns
    /// with away off too (`/new`, a Reply sent while at the keyboard), and
    /// those turns have no terminal regardless of where the user sits — with
    /// an away check in front, every such turn would run its gated calls
    /// unchecked. This is the regression test for exactly that bug.
    #[test]
    fn headless_gate_engages_with_away_off() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-away-off", false, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        drop(conn);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(approval_id) = conn.query_row(
                    "SELECT approval_id FROM pending_approvals WHERE decision IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    record_approval_decision(&conn, &approval_id, "allow", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("approval row never appeared — the gate did not engage");
        });
        let result = headless_gate(headless_payload());
        handle.join().expect("answering thread");
        assert_eq!(
            result["hookSpecificOutput"]["permissionDecision"], "allow",
            "{result}"
        );
    }

    /// Once a running bridge turn is established, an internal error must
    /// DENY, not fall through: under `bypassPermissions` an empty reply is
    /// execution, and "the config was unreadable" is not an approval.
    #[test]
    fn headless_gate_fails_closed_when_config_is_unreadable() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-bad-config", true, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        drop(conn);
        let config_path = crate::config::daemon_config_path().expect("config path");
        fs::write(&config_path, "{ not json").expect("corrupt config");

        let started = std::time::Instant::now();
        let result = headless_gate(headless_payload());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "failing closed must not wait for anything"
        );
        let out = &result["hookSpecificOutput"];
        assert_eq!(out["permissionDecision"], "deny", "{result}");
        assert!(
            out["permissionDecisionReason"]
                .as_str()
                .expect("reason")
                .contains("blocked"),
            "the reason must say the call was blocked, with the error: {result}"
        );
    }

    /// `PreToolUse` fires for every tool call, so the tool filter is the only
    /// thing keeping this gate off the hot path. A read must be waved through
    /// IMMEDIATELY — not after sitting out the timeout, which would look the
    /// same from the return value alone.
    #[test]
    fn headless_gate_does_not_touch_read_only_tools() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-read", true, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        drop(conn);
        let mut payload = headless_payload();
        payload["tool_name"] = json!("Read");
        payload["tool_input"] = json!({ "file_path": "/home/user/project/src/main.rs" });

        let started = std::time::Instant::now();
        assert_eq!(headless_gate(payload), json!({}));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "an ungated tool must not wait for an answer"
        );
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let approvals: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_approvals", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(approvals, 0, "and must not create an approval");
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            0,
            "nor push anything to Telegram"
        );
    }

    /// A tap answers in the PreToolUse contract, not the PermissionRequest
    /// one. The two shapes are different objects, and returning the wrong one
    /// would be read as "no opinion" — which for a headless turn means the
    /// call runs despite the user having denied it.
    #[test]
    fn headless_gate_answers_in_the_pretooluse_contract() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-answered", true, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        drop(conn);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(approval_id) = conn.query_row(
                    "SELECT approval_id FROM pending_approvals WHERE decision IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    record_approval_decision(&conn, &approval_id, "deny", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("approval row never appeared");
        });
        let result = headless_gate(headless_payload());
        handle.join().expect("answering thread");

        let out = &result["hookSpecificOutput"];
        assert_eq!(out["hookEventName"], "PreToolUse", "{result}");
        assert_eq!(out["permissionDecision"], "deny", "{result}");
        assert!(
            out.get("decision").is_none(),
            "the PermissionRequest shape must not leak into a PreToolUse reply: {result}"
        );
    }

    /// The two gates partition tool calls on `bypassPermissions` alone. If
    /// both engaged for the same call the user would get two Telegram
    /// requests for it; if neither did, it would run unchecked.
    #[test]
    fn the_two_gates_do_not_overlap() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("gate-split", true, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        drop(conn);

        // A normal interactive call is not the headless gate's business.
        let mut interactive = headless_payload();
        interactive["permission_mode"] = json!("default");
        let started = std::time::Instant::now();
        assert_eq!(headless_gate(interactive), json!({}));
        // A bypassing call is not the interactive gate's business (covered by
        // `gate_skips_bridge_initiated_turns`, asserted here as the other half
        // of the same partition).
        let mut bypassing = bash_payload();
        bypassing["permission_mode"] = json!("bypassPermissions");
        assert_eq!(gate(bypassing), json!({}));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "both must decline immediately, not after a timeout"
        );
    }

    /// The away-mode prompt coaching: silent at the keyboard, tool-pushing
    /// for interactive sessions, options-at-the-end for headless turns
    /// (which have no AskUserQuestion tool at all).
    #[test]
    fn prompt_context_matches_where_the_user_is() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("prompt-context", false, 30);
        let run = || {
            let mut reader = std::io::Cursor::new(r#"{"hook_event_name":"UserPromptSubmit"}"#);
            run_prompt_context(&mut reader)
        };

        // At the keyboard: no injection at all.
        assert_eq!(run(), json!({}), "away off must cost nothing");

        // Away, interactive: push the tool.
        write_away_marker_for_test(true).expect("away on");
        let value = run();
        let context = value
            .pointer("/hookSpecificOutput/additionalContext")
            .and_then(Value::as_str)
            .expect("context");
        assert!(context.contains("AskUserQuestion"), "{context}");
        assert!(context.contains("必须调用"), "{context}");
        assert_eq!(
            value.pointer("/hookSpecificOutput/hookEventName"),
            Some(&json!("UserPromptSubmit"))
        );

        // Away, headless turn: the tool does not exist there — options at
        // the end instead.
        std::env::set_var(crate::claude::BRIDGE_TURN_ENV, "turn-x");
        let value = run();
        std::env::remove_var(crate::claude::BRIDGE_TURN_ENV);
        let context = value
            .pointer("/hookSpecificOutput/additionalContext")
            .and_then(Value::as_str)
            .expect("context");
        assert!(context.contains("没有 AskUserQuestion"), "{context}");
        assert!(context.contains("列出编号选项"), "{context}");
    }

    /// The user's law (2026-08-27): while away is on, keyboard activity
    /// decides NOTHING — the buttons stay live and the first answer wins.
    /// The old design handed the prompt to the terminal within a poll tick
    /// of a keystroke, and pressing Yes on one dialog was itself the
    /// keystroke that killed the next approval's buttons. This test writes
    /// the REAL activity file — fresh at gate start and refreshed mid-wait —
    /// and the phone tap must still be the answer that lands.
    #[test]
    fn keystrokes_never_take_the_buttons_away() {
        let _guard = crate::state::test_env_lock();
        let env = GateEnv::new("present-user", true, 30);
        let activity = env.root.join(crate::daemon::INPUT_ACTIVITY_FILE);
        let stamp = |path: &std::path::Path| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_millis() as u64;
            fs::write(path, json!({ "lastInputAtMs": now }).to_string()).expect("stamp");
        };
        stamp(&activity);

        let tapper = {
            let activity = activity.clone();
            std::thread::spawn(move || {
                let conn = create_state_db(&state_db_path().expect("path")).expect("db");
                let deadline = std::time::Instant::now() + Duration::from_secs(20);
                loop {
                    // Typing continues the whole time the gate waits.
                    stamp(&activity);
                    if let Ok(id) = conn.query_row(
                        "SELECT approval_id FROM pending_approvals WHERE decision IS NULL",
                        [],
                        |row| row.get::<_, String>(0),
                    ) {
                        // Hold the buttons through a few more keystrokes
                        // before tapping — under the old design they would
                        // already be dead by now.
                        std::thread::sleep(Duration::from_millis(1500));
                        stamp(&activity);
                        crate::state::record_approval_decision(&conn, &id, "allow", 2000)
                            .expect("tap");
                        return;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "approval row never appeared"
                    );
                    std::thread::sleep(Duration::from_millis(100));
                }
            })
        };
        let result = gate(bash_payload());
        tapper.join().expect("tapper");
        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"], "allow",
            "the tap must decide it — keystrokes are not a declaration: {result}"
        );
    }

    /// A tap landing while the transcript also moves on must still decide
    /// the call. This exercises the ordinary decision poll (the tap is seen
    /// first); the settle-vs-tap contract on the evidence path itself is
    /// pinned at the state layer by
    /// `settling_honours_a_tap_and_only_withdraws_withdrawable_pushes`.
    #[test]
    fn a_tap_during_the_wait_decides_even_as_the_transcript_moves_on() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("tap-wins-race", true, 120);
        let transcript =
            std::env::temp_dir().join(format!("tinyctb-tap-race-{}.jsonl", std::process::id()));
        // Real shape at gate time: the transcript already ends with the
        // assistant record carrying THIS tool_use. Seeding only a user line
        // made the test easier than production, where the boundary always
        // has an assistant record immediately behind it.
        fs::write(
            &transcript,
            format!(
                "{}\n{}\n",
                json!({"type": "user", "message": {"role": "user", "content": "go"}}),
                json!({"type": "assistant", "timestamp": "1970-01-01T00:00:01.000Z",
                       "message": {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_probe", "name": "Bash",
                     "input": {"command": "rm -rf build/"}}
                ]}})
            ),
        )
        .expect("seed transcript");
        let mut payload = bash_payload();
        payload["transcript_path"] = json!(transcript.display().to_string());

        let writer = {
            let transcript = transcript.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(1200));
                // The tap lands FIRST...
                let conn = create_state_db(&state_db_path().expect("path")).expect("db");
                let id: String = conn
                    .query_row("SELECT approval_id FROM pending_approvals", [], |row| {
                        row.get(0)
                    })
                    .expect("approval row");
                crate::state::record_approval_decision(&conn, &id, "allow", 2000).expect("tap");
                // ...and only then does the transcript move on.
                use std::io::Write as _;
                let mut file = fs::OpenOptions::new()
                    .append(true)
                    .open(&transcript)
                    .expect("append");
                writeln!(
                    file,
                    "{}",
                    json!({"type": "assistant", "message": {"role": "assistant",
                            "content": [{"type": "text", "text": "moved on"}]}})
                )
                .expect("write");
            })
        };
        let mut reader = std::io::Cursor::new(payload.to_string());
        let result = run_approval_gate(&mut reader, 1000).expect("gate");
        writer.join().expect("writer");
        let _ = fs::remove_file(&transcript);

        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"], "allow",
            "the tap the user already saw accepted must be what the hook returns: {result}"
        );
    }

    /// Regression for the 2026-08-27 production bug: an active interactive
    /// session keeps writing main-chain assistant records (it is talking to
    /// its user while a background tool's approval waits). The removed
    /// transcript-`decided_elsewhere` heuristic read that growth as "the
    /// call was decided" and settled the approval to `expired` within
    /// seconds — killing the phone buttons before the user ever saw them
    /// (measured: a 1-hour window closed at 6 seconds). The gate must ignore
    /// transcript movement entirely: away on means the buttons stay live
    /// until a real answer or `/back`.
    #[test]
    fn transcript_growth_does_not_close_a_waiting_approval() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("transcript-growth", true, 30);
        let transcript =
            std::env::temp_dir().join(format!("tinyctb-growth-{}.jsonl", std::process::id()));
        fs::write(
            &transcript,
            format!(
                "{}\n{}\n",
                json!({"type": "user", "message": {"role": "user", "content": "go"}}),
                json!({"type": "assistant", "timestamp": "1970-01-01T00:00:01.000Z",
                       "message": {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_probe", "name": "Bash",
                     "input": {"command": "rm -rf build/"}}
                ]}})
            ),
        )
        .expect("seed transcript");
        let mut payload = bash_payload();
        payload["transcript_path"] = json!(transcript.display().to_string());

        let driver = {
            let transcript = transcript.clone();
            std::thread::spawn(move || {
                use std::io::Write as _;
                // The session keeps talking: main-chain assistant records
                // land past the approval boundary, again and again.
                for i in 0..8 {
                    std::thread::sleep(Duration::from_millis(400));
                    let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open(&transcript)
                        .expect("append");
                    writeln!(
                        file,
                        "{}",
                        json!({"type": "assistant", "message": {"role": "assistant",
                                "content": [{"type": "text", "text": format!("chatter {i}")}]}})
                    )
                    .expect("write");
                    // A tool_result for a DIFFERENT call must be as inert as
                    // chatter: only THIS call's result may fold the gate.
                    writeln!(
                        file,
                        "{}",
                        json!({"type": "user", "message": {"role": "user",
                                "content": [{"type": "tool_result",
                                             "tool_use_id": format!("toolu_other_{i}"),
                                             "content": "done"}]}})
                    )
                    .expect("write");
                }
                // The buttons must STILL be live after all that growth — a
                // premature close would have settled the row to `expired`.
                let conn = create_state_db(&state_db_path().expect("path")).expect("db");
                let live: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM pending_approvals WHERE decision IS NULL",
                        [],
                        |row| row.get(0),
                    )
                    .expect("query");
                assert_eq!(
                    live, 1,
                    "the approval row must still be un-decided (live buttons) despite \
                     transcript growth"
                );
                // Only now does the real answer arrive — from the phone.
                let id: String = conn
                    .query_row(
                        "SELECT approval_id FROM pending_approvals WHERE decision IS NULL",
                        [],
                        |row| row.get(0),
                    )
                    .expect("live approval row");
                crate::state::record_approval_decision(&conn, &id, "allow", 9000).expect("tap");
            })
        };
        let mut reader = std::io::Cursor::new(payload.to_string());
        let result = run_approval_gate(&mut reader, 1000).expect("gate");
        driver.join().expect("driver");
        let _ = fs::remove_file(&transcript);

        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"], "allow",
            "transcript growth must not close the gate; only the tap decides: {result}"
        );
    }

    /// A RECENT but already-COMPLETED identical call in the seed must not
    /// fold a fresh gate: the reviewer's repro was a call finishing well
    /// inside the ±120s window (here stamped 300-500ms before the gate)
    /// while the current call's PermissionRequest starts before its own
    /// tool_use flushes. The seed must bind only PENDING candidates, so the
    /// gate stays live and the phone tap remains the decider.
    #[test]
    fn a_recent_completed_call_in_the_seed_does_not_fold_a_fresh_gate() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("seed-completed", true, 120);
        let transcript = std::env::temp_dir().join(format!(
            "tinyctb-seed-completed-{}.jsonl",
            std::process::id()
        ));
        // 30 "seconds" before the gate's now=1000ms — within the ±120s
        // window, so only the pending-only rule keeps it from binding.
        fs::write(
            &transcript,
            format!(
                "{}\n{}\n",
                json!({"type": "assistant", "timestamp": "1970-01-01T00:00:00.500Z",
                       "message": {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_old", "name": "Bash",
                     "input": {"command": "rm -rf build/"}}
                ]}}),
                json!({"type": "user", "timestamp": "1970-01-01T00:00:00.700Z",
                       "message": {"role": "user",
                        "content": [{"type": "tool_result",
                                     "tool_use_id": "toolu_old",
                                     "content": "done earlier"}]}})
            ),
        )
        .expect("seed transcript");
        let mut payload = bash_payload();
        payload["transcript_path"] = json!(transcript.display().to_string());

        let tapper = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            loop {
                // The row must still be LIVE well past the first poll — an
                // early fold would have settled it to expired already.
                std::thread::sleep(Duration::from_millis(100));
                if let Ok(id) = conn.query_row(
                    "SELECT approval_id FROM pending_approvals WHERE decision IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) {
                    std::thread::sleep(Duration::from_millis(2500));
                    match record_approval_decision(&conn, &id, "allow", 9000).expect("tap") {
                        crate::state::ApprovalAnswer::Recorded => return,
                        other => panic!(
                            "the old call's result must not have settled the fresh gate: {other:?}"
                        ),
                    }
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "approval row never appeared"
                );
            }
        });
        let result = gate_with_timeout(payload, Duration::from_secs(60));
        tapper.join().expect("tapper");
        let _ = fs::remove_file(&transcript);
        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"], "allow",
            "the phone tap must decide it; a 30s-old identical call must not: {result}"
        );
    }

    /// Several PENDING matching candidates in the seed: the NEWEST is the
    /// bound one. The older sibling's result must not fold the gate; the
    /// newest's must.
    #[test]
    fn the_newest_of_several_pending_candidates_is_bound() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("seed-newest", true, 120);
        let transcript =
            std::env::temp_dir().join(format!("tinyctb-seed-newest-{}.jsonl", std::process::id()));
        let tool_use = |id: &str, ts: &str| {
            json!({"type": "assistant", "timestamp": ts,
                   "message": {"role": "assistant", "content": [
                {"type": "tool_use", "id": id, "name": "Bash",
                 "input": {"command": "rm -rf build/"}}
            ]}})
        };
        fs::write(
            &transcript,
            format!(
                "{}\n{}\n",
                tool_use("toolu_older", "1970-01-01T00:00:00.400Z"),
                tool_use("toolu_newest", "1970-01-01T00:00:00.900Z"),
            ),
        )
        .expect("seed transcript");
        let mut payload = bash_payload();
        payload["transcript_path"] = json!(transcript.display().to_string());

        let writer = {
            let transcript = transcript.clone();
            std::thread::spawn(move || {
                use std::io::Write as _;
                let append = |value: Value| {
                    let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open(&transcript)
                        .expect("append");
                    writeln!(file, "{value}").expect("write");
                };
                std::thread::sleep(Duration::from_millis(1500));
                // The OLDER sibling finishes: must not fold the gate.
                append(json!({"type": "user", "message": {"role": "user",
                        "content": [{"type": "tool_result",
                                     "tool_use_id": "toolu_older",
                                     "content": "older done"}]}}));
                std::thread::sleep(Duration::from_millis(2500));
                {
                    let conn = create_state_db(&state_db_path().expect("path")).expect("db");
                    let live: i64 = conn
                        .query_row(
                            "SELECT COUNT(*) FROM pending_approvals WHERE decision IS NULL",
                            [],
                            |row| row.get(0),
                        )
                        .expect("count");
                    assert_eq!(live, 1, "the older sibling's result must not fold the gate");
                }
                // The NEWEST (the bound one) finishes: folds.
                append(json!({"type": "user", "message": {"role": "user",
                        "content": [{"type": "tool_result",
                                     "tool_use_id": "toolu_newest",
                                     "content": "newest done"}]}}));
            })
        };
        let result = gate_with_timeout(payload, Duration::from_secs(60));
        writer.join().expect("writer");
        let _ = fs::remove_file(&transcript);
        assert_eq!(result, json!({}), "the newest candidate's answer folds it");
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let decision: Option<String> = conn
            .query_row("SELECT decision FROM pending_approvals", [], |row| {
                row.get(0)
            })
            .expect("row");
        assert_eq!(decision.as_deref(), Some("expired"), "row settled");
    }

    /// An EMPTY seed (the call's tool_use not flushed when the hook fires):
    /// the increments adopt the first recent match and its result folds the
    /// gate.
    #[test]
    fn an_empty_seed_adopts_from_increments_and_folds() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("seed-empty", true, 120);
        let transcript =
            std::env::temp_dir().join(format!("tinyctb-seed-empty-{}.jsonl", std::process::id()));
        fs::write(
            &transcript,
            format!(
                "{}\n",
                json!({"type": "user", "message": {"role": "user", "content": "go"}})
            ),
        )
        .expect("seed transcript");
        let mut payload = bash_payload();
        payload["transcript_path"] = json!(transcript.display().to_string());

        let writer = {
            let transcript = transcript.clone();
            std::thread::spawn(move || {
                use std::io::Write as _;
                std::thread::sleep(Duration::from_millis(1500));
                let mut file = fs::OpenOptions::new()
                    .append(true)
                    .open(&transcript)
                    .expect("append");
                // The tool_use flushes late, then its result lands.
                writeln!(
                    file,
                    "{}",
                    json!({"type": "assistant", "timestamp": "1970-01-01T00:00:01.200Z",
                           "message": {"role": "assistant", "content": [
                        {"type": "tool_use", "id": "toolu_late", "name": "Bash",
                         "input": {"command": "rm -rf build/"}}
                    ]}})
                )
                .expect("write");
                writeln!(
                    file,
                    "{}",
                    json!({"type": "user", "message": {"role": "user",
                            "content": [{"type": "tool_result",
                                         "tool_use_id": "toolu_late",
                                         "content": "done"}]}})
                )
                .expect("write");
            })
        };
        let started = std::time::Instant::now();
        let result = gate_with_timeout(payload, Duration::from_secs(60));
        writer.join().expect("writer");
        let _ = fs::remove_file(&transcript);
        assert_eq!(result, json!({}), "the late-adopted answer folds it");
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "the fold must come from the increments, not a window timeout: {:?}",
            started.elapsed()
        );
    }

    /// The terminal's permission dialog renders CONCURRENTLY with the
    /// blocking hook (re-verified 2026-08-28 under a pty capture), so the
    /// user can answer it there while the phone holds buttons. When THIS
    /// call's tool_result lands in the transcript, the gate must fold the
    /// phone side — settle the row and withdraw its queued push — instead of
    /// holding (and renewing) dead buttons for a command that already ran.
    #[test]
    fn an_answer_at_the_concurrent_dialog_folds_the_phone_side() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("terminal-answer", true, 120);
        let transcript = std::env::temp_dir().join(format!(
            "tinyctb-terminal-answer-{}.jsonl",
            std::process::id()
        ));
        fs::write(
            &transcript,
            format!(
                "{}\n{}\n",
                json!({"type": "user", "message": {"role": "user", "content": "go"}}),
                json!({"type": "assistant", "timestamp": "1970-01-01T00:00:01.000Z",
                       "message": {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_probe", "name": "Bash",
                     "input": {"command": "rm -rf build/"}}
                ]}})
            ),
        )
        .expect("seed transcript");
        let mut payload = bash_payload();
        payload["transcript_path"] = json!(transcript.display().to_string());

        let writer = {
            let transcript = transcript.clone();
            std::thread::spawn(move || {
                use std::io::Write as _;
                std::thread::sleep(Duration::from_millis(1500));
                // The user answers the terminal dialog; the tool runs and
                // its result lands in the transcript.
                let mut file = fs::OpenOptions::new()
                    .append(true)
                    .open(&transcript)
                    .expect("append");
                writeln!(
                    file,
                    "{}",
                    json!({"type": "user", "message": {"role": "user",
                            "content": [{"type": "tool_result",
                                         "tool_use_id": "toolu_probe",
                                         "content": "removed"}]}})
                )
                .expect("write");
            })
        };
        let started = std::time::Instant::now();
        let result = gate_with_timeout(payload, Duration::from_secs(60));
        writer.join().expect("writer");
        let _ = fs::remove_file(&transcript);

        assert_eq!(result, json!({}), "the terminal already decided it");
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "the gate must fold on the tool_result, not sit out its window: {:?}",
            started.elapsed()
        );
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let decision: Option<String> = conn
            .query_row("SELECT decision FROM pending_approvals", [], |row| {
                row.get(0)
            })
            .expect("row");
        assert_eq!(
            decision.as_deref(),
            Some("expired"),
            "the row must settle so /threads stops offering a dead button"
        );
        let stale_pushes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events WHERE status IN ('pending', 'failed')",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(stale_pushes, 0, "the queued push must be withdrawn");
    }

    /// An interrupted gate leaves its row un-decided with a lapsed
    /// expires_at. A re-run (only headless re-runs share an id — PreToolUse
    /// carries tool_use_id, PermissionRequest does not) must NOT sit out a
    /// fresh window waiting behind buttons whose taps already report
    /// "已过期": publication settles the lapsed row in its own transaction
    /// and the gate resolves it the way a timeout does — for headless, an
    /// immediate deny.
    #[test]
    fn a_rerun_onto_a_lapsed_row_denies_immediately() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("lapsed-rerun", true, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        // The row a killed gate left behind: same tool_use_id, never
        // decided, deadline long past.
        crate::state::create_pending_approval(
            &conn,
            "toolu_headless_1",
            "sess-headless",
            "Bash",
            "Bash: rm -rf build/",
            true,
            10,
            20,
        )
        .expect("stale row");
        // …and its push still sitting on the retry schedule. Settling the
        // row without withdrawing this would ship buttons already known to
        // be dead.
        assert!(crate::state::enqueue_outbound_event(
            &conn,
            &json!({
                "type": "approval_request",
                "threadId": "sess-headless",
                "eventKey": "approval:toolu_headless_1",
                "lastPreview": "Bash: rm -rf build/"
            }),
            15,
            "bridge",
        )
        .expect("stale push"));
        drop(conn);

        let started = std::time::Instant::now();
        let result = headless_gate(headless_payload());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "a lapsed row must resolve immediately, not hold a fresh window: {:?}",
            started.elapsed()
        );
        let out = &result["hookSpecificOutput"];
        assert_eq!(out["permissionDecision"], "deny", "{result}");
        // And the row is now settled, so its buttons answer honestly.
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        assert_eq!(
            crate::state::approval_decision(&conn, "toolu_headless_1")
                .expect("decision")
                .as_deref(),
            Some("expired"),
            "publication must settle the lapsed row"
        );
        let stale_pushes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events
                 WHERE json_extract(payload_json, '$.eventKey') = 'approval:toolu_headless_1'
                   AND status IN ('pending', 'failed')",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(
            stale_pushes, 0,
            "settling the lapsed row must also withdraw its queued push"
        );
    }

    /// A windowed interactive gate whose window lapses while away is still
    /// on must stay LOUD, not go quiet: the lapsed request settles, a fresh
    /// one is published with live buttons, the dead button honestly reports
    /// expired, and the fresh button still decides the tool. (Measured
    /// 2026-08-27 before this: buttons died after one hour and the blocked
    /// session then sat silent for seven.)
    #[test]
    fn a_lapsed_window_renews_and_the_fresh_button_decides() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("renewal", true, 5);
        let tapper = std::thread::spawn(move || {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            // The first window is 5 seconds; wait for the renewal to appear.
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            let (lapsed_id, renewed_id) = loop {
                let mut stmt = conn
                    .prepare("SELECT approval_id FROM pending_approvals ORDER BY created_at")
                    .expect("prepare");
                let ids = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .expect("query")
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .expect("ids");
                if ids.len() >= 2 {
                    break (ids[0].clone(), ids[1].clone());
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "no renewed approval appeared; rows: {ids:?}"
                );
                std::thread::sleep(Duration::from_millis(100));
            };
            assert_eq!(
                renewed_id,
                format!("{lapsed_id}:r1"),
                "the renewal must be a fresh round of the same request"
            );
            // The dead button from the lapsed window must not decide anything.
            assert_eq!(
                crate::state::record_approval_decision(&conn, &lapsed_id, "allow", 9000)
                    .expect("dead tap"),
                crate::state::ApprovalAnswer::Expired,
                "a lapsed round's button must report expired"
            );
            // The fresh button is live and decides the tool.
            assert_eq!(
                crate::state::record_approval_decision(&conn, &renewed_id, "allow", 9000)
                    .expect("live tap"),
                crate::state::ApprovalAnswer::Recorded,
                "the renewed round's button must accept the answer"
            );
        });
        let result = gate_with_timeout(bash_payload(), Duration::from_secs(60));
        tapper.join().expect("tapper");

        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"], "allow",
            "the answer given on the renewed buttons must decide the tool: {result}"
        );
        // Each round shipped its own message: two pushes, not one.
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let pushes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events WHERE event_type = 'approval_request'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(pushes, 2, "the renewal must publish a fresh push");
    }

    /// A HEADLESS turn must never be waved through by transcript evidence.
    /// Under `bypassPermissions` a `{}` reply means "run it", so a foreign
    /// assistant record — a parallel branch, a subagent's parent turn moving
    /// on — landing while the gate waits must NOT become permission. The
    /// only honest endings stay: an explicit Telegram decision, or deny.
    #[test]
    fn headless_gate_is_not_released_by_a_foreign_assistant_record() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-foreign-assistant", true, 5);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        drop(conn);
        let transcript = std::env::temp_dir().join(format!(
            "tinyctb-headless-foreign-{}.jsonl",
            std::process::id()
        ));
        fs::write(&transcript, "").expect("seed");
        let mut payload = headless_payload();
        payload["transcript_path"] = json!(transcript.display().to_string());

        let writer = {
            let transcript = transcript.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(500));
                use std::io::Write as _;
                let mut file = fs::OpenOptions::new()
                    .append(true)
                    .open(&transcript)
                    .expect("append");
                writeln!(
                    file,
                    "{}",
                    json!({"type": "assistant", "message": {"role": "assistant",
                            "content": [{"type": "text", "text": "some other branch"}]}})
                )
                .expect("write");
            })
        };
        let started = std::time::Instant::now();
        let result = headless_gate(payload);
        writer.join().expect("writer");
        let _ = fs::remove_file(&transcript);

        assert_eq!(
            result["hookSpecificOutput"]["permissionDecision"], "deny",
            "transcript evidence must never authorise a headless call: {result}"
        );
        assert!(
            started.elapsed() >= Duration::from_secs(4),
            "and it must wait out its window rather than exit on the record: {:?}",
            started.elapsed()
        );
    }

    /// away OFF silences BACKGROUND sessions too. The window probe answers
    /// "how is this session hosted", not "is anyone watching": a task forked
    /// into the daemon's bg-pty-host is answerable through its parent
    /// session's task panel, so with away off it must wait in its own pty
    /// exactly as it would without tinyctb — no row minted, no push, for
    /// both gates. (The old rule exempted background sessions from the away
    /// shortcut; measured 2026-08-28, /back still pushed a background
    /// task's question to the phone and the user rightly called it a bug.)
    #[test]
    fn background_sessions_go_silent_with_away_off_too() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("windowless-away-off", false, 5);
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "1");

        let started = std::time::Instant::now();
        let approval = gate(bash_payload());
        let question = question_gate(question_payload());
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "0");
        assert_eq!(approval, json!({}), "the terminal side owns it");
        assert_eq!(question, json!({}), "the terminal side owns it");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "away off must step aside immediately, not wait any window: {:?}",
            started.elapsed()
        );
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let minted: i64 = conn
            .query_row(
                "SELECT (SELECT COUNT(*) FROM pending_approvals)
                      + (SELECT COUNT(*) FROM pending_questions)
                      + (SELECT COUNT(*) FROM outbound_events)",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(
            minted, 0,
            "away off must mint no rows and push nothing, background or not"
        );
    }

    /// The mirror: a session that DOES have a terminal window, with away off,
    /// belongs to the terminal outright — no row, no push, no wait.
    #[test]
    fn windowed_question_with_away_off_returns_immediately() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("windowed-away-off", false, 30);

        let started = std::time::Instant::now();
        let result = question_gate(question_payload());
        assert_eq!(result, json!({}));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the terminal owns it, so the gate must not wait: {:?}",
            started.elapsed()
        );
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let asked: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_questions", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(asked, 0, "and must not mint a question row");
    }

    /// The question gate's two windowless behaviours, together: the row gets
    /// the day-long window (not the configured 30s), and the phone answer is
    /// what ends the wait. (This wait used to also exit on transcript
    /// evidence; that watcher was removed with presence detection — under
    /// the current TUI no dialog renders while the hook blocks, so nothing
    /// legitimate can decide the call behind the gate's back.)
    #[test]
    fn windowless_question_gets_the_long_window_and_the_phone_decides() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("windowless-question", true, 30);
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "1");

        let answerer = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            loop {
                if let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) {
                    if matches!(
                        crate::state::record_question_answer(&conn, &question_id, "SQLite", 2000),
                        Ok(crate::state::ApprovalAnswer::Recorded)
                    ) {
                        return;
                    }
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "question row never appeared"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        let started = std::time::Instant::now();
        let result = question_gate(question_payload());
        answerer.join().expect("answerer");
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "0");

        assert_eq!(
            result["hookSpecificOutput"]["hookEventName"], "PreToolUse",
            "the phone answer must complete the call: {result}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the phone answer must end the wait, not the 24h window: {:?}",
            started.elapsed()
        );
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let (created_at, expires_at): (i64, i64) = conn
            .query_row(
                "SELECT created_at, expires_at FROM pending_questions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("question row");
        assert_eq!(
            expires_at - created_at,
            WINDOWLESS_APPROVAL_WAIT.as_millis() as i64,
            "a windowless question must get the day-long window, not the configured 30s"
        );
    }

    /// A session with NO terminal window (background pty host) has nowhere
    /// to hand anything TO: the dialog would land in a pty nobody is
    /// watching and the tool would stay blocked. Measured 2026-08-17:
    /// exactly this froze a cchess session for 7h09m. The push goes out and
    /// the remote window stays open for a full day, which is also what
    /// makes /threads able to re-offer the buttons.
    #[test]
    fn windowless_session_keeps_the_remote_window_open() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("windowless-present", true, 30);
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "1");

        let answerer = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(1500));
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            let id: String = conn
                .query_row("SELECT approval_id FROM pending_approvals", [], |row| {
                    row.get(0)
                })
                .expect("an approval row");
            crate::state::record_approval_decision(&conn, &id, "allow", 1000).expect("tap");
        });
        let result = gate(bash_payload());
        answerer.join().expect("answerer");
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "0");

        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"], "allow",
            "the remote tap must decide it, not a terminal that does not exist: {result}"
        );
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let (created_at, expires_at): (i64, i64) = conn
            .query_row(
                "SELECT created_at, expires_at FROM pending_approvals",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("approval row");
        assert_eq!(
            expires_at - created_at,
            WINDOWLESS_APPROVAL_WAIT.as_millis() as i64,
            "a windowless session must get the long window, not the configured 30s"
        );
    }

    /// Session-scoped auto-allow: a grant the user paid a tap for keeps
    /// firing without a fresh prompt anywhere.
    #[test]
    fn auto_allow_short_circuits_the_gate() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("auto-allow-present", true, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        crate::state::set_approval_auto_allow(&conn, "sess-gate", "Bash", 900).expect("auto allow");
        drop(conn);

        let result = gate(bash_payload());
        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"], "allow",
            "the existing session grant must fire: {result}"
        );
    }

    /// `/back` mid-wait hands the prompt to the terminal within a poll tick
    /// instead of holding it for the rest of the remote window. This is the
    /// ONE hand-back left — the user's declaration, not a guess — and it is
    /// what makes a long remote window safe to configure.
    #[test]
    fn gate_releases_to_the_terminal_on_back() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("user-returns", true, 30);

        let returner = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(1200));
            write_away_marker_for_test(false).expect("back");
        });
        let started = std::time::Instant::now();
        let result = gate(bash_payload());
        returner.join().expect("returner");

        assert_eq!(result, json!({}), "the prompt must fall to the terminal");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "released by /back, not the 30s window: {:?}",
            started.elapsed()
        );
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let decision: Option<String> = conn
            .query_row("SELECT decision FROM pending_approvals", [], |row| {
                row.get(0)
            })
            .expect("row");
        assert_eq!(decision.as_deref(), Some("expired"));
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            0,
            "the hand-back must withdraw the queued push — an unsent retry \
             delivering after /back would break the silence /back declares"
        );
    }

    /// A tap that lands just before `/back` still wins — the settle is
    /// atomic, not a blind expiry. Telegram already told the user the tap
    /// was accepted, so discarding it at the hand-back would show "已允许"
    /// while the session quietly re-asked in the terminal.
    #[test]
    fn a_tap_racing_the_hand_back_still_wins() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("tap-vs-return", true, 30);

        let worker = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(approval_id) = conn.query_row(
                    "SELECT approval_id FROM pending_approvals WHERE decision IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                // Tap first, /back right after.
                if matches!(
                    record_approval_decision(&conn, &approval_id, "allow", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    write_away_marker_for_test(false).expect("back");
                    return;
                }
            }
            panic!("approval row never appeared");
        });
        let result = gate(bash_payload());
        worker.join().expect("worker");
        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"], "allow",
            "the recorded tap must be honoured, not discarded by the hand-back: {result}"
        );
    }

    /// The safety rule: waiting out the clock is NOT consent.
    #[test]
    fn gate_timeout_never_allows() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("timeout", true, 5); // clamped minimum
        let started = std::time::Instant::now();
        // While away stays on, an unanswered window RENEWS instead of ending
        // the gate (silence once wedged a session for seven hours), so the
        // test observes the first window lapse — never allowing — and then
        // turns away off, which is one of the endings a real gate has.
        let watcher = std::thread::spawn(move || {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                let rows: i64 = conn
                    .query_row("SELECT COUNT(*) FROM pending_approvals", [], |row| {
                        row.get(0)
                    })
                    .expect("count");
                if rows >= 2 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the lapsed window must renew, not end the gate"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
            // The first round lapsed with nobody answering — and settled as
            // exactly that, never as any form of allow.
            let first: Option<String> = conn
                .query_row(
                    "SELECT decision FROM pending_approvals ORDER BY created_at LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .expect("first round");
            assert_eq!(first.as_deref(), Some("expired"));
            write_away_marker_for_test(false).expect("away off");
        });
        let result = gate_with_timeout(bash_payload(), Duration::from_secs(60));
        watcher.join().expect("watcher");
        assert_eq!(
            result,
            json!({}),
            "an unanswered gate must yield no opinion"
        );
        assert!(
            started.elapsed() >= Duration::from_secs(4),
            "the gate must actually wait for an answer"
        );

        // The request that was pushed carries the real command, not just a
        // tool name, and rides the bridge origin so /back cannot drop it.
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let (payload_json, origin): (String, String) = conn
            .query_row(
                "SELECT payload_json, origin FROM outbound_events ORDER BY created_at LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("outbound row");
        assert_eq!(origin, "bridge");
        assert!(payload_json.contains("rm -rf build/"), "{payload_json}");
        assert!(payload_json.contains("approval_request"));
    }

    #[test]
    fn gate_returns_the_answer_given_from_telegram() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("answered", true, 30);
        // Answer asynchronously, as the daemon would on a button tap.
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(approval_id) = conn.query_row(
                    "SELECT approval_id FROM pending_approvals WHERE decision IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    record_approval_decision(&conn, &approval_id, "deny", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("approval row never appeared");
        });
        let result = gate(bash_payload());
        handle.join().expect("answering thread");

        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"], "deny",
            "{result}"
        );
        assert_eq!(
            result["hookSpecificOutput"]["hookEventName"],
            "PermissionRequest"
        );
        assert!(result["hookSpecificOutput"]["decision"]["message"]
            .as_str()
            .expect("message")
            .contains("Telegram"));
    }

    /// P2 regression: once the gate has given up, the session has already
    /// fallen back to its own prompt. A late tap must be refused — and must
    /// not claim success, least of all for "allow for this session" (whose
    /// side effect the exited hook can no longer perform).
    #[test]
    fn late_tap_after_timeout_is_refused() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("late-tap", true, 5);
        // Same ending as `gate_timeout_never_allows`: observe the first
        // window lapse (and renew), then end the gate by turning away off.
        let watcher = std::thread::spawn(move || {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                let rows: i64 = conn
                    .query_row("SELECT COUNT(*) FROM pending_approvals", [], |row| {
                        row.get(0)
                    })
                    .expect("count");
                if rows >= 2 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the lapsed window must renew, not end the gate"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
            write_away_marker_for_test(false).expect("away off");
        });
        let result = gate_with_timeout(bash_payload(), Duration::from_secs(60));
        watcher.join().expect("watcher");
        assert_eq!(result, json!({}), "an unanswered gate yields no opinion");

        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        // The LAPSED round is the one whose buttons the phone has been
        // holding the longest — the late taps below all land on it.
        let approval_id: String = conn
            .query_row(
                "SELECT approval_id FROM pending_approvals ORDER BY created_at LIMIT 1",
                [],
                |row| row.get(0),
            )
            .expect("first round");
        let decision_now: Option<String> =
            crate::state::approval_decision(&conn, &approval_id).expect("decision");
        assert_eq!(
            decision_now.as_deref(),
            Some("expired"),
            "the timed-out request must be settled, not left answerable"
        );

        for action in ["allow", "allow_session", "deny"] {
            assert_eq!(
                crate::state::record_approval_decision(&conn, &approval_id, action, 9_000_000)
                    .expect("late tap"),
                crate::state::ApprovalAnswer::Expired,
                "a late {action} must be reported as timed out, not as handled"
            );
        }
        assert!(
            !crate::state::approval_auto_allowed(&conn, "sess-gate", "Bash").expect("auto allow"),
            "a late 'allow for session' must not silently grant the session"
        );
    }

    /// The race the timeout used to lose silently: an answer that lands
    /// between the final poll and the expiry must be honoured by the hook,
    /// because Telegram has already told the user it was accepted.
    #[test]
    fn answer_landing_at_the_deadline_is_honoured_not_expired() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::create_pending_approval(
            &conn,
            "race-1",
            "sess-race",
            "Bash",
            "Bash: ls",
            false,
            1000,
            5000,
        )
        .expect("create");
        // The tap wins the row first (still inside the deadline)…
        assert_eq!(
            crate::state::record_approval_decision(&conn, "race-1", "allow", 4000).expect("tap"),
            crate::state::ApprovalAnswer::Recorded
        );
        // …so the hook's expiry attempt must not overwrite it, and must hand
        // the decision back instead of reporting a timeout.
        assert_eq!(
            crate::state::expire_or_take_decision(&conn, "race-1", 5001).expect("expire"),
            Some("allow".to_string()),
            "a decision that beat the expiry must be returned to the hook"
        );
        assert_eq!(
            crate::state::approval_decision(&conn, "race-1").expect("decision"),
            Some("allow".to_string()),
            "the expiry must not clobber a recorded answer"
        );
    }

    /// The interleaving the sequential tests cannot reach: the callback reads
    /// "unanswered", the hook expires the row, then the callback's update
    /// fails. Classifying that as "already handled" would tell the user their
    /// tap was redundant when it actually timed out. Raced repeatedly so the
    /// window is genuinely hit.
    #[test]
    fn racing_expiry_and_tap_never_misreports_the_outcome() {
        let dir = std::env::temp_dir().join(format!("tinyctb-race-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let db_path = dir.join("state.db");

        for round in 0..60u64 {
            let approval_id = format!("race-{round}");
            {
                let conn = create_state_db(&db_path).expect("db");
                crate::state::create_pending_approval(
                    &conn,
                    &approval_id,
                    "sess-race",
                    "Bash",
                    "Bash: ls",
                    false,
                    1000,
                    5000,
                )
                .expect("create");
            }
            let expire_path = db_path.clone();
            let expire_id = approval_id.clone();
            let expirer = std::thread::spawn(move || {
                let conn = create_state_db(&expire_path).expect("db");
                crate::state::expire_or_take_decision(&conn, &expire_id, 4000).expect("expire")
            });
            let tap_path = db_path.clone();
            let tap_id = approval_id.clone();
            let tapper = std::thread::spawn(move || {
                let conn = create_state_db(&tap_path).expect("db");
                crate::state::record_approval_decision(&conn, &tap_id, "allow", 4000).expect("tap")
            });
            let taken = expirer.join().expect("expirer");
            let outcome = tapper.join().expect("tapper");

            let conn = create_state_db(&db_path).expect("db");
            let final_decision = crate::state::approval_decision(&conn, &approval_id)
                .expect("decision")
                .expect("settled");
            match final_decision.as_str() {
                // The tap won: the hook must have been handed that decision.
                "allow" => {
                    assert_eq!(outcome, crate::state::ApprovalAnswer::Recorded);
                    assert_eq!(taken.as_deref(), Some("allow"));
                }
                // The expiry won: the tap must be told it timed out, never
                // "already handled".
                "expired" => {
                    assert_eq!(
                        outcome,
                        crate::state::ApprovalAnswer::Expired,
                        "round {round}: a tap that lost to the expiry must report a timeout"
                    );
                    assert_eq!(taken, None);
                }
                other => panic!("unexpected final state {other}"),
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// And the other way round: when the hook expires first, the button is
    /// told it timed out — not "already handled".
    #[test]
    fn expiry_winning_the_race_tells_the_user_it_timed_out() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::create_pending_approval(
            &conn,
            "race-2",
            "sess-race",
            "Bash",
            "Bash: ls",
            false,
            1000,
            5000,
        )
        .expect("create");
        assert_eq!(
            crate::state::expire_or_take_decision(&conn, "race-2", 5001).expect("expire"),
            None,
            "no answer had landed, so the hook expires it"
        );
        assert_eq!(
            crate::state::record_approval_decision(&conn, "race-2", "allow", 5002)
                .expect("late tap"),
            crate::state::ApprovalAnswer::Expired
        );
    }

    fn question_payload() -> Value {
        json!({
            "hook_event_name": "PreToolUse",
            "session_id": "sess-q",
            "tool_name": "AskUserQuestion",
            "permission_mode": "default",
            "cwd": "/home/user/project",
            "tool_input": { "questions": [{
                "question": "这个项目用哪个数据库？",
                "options": [
                    {"label": "Postgres", "description": "关系型"},
                    {"label": "SQLite", "description": "嵌入式"}
                ]
            }]}
        })
    }

    fn question_gate(payload: Value) -> Value {
        let mut reader = std::io::Cursor::new(payload.to_string());
        run_question_gate(&mut reader, 1000).expect("question gate")
    }

    /// The fixture's isolation is itself a contract: every variable it
    /// touches must come back, panic or not. A leaked seam does not fail the
    /// test that leaked it — it silently rewrites what a LATER test measures
    /// (a window seam left unset once made two tests read the developer's own
    /// session and pass only on this machine).
    #[test]
    fn the_gate_fixture_restores_every_variable_it_touches() {
        let _guard = crate::state::test_env_lock();
        let watched = [
            "TINYCTB_STATE_DIR",
            crate::claude::BRIDGE_TURN_ENV,
            "TINYCTB_TEST_SESSION_WINDOWLESS",
            "TINYCTB_TEST_SESSION_TTY",
        ];
        let sentinels: Vec<_> = watched
            .iter()
            .map(|key| crate::state::EnvVarGuard::set(key, format!("sentinel-for-{key}")))
            .collect();
        {
            let _env = GateEnv::new("fixture-raii", true, 5);
            // Inside, the fixture's own values are in force.
            assert_eq!(
                std::env::var("TINYCTB_TEST_SESSION_WINDOWLESS").ok(),
                Some("0".to_string()),
                "the window probe must be pinned, never inherited"
            );
            assert!(std::env::var(crate::claude::BRIDGE_TURN_ENV).is_err());
            assert!(std::env::var("TINYCTB_TEST_SESSION_TTY").is_err());
        }
        for key in watched {
            assert_eq!(
                std::env::var(key).ok(),
                Some(format!("sentinel-for-{key}")),
                "{key} must be restored when the fixture drops"
            );
        }
        drop(sentinels);
    }

    /// The user's law for the blocked window: the terminal must always show
    /// the question, never phone-only. While the hook blocks, Claude Code
    /// paints nothing, so the gate itself must paint — the question with its
    /// options at publish time, and a receipt when the phone answers.
    #[test]
    fn a_blocked_question_paints_a_banner_and_a_phone_receipt_on_the_tty() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-banner", true, 30);
        let tty = _env.root.join("fake-tty.txt");
        fs::write(&tty, "").expect("fake tty");
        std::env::set_var("TINYCTB_TEST_SESSION_TTY", &tty);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    crate::state::record_question_answer(&conn, &question_id, "SQLite", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        let result = question_gate(question_payload());
        handle.join().expect("answering thread");

        let painted = fs::read_to_string(&tty).expect("painted tty");
        assert!(painted.contains("有提问待作答"), "{painted}");
        assert!(painted.contains("这个项目用哪个数据库？"), "{painted}");
        assert!(
            painted.contains("[A] Postgres") && painted.contains("[B] SQLite"),
            "options with their letter codes must be on the banner: {painted}"
        );
        assert!(painted.contains("已由手机作答：SQLite"), "{painted}");
        // The durable receipt rides the hook reply itself — the tty banner
        // is chewed by the post-hook repaint and never reaches scrollback.
        assert_eq!(
            result["systemMessage"], "tinyCTB：已由手机作答「SQLite」",
            "{result}"
        );
    }

    /// A `/back` hand-over exits with no opinion; the banner must close on
    /// the hand-over rather than a still-waiting promise.
    #[test]
    fn a_reclaimed_question_paints_a_handover_note() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-handover", true, 30);
        let tty = _env.root.join("fake-tty.txt");
        fs::write(&tty, "").expect("fake tty");
        std::env::set_var("TINYCTB_TEST_SESSION_TTY", &tty);
        let returner = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(1200));
            write_away_marker_for_test(false).expect("back");
        });
        let started = std::time::Instant::now();
        let result = question_gate(question_payload());
        // Capture the gate's OWN duration before joining the returner — the
        // join would otherwise fold the returner's 1200ms sleep into the
        // measurement and make the timing assertion below vacuous.
        let gate_elapsed = started.elapsed();
        returner.join().expect("returner");
        assert_eq!(result, json!({}), "the hand-over must exit with no opinion");
        // It must have WAITED for /back, not handed over on the first poll.
        // With away still on and nobody answering, the gate holds; only the
        // /back at 1200ms releases it. A gate that handed back unconditionally
        // (the pre-2026-08-27 behavior) would return within a poll tick.
        assert!(
            gate_elapsed >= Duration::from_millis(1000),
            "the gate must hold until /back, not hand over while away is on: {gate_elapsed:?}"
        );
        let painted = fs::read_to_string(&tty).expect("painted tty");
        assert!(painted.contains("有提问待作答"), "{painted}");
        assert!(painted.contains("已交还本终端"), "{painted}");
        // The settle stamp is wall-clock time, not the gate's start instant:
        // a stamp equal to `created_at` collapses "born" and "handed back"
        // into one millisecond and forensics can no longer order them
        // (exactly how the 2026-08-22 production autopsy went wrong).
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let (created, stamped): (i64, i64) = conn
            .query_row(
                "SELECT created_at, answered_at FROM pending_questions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("settled row");
        assert!(
            stamped > created,
            "settle must stamp real time ({stamped} vs created {created})"
        );
    }

    /// A windowless session has no terminal anyone watches: its Telegram
    /// window is the only dialog, and painting its hidden pty would be
    /// writing to nobody.
    #[test]
    fn a_windowless_session_paints_no_banner() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-windowless-banner", true, 30);
        let tty = _env.root.join("fake-tty.txt");
        fs::write(&tty, "").expect("fake tty");
        std::env::set_var("TINYCTB_TEST_SESSION_TTY", &tty);
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "1");
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    crate::state::record_question_answer(&conn, &question_id, "SQLite", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        let result = question_gate(question_payload());
        handle.join().expect("answering thread");
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "0");
        // The answer still flows — with its receipt — but the tty stays
        // untouched.
        assert_eq!(
            result["hookSpecificOutput"]["updatedInput"]["answers"]["这个项目用哪个数据库？"],
            "SQLite",
            "{result}"
        );
        assert_eq!(
            fs::read_to_string(&tty).expect("fake tty"),
            "",
            "a windowless session must not paint"
        );
    }

    /// Not painting is only half of it: the phone must be TOLD that no
    /// terminal can show this question, or the reader assumes a dialog is
    /// waiting for them at the desk. Production 2026-08-23 — an M3d launch
    /// question from a background session read as "the terminal skipped it".
    /// The windowed case must keep saying the opposite, so the flag is a
    /// measurement rather than a decoration.
    #[test]
    fn a_question_push_carries_whether_any_terminal_can_show_it() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-windowless-flag", true, 30);
        let answer_from_phone = || {
            std::thread::spawn(|| {
                let conn = create_state_db(&state_db_path().expect("path")).expect("db");
                for _ in 0..100 {
                    std::thread::sleep(Duration::from_millis(100));
                    let Ok(question_id) = conn.query_row(
                        "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                        [],
                        |row| row.get::<_, String>(0),
                    ) else {
                        continue;
                    };
                    if matches!(
                        crate::state::record_question_answer(&conn, &question_id, "SQLite", 2000),
                        Ok(crate::state::ApprovalAnswer::Recorded)
                    ) {
                        return;
                    }
                }
                panic!("question row never appeared");
            })
        };
        let pushed_flag = || -> Value {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            let payload: String = conn
                .query_row(
                    "SELECT payload_json FROM outbound_events WHERE event_type = 'question_request'",
                    [],
                    |row| row.get(0),
                )
                .expect("pushed question event");
            serde_json::from_str::<Value>(&payload).expect("event json")["terminalVisibility"]
                .clone()
        };

        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "1");
        let handle = answer_from_phone();
        question_gate(question_payload());
        handle.join().expect("answering thread");
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "0");
        assert_eq!(
            pushed_flag(),
            json!("background"),
            "a background session's push must say its terminal cannot show the question"
        );

        // Same gate, same phone, a session that DOES have a window: the
        // single-row query above is the assertion that the first push is
        // gone, so clear both tables before the second half.
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        conn.execute("DELETE FROM outbound_events", [])
            .expect("clear pushes");
        conn.execute("DELETE FROM pending_questions", [])
            .expect("clear questions");
        drop(conn);
        let handle = answer_from_phone();
        question_gate(question_payload());
        handle.join().expect("answering thread");
        assert_eq!(
            pushed_flag(),
            json!("window"),
            "a windowed session's push must not claim to be terminal-less"
        );
    }

    /// The approval push needs the same fact for a sharper reason: the
    /// interactive hint promises "超时后回落到终端里的权限弹窗", and for a
    /// background session that fallback is a dialog nobody can see (measured
    /// 2026-08-17: 7h09m blocked behind exactly one of those).
    #[test]
    fn an_approval_push_carries_whether_any_terminal_can_show_it() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("approval-windowless-flag", true, 30);
        let answer_from_phone = || {
            std::thread::spawn(|| {
                let conn = create_state_db(&state_db_path().expect("path")).expect("db");
                for _ in 0..100 {
                    std::thread::sleep(Duration::from_millis(100));
                    let Ok(approval_id) = conn.query_row(
                        "SELECT approval_id FROM pending_approvals WHERE decision IS NULL",
                        [],
                        |row| row.get::<_, String>(0),
                    ) else {
                        continue;
                    };
                    if matches!(
                        record_approval_decision(&conn, &approval_id, "allow", 2000),
                        Ok(crate::state::ApprovalAnswer::Recorded)
                    ) {
                        return;
                    }
                }
                panic!("approval row never appeared");
            })
        };
        let gate = || {
            let mut reader = std::io::Cursor::new(bash_payload().to_string());
            run_approval_gate(&mut reader, 1000).expect("gate")
        };
        let pushed_flag = || -> Value {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            let payload: String = conn
                .query_row(
                    "SELECT payload_json FROM outbound_events WHERE event_type = 'approval_request'",
                    [],
                    |row| row.get(0),
                )
                .expect("pushed approval event");
            serde_json::from_str::<Value>(&payload).expect("event json")["terminalVisibility"]
                .clone()
        };

        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "1");
        let handle = answer_from_phone();
        gate();
        handle.join().expect("answering thread");
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "0");
        assert_eq!(
            pushed_flag(),
            json!("background"),
            "a background session's approval must not promise a terminal fallback"
        );

        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        conn.execute("DELETE FROM outbound_events", [])
            .expect("clear pushes");
        conn.execute("DELETE FROM pending_approvals", [])
            .expect("clear approvals");
        drop(conn);
        let handle = answer_from_phone();
        gate();
        handle.join().expect("answering thread");
        assert_eq!(
            pushed_flag(),
            json!("window"),
            "a windowed session's approval keeps its terminal-fallback hint"
        );
    }

    /// The probe can fail — no socket variable, unreadable /proc, a platform
    /// without one. The WAIT still follows the old policy (treat it like a
    /// session with a window: paint the banner, keep the configured window),
    /// but the message must publish the shrug rather than a fact nobody
    /// measured.
    #[test]
    fn an_unprobeable_session_publishes_the_shrug_but_keeps_the_old_policy() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-unverified", true, 30);
        let tty = _env.root.join("fake-tty.txt");
        fs::write(&tty, "").expect("fake tty");
        std::env::set_var("TINYCTB_TEST_SESSION_TTY", &tty);
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "unverified");
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    crate::state::record_question_answer(&conn, &question_id, "SQLite", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        question_gate(question_payload());
        handle.join().expect("answering thread");
        std::env::set_var("TINYCTB_TEST_SESSION_WINDOWLESS", "0");

        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let payload: String = conn
            .query_row(
                "SELECT payload_json FROM outbound_events WHERE event_type = 'question_request'",
                [],
                |row| row.get(0),
            )
            .expect("pushed question event");
        let (expires_at, created_at): (i64, i64) = conn
            .query_row(
                "SELECT expires_at, created_at FROM pending_questions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("question row");
        drop(conn);
        assert_eq!(
            serde_json::from_str::<Value>(&payload).expect("event json")["terminalVisibility"],
            json!("unverified"),
            "a failed probe must not be published as a measured window"
        );
        // Policy unchanged: the configured 30s window (not the day a
        // confirmed background session gets) and the banner still painted.
        assert_eq!(
            expires_at - created_at,
            30_000,
            "an unverified session keeps the configured window"
        );
        assert!(
            fs::read_to_string(&tty)
                .expect("fake tty")
                .contains("有提问待作答"),
            "and still gets its banner"
        );
    }

    /// A headless turn never runs the probe: it has no terminal at all, and
    /// its own `headless` flag says so. The event must therefore CARRY NO
    /// window claim — an omitted field, not a fabricated "window".
    #[test]
    fn a_headless_approval_publishes_no_window_claim() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("headless-no-window-claim", false, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        enter_bridge_turn(&conn, "sess-headless");
        drop(conn);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(approval_id) = conn.query_row(
                    "SELECT approval_id FROM pending_approvals WHERE decision IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    record_approval_decision(&conn, &approval_id, "allow", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("approval row never appeared");
        });
        headless_gate(headless_payload());
        handle.join().expect("answering thread");

        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let payload: String = conn
            .query_row(
                "SELECT payload_json FROM outbound_events WHERE event_type = 'approval_request'",
                [],
                |row| row.get(0),
            )
            .expect("pushed approval event");
        drop(conn);
        let event = serde_json::from_str::<Value>(&payload).expect("event json");
        assert_eq!(event["headless"], json!(true), "{event}");
        assert!(
            event.get("terminalVisibility").is_none(),
            "a gate that never probed must claim nothing: {event}"
        );
    }

    /// A pty whose output queue is already full: nobody reads the master,
    /// and junk written to the slave up-front leaves no room for more. Any
    /// further slave write blocks (or `WouldBlock`s) until someone drains
    /// the master — the shape a ^S/XOFF-paused or wedged terminal presents.
    /// The returned master must stay alive for the wedge to hold.
    #[cfg(target_os = "linux")]
    fn wedged_pty() -> (std::fs::File, std::path::PathBuf) {
        use std::os::unix::io::FromRawFd as _;
        unsafe {
            let master = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY);
            assert!(master >= 0, "posix_openpt failed");
            assert_eq!(libc::grantpt(master), 0, "grantpt failed");
            assert_eq!(libc::unlockpt(master), 0, "unlockpt failed");
            let mut name = [0 as libc::c_char; 256];
            assert_eq!(
                libc::ptsname_r(master, name.as_mut_ptr(), name.len()),
                0,
                "ptsname_r failed"
            );
            let path = std::ffi::CStr::from_ptr(name.as_ptr())
                .to_string_lossy()
                .into_owned();
            let slave = libc::open(
                name.as_ptr(),
                libc::O_WRONLY | libc::O_NONBLOCK | libc::O_NOCTTY,
            );
            assert!(slave >= 0, "slave open failed");
            let junk = [b'x'; 1024];
            loop {
                if libc::write(slave, junk.as_ptr().cast(), junk.len()) <= 0 {
                    break;
                }
            }
            libc::close(slave);
            (
                std::fs::File::from_raw_fd(master),
                std::path::PathBuf::from(path),
            )
        }
    }

    /// An `Interrupted` storm never touches the `WouldBlock` arm, so a
    /// budget checked only there would spin forever. The top-of-lap check
    /// must end it — and the recv here, not a hung test binary, is what
    /// goes red if that check is lost.
    #[test]
    fn an_interrupt_storm_cannot_outlive_the_write_budget() {
        struct InterruptStorm;
        impl std::io::Write for InterruptStorm {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::Interrupted.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(write_all_within(
                InterruptStorm,
                b"banner",
                Duration::from_millis(200),
            ));
        });
        let outcome = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the budget must bound an Interrupted storm");
        assert_eq!(
            outcome.expect_err("storm must time out").kind(),
            std::io::ErrorKind::TimedOut
        );
    }

    /// One byte per lap passes every per-error check; only a strict
    /// top-of-lap budget stops a tty that dribbles without ever finishing.
    #[test]
    fn a_dribbling_tty_still_exhausts_the_budget() {
        struct Dribble;
        impl std::io::Write for Dribble {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                std::thread::sleep(Duration::from_millis(20));
                Ok(1)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let outcome = write_all_within(Dribble, &[b'z'; 640], Duration::from_millis(200));
        assert_eq!(
            outcome.expect_err("dribble must time out").kind(),
            std::io::ErrorKind::TimedOut
        );
    }

    /// `Ok(0)` is a tty accepting nothing, forever — success here would
    /// silently drop the rest of the banner.
    #[test]
    fn a_zero_write_is_an_error_not_quiet_success() {
        struct Zero;
        impl std::io::Write for Zero {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Ok(0)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let outcome = write_all_within(Zero, b"banner", Duration::from_secs(5));
        assert_eq!(
            outcome.expect_err("zero write must fail").kind(),
            std::io::ErrorKind::WriteZero
        );
    }

    /// A blocking write would park the gate in the kernel with no error to
    /// catch. Against a wedged REAL pty the bounded write must come back
    /// within its budget and say so.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_wedged_tty_cannot_hang_a_banner_write() {
        let (_master, slave) = wedged_pty();
        let (tx, rx) = std::sync::mpsc::channel();
        let target = slave.clone();
        std::thread::spawn(move || {
            let _ = tx.send(write_bounded(
                &target,
                &[b'y'; 4096],
                Duration::from_millis(300),
            ));
        });
        // This recv IS the red path for a blocking regression: a writer
        // parked in the kernel never sends, and the assertion below — not a
        // hung test binary — is what fails.
        let outcome = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("write_bounded must return within its budget");
        let err = outcome.expect_err("a wedged tty must be reported, not ignored");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "{err}");
    }

    /// Sol's P1 scenario end-to-end: both banner writes (publish + receipt)
    /// hit a wedged tty, and the phone answer must still be consumed and
    /// returned. The banner is cosmetic; the answer is not.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_wedged_tty_still_lets_the_phone_answer_land() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-wedged-tty", true, 30);
        let (_master, slave) = wedged_pty();
        std::env::set_var("TINYCTB_TEST_SESSION_TTY", &slave);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    crate::state::record_question_answer(&conn, &question_id, "SQLite", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        // The gate runs on a WORKER thread with a recv_timeout around it: a
        // blocking-write regression parks the gate in the kernel, and an
        // elapsed assertion after the call would simply never run. The recv
        // is what goes red.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(question_gate(question_payload()));
        });
        let result = rx
            .recv_timeout(Duration::from_secs(20))
            .expect("the gate must return despite a wedged tty");
        handle.join().expect("answering thread");
        assert_eq!(
            result["hookSpecificOutput"]["updatedInput"]["answers"]["这个项目用哪个数据库？"],
            "SQLite",
            "{result}"
        );
    }

    /// Model-authored text is a terminal attack surface: a JSON-legal
    /// string can smuggle ESC/OSC/CSI/BEL/CR. Nothing of that may reach the
    /// tty — only the banner's own fixed ANSI — and the printable tails
    /// survive as harmless visible text.
    #[test]
    fn hostile_control_sequences_never_reach_the_tty() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-hostile", true, 30);
        let tty = _env.root.join("fake-tty.txt");
        fs::write(&tty, "").expect("fake tty");
        std::env::set_var("TINYCTB_TEST_SESSION_TTY", &tty);
        let payload = json!({
            "hook_event_name": "PreToolUse",
            "session_id": "sess-hostile",
            "tool_name": "AskUserQuestion",
            "permission_mode": "default",
            "cwd": "/home/user/project",
            "tool_input": { "questions": [{
                "question": "扫\u{1b}]52;c;RXZpbA==\u{7}码",
                "options": [
                    {"label": "\u{1b}[2Jwipe", "description": "clears"},
                    {"label": "B\r\nrow", "description": "splits"}
                ]
            }]}
        });
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    crate::state::record_question_answer(
                        &conn,
                        &question_id,
                        "答\u{1b}[2J\u{7}案\r\n完",
                        2000
                    ),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        let result = question_gate(payload);
        handle.join().expect("answering thread");

        let painted = fs::read_to_string(&tty).expect("painted tty");
        assert!(
            !painted.contains("\u{1b}]52"),
            "OSC 52 injected: {painted:?}"
        );
        assert!(
            !painted.contains("\u{1b}[2J"),
            "CSI wipe injected: {painted:?}"
        );
        assert!(!painted.contains('\u{7}'), "BEL injected: {painted:?}");
        assert!(
            painted.contains("]52;c;RXZpbA=="),
            "printable tail must survive as harmless text: {painted:?}"
        );
        assert!(
            painted.contains("B  row"),
            "CR/LF must become spaces: {painted:?}"
        );
        assert!(
            painted.contains("答[2J案  完"),
            "the receipt line must carry the sanitized answer: {painted:?}"
        );
        // Only the banner's own fixed ANSI remains: 2 ESCs per painted line
        // (header, question, options, footer, receipt).
        assert_eq!(painted.matches('\u{1b}').count(), 10, "{painted:?}");
        // The display receipt is sanitized; the tool-contract copy is
        // byte-exact — both, deliberately.
        assert_eq!(
            result["systemMessage"],
            "tinyCTB：已由手机作答「答[2J案  完」"
        );
        assert_eq!(
            result["hookSpecificOutput"]["updatedInput"]["answers"]
                ["扫\u{1b}]52;c;RXZpbA==\u{7}码"],
            "答\u{1b}[2J\u{7}案\r\n完",
            "{result}"
        );
    }

    /// Tapping an option answers the blocked question; the choice reaches the
    /// model as the tool's own result.
    #[test]
    fn question_gate_returns_the_option_the_user_tapped() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-option", true, 30);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    crate::state::record_question_answer(&conn, &question_id, "SQLite", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        let result = question_gate(question_payload());
        handle.join().expect("answering thread");

        // The official contract: the call is allowed to complete with the
        // answer filled into the tool's own `answers` map, so the model
        // receives it as a tool result rather than inferring it from a
        // refusal message.
        let out = &result["hookSpecificOutput"];
        assert_eq!(out["permissionDecision"], "allow", "{result}");
        assert_eq!(
            out["updatedInput"]["answers"]["这个项目用哪个数据库？"], "SQLite",
            "{result}"
        );
        // The original questions must be passed through untouched.
        assert_eq!(
            out["updatedInput"]["questions"][0]["options"][1]["label"], "SQLite",
            "{result}"
        );

        // Single-select options live on the buttons only: repeating them in
        // the body made the buttons read as one more block of text.
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let payload: String = conn
            .query_row("SELECT payload_json FROM outbound_events", [], |row| {
                row.get(0)
            })
            .expect("outbound");
        assert!(payload.contains("question_request"), "{payload}");
        assert!(
            !payload.contains("\\nA. Postgres"),
            "options must not be duplicated in the body: {payload}"
        );
        // Each option carries a distinct, saturated colour marker — that is
        // what makes a row read as a button rather than as text.
        assert!(payload.contains("🔴A Postgres"), "{payload}");
        assert!(payload.contains("🟠B SQLite"), "{payload}");
        assert!(payload.contains("这个项目用哪个数据库？"));
    }

    /// The free-text path — the same one that carries an ordering like
    /// `3,1,2`, which is why phases 2 and 3 are one feature.
    #[test]
    fn question_gate_accepts_a_free_text_answer() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-text", true, 30);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                // Simulate the daemon matching a text reply to the message
                // that carries the question.
                crate::state::attach_question_message_id(&conn, &question_id, 4242)
                    .expect("attach");
                crate::state::record_dialog_message(
                    &conn,
                    "456",
                    4242,
                    "question",
                    &question_id,
                    1000,
                )
                .expect("dialog message");
                let (kind, matched) = crate::state::dialog_for_message(&conn, "456", 4242)
                    .expect("lookup")
                    .expect("dialog");
                assert_eq!(kind, "question");
                assert_eq!(matched, question_id);
                if matches!(
                    crate::state::record_question_answer(&conn, &matched, "3,1,2", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        let result = question_gate(question_payload());
        handle.join().expect("answering thread");
        assert_eq!(
            result["hookSpecificOutput"]["updatedInput"]["answers"]["这个项目用哪个数据库？"],
            "3,1,2",
            "a free-text answer rides the same contract: {result}"
        );
    }

    /// A tapped label reaches the model exactly as the tool wrote it. Labels
    /// are free text and commas are ordinary punctuation in them, so the
    /// letter-code shortcut must not treat one as a separator.
    #[test]
    fn a_label_containing_a_comma_survives_a_tap_unchanged() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-comma-label", true, 30);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    crate::state::record_question_answer(
                        &conn,
                        &question_id,
                        "Washington, D.C.",
                        2000
                    ),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        let mut payload = question_payload();
        payload["tool_input"]["questions"][0]["options"] =
            json!([{"label": "Washington, D.C."}, {"label": "Seattle"}]);
        let result = question_gate(payload);
        handle.join().expect("answering thread");

        assert_eq!(
            result["hookSpecificOutput"]["updatedInput"]["answers"]["这个项目用哪个数据库？"],
            "Washington, D.C.",
            "the label must arrive byte for byte, space included: {result}"
        );
    }

    /// Prose that merely opens with a letter is prose. Swapping that letter
    /// for an option label would put words in the user's mouth — and the
    /// qualifier ("but only locally") is the part that carries the meaning.
    #[test]
    fn free_text_starting_with_a_letter_is_not_rewritten() {
        let options = vec!["Postgres".to_string(), "SQLite".to_string()];
        assert_eq!(
            resolve_answer("A, but only locally", &options),
            "A, but only locally"
        );
        // Whole-answer letter codes still expand, single or multiple.
        assert_eq!(resolve_answer("a", &options), "Postgres");
        assert_eq!(resolve_answer(" A , b ", &options), "Postgres,SQLite");
        // A letter with no option behind it is not a code.
        assert_eq!(resolve_answer("Z", &options), "Z");
        // An ordering keeps its digits and its separators.
        assert_eq!(resolve_answer("3,1,2", &options), "3,1,2");
    }

    /// A multi-select question must not get one-tap buttons: the first tap
    /// would submit and silently drop the other choices. It is answered by a
    /// comma-separated reply, the shape the tool documents.
    #[test]
    fn multi_select_question_asks_for_a_comma_separated_reply() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-multi", true, 30);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                // The user replies with option letters.
                if matches!(
                    crate::state::record_question_answer(&conn, &question_id, "A,C", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        let mut payload = question_payload();
        payload["tool_input"]["questions"][0]["multiSelect"] = json!(true);
        payload["tool_input"]["questions"][0]["options"] = json!([
            {"label": "Postgres"}, {"label": "MySQL"}, {"label": "SQLite"}
        ]);
        let result = question_gate(payload);
        handle.join().expect("answering thread");

        // Letters are resolved to labels and joined the way the tool expects.
        assert_eq!(
            result["hookSpecificOutput"]["updatedInput"]["answers"]["这个项目用哪个数据库？"],
            "Postgres,SQLite",
            "{result}"
        );

        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let payload_json: String = conn
            .query_row("SELECT payload_json FROM outbound_events", [], |row| {
                row.get(0)
            })
            .expect("outbound");
        let event: Value = serde_json::from_str(&payload_json).expect("json");
        assert!(
            event["buttons"].as_array().expect("buttons").is_empty(),
            "a multi-select question must not offer one-tap buttons: {event}"
        );
        assert!(
            event["lastPreview"]
                .as_str()
                .expect("body")
                .contains("逗号分隔"),
            "and must say how to answer: {event}"
        );
    }

    /// The answers map is keyed by the question text exactly as the tool sent
    /// it — a trimmed key would not match `questions[].question`.
    #[test]
    fn answers_key_preserves_the_original_question_text() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-key", true, 30);
        let handle = std::thread::spawn(|| {
            let conn = create_state_db(&state_db_path().expect("path")).expect("db");
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(100));
                let Ok(question_id) = conn.query_row(
                    "SELECT question_id FROM pending_questions WHERE answer IS NULL",
                    [],
                    |row| row.get::<_, String>(0),
                ) else {
                    continue;
                };
                if matches!(
                    crate::state::record_question_answer(&conn, &question_id, "SQLite", 2000),
                    Ok(crate::state::ApprovalAnswer::Recorded)
                ) {
                    return;
                }
            }
            panic!("question row never appeared");
        });
        let mut payload = question_payload();
        payload["tool_input"]["questions"][0]["question"] = json!("  带空白的问题  ");
        let result = question_gate(payload);
        handle.join().expect("answering thread");

        let answers = &result["hookSpecificOutput"]["updatedInput"]["answers"];
        assert_eq!(
            answers["  带空白的问题  "], "SQLite",
            "the key must be the raw question text: {result}"
        );
        assert!(
            answers.get("带空白的问题").is_none(),
            "a trimmed key would not match questions[].question: {result}"
        );
    }

    #[test]
    fn question_gate_stays_out_of_the_way() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-skip", true, 30);

        // Not the question tool.
        let mut other = question_payload();
        other["tool_name"] = json!("Bash");
        assert_eq!(question_gate(other), json!({}));

        // A bridge-started turn must not block on a question nobody sees.
        let mut bypass = question_payload();
        bypass["permission_mode"] = json!("bypassPermissions");
        assert_eq!(question_gate(bypass), json!({}));

        // Several questions at once would need several round trips; leave
        // those to the terminal dialog.
        let mut many = question_payload();
        many["tool_input"]["questions"] = json!([
            {"question": "一?", "options": []},
            {"question": "二?", "options": []}
        ]);
        assert_eq!(question_gate(many), json!({}));

        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_questions", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(rows, 0, "none of these may create a pending question");
    }

    /// Unanswered questions fall back to the terminal dialog, and a late tap
    /// is refused rather than reported as accepted.
    #[test]
    fn question_gate_timeout_falls_back_to_the_terminal() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("question-timeout", true, 5);
        let started = std::time::Instant::now();
        assert_eq!(question_gate(question_payload()), json!({}));
        assert!(started.elapsed() >= Duration::from_secs(4));

        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let question_id: String = conn
            .query_row("SELECT question_id FROM pending_questions", [], |row| {
                row.get(0)
            })
            .expect("question");
        assert_eq!(
            crate::state::record_question_answer(&conn, &question_id, "SQLite", 9_000_000)
                .expect("late"),
            crate::state::ApprovalAnswer::Expired
        );
    }

    /// "Allow for this session" must stop asking on the next call, otherwise
    /// an agent doing many Bash calls needs one tap per call.
    #[test]
    fn session_scoped_allow_short_circuits_later_calls() {
        let _guard = crate::state::test_env_lock();
        let _env = GateEnv::new("session-allow", true, 30);
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        crate::state::set_approval_auto_allow(&conn, "sess-gate", "Bash", 900).expect("auto allow");

        let started = std::time::Instant::now();
        let result = gate(bash_payload());
        assert_eq!(
            result["hookSpecificOutput"]["decision"]["behavior"],
            "allow"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "an already-approved tool must not wait for a new answer"
        );
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            0,
            "and must not push another request"
        );
    }
}
