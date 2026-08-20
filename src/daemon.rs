use anyhow::{bail, Context, Result};
use fs2::FileExt;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::claude::{
    filter_watch_events, parse_event_filter, read_bridge_turn_result, start_claude_watch_receiver,
    sync_state_from_sessions, turn_log_tail, watch_events_from_sync_result,
    watch_thread_error_event,
};
use crate::state::{
    create_state_db, delete_setting, deliver_due_outbound_events, enqueue_outbound_event,
    get_setting_text, mark_bridge_turn_finished, pending_outbound_count, prune_state_logs,
    record_transport_delivery, set_setting_text, should_emit_for_away_window, state_db_path,
    transport_delivered_at, BridgeTurn, OutboxDeliverySummary,
};
use crate::telegram::{
    deliver_telegram_event, extend_telegram_typing_indicator, process_telegram_updates,
    refresh_telegram_typing_indicators, telegram_set_my_commands,
};
use crate::{
    daemon_config_path, load_daemon_config, notification_event_id, now_millis, shell_quote,
    state_dir_path, DaemonConfig,
};

#[derive(Debug, Clone)]
pub(crate) struct DaemonServiceSpec {
    pub(crate) service_path: PathBuf,
    pub(crate) stdout_log: PathBuf,
    pub(crate) stderr_log: PathBuf,
    pub(crate) unit_name: String,
    pub(crate) contents: String,
    pub(crate) install_command: String,
    pub(crate) uninstall_command: String,
    pub(crate) start_command: String,
    pub(crate) stop_command: String,
    pub(crate) status_command: String,
}

pub(crate) const DEFAULT_DAEMON_LABEL: &str = "tinyctb";
const DAEMON_PRUNE_INTERVAL_MS: u64 = 10 * 60 * 1000;
const DAEMON_LOCK_TIMEOUT: Duration = Duration::from_secs(2);

struct DaemonLock {
    file: fs::File,
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn daemon_lock_path() -> Result<PathBuf> {
    Ok(state_db_path()?.with_file_name("daemon.lock"))
}

pub(crate) fn spawn_lock_path() -> Result<PathBuf> {
    Ok(state_db_path()?.with_file_name("spawn.lock"))
}

/// SHARED lock every spawner holds across create→register→spawn, so a
/// teardown holding it EXCLUSIVELY is provably alone: no daemon `/new` and
/// no CLI `new/reply` can slip a fresh turn in between the sweep and the
/// ledger deletion.
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn hold_spawn_lock_shared() -> Result<fs::File> {
    let path = spawn_lock_path()?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("failed to open spawn lock at {}", path.display()))?;
    match FileExt::try_lock_shared(&file) {
        Ok(()) => Ok(file),
        Err(error) if error.kind() == ErrorKind::WouldBlock => {
            bail!("bridge maintenance (reset/uninstall) is in progress; try again shortly")
        }
        Err(error) => {
            Err(error).with_context(|| format!("failed to lock spawn lock at {}", path.display()))
        }
    }
}

/// The teardown side: EXCLUSIVE, bounded wait, held by the caller through
/// the sweep and the deletion.
pub(crate) fn hold_spawn_lock_exclusive(wait: Duration) -> Result<fs::File> {
    let path = spawn_lock_path()?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("failed to open spawn lock at {}", path.display()))?;
    let started = Instant::now();
    loop {
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => return Ok(file),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if started.elapsed() >= wait {
                    bail!("a spawn is still in flight (spawn lock busy); try again");
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to lock spawn lock at {}", path.display()))
            }
        }
    }
}

fn acquire_daemon_lock() -> Result<DaemonLock> {
    let path = daemon_lock_path()?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("failed to open daemon lock at {}", path.display()))?;
    let started = Instant::now();
    loop {
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => return Ok(DaemonLock { file }),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if started.elapsed() >= DAEMON_LOCK_TIMEOUT {
                    bail!(
                        "another daemon instance is already running (lock: {})",
                        path.display()
                    );
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to lock daemon instance at {}", path.display())
                })
            }
        }
    }
}

pub(crate) fn daemon_lock_free() -> Result<bool> {
    let path = daemon_lock_path()?;
    let file = match fs::OpenOptions::new().read(true).write(true).open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(true),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to open daemon lock at {}", path.display()))
        }
    };
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("failed to probe daemon lock at {}", path.display()))
        }
    }
}

fn event_observed_at(event: &Value) -> Option<u64> {
    event
        .get("updatedAt")
        .and_then(Value::as_u64)
        .or_else(|| event.get("observedAt").and_then(Value::as_u64))
        .or_else(|| event.pointer("/thread/updatedAt").and_then(Value::as_u64))
}

fn should_enqueue_away_notification(conn: &Connection, event: &Value) -> Result<bool> {
    let away_status = crate::get_away_mode(conn)?;
    if !away_notifications_enabled_from_status(&away_status) {
        return Ok(false);
    }
    let away_started_at = away_status.get("awayStartedAt").and_then(Value::as_u64);
    Ok(should_emit_for_away_window(
        away_started_at,
        event_observed_at(event),
    ))
}

/// Is this event the 60s idle reminder repeating what the user was already
/// sent? True only when BOTH ends of the echo are positively identified:
/// the current wait must literally be an `idle_prompt` notification (the
/// prompt's raw notification_type — the folded "reply" kind also covers
/// `agent_needs_input` and MCP elicitations, which are genuine questions
/// that may fire right after a completion while lastPreview still shows the
/// completion text), and its preview must EXACTLY match a completion push
/// delivered to the same thread moments ago (type and recency constraints
/// live in `last_delivered_completion_preview`). Anything unknown or
/// mismatched keeps the notification — fail open to noise, never to
/// silence.
fn redundant_idle_reminder(
    conn: &Connection,
    event: &Value,
    thread_id: Option<&str>,
    now: u64,
) -> Result<bool> {
    if event.get("type").and_then(Value::as_str) != Some("thread_waiting") {
        return Ok(false);
    }
    let notification_type = event
        .pointer("/thread/pendingPrompt/notificationType")
        .and_then(Value::as_str);
    if notification_type != Some("idle_prompt") {
        return Ok(false);
    }
    let Some(thread_id) = thread_id else {
        return Ok(false);
    };
    let preview = event
        .get("lastPreview")
        .or_else(|| event.pointer("/thread/lastPreview"))
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if preview.is_empty() {
        return Ok(false);
    }
    Ok(
        crate::state::last_delivered_completion_preview(conn, thread_id, now)?.as_deref()
            == Some(preview),
    )
}

pub(crate) fn enqueue_daemon_notification_events(
    conn: &Connection,
    events: &[Value],
    now: u64,
) -> Result<usize> {
    let mut enqueued = 0usize;
    for event in events {
        // An answer owed to a Telegram message injected into a live session
        // is bridge traffic: it bypasses the away gate and survives /back,
        // like any other reply the user asked for from their phone.
        let thread_id = crate::event_thread_id(event);
        let event_at = event_observed_at(event);
        let owed = match thread_id.as_deref() {
            Some(thread_id) => {
                crate::state::live_injection_pending(conn, thread_id, event_at, now)?
            }
            None => false,
        };
        if !owed && !should_enqueue_away_notification(conn, event)? {
            continue;
        }
        // Claude fires an idle reminder 60s after every turn ends, carrying
        // the SAME text as the completion push a minute earlier — the user
        // reads every answer twice. An idle wait whose preview matches the
        // last delivered push adds nothing: drop it. Approval waits and
        // owed bridge answers are never suppressed.
        if !owed && redundant_idle_reminder(conn, event, thread_id.as_deref(), now)? {
            eprintln!(
                "tinyctb: suppressed redundant idle reminder for {}",
                thread_id.as_deref().unwrap_or("?")
            );
            continue;
        }
        // Queueing the answer and settling the debt must be one unit: a crash
        // between them would leave the answer queued and the debt open, so
        // the next completion would be pushed as a second "answer" too.
        let tx = conn.unchecked_transaction()?;
        let inserted =
            enqueue_outbound_event(&tx, event, now, if owed { "bridge" } else { "away" })?;
        if inserted && owed && event.get("type").and_then(Value::as_str) == Some("thread_completed")
        {
            if let Some(thread_id) = thread_id.as_deref() {
                crate::state::consume_live_injection(&tx, thread_id, event_at, now)?;
            }
        }
        tx.commit()?;
        if inserted {
            enqueued += 1;
        }
    }
    Ok(enqueued)
}

const BRIDGE_TURN_FAILURE_GRACE_MS: u64 = 10_000;
/// Absolute backstop: after a daemon restart the child handle is lost and PID
/// reuse can make `kill -0` claim a dead turn is alive forever.
const BRIDGE_TURN_MAX_RUNTIME_MS: u64 = 6 * 60 * 60 * 1000;

fn bridge_turn_process_alive(pid: Option<u32>) -> bool {
    let Some(pid) = pid.filter(|pid| *pid > 0) else {
        return false;
    };
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Thread context for rendering a bridge-turn message (display name, project).
fn bridge_turn_thread_json(conn: &Connection, turn: &BridgeTurn, preview: &str) -> Result<Value> {
    use rusqlite::OptionalExtension;
    let (name, cwd): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT name, cwd FROM threads_cache WHERE thread_id = ?1",
            rusqlite::params![turn.thread_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .unwrap_or((None, None));
    let project = crate::projects::derive_project_label(cwd.as_deref());
    let display_name = crate::state::derive_thread_display_name(
        name.as_deref(),
        project.as_deref(),
        None,
        &turn.thread_id,
    );
    Ok(json!({
        "threadId": turn.thread_id,
        "name": name,
        "displayName": display_name,
        "project": project,
        "cwd": cwd,
        "lastPreview": preview
    }))
}

/// Apply a "this turn is dead" verdict that was formed from a snapshot.
///
/// The verdict must be CLAIMED before it is announced, because the snapshot
/// can be stale in one dangerous way: a turn read as `pid NULL` (registered,
/// identity not yet written) whose identity write lands in between. The old
/// order — enqueue the failure notice, then unconditionally write `failed` —
/// would close a turn whose caller was just told "started": its token then
/// points at a failed row, every gated call gets denied, and the eventual
/// answer is never delivered. `claim_bridge_turn_failure` re-asserts
/// `pid IS NULL` for exactly that verdict, so losing the race skips both the
/// write and the notice; the turn is simply still running.
///
/// The verdict's side effects are ordered for CRASH consistency, because the
/// terminal status is unrecoverable the moment it commits — a settled turn
/// is invisible to every future `list_running_bridge_turns` scan:
/// - the kill runs BEFORE anything commits. Dying right after it leaves the
///   turn `running`, and the next cycle re-judges the timeout and repeats
///   the (idempotent) kill — whereas a committed `expired` with the process
///   alive would orphan it forever;
/// - the claim and the failure notice commit in ONE transaction. Dying (or
///   an enqueue error) between them can therefore not produce a settled
///   turn whose user was never told: the claim rolls back, the turn stays
///   `running`, and the next cycle retries the whole verdict.
///
/// Returns whether the failure was claimed (false = verdict dropped).
/// Drive a turn the user asked to stop towards a settled state.
///
/// `/stop` records the intent and signals, but it cannot be the place that
/// PROVES the process died: it may crash before signalling, the leader may
/// die while a grandchild keeps working, or it may be killed between the
/// kill and the settle. Recovery therefore lives here, in the loop that runs
/// every cycle, and it has exactly three answers:
///  - group provably gone → settle as `stopped`, and close the prompts and
///    queued buttons the turn owned, in ONE transaction;
///  - group still alive → signal again (the kill is idempotent) and wait;
///  - cannot tell → stay `stopping`. Never settle on ignorance; a settled
///    turn is invisible to this loop forever after.
fn advance_stopping_turn(
    conn: &Connection,
    turn: &crate::state::BridgeTurn,
    now: u64,
) -> Result<bool> {
    use crate::claude::{KillOutcome, Liveness};
    // Owned dialogs die at the INTENT, not at the settle — `/stop` closes
    // them in its own transaction, and creation refuses owners that are not
    // running. Rows written by an older binary (or an interrupted recovery)
    // may still carry open prompts, so every pass re-closes idempotently:
    // stopping turns are rare and the sweep is a no-op when clean.
    {
        let tx = conn.unchecked_transaction()?;
        crate::state::settle_prompts_for_turn(&tx, &turn.turn_id, now)?;
        tx.commit()?;
    }
    // The ownership object outranks every pid-derived probe: `populated`
    // is a kernel fact about OUR object, immune to reuse and recycling.
    let cgroup = turn
        .cgroup_path
        .as_deref()
        .and_then(|path| crate::claude::turn_cgroup::validated(path, &turn.turn_id));
    let group = match &cgroup {
        Some(dir) => match crate::claude::turn_cgroup::populated(dir) {
            Some(true) => Liveness::Alive,
            Some(false) => Liveness::Ended,
            None => Liveness::Unknown,
        },
        None => match turn.pgid {
            Some(pgid) => crate::claude::group_liveness(pgid),
            // No persisted group id: nothing here can be proven, and
            // guessing `pid` as the group would risk signalling an
            // unrelated one.
            None => Liveness::Unknown,
        },
    };
    match group {
        Liveness::Alive => {
            // Idempotent — but not free: an attempt can hold this loop for
            // ~2s of confirmation, so re-kills back off exponentially
            // (1s → 64s cap) instead of burning that cost every cycle
            // against a group that shrugs KILL off (uninterruptible IO, a
            // descendant under another uid).
            let (attempts, last) = crate::state::stop_attempt_state(conn, &turn.turn_id)?;
            let delay_ms = 1_000u64 << attempts.min(6);
            // `last <= now` fails OPEN on wall-clock regression: a stamp
            // from the future would otherwise freeze retries until the
            // clock catches back up.
            if last <= now && now < last.saturating_add(delay_ms) {
                return Ok(false);
            }
            crate::state::record_stop_attempt(conn, &turn.turn_id, now)?;
            let _ = crate::claude::kill_turn_process(turn);
            Ok(false)
        }
        Liveness::Unknown => Ok(false),
        Liveness::Ended => {
            let tx = conn.unchecked_transaction()?;
            // The terminal is chosen from PERSISTED intent, never guessed:
            // `stopping` status is the user's stop; a failure-flavoured
            // marker (2) is a spawn failure's cleanup — reporting that as
            // "已停止" claimed a stop the user never made.
            let (row_status, marker): (String, i64) = tx.query_row(
                "SELECT status, cleanup_pending FROM bridge_turns WHERE turn_id = ?1",
                rusqlite::params![turn.turn_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let failure_cleanup = row_status != "stopping" && marker == 2;
            let terminal = if failure_cleanup { "failed" } else { "stopped" };
            let settled = crate::state::settle_supervised_turn(&tx, &turn.turn_id, terminal, now)?;
            if settled {
                crate::state::settle_prompts_for_turn(&tx, &turn.turn_id, now)?;
                // The final confirmation, in the SAME transaction as the
                // settle: a stop that was only confirmed by this recovery
                // loop would otherwise end silently — the `/stop` reply
                // already said "unconfirmed" and nothing else would follow.
                // Undelivered pre-final chatter is withdrawn first: retried
                // out of order it would arrive AFTER this terminal answer.
                crate::state::withdraw_undelivered_stop_chatter(&tx, &turn.turn_id, now)?;
                let label = crate::telegram::short_thread_id(&turn.thread_id);
                let event = if failure_cleanup {
                    json!({
                        "type": "thread_error",
                        "threadId": turn.thread_id,
                        "observedAt": now,
                        "eventKey": format!("cleanup-settled:{}", turn.turn_id),
                        "message": format!(
                            "🧵 {label} — 启动失败的回合已清理完毕（进程组确认退出，按失败结算）"
                        ),
                    })
                } else {
                    json!({
                        "type": "bridge_notice",
                        "threadId": turn.thread_id,
                        "observedAt": now,
                        "eventKey": format!("stop-settled:{}", turn.turn_id),
                        "message": format!("🧵 {label} — 已停止（daemon 确认整组退出）"),
                    })
                };
                enqueue_outbound_event(&tx, &event, now, "bridge")?;
            }
            // Supervision ends HERE either way: the group is provably
            // empty. (A row that was already terminal keeps its status;
            // only the marker is lifted.)
            crate::state::clear_cleanup_pending(&tx, &turn.turn_id)?;
            tx.commit()?;
            if let Some(dir) = &cgroup {
                // Emptiness is proven and settled; the object has served.
                // Removal refuses a non-empty directory by construction.
                let _ = crate::claude::turn_cgroup::remove(dir);
            }
            if settled {
                // Only once nothing is left running for this session.
                let others = crate::state::list_running_bridge_turns(conn)?;
                if !others.iter().any(|other| other.thread_id == turn.thread_id) {
                    let _ = crate::telegram::clear_typing_for_thread(conn, &turn.thread_id);
                }
            }
            let _ = KillOutcome::Terminated; // documents the state we reached
            Ok(settled)
        }
    }
}

/// How long a registered turn may sit with NO identity before the bridge
/// concludes the identity will never land. Normal spawns record it within
/// milliseconds; the unwinding paths within seconds. Generous on purpose.
const IDENTITY_GRACE_MS: u64 = 60_000;

/// A supervised turn with NO identity: nothing can be killed or probed,
/// and — decisively — nothing can be PROVEN. Sixty quiet seconds prove
/// only that the identity write is late, not that no process exists (the
/// CLI can die between a successful spawn and the write, leaving a live
/// group). So this NEVER settles: within the grace the row is left
/// entirely alone (a healthy newborn's first approval must survive the
/// tick), and past it the user is alarmed ONCE — the row stays visible
/// and supervised until something with evidence resolves it: the identity
/// handshake a gated tool call performs, the turn's own completion, or
/// the hard timeout.
fn alarm_identityless_turn(
    conn: &Connection,
    turn: &crate::state::BridgeTurn,
    now: u64,
) -> Result<()> {
    if now.saturating_sub(turn.started_at) <= IDENTITY_GRACE_MS {
        return Ok(());
    }
    let event = json!({
        "type": "thread_error",
        "threadId": turn.thread_id,
        "observedAt": now,
        "eventKey": format!("identityless-turn:{}", turn.turn_id),
        "message": format!(
            "🧵 {} 的进程身份仍未落库：无法监管也无法安全终止。回合保持可见，直到它自证身份（下一次工具调用）、自行完成或到达硬超时。若怀疑有失控进程请人工检查。",
            crate::telegram::short_thread_id(&turn.thread_id)
        ),
    });
    // Keyed once per turn; the outbox deduplicates re-attempts away.
    enqueue_outbound_event(conn, &event, now, "bridge")?;
    Ok(())
}

/// Reclaim ownership objects NO ROW references — the crash window between
/// `create` and the registration commit, or a red test run that panicked
/// before its cleanup, leaves a directory nobody will ever settle. The
/// name alone proves it is ours (it lives under our owner root and is
/// shaped `turn-*`), so an empty one is removed and a populated one is
/// killed first, loudly: work with no owning row is work nobody asked to
/// keep.
fn gc_orphan_turn_objects(conn: &Connection, grace: std::time::Duration) -> Result<()> {
    let Some(root) = crate::claude::turn_cgroup::owner_root() else {
        return Ok(());
    };
    let Ok(entries) = std::fs::read_dir(&root) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(_) = name.strip_prefix("turn-") else {
            continue;
        };
        // The GRACE is load-bearing twice over: a CLI in its create→register
        // window, and parallel tests owning rows in their own databases,
        // both look "unreferenced" for a moment. Crash orphans have all the
        // time in the world.
        let fresh = match entry
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|modified| modified.elapsed().ok())
        {
            Some(age) => age < grace,
            // Unreadable age: err on the side of keeping it.
            None => true,
        };
        if fresh {
            continue;
        }
        let path = entry.path();
        let Some(path_str) = path.to_str() else {
            continue;
        };
        let referenced: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bridge_turns WHERE cgroup_path = ?1",
            rusqlite::params![path_str],
            |row| row.get(0),
        )?;
        if referenced > 0 {
            continue;
        }
        if crate::claude::turn_cgroup::populated(&path) == Some(true) {
            eprintln!(
                "tinyctb: killing ORPHAN turn object {} — populated but referenced by no row",
                path.display()
            );
            let _ = crate::claude::turn_cgroup::kill(&path);
            if !crate::claude::turn_cgroup::confirmed_empty(
                &path,
                std::time::Duration::from_secs(2),
            ) {
                continue;
            }
        }
        let _ = crate::claude::turn_cgroup::remove(&path);
    }
    Ok(())
}

/// Terminal settlement of a cgroup-owned turn: mark terminal AND set the
/// cleanup marker in ONE transaction, then kill the object and attempt a
/// quick confirmation. Confirmed → marker released, object removed. Not
/// confirmed → the marker keeps the row in the supervised loop until
/// `populated 0` is a fact. A turn's end means its whole tree's end —
/// including any background processes it started.
fn finish_turn_and_sweep(
    conn: &Connection,
    turn: &crate::state::BridgeTurn,
    status: &str,
    now: u64,
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    mark_bridge_turn_finished(&tx, &turn.turn_id, status, now)?;
    // A finished turn's dialogs finish WITH it, atomically: the only other
    // sweeper is the stopping-recovery path, which a turn that terminates
    // cleanly (and proves empty in the quick window) never visits — its
    // approval rows and queued buttons would otherwise stay answerable.
    crate::state::settle_prompts_for_turn(&tx, &turn.turn_id, now)?;
    if turn.cgroup_path.is_some() {
        crate::state::set_cleanup_marker(&tx, &turn.turn_id)?;
    }
    tx.commit()?;
    sweep_terminal_turn_object(conn, turn, now)?;
    Ok(())
}

/// Kill the settled turn's object and, on a quick proof of emptiness,
/// release the marker and remove it. A missed confirmation leaves the
/// marker: the supervised loop owns the rest.
fn sweep_terminal_turn_object(
    conn: &Connection,
    turn: &crate::state::BridgeTurn,
    now: u64,
) -> Result<()> {
    let _ = now;
    let Some(dir) = turn
        .cgroup_path
        .as_deref()
        .and_then(|path| crate::claude::turn_cgroup::validated(path, &turn.turn_id))
    else {
        return Ok(());
    };
    let _ = crate::claude::turn_cgroup::kill(&dir);
    if crate::claude::turn_cgroup::confirmed_empty(&dir, std::time::Duration::from_secs(2)) {
        crate::state::clear_cleanup_pending(conn, &turn.turn_id)?;
        let _ = crate::claude::turn_cgroup::remove(&dir);
    }
    Ok(())
}

fn settle_dead_turn(
    conn: &Connection,
    turn: &crate::state::BridgeTurn,
    timed_out: bool,
    now: u64,
) -> Result<bool> {
    // Killing before the claim can in principle kill a turn whose claim then
    // loses (another writer settled it in between) — acceptable, and not
    // new: a process past the hard timeout is dead by fiat, and the old code
    // killed it unconditionally too.
    if timed_out && !turn.exited {
        // Outcome deliberately ignored here: a turn past the hard timeout is
        // dead by fiat and gets settled either way, which is the pre-existing
        // contract. `/stop` is the caller that must respect the outcome.
        let _ = crate::claude::kill_turn_process(turn);
    }
    let reason = if turn.exited {
        format!(
            "exited (status {}) without producing an answer",
            turn.exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        )
    } else if timed_out {
        "ran past the hard timeout without producing an answer".to_string()
    } else {
        "exited without producing an answer".to_string()
    };
    let tail = turn_log_tail(std::path::Path::new(&turn.log_path), 400);
    let event = json!({
        "type": "thread_error",
        "threadId": turn.thread_id,
        "observedAt": now,
        "eventKey": format!("bridge-turn-failed:{}", turn.turn_id),
        "message": format!(
            "The headless turn {reason}.\nLog tail:\n{}",
            if tail.is_empty() { "(empty log)".to_string() } else { tail }
        ),
    });
    let tx = conn.unchecked_transaction()?;
    // A timed-out turn is dead by fiat, not by snapshot evidence, so its
    // claim does not depend on the pid still being missing. Only the
    // "no pid ever recorded" verdict is snapshot-fragile.
    let pid_still_missing = turn.pid.is_none() && !turn.exited && !timed_out;
    let claimed = crate::state::claim_bridge_turn_failure(
        &tx,
        &turn.turn_id,
        if timed_out { "expired" } else { "failed" },
        now,
        pid_still_missing,
    )?;
    if claimed {
        enqueue_outbound_event(&tx, &event, now, "bridge")?;
        // The failed turn's dialogs close with it, atomically — this row
        // will not pass the stopping-recovery sweeper again.
        crate::state::settle_prompts_for_turn(&tx, &turn.turn_id, now)?;
        // A cgroup-owned turn's end is its whole TREE's end: the marker
        // commits with the terminal status, and the sweep (or the
        // supervised loop, if the quick confirmation misses) releases it
        // only on proven emptiness. Without this, a terminal row left a
        // populated object outside every future scan.
        if turn.cgroup_path.is_some() {
            crate::state::set_cleanup_marker(&tx, &turn.turn_id)?;
        }
    }
    tx.commit()?;
    if claimed {
        sweep_terminal_turn_object(conn, turn, now)?;
    }
    Ok(claimed)
}

/// Deliver answers of bridge-initiated turns from their own turn logs.
/// This is attribution by construction: the log is this turn's output, so a
/// concurrently active session cannot mislabel its own Stop as the answer.
/// Pushes bypass away gating and the events filter (origin 'bridge').
pub(crate) fn process_bridge_turns(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
) -> Result<Value> {
    // Reap finished children first: this prevents zombies (which would fool
    // `kill -0`) and records authoritative exit facts for crash detection.
    for (turn_id, exit_code) in crate::claude::reap_finished_turn_processes() {
        crate::state::record_bridge_turn_exit(conn, &turn_id, exit_code)?;
    }
    // The SUPERVISED set, not just running/stopping: a cleanup-marked row
    // whose status a racing writer already made terminal still needs its
    // group probed until it is proven empty.
    let turns = crate::state::list_supervised_bridge_turns(conn)?;
    gc_orphan_turn_objects(conn, std::time::Duration::from_secs(600))?;
    let mut answered = 0usize;
    let mut failed = 0usize;
    let mut running = 0usize;
    for turn in turns {
        // A turn the user asked to stop follows its own path: it must not be
        // read for an answer (there will not be one worth pushing), and it
        // must not be reported as a failure. Its only question is whether the
        // process group is really gone yet.
        if crate::state::needs_stop_supervision(conn, &turn.turn_id)? {
            // Two very different supervised shapes, split on EVIDENCE. An
            // identity (pgid) to act on means real cleanup: sweep, re-kill,
            // probe — and nothing else this cycle.
            if turn.pgid.is_some() || turn.cgroup_path.is_some() {
                if advance_stopping_turn(conn, &turn, now)? {
                    failed += 1; // counted as settled work, not as an answer
                } else {
                    running += 1;
                }
                continue;
            }
            // NO identity: alarm once past the grace, and for a row still
            // RUNNING fall through — the ordinary flow below (answer
            // delivery, exit accounting, the six-hour hard timeout) must
            // stay reachable, or a turn that completed without ever making
            // a gated call would show running forever, its answer never
            // delivered. An unconditional `continue` here was exactly that
            // bug. Non-running identityless rows (a raced stop, a fiat
            // timeout) have nothing below for them: alarmed and kept.
            alarm_identityless_turn(conn, &turn, now)?;
            if crate::state::bridge_turn_status(conn, &turn.turn_id)?.as_deref() != Some("running")
            {
                running += 1;
                continue;
            }
        }
        let log_path = std::path::PathBuf::from(&turn.log_path);
        match read_bridge_turn_result(&log_path) {
            Some(result) => {
                // While away, the sync pass may have already enqueued this
                // very completion (Stop hook) — possibly in an EARLIER cycle:
                // the Stop can wake the daemon before the result JSON hits the
                // log. So the window is "since this turn started", and the
                // check is bound to the ANSWER CONTENT, not just the session:
                // a terminal Stop or another concurrent reply must never make
                // this turn's distinct answer get dropped.
                let already_pushed_by_away: bool = {
                    let mut stmt = conn.prepare(
                        "SELECT payload_json FROM outbound_events
                         WHERE thread_id = ?1 AND event_type = 'thread_completed'
                           AND origin = 'away' AND created_at >= ?2",
                    )?;
                    let payloads = stmt
                        .query_map(
                            rusqlite::params![turn.thread_id, turn.started_at as i64],
                            |row| row.get::<_, String>(0),
                        )?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    let answer = result.text.trim();
                    payloads
                        .iter()
                        .filter_map(|raw| serde_json::from_str::<Value>(raw).ok())
                        .filter_map(|payload| {
                            payload
                                .get("lastPreview")
                                .and_then(Value::as_str)
                                .map(|preview| preview.trim().to_string())
                        })
                        // The away preview may be a truncated prefix of the
                        // full result text.
                        .any(|preview| {
                            !preview.is_empty()
                                && (preview == answer || answer.starts_with(&preview))
                        })
                };
                if !already_pushed_by_away {
                    let preview = if result.is_error {
                        format!("⚠️ The turn ended with an error:\n{}", result.text)
                    } else {
                        result.text.clone()
                    };
                    let event = json!({
                        "type": "thread_completed",
                        "threadId": turn.thread_id,
                        "eventUid": turn.turn_id,
                        "updatedAt": now,
                        "lastPreview": preview,
                        "eventKey": format!("bridge-turn-result:{}", turn.turn_id),
                        "thread": bridge_turn_thread_json(conn, &turn, &preview)?
                    });
                    enqueue_outbound_event(conn, &event, now, "bridge")?;
                }
                finish_turn_and_sweep(
                    conn,
                    &turn,
                    if result.is_error { "error" } else { "done" },
                    now,
                )?;
                answered += 1;
            }
            None => {
                let age = now.saturating_sub(turn.started_at);
                let timed_out = age > BRIDGE_TURN_MAX_RUNTIME_MS;
                // `exited` is authoritative (the daemon reaped the child);
                // `kill -0` is a best-effort fallback for turns that survived
                // a daemon restart, backstopped by the hard timeout because
                // PID reuse can make it lie forever.
                let alive = !turn.exited && !timed_out && bridge_turn_process_alive(turn.pid);
                if alive || (!turn.exited && !timed_out && age <= BRIDGE_TURN_FAILURE_GRACE_MS) {
                    // Still queued or working: keep the chat's typing
                    // indicator alive so the wait is visible.
                    if alive {
                        if let Some(telegram) = config.telegram.as_ref() {
                            extend_telegram_typing_indicator(conn, telegram, &turn.thread_id, now)?;
                        }
                    }
                    running += 1;
                } else if settle_dead_turn(conn, &turn, timed_out, now)? {
                    failed += 1;
                } else {
                    // The identity write won the race (see
                    // `settle_dead_turn`): the turn is alive after all.
                    running += 1;
                }
            }
        }
    }
    Ok(json!({
        "ok": true,
        "answered": answered,
        "failed": failed,
        "running": running
    }))
}

fn away_notifications_enabled_from_status(away_status: &Value) -> bool {
    away_status.get("away").and_then(Value::as_bool) == Some(true)
}

/// One event's trip through the transports: skip what the transport log
/// already recorded, send the rest. `send` is the real Telegram call in
/// production and a stub under test, so the skip/timestamp logic below is
/// exercised as written rather than re-implemented in the test.
///
/// The send-then-record-then-mark sequence is crash-visible at both seams.
/// When recovery finds a transport record without a delivered outbound row,
/// the SEND already happened at the logged time — that timestamp travels
/// back as `deliveredAt` so the outbound row is stamped with when the user
/// actually saw the message, not with the recovery cycle's clock. Ordering
/// by a recovery-cycle stamp would rank a message the user read before the
/// crash *after* everything they received later.
fn deliver_event_through_transports<S>(
    conn: &Connection,
    config: &DaemonConfig,
    event: &Value,
    now: u64,
    send: S,
) -> Result<Value>
where
    S: FnOnce(&crate::TelegramConfig) -> Result<Value>,
{
    let event_id = notification_event_id(event);
    let mut delivered_at = None;
    let telegram = if let Some(telegram) = config.telegram.as_ref() {
        match transport_delivered_at(conn, &event_id, "telegram")? {
            Some(sent_at) => {
                delivered_at = Some(sent_at);
                json!({
                    "ok": true,
                    "transport": "telegram",
                    "skipped": "already_delivered",
                    "sentAt": sent_at
                })
            }
            None => {
                let result = send(telegram)?;
                record_transport_delivery(conn, &event_id, "telegram", &result, now)?;
                result
            }
        }
    } else {
        Value::Null
    };
    Ok(json!({ "telegram": telegram, "deliveredAt": delivered_at }))
}

fn deliver_outbound_events(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
    deadline: Instant,
) -> Result<OutboxDeliverySummary> {
    deliver_due_outbound_events(conn, now, 100, Some(deadline), |event| {
        deliver_event_through_transports(conn, config, event, now, |telegram| {
            deliver_telegram_event(conn, telegram, event, now, timeout)
        })
    })
}

fn daemon_cycle_budget(timeout: Duration) -> Duration {
    timeout.max(Duration::from_secs(5)).saturating_mul(3)
}

/// Fingerprint of the sync error the user has already been notified about.
/// While set, identical errors on subsequent cycles stay quiet; a successful
/// sync clears it so the same error recurring later is a fresh incident.
const SYNC_ERROR_NOTIFIED_KEY: &str = "sync_error_notified_fingerprint";

/// Also called when away mode turns off: /back may delete an undelivered
/// error notification from the outbox, so the streak must re-arm or the same
/// persistent error would stay silent for the whole next away session.
pub(crate) fn end_sync_error_streak(conn: &Connection) -> Result<()> {
    delete_setting(conn, SYNC_ERROR_NOTIFIED_KEY)
}

fn enqueue_sync_error_notification(
    conn: &Connection,
    filter: Option<&std::collections::BTreeSet<String>>,
    error: &anyhow::Error,
    now: u64,
) -> Result<(Vec<Value>, u64)> {
    let fingerprint = crate::sha256_hex(error.to_string().as_bytes());
    let already_notified =
        get_setting_text(conn, SYNC_ERROR_NOTIFIED_KEY)?.as_deref() == Some(fingerprint.as_str());
    let events = filter_watch_events(vec![watch_thread_error_event(error, now)], filter);
    let mut enqueued = 0u64;
    if !already_notified {
        // thread_error events (for users who opt into them via the events
        // config) go through the normal enqueue policy (away gating included).
        enqueued = enqueue_daemon_notification_events(conn, &events, now)? as u64;
        // Only a notification that actually went out starts a quiet streak:
        // an error observed while not away must still notify once the user
        // leaves (or enables the event type).
        if enqueued > 0 {
            set_setting_text(conn, SYNC_ERROR_NOTIFIED_KEY, &fingerprint)?;
        }
    }
    Ok((events, enqueued))
}

/// Which lanes a wake runs. The 500ms base tick must stay CHEAP: the full
/// transcript sync (parsing up to 50 session files) and the Telegram
/// getUpdates HTTP round trip cannot run on every tick — the first for CPU,
/// the second because Telegram treats aggressive short-polling as abuse. Local
/// work (outbound delivery, headless turn logs, socket peek) runs every
/// wake; the expensive lanes keep their own floors.
#[derive(Debug, Clone, Copy)]
struct CycleLanes {
    telegram_updates: bool,
    full_sync: bool,
}

const TELEGRAM_UPDATES_MIN_INTERVAL_MS: u64 = 250;
const FULL_SYNC_MIN_INTERVAL_MS: u64 = 1500;

fn daemon_cycle(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    daemon_cycle_lanes(
        conn,
        config,
        now,
        timeout,
        CycleLanes {
            telegram_updates: true,
            full_sync: true,
        },
    )
}

fn daemon_cycle_lanes(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
    lanes: CycleLanes,
) -> Result<Value> {
    let deadline = Instant::now() + daemon_cycle_budget(timeout);
    let filter = parse_event_filter(Some(&config.events));
    // Learn where live sessions listen BEFORE handling Telegram updates: a
    // reply arriving in the same cycle as that session's first hook event
    // must already find the mapping, or it would fall back to a headless
    // `--resume` and fork the session this feature exists to protect.
    // Non-destructive — the spool is still consumed by the sync below.
    if lanes.telegram_updates {
        if let Err(error) = crate::claude::peek_session_sockets(conn, now) {
            println!(
                "{}",
                json!({
                    "ok": false,
                    "action": "session_socket_peek_error",
                    "error": format!("{error:#}")
                })
            );
        }
    }
    let telegram_updates = match config.telegram.as_ref().filter(|_| lanes.telegram_updates) {
        Some(telegram) => {
            let mut result =
                match process_telegram_updates(conn, config, now, timeout, Some(deadline)) {
                    Ok(result) => result,
                    Err(error) => json!({
                        "ok": false,
                        "transport": "telegram",
                        "error": format!("{error:#}")
                    }),
                };
            let typing = if Instant::now() < deadline {
                refresh_telegram_typing_indicators(conn, telegram, now, timeout).unwrap_or_else(
                    |error| {
                        json!({
                            "ok": false,
                            "transport": "telegram",
                            "error": format!("{error:#}")
                        })
                    },
                )
            } else {
                json!({
                    "ok": true,
                    "transport": "telegram",
                    "skipped": "cycle_deadline"
                })
            };
            if let Some(object) = result.as_object_mut() {
                object.insert("typing".to_string(), typing);
            }
            result
        }
        None => Value::Null,
    };
    // Notification enqueueing happens inside the sync (spool consumption and
    // notification persistence must be atomic from the caller's perspective).
    let (events, enqueued) = if lanes.full_sync {
        match sync_state_from_sessions(conn, config, now, 50, true) {
            Ok(sync_result) => {
                end_sync_error_streak(conn)?;
                (
                    watch_events_from_sync_result(&sync_result, filter.as_ref()),
                    sync_result
                        .get("enqueued")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                )
            }
            Err(error) => enqueue_sync_error_notification(conn, filter.as_ref(), &error, now)?,
        }
    } else {
        (Vec::new(), 0)
    };
    // Bridge-turn answers are collected AFTER the sync so the away-duplicate
    // check can see anything the sync just enqueued for the same completion.
    let bridge_turns = process_bridge_turns(conn, config, now)
        .unwrap_or_else(|error| json!({ "ok": false, "error": format!("{error:#}") }));
    // Delivery is not gated on away mode: enqueueing is the policy point.
    // While the user is present the outbox only ever holds answers to turns
    // they started from Telegram, which must always be delivered.
    let delivery = deliver_outbound_events(conn, config, now, timeout, deadline)?;
    Ok(json!({
        "ok": true,
        "action": "daemon_cycle",
        "observed": events.len(),
        "enqueued": enqueued,
        "bridgeTurns": bridge_turns,
        "delivery": delivery,
        "telegramUpdates": telegram_updates,
        "pending": pending_outbound_count(conn)?
    }))
}

/// Where the keyboard listener records the last keypress it saw.
pub(crate) const INPUT_ACTIVITY_FILE: &str = "input-activity.json";

/// One write per this interval is plenty: the gates compare against a 15s
/// window, and a burst of typing would otherwise rewrite the file per key.
const INPUT_ACTIVITY_MIN_GAP: Duration = Duration::from_millis(1000);
/// How long a watcher waits before reconnecting to its own candidate.
const INPUT_LISTENER_RETRY: Duration = Duration::from_secs(5);
/// How often the supervisor re-scans for X sessions appearing or vanishing.
const INPUT_LISTENER_RESCAN: Duration = Duration::from_secs(30);

type XCandidate = (String, Option<String>);

/// A live `xinput` stream and the process behind it. The child is carried
/// alongside the reader, never dropped on the floor: a forgotten child is a
/// zombie, and a watcher reconnecting every 5s would mint ~720 an hour until
/// the PID or task quota ran out.
struct XSession {
    reader: Box<dyn std::io::BufRead + Send>,
    child: Option<std::process::Child>,
}

type XStreamOpener = dyn Fn(&XCandidate) -> Option<XSession> + Send + Sync;

/// Serialises publication across every watcher thread. They share one PID
/// and one destination, so without this they truncate each other's staging
/// file and a gate can read half a JSON object — and each thread throttling
/// itself would let N sessions write N times a second.
#[derive(Default)]
struct ActivityPublisher {
    last_write: std::sync::Mutex<Option<Instant>>,
    /// Test-only fault injection: pretend the write failed AFTER the staging
    /// file was created, which is the case that leaks — the name carries an
    /// ever-increasing sequence, so every failure would leave another
    /// partial file behind.
    #[cfg(test)]
    fail_after_create: std::sync::atomic::AtomicBool,
}

impl ActivityPublisher {
    /// Publish if the shared gap has elapsed. Returns whether it wrote.
    fn publish(&self, dir: &Path, now_ms: u64, min_gap: Duration) -> bool {
        let mut last = self
            .last_write
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Monotonic, never wall clock: a wall-clock comparison would refuse
        // to record real keypresses for as long as a backwards jump lasted.
        if last.is_some_and(|at| at.elapsed() < min_gap) {
            return false;
        }
        // Unique staging name per write: same pid, many threads.
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let staged = dir.join(format!(".input-activity.{}.{seq}.tmp", std::process::id()));
        let payload = json!({ "lastInputAtMs": now_ms }).to_string();
        #[cfg(test)]
        let injected = self.fail_after_create.load(Ordering::Relaxed);
        #[cfg(not(test))]
        let injected = false;
        if injected {
            // Simulate the half-written file the real failure would leave.
            let _ = std::fs::write(&staged, &payload[..payload.len() / 2]);
        }
        if injected || std::fs::write(&staged, payload).is_err() {
            // Clean up either way: the name carries an ever-increasing
            // sequence, so a persistent write failure would otherwise pile
            // up partial files without bound.
            let _ = std::fs::remove_file(&staged);
            return false;
        }
        // Rename is what makes a reader see either the old file or the whole
        // new one, never a partial write.
        if std::fs::rename(&staged, dir.join(INPUT_ACTIVITY_FILE)).is_err() {
            let _ = std::fs::remove_file(&staged);
            return false;
        }
        *last = Some(Instant::now());
        true
    }
}

/// Is this line a keypress at all?
///
/// The whole point of the listener is what it IGNORES: `RawMotion` is what a
/// drifting mouse sensor emits by the hundred per second, and counting it is
/// what made every approval look like "the user is at the keyboard".
fn is_keypress_line(line: &str) -> bool {
    line.contains("RawKeyPress")
}

/// Drain one stream, publishing as keypresses arrive. Returns when the
/// stream ends — for a real session that means the child exited, which the
/// caller reaps before reconnecting. The count is for tests only: nothing in
/// production branches on it, since each candidate has its own watcher and
/// there is no "try the next one" decision left to make.
fn record_keypresses<R: std::io::BufRead>(
    reader: R,
    dir: &Path,
    publisher: &ActivityPublisher,
    min_gap: Duration,
    clock: &dyn Fn() -> u64,
) -> usize {
    let mut written = 0usize;
    for line in reader.lines().map_while(Result::ok) {
        if is_keypress_line(&line) && publisher.publish(dir, clock(), min_gap) {
            written += 1;
        }
    }
    written
}

/// A watcher's cancellation state and the child it currently holds, under
/// ONE lock.
///
/// They were two separate fields and that was a deadlock: `cancel` could
/// observe an empty child slot (the opener had not returned yet), conclude
/// there was nothing to kill, and go straight to `join` — while the worker
/// then registered a live, silent child and blocked reading it forever, with
/// nobody left holding a handle to kill. Sol hit exactly this on a full test
/// run: the watcher never finished. Sharing the lock makes "am I cancelled"
/// and "here is my child" a single ordered decision.
#[derive(Default)]
struct WorkerState {
    stopping: bool,
    child: Option<std::process::Child>,
}

/// One candidate's watcher.
struct Worker {
    state: Arc<std::sync::Mutex<WorkerState>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

fn lock_state(state: &std::sync::Mutex<WorkerState>) -> std::sync::MutexGuard<'_, WorkerState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Kill and reap a child. A forgotten child is a zombie, and killing is also
/// what interrupts a read that would otherwise never return.
fn reap(child: Option<std::process::Child>) {
    if let Some(mut child) = child {
        let _ = child.kill();
        let _ = child.wait();
    }
}

impl Worker {
    /// Stop the watcher, reap whatever it holds, and wait for its thread.
    fn cancel(mut self) {
        let child = {
            let mut state = lock_state(&self.state);
            state.stopping = true;
            state.child.take()
        };
        reap(child);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Start one watcher for a candidate. Every candidate gets its own thread on
/// purpose: the drain blocks until its stream ends, so trying candidates in
/// sequence let a session that CONNECTS but stays silent — an idle or wrong
/// X server, or one emitting nothing but the pointer motion we ignore — hold
/// every later candidate and every rescan behind it forever.
fn spawn_worker(
    dir: PathBuf,
    candidate: XCandidate,
    open: Arc<XStreamOpener>,
    publisher: Arc<ActivityPublisher>,
    min_gap: Duration,
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    retry: Duration,
) -> Worker {
    let state = Arc::new(std::sync::Mutex::new(WorkerState::default()));
    let thread_state = Arc::clone(&state);
    let handle = std::thread::spawn(move || loop {
        if lock_state(&thread_state).stopping {
            break;
        }
        if let Some(mut session) = open(&candidate) {
            let child = session.child.take();
            // Re-check UNDER THE LOCK before committing to a blocking read:
            // a cancel that landed while the opener was running has already
            // given up on finding a child here, so this one must be reaped
            // right now rather than read from.
            let orphan = {
                let mut state = lock_state(&thread_state);
                if state.stopping {
                    child
                } else {
                    state.child = child;
                    None
                }
            };
            let rejected = orphan.is_some();
            reap(orphan);
            if rejected {
                // Cancel landed while the opener was running: this child was
                // never visible to `cancel`, so reap it here and stop.
                break;
            }
            record_keypresses(session.reader, &dir, &publisher, min_gap, &|| clock());
            // EOF: the child is gone or going. Reap it — a forgotten child
            // becomes a zombie, once per reconnect.
            let child = lock_state(&thread_state).child.take();
            reap(child);
        }
        // Interruptible wait: a cancel must not have to sit out the retry
        // delay, or switching candidates costs N × retry.
        if wait_or_cancelled(&thread_state, retry) {
            break;
        }
    });
    Worker {
        state,
        handle: Some(handle),
    }
}

/// Sleep for `delay`, returning early (true) as soon as the worker is
/// cancelled. Polled rather than condvar-signalled to keep the state lock a
/// plain mutex that is never held across the blocking kill/wait; 20ms
/// granularity is well inside what "cancel returns promptly" needs.
fn wait_or_cancelled(state: &std::sync::Mutex<WorkerState>, delay: Duration) -> bool {
    let deadline = Instant::now() + delay;
    while Instant::now() < deadline {
        if lock_state(state).stopping {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20).min(delay));
    }
    lock_state(state).stopping
}

/// Bring the running watchers in line with the candidates that exist now:
/// start one for each new candidate, cancel and join the ones whose session
/// is gone. Without the removal half, a rotated session leaves a worker
/// retrying a dead display forever and both threads and children grow
/// without bound.
fn reconcile_workers(
    workers: &mut std::collections::HashMap<XCandidate, Worker>,
    candidates: Vec<XCandidate>,
    mut start: impl FnMut(&XCandidate) -> Worker,
) {
    let wanted: std::collections::HashSet<XCandidate> = candidates.iter().cloned().collect();
    let gone = workers
        .keys()
        .filter(|key| !wanted.contains(*key))
        .cloned()
        .collect::<Vec<_>>();
    for key in gone {
        if let Some(worker) = workers.remove(&key) {
            worker.cancel();
        }
    }
    for candidate in candidates {
        if let std::collections::hash_map::Entry::Vacant(slot) = workers.entry(candidate.clone()) {
            slot.insert(start(&candidate));
        }
    }
}

/// Open a real `xinput` stream for one candidate.
fn open_xinput_stream(candidate: &XCandidate) -> Option<XSession> {
    let (display, xauthority) = candidate;
    let mut command = std::process::Command::new("xinput");
    command
        .args(["test-xi2", "--root"])
        .env("DISPLAY", display)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    match xauthority {
        Some(path) => {
            command.env("XAUTHORITY", path);
        }
        // No XAUTHORITY is legitimate — Xlib then falls back to
        // $HOME/.Xauthority, which is the common setup.
        None => {
            command.env_remove("XAUTHORITY");
        }
    }
    let mut child = command.spawn().ok()?;
    let stdout = child.stdout.take()?;
    Some(XSession {
        reader: Box::new(std::io::BufReader::new(stdout)),
        child: Some(child),
    })
}

/// Watches the X server for KEYSTROKES ONLY and stamps a file the approval
/// gates read.
///
/// The desktop idle timer cannot serve this: it counts pointer motion, and a
/// mouse with a drifting sensor emits it continuously — measured 2026-08-17,
/// ~600 phantom events a second holding idle at 1-48ms for hours while the
/// user was out. Every approval was ruled "user is at the keyboard" and
/// handed to the terminal one poll tick after its buttons reached the phone.
/// Mutter exposes only a combined `/Core` monitor, so separating the two
/// means filtering raw events, and `xinput test-xi2` does it without needing
/// membership of the `input` group.
///
/// Failure is silent and total: no X session, no xinput, a crashed child —
/// the file simply stops advancing, gates read "no recent keypress", and
/// away mode keeps the prompt on the phone. That is the safe direction.
fn spawn_keyboard_listener() {
    std::thread::spawn(|| {
        let Ok(dir) = crate::state::state_dir_path() else {
            return;
        };
        let publisher = Arc::new(ActivityPublisher::default());
        let mut workers: std::collections::HashMap<XCandidate, Worker> =
            std::collections::HashMap::new();
        loop {
            let candidates = x_environment_candidates();
            reconcile_workers(&mut workers, candidates, |candidate| {
                spawn_worker(
                    dir.clone(),
                    candidate.clone(),
                    Arc::new(open_xinput_stream),
                    Arc::clone(&publisher),
                    INPUT_ACTIVITY_MIN_GAP,
                    Arc::new(|| now_millis().unwrap_or(0)),
                    INPUT_LISTENER_RETRY,
                )
            });
            std::thread::sleep(INPUT_LISTENER_RESCAN);
        }
    });
}

/// Every X session we could plausibly attach to, best first.
///
/// `DISPLAY` is required; `XAUTHORITY` is not — a session that relies on the
/// default `$HOME/.Xauthority` is perfectly normal, and demanding the
/// variable skipped those entirely.
fn x_environment_candidates() -> Vec<(String, Option<String>)> {
    let mut candidates = Vec::new();
    let mut push = |display: String, xauthority: Option<String>| {
        if display.is_empty() {
            return;
        }
        let entry = (display, xauthority.filter(|value| !value.is_empty()));
        if !candidates.contains(&entry) {
            candidates.push(entry);
        }
    };
    // Ours first: a daemon started from the desktop session already has the
    // right credentials, on any compositor.
    if let Ok(display) = env::var("DISPLAY") {
        push(display, env::var("XAUTHORITY").ok());
    }
    // Then borrow from compositor processes. The extraction happens INSIDE
    // the scan so an unreadable or incomplete candidate is skipped rather
    // than ending the search.
    let compositors = ["gnome-shell", "kwin_x11", "xfwm4", "mutter", "cinnamon"];
    if let Ok(entries) = std::fs::read_dir("/proc") {
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            let Ok(comm) = std::fs::read_to_string(format!("/proc/{pid}/comm")) else {
                continue;
            };
            if !compositors.contains(&comm.trim()) {
                continue;
            }
            let Ok(environ) = std::fs::read(format!("/proc/{pid}/environ")) else {
                continue;
            };
            let mut display = None;
            let mut xauthority = None;
            for field in environ.split(|byte| *byte == 0) {
                let text = String::from_utf8_lossy(field);
                if let Some(value) = text.strip_prefix("DISPLAY=") {
                    display = Some(value.to_string());
                } else if let Some(value) = text.strip_prefix("XAUTHORITY=") {
                    xauthority = Some(value.to_string());
                }
            }
            if let Some(display) = display {
                push(display, xauthority);
            }
        }
    }
    candidates
}

pub(crate) fn run_daemon(once: bool, poll_interval: u64, timeout: Duration) -> Result<()> {
    let _daemon_lock = acquire_daemon_lock()?;
    let db_path = state_db_path()?;
    let conn = create_state_db(&db_path)?;
    let config = load_daemon_config()?;
    if once {
        let result = daemon_cycle(&conn, &config, now_millis()?, timeout)?;
        println!("{}", serde_json::to_string(&result)?);
        return Ok(());
    }

    spawn_keyboard_listener();

    let telegram_commands = config.telegram.as_ref().map(|telegram| {
        telegram_set_my_commands(telegram, timeout)
            .map(|_| json!({ "registered": true }))
            .unwrap_or_else(|error| {
                json!({
                    "registered": false,
                    "error": format!("{error:#}")
                })
            })
    });

    println!(
        "{}",
        serde_json::to_string(&json!({
            "ok": true,
            "action": "daemon_started",
            "configPath": daemon_config_path()?.display().to_string(),
            "events": config.events,
            "telegramCommands": telegram_commands
        }))?
    );
    let watch_rx = start_claude_watch_receiver().ok();
    let mut last_prune_at = 0u64;
    let mut last_updates_at = 0u64;
    let mut last_sync_at = 0u64;
    // A spool/projects wake means a hook just fired: the next cycle must run
    // the full sync immediately, whatever the cadence says — that is the
    // push-latency path.
    let mut woken = true;
    let mut cached_config: Option<DaemonConfig> = None;
    loop {
        // Reloading (read + parse) 10x a second is pure waste; the config
        // refreshes on the sync cadence, which is how often it mattered
        // before the fast tick existed.
        if cached_config.is_none()
            || now_millis()
                .map(|now| now.saturating_sub(last_sync_at) >= FULL_SYNC_MIN_INTERVAL_MS)
                .unwrap_or(true)
        {
            cached_config = None;
        }
        let config = match cached_config.clone() {
            Some(config) => config,
            None => match load_daemon_config() {
                Ok(config) => {
                    cached_config = Some(config.clone());
                    config
                }
                Err(error) => {
                    println!(
                        "{}",
                        json!({
                            "ok": false,
                            "action": "daemon_config_error",
                            "error": format!("{error:#}")
                        })
                    );
                    thread::sleep(Duration::from_millis(poll_interval));
                    continue;
                }
            },
        };
        let now = match now_millis() {
            Ok(now) => now,
            Err(error) => {
                println!(
                    "{}",
                    json!({
                        "ok": false,
                        "action": "daemon_clock_error",
                        "error": format!("{error:#}")
                    })
                );
                thread::sleep(Duration::from_millis(poll_interval));
                continue;
            }
        };
        if now.saturating_sub(last_prune_at) >= DAEMON_PRUNE_INTERVAL_MS {
            match prune_state_logs(&conn, now) {
                Ok(removed) => {
                    println!(
                        "{}",
                        json!({
                            "ok": true,
                            "action": "daemon_logs_pruned",
                            "removed": removed
                        })
                    );
                }
                Err(error) => {
                    println!(
                        "{}",
                        json!({
                            "ok": false,
                            "action": "daemon_logs_prune_error",
                            "error": format!("{error:#}")
                        })
                    );
                }
            }
            last_prune_at = now;
        }
        let lanes = CycleLanes {
            telegram_updates: now.saturating_sub(last_updates_at)
                >= TELEGRAM_UPDATES_MIN_INTERVAL_MS,
            full_sync: woken || now.saturating_sub(last_sync_at) >= FULL_SYNC_MIN_INTERVAL_MS,
        };
        if lanes.telegram_updates {
            last_updates_at = now;
        }
        if lanes.full_sync {
            last_sync_at = now;
        }
        match daemon_cycle_lanes(&conn, &config, now, timeout, lanes) {
            // Ten JSON lines a second would drown the log in idle ticks:
            // fast lanes stay silent unless something actually happened.
            Ok(result) => {
                let noteworthy = lanes.full_sync
                    || result.get("enqueued").and_then(Value::as_u64).unwrap_or(0) > 0
                    || result
                        .pointer("/delivery/delivered")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        > 0
                    || result
                        .pointer("/bridgeTurns/answered")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        > 0;
                if noteworthy {
                    println!("{}", result);
                }
            }
            Err(error) => {
                println!(
                    "{}",
                    json!({
                        "ok": false,
                        "action": "daemon_cycle_error",
                        "error": format!("{error:#}")
                    })
                );
            }
        }
        woken = match watch_rx.as_ref() {
            Some(rx) => rx.recv_timeout(Duration::from_millis(poll_interval)),
            None => {
                thread::sleep(Duration::from_millis(poll_interval));
                false
            }
        };
    }
}

fn validate_daemon_label(label: &str) -> Result<&str> {
    let trimmed = label.trim();
    if trimmed.is_empty() {
        bail!("daemon label cannot be empty");
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains('\'') {
        bail!("daemon label contains unsupported characters");
    }
    Ok(trimmed)
}

fn push_path_entry(entries: &mut Vec<String>, path: impl Into<String>) {
    let path = path.into();
    if path.trim().is_empty() || entries.iter().any(|entry| entry == &path) {
        return;
    }
    entries.push(path);
}

fn push_home_path(entries: &mut Vec<String>, home: &Path, suffix: &str) {
    push_path_entry(entries, home.join(suffix).display().to_string());
}

fn push_node_version_bins(entries: &mut Vec<String>, versions_dir: PathBuf) {
    let Ok(children) = fs::read_dir(versions_dir) else {
        return;
    };
    let mut bins = children
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("bin"))
        .filter(|path| path.is_dir())
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    bins.sort();
    for bin in bins {
        push_path_entry(entries, bin);
    }
}

fn daemon_runtime_path() -> String {
    let mut entries = Vec::new();
    if let Some(home) = dirs::home_dir() {
        push_home_path(&mut entries, &home, ".local/bin");
        push_home_path(&mut entries, &home, ".bun/bin");
        push_home_path(&mut entries, &home, ".cargo/bin");
        push_home_path(&mut entries, &home, ".deno/bin");
        push_home_path(&mut entries, &home, ".pyenv/shims");
        push_home_path(&mut entries, &home, ".asdf/shims");
        push_home_path(&mut entries, &home, "Library/pnpm");
        push_home_path(&mut entries, &home, "go/bin");
        push_node_version_bins(&mut entries, home.join(".config/nvm/versions/node"));
        push_node_version_bins(&mut entries, home.join(".nvm/versions/node"));
    }

    for path in [
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
    ] {
        push_path_entry(&mut entries, path);
    }

    if let Some(current) = env::var_os("PATH").and_then(|value| value.into_string().ok()) {
        for path in current.split(':') {
            push_path_entry(&mut entries, path);
        }
    }

    entries.join(":")
}

fn systemd_escape_env(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%")
}

/// Directory holding the service definition (`~/.config/systemd/user` /
/// `~/Library/LaunchAgents`). `TINYCTB_SERVICE_DIR` overrides it so tests
/// never see — let alone stop — the user's real daemon service.
fn service_definition_dir(platform_default: PathBuf) -> PathBuf {
    match env::var("TINYCTB_SERVICE_DIR") {
        Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
        _ => platform_default,
    }
}

pub(crate) fn daemon_service_spec(label: &str, bridge_command: &str) -> Result<DaemonServiceSpec> {
    let label = validate_daemon_label(label)?;
    let bridge_command = bridge_command.trim();
    if bridge_command.is_empty() {
        bail!("bridge command cannot be empty");
    }
    let service_bridge_command = resolve_service_bridge_command(bridge_command);
    let state_dir = state_dir_path()?;
    let logs_dir = state_dir.join("logs");
    let stdout_log = logs_dir.join("daemon.out.log");
    let stderr_log = logs_dir.join("daemon.err.log");
    let runtime_path = daemon_runtime_path();
    // A user-set CLAUDE_BIN is authoritative for the backend (no fallback), so
    // it must reach the service environment too — otherwise the terminal and
    // the background daemon would resolve different claude binaries.
    // Relative CLAUDE_BIN values are resolved against the install-time cwd:
    // the service manager starts the daemon from a different working directory,
    // so a relative path that works in the terminal would break in the agent.
    // Symlinks are deliberately NOT resolved (a claude upgrade may repoint them).
    let claude_bin_override = env::var("CLAUDE_BIN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| absolutize_env_path(&value));
    // A TINYCTB_STATE_DIR override must reach the service environment for the
    // same reason: an isolated run (tests, scripts/verify_macos.sh) that
    // installs a service against an alternate state dir needs the daemon it
    // starts to read that same directory instead of the real ~/.tinyctb.
    let state_dir_override = env::var("TINYCTB_STATE_DIR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| absolutize_env_path(&value));
    if cfg!(target_os = "macos") {
        let launch_agents_dir = service_definition_dir(
            dirs::home_dir()
                .context("home directory is not available")?
                .join("Library")
                .join("LaunchAgents"),
        );
        return Ok(macos_launchd_spec(
            label,
            &service_bridge_command,
            &launch_agents_dir,
            &stdout_log,
            &stderr_log,
            &runtime_path,
            claude_bin_override.as_deref(),
            state_dir_override.as_deref(),
        ));
    }
    if cfg!(target_os = "linux") {
        let unit_name = if label.ends_with(".service") {
            label.to_string()
        } else {
            format!("{label}.service")
        };
        let service_path = service_definition_dir(
            dirs::home_dir()
                .context("home directory is not available")?
                .join(".config")
                .join("systemd")
                .join("user"),
        )
        .join(&unit_name);
        let extra_env = [
            ("CLAUDE_BIN", claude_bin_override.as_deref()),
            ("TINYCTB_STATE_DIR", state_dir_override.as_deref()),
        ]
        .iter()
        .filter_map(|(key, value)| {
            value.map(|value| format!("Environment=\"{key}={}\"\n", systemd_escape_env(value)))
        })
        .collect::<String>();
        let contents = format!(
            // Delegate=yes: systemd hands the service its cgroup SUBTREE,
            // which is what lets the daemon create one cgroup per headless
            // turn — the ownership object the whole /stop machinery kills
            // and probes. Without it, sub-cgroup writes are unsupported.
            "[Unit]\nDescription=tinyCTB Claude Telegram bridge daemon\n\n[Service]\nType=simple\nDelegate=yes\nKillMode=process\nEnvironment=\"PATH={}\"\n{}ExecStart={} daemon run\nRestart=always\nRestartSec=2\nStandardOutput=append:{}\nStandardError=append:{}\n\n[Install]\nWantedBy=default.target\n",
            systemd_escape_env(&runtime_path),
            extra_env,
            shell_quote(&service_bridge_command),
            stdout_log.display(),
            stderr_log.display()
        );
        Ok(DaemonServiceSpec {
            service_path,
            stdout_log,
            stderr_log,
            unit_name: unit_name.clone(),
            contents,
            install_command: format!(
                "systemctl --user daemon-reload && systemctl --user enable --now {}",
                shell_quote(&unit_name)
            ),
            uninstall_command: format!(
                "systemctl --user disable --now {} 2>/dev/null || true",
                shell_quote(&unit_name)
            ),
            start_command: format!(
                "systemctl --user daemon-reload && systemctl --user enable --now {}",
                shell_quote(&unit_name)
            ),
            stop_command: format!("systemctl --user stop {}", shell_quote(&unit_name)),
            status_command: format!("systemctl --user status {}", shell_quote(&unit_name)),
        })
    } else {
        bail!("daemon service install is only supported on macOS launchd and Linux systemd")
    }
}

/// Resolve an env-var path override (CLAUDE_BIN, TINYCTB_STATE_DIR) to an
/// absolute path against the current working directory. The service manager
/// launches the daemon from a different cwd, so a relative value that works in
/// the terminal would otherwise break. Symlinks are intentionally NOT resolved
/// (a claude upgrade may repoint them).
fn absolutize_env_path(value: &str) -> String {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        return value.to_string();
    }
    env::current_dir()
        .map(|cwd| cwd.join(&path).display().to_string())
        .unwrap_or_else(|_| value.to_string())
}

/// Pure builder for the macOS LaunchAgent spec so the generated plist and
/// launchctl commands can be unit-tested on any platform.
#[allow(clippy::too_many_arguments)]
fn macos_launchd_spec(
    label: &str,
    service_bridge_command: &str,
    launch_agents_dir: &Path,
    stdout_log: &Path,
    stderr_log: &Path,
    runtime_path: &str,
    claude_bin_override: Option<&str>,
    state_dir_override: Option<&str>,
) -> DaemonServiceSpec {
    let service_path = launch_agents_dir.join(format!("{label}.plist"));
    let run_args = [
        service_bridge_command.to_string(),
        "daemon".to_string(),
        "run".to_string(),
    ];
    let args_xml = run_args
        .iter()
        .map(|arg| format!("        <string>{}</string>", xml_escape(arg)))
        .collect::<Vec<_>>()
        .join("\n");
    let extra_env_xml = [
        ("CLAUDE_BIN", claude_bin_override),
        ("TINYCTB_STATE_DIR", state_dir_override),
    ]
    .iter()
    .filter_map(|(key, value)| {
        value.map(|value| {
            format!(
                "        <key>{key}</key>\n        <string>{}</string>\n",
                xml_escape(value)
            )
        })
    })
    .collect::<String>();
    let contents = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{}</string>
    <key>ProgramArguments</key>
    <array>
{}
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{}</string>
{}    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{}</string>
    <key>StandardErrorPath</key>
    <string>{}</string>
</dict>
</plist>
"#,
        xml_escape(label),
        args_xml,
        xml_escape(runtime_path),
        extra_env_xml,
        xml_escape(&stdout_log.display().to_string()),
        xml_escape(&stderr_log.display().to_string())
    );
    let quoted_path = shell_quote(&service_path.display().to_string());
    let bootstrap_command = format!("launchctl bootstrap gui/$(id -u) {quoted_path}");
    let bootout_command = format!("launchctl bootout gui/$(id -u)/{}", shell_quote(label));
    // Reload semantics: an already-loaded agent keeps its old definition, so
    // both install and start must bootout the existing job (ignoring "not
    // loaded" failures) before bootstrapping the current plist.
    let reload_command = format!("{bootout_command} 2>/dev/null; {bootstrap_command}");
    DaemonServiceSpec {
        service_path,
        stdout_log: stdout_log.to_path_buf(),
        stderr_log: stderr_log.to_path_buf(),
        unit_name: label.to_string(),
        contents,
        install_command: reload_command.clone(),
        uninstall_command: format!("{bootout_command} 2>/dev/null || true"),
        start_command: reload_command,
        stop_command: bootout_command,
        status_command: format!("launchctl print gui/$(id -u)/{}", shell_quote(label)),
    }
}

fn resolve_service_bridge_command(bridge_command: &str) -> String {
    let trimmed = bridge_command.trim();
    if trimmed.contains('/') {
        let path = PathBuf::from(trimmed);
        return if path.is_absolute() {
            path.display().to_string()
        } else {
            env::current_dir()
                .map(|cwd| cwd.join(path).display().to_string())
                .unwrap_or_else(|_| trimmed.to_string())
        };
    }
    if let Ok(path) = which::which(trimmed) {
        return path.display().to_string();
    }
    // The service's first ProgramArguments/ExecStart entry must be an absolute
    // path — a bare name would depend on the service manager's own PATH.
    env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| trimmed.to_string())
}

pub(crate) fn install_daemon_service(
    label: &str,
    bridge_command: &str,
    dry_run: bool,
) -> Result<Value> {
    let spec = daemon_service_spec(label, bridge_command)?;
    if !dry_run {
        if let Some(parent) = spec.service_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = spec.stdout_log.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&spec.service_path, &spec.contents)?;
    }
    Ok(json!({
        "ok": true,
        "action": "daemon_install",
        "dryRun": dry_run,
        "label": spec.unit_name,
        "servicePath": spec.service_path,
        "runCommand": crate::daemon_run_command(bridge_command),
        "installCommand": spec.install_command,
        "startCommand": spec.start_command,
        "stopCommand": spec.stop_command,
        "statusCommand": spec.status_command,
        "logs": {
            "stdout": spec.stdout_log,
            "stderr": spec.stderr_log
        },
        "contents": if dry_run { Some(spec.contents) } else { None }
    }))
}

pub(crate) fn uninstall_daemon_service(label: &str, dry_run: bool) -> Result<Value> {
    let spec = daemon_service_spec(label, "tinyctb")?;
    let output = if dry_run {
        None
    } else {
        Some(run_shell_command(&spec.uninstall_command)?)
    };
    // The uninstall command tolerates failures (`|| true`), so "stopped"
    // must be PROVEN, not assumed: the daemon lock has to be free, the
    // spawn lock held exclusively, and only then are the turn objects
    // swept — with KillMode=process sparing sub-trees, walking away here
    // would strand them. Anything that cannot be proven empty and removed
    // aborts before the unit file is touched.
    // Held past the unit-file removal below.
    let mut _spawn_freeze: Option<fs::File> = None;
    if !dry_run {
        let started = Instant::now();
        loop {
            if daemon_lock_free()? {
                break;
            }
            if started.elapsed() >= Duration::from_secs(5) {
                bail!("the daemon is still running despite the stop; refusing to uninstall");
            }
            thread::sleep(Duration::from_millis(200));
        }
        _spawn_freeze = Some(hold_spawn_lock_exclusive(Duration::from_secs(5))?);
        let report = crate::claude::turn_cgroup::sweep_all(std::time::Duration::from_secs(5))?;
        if !report.stubborn.is_empty() {
            bail!(
                "refusing to uninstall: {} turn cgroup(s) could not be proven empty and removed                  ({}); stop the work they contain first",
                report.stubborn.len(),
                report
                    .stubborn
                    .iter()
                    .map(|dir| dir.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        let db_path = state_db_path()?;
        if db_path.exists() {
            match crate::state::create_state_db(&db_path) {
                Ok(conn) => {
                    let legacy_active = crate::state::count_active_legacy_turns(&conn)?;
                    if legacy_active > 0 {
                        bail!(
                            "refusing to uninstall: {legacy_active} legacy-supervised turn(s) \
                             (no cgroup) are still active; stop them or let them finish first"
                        );
                    }
                }
                Err(err) => {
                    // FAIL CLOSED, same as reset: unreadable is not empty.
                    bail!(
                        "refusing to uninstall: state.db exists but is unreadable ({err:#}); \
                         verify no legacy-supervised process survives, remove the file \
                         manually, then uninstall again"
                    );
                }
            }
        }
    }
    if !dry_run && spec.service_path.exists() {
        fs::remove_file(&spec.service_path)?;
    }
    Ok(json!({
        "ok": true,
        "action": "daemon_uninstall",
        "dryRun": dry_run,
        "label": spec.unit_name,
        "servicePath": spec.service_path,
        "uninstallCommand": spec.uninstall_command,
        "output": output
    }))
}

fn run_shell_command(command: &str) -> Result<Value> {
    let output = Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .output()
        .with_context(|| format!("failed to run `{command}`"))?;
    Ok(json!({
        "status": output.status.code(),
        "success": output.status.success(),
        "stdout": String::from_utf8_lossy(&output.stdout).trim(),
        "stderr": String::from_utf8_lossy(&output.stderr).trim()
    }))
}

fn macos_service_runtime(output: &Value) -> Value {
    let stdout = output
        .get("stdout")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let stderr = output
        .get("stderr")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let success = output
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !success {
        let not_loaded = stderr.contains("Could not find service")
            || stderr.contains("could not find service")
            || stderr.contains("service not found");
        return json!({
            "loaded": !not_loaded,
            "running": false,
            "state": Value::Null,
            "raw": output
        });
    }

    let state = stdout.lines().find_map(|line| {
        line.trim()
            .strip_prefix("state = ")
            .map(|value| value.trim().to_string())
    });
    let running = matches!(state.as_deref(), Some("running"));
    json!({
        "loaded": true,
        "running": running,
        "state": state,
        "raw": output
    })
}

fn linux_service_runtime(unit_name: &str, status_output: &Value) -> Result<Value> {
    let active_output = run_shell_command(&format!(
        "systemctl --user is-active {}",
        shell_quote(unit_name)
    ))?;
    let active = active_output
        .get("stdout")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    let loaded = status_output
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || active != "inactive";
    Ok(json!({
        "loaded": loaded,
        "running": active == "active",
        "state": if active.is_empty() { Value::Null } else { json!(active) },
        "raw": {
            "status": status_output,
            "isActive": active_output
        }
    }))
}

fn service_runtime_status(spec: &DaemonServiceSpec) -> Result<Value> {
    if !spec.service_path.exists() {
        return Ok(json!({
            "loaded": false,
            "running": false,
            "state": Value::Null,
            "raw": Value::Null
        }));
    }

    if cfg!(target_os = "macos") {
        let output = run_shell_command(&spec.status_command)?;
        return Ok(macos_service_runtime(&output));
    }

    if cfg!(target_os = "linux") {
        let output = run_shell_command(&spec.status_command)?;
        return linux_service_runtime(&spec.unit_name, &output);
    }

    Ok(json!({
        "loaded": false,
        "running": false,
        "state": Value::Null,
        "raw": Value::Null
    }))
}

pub(crate) fn start_daemon_service(label: &str, dry_run: bool) -> Result<Value> {
    let spec = daemon_service_spec(label, "tinyctb")?;
    let command = spec.start_command.clone();
    if dry_run {
        return Ok(json!({
            "ok": true,
            "action": "daemon_start",
            "dryRun": true,
            "label": spec.unit_name,
            "command": command,
            "output": Value::Null
        }));
    }
    let output = run_shell_command(&command)?;
    let command_ok = output
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    // The start command returning 0 does not prove the daemon stayed up (bad
    // plist/unit contents, wrong binary path, crash on boot). Poll the service
    // manager until it reports running or the grace window expires.
    let mut verified_running = false;
    let mut runtime = Value::Null;
    if command_ok {
        for _ in 0..15 {
            runtime = service_runtime_status(&spec)?;
            if runtime.get("running").and_then(Value::as_bool) == Some(true) {
                verified_running = true;
                break;
            }
            thread::sleep(Duration::from_millis(400));
        }
    }
    // A failed start is a hard error so `tinyctb daemon start` exits non-zero
    // and callers (setup) cannot silently report success.
    if !command_ok {
        bail!(
            "daemon start command failed (`{}`): status {:?}, stderr: {}",
            command,
            output.get("status").and_then(Value::as_i64),
            output.get("stderr").and_then(Value::as_str).unwrap_or("")
        );
    }
    if !verified_running {
        bail!(
            "daemon start command succeeded but service `{}` never reached running state within the grace window; check the daemon logs ({})",
            spec.unit_name,
            spec.stderr_log.display()
        );
    }
    Ok(json!({
        "ok": true,
        "action": "daemon_start",
        "dryRun": false,
        "label": spec.unit_name,
        "command": command,
        "output": output,
        "verifiedRunning": true,
        "serviceStatus": runtime
    }))
}

pub(crate) fn stop_daemon_service(label: &str, dry_run: bool) -> Result<Value> {
    let spec = daemon_service_spec(label, "tinyctb")?;
    let command = spec.stop_command.clone();
    let output = if dry_run {
        None
    } else {
        Some(run_shell_command(&command)?)
    };
    Ok(json!({
        "ok": true,
        "action": "daemon_stop",
        "dryRun": dry_run,
        "label": spec.unit_name,
        "command": command,
        "output": output
    }))
}

pub(crate) fn daemon_service_status(label: &str) -> Result<Value> {
    let spec = daemon_service_spec(label, "tinyctb")?;
    let config_path = daemon_config_path()?;
    let service_status = service_runtime_status(&spec)?;
    let running = service_status
        .get("running")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let healthy = config_path.exists() && spec.service_path.exists() && running;
    Ok(json!({
        "ok": true,
        "action": "daemon_status",
        "label": spec.unit_name,
        "healthy": healthy,
        "configPath": config_path,
        "configExists": config_path.exists(),
        "servicePath": spec.service_path,
        "serviceExists": spec.service_path.exists(),
        "serviceStatus": service_status,
        "nextStep": if healthy {
            Value::Null
        } else {
            json!("Run `tinyctb daemon start` to restore background Telegram delivery.")
        },
        "statusCommand": spec.status_command,
        "logs": {
            "stdout": spec.stdout_log,
            "stderr": spec.stderr_log
        }
    }))
}

pub(crate) fn daemon_service_logs(label: &str) -> Result<Value> {
    let spec = daemon_service_spec(label, "tinyctb")?;
    Ok(json!({
        "ok": true,
        "action": "daemon_logs",
        "label": spec.unit_name,
        "stdout": spec.stdout_log,
        "stderr": spec.stderr_log,
        "tailCommand": format!(
            "tail -f {} {}",
            shell_quote(&spec.stdout_log.display().to_string()),
            shell_quote(&spec.stderr_log.display().to_string())
        )
    }))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claude::set_away_mode;
    use crate::state::{create_state_db_in_memory, pending_outbound_count};

    struct TempServiceEnv {
        previous_state_dir: Option<String>,
        previous_service_dir: Option<String>,
        root: std::path::PathBuf,
    }

    impl TempServiceEnv {
        /// Keeps service-spec tests away from the real ~/.tinyctb and the real
        /// service definitions (read-only CI homes, and `reset`-style stops).
        fn new(name: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("tinyctb-daemon-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create temp daemon dir");
            let previous_state_dir = std::env::var("TINYCTB_STATE_DIR").ok();
            let previous_service_dir = std::env::var("TINYCTB_SERVICE_DIR").ok();
            std::env::set_var("TINYCTB_STATE_DIR", root.join("state"));
            std::env::set_var("TINYCTB_SERVICE_DIR", root.join("service"));
            Self {
                previous_state_dir,
                previous_service_dir,
                root,
            }
        }
    }

    impl Drop for TempServiceEnv {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous_state_dir {
                std::env::set_var("TINYCTB_STATE_DIR", previous);
            } else {
                std::env::remove_var("TINYCTB_STATE_DIR");
            }
            if let Some(previous) = &self.previous_service_dir {
                std::env::set_var("TINYCTB_SERVICE_DIR", previous);
            } else {
                std::env::remove_var("TINYCTB_SERVICE_DIR");
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn text_session(text: &str) -> XSession {
        XSession {
            reader: Box::new(std::io::Cursor::new(text.to_string())),
            child: None,
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tinyctb-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        dir
    }

    /// Candidates are watched IN PARALLEL, so one that connects but never
    /// says anything useful cannot hold the others hostage. Trying them in
    /// sequence deadlocked exactly here: the drain blocks until EOF, and a
    /// live-but-silent stream never reaches it.
    ///
    /// The wait happens on ANOTHER thread with a channel timeout, so a
    /// regression to serial blocking fails this test instead of hanging it.
    /// A turn whose log produced an answer just before the user stopped it
    /// is still a STOPPED turn. Letting `done` win would erase the intent —
    /// and with it the daemon's obligation to keep confirming the process
    /// actually died.
    #[test]
    fn a_completion_cannot_overwrite_a_stop_in_progress() {
        let conn = create_state_db_in_memory().expect("db");
        crate::state::register_bridge_turn(
            &conn,
            "turn-1",
            "sess-1",
            "/tmp/t.log",
            Some(4_000_090),
            None,
            None,
            Some(4_000_090),
            None,
            None,
            1000,
        )
        .expect("register");
        crate::state::mark_bridge_turn_stopping(&conn, "turn-1", 1500).expect("intent");

        crate::state::mark_bridge_turn_finished(&conn, "turn-1", "done", 2000).expect("finish");

        assert_eq!(
            crate::state::bridge_turn_status(&conn, "turn-1").expect("status"),
            Some("stopping".to_string()),
            "a late completion must not erase the stop intent"
        );
        // And a turn that was never stopping still settles normally.
        crate::state::register_bridge_turn(
            &conn,
            "turn-2",
            "sess-1",
            "/tmp/t.log",
            Some(4_000_091),
            None,
            None,
            Some(4_000_091),
            None,
            None,
            1000,
        )
        .expect("register");
        crate::state::mark_bridge_turn_finished(&conn, "turn-2", "done", 2000).expect("finish");
        assert_eq!(
            crate::state::bridge_turn_status(&conn, "turn-2").expect("status"),
            Some("done".to_string())
        );
    }

    /// The recovery state machine has exactly three answers, and the wrong
    /// one in either direction is a real failure: settling on ignorance
    /// loses a live process forever, and refusing to settle a dead one
    /// leaves the user's stop half-done.
    #[test]
    fn stopping_recovery_settles_only_on_a_provably_empty_group() {
        use crate::claude::Liveness;
        let _guard = crate::state::test_env_lock();

        for (verdict, expect_settled, expect_status) in [
            (Liveness::Unknown, false, "stopping"),
            (Liveness::Alive, false, "stopping"),
            (Liveness::Ended, true, "stopped"),
        ] {
            let conn = create_state_db_in_memory().expect("db");
            let _ = crate::claude::test_kill::take();
            crate::state::register_bridge_turn(
                &conn,
                "turn-1",
                "sess-1",
                "/tmp/t.log",
                Some(4_000_080),
                None,
                None,
                Some(4_000_080),
                None,
                None,
                1000,
            )
            .expect("register");
            crate::state::mark_bridge_turn_stopping(&conn, "turn-1", 1500).expect("intent");
            crate::state::create_pending_approval(
                &conn,
                "ap-1",
                "sess-1",
                "Bash",
                "x",
                true,
                1000,
                9_000_000_000,
            )
            .expect("approval");
            crate::state::record_approval_turn_owner(&conn, "ap-1", "turn-1").expect("owner");

            let _probe = crate::claude::test_group_probe::ProbeGuard::set(verdict);
            let turn = crate::state::list_running_bridge_turns(&conn)
                .expect("turns")
                .into_iter()
                .next()
                .expect("turn");
            let settled = advance_stopping_turn(&conn, &turn, 5000).expect("advance");
            drop(_probe);

            assert_eq!(settled, expect_settled, "{verdict:?}");
            assert_eq!(
                crate::state::bridge_turn_status(&conn, "turn-1").expect("status"),
                Some(expect_status.to_string()),
                "{verdict:?}: wrong recovery verdict"
            );
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
                "{verdict:?}: a stopping turn's dialogs close on every pass — the \
                 stop INTENT is what ends them, not the settle"
            );
            let notice: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM outbound_events
                     WHERE json_extract(payload_json, '$.eventKey') = 'stop-settled:turn-1'",
                    [],
                    |row| row.get(0),
                )
                .expect("count");
            assert_eq!(
                notice,
                i64::from(expect_settled),
                "{verdict:?}: exactly a SETTLED recovery must enqueue its confirmation"
            );
            if verdict == Liveness::Alive {
                assert_eq!(
                    crate::claude::test_kill::take(),
                    vec![4_000_080],
                    "a group still alive must be signalled again"
                );
            } else {
                assert!(
                    crate::claude::test_kill::take().is_empty(),
                    "{verdict:?}: nothing to signal"
                );
            }
        }
    }

    /// A supervised turn is ROUTED into stopping-recovery, not into the
    /// crash-claim path: with the leader already reaped (`exited`), the
    /// ordinary flow would settle it as an unexplained failure — burying
    /// the unproven group — where recovery re-kills and keeps probing.
    #[test]
    fn a_supervised_turn_is_routed_to_recovery_not_the_crash_claim() {
        use crate::claude::Liveness;
        let _guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        crate::state::register_bridge_turn(
            &conn,
            "turn-1",
            "sess-1",
            "/tmp/t.log",
            Some(4_000_097),
            None,
            None,
            Some(4_000_097),
            None,
            None,
            1000,
        )
        .expect("register");
        crate::state::record_bridge_turn_exit(&conn, "turn-1", Some(0)).expect("exit");
        conn.execute(
            "UPDATE bridge_turns SET cleanup_pending = 1 WHERE turn_id = 'turn-1'",
            [],
        )
        .expect("marker");

        let _probe = crate::claude::test_group_probe::ProbeGuard::set(Liveness::Alive);
        process_bridge_turns(&conn, &bridge_test_config(), 20_000).expect("cycle");

        let status: String = conn
            .query_row("SELECT status FROM bridge_turns", [], |row| row.get(0))
            .expect("status");
        assert_eq!(
            status, "running",
            "a supervised turn must not be settled as an unexplained crash"
        );
        assert_eq!(
            crate::claude::test_kill::take(),
            vec![4_000_097],
            "recovery must have re-signalled the still-live group instead"
        );
    }

    /// An identityless turn that COMPLETES must still deliver its answer
    /// and settle, and one that never completes must still hit the hard
    /// timeout — the ordinary flow stays reachable behind the alarm. An
    /// unconditional `continue` in the supervised branch once made both
    /// unreachable: answered turns showed running forever.
    #[test]
    fn an_identityless_turn_still_completes_and_times_out() {
        let _guard = crate::state::test_env_lock();
        // Completion: the answer in the log is delivered and the turn done.
        let conn = create_state_db_in_memory().expect("db");
        let log = write_turn_log(
            "sess-idless-done",
            &[r#"{"type":"result","subtype":"success","is_error":false,"result":"答案"}"#],
        );
        crate::state::register_bridge_turn(
            &conn,
            "turn-1",
            "sess-idless",
            &log.display().to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        let summary = process_bridge_turns(&conn, &bridge_test_config(), 5_000).expect("cycle");
        assert_eq!(
            summary["answered"], 1,
            "the answer must be delivered: {summary}"
        );
        assert_eq!(
            crate::state::bridge_turn_status(&conn, "turn-1").expect("status"),
            Some("done".to_string()),
            "a completed identityless turn settles"
        );

        // Hard timeout: a silent identityless turn expires by fiat at 6h.
        let conn = create_state_db_in_memory().expect("db");
        let log = write_turn_log("sess-idless-quiet", &["nothing to see"]);
        crate::state::register_bridge_turn(
            &conn,
            "turn-2",
            "sess-idless-2",
            &log.display().to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        let past_timeout = 1000 + BRIDGE_TURN_MAX_RUNTIME_MS + 1;
        process_bridge_turns(&conn, &bridge_test_config(), past_timeout).expect("cycle");
        assert_eq!(
            crate::state::bridge_turn_status(&conn, "turn-2").expect("status"),
            Some("expired".to_string()),
            "the hard timeout must remain reachable for identityless turns"
        );
    }

    /// The birth debt is NOT a stop: a healthy newborn (registered,
    /// identity not yet landed) whose first tool call already published an
    /// approval must come through a daemon tick with its dialog open and
    /// nothing signalled — sweeping it was the review's P1.
    #[test]
    fn a_newborns_first_approval_survives_the_daemon_tick() {
        let _guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        crate::state::register_bridge_turn(
            &conn,
            "turn-new",
            "sess-new",
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
        crate::state::create_pending_approval(
            &conn,
            "ap-1",
            "sess-new",
            "Bash",
            "x",
            true,
            1500,
            9_000_000_000,
        )
        .expect("approval");
        crate::state::record_approval_turn_owner(&conn, "ap-1", "turn-new").expect("owner");

        process_bridge_turns(&conn, &bridge_test_config(), 6_000).expect("cycle");

        let decision: Option<String> = conn
            .query_row(
                "SELECT decision FROM pending_approvals WHERE approval_id = 'ap-1'",
                [],
                |row| row.get(0),
            )
            .expect("approval");
        assert_eq!(
            decision, None,
            "the newborn's dialog must survive the tick — the birth debt is not a stop"
        );
        assert!(
            crate::claude::test_kill::take().is_empty(),
            "and nothing may be signalled at a turn that was never stopped"
        );
        assert_eq!(
            crate::state::bridge_turn_status(&conn, "turn-new").expect("status"),
            Some("running".to_string())
        );
    }

    /// Sixty quiet seconds prove only that the identity write is late —
    /// not that no process exists (the CLI can die between a successful
    /// spawn and the write, leaving a live group). Past the grace the turn
    /// is alarmed ONCE and KEPT: no terminal status, no marker clearing,
    /// in both the `running` and the raced-`stopping` shapes.
    #[test]
    fn an_identityless_turn_is_alarmed_and_kept_not_buried() {
        let _guard = crate::state::test_env_lock();
        for stopping in [false, true] {
            let conn = create_state_db_in_memory().expect("db");
            let _ = crate::claude::test_kill::take();
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
            if stopping {
                crate::state::mark_bridge_turn_stopping(&conn, "turn-1", 1500).expect("stop");
            }
            let expected_status = if stopping { "stopping" } else { "running" };

            // Two ticks past the grace: the alarm must fire exactly once.
            process_bridge_turns(&conn, &bridge_test_config(), 1000 + IDENTITY_GRACE_MS + 1)
                .expect("cycle");
            process_bridge_turns(
                &conn,
                &bridge_test_config(),
                1000 + IDENTITY_GRACE_MS + 5000,
            )
            .expect("cycle again");

            let (status, marker): (String, i64) = conn
                .query_row(
                    "SELECT status, cleanup_pending FROM bridge_turns",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("row");
            assert_eq!(
                status, expected_status,
                "no evidence, no terminal: a live group may exist behind this row"
            );
            if !stopping {
                assert_eq!(marker, 1, "and the birth debt stays on the books");
            }
            assert!(
                crate::claude::test_kill::take().is_empty(),
                "stopping={stopping}: nothing may be signalled without an identity"
            );
            let notice: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM outbound_events
                     WHERE json_extract(payload_json, '$.eventKey') = 'identityless-turn:turn-1'",
                    [],
                    |row| row.get(0),
                )
                .expect("count");
            assert_eq!(
                notice, 1,
                "stopping={stopping}: alarmed exactly once, kept forever"
            );
        }
    }

    /// A TERMINAL row that still carries the cleanup marker is walked by
    /// the daemon end to end: scanned (a running-only scan never saw it),
    /// probed, and — once the group is proven empty — released; prune
    /// spares it while marked and collects it after.
    #[test]
    fn a_terminal_marked_row_is_supervised_until_proven_empty() {
        use crate::claude::Liveness;
        let _guard = crate::state::test_env_lock();
        for marker in [1_i64, 2] {
            let conn = create_state_db_in_memory().expect("db");
            let _ = crate::claude::test_kill::take();
            crate::state::register_bridge_turn(
                &conn,
                "turn-1",
                "sess-1",
                "/tmp/t.log",
                Some(4_000_098),
                None,
                None,
                Some(4_000_098),
                None,
                None,
                1000,
            )
            .expect("register");
            crate::state::mark_bridge_turn_finished(&conn, "turn-1", "failed", 1500).expect("bury");
            // BOTH marker flavours: `cleanup_pending = 1` in the scan predicate
            // let value 2 (a failure cleanup racing a terminal writer) slip out
            // of supervision forever.
            conn.execute(
                "UPDATE bridge_turns SET cleanup_pending = ?1 WHERE turn_id = 'turn-1'",
                rusqlite::params![marker],
            )
            .expect("marker");

            let far_future = 1000 + 31 * 24 * 60 * 60 * 1000;
            crate::state::prune_state_logs(&conn, far_future).expect("prune");
            let rows: i64 = conn
                .query_row("SELECT COUNT(*) FROM bridge_turns", [], |row| row.get(0))
                .expect("count");
            assert_eq!(
                rows, 1,
                "prune must spare a supervised row, whatever its age"
            );

            {
                let _probe = crate::claude::test_group_probe::ProbeGuard::set(Liveness::Ended);
                process_bridge_turns(&conn, &bridge_test_config(), 2_000).expect("cycle");
            }
            let (status, marker): (String, i64) = conn
                .query_row(
                    "SELECT status, cleanup_pending FROM bridge_turns",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("row");
            assert_eq!(status, "failed", "the terminal status is not rewritten");
            assert_eq!(marker, 0, "but the proven-empty group releases the marker");

            crate::state::prune_state_logs(&conn, far_future).expect("prune again");
            let rows: i64 = conn
                .query_row("SELECT COUNT(*) FROM bridge_turns", [], |row| row.get(0))
                .expect("count");
            assert_eq!(rows, 0, "and an unmarked settled row is ordinary history");
        }
    }

    /// A turn's ORDINARY end closes what it owns, atomically — completion
    /// and failure both. These rows never pass the stopping-recovery
    /// sweeper, so without this their approvals and queued buttons stayed
    /// answerable after the terminal status.
    #[test]
    fn ordinary_terminals_close_owned_dialogs() {
        let _guard = crate::state::test_env_lock();
        for answered in [true, false] {
            let conn = create_state_db_in_memory().expect("db");
            let log = write_turn_log(
                &format!("dlg{}-{}", answered as u8, std::process::id()),
                if answered {
                    &[r#"{"type":"result","subtype":"success","is_error":false,"result":"答"}"#]
                } else {
                    &["no result"]
                },
            );
            crate::state::register_bridge_turn(
                &conn,
                "turn-1",
                "sess-dlg",
                &log.display().to_string(),
                Some(4_300_000),
                None,
                None,
                None,
                None,
                None,
                1000,
            )
            .expect("register");
            if !answered {
                crate::state::record_bridge_turn_exit(&conn, "turn-1", Some(1)).expect("exit");
            }
            crate::state::create_pending_approval(
                &conn,
                "ap-1",
                "sess-dlg",
                "Bash",
                "x",
                true,
                1500,
                9_000_000_000,
            )
            .expect("approval");
            crate::state::record_approval_turn_owner(&conn, "ap-1", "turn-1").expect("owner");
            enqueue_outbound_event(
                &conn,
                &json!({
                    "type": "approval_request", "threadId": "sess-dlg",
                    "eventKey": "approval:ap-1", "observedAt": 1500
                }),
                1500,
                "bridge",
            )
            .expect("button");

            process_bridge_turns(&conn, &bridge_test_config(), 20_000).expect("cycle");

            let decision: Option<String> = conn
                .query_row(
                    "SELECT decision FROM pending_approvals WHERE approval_id = 'ap-1'",
                    [],
                    |row| row.get(0),
                )
                .expect("row");
            assert_eq!(
                decision.as_deref(),
                Some("expired"),
                "answered={answered}: the dialog closes with the terminal status"
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
                "answered={answered}: and its queued button is withdrawn"
            );
        }
    }

    /// Orphan reclaim: an object no row references is removed once past
    /// the grace; a referenced one is untouched; a FRESH unreferenced one
    /// survives — that window is a CLI between `create` and its
    /// registration commit, or a parallel test with rows in its own
    /// database.
    #[test]
    fn orphan_turn_objects_are_reclaimed_after_the_grace() {
        let _guard = crate::state::test_env_lock();
        let root = std::env::temp_dir().join(format!("tinyctb-gcroot-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let _root_guard = crate::state::EnvVarGuard::set("TINYCTB_CGROUP_ROOT", &root);
        let conn = create_state_db_in_memory().expect("db");

        let orphan = root.join("turn-orphan");
        fs::create_dir(&orphan).expect("orphan");
        let owned = root.join("turn-owned");
        fs::create_dir(&owned).expect("owned");
        crate::state::register_bridge_turn(
            &conn,
            "owned",
            "sess-gc",
            "/tmp/t.log",
            Some(4_200_000),
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        crate::state::record_turn_cgroup(&conn, "owned", owned.to_str().expect("utf8"))
            .expect("bind");

        gc_orphan_turn_objects(&conn, Duration::ZERO).expect("gc");
        assert!(!orphan.exists(), "an unreferenced object is reclaimed");
        assert!(owned.exists(), "a referenced object is untouched");

        fs::create_dir(&orphan).expect("fresh orphan");
        gc_orphan_turn_objects(&conn, Duration::from_secs(600)).expect("gc");
        assert!(
            orphan.exists(),
            "a fresh object survives the grace — the create→register window"
        );
        let _ = fs::remove_dir_all(&root);
    }

    /// A member process entering a turn's object exactly as spawned turns
    /// do: fork-time raw write into cgroup.procs.
    #[cfg(target_os = "linux")]
    fn spawn_member_into(dir: &std::path::Path) -> std::process::Child {
        let procs = fs::OpenOptions::new()
            .write(true)
            .open(dir.join("cgroup.procs"))
            .expect("open cgroup.procs");
        use std::os::unix::io::AsRawFd;
        use std::os::unix::process::CommandExt;
        let fd = procs.as_raw_fd();
        let mut command = std::process::Command::new("sleep");
        command
            .arg("300")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        // SAFETY: raw write on an inherited fd, nothing else.
        unsafe {
            command.pre_exec(move || {
                let buf = b"0\n";
                if libc::write(fd, buf.as_ptr().cast(), buf.len()) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().expect("spawn member");
        drop(procs);
        child
    }

    /// A turn's ORDINARY end sweeps its object too. An answered turn whose
    /// background survivor keeps the cgroup populated must not slip out of
    /// every scan as `done` — the settlement kills the tree, proves it
    /// empty, and removes the object. Likewise the no-answer failure path.
    #[cfg(target_os = "linux")]
    #[test]
    fn ordinary_completion_and_failure_sweep_the_object() {
        let _guard = crate::state::test_env_lock();
        for answered in [true, false] {
            let conn = create_state_db_in_memory().expect("db");
            let _ = crate::claude::test_kill::take();
            let turn_id = format!("cgend{}-{}", answered as u8, std::process::id());
            let Some(dir) = crate::claude::turn_cgroup::create(&turn_id) else {
                eprintln!("host cannot provide a cgroup subtree; skipping");
                return;
            };
            let log = write_turn_log(
                &format!("{turn_id}-log"),
                if answered {
                    &[r#"{"type":"result","subtype":"success","is_error":false,"result":"答案"}"#]
                } else {
                    &["no result at all"]
                },
            );
            crate::state::register_bridge_turn(
                &conn,
                &turn_id,
                "sess-cgend",
                &log.display().to_string(),
                Some(4_100_100),
                None,
                None,
                None,
                None,
                None,
                1000,
            )
            .expect("register");
            crate::state::record_turn_cgroup(&conn, &turn_id, &dir.display().to_string())
                .expect("bind");
            if !answered {
                // The leader is reaped without an answer: the failure path.
                crate::state::record_bridge_turn_exit(&conn, &turn_id, Some(0)).expect("exit");
            }

            // The background survivor, plus a stand-in for init that reaps
            // it once the sweep's kill lands.
            let member = spawn_member_into(&dir);
            let (reaped_tx, reaped_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let mut member = member;
                let _ = member.wait();
                let _ = reaped_tx.send(());
            });

            process_bridge_turns(&conn, &bridge_test_config(), 20_000).expect("cycle");

            assert!(
                reaped_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
                "answered={answered}: the survivor must be killed by the settlement sweep"
            );
            let (status, marker): (String, i64) = conn
                .query_row(
                    "SELECT status, cleanup_pending FROM bridge_turns",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("row");
            assert_eq!(
                status,
                if answered { "done" } else { "failed" },
                "answered={answered}"
            );
            assert_eq!(
                marker, 0,
                "answered={answered}: emptiness proven, supervision released"
            );
            assert!(
                !dir.exists(),
                "answered={answered}: the object goes with the turn"
            );
        }
    }

    /// The ownership object drives recovery END TO END on a REAL kernel
    /// cgroup: a populated object reads Alive (and the turn is
    /// re-signalled), an emptied one settles the turn, and the settle
    /// removes the object. No pids are consulted anywhere.
    #[cfg(target_os = "linux")]
    #[test]
    fn cgroup_ownership_drives_recovery_end_to_end() {
        let _guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        let turn_id = format!("cgtest-{}", std::process::id());
        let Some(dir) = crate::claude::turn_cgroup::create(&turn_id) else {
            eprintln!("host cannot provide a cgroup subtree; skipping");
            return;
        };
        crate::state::register_bridge_turn(
            &conn,
            &turn_id,
            "sess-cg",
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
        crate::state::record_turn_cgroup(&conn, &turn_id, &dir.display().to_string())
            .expect("bind");
        crate::state::mark_bridge_turn_stopping(&conn, &turn_id, 1500).expect("stop");

        // A REAL member enters the object the same way spawned turns do:
        // by writing itself into cgroup.procs between fork and exec.
        let procs = std::fs::OpenOptions::new()
            .write(true)
            .open(dir.join("cgroup.procs"))
            .expect("open cgroup.procs");
        let mut member = {
            use std::os::unix::io::AsRawFd;
            use std::os::unix::process::CommandExt;
            let fd = procs.as_raw_fd();
            let mut command = std::process::Command::new("sleep");
            command
                .arg("300")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            // SAFETY: raw write on an inherited fd, nothing else.
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

        let turn = crate::state::list_running_bridge_turns(&conn)
            .expect("turns")
            .into_iter()
            .find(|turn| turn.turn_id == turn_id)
            .expect("turn");
        assert_eq!(
            turn.cgroup_path.as_deref(),
            Some(dir.to_str().expect("utf8")),
            "the row must carry its ownership object"
        );

        let settled = advance_stopping_turn(&conn, &turn, 5_000).expect("advance");
        assert!(!settled, "a populated object must not settle");
        assert_eq!(
            crate::claude::test_kill::take().len(),
            1,
            "and the live group is re-signalled"
        );

        let _ = member.kill();
        let _ = member.wait();

        let settled = advance_stopping_turn(&conn, &turn, 9_000).expect("advance");
        assert!(settled, "an emptied object settles the turn");
        assert_eq!(
            crate::state::bridge_turn_status(&conn, &turn_id).expect("status"),
            Some("stopped".to_string())
        );
        assert!(!dir.exists(), "and the settle removes the ownership object");
    }

    /// A supervised (cleanup-marked) turn is held open through Alive and
    /// Unknown verdicts — status untouched, marker kept — and released
    /// exactly when the group is proven empty: settled, marker lifted.
    #[test]
    fn supervision_ends_only_when_the_group_is_proven_empty() {
        use crate::claude::Liveness;
        let _guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        crate::state::register_bridge_turn(
            &conn,
            "turn-1",
            "sess-1",
            "/tmp/t.log",
            Some(4_000_095),
            None,
            None,
            Some(4_000_095),
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
        let turn = crate::state::list_running_bridge_turns(&conn)
            .expect("turns")
            .into_iter()
            .next()
            .expect("turn");
        let state = |conn: &Connection| -> (String, i64) {
            conn.query_row(
                "SELECT status, cleanup_pending FROM bridge_turns",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("row")
        };

        {
            let _probe = crate::claude::test_group_probe::ProbeGuard::set(Liveness::Alive);
            advance_stopping_turn(&conn, &turn, 5_000).expect("advance");
        }
        assert_eq!(
            state(&conn),
            ("running".to_string(), 1),
            "a live group keeps the row open and supervised"
        );

        {
            let _probe = crate::claude::test_group_probe::ProbeGuard::set(Liveness::Ended);
            advance_stopping_turn(&conn, &turn, 9_000).expect("advance");
        }
        assert_eq!(
            state(&conn),
            ("stopped".to_string(), 0),
            "a proven-empty group settles the row and lifts the marker"
        );
    }

    /// Re-kills of a stopping turn back off exponentially: each attempt can
    /// hold the daemon loop for seconds of confirmation, so a group that
    /// shrugs KILL off must not be re-signalled every cycle.
    #[test]
    fn alive_rekills_back_off_between_attempts() {
        use crate::claude::Liveness;
        let _guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        crate::state::register_bridge_turn(
            &conn,
            "turn-1",
            "sess-1",
            "/tmp/t.log",
            Some(4_000_085),
            None,
            None,
            Some(4_000_085),
            None,
            None,
            1000,
        )
        .expect("register");
        crate::state::mark_bridge_turn_stopping(&conn, "turn-1", 1500).expect("intent");
        let _probe = crate::claude::test_group_probe::ProbeGuard::set(Liveness::Alive);
        let turn = crate::state::list_running_bridge_turns(&conn)
            .expect("turns")
            .into_iter()
            .next()
            .expect("turn");

        advance_stopping_turn(&conn, &turn, 5_000).expect("advance");
        assert_eq!(
            crate::claude::test_kill::take(),
            vec![4_000_085],
            "the first pass signals"
        );
        advance_stopping_turn(&conn, &turn, 5_500).expect("advance");
        assert!(
            crate::claude::test_kill::take().is_empty(),
            "a re-kill inside the backoff window must be skipped"
        );
        advance_stopping_turn(&conn, &turn, 8_000).expect("advance");
        assert_eq!(
            crate::claude::test_kill::take(),
            vec![4_000_085],
            "past the window it signals again"
        );
        // Wall-clock regression fails OPEN: a stamp from the future must
        // not freeze retries until the clock catches back up to it.
        conn.execute(
            "UPDATE bridge_turns SET last_stop_attempt_at = 1000000 WHERE turn_id = 'turn-1'",
            [],
        )
        .expect("future stamp");
        advance_stopping_turn(&conn, &turn, 9_000).expect("advance");
        assert_eq!(
            crate::claude::test_kill::take(),
            vec![4_000_085],
            "a future stamp must not freeze the re-kill"
        );
    }

    #[test]
    fn a_silent_candidate_does_not_block_a_working_one() {
        let dir = temp_dir("parallel");
        let publisher = Arc::new(ActivityPublisher::default());
        let open: Arc<XStreamOpener> = Arc::new(|candidate: &XCandidate| {
            if candidate.0 != ":silent" {
                return Some(text_session("EVENT type 13 (RawKeyPress)\n"));
            }
            // A REAL child that connects, never stops, and only ever emits
            // the motion we ignore — the exact shape that deadlocked the
            // serial version. Being a real child also makes it cancellable,
            // which is how production interrupts a blocked read.
            // A single process (no grandchildren to keep the pipe open when
            // the parent is killed) writing motion lines without pause.
            let mut child = std::process::Command::new("yes")
                .arg("EVENT type 17 (RawMotion)")
                .stdout(std::process::Stdio::piped())
                .spawn()
                .ok()?;
            let stdout = child.stdout.take()?;
            Some(XSession {
                reader: Box::new(std::io::BufReader::new(stdout)),
                child: Some(child),
            })
        });
        let mut workers = std::collections::HashMap::new();
        reconcile_workers(
            &mut workers,
            vec![
                (":silent".to_string(), None),
                (":works".to_string(), Some("/tmp/xauth".to_string())),
            ],
            |candidate| {
                spawn_worker(
                    dir.clone(),
                    candidate.clone(),
                    Arc::clone(&open),
                    Arc::clone(&publisher),
                    Duration::from_millis(0),
                    Arc::new(|| 4_242_000),
                    Duration::from_millis(50),
                )
            },
        );

        let (tx, rx) = std::sync::mpsc::channel();
        let watched = dir.join(INPUT_ACTIVITY_FILE);
        std::thread::spawn(move || {
            while !watched.exists() {
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = tx.send(());
        });
        let landed = rx.recv_timeout(Duration::from_secs(5)).is_ok();
        for (_, worker) in workers.drain() {
            worker.cancel();
        }
        assert!(
            landed,
            "the working candidate must land its keypress while the silent one keeps talking"
        );
        let value: Value =
            serde_json::from_str(&fs::read_to_string(dir.join(INPUT_ACTIVITY_FILE)).expect("read"))
                .expect("json");
        assert_eq!(value["lastInputAtMs"], 4_242_000);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Every child a watcher opens must be reaped. A forgotten one becomes a
    /// zombie, and a watcher reconnecting every few seconds mints them
    /// steadily until the PID or task quota runs out.
    /// Linux-only: the assertion reads process state from `/proc`, and on a
    /// platform without it an unreadable file would silently pass.
    #[cfg(target_os = "linux")]
    #[test]
    fn every_child_a_watcher_opens_is_reaped() {
        let dir = temp_dir("reap");
        let publisher = Arc::new(ActivityPublisher::default());
        let pids = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&pids);
        let open: Arc<XStreamOpener> = Arc::new(move |_: &XCandidate| {
            // A real child that exits immediately: EOF arrives at once, so
            // the watcher reconnects and mints another.
            let mut child = std::process::Command::new("true")
                .stdout(std::process::Stdio::piped())
                .spawn()
                .ok()?;
            let stdout = child.stdout.take()?;
            seen.lock().expect("lock").push(child.id());
            Some(XSession {
                reader: Box::new(std::io::BufReader::new(stdout)),
                child: Some(child),
            })
        });
        let worker = spawn_worker(
            dir.clone(),
            (":reap".to_string(), None),
            open,
            publisher,
            Duration::from_millis(0),
            Arc::new(|| 1),
            Duration::from_millis(20),
        );
        std::thread::sleep(Duration::from_millis(300));
        worker.cancel();

        let pids = pids.lock().expect("lock").clone();
        assert!(
            pids.len() >= 2,
            "the watcher must have reconnected: {pids:?}"
        );
        for pid in pids {
            // A reaped pid may be gone entirely (NotFound is fine); any
            // OTHER read error means the check did not actually run and
            // must not be mistaken for a pass.
            match fs::read_to_string(format!("/proc/{pid}/stat")) {
                Ok(state) => assert!(
                    !state.contains(") Z"),
                    "pid {pid} was left as a zombie: {state}"
                ),
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => panic!("could not inspect pid {pid}: {error}"),
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// Every watcher shares one destination and one PID, so publication has
    /// to be serialised: two of them writing at once must never let a reader
    /// see a partial file, and the throttle is GLOBAL, not per thread.
    #[test]
    fn concurrent_watchers_publish_atomically_and_throttle_globally() {
        let dir = temp_dir("publish");
        let publisher = Arc::new(ActivityPublisher::default());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let reader_stop = Arc::new(AtomicBool::new(false));

        // A reader hammering the file for the whole run: every read that
        // sees a file must see a COMPLETE JSON object.
        let watched = dir.join(INPUT_ACTIVITY_FILE);
        let stop_reading = Arc::clone(&reader_stop);
        let reader = std::thread::spawn(move || {
            let mut partial = 0usize;
            let mut complete = 0usize;
            while !stop_reading.load(Ordering::Relaxed) {
                if let Ok(text) = fs::read_to_string(&watched) {
                    if serde_json::from_str::<Value>(&text)
                        .ok()
                        .and_then(|value| value.get("lastInputAtMs").and_then(Value::as_u64))
                        .is_none()
                    {
                        partial += 1;
                    } else {
                        complete += 1;
                    }
                }
            }
            (partial, complete)
        });

        let writers = (0..2)
            .map(|_| {
                let dir = dir.clone();
                let publisher = Arc::clone(&publisher);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let mut wrote = 0usize;
                    for _ in 0..200 {
                        if publisher.publish(&dir, 9_000, Duration::from_secs(30)) {
                            wrote += 1;
                        }
                    }
                    wrote
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let total: usize = writers.into_iter().map(|w| w.join().expect("writer")).sum();
        // Do not stop the reader until it has demonstrably read the file at
        // least once, or `partial == 0` below could be vacuously true.
        let watched_again = dir.join(INPUT_ACTIVITY_FILE);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !watched_again.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(50));
        reader_stop.store(true, Ordering::Relaxed);
        let (partial, complete) = reader.join().expect("reader");

        assert_eq!(
            total, 1,
            "the gap is global: 400 attempts across two threads must write once"
        );
        assert_eq!(
            partial, 0,
            "a reader must never observe a half-written file"
        );
        // Without this the assertion above is vacuous: a reader that never
        // managed to open the file would also report zero partials.
        assert!(
            complete > 0,
            "the reader must actually have read the published file at least once"
        );
        // And no staging leftovers.
        let staged = fs::read_dir(&dir)
            .expect("dir")
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(staged, 0, "staging files must be renamed away, not left");
        let _ = fs::remove_dir_all(&dir);
    }

    /// Sessions come and go. The supervisor must add watchers for new
    /// candidates AND cancel the ones whose session vanished — otherwise a
    /// rotated display leaves a worker retrying forever and thread and child
    /// counts grow without bound.
    #[test]
    fn workers_follow_candidates_appearing_and_disappearing() {
        let dir = temp_dir("lifecycle");
        let publisher = Arc::new(ActivityPublisher::default());
        let open: Arc<XStreamOpener> =
            Arc::new(|_: &XCandidate| Some(text_session("EVENT type 17 (RawMotion)\n")));
        let start = |candidate: &XCandidate| {
            spawn_worker(
                dir.clone(),
                candidate.clone(),
                Arc::clone(&open),
                Arc::clone(&publisher),
                Duration::from_millis(0),
                Arc::new(|| 1),
                Duration::from_millis(20),
            )
        };
        let a = (":a".to_string(), None);
        let b = (":b".to_string(), None);
        let mut workers = std::collections::HashMap::new();

        reconcile_workers(&mut workers, vec![a.clone()], start);
        assert_eq!(workers.keys().collect::<Vec<_>>(), vec![&a]);

        reconcile_workers(&mut workers, vec![a.clone(), b.clone()], start);
        assert_eq!(workers.len(), 2, "a new session gets its own watcher");

        // A disappears: its worker must be cancelled and joined, not leaked.
        reconcile_workers(&mut workers, vec![b.clone()], start);
        assert_eq!(
            workers.keys().collect::<Vec<_>>(),
            vec![&b],
            "the vanished candidate's worker must be dropped"
        );

        for (_, worker) in workers.drain() {
            worker.cancel();
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// The retry wait must be interruptible AT THE PRODUCTION DELAY. With a
    /// 20ms retry an uninterruptible `sleep(retry)` would pass too, so this
    /// uses the real 5s: workers are pushed into their retry gap, then two
    /// of them are removed at once and both must join promptly while the new
    /// candidate starts immediately rather than queueing behind `N × 5s`.
    #[test]
    fn removing_workers_does_not_wait_out_the_production_retry() {
        let dir = temp_dir("retry-cancel");
        let publisher = Arc::new(ActivityPublisher::default());
        let started = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&started);
        // An immediately-empty stream: the worker drains it at once and goes
        // straight into the retry gap, which is where we want it.
        let open: Arc<XStreamOpener> = Arc::new(move |candidate: &XCandidate| {
            seen.lock().expect("lock").push(candidate.0.clone());
            Some(text_session(""))
        });
        let start = |candidate: &XCandidate| {
            spawn_worker(
                dir.clone(),
                candidate.clone(),
                Arc::clone(&open),
                Arc::clone(&publisher),
                Duration::from_millis(0),
                Arc::new(|| 1),
                INPUT_LISTENER_RETRY, // the production 5s
            )
        };
        let a = (":a".to_string(), None);
        let b = (":b".to_string(), None);
        let c = (":c".to_string(), None);
        let mut workers = std::collections::HashMap::new();
        reconcile_workers(&mut workers, vec![a.clone(), b.clone()], start);

        // Wait until both have consumed their stream and are sitting in the
        // 5s gap.
        let deadline = Instant::now() + Duration::from_secs(3);
        while started.lock().expect("lock").len() < 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            started.lock().expect("lock").len(),
            2,
            "both workers must have entered their retry gap"
        );

        // Remove BOTH and add a third: an uninterruptible sleep would make
        // this take 2 × 5s before :c even starts.
        let swap_started = Instant::now();
        reconcile_workers(&mut workers, vec![c.clone()], start);
        let elapsed = swap_started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "cancelling two workers mid-retry must not wait out {:?} each: took {elapsed:?}",
            INPUT_LISTENER_RETRY
        );
        assert_eq!(workers.keys().collect::<Vec<_>>(), vec![&c]);
        // The replacement's thread is already spawned; give it a moment to
        // reach its opener. The point of the assertion is that it does not
        // have to wait out the removed workers' retry gaps first.
        let deadline = Instant::now() + Duration::from_secs(2);
        while !started.lock().expect("lock").contains(&":c".to_string())
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            started.lock().expect("lock").contains(&":c".to_string()),
            "the replacement must start without waiting out the removed workers' retries"
        );
        for (_, worker) in workers.drain() {
            worker.cancel();
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// A write that fails AFTER creating the staging file must not leave it
    /// behind — the name carries an ever-increasing sequence, so每 failure
    /// would otherwise add another partial file forever. The throttle must
    /// also stay unarmed, so the next real keypress still publishes.
    #[test]
    fn a_failed_publish_cleans_up_and_does_not_arm_the_throttle() {
        let dir = temp_dir("publish-fail");
        let publisher = ActivityPublisher::default();
        publisher.fail_after_create.store(true, Ordering::Relaxed);

        assert!(
            !publisher.publish(&dir, 111, Duration::from_secs(30)),
            "an injected failure must report failure"
        );
        let leftovers = fs::read_dir(&dir)
            .expect("dir")
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "a failed write must leave nothing behind: {leftovers:?}"
        );

        // The gap is 30s, but the failed attempt must not have started it.
        publisher.fail_after_create.store(false, Ordering::Relaxed);
        assert!(
            publisher.publish(&dir, 222, Duration::from_secs(30)),
            "the next attempt must publish, not be throttled by a failure"
        );
        let value: Value =
            serde_json::from_str(&fs::read_to_string(dir.join(INPUT_ACTIVITY_FILE)).expect("read"))
                .expect("json");
        assert_eq!(value["lastInputAtMs"], 222);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The deadlock Sol hit on a real run: `cancel` looks for a child, the
    /// opener has not returned one yet, so it finds nothing to kill and goes
    /// straight to `join` — while the worker then registers a live silent
    /// child and blocks on it forever, with no handle left to interrupt it.
    ///
    /// TWO handshakes, not one barrier. A single barrier deadlocked the test
    /// itself: if cancel won the start, the worker saw `stopping` at the top
    /// of its loop and exited WITHOUT ever entering the opener, leaving the
    /// main thread waiting on a barrier participant that would never arrive
    /// (Sol reproduced it on the 21st consecutive run). Waiting for the
    /// opener to be entered FIRST makes the interleaving the test claims to
    /// exercise the only one it can produce.
    #[test]
    fn a_cancel_racing_child_registration_still_finishes() {
        let dir = temp_dir("cancel-race");
        let publisher = Arc::new(ActivityPublisher::default());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = std::sync::Mutex::new(release_rx);
        let open: Arc<XStreamOpener> = Arc::new(move |_: &XCandidate| {
            // 1. Tell the test we are inside the opener...
            let _ = entered_tx.send(());
            // 2. ...and hold the child back until it says cancel has landed.
            let _ = release_rx.lock().expect("release lock").recv();
            let mut child = std::process::Command::new("sleep")
                .arg("300")
                .stdout(std::process::Stdio::piped())
                .spawn()
                .ok()?;
            let stdout = child.stdout.take()?;
            Some(XSession {
                reader: Box::new(std::io::BufReader::new(stdout)),
                child: Some(child),
            })
        });
        let worker = spawn_worker(
            dir.clone(),
            (":racy".to_string(), None),
            open,
            publisher,
            Duration::from_millis(0),
            Arc::new(|| 1),
            Duration::from_millis(20),
        );

        // The worker must be INSIDE the opener before cancel starts, or it
        // would exit at the top of its loop and never reach the race.
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker must reach the opener");

        let observed = Arc::clone(&worker.state);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let canceller = std::thread::spawn(move || {
            worker.cancel();
            let _ = done_tx.send(());
        });

        // Wait for cancel's linearization point: stopping set, child slot
        // taken (still empty). This is exactly the window the old code lost.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            {
                let state = lock_state(&observed);
                if state.stopping && state.child.is_none() {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "cancel never reached its linearization point"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        // Now let the opener hand a live child into an already-stopping
        // worker: it must reap it itself rather than block reading it.
        release_tx.send(()).expect("release opener");
        assert!(
            done_rx.recv_timeout(Duration::from_secs(10)).is_ok(),
            "cancel must finish even when a child is registered underneath it"
        );
        canceller.join().expect("canceller");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A worker blocked on a silent stream must still stop promptly when
    /// cancelled — killing its child is what interrupts the read.
    #[test]
    fn a_silent_worker_can_still_be_cancelled_promptly() {
        let dir = temp_dir("cancel");
        let publisher = Arc::new(ActivityPublisher::default());
        let open: Arc<XStreamOpener> = Arc::new(|_: &XCandidate| {
            // `sleep` holds the pipe open and says nothing: the read blocks
            // until the child is killed.
            let mut child = std::process::Command::new("sleep")
                .arg("300")
                .stdout(std::process::Stdio::piped())
                .spawn()
                .ok()?;
            let stdout = child.stdout.take()?;
            Some(XSession {
                reader: Box::new(std::io::BufReader::new(stdout)),
                child: Some(child),
            })
        });
        let worker = spawn_worker(
            dir.clone(),
            (":quiet".to_string(), None),
            open,
            publisher,
            Duration::from_millis(0),
            Arc::new(|| 1),
            Duration::from_millis(20),
        );
        std::thread::sleep(Duration::from_millis(100));

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            worker.cancel();
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "cancelling must interrupt a blocked read, not wait for a stream that never ends"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// `XAUTHORITY` is optional: a session using the default
    /// `$HOME/.Xauthority` is normal, and demanding the variable skipped
    /// those sessions entirely. `DISPLAY` alone must still be a candidate,
    /// and ours must come first so a desktop-launched daemon uses its own.
    #[test]
    fn x_candidates_accept_display_without_xauthority() {
        let _guard = crate::state::test_env_lock();
        // RAII: asserts fire BETWEEN the mutations below, so a failure must
        // still hand the session's real DISPLAY/XAUTHORITY back.
        let _display = crate::state::EnvVarGuard::set("DISPLAY", ":99");
        let _xauthority = crate::state::EnvVarGuard::clear("XAUTHORITY");
        let candidates = x_environment_candidates();
        assert_eq!(
            candidates.first(),
            Some(&(":99".to_string(), None)),
            "DISPLAY alone must be tried, ours first: {candidates:?}"
        );

        env::set_var("XAUTHORITY", "/tmp/does-not-matter");
        let candidates = x_environment_candidates();
        assert_eq!(
            candidates.first(),
            Some(&(":99".to_string(), Some("/tmp/does-not-matter".to_string())))
        );

        // An empty DISPLAY is not a candidate at all.
        env::set_var("DISPLAY", "");
        assert!(
            !x_environment_candidates()
                .iter()
                .any(|(display, _)| display.is_empty()),
            "an empty DISPLAY must never be offered"
        );
    }

    #[test]
    fn daemon_install_dry_run_resolves_relative_bridge_command_for_services() {
        if !cfg!(any(target_os = "linux", target_os = "macos")) {
            return;
        }
        let _guard = crate::state::test_env_lock();
        let _env = TempServiceEnv::new("install-dry-run");
        let result = install_daemon_service(DEFAULT_DAEMON_LABEL, "bin/tinyctb", true)
            .expect("daemon install dry run");
        let expected = std::env::current_dir()
            .expect("cwd")
            .join("bin/tinyctb")
            .display()
            .to_string();
        assert!(result["contents"]
            .as_str()
            .expect("service contents")
            .contains(&expected));
    }

    #[test]
    fn daemon_notification_policy_only_enqueues_events_while_away() {
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_1",
            "updatedAt": 1500
        });

        let off_count =
            enqueue_daemon_notification_events(&conn, std::slice::from_ref(&event), 2000)
                .expect("away off enqueue");
        assert_eq!(
            off_count, 0,
            "daemon should stay quiet while user is present"
        );

        set_away_mode(&conn, true, 1000).expect("away on");
        let on_count =
            enqueue_daemon_notification_events(&conn, &[event], 2000).expect("away on enqueue");
        assert_eq!(on_count, 1, "daemon should notify while user is away");
    }

    fn write_turn_log(name: &str, lines: &[&str]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("tinyctb-turnlog-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("turn log dir");
        let path = dir.join(format!("{name}.log"));
        fs::write(&path, lines.join("\n")).expect("write turn log");
        path
    }

    fn bridge_test_config() -> DaemonConfig {
        DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: "thread_waiting".to_string(), // narrow on purpose: results must ignore it
            telegram: None,
            claude: None,
            projects: vec![],
        }
    }

    /// Core scenario that Stop-hook attribution got wrong in production: the
    /// answer comes from the turn's own log, away off, and survives /back.
    #[test]
    fn bridge_turn_result_is_pushed_from_its_log_and_survives_back() {
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let log = write_turn_log(
            "sess-log-1000",
            &[
                "some stderr noise",
                r#"{"type":"result","subtype":"success","is_error":false,"result":"校验仍在跑，十来分钟后终报。"}"#,
            ],
        );
        crate::state::register_bridge_turn(
            &conn,
            "sess-log-1000",
            "sess-log",
            &log.display().to_string(),
            Some(0),
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");

        let summary = process_bridge_turns(&conn, &bridge_test_config(), 2000).expect("process");
        assert_eq!(summary["answered"], 1, "{summary}");
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);
        let (payload, origin): (String, String) = conn
            .query_row(
                "SELECT payload_json, origin FROM outbound_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("outbound row");
        assert_eq!(origin, "bridge");
        assert!(payload.contains("校验仍在跑"), "payload: {payload}");

        // Turn is done: a second poll neither re-pushes nor errors.
        let again = process_bridge_turns(&conn, &bridge_test_config(), 3000).expect("repoll");
        assert_eq!(again["answered"], 0);
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);

        // /back clears only the away backlog; the answer stays queued.
        set_away_mode(&conn, false, 4000).expect("back");
        assert_eq!(
            pending_outbound_count(&conn).expect("pending after back"),
            1
        );
    }

    /// Two concurrent replies to one session = two registered turns with two
    /// logs; both answers must be pushed, each with its own text.
    #[test]
    fn concurrent_bridge_turns_each_push_their_own_answer() {
        let conn = create_state_db_in_memory().expect("db");
        for (turn_id, text) in [
            (
                "sess-c-1000",
                r#"{"type":"result","subtype":"success","result":"answer one"}"#,
            ),
            (
                "sess-c-1100",
                r#"{"type":"result","subtype":"success","result":"answer two"}"#,
            ),
        ] {
            let log = write_turn_log(turn_id, &[text]);
            crate::state::register_bridge_turn(
                &conn,
                turn_id,
                "sess-c",
                &log.display().to_string(),
                Some(0),
                None,
                None,
                None,
                None,
                None,
                1000,
            )
            .expect("register");
        }

        let summary = process_bridge_turns(&conn, &bridge_test_config(), 2000).expect("process");
        assert_eq!(summary["answered"], 2, "{summary}");
        let payloads: Vec<String> = conn
            .prepare("SELECT payload_json FROM outbound_events ORDER BY event_id")
            .expect("stmt")
            .query_map([], |row| row.get(0))
            .expect("rows")
            .collect::<rusqlite::Result<_>>()
            .expect("payloads");
        assert_eq!(payloads.len(), 2);
        assert!(payloads
            .iter()
            .any(|payload| payload.contains("answer one")));
        assert!(payloads
            .iter()
            .any(|payload| payload.contains("answer two")));
    }

    /// The 60s idle reminder repeats the completion push word for word —
    /// the user reads every answer twice. A reply-kind wait matching the
    /// last DELIVERED preview is suppressed; a new text, an approval wait,
    /// or a first-ever push all still go out.
    #[test]
    fn idle_reminder_repeating_the_delivered_answer_is_suppressed() {
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let completed = json!({
            "type": "thread_completed",
            "threadId": "sess-echo",
            "updatedAt": 1000,
            "eventKey": "stop:1",
            "lastPreview": "最终答案在此",
            "thread": {"threadId": "sess-echo", "lastPreview": "最终答案在此"}
        });
        // Delivered completion push on record.
        enqueue_outbound_event(&conn, &completed, 1000, "away").expect("enqueue");
        conn.execute(
            "UPDATE outbound_events SET delivered_at = 1500, status = 'delivered'",
            [],
        )
        .expect("mark delivered");

        let waiting = |preview: &str, kind: &str, notification_type: &str, key: &str| {
            json!({
                "type": "thread_waiting",
                "threadId": "sess-echo",
                "updatedAt": 2000,
                "eventKey": key,
                "lastPreview": preview,
                "thread": {
                    "threadId": "sess-echo",
                    "lastPreview": preview,
                    "pendingPrompt": {"promptId": key, "kind": kind, "promptKind": kind,
                                       "notificationType": notification_type,
                                       "status": "pending", "question": "Claude is waiting"}
                }
            })
        };
        crate::claude::set_away_mode(&conn, true, 1900).expect("away on");

        // Same text, literal idle_prompt: suppressed.
        let n = enqueue_daemon_notification_events(
            &conn,
            &[waiting("最终答案在此", "reply", "idle_prompt", "notify:2")],
            2000,
        )
        .expect("enqueue");
        assert_eq!(n, 0, "the echo reminder must be dropped");

        // Different text: a genuinely new wait goes out.
        let n = enqueue_daemon_notification_events(
            &conn,
            &[waiting(
                "请选择方案 A 或 B",
                "reply",
                "idle_prompt",
                "notify:3",
            )],
            2100,
        )
        .expect("enqueue");
        assert_eq!(n, 1, "new content must still notify");

        // Same text but an APPROVAL wait: never suppressed.
        let n = enqueue_daemon_notification_events(
            &conn,
            &[waiting(
                "最终答案在此",
                "approval",
                "permission_prompt",
                "notify:4",
            )],
            2200,
        )
        .expect("enqueue");
        assert_eq!(n, 1, "approval waits are never dropped");

        // Same text, reply kind, but a GENUINE question that happened to
        // fire right after the completion (agent_needs_input, MCP
        // elicitation): the folded kind says "reply", the raw
        // notification_type says it is not an idle reminder — never drop.
        for (notification_type, key) in [
            ("agent_needs_input", "notify:5"),
            ("elicitation_dialog", "notify:6"),
        ] {
            let n = enqueue_daemon_notification_events(
                &conn,
                &[waiting("最终答案在此", "reply", notification_type, key)],
                2300,
            )
            .expect("enqueue");
            assert_eq!(n, 1, "{notification_type} must never be suppressed");
        }

        // No notificationType at all (rows predating the column, non-hook
        // prompts): unknown provenance fails open.
        let mut legacy = waiting("最终答案在此", "reply", "ignored", "notify:7");
        legacy
            .pointer_mut("/thread/pendingPrompt")
            .and_then(Value::as_object_mut)
            .expect("prompt object")
            .remove("notificationType");
        let n = enqueue_daemon_notification_events(&conn, &[legacy], 2400).expect("enqueue");
        assert_eq!(n, 1, "unknown notification provenance must fail open");
    }

    /// The suppression is scoped to the completion→reminder echo, nothing
    /// wider. Two counterexamples that a bare "same preview as last
    /// delivered" check would get wrong:
    /// - `events="thread_waiting"` configs never deliver completions, so the
    ///   last delivered push is a WAIT — the next real wait with identical
    ///   text is new information, not an echo;
    /// - a completion delivered long ago cannot vouch for today's wait even
    ///   if the words match.
    #[test]
    fn idle_reminder_suppression_requires_a_recent_completion() {
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        crate::claude::set_away_mode(&conn, true, 500).expect("away on");
        let event = |etype: &str, thread: &str, key: &str, at: u64, kind: &str| {
            json!({
                "type": etype,
                "threadId": thread,
                "updatedAt": at,
                "eventKey": key,
                "lastPreview": "同样的结束语",
                "thread": {
                    "threadId": thread,
                    "lastPreview": "同样的结束语",
                    "pendingPrompt": {"promptId": key, "kind": kind, "promptKind": kind,
                                       "notificationType": "idle_prompt",
                                       "status": "pending", "question": "Claude is waiting"}
                }
            })
        };

        // Prior delivered push is a WAIT (completion was filtered by config):
        // the next identical wait must still go out.
        enqueue_outbound_event(
            &conn,
            &event("thread_waiting", "sess-wait-only", "wait:1", 1000, "reply"),
            1000,
            "away",
        )
        .expect("enqueue prior wait");
        conn.execute(
            "UPDATE outbound_events SET delivered_at = 1500, status = 'delivered'
             WHERE thread_id = 'sess-wait-only'",
            [],
        )
        .expect("mark delivered");
        let n = enqueue_daemon_notification_events(
            &conn,
            &[event(
                "thread_waiting",
                "sess-wait-only",
                "wait:2",
                2000,
                "reply",
            )],
            2000,
        )
        .expect("enqueue");
        assert_eq!(
            n, 1,
            "a prior delivered wait must never suppress the next one"
        );

        // Completion delivered outside the echo window: identical wait is new.
        enqueue_outbound_event(
            &conn,
            &event("thread_completed", "sess-stale", "stop:1", 1000, "reply"),
            1000,
            "away",
        )
        .expect("enqueue stale completion");
        conn.execute(
            "UPDATE outbound_events SET delivered_at = 1500, status = 'delivered'
             WHERE thread_id = 'sess-stale'",
            [],
        )
        .expect("mark delivered");
        let long_after = 1500 + crate::state::IDLE_REMINDER_ECHO_WINDOW_MS + 60_000;
        let n = enqueue_daemon_notification_events(
            &conn,
            &[event(
                "thread_waiting",
                "sess-stale",
                "wait:3",
                long_after,
                "reply",
            )],
            long_after,
        )
        .expect("enqueue");
        assert_eq!(n, 1, "a stale completion cannot vouch for a new wait");
    }

    /// A crash between "Telegram accepted the message" and "the outbound row
    /// says delivered" must not rewrite delivery history. The recovery cycle
    /// re-runs the same production skip path, and the row must end up
    /// stamped with the ORIGINAL send time — otherwise a completion the user
    /// read before the crash outranks the wait they received afterwards, and
    /// idle-echo suppression silently eats a real question.
    #[test]
    fn crash_recovery_preserves_the_original_delivery_order() {
        let conn = create_state_db_in_memory().expect("db");
        let config = DaemonConfig {
            telegram: Some(crate::TelegramConfig {
                bot_token: "token".to_string(),
                chat_id: "chat".to_string(),
                allowed_user_id: None,
            }),
            ..bridge_test_config()
        };
        let completion = json!({
            "type": "thread_completed",
            "threadId": "sess-crash",
            "updatedAt": 1000,
            "eventKey": "stop:1",
            "lastPreview": "完成文本"
        });
        let wait = json!({
            "type": "thread_waiting",
            "threadId": "sess-crash",
            "updatedAt": 1100,
            "eventKey": "notify:1",
            "lastPreview": "完成文本",
            "thread": {
                "threadId": "sess-crash",
                "lastPreview": "完成文本",
                "pendingPrompt": {"promptId": "notify:1", "kind": "reply",
                                   "promptKind": "reply", "notificationType": "idle_prompt",
                                   "status": "pending", "question": "Claude is waiting"}
            }
        });
        enqueue_outbound_event(&conn, &completion, 1000, "away").expect("enqueue completion");
        enqueue_outbound_event(&conn, &wait, 1100, "away").expect("enqueue wait");

        // t=1200: the completion reaches Telegram and the transport log
        // records it — then the daemon dies before the outbound row is
        // marked delivered.
        deliver_event_through_transports(&conn, &config, &completion, 1200, |_| {
            Ok(json!({ "ok": true, "messageId": 1 }))
        })
        .expect("send completion");
        assert_eq!(
            pending_outbound_count(&conn).expect("pending"),
            2,
            "the crash left BOTH outbound rows unmarked"
        );

        // t=9000: the daemon restarts and drains the outbox in one batch.
        // The completion is skipped (already sent), the wait is sent now.
        let summary = deliver_due_outbound_events(&conn, 9000, 10, None, |event| {
            deliver_event_through_transports(&conn, &config, event, 9000, |_| {
                Ok(json!({ "ok": true, "messageId": 2 }))
            })
        })
        .expect("recovery batch");
        assert_eq!(summary.delivered, 2);

        // The completion keeps its pre-crash timestamp; the wait carries the
        // recovery cycle's. Delivery order therefore still reads
        // completion-then-wait, exactly what the user saw.
        let stamps: Vec<(String, i64)> = {
            let mut stmt = conn
                .prepare(
                    "SELECT event_type, delivered_at FROM outbound_events
                     ORDER BY delivered_at ASC",
                )
                .expect("stmt");
            let rows = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .expect("query")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("rows");
            rows
        };
        assert_eq!(
            stamps,
            vec![
                ("thread_completed".to_string(), 1200),
                ("thread_waiting".to_string(), 9000)
            ],
            "the crash-orphaned completion must keep its original send time"
        );

        // And the consequence that matters: the wait delivered after the
        // completion breaks the vouch, so an identical idle reminder
        // arriving later is NOT suppressed.
        assert_eq!(
            crate::state::last_delivered_completion_preview(&conn, "sess-crash", 9100)
                .expect("query"),
            None,
            "a completion the user saw BEFORE a later wait must not vouch"
        );
    }

    /// Crash consistency of the death verdict, both halves:
    /// - a broken outbox must roll the CLAIM back too — a settled turn whose
    ///   user was never told is unrecoverable (settled turns are invisible
    ///   to every later scan), while a still-running turn retries next cycle;
    /// - the kill must land BEFORE the terminal state commits, so a daemon
    ///   dying in between leaves a `running` turn whose next cycle repeats
    ///   the idempotent kill — never a committed `expired` hiding a live
    ///   process from reaping forever.
    #[test]
    fn broken_outbox_rolls_back_the_claim_and_the_kill_is_repeatable() {
        let conn = create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        crate::state::register_bridge_turn(
            &conn,
            "turn-tx",
            "sess-tx",
            "/tmp/tx.log",
            Some(4_000_000_000),
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        let snapshot = crate::state::BridgeTurn {
            turn_id: "turn-tx".to_string(),
            thread_id: "sess-tx".to_string(),
            log_path: "/tmp/tx.log".to_string(),
            pid: Some(4_000_000_000),
            started_at: 1000,
            exited: false,
            exit_code: None,
            pgid: None,
            proc_start_ticks: None,
            boot_id: None,
            cgroup_path: None,
        };
        // Sabotage the outbox: every insert aborts, as a full-disk or
        // corrupted database would.
        conn.execute_batch(
            "CREATE TRIGGER outbox_down BEFORE INSERT ON outbound_events
             BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END;",
        )
        .expect("trigger");

        // Timed out (dead by fiat) + broken outbox.
        let err = settle_dead_turn(&conn, &snapshot, true, 50_000);
        assert!(err.is_err(), "a lost notice must surface as an error");
        assert_eq!(
            crate::claude::test_kill::take(),
            vec![4_000_000_000],
            "the kill must have happened BEFORE anything committed"
        );
        let status: String = conn
            .query_row("SELECT status FROM bridge_turns", [], |row| row.get(0))
            .expect("row");
        assert_eq!(
            status, "running",
            "the claim must roll back with the notice, so the verdict retries"
        );
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            0
        );

        // Outbox heals: the next cycle's verdict must fully land — claim,
        // notice, and the (idempotent) re-kill.
        conn.execute_batch("DROP TRIGGER outbox_down;")
            .expect("drop");
        let claimed = settle_dead_turn(&conn, &snapshot, true, 60_000).expect("settle");
        assert!(claimed);
        assert_eq!(crate::claude::test_kill::take(), vec![4_000_000_000]);
        let status: String = conn
            .query_row("SELECT status FROM bridge_turns", [], |row| row.get(0))
            .expect("row");
        assert_eq!(status, "expired");
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            1,
            "the user is told exactly once"
        );
    }

    /// The reverse race of `identity_persist_refuses_a_turn_the_daemon_already_settled`:
    /// the daemon snapshots `pid NULL`, the identity write lands, and only
    /// THEN does the daemon apply its "no pid = dead" verdict. The claim must
    /// lose — no failed status, no thread_error — because the caller of that
    /// turn was already told "started" and its token must stay valid.
    #[test]
    fn identity_write_landing_after_the_snapshot_averts_the_failure_verdict() {
        let conn = create_state_db_in_memory().expect("db");
        crate::state::register_bridge_turn(
            &conn,
            "turn-race",
            "sess-race",
            "/tmp/race.log",
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        // The daemon's snapshot, taken while the pid was still unwritten:
        let snapshot = crate::state::BridgeTurn {
            turn_id: "turn-race".to_string(),
            thread_id: "sess-race".to_string(),
            log_path: "/tmp/race.log".to_string(),
            pid: None,
            started_at: 1000,
            exited: false,
            exit_code: None,
            pgid: None,
            proc_start_ticks: None,
            boot_id: None,
            cgroup_path: None,
        };
        // The identity write lands before the verdict is applied.
        assert_eq!(
            crate::state::record_bridge_turn_spawn(
                &conn,
                "turn-race",
                Some(std::process::id()),
                None,
                None,
                None,
                None,
                None,
            )
            .expect("identity write"),
            1
        );
        // Well past the failure grace: on the stale snapshot alone this turn
        // is dead. The claim must notice the pid and drop the verdict.
        let claimed = settle_dead_turn(&conn, &snapshot, false, 50_000).expect("settle");
        assert!(!claimed, "the verdict must lose to the identity write");
        let status: String = conn
            .query_row("SELECT status FROM bridge_turns", [], |row| row.get(0))
            .expect("row");
        assert_eq!(status, "running", "the turn must stay open");
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            0,
            "and no thread_error may be announced for a turn that is alive"
        );
    }

    /// A dead process without a result must produce a loud failure notice,
    /// not an eternal silent wait.
    #[test]
    fn dead_bridge_turn_without_result_notifies_failure() {
        let conn = create_state_db_in_memory().expect("db");
        let log = write_turn_log("sess-dead-1000", &["Error: model exploded"]);
        crate::state::register_bridge_turn(
            &conn,
            "sess-dead-1000",
            "sess-dead",
            &log.display().to_string(),
            Some(4_000_000_000), // no such pid
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");

        // Within the grace window: still counted as running, no notice yet.
        let early = process_bridge_turns(&conn, &bridge_test_config(), 5000).expect("early");
        assert_eq!(early["failed"], 0);
        assert_eq!(early["running"], 1);

        let late = process_bridge_turns(&conn, &bridge_test_config(), 20_000).expect("late");
        assert_eq!(late["failed"], 1, "{late}");
        let payload: String = conn
            .query_row("SELECT payload_json FROM outbound_events", [], |row| {
                row.get(0)
            })
            .expect("failure row");
        assert!(payload.contains("exited without producing an answer"));
        assert!(payload.contains("model exploded"), "payload: {payload}");
    }

    /// A reaped child exit is authoritative: even when the PID looks alive
    /// (reuse), the turn must fail loudly with the recorded exit status.
    #[test]
    fn reaped_exit_marks_turn_failed_even_if_pid_looks_alive() {
        let conn = create_state_db_in_memory().expect("db");
        let log = write_turn_log("sess-exit-1000", &["Error: model exploded"]);
        let own_pid = std::process::id(); // definitely passes `kill -0`
        crate::state::register_bridge_turn(
            &conn,
            "sess-exit-1000",
            "sess-exit",
            &log.display().to_string(),
            Some(own_pid),
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        crate::state::record_bridge_turn_exit(&conn, "sess-exit-1000", Some(1))
            .expect("record exit");

        let summary = process_bridge_turns(&conn, &bridge_test_config(), 20_000).expect("process");
        assert_eq!(summary["failed"], 1, "{summary}");
        let payload: String = conn
            .query_row("SELECT payload_json FROM outbound_events", [], |row| {
                row.get(0)
            })
            .expect("failure row");
        assert!(payload.contains("status 1"), "payload: {payload}");
    }

    /// A reaped exit lands on exactly the reaped TURN. Two active rows can
    /// share a pid across a daemon restart (recycling); stamping by pid
    /// marked both, and the innocent one was then settled as a crash.
    #[test]
    fn a_reaped_exit_marks_only_its_own_turn() {
        let conn = create_state_db_in_memory().expect("db");
        for turn_id in ["turn-stale", "turn-fresh"] {
            crate::state::register_bridge_turn(
                &conn,
                turn_id,
                "sess-1",
                "/tmp/t.log",
                Some(7_777),
                None,
                None,
                Some(7_777),
                None,
                None,
                1000,
            )
            .expect("register");
        }
        crate::state::mark_bridge_turn_stopping(&conn, "turn-stale", 1500).expect("intent");

        crate::state::record_bridge_turn_exit(&conn, "turn-fresh", Some(0)).expect("record");

        let exited = |turn: &str| -> i64 {
            conn.query_row(
                "SELECT exited FROM bridge_turns WHERE turn_id = ?1",
                [turn],
                |row| row.get(0),
            )
            .expect("row")
        };
        assert_eq!(exited("turn-fresh"), 1, "the reaped turn records its exit");
        assert_eq!(
            exited("turn-stale"),
            0,
            "the stale turn sharing the pid must NOT be marked exited"
        );
    }

    /// PID reuse after a daemon restart can make `kill -0` lie forever; the
    /// hard timeout is the backstop.
    #[test]
    fn bridge_turn_hard_timeout_fails_alive_looking_turns() {
        let conn = create_state_db_in_memory().expect("db");
        let log = write_turn_log("sess-timeout-1000", &["still nothing"]);
        crate::state::register_bridge_turn(
            &conn,
            "sess-timeout-1000",
            "sess-timeout",
            &log.display().to_string(),
            Some(std::process::id()),
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");

        let before = process_bridge_turns(
            &conn,
            &bridge_test_config(),
            1000 + BRIDGE_TURN_MAX_RUNTIME_MS,
        )
        .expect("before timeout");
        assert_eq!(before["running"], 1);

        let _ = crate::claude::test_kill::take();
        let after = process_bridge_turns(
            &conn,
            &bridge_test_config(),
            1000 + BRIDGE_TURN_MAX_RUNTIME_MS + 1,
        )
        .expect("after timeout");
        assert_eq!(after["failed"], 1, "{after}");
        let payload: String = conn
            .query_row("SELECT payload_json FROM outbound_events", [], |row| {
                row.get(0)
            })
            .expect("timeout row");
        assert!(payload.contains("hard timeout"), "payload: {payload}");
        assert_eq!(
            crate::claude::test_kill::take(),
            vec![std::process::id()],
            "a timed-out turn must actually be terminated, not just untracked"
        );
    }

    /// The Stop hook can wake the daemon a cycle BEFORE the result JSON hits
    /// the turn log; the away push from that earlier cycle must still count
    /// as this answer's delivery.
    #[test]
    fn bridge_turn_skips_push_when_away_pushed_in_earlier_cycle() {
        let conn = create_state_db_in_memory().expect("db");
        let log = write_turn_log(
            "sess-early-1000",
            &[r#"{"type":"result","subtype":"success","result":"the answer"}"#],
        );
        crate::state::register_bridge_turn(
            &conn,
            "sess-early-1000",
            "sess-early",
            &log.display().to_string(),
            Some(0),
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        // Away sync pushed the completion at an earlier cycle (1500), the
        // result JSON is only read at 3000.
        enqueue_outbound_event(
            &conn,
            &json!({
                "type": "thread_completed",
                "threadId": "sess-early",
                "updatedAt": 1500,
                "lastPreview": "the answer"
            }),
            1500,
            "away",
        )
        .expect("away enqueue");

        let summary = process_bridge_turns(&conn, &bridge_test_config(), 3000).expect("process");
        assert_eq!(summary["answered"], 1);
        assert_eq!(
            pending_outbound_count(&conn).expect("pending"),
            1,
            "the answer already went out in the earlier cycle"
        );
    }

    /// The away-duplicate check is bound to the answer content: someone
    /// else's completion (terminal turn, another reply) in the same cycle
    /// must not swallow this turn's distinct answer.
    #[test]
    fn bridge_turn_pushes_answer_when_away_completion_is_someone_elses() {
        let conn = create_state_db_in_memory().expect("db");
        let log = write_turn_log(
            "sess-mix-1000",
            &[r#"{"type":"result","subtype":"success","result":"my distinct answer"}"#],
        );
        crate::state::register_bridge_turn(
            &conn,
            "sess-mix-1000",
            "sess-mix",
            &log.display().to_string(),
            Some(0),
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        // Same session, same cycle, but a DIFFERENT completion (e.g. the
        // session's own terminal activity while away).
        enqueue_outbound_event(
            &conn,
            &json!({
                "type": "thread_completed",
                "threadId": "sess-mix",
                "updatedAt": 2000,
                "lastPreview": "the agent's own loop report"
            }),
            2000,
            "away",
        )
        .expect("away enqueue");

        let summary = process_bridge_turns(&conn, &bridge_test_config(), 2000).expect("process");
        assert_eq!(summary["answered"], 1);
        assert_eq!(
            pending_outbound_count(&conn).expect("pending"),
            2,
            "a mismatched away completion must not swallow the reply's answer"
        );
    }

    /// While away, the Stop-hook sync path may enqueue the same completion in
    /// the same cycle; the bridge push must then stand down.
    #[test]
    fn bridge_turn_skips_push_when_away_sync_already_enqueued_it() {
        let conn = create_state_db_in_memory().expect("db");
        let log = write_turn_log(
            "sess-dup-1000",
            &[r#"{"type":"result","subtype":"success","result":"the answer"}"#],
        );
        crate::state::register_bridge_turn(
            &conn,
            "sess-dup-1000",
            "sess-dup",
            &log.display().to_string(),
            Some(0),
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        // Simulate the away sync having enqueued this completion at `now`.
        enqueue_outbound_event(
            &conn,
            &json!({
                "type": "thread_completed",
                "threadId": "sess-dup",
                "updatedAt": 2000,
                "lastPreview": "the answer"
            }),
            2000,
            "away",
        )
        .expect("away enqueue");

        let summary = process_bridge_turns(&conn, &bridge_test_config(), 2000).expect("process");
        assert_eq!(summary["answered"], 1);
        assert_eq!(
            pending_outbound_count(&conn).expect("pending"),
            1,
            "the same answer must not be queued twice"
        );
    }

    #[test]
    fn daemon_notification_policy_skips_events_before_away_started() {
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        set_away_mode(&conn, true, 2000).expect("away on");
        let events = vec![
            json!({
                "type": "thread_waiting",
                "threadId": "thr_old",
                "updatedAt": 1500
            }),
            json!({
                "type": "thread_waiting",
                "threadId": "thr_new",
                "updatedAt": 2500
            }),
        ];

        let count = enqueue_daemon_notification_events(&conn, &events, 3000).expect("enqueue");

        assert_eq!(count, 1);
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);
    }

    #[test]
    fn daemon_notification_policy_accepts_second_granularity_timestamps() {
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        set_away_mode(&conn, true, 1_776_219_288_240).expect("away on");
        let events = vec![
            json!({
                "type": "thread_completed",
                "threadId": "thr_old",
                "updatedAt": 1_776_219_200
            }),
            json!({
                "type": "thread_completed",
                "threadId": "thr_new",
                "updatedAt": 1_776_219_396
            }),
        ];

        let count = enqueue_daemon_notification_events(&conn, &events, 1_776_219_397_000)
            .expect("mixed timestamp enqueue");

        assert_eq!(count, 1);
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);
    }

    #[test]
    fn thread_error_streak_notifies_once_and_rearms_after_recovery() {
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let no_filter: Option<&std::collections::BTreeSet<String>> = None;

        // Not away: nothing notifies, and no quiet streak starts.
        let (_, quiet) =
            enqueue_sync_error_notification(&conn, no_filter, &anyhow::anyhow!("scan failed"), 600)
                .expect("not away");
        assert_eq!(quiet, 0);

        // Going away with the error still present must notify (the flag was
        // not set by the silent occurrence above).
        set_away_mode(&conn, true, 1000).expect("away on");
        let (_, first) = enqueue_sync_error_notification(
            &conn,
            no_filter,
            &anyhow::anyhow!("scan failed"),
            2000,
        )
        .expect("first away notify");
        assert_eq!(first, 1, "errors observed while away must notify");

        // Continuous failure: later cycles stay quiet.
        let (_, repeat) = enqueue_sync_error_notification(
            &conn,
            no_filter,
            &anyhow::anyhow!("scan failed"),
            3500,
        )
        .expect("repeat");
        assert_eq!(repeat, 0, "a persistent error must not notify every cycle");

        // A different error mid-streak is a new notification.
        let (_, other) = enqueue_sync_error_notification(
            &conn,
            no_filter,
            &anyhow::anyhow!("other failure"),
            4000,
        )
        .expect("other error");
        assert_eq!(other, 1);

        // Recovery (successful sync) re-arms: the ORIGINAL error recurring
        // later — even much later, e.g. the next away session — notifies again.
        end_sync_error_streak(&conn).expect("streak end");
        let (_, recurrence) = enqueue_sync_error_notification(
            &conn,
            no_filter,
            &anyhow::anyhow!("scan failed"),
            9000,
        )
        .expect("recurrence");
        assert_eq!(
            recurrence, 1,
            "the same error after a successful sync is a fresh incident"
        );
    }

    /// /back can delete an undelivered error notification from the outbox;
    /// the streak flag must re-arm with it, or the same persistent error
    /// would stay silent for the entire next away session.
    #[test]
    fn thread_error_streak_rearms_when_away_turns_off() {
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        let no_filter: Option<&std::collections::BTreeSet<String>> = None;

        set_away_mode(&conn, true, 1000).expect("away on");
        let (_, first) = enqueue_sync_error_notification(
            &conn,
            no_filter,
            &anyhow::anyhow!("scan failed"),
            2000,
        )
        .expect("first notify");
        assert_eq!(first, 1);

        // Delivery never happened; /back wipes the pending outbox row.
        set_away_mode(&conn, false, 3000).expect("back");
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 0);

        // Next away session with the error still present must notify again.
        set_away_mode(&conn, true, 4000).expect("away again");
        let (_, again) = enqueue_sync_error_notification(
            &conn,
            no_filter,
            &anyhow::anyhow!("scan failed"),
            5000,
        )
        .expect("second away notify");
        assert_eq!(
            again, 1,
            "/back must re-arm the error streak alongside clearing the outbox"
        );
    }

    #[test]
    fn away_off_clears_pending_daemon_notifications() {
        // Serialised with every other test that touches the process-wide
        // away marker: these write it through the shared state dir, and an
        // unlocked writer flipping it mid-run is what made the question-gate
        // tests fail intermittently.
        let _env_guard = crate::state::test_env_lock();
        let conn = create_state_db_in_memory().expect("db");
        set_away_mode(&conn, true, 1000).expect("away on");
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_1",
            "updatedAt": 1500
        });
        enqueue_daemon_notification_events(&conn, &[event], 2000).expect("enqueue");
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);

        let disabled = set_away_mode(&conn, false, 2500).expect("away off");

        assert_eq!(disabled["away"], false);
        assert_eq!(disabled["clearedPendingNotifications"], 1);
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 0);
    }

    #[test]
    fn macos_service_runtime_marks_running_services_as_loaded() {
        let runtime = macos_service_runtime(&json!({
            "success": true,
            "stdout": "gui/501/tinyctb = {\n\tstate = running\n}\n",
            "stderr": ""
        }));

        assert_eq!(runtime["loaded"], true);
        assert_eq!(runtime["running"], true);
        assert_eq!(runtime["state"], "running");
    }

    #[test]
    fn macos_service_runtime_marks_missing_services_as_not_loaded() {
        let runtime = macos_service_runtime(&json!({
            "success": false,
            "stdout": "",
            "stderr": "Bad request.\nCould not find service \"tinyctb\" in domain for user gui: 501"
        }));

        assert_eq!(runtime["loaded"], false);
        assert_eq!(runtime["running"], false);
        assert_eq!(runtime["state"], Value::Null);
    }

    #[test]
    fn macos_launchd_spec_generates_reloadable_plist() {
        let spec = macos_launchd_spec(
            "tinyctb",
            "/opt/tiny ctb/bin/tinyctb",
            Path::new("/Users/test/Library/LaunchAgents"),
            Path::new("/Users/test/.tinyctb/logs/daemon.out.log"),
            Path::new("/Users/test/.tinyctb/logs/daemon.err.log"),
            "/opt/homebrew/bin:/usr/bin",
            None,
            None,
        );

        assert_eq!(
            spec.service_path.display().to_string(),
            "/Users/test/Library/LaunchAgents/tinyctb.plist"
        );
        assert!(spec.contents.contains("<key>Label</key>"));
        // Space in the binary path must survive as-is inside ProgramArguments
        // (array semantics, no shell), with XML escaping applied.
        assert!(spec
            .contents
            .contains("<string>/opt/tiny ctb/bin/tinyctb</string>"));
        assert!(spec.contents.contains("<string>daemon</string>"));
        assert!(spec.contents.contains("<string>run</string>"));
        assert!(spec.contents.contains("<key>RunAtLoad</key>"));
        assert!(spec.contents.contains("<key>KeepAlive</key>"));
        assert!(spec
            .contents
            .contains("<string>/Users/test/.tinyctb/logs/daemon.out.log</string>"));
        assert!(!spec.contents.contains("CLAUDE_BIN"));

        // Install and start must bootout the existing job before bootstrapping
        // so an updated plist actually takes effect.
        for command in [&spec.install_command, &spec.start_command] {
            let bootout = command.find("launchctl bootout").expect("bootout present");
            let bootstrap = command
                .find("launchctl bootstrap")
                .expect("bootstrap present");
            assert!(
                bootout < bootstrap,
                "bootout must run before bootstrap: {command}"
            );
        }
        assert!(!spec.start_command.contains("kickstart"));
        assert!(spec.status_command.starts_with("launchctl print gui/"));
    }

    #[test]
    fn macos_launchd_spec_escapes_xml_and_passes_claude_bin() {
        let spec = macos_launchd_spec(
            "tinyctb",
            "/opt/a&b/tinyctb",
            Path::new("/Users/test/Library/LaunchAgents"),
            Path::new("/Users/test/.tinyctb/logs/daemon.out.log"),
            Path::new("/Users/test/.tinyctb/logs/daemon.err.log"),
            "/usr/bin",
            Some("/custom/<claude> & bin"),
            Some("/tmp/verify state"),
        );

        assert!(spec
            .contents
            .contains("<string>/opt/a&amp;b/tinyctb</string>"));
        assert!(spec.contents.contains("<key>CLAUDE_BIN</key>"));
        assert!(spec.contents.contains("<key>TINYCTB_STATE_DIR</key>"));
        assert!(spec.contents.contains("<string>/tmp/verify state</string>"));
        assert!(spec
            .contents
            .contains("<string>/custom/&lt;claude&gt; &amp; bin</string>"));
        assert!(!spec.contents.contains("<string>/custom/<claude>"));
    }

    #[test]
    fn service_bridge_command_falls_back_to_current_exe_for_unknown_names() {
        let resolved = resolve_service_bridge_command("definitely-not-a-real-binary-tinyctb-test");
        assert!(
            std::path::Path::new(&resolved).is_absolute(),
            "service command must be absolute, got: {resolved}"
        );
    }

    #[test]
    fn absolutize_env_path_makes_relative_paths_absolute() {
        let absolute = absolutize_env_path("/opt/claude/bin/claude");
        assert_eq!(absolute, "/opt/claude/bin/claude");

        let relative = absolutize_env_path("./bin/claude");
        assert!(
            Path::new(&relative).is_absolute(),
            "relative CLAUDE_BIN must be absolutized, got: {relative}"
        );
        assert!(relative.ends_with("bin/claude"));
    }
}
