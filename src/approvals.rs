//! Remote approvals: answering Claude's permission prompts from Telegram.
//!
//! Runs as a `PermissionRequest` hook — the event Claude Code raises right
//! before it would show a permission prompt. That timing is the whole point:
//! the gate only ever engages for calls that genuinely need an answer, so
//! there is no need to guess which tools are risky or to re-implement the
//! `permissions.allow` rules (a `PreToolUse` gate, which fires for every
//! single tool call, would have to do both and would still get it wrong).
//!
//! The session's messaging socket cannot answer a permission prompt — it
//! accepts user messages only — so a blocking hook is the mechanism.
//!
//! Safety rules, all load-bearing:
//! - a timeout NEVER allows; it returns "no opinion" and the normal
//!   permission flow takes over (the terminal dialog, exactly as today);
//! - nothing is gated unless away mode is on, so being at the keyboard
//!   behaves as it always did;
//! - turns the bridge itself started run with `bypassPermissions` and are
//!   skipped, otherwise a Telegram-initiated turn would block on its own
//!   approval request with nobody watching.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::io::Read;
use std::time::Duration;

use crate::claude::{generate_session_uuid, truncate_tool_detail};
use crate::config::load_daemon_config;
use crate::state::{
    approval_auto_allowed, approval_decision, create_pending_approval, create_state_db,
    enqueue_outbound_event, insert_telegram_callback_route, remote_mode_status_path,
    set_approval_auto_allow, state_db_path, TelegramCallbackAction, TelegramCallbackRoute,
};

const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// "No opinion": the normal permission flow decides. This is the answer for
/// every path that is not an explicit remote allow/deny.
fn no_opinion() -> Value {
    json!({})
}

fn allow(reason: &str) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "PermissionRequest",
            "decision": { "behavior": "allow" },
            "permissionDecisionReason": reason
        }
    })
}

fn deny(message: &str) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "PermissionRequest",
            "decision": { "behavior": "deny", "message": message }
        }
    })
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

pub(crate) fn run_approval_gate<R: Read>(reader: &mut R, now: u64) -> Result<Value> {
    let mut raw = String::new();
    reader
        .take(1024 * 1024)
        .read_to_string(&mut raw)
        .context("failed to read PermissionRequest payload")?;
    let payload: Value =
        serde_json::from_str(raw.trim()).context("PermissionRequest payload is not valid JSON")?;

    // --- fast paths, no database ------------------------------------------
    if !away_mode_active() {
        return Ok(no_opinion());
    }
    let tool_name = payload
        .get("tool_name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if tool_name.is_empty() {
        return Ok(no_opinion());
    }
    // A turn the bridge started for the user runs unattended by design; it
    // must not stop to ask the very person who is not there.
    if payload.get("permission_mode").and_then(Value::as_str) == Some("bypassPermissions") {
        return Ok(no_opinion());
    }
    let config = match load_daemon_config() {
        Ok(config) => config,
        // Unconfigured bridge: stay out of the way entirely.
        Err(_) => return Ok(no_opinion()),
    };
    let claude = config.claude.clone().unwrap_or_default();
    // The event already means "this call needs an answer", so an empty list
    // gates everything that asks. A non-empty list is an explicit narrowing
    // for people who only want to be bothered about certain tools.
    if !claude.approval_tools.is_empty()
        && !claude
            .approval_tools
            .iter()
            .any(|gated| gated == &tool_name)
    {
        return Ok(no_opinion());
    }
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

    // --- database-backed path ---------------------------------------------
    let conn = create_state_db(&state_db_path()?)?;
    if approval_auto_allowed(&conn, &thread_id, &tool_name)? {
        return Ok(allow(&format!(
            "{tool_name} was approved for this session from Telegram"
        )));
    }

    let approval_id = payload
        .get("tool_use_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| generate_session_uuid().unwrap_or_else(|_| now.to_string()));
    let summary = tool_call_summary(&tool_name, payload.get("tool_input"));
    let wait = Duration::from_secs(claude.approval_timeout_seconds.clamp(5, 3600));
    create_pending_approval(
        &conn,
        &approval_id,
        &thread_id,
        &tool_name,
        &summary,
        now,
        now + wait.as_millis() as u64,
    )?;

    // The buttons are registered before the message is sent so the callback
    // can be resolved the moment the user taps.
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
            &conn,
            &TelegramCallbackRoute {
                callback_id: callback_id.clone(),
                chat_id: telegram.chat_id.clone(),
                message_id: None,
                thread_id: thread_id.clone(),
                action,
                approval_id: Some(approval_id.clone()),
            },
            now,
        )?;
        buttons
            .push(json!({ "text": label, "callbackId": callback_id, "action": action.as_str() }));
    }

    let cwd = payload.get("cwd").and_then(Value::as_str);
    let event = json!({
        "type": "approval_request",
        "threadId": thread_id,
        "approvalId": approval_id,
        "toolName": tool_name,
        "observedAt": now,
        "eventKey": format!("approval:{approval_id}"),
        "lastPreview": summary,
        "buttons": buttons,
        "thread": {
            "threadId": thread_id,
            "cwd": cwd,
            "project": crate::projects::derive_project_label(cwd),
            "lastPreview": summary
        }
    });
    // origin "bridge": an approval request is something the user asked for by
    // going away, and it must survive /back's away-backlog cleanup.
    enqueue_outbound_event(&conn, &event, now, "bridge")?;

    // --- wait for the answer ----------------------------------------------
    let deadline = std::time::Instant::now() + wait;
    while std::time::Instant::now() < deadline {
        std::thread::sleep(POLL_INTERVAL);
        if let Some(decision) = approval_decision(&conn, &approval_id)? {
            if decision != "expired" {
                return apply_decision(&conn, &thread_id, &tool_name, &decision, now);
            }
        }
    }
    // Timed out. Settling the record and taking a decision is one atomic
    // step: if a tap landed in the instant between the last poll and here,
    // that answer wins and must be honoured — otherwise Telegram would show
    // "已允许" while the session quietly fell back to its own prompt.
    match crate::state::expire_or_take_decision(&conn, &approval_id, now)? {
        Some(decision) => apply_decision(&conn, &thread_id, &tool_name, &decision, now),
        // Nobody answered: explicitly NOT an approval. The normal permission
        // flow takes over, which in an interactive session is the terminal
        // dialog.
        None => Ok(no_opinion()),
    }
}

/// Turn a recorded answer into the hook's reply. Kept in one place so the
/// polling loop and the timeout race resolve an answer identically.
fn apply_decision(
    conn: &rusqlite::Connection,
    thread_id: &str,
    tool_name: &str,
    decision: &str,
    now: u64,
) -> Result<Value> {
    match decision {
        "allow" => Ok(allow("Approved from Telegram")),
        "allow_session" => {
            set_approval_auto_allow(conn, thread_id, tool_name, now)?;
            Ok(allow(&format!(
                "Approved from Telegram; {tool_name} is allowed for this session"
            )))
        }
        "deny" => Ok(deny("Denied from Telegram")),
        // Unknown value: refuse to guess, fall back to the terminal prompt.
        _ => Ok(no_opinion()),
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
    use crate::config::{ClaudeConfig, DaemonConfig, TelegramConfig};
    use crate::state::{record_approval_decision, write_away_marker_for_test};
    use std::fs;
    use std::path::PathBuf;

    struct GateEnv {
        root: PathBuf,
        previous_state_dir: Option<String>,
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
            let previous_state_dir = std::env::var("TINYCTB_STATE_DIR").ok();
            std::env::set_var("TINYCTB_STATE_DIR", &root);
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
            Self {
                root,
                previous_state_dir,
            }
        }
    }

    impl Drop for GateEnv {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous_state_dir {
                std::env::set_var("TINYCTB_STATE_DIR", previous);
            } else {
                std::env::remove_var("TINYCTB_STATE_DIR");
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn gate(payload: Value) -> Value {
        let mut reader = std::io::Cursor::new(payload.to_string());
        run_approval_gate(&mut reader, 1000).expect("gate")
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

    /// The gate mints its own approval id, so tests look it up.
    fn pending_approval_id(conn: &rusqlite::Connection) -> String {
        conn.query_row(
            "SELECT approval_id FROM pending_approvals ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .expect("an approval row")
    }

    #[test]
    fn gate_stays_out_of_the_way_when_not_away() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _env = GateEnv::new("not-away", false, 5);
        assert_eq!(gate(bash_payload()), json!({}), "no opinion while present");
    }

    #[test]
    fn gate_skips_bridge_initiated_turns() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
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
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
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

    /// The safety rule: waiting out the clock is NOT consent.
    #[test]
    fn gate_timeout_never_allows() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _env = GateEnv::new("timeout", true, 5); // clamped minimum
        let started = std::time::Instant::now();
        let result = gate(bash_payload());
        assert_eq!(result, json!({}), "timeout must yield no opinion");
        assert!(
            started.elapsed() >= Duration::from_secs(4),
            "the gate must actually wait for an answer"
        );

        // The request that was pushed carries the real command, not just a
        // tool name, and rides the bridge origin so /back cannot drop it.
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let (payload_json, origin): (String, String) = conn
            .query_row(
                "SELECT payload_json, origin FROM outbound_events",
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
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
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
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _env = GateEnv::new("late-tap", true, 5);
        let result = gate(bash_payload());
        assert_eq!(result, json!({}), "timeout yields no opinion");

        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        let approval_id = pending_approval_id(&conn);
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

    /// "Allow for this session" must stop asking on the next call, otherwise
    /// an agent doing many Bash calls needs one tap per call.
    #[test]
    fn session_scoped_allow_short_circuits_later_calls() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
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
