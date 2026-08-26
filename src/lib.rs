use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod approvals;
mod claude;
mod cli;
mod config;
mod daemon;
mod hooks;
mod projects;
mod state;
mod telegram;

use crate::claude::{
    find_session_file, get_away_mode, normalized_message, parse_transcript_messages,
    parse_transcript_summary, resolve_claude_binary, set_away_mode, sync_state_from_sessions,
    write_hook_event_from_reader,
};
use crate::cli::{
    AwayCommands, Cli, Commands, DaemonCommands, HookCommands, ProjectCommands, TelegramCommands,
};
use crate::daemon::{
    daemon_lock_free, daemon_service_logs, daemon_service_spec, daemon_service_status,
    install_daemon_service, run_daemon, start_daemon_service, stop_daemon_service,
    uninstall_daemon_service, DEFAULT_DAEMON_LABEL,
};
use crate::hooks::{hooks_status, install_hooks, uninstall_hooks};
use crate::projects::{build_registered_project, ensure_unique_project_id, slugify_project_token};
#[cfg(test)]
use crate::state::{classify_inbox_item, create_state_db_in_memory, BridgeThreadSnapshot};
use crate::state::{
    create_state_db, list_inbox_from_db, list_waiting_from_db, observed_workspaces_from_db,
    record_action, state_db_path, ObservedWorkspace,
};
#[cfg(test)]
use crate::state::{
    deliver_due_outbound_events, enqueue_outbound_event, record_transport_delivery,
    transport_delivered_at, OutboxDeliverySummary,
};
use crate::telegram::{telegram_setup_result, telegram_status_result, telegram_test_result};
use clap::Parser;
#[cfg(test)]
pub(crate) use config::ClaudeConfig;
pub(crate) use config::{
    daemon_config_path, load_daemon_config, merged_daemon_config, read_daemon_config_raw,
    redacted_daemon_config, resolve_telegram_bot_token, write_daemon_config, DaemonConfig,
    RegisteredProject, SetupOptions, TelegramConfig, TelegramSetupOptions,
};
pub(crate) use state::state_dir_path;

#[derive(Serialize)]
struct ErrorEnvelope {
    ok: bool,
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

#[derive(Serialize)]
struct DoctorEnvelope {
    ok: bool,
    claude: DoctorClaude,
    hooks: Value,
    bridge: DoctorBridge,
}

#[derive(Serialize)]
struct DoctorClaude {
    resolved_path: String,
    source: String,
    version_stdout: String,
}

#[derive(Serialize)]
struct DoctorBridge {
    config_path: String,
    config_exists: bool,
    telegram_configured: bool,
    permission_mode: Option<String>,
    daemon_service_path: String,
    daemon_service_exists: bool,
    daemon_service_loaded: bool,
    daemon_running: bool,
    daemon_state: Option<String>,
    daemon_healthy: bool,
    next_step: Option<String>,
}

#[derive(Serialize)]
struct ReplyResult<'a> {
    ok: bool,
    action: &'a str,
    dry_run: bool,
    thread_id: &'a str,
    message: &'a str,
    sent_at: u64,
}

/// Runs the CLI and returns the process exit code. `main` owns the actual
/// `process::exit`, so library code never terminates the process itself —
/// a command that printed its own failure report (e.g. `setup` with a failed
/// daemon start) returns a non-zero code without a second error envelope.
pub fn main_entry() -> anyhow::Result<i32> {
    run()
}

pub fn render_error_envelope(error: &anyhow::Error) -> String {
    let envelope = ErrorEnvelope {
        ok: false,
        error: ErrorBody {
            code: "internal_error",
            message: format!("{error:#}"),
        },
    };

    serde_json::to_string(&envelope).expect("serialize error envelope")
}

fn run() -> Result<i32> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Setup {
            bot_token,
            chat_id,
            allowed_user_id,
            events,
            bridge_command,
            daemon_label,
            install_daemon,
            start_daemon,
            install_hooks,
            pair_timeout_ms,
            dry_run,
        } => {
            let result = setup_result(SetupOptions {
                bot_token: bot_token.as_deref(),
                chat_id: chat_id.as_deref(),
                allowed_user_id: allowed_user_id.as_deref(),
                events: &events,
                bridge_command: &bridge_command,
                daemon_label: &daemon_label,
                install_daemon,
                start_daemon,
                install_hooks,
                dry_run,
                pair_timeout_ms,
            })?;
            println!("{}", serde_json::to_string(&result)?);
            if result.get("ok").and_then(Value::as_bool) != Some(true) {
                return Ok(1);
            }
        }
        Commands::Doctor => {
            let resolved = resolve_claude_binary()?;
            let output = Command::new(&resolved.path)
                .arg("--version")
                .output()
                .with_context(|| {
                    format!("failed to execute {} --version", resolved.path.display())
                })?;
            if !output.status.success() {
                bail!(
                    "claude binary {} returned non-zero exit status for --version",
                    resolved.path.display()
                );
            }
            let payload = DoctorEnvelope {
                ok: true,
                claude: DoctorClaude {
                    resolved_path: resolved.path.display().to_string(),
                    source: resolved.source.to_string(),
                    version_stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
                },
                hooks: hooks_status()?,
                bridge: doctor_bridge()?,
            };
            println!("{}", serde_json::to_string(&payload)?);
        }
        Commands::Away { command } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let payload = match command {
                AwayCommands::On => set_away_mode(&conn, true, now)?,
                AwayCommands::Off => set_away_mode(&conn, false, now)?,
                AwayCommands::Status => get_away_mode(&conn)?,
            };
            println!("{}", serde_json::to_string(&payload)?);
        }
        Commands::Threads { limit } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let config = load_daemon_config()?;
            let result = sync_state_from_sessions(&conn, &config, now, limit, false)?;
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "threads": result["threads"].clone()
                }))?
            );
        }
        Commands::Waiting { project, limit } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let config = load_daemon_config()?;
            sync_state_from_sessions(&conn, &config, now, limit.max(25), false)?;
            let result = list_waiting_from_db(&conn, project.as_deref(), limit)?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::Inbox {
            project,
            status,
            attention,
            waiting_on,
            limit,
        } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let config = load_daemon_config()?;
            sync_state_from_sessions(&conn, &config, now, limit.max(25), false)?;
            let result = list_inbox_from_db(
                &conn,
                now,
                project.as_deref(),
                status.as_deref(),
                attention.as_deref(),
                waiting_on.as_deref(),
                limit,
            )?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::Daemon { command } => match command {
            DaemonCommands::Run {
                once,
                poll_interval,
                timeout_ms,
            } => {
                run_daemon(once, poll_interval, Duration::from_millis(timeout_ms))?;
            }
            DaemonCommands::Install {
                dry_run,
                label,
                bridge_command,
            } => {
                let result = install_daemon_service(&label, &bridge_command, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Uninstall { dry_run, label } => {
                let result = uninstall_daemon_service(&label, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Start { dry_run, label } => {
                let result = start_daemon_service(&label, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Stop { dry_run, label } => {
                let result = stop_daemon_service(&label, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Status { label } => {
                let result = daemon_service_status(&label)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            DaemonCommands::Logs { label } => {
                let result = daemon_service_logs(&label)?;
                println!("{}", serde_json::to_string(&result)?);
            }
        },
        Commands::Telegram { command } => match command {
            TelegramCommands::Setup {
                bot_token,
                chat_id,
                allowed_user_id,
                events,
                bridge_command,
                pair_timeout_ms,
                dry_run,
            } => {
                let result = telegram_setup_result(TelegramSetupOptions {
                    bot_token: bot_token.as_deref(),
                    chat_id: chat_id.as_deref(),
                    allowed_user_id: allowed_user_id.as_deref(),
                    events: &events,
                    bridge_command: &bridge_command,
                    dry_run,
                    pair_timeout_ms,
                })?;
                println!("{}", serde_json::to_string(&result)?);
            }
            TelegramCommands::Status => {
                let result = telegram_status_result()?;
                println!("{}", serde_json::to_string(&result)?);
            }
            TelegramCommands::Test {
                message,
                timeout_ms,
                dry_run,
            } => {
                let result =
                    telegram_test_result(&message, Duration::from_millis(timeout_ms), dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
        },
        Commands::Hooks { command } => match command {
            HookCommands::Install {
                bridge_command,
                dry_run,
            } => {
                let result = install_hooks(&bridge_command, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            HookCommands::Uninstall { dry_run } => {
                let result = uninstall_hooks(dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            HookCommands::Status => {
                let result = hooks_status()?;
                println!("{}", serde_json::to_string(&result)?);
            }
        },
        Commands::HookEvent => {
            // Called from inside Claude Code hooks: never fail the hosting
            // session, always exit 0, and keep stdout parseable.
            let now = now_millis().unwrap_or(0);
            let stdin = std::io::stdin();
            match write_hook_event_from_reader(&mut stdin.lock(), now) {
                Ok(result) => println!("{}", serde_json::to_string(&result)?),
                Err(error) => println!(
                    "{}",
                    json!({
                        "ok": false,
                        "action": "hook_event",
                        "error": format!("{error:#}")
                    })
                ),
            }
        }
        Commands::ApprovalGate => {
            // Runs inside Claude Code before every tool call: it must never
            // break the session, so any failure degrades to "no opinion"
            // (the normal permission flow) rather than an error.
            let now = now_millis().unwrap_or(0);
            let stdin = std::io::stdin();
            let result =
                approvals::run_approval_gate(&mut stdin.lock(), now).unwrap_or_else(|_| json!({}));
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::HeadlessApprovalGate => {
            // No degrade-to-"no opinion" here, unlike the two gates around
            // it: under `bypassPermissions` an empty reply IS execution, so
            // errors must deny. The gate itself is infallible and confines
            // failing closed to sessions with a running bridge turn.
            let now = now_millis().unwrap_or(0);
            let stdin = std::io::stdin();
            let result = approvals::run_headless_approval_gate(&mut stdin.lock(), now);
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::QuestionGate => {
            let now = now_millis().unwrap_or(0);
            let stdin = std::io::stdin();
            let result =
                approvals::run_question_gate(&mut stdin.lock(), now).unwrap_or_else(|_| json!({}));
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::PromptContext => {
            let stdin = std::io::stdin();
            let result = approvals::run_prompt_context(&mut stdin.lock());
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::Projects { command } => match command {
            ProjectCommands::List { observed_limit } => {
                let result = projects_list_result(observed_limit)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            ProjectCommands::Add {
                cwd,
                id,
                label,
                aliases,
                dry_run,
            } => {
                let result =
                    project_add_result(&cwd, id.as_deref(), label.as_deref(), &aliases, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            ProjectCommands::Import { limit, dry_run } => {
                let result = project_import_result(limit, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            ProjectCommands::Remove { id, dry_run } => {
                let result = project_remove_result(&id, dry_run)?;
                println!("{}", serde_json::to_string(&result)?);
            }
        },
        Commands::Sync { limit } => {
            let now = now_millis()?;
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let config = load_daemon_config()?;
            let result = sync_state_from_sessions(&conn, &config, now, limit, false)?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Commands::New {
            cwd,
            message,
            dry_run,
            prompt,
        } => {
            let message = normalized_message(message.as_deref()).or_else(|| {
                let joined = prompt.join(" ").trim().to_string();
                (!joined.is_empty()).then_some(joined)
            });
            if dry_run {
                println!(
                    "{}",
                    serde_json::to_string(&json!({
                        "ok": true,
                        "action": "new",
                        "dryRun": true,
                        "cwd": cwd,
                        "message": message
                    }))?
                );
            } else {
                // Same rule as `reply`: the shared spawn lock precedes the
                // database handle.
                let _spawn_permit = daemon::hold_spawn_lock_shared()?;
                let now = now_millis()?;
                let config = load_daemon_config()?;
                let db_path = state_db_path()?;
                let conn = create_state_db(&db_path)?;
                let result = claude::start_thread_in_cwd(
                    &conn,
                    &config,
                    cwd.as_deref(),
                    message.as_deref(),
                    now,
                )?;
                if let Some(thread_id) = result.get("threadId").and_then(Value::as_str) {
                    record_action(
                        &conn,
                        thread_id,
                        "new",
                        json!({ "result": result.clone(), "sentAt": now }),
                        now,
                    )?;
                }
                println!("{}", serde_json::to_string(&result)?);
            }
        }
        Commands::Show {
            thread_id,
            messages,
        } => {
            let db_path = state_db_path()?;
            let conn = create_state_db(&db_path)?;
            let info = find_session_file(&thread_id)?
                .with_context(|| format!("no Claude session transcript found for {thread_id}"))?;
            let summary = parse_transcript_summary(&info.path, now_millis()?)?;
            let message_list = parse_transcript_messages(&info.path, messages)?;
            let cached = conn
                .query_row(
                    "SELECT thread_id FROM threads_cache WHERE thread_id = ?1",
                    rusqlite::params![thread_id],
                    |_| Ok(()),
                )
                .is_ok();
            let history = state::recent_actions_json(&conn, &thread_id, 10)?;
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "ok": true,
                    "action": "show",
                    "threadId": thread_id,
                    "transcriptPath": info.path.display().to_string(),
                    "updatedAt": info.mtime_ms,
                    "name": summary.name,
                    "cwd": summary.cwd,
                    "lastPreview": summary.last_assistant_text,
                    "cached": cached,
                    "messages": message_list,
                    "recentActions": history
                }))?
            );
        }
        Commands::Reply {
            thread_id,
            message,
            dry_run,
            prompt,
        } => {
            let message = message
                .or_else(|| {
                    let joined = prompt.join(" ").trim().to_string();
                    (!joined.is_empty()).then_some(joined)
                })
                .unwrap_or_default()
                .trim()
                .to_string();
            if message.is_empty() {
                bail!("Reply message cannot be empty");
            }
            let sent_at = now_millis()?;
            if dry_run {
                let payload = ReplyResult {
                    ok: true,
                    action: "reply",
                    dry_run: true,
                    thread_id: &thread_id,
                    message: &message,
                    sent_at,
                };
                println!("{}", serde_json::to_string(&payload)?);
            } else {
                // The SHARED spawn lock comes BEFORE the database opens: a
                // teardown that already unlinked state.db must not find us
                // spawning through a handle to the deleted file.
                let _spawn_permit = daemon::hold_spawn_lock_shared()?;
                let config = load_daemon_config()?;
                let db_path = state_db_path()?;
                let conn = create_state_db(&db_path)?;
                let result =
                    claude::send_user_message(&conn, &config, &thread_id, &message, None, sent_at)?;
                record_action(
                    &conn,
                    &thread_id,
                    "reply",
                    json!({
                        "message": message,
                        "result": result.clone(),
                        "sentAt": sent_at
                    }),
                    sent_at,
                )?;
                println!("{}", serde_json::to_string(&result)?);
            }
        }
        Commands::Reset { dry_run } => {
            let result = reset_result(dry_run)?;
            println!("{}", serde_json::to_string(&result)?);
        }
    }

    Ok(0)
}

pub(crate) fn now_millis() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow!(e))?
        .as_millis() as u64)
}

fn importable_projects_from_observed(
    observed: &[ObservedWorkspace],
    existing_projects: &[RegisteredProject],
) -> Vec<RegisteredProject> {
    let mut projects = Vec::new();
    let mut existing_ids = existing_projects
        .iter()
        .map(|project| project.id.clone())
        .collect::<BTreeSet<_>>();
    let existing_cwds = existing_projects
        .iter()
        .map(|project| project.cwd.clone())
        .collect::<BTreeSet<_>>();
    for workspace in observed {
        if existing_cwds.contains(&workspace.cwd)
            || projects
                .iter()
                .any(|project: &RegisteredProject| project.cwd == workspace.cwd)
        {
            continue;
        }
        let base_id =
            slugify_project_token(&workspace.label).unwrap_or_else(|| "project".to_string());
        let id = ensure_unique_project_id(&base_id, &existing_ids);
        existing_ids.insert(id.clone());
        projects.push(RegisteredProject {
            id,
            label: workspace.label.clone(),
            cwd: workspace.cwd.clone(),
            aliases: Vec::new(),
        });
    }
    projects
}

fn daemon_run_command(bridge_command: &str) -> String {
    format!("{} daemon run", shell_quote(bridge_command))
}

fn doctor_bridge() -> Result<DoctorBridge> {
    let config_path = daemon_config_path()?;
    let config = read_daemon_config_raw()?;
    let telegram = config.as_ref().and_then(|config| config.telegram.as_ref());
    let permission_mode = config
        .as_ref()
        .and_then(|config| config.claude.as_ref())
        .map(|claude| claude.permission_mode.clone());
    let service = daemon_service_spec(DEFAULT_DAEMON_LABEL, "tinyctb")?;
    let status = daemon_service_status(DEFAULT_DAEMON_LABEL)?;
    Ok(DoctorBridge {
        config_path: config_path.display().to_string(),
        config_exists: config_path.exists(),
        telegram_configured: telegram.is_some(),
        permission_mode,
        daemon_service_path: service.service_path.display().to_string(),
        daemon_service_exists: service.service_path.exists(),
        daemon_service_loaded: status
            .pointer("/serviceStatus/loaded")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        daemon_running: status
            .pointer("/serviceStatus/running")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        daemon_state: status
            .pointer("/serviceStatus/state")
            .and_then(Value::as_str)
            .map(str::to_string),
        daemon_healthy: status
            .get("healthy")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        next_step: status
            .get("nextStep")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn setup_result(options: SetupOptions<'_>) -> Result<Value> {
    let resolved = resolve_claude_binary()?;
    let telegram = telegram_setup_result(TelegramSetupOptions {
        bot_token: options.bot_token,
        chat_id: options.chat_id,
        allowed_user_id: options.allowed_user_id,
        events: options.events,
        bridge_command: options.bridge_command,
        dry_run: options.dry_run,
        pair_timeout_ms: options.pair_timeout_ms,
    })?;
    let hooks = if options.install_hooks {
        Some(install_hooks(options.bridge_command, options.dry_run)?)
    } else {
        None
    };
    let daemon_install = if options.install_daemon {
        Some(install_daemon_service(
            options.daemon_label,
            options.bridge_command,
            options.dry_run,
        )?)
    } else {
        None
    };
    // Config, hooks, and service install above have already happened by the
    // time the daemon starts, so a start failure must not discard the report —
    // it is embedded here and aggregated into the top-level `ok`.
    let daemon_start = if options.start_daemon {
        Some(
            match start_daemon_service(options.daemon_label, options.dry_run) {
                Ok(result) => result,
                Err(error) => json!({
                    "ok": false,
                    "action": "daemon_start",
                    "error": format!("{error:#}")
                }),
            },
        )
    } else {
        None
    };
    Ok(assemble_setup_report(
        options.dry_run,
        json!({
            "resolvedPath": resolved.path.display().to_string(),
            "source": resolved.source
        }),
        telegram,
        hooks,
        daemon_install,
        daemon_start,
    ))
}

/// Pure assembly of the `setup` report so the failure-aggregation logic (a
/// failed daemon start must drive top-level `ok:false` while preserving the
/// install results) is unit-testable without shelling out to a service manager.
fn assemble_setup_report(
    dry_run: bool,
    claude: Value,
    telegram: Value,
    hooks: Option<Value>,
    daemon_install: Option<Value>,
    daemon_start: Option<Value>,
) -> Value {
    let daemon_start_failed = daemon_start
        .as_ref()
        .map(|result| result.get("ok").and_then(Value::as_bool) != Some(true))
        .unwrap_or(false);

    json!({
        "ok": !daemon_start_failed,
        "action": "setup",
        "dryRun": dry_run,
        "claude": claude,
        "telegram": telegram,
        "hooks": hooks,
        "daemon": {
            "install": daemon_install,
            "start": daemon_start
        },
        "nextStep": if daemon_start_failed {
            "Setup wrote the config, hooks, and service, but the daemon failed to start. Inspect `daemon.start.error`, then run `tinyctb daemon start` again."
        } else if dry_run {
            "Run setup without --dry-run, then send /away to the Telegram bot when leaving your computer."
        } else {
            "Restart running interactive Claude Code sessions so hooks load, then send /away to the Telegram bot when leaving your computer. Reply to Claude Telegram messages to keep working from Telegram."
        }
    })
}

const RESET_RUNTIME_FILES: &[&str] = &[
    "state.db",
    "state.db-wal",
    "state.db-shm",
    "remote-mode.json",
    "daemon.lock",
    approvals::PRESENCE_STATE_FILE,
    approvals::PRESENCE_LOCK_FILE,
    daemon::INPUT_ACTIVITY_FILE,
];

fn reset_result(dry_run: bool) -> Result<Value> {
    let state_dir = state_dir_path()?;
    let config_path = daemon_config_path()?;
    let config_preserved = config_path.exists();

    let mut daemon_stopped = false;
    // Held to the END of this function: the exclusive spawn freeze must
    // outlive every deletion below, or a spawner could slip in after the
    // sweep and before (or during) the unlink.
    let mut _spawn_freeze: Option<std::fs::File> = None;

    if !dry_run {
        let status = daemon_service_status(DEFAULT_DAEMON_LABEL)?;
        let service_exists = status
            .get("serviceExists")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let service_running = status
            .pointer("/serviceStatus/running")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if service_exists && service_running {
            let stop = stop_daemon_service(DEFAULT_DAEMON_LABEL, false)?;
            daemon_stopped = stop
                .pointer("/output/success")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        }

        let started = Instant::now();
        loop {
            if daemon_lock_free()? {
                break;
            }
            if started.elapsed() >= Duration::from_secs(5) {
                bail!("daemon is still running; stop it first with `tinyctb daemon stop`");
            }
            std::thread::sleep(Duration::from_millis(200));
        }

        // ORDER is the point: daemon provably gone (lock free) FIRST, then
        // the spawn lock held EXCLUSIVELY — no daemon `/new` and no CLI
        // spawn can mint a turn between the sweep and the deletion — and
        // only then the sweep. Sweeping before the stop let a live daemon
        // create an object right after the enumeration, which the ledger
        // deletion below then orphaned. Any object that cannot be proven
        // empty AND removed aborts with the ledger untouched.
        _spawn_freeze = Some(daemon::hold_spawn_lock_exclusive(Duration::from_secs(5))?);
        let report = crate::claude::turn_cgroup::sweep_all(std::time::Duration::from_secs(5))?;
        if !report.stubborn.is_empty() {
            bail!(
                "refusing to reset: {} turn cgroup(s) could not be proven empty and removed ({});                  stop the work they contain first",
                report.stubborn.len(),
                report
                    .stubborn
                    .iter()
                    .map(|dir| dir.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        // Legacy-regime turns (no ownership object) are INVISIBLE to the
        // sweep, and deleting the ledger under them strands both process
        // and record. The explicit degraded mode gets an explicit refusal.
        let db_path = state_db_path()?;
        if db_path.exists() {
            match create_state_db(&db_path) {
                Ok(conn) => {
                    let legacy_active = crate::state::count_active_legacy_turns(&conn)?;
                    if legacy_active > 0 {
                        bail!(
                            "refusing to reset: {legacy_active} legacy-supervised turn(s) (no \
                             cgroup) are still active; stop them or let them finish first"
                        );
                    }
                }
                Err(err) => {
                    // FAIL CLOSED: an unreadable ledger is not evidence
                    // that nothing is alive, and with KillMode=process a
                    // stopped daemon spares legacy descendants. The manual
                    // exit is explicit: verify no tinyctb-spawned process
                    // survives (the service cgroup should be empty), then
                    // remove state.db by hand and reset again.
                    bail!(
                        "refusing to reset: state.db exists but is unreadable ({err:#}); \
                         verify no legacy-supervised process survives, remove the file \
                         manually, then reset again"
                    );
                }
            }
        }
    }

    let mut removed: Vec<String> = Vec::new();
    let mut missing: Vec<String> = Vec::new();
    for name in RESET_RUNTIME_FILES {
        let path = state_dir.join(name);
        if dry_run {
            if path.exists() {
                removed.push((*name).to_string());
            } else {
                missing.push((*name).to_string());
            }
            continue;
        }
        match fs::remove_file(&path) {
            Ok(()) => removed.push((*name).to_string()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push((*name).to_string())
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to remove runtime state file {}", path.display())
                })
            }
        }
    }
    // Crash-leftover probe staging files have dynamic names; sweep by
    // shape. Failures are LOUD like the static list above — a reset that
    // reports success while leaving files behind is worse than one that
    // fails visibly.
    let entries = fs::read_dir(&state_dir)
        .with_context(|| format!("failed to list state dir {}", state_dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| {
            format!("failed to read state dir entry in {}", state_dir.display())
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let staged = (name.starts_with(".presence-probe.") || name.starts_with(".input-activity."))
            && name.ends_with(".tmp");
        if !staged {
            continue;
        }
        if dry_run {
            removed.push(name);
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed.push(name),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to remove probe staging file {}",
                        entry.path().display()
                    )
                })
            }
        }
    }
    let events_dir = state_dir.join("events");
    if events_dir.exists() {
        if dry_run {
            removed.push("events/".to_string());
        } else {
            fs::remove_dir_all(&events_dir).with_context(|| {
                format!("failed to remove hook spool dir {}", events_dir.display())
            })?;
            removed.push("events/".to_string());
        }
    }
    let logs_dir = state_dir.join("logs");
    if logs_dir.exists() {
        if dry_run {
            removed.push("logs/".to_string());
        } else {
            fs::remove_dir_all(&logs_dir)
                .with_context(|| format!("failed to remove logs dir {}", logs_dir.display()))?;
            removed.push("logs/".to_string());
        }
    }

    let removed_value: Value = removed.into();
    let missing_value: Value = missing.into();
    let removed_files = if dry_run {
        Value::Null
    } else {
        removed_value.clone()
    };
    let would_remove_files = if dry_run { removed_value } else { Value::Null };

    Ok(json!({
        "ok": true,
        "action": "reset",
        "dryRun": dry_run,
        "stateFolderPath": state_dir,
        "configPath": config_path,
        "configPreserved": config_preserved,
        "daemonStopped": daemon_stopped,
        "removedFiles": removed_files,
        "wouldRemoveFiles": would_remove_files,
        "missingFiles": missing_value,
        "keptFiles": json!(["config.json"]),
        "nextStep": if config_preserved {
            json!("Config was preserved. Run `tinyctb daemon start` to resume background delivery with fresh state.")
        } else {
            json!("No config found. Run `tinyctb setup` to configure the bridge.")
        }
    }))
}

fn projects_list_result(observed_limit: u64) -> Result<Value> {
    let config = load_daemon_config()?;
    let db_path = state_db_path()?;
    let conn = create_state_db(&db_path)?;
    let observed = observed_workspaces_from_db(&conn, observed_limit)?;
    let importable = importable_projects_from_observed(&observed, &config.projects);
    Ok(json!({
        "ok": true,
        "action": "projects_list",
        "configured": config.projects,
        "observed": observed.into_iter().map(|workspace| json!({
            "label": workspace.label,
            "cwd": workspace.cwd,
            "lastSeenAt": workspace.last_seen_at
        })).collect::<Vec<_>>(),
        "importable": importable
    }))
}

fn project_add_result(
    cwd: &str,
    id: Option<&str>,
    label: Option<&str>,
    aliases: &[String],
    dry_run: bool,
) -> Result<Value> {
    let mut config = load_daemon_config()?;
    let project = build_registered_project(cwd, id, label, aliases, &config.projects)?;
    if config
        .projects
        .iter()
        .any(|existing| existing.cwd == project.cwd)
    {
        bail!("project cwd `{}` is already registered", project.cwd);
    }
    if !dry_run {
        config.projects.push(project.clone());
        write_daemon_config(&config)?;
    }
    Ok(json!({
        "ok": true,
        "action": "projects_add",
        "dryRun": dry_run,
        "project": project
    }))
}

fn project_import_result(limit: u64, dry_run: bool) -> Result<Value> {
    let mut config = load_daemon_config()?;
    let db_path = state_db_path()?;
    let conn = create_state_db(&db_path)?;
    let observed = observed_workspaces_from_db(&conn, limit)?;
    let importable = importable_projects_from_observed(&observed, &config.projects);
    if !dry_run && !importable.is_empty() {
        config.projects.extend(importable.clone());
        write_daemon_config(&config)?;
    }
    Ok(json!({
        "ok": true,
        "action": "projects_import",
        "dryRun": dry_run,
        "imported": importable,
        "count": importable.len()
    }))
}

fn project_remove_result(id: &str, dry_run: bool) -> Result<Value> {
    let mut config = load_daemon_config()?;
    let Some(project) = config
        .projects
        .iter()
        .find(|project| project.id == id)
        .cloned()
    else {
        bail!("project `{id}` was not found");
    };
    if !dry_run {
        config.projects.retain(|candidate| candidate.id != id);
        write_daemon_config(&config)?;
    }
    Ok(json!({
        "ok": true,
        "action": "projects_remove",
        "dryRun": dry_run,
        "project": project
    }))
}

const DEFAULT_NOTIFICATION_EVENTS: &str = "thread_waiting,thread_completed";

fn redact_secret_text(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        text.to_string()
    } else {
        text.replace(secret, "<redacted>")
    }
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | ',' | ':' | '=')
        })
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn event_thread_id(event: &Value) -> Option<String> {
    event
        .get("threadId")
        .and_then(Value::as_str)
        .or_else(|| event.pointer("/thread/threadId").and_then(Value::as_str))
        .or_else(|| event.pointer("/thread/id").and_then(Value::as_str))
        .map(str::to_string)
}

fn notification_event_id(event: &Value) -> String {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("bridge_event");
    let thread_id = event_thread_id(event).unwrap_or_else(|| "unknown".to_string());
    let discriminator = event
        .get("eventKey")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            event
                .get("updatedAt")
                .and_then(Value::as_u64)
                .map(|value| value.to_string())
        })
        .or_else(|| {
            event
                .get("observedAt")
                .and_then(Value::as_u64)
                .map(|value| value.to_string())
        })
        .unwrap_or_else(|| {
            serde_json::to_string(event)
                .map(|raw| sha256_hex(raw.as_bytes()))
                .unwrap_or_else(|_| "event".to_string())
        });
    sanitize_delivery_id(&format!("claude:{event_type}:{thread_id}:{discriminator}"))
}

fn sanitize_delivery_id(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn sha256_hex(message: &[u8]) -> String {
    hex_lower(&Sha256::digest(message))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{pending_outbound_count, PendingPrompt};
    use std::fs;
    use std::path::PathBuf;

    struct TempStateDir {
        previous_state_dir: Option<String>,
        previous_service_dir: Option<String>,
        previous_cgroup_root: Option<String>,
        root: PathBuf,
    }

    impl TempStateDir {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!("tinyctb-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create temp state dir");
            let previous_state_dir = std::env::var("TINYCTB_STATE_DIR").ok();
            let previous_service_dir = std::env::var("TINYCTB_SERVICE_DIR").ok();
            let previous_cgroup_root = std::env::var("TINYCTB_CGROUP_ROOT").ok();
            std::env::set_var("TINYCTB_STATE_DIR", &root);
            // Keep service lookups away from the user's real systemd/launchd
            // definitions: `reset` stops a running service if it sees one.
            std::env::set_var("TINYCTB_SERVICE_DIR", root.join("service"));
            // And keep the teardown SWEEP away from the user's real turn
            // objects — `reset` kills and removes everything under the
            // owner root, which in a test must never be the live service
            // subtree. (The same isolation lesson as TINYCTB_SERVICE_DIR.)
            let cgroups = root.join("cgroups");
            fs::create_dir_all(&cgroups).expect("cgroup root");
            std::env::set_var("TINYCTB_CGROUP_ROOT", cgroups);
            Self {
                previous_state_dir,
                previous_service_dir,
                previous_cgroup_root,
                root,
            }
        }
    }

    impl Drop for TempStateDir {
        fn drop(&mut self) {
            if let Some(previous_state_dir) = &self.previous_state_dir {
                std::env::set_var("TINYCTB_STATE_DIR", previous_state_dir);
            } else {
                std::env::remove_var("TINYCTB_STATE_DIR");
            }
            if let Some(previous_service_dir) = &self.previous_service_dir {
                std::env::set_var("TINYCTB_SERVICE_DIR", previous_service_dir);
            } else {
                std::env::remove_var("TINYCTB_SERVICE_DIR");
            }
            if let Some(previous_cgroup_root) = &self.previous_cgroup_root {
                std::env::set_var("TINYCTB_CGROUP_ROOT", previous_cgroup_root);
            } else {
                std::env::remove_var("TINYCTB_CGROUP_ROOT");
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    /// Unreadable is not empty: a ledger that cannot be opened is no
    /// evidence that nothing is alive, and with KillMode=process a stopped
    /// daemon spares legacy descendants — teardown fail-closes.
    #[test]
    fn reset_fail_closes_on_an_unreadable_ledger() {
        let _guard = crate::state::test_env_lock();
        let state = TempStateDir::new("reset-unreadable");
        fs::write(state.root.join("state.db"), "not a database").expect("corrupt ledger");

        let err = reset_result(false).expect_err("an unreadable ledger must refuse");
        assert!(
            format!("{err}").contains("unreadable"),
            "the refusal names the reason: {err}"
        );
        assert!(
            state.root.join("state.db").exists(),
            "and nothing was deleted"
        );
    }

    /// A terminal legacy row still carrying a supervision marker is a
    /// group nobody proved empty — the sweep cannot see it (no cgroup),
    /// so the teardown must refuse on it too.
    #[test]
    fn reset_refuses_a_terminal_but_supervised_legacy_turn() {
        let _guard = crate::state::test_env_lock();
        let _state = TempStateDir::new("reset-legacy-terminal");
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        crate::state::register_bridge_turn(
            &conn,
            "turn-lt",
            "sess-lt",
            "/tmp/t.log",
            Some(4_500_000),
            None,
            None,
            Some(4_500_000),
            None,
            None,
            1000,
        )
        .expect("register");
        crate::state::mark_bridge_turn_finished(&conn, "turn-lt", "failed", 2000)
            .expect("terminal");
        crate::state::set_failure_cleanup_marker(&conn, "turn-lt").expect("marker");
        drop(conn);

        let err = reset_result(false).expect_err("an unproven legacy group must refuse");
        assert!(
            format!("{err}").contains("legacy-supervised"),
            "the refusal names the reason: {err}"
        );
    }

    /// A held spawn permit (a CLI or daemon mid-spawn) must block the
    /// teardown's exclusive freeze — through the PRODUCTION lock wiring.
    #[test]
    fn reset_waits_out_or_refuses_an_in_flight_spawn() {
        let _guard = crate::state::test_env_lock();
        let _state = TempStateDir::new("reset-inflight");
        let _permit = daemon::hold_spawn_lock_shared().expect("spawner permit");

        let err = reset_result(false).expect_err("an in-flight spawn must block the reset");
        assert!(
            format!("{err}").contains("spawn is still in flight"),
            "the refusal names the reason: {err}"
        );
    }

    /// Legacy-regime turns (no ownership object) are invisible to the
    /// sweep: while one is active the reset must refuse rather than delete
    /// the only ledger that knows about it.
    #[test]
    fn reset_refuses_while_a_legacy_turn_is_active() {
        let _guard = crate::state::test_env_lock();
        let _state = TempStateDir::new("reset-legacy");
        let conn = create_state_db(&state_db_path().expect("path")).expect("db");
        crate::state::register_bridge_turn(
            &conn,
            "turn-legacy",
            "sess-legacy",
            "/tmp/t.log",
            Some(4_400_000),
            None,
            None,
            Some(4_400_000),
            None,
            None,
            1000,
        )
        .expect("register");
        drop(conn);

        let err = reset_result(false).expect_err("an active legacy turn must block the reset");
        assert!(
            format!("{err}").contains("legacy-supervised"),
            "the refusal names the reason: {err}"
        );
        assert!(
            state_db_path().expect("path").exists(),
            "and the ledger survives"
        );
    }

    /// `reset` must REFUSE while any turn object cannot be proven empty:
    /// with KillMode=process the stopped service spares the sub-trees, and
    /// deleting the ledger under live work would strand it forever.
    #[test]
    fn reset_refuses_while_a_turn_object_cannot_be_proven_empty() {
        let _guard = crate::state::test_env_lock();
        let state = TempStateDir::new("reset-refuse");
        let stuck = state.root.join("cgroups/turn-stuck");
        fs::create_dir_all(&stuck).expect("stuck");
        fs::write(stuck.join("cgroup.events"), "populated 1\nfrozen 0\n").expect("events");

        let err = reset_result(false).expect_err("a live object must abort the reset");
        assert!(
            format!("{err}").contains("refusing to reset"),
            "the refusal names its reason: {err}"
        );
        assert!(
            state.root.join("cgroups/turn-stuck").exists(),
            "and the object is left in place"
        );
    }

    #[test]
    fn outbound_events_dedupe_retry_and_deliver_durably() {
        let conn = create_state_db_in_memory().expect("db");
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_1",
            "updatedAt": 42
        });

        assert!(enqueue_outbound_event(&conn, &event, 1000, "away").expect("enqueue"));
        assert!(
            !enqueue_outbound_event(&conn, &event, 1001, "away").expect("dedupe"),
            "same delivery id should not enqueue twice"
        );

        let failed =
            deliver_due_outbound_events(&conn, 1000, 10, None, |_| bail!("transport offline"))
                .expect("failed delivery summary");
        assert_eq!(
            failed,
            OutboxDeliverySummary {
                attempted: 1,
                delivered: 0,
                failed: 1
            }
        );
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 1);

        let delayed =
            deliver_due_outbound_events(&conn, 1000, 10, None, |_| Ok(json!({"ok": true})))
                .expect("not due summary");
        assert_eq!(delayed.attempted, 0);

        let delivered =
            deliver_due_outbound_events(&conn, 2000, 10, None, |_| Ok(json!({"ok": true})))
                .expect("delivered summary");
        assert_eq!(
            delivered,
            OutboxDeliverySummary {
                attempted: 1,
                delivered: 1,
                failed: 0
            }
        );
        assert_eq!(pending_outbound_count(&conn).expect("pending"), 0);
    }

    #[test]
    fn transport_delivery_log_tracks_each_transport_once() {
        let conn = create_state_db_in_memory().expect("db");

        assert!(
            transport_delivered_at(&conn, "event_1", "telegram")
                .expect("lookup")
                .is_none(),
            "transport should not start delivered"
        );
        record_transport_delivery(
            &conn,
            "event_1",
            "telegram",
            &json!({ "messageId": 111 }),
            1000,
        )
        .expect("record delivery");

        assert!(
            transport_delivered_at(&conn, "event_1", "telegram")
                .expect("lookup")
                .is_some(),
            "recorded transport should be treated as delivered"
        );
        assert!(
            transport_delivered_at(&conn, "event_1", "other")
                .expect("lookup")
                .is_none(),
            "other transports for the same event must remain pending"
        );
    }

    #[test]
    fn inbox_age_seconds_handles_mixed_timestamp_units() {
        let snapshot = BridgeThreadSnapshot {
            thread_id: "thr_recent".to_string(),
            name: None,
            cwd: Some("/tmp/project".to_string()),
            updated_at: Some(1_776_219_396),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: Some("completed".to_string()),
            last_preview: Some("preview".to_string()),
            pending_prompt: None,
            event_uid: None,
        };

        let item = classify_inbox_item(&snapshot, 1_776_219_400_000);

        assert_eq!(item.age_seconds, Some(4));
    }

    #[test]
    fn notification_event_id_is_stable_and_sanitized() {
        let event = json!({
            "type": "thread_completed",
            "threadId": "sess/with:odd chars",
            "updatedAt": 42
        });
        let id = notification_event_id(&event);
        assert_eq!(id, notification_event_id(&event));
        assert!(id.starts_with("claude-thread_completed"));
        assert!(id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')));
    }

    #[test]
    fn pending_prompt_snapshot_classifies_as_waiting_inbox_item() {
        let snapshot = BridgeThreadSnapshot {
            thread_id: "thr_wait".to_string(),
            name: None,
            cwd: Some("/tmp/project".to_string()),
            updated_at: Some(1000),
            status_type: "active".to_string(),
            status_flags: vec!["waitingOnUserInput".to_string()],
            last_turn_status: None,
            last_preview: Some("Can you confirm?".to_string()),
            pending_prompt: Some(PendingPrompt {
                prompt_id: "notify:1000".to_string(),
                kind: "reply".to_string(),
                status: "pending".to_string(),
                question: Some("Can you confirm?".to_string()),
                transcript_bytes: None,
                notification_type: None,
            }),
            event_uid: None,
        };

        let item = classify_inbox_item(&snapshot, 2000);
        assert_eq!(item.prompt_kind.as_deref(), Some("reply"));
        assert_eq!(item.question.as_deref(), Some("Can you confirm?"));
    }

    #[test]
    fn reset_dry_run_reports_runtime_files_and_preserves_config() {
        let _guard = crate::state::test_env_lock();
        let state = TempStateDir::new("reset-dry-run");
        write_daemon_config(&DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: "thread_waiting".to_string(),
            telegram: Some(TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            }),
            claude: Some(ClaudeConfig::default()),
            projects: Vec::new(),
        })
        .expect("write config");
        for name in [
            "state.db",
            "state.db-wal",
            "remote-mode.json",
            "presence-probe.json",
            "presence-probe.lock",
            "input-activity.json",
            ".presence-probe.4242.tmp",
            ".input-activity.4242.tmp",
        ] {
            fs::write(state.root.join(name), "runtime").expect("write runtime file");
        }
        fs::create_dir_all(state.root.join("events")).expect("events dir");
        fs::write(state.root.join("events/1-1-Stop.json"), "{}").expect("spool file");

        let result = reset_result(true).expect("reset dry run");

        assert_eq!(result["ok"], true);
        assert_eq!(result["action"], "reset");
        assert_eq!(result["dryRun"], true);
        assert_eq!(result["configPreserved"], true);
        let would_remove = result["wouldRemoveFiles"]
            .as_array()
            .expect("wouldRemoveFiles array");
        let names = would_remove
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert!(names.contains(&"state.db"));
        assert!(names.contains(&"state.db-wal"));
        assert!(names.contains(&"remote-mode.json"));
        // The keypress record and its staging files are runtime state too:
        // a reset that leaves them behind hands the next run a stale
        // "someone was at the keyboard" reading.
        assert!(
            names.contains(&"input-activity.json"),
            "dry run must report the keypress record: {names:?}"
        );
        assert!(
            names.contains(&".input-activity.4242.tmp"),
            "and its staging leftovers: {names:?}"
        );
        assert!(names.contains(&"presence-probe.json"));
        assert!(names.contains(&"presence-probe.lock"));
        assert!(names.contains(&".presence-probe.4242.tmp"));
        assert!(names.contains(&"events/"));
        assert!(result["removedFiles"].is_null());
        assert!(
            state.root.join("state.db").exists(),
            "dry run must not remove files"
        );
        assert!(
            state.root.join("config.json").exists(),
            "config must be preserved"
        );
    }

    #[test]
    fn reset_removes_runtime_state_and_preserves_config() {
        let _guard = crate::state::test_env_lock();
        let state = TempStateDir::new("reset-removes");
        write_daemon_config(&DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: "thread_waiting".to_string(),
            telegram: Some(TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            }),
            claude: Some(ClaudeConfig::default()),
            projects: Vec::new(),
        })
        .expect("write config");
        for name in [
            "state.db-wal",
            "state.db-shm",
            "remote-mode.json",
            "daemon.lock",
            "presence-probe.json",
            "presence-probe.lock",
            ".presence-probe.4242.tmp",
            "input-activity.json",
            ".input-activity.4242.tmp",
        ] {
            fs::write(state.root.join(name), "runtime").expect("write runtime file");
        }
        // A REAL (empty) ledger: reset now fail-closes on an unreadable
        // one, and that refusal has its own test.
        drop(create_state_db(&state.root.join("state.db")).expect("real ledger"));
        fs::create_dir_all(state.root.join("events")).expect("events dir");
        fs::write(state.root.join("events/1-1-Stop.json"), "{}").expect("spool file");
        fs::create_dir_all(state.root.join("logs")).expect("logs dir");
        fs::write(state.root.join("logs/daemon.out.log"), "log").expect("log file");

        let result = reset_result(false).expect("reset");

        assert_eq!(result["ok"], true);
        assert_eq!(result["configPreserved"], true);
        for name in [
            "state.db",
            "state.db-wal",
            "state.db-shm",
            "remote-mode.json",
            "daemon.lock",
            "presence-probe.json",
            "presence-probe.lock",
            ".presence-probe.4242.tmp",
            "input-activity.json",
            ".input-activity.4242.tmp",
            "events",
            "logs",
        ] {
            assert!(
                !state.root.join(name).exists(),
                "{name} should be removed by reset"
            );
        }
        assert!(
            state.root.join("config.json").exists(),
            "config must be preserved"
        );
        let removed = result["removedFiles"]
            .as_array()
            .expect("removedFiles array");
        let names = removed.iter().filter_map(Value::as_str).collect::<Vec<_>>();
        assert!(names.contains(&"state.db"));
        assert!(names.contains(&"events/"));
        assert!(result["wouldRemoveFiles"].is_null());
    }

    #[test]
    fn setup_report_aggregates_daemon_start_failure() {
        let report = assemble_setup_report(
            false,
            json!({"resolvedPath": "/usr/bin/claude", "source": "path"}),
            json!({"ok": true}),
            Some(json!({"ok": true, "action": "hooks_install"})),
            Some(json!({"ok": true, "action": "daemon_install"})),
            Some(json!({"ok": false, "action": "daemon_start", "error": "boom"})),
        );
        assert_eq!(
            report["ok"], false,
            "failed daemon start must fail the whole setup"
        );
        // Install steps that already succeeded stay in the report.
        assert_eq!(report["daemon"]["install"]["ok"], true);
        assert_eq!(report["hooks"]["ok"], true);
        assert_eq!(report["daemon"]["start"]["error"], "boom");
        assert!(report["nextStep"]
            .as_str()
            .unwrap()
            .contains("daemon failed to start"));
    }

    #[test]
    fn setup_report_ok_when_daemon_start_succeeds() {
        let report = assemble_setup_report(
            false,
            json!({"resolvedPath": "/usr/bin/claude", "source": "path"}),
            json!({"ok": true}),
            Some(json!({"ok": true})),
            Some(json!({"ok": true})),
            Some(json!({"ok": true, "verifiedRunning": true})),
        );
        assert_eq!(report["ok"], true);
    }

    #[test]
    fn setup_report_ok_when_daemon_start_skipped() {
        let report = assemble_setup_report(
            true,
            json!({"resolvedPath": "/usr/bin/claude", "source": "path"}),
            json!({"ok": true}),
            None,
            None,
            None,
        );
        assert_eq!(report["ok"], true, "no daemon start requested => setup ok");
        assert!(report["nextStep"]
            .as_str()
            .unwrap()
            .contains("without --dry-run"));
    }
}
