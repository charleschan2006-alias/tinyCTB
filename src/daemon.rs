use anyhow::{bail, Context, Result};
use fs2::FileExt;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use crate::claude::{
    filter_watch_events, parse_event_filter, start_claude_watch_receiver,
    sync_state_from_sessions, watch_events_from_sync_result, watch_thread_error_event,
};
use crate::state::{
    create_state_db, deliver_due_outbound_events, enqueue_outbound_event, pending_outbound_count,
    prune_state_logs, record_transport_delivery, should_emit_for_away_window, state_db_path,
    transport_delivery_exists, OutboxDeliverySummary,
};
use crate::telegram::{
    deliver_telegram_event, process_telegram_updates, refresh_telegram_typing_indicators,
    telegram_set_my_commands,
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

fn acquire_daemon_lock() -> Result<DaemonLock> {
    let path = daemon_lock_path()?;
    let file = fs::OpenOptions::new()
        .create(true)
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
        Err(error) => Err(error)
            .with_context(|| format!("failed to probe daemon lock at {}", path.display())),
    }
}

fn event_observed_at(event: &Value) -> Option<u64> {
    event
        .get("updatedAt")
        .and_then(Value::as_u64)
        .or_else(|| event.get("observedAt").and_then(Value::as_u64))
        .or_else(|| event.pointer("/thread/updatedAt").and_then(Value::as_u64))
}

fn should_enqueue_daemon_notification(conn: &Connection, event: &Value) -> Result<bool> {
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

pub(crate) fn enqueue_daemon_notification_events(
    conn: &Connection,
    events: &[Value],
    now: u64,
) -> Result<usize> {
    let mut enqueued = 0usize;
    for event in events {
        if should_enqueue_daemon_notification(conn, event)?
            && enqueue_outbound_event(conn, event, now)?
        {
            enqueued += 1;
        }
    }
    Ok(enqueued)
}

fn away_notifications_enabled_from_status(away_status: &Value) -> bool {
    away_status.get("away").and_then(Value::as_bool) == Some(true)
}

fn away_notifications_enabled(conn: &Connection) -> Result<bool> {
    Ok(away_notifications_enabled_from_status(
        &crate::get_away_mode(conn)?,
    ))
}

fn deliver_outbound_events(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
    deadline: Instant,
) -> Result<OutboxDeliverySummary> {
    deliver_due_outbound_events(conn, now, 100, Some(deadline), |event| {
        let event_id = notification_event_id(event);
        let telegram = if let Some(telegram) = config.telegram.as_ref() {
                if transport_delivery_exists(conn, &event_id, "telegram")? {
                    json!({ "ok": true, "transport": "telegram", "skipped": "already_delivered" })
                } else {
                    let result = deliver_telegram_event(conn, telegram, event, now, timeout)?;
                    record_transport_delivery(conn, &event_id, "telegram", &result, now)?;
                    result
                }
            } else {
                Value::Null
            };
        Ok(json!({ "telegram": telegram }))
    })
}

fn daemon_cycle_budget(timeout: Duration) -> Duration {
    timeout.max(Duration::from_secs(5)).saturating_mul(3)
}

fn daemon_cycle(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let deadline = Instant::now() + daemon_cycle_budget(timeout);
    let filter = parse_event_filter(Some(&config.events));
    let telegram_updates = match config.telegram.as_ref() {
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
    let events = match sync_state_from_sessions(conn, config, now, 50, true) {
        Ok(sync_result) => watch_events_from_sync_result(&sync_result, filter.as_ref()),
        Err(error) => filter_watch_events(vec![watch_thread_error_event(&error)], filter.as_ref()),
    };
    let enqueued = enqueue_daemon_notification_events(conn, &events, now)?;
    let delivery = if away_notifications_enabled(conn)? {
        deliver_outbound_events(conn, config, now, timeout, deadline)?
    } else {
        OutboxDeliverySummary::default()
    };
    Ok(json!({
        "ok": true,
        "action": "daemon_cycle",
        "observed": events.len(),
        "enqueued": enqueued,
        "delivery": delivery,
        "telegramUpdates": telegram_updates,
        "pending": pending_outbound_count(conn)?
    }))
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

    let telegram_commands = config
        .telegram
        .as_ref()
        .map(|telegram| {
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
    loop {
        let config = match load_daemon_config() {
            Ok(config) => config,
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
        match daemon_cycle(&conn, &config, now, timeout) {
            Ok(result) => println!("{}", result),
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
        if let Some(rx) = watch_rx.as_ref() {
            rx.recv_timeout(Duration::from_millis(poll_interval));
        } else {
            thread::sleep(Duration::from_millis(poll_interval));
        }
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
        .map(|value| {
            let path = PathBuf::from(&value);
            if path.is_absolute() {
                value
            } else {
                env::current_dir()
                    .map(|cwd| cwd.join(&path).display().to_string())
                    .unwrap_or(value)
            }
        });
    if cfg!(target_os = "macos") {
        let launch_agents_dir = dirs::home_dir()
            .context("home directory is not available")?
            .join("Library")
            .join("LaunchAgents");
        return Ok(macos_launchd_spec(
            label,
            &service_bridge_command,
            &launch_agents_dir,
            &stdout_log,
            &stderr_log,
            &runtime_path,
            claude_bin_override.as_deref(),
        ));
    }
    if cfg!(target_os = "linux") {
        let unit_name = if label.ends_with(".service") {
            label.to_string()
        } else {
            format!("{label}.service")
        };
        let service_path = dirs::home_dir()
            .context("home directory is not available")?
            .join(".config")
            .join("systemd")
            .join("user")
            .join(&unit_name);
        let claude_bin_env = claude_bin_override
            .as_deref()
            .map(|value| format!("Environment=\"CLAUDE_BIN={}\"\n", systemd_escape_env(value)))
            .unwrap_or_default();
        let contents = format!(
            "[Unit]\nDescription=tinyCTB Claude Telegram bridge daemon\n\n[Service]\nType=simple\nEnvironment=\"PATH={}\"\n{}ExecStart={} daemon run\nRestart=always\nRestartSec=2\nStandardOutput=append:{}\nStandardError=append:{}\n\n[Install]\nWantedBy=default.target\n",
            systemd_escape_env(&runtime_path),
            claude_bin_env,
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
    let claude_bin_xml = claude_bin_override
        .map(|value| {
            format!(
                "        <key>CLAUDE_BIN</key>\n        <string>{}</string>\n",
                xml_escape(value)
            )
        })
        .unwrap_or_default();
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
        claude_bin_xml,
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

    #[test]
    fn daemon_install_dry_run_resolves_relative_bridge_command_for_services() {
        if !cfg!(any(target_os = "linux", target_os = "macos")) {
            return;
        }
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

    #[test]
    fn daemon_notification_policy_skips_events_before_away_started() {
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
    fn away_off_clears_pending_daemon_notifications() {
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
            assert!(bootout < bootstrap, "bootout must run before bootstrap: {command}");
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
        );

        assert!(spec.contents.contains("<string>/opt/a&amp;b/tinyctb</string>"));
        assert!(spec.contents.contains("<key>CLAUDE_BIN</key>"));
        assert!(spec
            .contents
            .contains("<string>/custom/&lt;claude&gt; &amp; bin</string>"));
        assert!(!spec.contents.contains("<string>/custom/<claude>"));
    }

    #[test]
    fn service_bridge_command_falls_back_to_current_exe_for_unknown_names() {
        let resolved =
            resolve_service_bridge_command("definitely-not-a-real-binary-tinyctb-test");
        assert!(
            std::path::Path::new(&resolved).is_absolute(),
            "service command must be absolute, got: {resolved}"
        );
    }
}
