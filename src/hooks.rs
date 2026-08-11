//! Idempotent installation of tinyCTB hook entries into Claude Code's
//! `~/.claude/settings.json`. The hooks forward Stop / Notification /
//! SessionStart payloads into the daemon's spool directory via
//! `tinyctb hook-event`.

use anyhow::{Context, Result};
use serde_json::{json, Map, Value};
use std::env;
use std::fs;
use std::path::PathBuf;

use crate::claude::{claude_settings_path, events_spool_dir};
use crate::shell_quote;

pub(crate) const HOOKED_EVENTS: &[&str] = &["Stop", "Notification", "SessionStart"];
/// Raised right before Claude Code would show a permission prompt, so the
/// gate engages only for calls that actually need an answer. Installed
/// separately from the spool hooks: different subcommand, and a timeout long
/// enough to outlast a human reaching for their phone.
pub(crate) const APPROVAL_HOOK_EVENT: &str = "PermissionRequest";
const APPROVAL_HOOK_MARKER: &str = "approval-gate";
/// Answering an `AskUserQuestion` needs `PreToolUse` (no hook can take over
/// the question dialog), but the matcher pins it to that one tool so it does
/// not run on every tool call.
pub(crate) const QUESTION_HOOK_EVENT: &str = "PreToolUse";
const QUESTION_HOOK_MATCHER: &str = "AskUserQuestion";
const QUESTION_HOOK_MARKER: &str = "question-gate";
const HOOK_MARKER: &str = "hook-event";
const HOOK_TIMEOUT_SECONDS: u64 = 10;

fn resolve_bridge_binary_for_hooks(bridge_command: &str) -> Result<PathBuf> {
    let trimmed = bridge_command.trim();
    if trimmed.contains('/') {
        let path = PathBuf::from(trimmed);
        if path.is_absolute() {
            return Ok(path);
        }
        return Ok(env::current_dir()?.join(path));
    }
    if let Ok(path) = which::which(trimmed) {
        return Ok(path);
    }
    env::current_exe().context("failed to resolve the tinyctb binary path for hook installation")
}

pub(crate) fn hook_command_string(bridge_command: &str) -> Result<String> {
    let binary = resolve_bridge_binary_for_hooks(bridge_command)?;
    Ok(format!(
        "{} {}",
        shell_quote(&binary.display().to_string()),
        HOOK_MARKER
    ))
}

fn is_tinyctb_hook(entry: &Value) -> bool {
    entry
        .get("command")
        .and_then(Value::as_str)
        .map(|command| {
            command.contains("tinyctb")
                && (command.contains(HOOK_MARKER)
                    || command.contains(APPROVAL_HOOK_MARKER)
                    || command.contains(QUESTION_HOOK_MARKER))
        })
        .unwrap_or(false)
}

/// Hook timeout = the configured approval wait plus headroom, so Claude Code
/// never kills the gate while the answer is still in flight.
fn approval_hook_timeout_seconds() -> u64 {
    crate::config::read_daemon_config_raw()
        .ok()
        .flatten()
        .and_then(|config| config.claude)
        .map(|claude| claude.approval_timeout_seconds)
        .unwrap_or(300)
        .clamp(5, 3600)
        + 15
}

fn read_settings() -> Result<(PathBuf, Map<String, Value>)> {
    let path = claude_settings_path()?;
    let settings = if path.exists() {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read Claude settings at {}", path.display()))?;
        if raw.trim().is_empty() {
            Map::new()
        } else {
            serde_json::from_str::<Value>(&raw)
                .with_context(|| format!("failed to parse Claude settings at {}", path.display()))?
                .as_object()
                .cloned()
                .context("Claude settings.json must contain a JSON object")?
        }
    } else {
        Map::new()
    };
    Ok((path, settings))
}

fn write_settings(path: &PathBuf, settings: &Map<String, Value>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut contents = serde_json::to_string_pretty(&Value::Object(settings.clone()))?;
    contents.push('\n');
    fs::write(path, contents)?;
    Ok(())
}

/// Remove tinyctb entries from one event's hook groups, dropping groups that
/// become empty. Returns how many entries were removed.
fn strip_tinyctb_entries(groups: &mut Vec<Value>) -> usize {
    let mut removed = 0usize;
    groups.retain_mut(|group| {
        let Some(hooks) = group.get_mut("hooks").and_then(Value::as_array_mut) else {
            return true;
        };
        let before = hooks.len();
        hooks.retain(|entry| !is_tinyctb_hook(entry));
        removed += before - hooks.len();
        !hooks.is_empty()
    });
    removed
}

pub(crate) fn install_hooks(bridge_command: &str, dry_run: bool) -> Result<Value> {
    let command = hook_command_string(bridge_command)?;
    let (path, mut settings) = read_settings()?;
    let hooks_value = settings
        .entry("hooks".to_string())
        .or_insert_with(|| json!({}));
    let hooks = hooks_value
        .as_object_mut()
        .context("Claude settings hooks section must be a JSON object")?;

    let mut installed = Vec::new();
    for event in HOOKED_EVENTS {
        let groups_value = hooks.entry(event.to_string()).or_insert_with(|| json!([]));
        let groups = groups_value
            .as_array_mut()
            .with_context(|| format!("Claude settings hooks.{event} must be an array"))?;
        strip_tinyctb_entries(groups);
        groups.push(json!({
            "hooks": [{
                "type": "command",
                "command": command,
                "timeout": HOOK_TIMEOUT_SECONDS
            }]
        }));
        installed.push(event.to_string());
    }

    // The approval gate answers permission prompts from Telegram, so it must
    // outlive a human reaction time; the spool hooks stay short because they
    // only write a file.
    let approval_command = format!(
        "{} approval-gate",
        shell_quote(
            &resolve_bridge_binary_for_hooks(bridge_command)?
                .display()
                .to_string()
        )
    );
    let approval_timeout = approval_hook_timeout_seconds();
    let groups_value = hooks
        .entry(APPROVAL_HOOK_EVENT.to_string())
        .or_insert_with(|| json!([]));
    let groups = groups_value
        .as_array_mut()
        .with_context(|| format!("Claude settings hooks.{APPROVAL_HOOK_EVENT} must be an array"))?;
    strip_tinyctb_entries(groups);
    groups.push(json!({
        "matcher": "*",
        "hooks": [{
            "type": "command",
            "command": approval_command,
            "timeout": approval_timeout
        }]
    }));
    installed.push(APPROVAL_HOOK_EVENT.to_string());

    let question_command = format!(
        "{} question-gate",
        shell_quote(
            &resolve_bridge_binary_for_hooks(bridge_command)?
                .display()
                .to_string()
        )
    );
    let groups_value = hooks
        .entry(QUESTION_HOOK_EVENT.to_string())
        .or_insert_with(|| json!([]));
    let groups = groups_value
        .as_array_mut()
        .with_context(|| format!("Claude settings hooks.{QUESTION_HOOK_EVENT} must be an array"))?;
    strip_tinyctb_entries(groups);
    groups.push(json!({
        "matcher": QUESTION_HOOK_MATCHER,
        "hooks": [{
            "type": "command",
            "command": question_command,
            "timeout": approval_timeout
        }]
    }));
    installed.push(format!("{QUESTION_HOOK_EVENT}({QUESTION_HOOK_MATCHER})"));

    if !dry_run {
        write_settings(&path, &settings)?;
        fs::create_dir_all(events_spool_dir()?)?;
    }
    Ok(json!({
        "ok": true,
        "action": "hooks_install",
        "dryRun": dry_run,
        "settingsPath": path.display().to_string(),
        "command": command,
        "approvalCommand": approval_command,
        "questionCommand": question_command,
        "approvalTimeoutSeconds": approval_timeout,
        "events": installed,
        "note": "Restart running interactive Claude Code sessions so they pick up the new hooks."
    }))
}

pub(crate) fn uninstall_hooks(dry_run: bool) -> Result<Value> {
    let (path, mut settings) = read_settings()?;
    let mut removed = 0usize;
    if let Some(hooks) = settings.get_mut("hooks").and_then(Value::as_object_mut) {
        for event in HOOKED_EVENTS
            .iter()
            .chain(std::iter::once(&APPROVAL_HOOK_EVENT))
            .chain(std::iter::once(&QUESTION_HOOK_EVENT))
        {
            if let Some(groups) = hooks.get_mut(*event).and_then(Value::as_array_mut) {
                removed += strip_tinyctb_entries(groups);
            }
        }
        hooks.retain(|_, groups| {
            groups
                .as_array()
                .map(|groups| !groups.is_empty())
                .unwrap_or(true)
        });
    }
    if !dry_run && removed > 0 {
        write_settings(&path, &settings)?;
    }
    Ok(json!({
        "ok": true,
        "action": "hooks_uninstall",
        "dryRun": dry_run,
        "settingsPath": path.display().to_string(),
        "removed": removed
    }))
}

pub(crate) fn hooks_status() -> Result<Value> {
    let (path, settings) = read_settings()?;
    let mut events = Map::new();
    let mut missing = Vec::new();
    for event in HOOKED_EVENTS
        .iter()
        .chain(std::iter::once(&APPROVAL_HOOK_EVENT))
        .chain(std::iter::once(&QUESTION_HOOK_EVENT))
    {
        let installed = settings
            .get("hooks")
            .and_then(|hooks| hooks.get(*event))
            .and_then(Value::as_array)
            .map(|groups| {
                groups.iter().any(|group| {
                    group
                        .get("hooks")
                        .and_then(Value::as_array)
                        .map(|entries| entries.iter().any(is_tinyctb_hook))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if !installed {
            missing.push(event.to_string());
        }
        events.insert(event.to_string(), Value::Bool(installed));
    }
    let spool = events_spool_dir()?;
    Ok(json!({
        "ok": true,
        "action": "hooks_status",
        "settingsPath": path.display().to_string(),
        "settingsExists": path.exists(),
        "events": events,
        "installed": missing.is_empty(),
        "missing": missing,
        "spoolDir": spool.display().to_string(),
        "spoolDirExists": spool.exists()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct HookTestEnv {
        root: PathBuf,
    }

    impl HookTestEnv {
        fn new(name: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("tinyctb-hooks-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create hook test root");
            std::env::set_var(
                "TINYCTB_CLAUDE_SETTINGS_PATH",
                root.join("settings.json").display().to_string(),
            );
            std::env::set_var(
                "TINYCTB_STATE_DIR",
                root.join("state").display().to_string(),
            );
            Self { root }
        }
    }

    impl Drop for HookTestEnv {
        fn drop(&mut self) {
            std::env::remove_var("TINYCTB_CLAUDE_SETTINGS_PATH");
            std::env::remove_var("TINYCTB_STATE_DIR");
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn install_is_idempotent_and_preserves_foreign_hooks() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let env = HookTestEnv::new("idempotent");
        let settings_path = env.root.join("settings.json");
        fs::write(
            &settings_path,
            json!({
                "hooks": {
                    "Stop": [{
                        "hooks": [{ "type": "command", "command": "other-tool notify" }]
                    }]
                },
                "model": "opus"
            })
            .to_string(),
        )
        .expect("seed settings");

        install_hooks("/usr/local/bin/tinyctb", false).expect("install 1");
        install_hooks("/usr/local/bin/tinyctb", false).expect("install 2");

        let settings: Value =
            serde_json::from_str(&fs::read_to_string(&settings_path).expect("read settings"))
                .expect("parse settings");
        assert_eq!(settings["model"], "opus");
        let stop_groups = settings["hooks"]["Stop"].as_array().expect("stop groups");
        let ours = stop_groups
            .iter()
            .flat_map(|group| group["hooks"].as_array().cloned().unwrap_or_default())
            .filter(is_tinyctb_hook)
            .count();
        assert_eq!(ours, 1, "double install must not duplicate hook entries");
        let foreign = stop_groups
            .iter()
            .flat_map(|group| group["hooks"].as_array().cloned().unwrap_or_default())
            .filter(|entry| entry["command"] == "other-tool notify")
            .count();
        assert_eq!(foreign, 1, "foreign hooks must be preserved");
        for event in HOOKED_EVENTS {
            assert!(
                settings["hooks"][event].as_array().is_some(),
                "missing hooks for {event}"
            );
        }

        let status = hooks_status().expect("status");
        assert_eq!(status["installed"], true);
        // The approval gate is installed as its own PreToolUse entry, with a
        // timeout long enough to outlast a human tapping a Telegram button.
        // The question gate is pinned to AskUserQuestion so it does not run
        // on every tool call.
        let question = settings["hooks"][QUESTION_HOOK_EVENT][0].clone();
        assert_eq!(question["matcher"], QUESTION_HOOK_MATCHER);
        assert!(question["hooks"][0]["command"]
            .as_str()
            .expect("question command")
            .ends_with("question-gate"));

        let approval = settings["hooks"][APPROVAL_HOOK_EVENT][0]["hooks"][0].clone();
        assert!(approval["command"]
            .as_str()
            .expect("approval command")
            .ends_with("approval-gate"));
        assert!(
            approval["timeout"].as_u64().expect("timeout") >= 60,
            "approval hook must not be killed while waiting: {approval}"
        );

        let uninstalled = uninstall_hooks(false).expect("uninstall");
        assert_eq!(
            uninstalled["removed"],
            (HOOKED_EVENTS.len() + 2) as i64,
            "the spool hooks plus the PermissionRequest approval gate and the              PreToolUse question gate"
        );
        let settings: Value =
            serde_json::from_str(&fs::read_to_string(&settings_path).expect("read settings"))
                .expect("parse settings");
        let stop_groups = settings["hooks"]["Stop"].as_array().expect("stop groups");
        assert_eq!(stop_groups.len(), 1, "foreign hook group should survive");
        assert!(settings["hooks"].get("SessionStart").is_none());
    }
}
