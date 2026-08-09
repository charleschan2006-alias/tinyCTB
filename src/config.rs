use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::path::PathBuf;

use crate::state_dir_path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DaemonConfig {
    pub(crate) version: u32,
    pub(crate) bridge_command: String,
    pub(crate) events: String,
    #[serde(default)]
    pub(crate) telegram: Option<TelegramConfig>,
    #[serde(default)]
    pub(crate) claude: Option<ClaudeConfig>,
    #[serde(default)]
    pub(crate) projects: Vec<RegisteredProject>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClaudeConfig {
    #[serde(default = "default_permission_mode")]
    pub(crate) permission_mode: String,
    #[serde(default = "default_session_scan_limit")]
    pub(crate) session_scan_limit: u64,
}

fn default_permission_mode() -> String {
    "bypassPermissions".to_string()
}

fn default_session_scan_limit() -> u64 {
    50
}

impl Default for ClaudeConfig {
    fn default() -> Self {
        ClaudeConfig {
            permission_mode: default_permission_mode(),
            session_scan_limit: default_session_scan_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TelegramConfig {
    pub(crate) bot_token: String,
    pub(crate) chat_id: String,
    #[serde(default)]
    pub(crate) allowed_user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RegisteredProject {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) cwd: String,
    #[serde(default)]
    pub(crate) aliases: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TelegramSetupOptions<'a> {
    pub(crate) bot_token: Option<&'a str>,
    pub(crate) chat_id: Option<&'a str>,
    pub(crate) allowed_user_id: Option<&'a str>,
    pub(crate) events: &'a str,
    pub(crate) bridge_command: &'a str,
    pub(crate) dry_run: bool,
    pub(crate) pair_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SetupOptions<'a> {
    pub(crate) bot_token: Option<&'a str>,
    pub(crate) chat_id: Option<&'a str>,
    pub(crate) allowed_user_id: Option<&'a str>,
    pub(crate) events: &'a str,
    pub(crate) bridge_command: &'a str,
    pub(crate) daemon_label: &'a str,
    pub(crate) install_daemon: bool,
    pub(crate) start_daemon: bool,
    pub(crate) install_hooks: bool,
    pub(crate) dry_run: bool,
    pub(crate) pair_timeout_ms: u64,
}

pub(crate) fn daemon_config_path() -> Result<PathBuf> {
    Ok(state_dir_path()?.join("config.json"))
}

pub(crate) fn merged_daemon_config(
    existing: Option<&DaemonConfig>,
    bridge_command: &str,
    events: &str,
    telegram: TelegramConfig,
    claude: ClaudeConfig,
) -> DaemonConfig {
    DaemonConfig {
        version: 1,
        bridge_command: bridge_command.to_string(),
        events: events.to_string(),
        telegram: Some(telegram),
        claude: Some(claude),
        projects: existing
            .map(|config| config.projects.clone())
            .unwrap_or_default(),
    }
}

pub(crate) fn write_daemon_config(config: &DaemonConfig) -> Result<PathBuf> {
    let path = daemon_config_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, serde_json::to_vec_pretty(config)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(path)
}

const VALID_PERMISSION_MODES: &[&str] = &[
    "default",
    "acceptEdits",
    "plan",
    "dontAsk",
    "bypassPermissions",
];

pub(crate) fn load_daemon_config() -> Result<DaemonConfig> {
    let path = daemon_config_path()?;
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("daemon config not found at {}", path.display()))?;
    let mut config: DaemonConfig = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse daemon config at {}", path.display()))?;
    if config.claude.is_none() {
        config.claude = Some(ClaudeConfig::default());
    }
    if config.events.trim().is_empty() {
        bail!("daemon config events cannot be empty");
    }
    if let Some(telegram) = config.telegram.as_ref() {
        if telegram.bot_token.trim().is_empty() {
            bail!("daemon config telegram.botToken cannot be empty");
        }
        if telegram.chat_id.trim().is_empty() {
            bail!("daemon config telegram.chatId cannot be empty");
        }
    }
    if config.telegram.is_none() {
        bail!("daemon config must include Telegram configuration. Run `tinyctb setup`.");
    }
    let claude = config.claude.as_ref().expect("claude config defaulted");
    if !VALID_PERMISSION_MODES.contains(&claude.permission_mode.as_str()) {
        bail!(
            "daemon config claude.permissionMode must be one of {:?}",
            VALID_PERMISSION_MODES
        );
    }
    for project in &config.projects {
        if project.id.trim().is_empty() {
            bail!("daemon config project id cannot be empty");
        }
        if project.label.trim().is_empty() {
            bail!("daemon config project label cannot be empty");
        }
        if project.cwd.trim().is_empty() {
            bail!("daemon config project cwd cannot be empty");
        }
    }
    Ok(config)
}

pub(crate) fn redacted_daemon_config(config: &DaemonConfig) -> Value {
    json!({
        "version": config.version,
        "bridgeCommand": config.bridge_command,
        "events": config.events,
        "claude": config.claude.as_ref().map(|claude| json!({
            "permissionMode": claude.permission_mode,
            "sessionScanLimit": claude.session_scan_limit
        })),
        "telegram": config.telegram.as_ref().map(|telegram| json!({
            "botToken": "<redacted>",
            "chatId": telegram.chat_id,
            "allowedUserId": telegram.allowed_user_id
        })),
        "projects": config.projects.iter().map(|project| json!({
            "id": project.id,
            "label": project.label,
            "cwd": project.cwd,
            "aliases": project.aliases
        })).collect::<Vec<_>>()
    })
}

pub(crate) fn read_daemon_config_raw() -> Result<Option<DaemonConfig>> {
    let path = daemon_config_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read daemon config at {}", path.display()))?;
    let mut config: DaemonConfig = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse daemon config at {}", path.display()))?;
    if config.claude.is_none() {
        config.claude = Some(ClaudeConfig::default());
    }
    Ok(Some(config))
}

pub(crate) fn resolve_telegram_bot_token(explicit: Option<&str>) -> Result<String> {
    explicit
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            env::var("TELEGRAM_BOT_TOKEN")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .context(
            "Telegram bot token is required. Pass --bot-token or set TELEGRAM_BOT_TOKEN after creating a bot with @BotFather.",
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;

    fn config_test_lock() -> &'static Mutex<()> {
        crate::state::test_env_lock()
    }

    struct ConfigBackup {
        path: std::path::PathBuf,
        contents: Option<Vec<u8>>,
    }

    impl ConfigBackup {
        fn capture() -> anyhow::Result<Self> {
            let path = daemon_config_path()?;
            let contents = fs::read(&path).ok();
            Ok(Self { path, contents })
        }
    }

    impl Drop for ConfigBackup {
        fn drop(&mut self) {
            match &self.contents {
                Some(contents) => {
                    if let Some(parent) = self.path.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    let _ = fs::write(&self.path, contents);
                }
                None => {
                    if self.path.exists() {
                        let _ = fs::remove_file(&self.path);
                    }
                }
            }
        }
    }

    #[test]
    fn merged_daemon_config_preserves_existing_projects() {
        let merged = merged_daemon_config(
            Some(&DaemonConfig {
                version: 1,
                bridge_command: "old-bridge".to_string(),
                events: "thread_waiting".to_string(),
                telegram: Some(TelegramConfig {
                    bot_token: "old".to_string(),
                    chat_id: "old-chat".to_string(),
                    allowed_user_id: None,
                }),
                claude: Some(ClaudeConfig::default()),
                projects: vec![RegisteredProject {
                    id: "bridge".to_string(),
                    label: "tinyCTB".to_string(),
                    cwd: "/home/user/tinyCTB".to_string(),
                    aliases: vec!["ctb".to_string()],
                }],
            }),
            "tinyctb",
            crate::DEFAULT_NOTIFICATION_EVENTS,
            TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            },
            ClaudeConfig::default(),
        );

        assert_eq!(merged.version, 1);
        assert_eq!(merged.projects.len(), 1);
        assert_eq!(merged.projects[0].id, "bridge");
        assert_eq!(merged.telegram.as_ref().unwrap().chat_id, "456");
        let claude = merged.claude.as_ref().expect("claude config");
        assert_eq!(claude.permission_mode, "bypassPermissions");
    }

    #[test]
    fn load_daemon_config_defaults_claude_config() {
        let _guard = config_test_lock().lock().expect("config lock");
        let _backup = ConfigBackup::capture().expect("capture config backup");
        write_daemon_config(&DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: Some(TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            }),
            claude: None,
            projects: vec![],
        })
        .expect("write config");

        let loaded = load_daemon_config().expect("load config");
        assert_eq!(loaded.claude, Some(ClaudeConfig::default()));
    }

    #[test]
    fn load_daemon_config_rejects_invalid_permission_mode() {
        let _guard = config_test_lock().lock().expect("config lock");
        let _backup = ConfigBackup::capture().expect("capture config backup");
        write_daemon_config(&DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: Some(TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: None,
            }),
            claude: Some(ClaudeConfig {
                permission_mode: "yolo".to_string(),
                session_scan_limit: 50,
            }),
            projects: vec![],
        })
        .expect("write config");

        let error = load_daemon_config().expect_err("load should reject bad permission mode");
        assert!(
            format!("{error:#}").contains("permissionMode"),
            "unexpected error: {error:#}"
        );
    }
}
