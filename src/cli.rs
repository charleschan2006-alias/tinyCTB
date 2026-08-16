use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "tinyctb")]
#[command(version)]
#[command(about = "Control local Claude Code sessions and keep working through Telegram when away")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    #[command(
        about = "Configure Telegram delivery, Claude Code hooks, and the daemon service in one step"
    )]
    Setup {
        #[arg(long)]
        bot_token: Option<String>,
        #[arg(long)]
        chat_id: Option<String>,
        #[arg(long)]
        allowed_user_id: Option<String>,
        #[arg(long, default_value = crate::DEFAULT_NOTIFICATION_EVENTS)]
        events: String,
        #[arg(long, default_value = "tinyctb")]
        bridge_command: String,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        daemon_label: String,
        #[arg(long = "no-install-daemon", default_value_t = true, action = clap::ArgAction::SetFalse)]
        install_daemon: bool,
        #[arg(long = "no-start-daemon", default_value_t = true, action = clap::ArgAction::SetFalse)]
        start_daemon: bool,
        #[arg(long = "no-install-hooks", default_value_t = true, action = clap::ArgAction::SetFalse)]
        install_hooks: bool,
        #[arg(long, default_value_t = 60_000)]
        pair_timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Inspect the Claude Code and bridge setup")]
    Doctor,
    #[command(about = "Choose whether Claude may notify you remotely")]
    Away {
        #[command(subcommand)]
        command: AwayCommands,
    },
    #[command(about = "List recent Claude Code sessions")]
    Threads {
        #[arg(long, default_value_t = 25)]
        limit: u64,
    },
    #[command(about = "List sessions waiting for user attention")]
    Waiting {
        #[arg(long)]
        project: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: u64,
    },
    #[command(about = "List actionable session inbox rows")]
    Inbox {
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        attention: Option<String>,
        #[arg(long = "waiting-on")]
        waiting_on: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: u64,
    },
    #[command(about = "Run and manage the proactive notification daemon")]
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },
    #[command(about = "Configure direct Telegram delivery and reply routing")]
    Telegram {
        #[command(subcommand)]
        command: TelegramCommands,
    },
    #[command(about = "Manage Claude Code hook registration for tinyCTB")]
    Hooks {
        #[command(subcommand)]
        command: HookCommands,
    },
    #[command(
        hide = true,
        about = "Receive one Claude Code hook payload on stdin and spool it for the daemon"
    )]
    HookEvent,
    #[command(
        hide = true,
        about = "PermissionRequest hook: confirm a gated tool call from Telegram while away"
    )]
    ApprovalGate,
    #[command(
        hide = true,
        about = "PreToolUse hook: confirm a gated tool call inside a headless Telegram turn"
    )]
    HeadlessApprovalGate,
    #[command(
        hide = true,
        about = "PreToolUse hook: answer an AskUserQuestion from Telegram while away"
    )]
    QuestionGate,
    #[command(
        hide = true,
        about = "UserPromptSubmit hook: teach away-mode sessions to ask choices via AskUserQuestion"
    )]
    PromptContext,
    #[command(about = "Manage the curated project registry for Telegram-created sessions")]
    Projects {
        #[command(subcommand)]
        command: ProjectCommands,
    },
    #[command(hide = true, about = "Sync local Claude session state into the cache")]
    Sync {
        #[arg(long, default_value_t = 50)]
        limit: u64,
    },
    #[command(about = "Start a new Claude Code session in a project directory")]
    New {
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        message: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        prompt: Vec<String>,
    },
    #[command(about = "Show a session with derived bridge state and recent messages")]
    Show {
        thread_id: String,
        #[arg(long, default_value_t = 20)]
        messages: usize,
    },
    #[command(about = "Send a follow-up message into an existing Claude Code session")]
    Reply {
        thread_id: String,
        #[arg(long)]
        message: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        prompt: Vec<String>,
    },
    #[command(about = "Reset runtime state to initial (keeps config.json)")]
    Reset {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum AwayCommands {
    On,
    Off,
    Status,
}

#[derive(Subcommand, Debug)]
pub(crate) enum HookCommands {
    #[command(about = "Install Stop/Notification/SessionStart hooks into Claude settings")]
    Install {
        #[arg(long, default_value = "tinyctb")]
        bridge_command: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Remove tinyCTB hooks from Claude settings")]
    Uninstall {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Show hook installation status")]
    Status,
}

#[derive(Subcommand, Debug)]
pub(crate) enum DaemonCommands {
    #[command(about = "Run the proactive notification daemon")]
    Run {
        #[arg(long, default_value_t = false)]
        once: bool,
        #[arg(long = "poll-interval", default_value_t = 500)]
        poll_interval: u64,
        #[arg(long, default_value_t = 10000)]
        timeout_ms: u64,
    },
    #[command(about = "Install the proactive notification daemon as a user service")]
    Install {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
        #[arg(long, default_value = "tinyctb")]
        bridge_command: String,
    },
    #[command(about = "Uninstall the proactive notification daemon user service")]
    Uninstall {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
    #[command(about = "Start the proactive notification daemon user service")]
    Start {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
    #[command(about = "Stop the proactive notification daemon user service")]
    Stop {
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
    #[command(about = "Show proactive notification daemon status")]
    Status {
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
    #[command(about = "Show proactive notification daemon log paths")]
    Logs {
        #[arg(long, default_value = crate::DEFAULT_DAEMON_LABEL)]
        label: String,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum TelegramCommands {
    #[command(about = "Configure the bridge-owned Telegram bot transport")]
    Setup {
        #[arg(long)]
        bot_token: Option<String>,
        #[arg(long)]
        chat_id: Option<String>,
        #[arg(long)]
        allowed_user_id: Option<String>,
        #[arg(long, default_value = crate::DEFAULT_NOTIFICATION_EVENTS)]
        events: String,
        #[arg(long, default_value = "tinyctb")]
        bridge_command: String,
        #[arg(long, default_value_t = 60_000)]
        pair_timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Show direct Telegram transport configuration")]
    Status,
    #[command(about = "Send a test Telegram notification using the bridge config")]
    Test {
        #[arg(long, default_value = "tinyCTB Telegram bridge test")]
        message: String,
        #[arg(long, default_value_t = 10000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ProjectCommands {
    #[command(about = "List configured projects and observed workspace candidates")]
    List {
        #[arg(long, default_value_t = 10)]
        observed_limit: u64,
    },
    #[command(about = "Add one curated project to the registry")]
    Add {
        cwd: String,
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        label: Option<String>,
        #[arg(long = "alias")]
        aliases: Vec<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Import curated project entries from observed Claude workspaces")]
    Import {
        #[arg(long, default_value_t = 25)]
        limit: u64,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    #[command(about = "Remove one project from the curated registry")]
    Remove {
        id: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[test]
    fn cli_accepts_daemon_lifecycle_commands() {
        for args in [
            vec!["tinyctb", "daemon", "run", "--once"],
            vec![
                "tinyctb",
                "daemon",
                "install",
                "--dry-run",
                "--bridge-command",
                "tinyctb",
            ],
            vec!["tinyctb", "daemon", "start", "--dry-run"],
            vec!["tinyctb", "daemon", "stop", "--dry-run"],
            vec!["tinyctb", "daemon", "status"],
            vec!["tinyctb", "daemon", "logs"],
        ] {
            let parsed = Cli::try_parse_from(args.clone());
            assert!(
                parsed.is_ok(),
                "daemon command should parse: {args:?}: {parsed:?}"
            );
        }
    }

    #[test]
    fn cli_accepts_hook_commands() {
        for args in [
            vec!["tinyctb", "hooks", "install", "--dry-run"],
            vec!["tinyctb", "hooks", "uninstall", "--dry-run"],
            vec!["tinyctb", "hooks", "status"],
            vec!["tinyctb", "hook-event"],
        ] {
            let parsed = Cli::try_parse_from(args.clone());
            assert!(
                parsed.is_ok(),
                "hook command should parse: {args:?}: {parsed:?}"
            );
        }
    }

    #[test]
    fn cli_accepts_product_setup_command() {
        let parsed = Cli::try_parse_from([
            "tinyctb",
            "setup",
            "--bot-token",
            "123:abc",
            "--chat-id",
            "456",
            "--allowed-user-id",
            "789",
            "--no-install-daemon",
            "--no-start-daemon",
            "--no-install-hooks",
            "--dry-run",
        ])
        .expect("setup command should parse");
        match parsed.command {
            Commands::Setup {
                install_daemon,
                start_daemon,
                install_hooks,
                dry_run,
                ..
            } => {
                assert!(!install_daemon);
                assert!(!start_daemon);
                assert!(!install_hooks);
                assert!(dry_run);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn cli_accepts_reply_and_new_commands() {
        let reply = Cli::try_parse_from(["tinyctb", "reply", "sess-1", "--message", "ok"]);
        assert!(reply.is_ok(), "reply should parse: {reply:?}");
        let new = Cli::try_parse_from(["tinyctb", "new", "--cwd", "/tmp", "do", "the", "thing"]);
        assert!(new.is_ok(), "new should parse: {new:?}");
        let show = Cli::try_parse_from(["tinyctb", "show", "sess-1", "--messages", "5"]);
        assert!(show.is_ok(), "show should parse: {show:?}");
    }

    #[test]
    fn cli_rejects_removed_codex_era_commands() {
        for args in [
            vec!["tinyctb", "remote", "on"],
            vec!["tinyctb", "mcp"],
            vec!["tinyctb", "hermes", "install"],
            vec!["tinyctb", "fork", "sess-1"],
            vec!["tinyctb", "follow", "sess-1"],
            vec!["tinyctb", "watch", "--once"],
            vec!["tinyctb", "archive", "sess-1"],
            vec!["tinyctb", "approve", "sess-1", "approve"],
        ] {
            let parsed = Cli::try_parse_from(args.clone());
            assert!(
                parsed.is_err(),
                "removed command should not parse: {args:?}"
            );
        }
    }

    #[test]
    fn cli_exposes_version_and_command_descriptions() {
        let mut command = Cli::command();
        assert_eq!(command.get_version(), Some(env!("CARGO_PKG_VERSION")));

        let help = command.render_long_help().to_string();
        assert!(help.contains("Inspect the Claude Code and bridge setup"));
        assert!(help.contains("Configure Telegram delivery"));
        assert!(
            !help.contains("hook payload on stdin"),
            "hook-event stays hidden"
        );
    }

    #[test]
    fn cli_accepts_project_registry_commands() {
        for args in [
            vec!["tinyctb", "projects", "list"],
            vec![
                "tinyctb",
                "projects",
                "add",
                "/home/user/projects/tinyCTB",
                "--id",
                "bridge",
                "--label",
                "tinyCTB",
                "--alias",
                "ctb",
            ],
            vec!["tinyctb", "projects", "import", "--limit", "10"],
            vec!["tinyctb", "projects", "remove", "bridge", "--dry-run"],
        ] {
            let parsed = Cli::try_parse_from(args.clone());
            assert!(
                parsed.is_ok(),
                "projects command should parse: {args:?}: {parsed:?}"
            );
        }
    }
}
