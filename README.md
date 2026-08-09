# tinyCTB

**Claude Code × Telegram away-mode bridge.** A port of
[codex-telegram-bridge](https://github.com/HanifCarroll/codex-telegram-bridge)
that replaces the Codex backend with local **Claude Code** sessions.

[English](#how-it-works) | [简体中文](#原理简述)

The product rule is simple:

- while you are at your computer, nothing is sent remotely
- send `/away` to your Telegram bot before leaving: from then on, every Claude
  Code session that finishes a response (or waits for your permission) pushes a
  notification to Telegram
- use Telegram's **Reply** action on a notification to send a follow-up into
  that exact session — a detached `claude -p --resume` continues it, and the
  answer comes back to Telegram
- `/back` stops remote notifications and clears the pending queue

Linux (Ubuntu, systemd user service) is the supported platform. macOS support
is a planned extension.

## How it works

There is no long-running model server to manage. tinyCTB glues three Claude
Code surfaces together:

1. **Hooks** — `tinyctb setup` registers `Stop`, `Notification`, and
   `SessionStart` hooks in `~/.claude/settings.json`. Each hook pipes its JSON
   payload into `tinyctb hook-event`, which spools it under
   `~/.tinyctb/events/`. This is how the daemon learns "Claude finished" /
   "Claude is waiting for you" — for interactive terminal sessions and headless
   bridge-started turns alike.
2. **Session transcripts** — `~/.claude/projects/*/<session-id>.jsonl` are
   scanned (defensively; the format is not a stable API) for `/threads`
   listings: title, working directory, and the last assistant answer.
3. **Headless CLI** — replies from Telegram spawn a detached
   `claude -p "<text>" --resume <session-id> --output-format json
   --permission-mode <configured>`. `/new` generates a session UUID locally and
   passes `--session-id`, so reply routing works before the first token is
   produced. Turn output is logged to `~/.tinyctb/logs/turns/`.

The daemon loop (systemd user service): ingest spooled hook events → refresh
the session cache → process Telegram updates (commands + reply routing) →
refresh typing indicators → deliver queued notifications (away mode only, with
retry/backoff and per-event dedupe in sqlite).

Permission prompts inside an interactive terminal session can only be answered
in that terminal — tinyCTB notifies you about them but deliberately has no
remote Approve/Deny buttons. Headless turns started from Telegram run with the
configured `permissionMode` (default `bypassPermissions`) and never block.

## Install

```bash
cargo build --release
cargo install --path .
```

## Quick start

```bash
# one-step setup: Telegram pairing + hooks + systemd daemon
tinyctb setup --bot-token <telegram-bot-token>

# verify everything
tinyctb doctor
tinyctb telegram test --message "tinyCTB is ready"
```

Send `/start` to your bot during setup to pair the chat automatically (or pass
`--chat-id` / `--allowed-user-id` for non-interactive setup).

Restart any interactive Claude Code session that was already running so it
picks up the new hooks.

Then, when leaving your computer, send `/away` to the bot. When you return,
send `/back`.

### Telegram commands

| Command | Effect |
|---|---|
| `/away` | enable remote notifications (verifies hooks + claude binary first) |
| `/back` | disable notifications, clear the pending queue |
| `/status` | away state, backend health, waiting sessions |
| `/threads [n]` | one message per recent session; reply to any of them to continue it |
| `/new <prompt>` | start a new session in the current project |
| `/project [id]` | list / switch the project used by `/new` |
| `/repair` | re-install hooks and re-check the claude binary |

### CLI surface

`setup`, `doctor`, `away on|off|status`, `threads`, `waiting`, `inbox`,
`show <id>`, `reply <id> -m ...`, `new --cwd ... <prompt>`,
`projects list|add|import|remove`, `hooks install|uninstall|status`,
`daemon run|install|start|stop|status|logs`, `telegram setup|status|test`,
`reset`.

All state lives in `~/.tinyctb/` (`config.json` mode 0600, `state.db`,
`events/` spool, `logs/`). `tinyctb reset` wipes runtime state but keeps the
config.

## Configuration

`~/.tinyctb/config.json` — see
[examples/config.example.json](examples/config.example.json).

- `claude.permissionMode` — permission mode for headless turns started from
  Telegram: `bypassPermissions` (default), `acceptEdits`, `dontAsk`, `plan`,
  `default`. With anything stricter than `bypassPermissions`, a headless turn
  that hits a denied action reports what it could not do instead of finishing
  the task.
- `claude.sessionScanLimit` — how many recent session transcripts each daemon
  cycle rescans (default 50).
- `CLAUDE_BIN` — overrides claude binary resolution (authoritative: if set and
  broken, tinyctb errors instead of falling back).
- `TINYCTB_STATE_DIR` — overrides `~/.tinyctb` (mainly for tests).

## 原理简述

- **通知源**：Claude Code 的 `Stop` / `Notification` / `SessionStart` hooks 把
  事件写进 `~/.tinyctb/events/` spool，daemon 轮询消化，away 模式下推送 Telegram。
- **回复路由**：Telegram 里对某条通知使用 Reply，桥会派生一个独立的
  `claude -p --resume <会话ID>` 无头进程续写该会话；答案由该进程触发的 Stop
  hook 事件送回 Telegram。
- **新会话**：`/new` 在项目注册表指定的目录下用本地生成的 UUID
  （`--session-id`）启动无头会话，因此确认消息可以立刻用于回复路由。
- **审批**：交互式终端里的权限确认只能在终端处理，Telegram 只做提醒；
  从 Telegram 发起的无头回合按 `permissionMode` 配置自主执行，不会卡住。

## Notes

- The session JSONL format is internal to Claude Code and may change between
  versions; the parser skips anything it does not recognize, and hooks (a
  stable, documented interface) carry the load-bearing signals.
- The Telegram bot token is stored in the local config only and redacted from
  command output. Use a bot dedicated to this bridge.
- `tinyctb doctor` is the fastest way to check that the claude binary, hooks,
  config, and daemon service are all healthy.
