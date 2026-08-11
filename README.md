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
- turns started **from Telegram** (a reply or `/new`) always push their answer
  back to Telegram, whether or not away mode is on — away only gates
  notifications about local terminal sessions

Linux (Ubuntu, systemd user service) is the primary platform. macOS (launchd
user agent) has experimental support, not yet verified on real hardware:
`daemon install` writes `~/Library/LaunchAgents/tinyctb.plist`, and
`daemon start` (which `setup` runs by default) loads or reloads it with
`launchctl bootout` followed by `launchctl bootstrap`, so an updated plist
always takes effect. On Linux the equivalent pair is `systemctl --user
daemon-reload && enable --now`.

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
3. **Live-session injection** — if the target session is still running, the
   reply is written straight into its unix messaging socket
   (`$XDG_RUNTIME_DIR/cc-socks/<pid>.sock`, one JSON line), so it appears in
   the terminal the user is sitting at. The bridge learns that socket from
   the session itself: hooks are children of the session process and inherit
   `CLAUDE_CODE_MESSAGING_SOCKET`, which `hook-event` records. Without this,
   a `--resume` of a live session forks the transcript — the terminal never
   sees the message and both branches edit the same files unaware.
4. **Headless CLI** (fallback, for idle or closed sessions) — replies from Telegram spawn a detached
   `claude -p "<text>" --resume <session-id> --output-format json
   --permission-mode <configured>`. `/new` generates a session UUID locally and
   passes `--session-id`, so reply routing works before the first token is
   produced. The injected text is prefixed with `telegram：` so it is
   recognizable inside the session transcript. Each turn's output goes to its
   own log under `~/.tinyctb/logs/turns/`, and **the answer pushed back to
   Telegram is read from that log** — never attributed from Stop hooks, so a
   session that is concurrently active in a terminal cannot mislabel its own
   output as the answer. (A message delivered by live injection has no such
   log: it joins the running session's own queue, so its answer is the next
   completion that session reports **after** the injection. That answer is
   pushed whatever the away switch or events filter say.) If the turn's process dies without producing a
   result, a failure notice (with the log tail) is pushed instead, and the
   chat shows a typing indicator while the turn is queued or running.

The daemon loop (systemd user service / macOS LaunchAgent): ingest spooled hook events → refresh
the session cache → process Telegram updates (commands + reply routing) →
refresh typing indicators → deliver queued notifications (with retry/backoff
and per-event dedupe in sqlite). Local-session notifications are enqueued only
in away mode; answers to bridge-initiated turns are always enqueued.

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
# one-step setup: Telegram pairing + hooks + daemon service (systemd / launchd)
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
- **回复路由**：Telegram 里对某条通知使用 Reply。若目标会话**仍在运行**（仅 Linux，
  见下），桥直接把消息写进它的 unix 消息 socket（`cc-socks/<pid>.sock`，一行 JSON），
  消息会出现在你正在用的终端里；socket 由会话自己上报（hook 是会话子进程，继承
  `CLAUDE_CODE_MESSAGING_SOCKET`）。会话已空闲/关闭时才回退到派生
  `claude -p --resume <会话ID>` 无头进程。注入文本带 `telegram：`
  前缀（转录内可辨识）；**无头回合的答案从该回合专属的 turn log 读回**（归属精确，
  目标会话即使同时在终端活跃也不会张冠李戴），排队/运行期间聊天窗口持续显示
  typing，进程崩溃无结果则推送响亮的失败通知。**直投的消息没有专属 log**——它进的是
  活跃会话自己的队列，答案即该会话在注入**之后**的下一次完成，同样无视 away 开关与
  events 过滤器推回。从 Telegram 发起的回合无论 away 开关与否都会推回答案；away
  只门控本地终端会话的通知。
- **直投仅限 Linux**：投递前必须证明该 socket 仍属于当初上报它的会话（路径含 pid，
  会话退出后可能被复用重绑），判据是 boot id + 该 pid 的 starttime ticks，均取自
  `/proc`。macOS 无从取证，因此一律 fail closed 退回无头 `--resume` 路径。
- **新会话**：`/new` 在项目注册表指定的目录下用本地生成的 UUID
  （`--session-id`）启动无头会话，因此确认消息可以立刻用于回复路由。
- **审批**：交互式终端里的权限确认只能在终端处理，Telegram 只做提醒；
  从 Telegram 发起的无头回合按 `permissionMode` 配置自主执行，不会卡住。

## Notes

- The session JSONL format is internal to Claude Code and may change between
  versions; the parser skips anything it does not recognize, and hooks (a
  stable, documented interface) carry the load-bearing signals.
- Killing a timed-out headless turn after a **daemon restart** requires the
  Linux process-identity chain (boot id + process group + starttime ticks).
  On macOS this cannot be verified, so such turns are reported and marked
  expired but deliberately NOT signalled — killing on weak identity risks
  hitting an innocent reused PID. (Turns owned by the current daemon process
  are killed and reaped normally on both platforms.)
- The Telegram bot token is stored in the local config only and redacted from
  command output. Use a bot dedicated to this bridge.
- `tinyctb doctor` is the fastest way to check that the claude binary, hooks,
  config, and daemon service are all healthy.
