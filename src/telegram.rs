mod api;
pub(crate) mod render;

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::thread;
use std::time::{Duration, Instant};

use crate::claude::{
    normalized_message, resolve_claude_binary, send_user_message, set_away_mode,
    start_thread_in_cwd, sync_state_from_sessions,
};
use crate::hooks::{hooks_status, install_hooks};
use crate::projects::{resolve_new_thread_request, resolve_project_query};
use crate::state::{
    delete_setting, get_setting_number, get_telegram_current_project_id,
    insert_telegram_command_route, insert_telegram_message_route,
    list_recent_thread_snapshots_from_db, lookup_telegram_command_route,
    lookup_telegram_message_route, mark_telegram_callback_route_used,
    mark_telegram_command_route_used, observed_workspaces_from_db, record_action,
    record_telegram_inbound_processed, set_setting, set_setting_text,
    set_telegram_current_project_id, telegram_inbound_processed,
    update_telegram_callback_message_id, ApprovalAnswer, BridgeThreadSnapshot,
    TelegramCallbackAction, TelegramCommandRouteKind, TelegramInboundLogContext,
};
#[cfg(test)]
use crate::ClaudeConfig;
use crate::{
    daemon_config_path, load_daemon_config, merged_daemon_config, read_daemon_config_raw,
    redacted_daemon_config, resolve_telegram_bot_token, write_daemon_config, DaemonConfig,
    RegisteredProject, TelegramConfig, TelegramSetupOptions,
};

use self::api::{
    telegram_answer_callback_query, telegram_bot_commands, telegram_chat_id,
    telegram_delete_webhook, telegram_from_user_id, telegram_get_updates, telegram_message_id,
    telegram_send_chat_action, telegram_send_message, telegram_send_text,
    telegram_send_text_message_id, telegram_updates_array,
};
use self::render::{
    prepare_telegram_delivery, prepare_telegram_thread_snapshot_delivery, telegram_help_text,
    telegram_new_thread_confirmation_text, telegram_project_text, telegram_projects_text,
    telegram_status_text, PreparedTelegramDelivery,
};

pub(crate) use self::api::{telegram_bot_id, telegram_set_my_commands};

#[derive(Debug, Clone, PartialEq, Eq)]
enum TelegramInboundCommand {
    Start,
    Help,
    Away,
    Back,
    Repair,
    Status,
    Threads(Option<String>),
    NewThread(Option<String>),
    Project(Option<String>),
    Unknown(String),
}

const DEFAULT_TELEGRAM_THREADS_LIMIT: u64 = 5;
const MAX_TELEGRAM_THREADS_LIMIT: u64 = 25;
const TELEGRAM_TYPING_TTL_MS: u64 = 120_000;
const TELEGRAM_TYPING_REFRESH_MS: u64 = 4_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutedTelegramCommandPromptReply {
    pub(crate) kind: TelegramCommandRouteKind,
    pub(crate) message: String,
    pub(crate) project_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutedTelegramReply {
    pub(crate) thread_id: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutedTelegramCallback {
    pub(crate) callback_query_id: String,
    pub(crate) callback_id: String,
    pub(crate) thread_id: String,
    pub(crate) action: TelegramCallbackAction,
    pub(crate) approval_id: Option<String>,
}

pub(crate) fn telegram_setup_result(options: TelegramSetupOptions<'_>) -> Result<Value> {
    let bot_token = resolve_telegram_bot_token(options.bot_token)?;
    let events = options.events.trim();
    let bridge_command = options.bridge_command.trim();
    if events.is_empty() {
        bail!("telegram setup events cannot be empty");
    }
    if bridge_command.is_empty() {
        bail!("telegram setup bridge command cannot be empty");
    }
    if !options.dry_run {
        telegram_delete_webhook(&bot_token, Duration::from_secs(10))
            .context("failed to clear existing Telegram webhook before enabling long polling")?;
    }

    let pair_hint = json!({
        "message": "Send /start to the Telegram bot to pair this chat automatically.",
        "timeoutMs": options.pair_timeout_ms
    });
    let paired = if let Some(chat_id) = options.chat_id.map(str::trim).filter(|v| !v.is_empty()) {
        TelegramConfig {
            bot_token: bot_token.clone(),
            chat_id: chat_id.to_string(),
            allowed_user_id: options
                .allowed_user_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
        }
    } else if options.dry_run {
        TelegramConfig {
            bot_token: bot_token.clone(),
            chat_id: "<paired by /start>".to_string(),
            allowed_user_id: options
                .allowed_user_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
        }
    } else {
        discover_telegram_pairing(&bot_token, options.pair_timeout_ms)?
    };

    let existing = read_daemon_config_raw()?;
    let claude = existing
        .as_ref()
        .and_then(|config| config.claude.clone())
        .unwrap_or_default();
    let config = merged_daemon_config(
        existing.as_ref(),
        bridge_command,
        events,
        paired.clone(),
        claude,
    );
    let commands = telegram_bot_commands();
    let commands_registration = if options.dry_run {
        json!({ "registered": false, "dryRun": true, "commands": commands })
    } else {
        json!({
            "registered": true,
            "commands": commands,
            "response": telegram_set_my_commands(&paired, Duration::from_secs(10))
                .context("failed to register Telegram slash commands")?
        })
    };

    let config_path = if options.dry_run {
        daemon_config_path()?
    } else {
        write_daemon_config(&config)?
    };

    Ok(json!({
        "ok": true,
        "action": "telegram_setup",
        "dryRun": options.dry_run,
        "configPath": config_path.display().to_string(),
        "telegram": {
            "configured": true,
            "botToken": "<redacted>",
            "chatId": paired.chat_id,
            "allowedUserId": paired.allowed_user_id,
            "pairing": if options.chat_id.is_some() { Value::Null } else { pair_hint },
            "commands": commands_registration
        },
        "config": redacted_daemon_config(&config),
        "daemonCommand": crate::daemon_run_command(bridge_command),
        "daemonInstallCommand": format!(
            "{} daemon install --bridge-command {}",
            crate::shell_quote(bridge_command),
            crate::shell_quote(bridge_command)
        ),
        "nextStep": "Install and start the daemon. Send /away to the Telegram bot before leaving; replies and new sessions run through headless Claude Code."
    }))
}

pub(crate) fn telegram_status_result() -> Result<Value> {
    let config = read_daemon_config_raw()?;
    let telegram = config.as_ref().and_then(|config| config.telegram.as_ref());
    Ok(json!({
        "ok": true,
        "action": "telegram_status",
        "configPath": daemon_config_path()?.display().to_string(),
        "configured": telegram.is_some(),
        "config": config.as_ref().map(redacted_daemon_config)
    }))
}

#[cfg(not(test))]
fn send_telegram_command_text(
    telegram: &TelegramConfig,
    text: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_send_text(telegram, text, timeout)
}

#[cfg(test)]
fn send_telegram_command_text(
    _telegram: &TelegramConfig,
    text: &str,
    _timeout: Duration,
) -> Result<Value> {
    Ok(json!({
        "ok": true,
        "result": {
            "message_id": 1,
            "text": text
        }
    }))
}

pub(crate) fn telegram_test_result(
    message: &str,
    timeout: Duration,
    dry_run: bool,
) -> Result<Value> {
    let config = load_daemon_config()?;
    let telegram = config
        .telegram
        .as_ref()
        .context("Telegram is not configured. Run telegram setup first.")?;
    let text = normalized_message(Some(message))
        .unwrap_or_else(|| "tinyCTB Telegram bridge test".to_string());
    let payload = json!({
        "chat_id": telegram.chat_id,
        "text": text,
        "disable_web_page_preview": true
    });
    if dry_run {
        return Ok(json!({
            "ok": true,
            "action": "telegram_test",
            "dryRun": true,
            "payload": payload
        }));
    }
    let sent = telegram_send_message(telegram, &payload, timeout)?;
    Ok(json!({
        "ok": true,
        "action": "telegram_test",
        "dryRun": false,
        "messageId": sent.pointer("/result/message_id").cloned().unwrap_or(Value::Null)
    }))
}

fn discover_telegram_pairing(bot_token: &str, timeout_ms: u64) -> Result<TelegramConfig> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms.max(1));
    let mut offset = None;
    while std::time::Instant::now() < deadline {
        let updates = telegram_get_updates(bot_token, offset, 10, Duration::from_secs(15))?;
        for update in telegram_updates_array(&updates)? {
            if let Some(update_id) = update.get("update_id").and_then(Value::as_i64) {
                offset = Some(update_id.saturating_add(1));
            }
            if let Some(message) = update.get("message") {
                let text = message.get("text").and_then(Value::as_str).unwrap_or("");
                if text.trim() != "/start" {
                    continue;
                }
                let chat_id = telegram_chat_id(message)
                    .context("Telegram /start update did not include chat.id")?;
                let allowed_user_id = telegram_from_user_id(message);
                return Ok(TelegramConfig {
                    bot_token: bot_token.to_string(),
                    chat_id,
                    allowed_user_id,
                });
            }
        }
        thread::sleep(Duration::from_millis(500));
    }
    bail!("Timed out waiting for Telegram /start. Send /start to the bot and rerun telegram setup.")
}

fn telegram_authorized(
    telegram: &TelegramConfig,
    chat_id: Option<&str>,
    user_id: Option<&str>,
) -> bool {
    if chat_id != Some(telegram.chat_id.as_str()) {
        return false;
    }
    match telegram.allowed_user_id.as_deref() {
        Some(allowed) => user_id == Some(allowed),
        None => true,
    }
}

fn parse_telegram_command_text(text: &str) -> Option<TelegramInboundCommand> {
    let trimmed = text.trim();
    if !trimmed.starts_with('/') {
        return None;
    }
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let raw_command = parts.next().unwrap_or_default();
    let rest = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let command = raw_command
        .split_once('@')
        .map(|(name, _)| name)
        .unwrap_or(raw_command)
        .to_ascii_lowercase();
    match command.as_str() {
        "/start" => Some(TelegramInboundCommand::Start),
        "/help" => Some(TelegramInboundCommand::Help),
        "/away" => Some(TelegramInboundCommand::Away),
        "/back" => Some(TelegramInboundCommand::Back),
        "/repair" => Some(TelegramInboundCommand::Repair),
        "/status" => Some(TelegramInboundCommand::Status),
        "/threads" => Some(TelegramInboundCommand::Threads(rest.map(str::to_string))),
        "/new" => Some(TelegramInboundCommand::NewThread(rest.map(str::to_string))),
        "/project" => Some(TelegramInboundCommand::Project(rest.map(str::to_string))),
        _ => Some(TelegramInboundCommand::Unknown(raw_command.to_string())),
    }
}

fn parse_telegram_threads_limit(raw: Option<&str>) -> Result<u64> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(DEFAULT_TELEGRAM_THREADS_LIMIT);
    };
    let limit = raw.parse::<u64>().with_context(|| {
        format!(
            "Use /threads or /threads <count>, with count between 1 and {MAX_TELEGRAM_THREADS_LIMIT}"
        )
    })?;
    if !(1..=MAX_TELEGRAM_THREADS_LIMIT).contains(&limit) {
        bail!(
            "Use /threads or /threads <count>, with count between 1 and {MAX_TELEGRAM_THREADS_LIMIT}"
        );
    }
    Ok(limit)
}

fn extract_telegram_command(
    message: &Value,
    telegram: &TelegramConfig,
) -> Result<Option<TelegramInboundCommand>> {
    let chat_id = telegram_chat_id(message);
    let user_id = telegram_from_user_id(message);
    if !telegram_authorized(telegram, chat_id.as_deref(), user_id.as_deref()) {
        return Ok(None);
    }
    if message.get("reply_to_message").is_some() {
        return Ok(None);
    }
    Ok(message
        .get("text")
        .and_then(Value::as_str)
        .and_then(parse_telegram_command_text))
}

pub(crate) fn extract_telegram_reply_route(
    conn: &Connection,
    message: &Value,
    telegram: &TelegramConfig,
) -> Result<Option<RoutedTelegramReply>> {
    let chat_id = telegram_chat_id(message);
    let user_id = telegram_from_user_id(message);
    if !telegram_authorized(telegram, chat_id.as_deref(), user_id.as_deref()) {
        return Ok(None);
    }
    let reply_message_id = message
        .get("reply_to_message")
        .and_then(telegram_message_id);
    let Some(reply_message_id) = reply_message_id else {
        return Ok(None);
    };
    let text = message
        .get("text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let Some(text) = text else {
        return Ok(None);
    };
    let Some(chat_id) = chat_id else {
        return Ok(None);
    };
    let thread_id = lookup_telegram_message_route(conn, &chat_id, reply_message_id)?;
    Ok(thread_id.map(|thread_id| RoutedTelegramReply {
        thread_id,
        message: text.to_string(),
    }))
}

pub(crate) fn extract_telegram_command_prompt_reply(
    conn: &Connection,
    message: &Value,
    telegram: &TelegramConfig,
) -> Result<Option<RoutedTelegramCommandPromptReply>> {
    let chat_id = telegram_chat_id(message);
    let user_id = telegram_from_user_id(message);
    if !telegram_authorized(telegram, chat_id.as_deref(), user_id.as_deref()) {
        return Ok(None);
    }
    let Some(chat_id) = chat_id else {
        return Ok(None);
    };
    let Some(reply_message_id) = message
        .get("reply_to_message")
        .and_then(telegram_message_id)
    else {
        return Ok(None);
    };
    let Some(message_text) = message
        .get("text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let Some((kind, payload)) = lookup_telegram_command_route(conn, &chat_id, reply_message_id)?
    else {
        return Ok(None);
    };
    Ok(Some(RoutedTelegramCommandPromptReply {
        kind,
        message: message_text.to_string(),
        project_id: payload
            .as_ref()
            .and_then(|value| value.get("projectId"))
            .and_then(Value::as_str)
            .map(str::to_string),
    }))
}

pub(crate) fn extract_telegram_callback_route(
    conn: &Connection,
    callback_query: &Value,
    telegram: &TelegramConfig,
) -> Result<Option<RoutedTelegramCallback>> {
    let message = callback_query.get("message");
    let chat_id = message.and_then(telegram_chat_id);
    let user_id = callback_query
        .get("from")
        .and_then(|from| from.get("id"))
        .and_then(|value| {
            value
                .as_i64()
                .map(|id| id.to_string())
                .or_else(|| value.as_str().map(str::to_string))
        });
    if !telegram_authorized(telegram, chat_id.as_deref(), user_id.as_deref()) {
        return Ok(None);
    }
    let callback_query_id = callback_query
        .get("id")
        .and_then(Value::as_str)
        .context("callback query missing id")?;
    let Some(callback_id) = callback_query
        .get("data")
        .and_then(Value::as_str)
        .and_then(|data| data.strip_prefix("claude:"))
    else {
        return Ok(None);
    };
    let route = conn
        .query_row(
            "SELECT thread_id, action, approval_id FROM telegram_callback_routes
             WHERE callback_id = ?1 AND used_at IS NULL",
            params![callback_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()?;
    Ok(route.and_then(|(thread_id, action, approval_id)| {
        TelegramCallbackAction::from_str(&action).map(|action| RoutedTelegramCallback {
            callback_query_id: callback_query_id.to_string(),
            callback_id: callback_id.to_string(),
            thread_id,
            action,
            approval_id,
        })
    }))
}

pub(crate) fn deliver_telegram_event(
    conn: &Connection,
    telegram: &TelegramConfig,
    event: &Value,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let mut prepared = prepare_telegram_delivery(&telegram.chat_id, event)?;
    deliver_prepared_telegram_delivery(conn, telegram, &mut prepared, now, timeout)
}

fn deliver_prepared_telegram_delivery(
    conn: &Connection,
    telegram: &TelegramConfig,
    prepared: &mut PreparedTelegramDelivery,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let mut message_ids = Vec::with_capacity(prepared.payloads.len());
    for payload in &prepared.payloads {
        let response = telegram_send_message(telegram, payload, timeout)?;
        let message_id = response
            .pointer("/result/message_id")
            .and_then(Value::as_i64)
            .context("Telegram sendMessage response missing result.message_id")?;
        if let Some(thread_id) = prepared.thread_id.as_deref() {
            insert_telegram_message_route(
                conn,
                &telegram.chat_id,
                message_id,
                thread_id,
                &prepared.event_id,
                now,
            )?;
        }
        message_ids.push(message_id);
    }
    let first_message_id = *message_ids
        .first()
        .context("Telegram delivery did not send any messages")?;
    for route in &mut prepared.callback_routes {
        route.message_id = Some(first_message_id);
        update_telegram_callback_message_id(conn, &route.callback_id, first_message_id)?;
    }
    if let Some(thread_id) = prepared.thread_id.as_deref() {
        clear_telegram_typing_indicator(conn, telegram, thread_id)?;
    }
    Ok(json!({
        "ok": true,
        "transport": "telegram",
        "messageId": first_message_id,
        "messageIds": message_ids,
        "chunks": prepared.payloads.len(),
        "threadId": prepared.thread_id,
        "callbacks": prepared.callback_routes.len()
    }))
}

/// Feedback for an authorized message that matched no route. Plain text never
/// reaches a session on its own — say so loudly instead of dropping it.
fn unrouted_message_hint_text(message: &Value) -> String {
    match message
        .get("text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        Some(text) => format!(
            "This message isn't routed to any Claude session, so nothing happened.\n\nTo continue a session: use Telegram's Reply action on one of its messages (list them with /threads).\nTo start a new session with it, send:\n/new {}",
            render::trim_for_telegram_line(text, 200)
        ),
        None => "Only text messages can reach Claude sessions. Use /threads to list sessions, or /new <prompt> to start one.".to_string(),
    }
}

/// Register a spawned headless turn so the daemon watches its log for the
/// answer (see `process_bridge_turns`). Answers to bridge-initiated turns are
/// always pushed back, away mode or not.
fn register_bridge_turn_from_result(conn: &Connection, result: &Value, now: u64) -> Result<()> {
    let thread_id = result
        .get("threadId")
        .and_then(Value::as_str)
        .context("headless turn result missing threadId")?;
    let turn_id = result
        .pointer("/claude/turnId")
        .and_then(Value::as_str)
        .context("headless turn result missing claude.turnId")?;
    let log_path = result
        .pointer("/claude/logPath")
        .and_then(Value::as_str)
        .context("headless turn result missing claude.logPath")?;
    let pid = result
        .pointer("/claude/pid")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok());
    crate::state::register_bridge_turn(
        conn,
        turn_id,
        thread_id,
        log_path,
        pid,
        result.pointer("/claude/procStart").and_then(Value::as_str),
        // The RESOLVED executable of the spawned process (/proc/<pid>/exe),
        // not the configured binary path — restart-kill compares it exactly.
        result.pointer("/claude/procExe").and_then(Value::as_str),
        result
            .pointer("/claude/pgid")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok()),
        result
            .pointer("/claude/procStartTicks")
            .and_then(Value::as_str),
        result.pointer("/claude/procBootId").and_then(Value::as_str),
        now,
    )
}

fn telegram_typing_key(chat_id: &str, thread_id: &str) -> String {
    format!("telegram_typing:{chat_id}:{thread_id}")
}

fn register_telegram_typing_indicator(
    conn: &Connection,
    telegram: &TelegramConfig,
    thread_id: &str,
    now: u64,
) -> Result<()> {
    set_setting_text(
        conn,
        &telegram_typing_key(&telegram.chat_id, thread_id),
        &json!({
            "chatId": telegram.chat_id,
            "threadId": thread_id,
            "until": now + TELEGRAM_TYPING_TTL_MS,
            "nextAt": now
        })
        .to_string(),
    )
}

/// Keep an existing typing indicator alive without resetting its send cadence
/// (used every daemon cycle while a bridge turn is queued or running, so long
/// waits stay visible in the chat). Registers a fresh one if none exists.
pub(crate) fn extend_telegram_typing_indicator(
    conn: &Connection,
    telegram: &TelegramConfig,
    thread_id: &str,
    now: u64,
) -> Result<()> {
    let key = telegram_typing_key(&telegram.chat_id, thread_id);
    let Some(raw) = crate::state::get_setting_text(conn, &key)? else {
        return register_telegram_typing_indicator(conn, telegram, thread_id, now);
    };
    let Ok(mut value) = serde_json::from_str::<Value>(&raw) else {
        return register_telegram_typing_indicator(conn, telegram, thread_id, now);
    };
    if let Some(object) = value.as_object_mut() {
        object.insert("until".to_string(), json!(now + TELEGRAM_TYPING_TTL_MS));
    }
    set_setting_text(conn, &key, &value.to_string())
}

pub(crate) fn refresh_telegram_typing_indicators(
    conn: &Connection,
    telegram: &TelegramConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let rows = crate::state::list_settings_with_prefix(conn, "telegram_typing:")?;
    let mut active = 0usize;
    let mut sent = 0usize;
    let mut expired = 0usize;
    let mut failed = 0usize;
    for (key, raw) in rows {
        let value: Value = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(_) => {
                delete_setting(conn, &key)?;
                expired += 1;
                continue;
            }
        };
        let chat_id = value.get("chatId").and_then(Value::as_str);
        if chat_id != Some(telegram.chat_id.as_str()) {
            continue;
        }
        let until = value.get("until").and_then(Value::as_u64).unwrap_or(0);
        if until <= now {
            delete_setting(conn, &key)?;
            expired += 1;
            continue;
        }
        active += 1;
        let next_at = value.get("nextAt").and_then(Value::as_u64).unwrap_or(0);
        if next_at > now {
            continue;
        }
        match telegram_send_chat_action(telegram, "typing", timeout) {
            Ok(_) => {
                sent += 1;
                set_setting_text(
                    conn,
                    &key,
                    &json!({
                        "chatId": telegram.chat_id,
                        "threadId": value.get("threadId").cloned().unwrap_or(Value::Null),
                        "until": until,
                        "nextAt": now + TELEGRAM_TYPING_REFRESH_MS
                    })
                    .to_string(),
                )?;
            }
            Err(_) => failed += 1,
        }
    }
    Ok(json!({
        "ok": failed == 0,
        "transport": "telegram",
        "active": active,
        "sent": sent,
        "expired": expired,
        "failed": failed
    }))
}

fn clear_telegram_typing_indicator(
    conn: &Connection,
    telegram: &TelegramConfig,
    thread_id: &str,
) -> Result<()> {
    delete_setting(conn, &telegram_typing_key(&telegram.chat_id, thread_id))
}

fn backend_log_context_from_result<'a>(
    result: &'a Value,
    thread_id: Option<&'a str>,
    route_message_id: Option<i64>,
) -> TelegramInboundLogContext<'a> {
    TelegramInboundLogContext {
        thread_id,
        route_message_id,
        result_action: result.get("action").and_then(Value::as_str),
        backend_transport: result.pointer("/claude/transport").and_then(Value::as_str),
        backend_pid: result
            .pointer("/claude/pid")
            .and_then(Value::as_u64)
            .and_then(|value| {
                if value <= u32::MAX as u64 {
                    Some(value as u32)
                } else {
                    None
                }
            }),
    }
}

fn send_claude_reply_to_thread(
    conn: &Connection,
    config: &DaemonConfig,
    thread_id: &str,
    message: &str,
    now: u64,
) -> Result<Value> {
    let cwd_hint: Option<String> = conn
        .query_row(
            "SELECT cwd FROM threads_cache WHERE thread_id = ?1",
            params![thread_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    // The `telegram：` prefix makes remote-injected messages recognizable in
    // the session transcript (and tells the model the user is on the phone).
    let prefixed = format!("telegram：{message}");
    // Prefer delivering into the session while it is LIVE: a headless
    // `--resume` would fork the transcript from the state it saw at spawn,
    // so the terminal the user is sitting at would never see the message and
    // the two branches would edit the same files unaware of each other.
    let live_socket = crate::state::session_messaging_socket(conn, thread_id)?;
    let injected = match live_socket.as_ref() {
        Some(socket) => crate::claude::inject_into_live_session(
            &socket.path,
            (socket.inode, socket.boot_id.clone()),
            &prefixed,
        )
        .unwrap_or(false)
        .then(|| socket.path.clone()),
        None => None,
    };
    if injected.is_some() {
        // The live session now owes an answer to Telegram; it is claimed by
        // that session's next completion (there is no per-turn log to read,
        // unlike the headless path).
        crate::state::record_live_injection(conn, thread_id, now)?;
    }
    let result = match injected.as_deref() {
        Some(socket) => json!({
            "ok": true,
            "action": "reply_injected",
            "threadId": thread_id,
            "message": prefixed,
            "cwd": cwd_hint,
            "claude": {
                "transport": "live-session-socket",
                "socket": socket
            },
            "delivery": {
                "mode": "live_injection",
                "status": "delivered_to_live_session"
            },
            "sentAt": now
        }),
        None => {
            let result = send_user_message(config, thread_id, &prefixed, cwd_hint.as_deref(), now)?;
            register_bridge_turn_from_result(conn, &result, now)?;
            result
        }
    };
    record_action(
        conn,
        thread_id,
        "telegram_reply",
        json!({
            "message": message,
            "result": result.clone(),
            "sentAt": now
        }),
        now,
    )?;
    if let Some(telegram) = config.telegram.as_ref() {
        register_telegram_typing_indicator(conn, telegram, thread_id, now)?;
        let _ = refresh_telegram_typing_indicators(conn, telegram, now, Duration::from_secs(5));
    }
    let mut reply = result;
    if let Some(object) = reply.as_object_mut() {
        object.insert("action".to_string(), json!("telegram_reply"));
    }
    Ok(reply)
}

fn current_project_for_identity<'a>(
    config: &'a DaemonConfig,
    conn: &Connection,
    chat_id: &str,
    user_id: Option<&str>,
) -> Result<Option<&'a RegisteredProject>> {
    if let Some(project_id) = get_telegram_current_project_id(conn, chat_id, user_id)? {
        if let Some(project) = config
            .projects
            .iter()
            .find(|project| project.id == project_id)
        {
            return Ok(Some(project));
        }
    }
    if config.projects.len() == 1 {
        return Ok(config.projects.first());
    }
    Ok(None)
}

fn start_new_thread_from_telegram(
    conn: &Connection,
    config: &DaemonConfig,
    project: &RegisteredProject,
    message: &str,
    now: u64,
) -> Result<Value> {
    let result = start_thread_in_cwd(config, Some(&project.cwd), Some(message), now)?;
    let thread_id = result
        .get("threadId")
        .and_then(Value::as_str)
        .context("new Claude session result missing threadId")?
        .to_string();
    register_bridge_turn_from_result(conn, &result, now)?;
    record_action(
        conn,
        &thread_id,
        "telegram_new_thread",
        json!({
            "projectId": project.id,
            "projectLabel": project.label,
            "cwd": project.cwd,
            "message": message,
            "result": result.clone(),
            "sentAt": now
        }),
        now,
    )?;
    // Register the freshly started session so /threads and reply routing see it
    // before the first hook event arrives.
    crate::state::upsert_thread_snapshot(
        conn,
        &BridgeThreadSnapshot {
            thread_id: thread_id.clone(),
            name: None,
            cwd: Some(project.cwd.clone()),
            updated_at: Some(now),
            status_type: "active".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        },
        now,
    )?;
    if let Some(telegram) = config.telegram.as_ref() {
        register_telegram_typing_indicator(conn, telegram, &thread_id, now)?;
        let _ = refresh_telegram_typing_indicators(conn, telegram, now, Duration::from_secs(5));
    }
    Ok(result)
}

fn send_new_thread_confirmation(
    conn: &Connection,
    telegram: &TelegramConfig,
    project: &RegisteredProject,
    result: &Value,
    timeout: Duration,
    now: u64,
) -> Result<Value> {
    let thread_id = result
        .get("threadId")
        .and_then(Value::as_str)
        .context("new thread result missing threadId")?;
    let text = telegram_new_thread_confirmation_text(project, result)?;
    let message_id = telegram_send_text_message_id(telegram, &text, timeout)?;
    insert_telegram_message_route(
        conn,
        &telegram.chat_id,
        message_id,
        thread_id,
        &format!("telegram_new_thread:{thread_id}"),
        now,
    )?;
    Ok(json!({
        "ok": true,
        "action": "telegram_new_thread_confirmation",
        "threadId": thread_id,
        "messageId": message_id
    }))
}

fn send_new_thread_prompt_for_project(
    conn: &Connection,
    telegram: &TelegramConfig,
    project: &RegisteredProject,
    timeout: Duration,
    now: u64,
) -> Result<Value> {
    let text = format!(
        "What should Claude work on in {}?\n{}\n\nUse Telegram's Reply action on this message with the prompt for the new session.",
        project.label, project.cwd
    );
    let message_id = telegram_send_text_message_id(telegram, &text, timeout)?;
    insert_telegram_command_route(
        conn,
        &telegram.chat_id,
        message_id,
        TelegramCommandRouteKind::NewThread,
        Some(&json!({ "projectId": project.id })),
        now,
    )?;
    Ok(json!({
        "ok": true,
        "action": "telegram_new_thread_prompt",
        "projectId": project.id,
        "messageId": message_id
    }))
}

/// Verify the Claude Code backend is usable and (re)install hooks when
/// missing. This is the /away and /repair backbone: without a shared server
/// process there is nothing to spawn, so backend health means "claude binary
/// resolves + hooks installed".
fn ensure_claude_backend(config: &DaemonConfig, force_reinstall: bool) -> Result<Value> {
    let binary = resolve_claude_binary()?;
    let status = hooks_status()?;
    let installed = status.get("installed").and_then(Value::as_bool) == Some(true);
    let install_result = if !installed || force_reinstall {
        Some(install_hooks(&config.bridge_command, false)?)
    } else {
        None
    };
    Ok(json!({
        "ok": true,
        "binary": binary.path.display().to_string(),
        "binarySource": binary.source,
        "hooksWereInstalled": installed,
        "hooksInstall": install_result,
        "action": if force_reinstall { "repaired" } else if installed { "reused" } else { "installed" }
    }))
}

fn telegram_backend_text(title: &str, backend: &Value, away: &Value) -> String {
    let away_state = if away.get("away").and_then(Value::as_bool) == Some(true) {
        "on"
    } else {
        "off"
    };
    format!(
        "{title}\nBackend: hooks {action}, claude binary ok.\nAway mode: {away_state}.",
        action = backend
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
    )
}

fn telegram_backend_failure_text(title: &str, error: &anyhow::Error) -> String {
    format!("{title}\nError: {error:#}\nTry /repair. If that keeps failing, check `tinyctb doctor` locally.")
}

fn telegram_backend_failure_result(
    telegram: &TelegramConfig,
    action: &str,
    title: &str,
    error: anyhow::Error,
    timeout: Duration,
) -> Result<Value> {
    let message = telegram_backend_failure_text(title, &error);
    let sent = send_telegram_command_text(telegram, &message, timeout)?;
    Ok(json!({
        "ok": false,
        "action": action,
        "error": format!("{error:#}"),
        "sent": sent
    }))
}

fn execute_away_command(
    conn: &Connection,
    telegram: &TelegramConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let config = load_daemon_config()?;
    let backend = ensure_claude_backend(&config, false)?;
    let away = set_away_mode(conn, true, now)?;
    let message = telegram_backend_text("Remote Claude mode is on.", &backend, &away);
    let sent = send_telegram_command_text(telegram, &message, timeout)?;
    Ok(json!({
        "ok": true,
        "action": "telegram_away",
        "backend": backend,
        "away": away,
        "sent": sent
    }))
}

fn execute_repair_command(
    conn: &Connection,
    telegram: &TelegramConfig,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let config = load_daemon_config()?;
    let backend = ensure_claude_backend(&config, true)?;
    let away = set_away_mode(conn, true, now)?;
    let message = telegram_backend_text("Remote Claude mode was repaired.", &backend, &away);
    let sent = send_telegram_command_text(telegram, &message, timeout)?;
    Ok(json!({
        "ok": true,
        "action": "telegram_repair",
        "backend": backend,
        "away": away,
        "sent": sent
    }))
}

fn telegram_threads_limit_error_text(error: &anyhow::Error) -> String {
    format!(
        "{error:#}\n\nExamples:\n/threads\n/threads 10\n\nThe maximum is {MAX_TELEGRAM_THREADS_LIMIT}."
    )
}

fn telegram_threads_failure_text(error: &anyhow::Error) -> String {
    format!("I couldn't fetch recent Claude sessions.\nError: {error:#}\n\nTry /repair if the backend looks broken.")
}

fn execute_threads_command(
    conn: &Connection,
    telegram: &TelegramConfig,
    raw_limit: Option<String>,
    now: u64,
    timeout: Duration,
    deadline: Option<Instant>,
) -> Result<Value> {
    let limit = match parse_telegram_threads_limit(raw_limit.as_deref()) {
        Ok(limit) => limit,
        Err(error) => {
            let text = telegram_threads_limit_error_text(&error);
            let sent = telegram_send_text(telegram, &text, timeout)?;
            return Ok(json!({
                "ok": true,
                "action": "telegram_threads_invalid_limit",
                "error": format!("{error:#}"),
                "sent": sent
            }));
        }
    };

    let config = load_daemon_config()?;
    sync_state_from_sessions(conn, &config, now, limit, false)?;
    let snapshots = list_recent_thread_snapshots_from_db(conn, limit)?;
    if snapshots.is_empty() {
        let sent = telegram_send_text(
            telegram,
            "No recent Claude sessions are cached yet. Open Claude Code locally or try again after the daemon syncs.",
            timeout,
        )?;
        return Ok(json!({
            "ok": true,
            "action": "telegram_threads_empty",
            "limit": limit,
            "sent": sent
        }));
    }

    let mut sent = Vec::with_capacity(snapshots.len());
    let mut render_failed = 0usize;
    for snapshot in &snapshots {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        // ONLY a rendering failure is isolated: it is specific to this one
        // snapshot while the transport still works, so report it in place and
        // keep listing the rest.
        let mut prepared =
            match prepare_telegram_thread_snapshot_delivery(&telegram.chat_id, snapshot) {
                Ok(prepared) => prepared,
                Err(error) => {
                    render_failed += 1;
                    let notice = format!(
                        "⚠️ Could not render session {}: {error:#}",
                        snapshot.thread_id.chars().take(8).collect::<String>()
                    );
                    // Propagate if even this cannot be sent — that is a
                    // transport failure, not a per-snapshot problem.
                    telegram_send_text(telegram, &notice, timeout)?;
                    sent.push(json!({
                        "ok": false,
                        "threadId": snapshot.thread_id,
                        "stage": "render",
                        "error": format!("{error:#}")
                    }));
                    continue;
                }
            };
        // Delivery and reply-route persistence failures are transport/state
        // level, not snapshot level: reporting them as "could not render"
        // would be wrong, and a half-delivered snapshot (sent but unroutable)
        // must surface as a real error rather than be swallowed here.
        sent.push(deliver_prepared_telegram_delivery(
            conn,
            telegram,
            &mut prepared,
            now,
            timeout,
        )?);
    }

    Ok(json!({
        "ok": render_failed == 0,
        "action": "telegram_threads",
        "limit": limit,
        "count": snapshots.len(),
        "renderFailed": render_failed,
        "sent": sent
    }))
}

fn execute_telegram_command(
    conn: &Connection,
    telegram: &TelegramConfig,
    message: &Value,
    command: TelegramInboundCommand,
    now: u64,
    timeout: Duration,
    deadline: Option<Instant>,
) -> Result<Value> {
    let chat_id = telegram_chat_id(message).context("Telegram command missing chat.id")?;
    let user_id = telegram_from_user_id(message);
    match command {
        TelegramInboundCommand::Start | TelegramInboundCommand::Help => {
            let sent = telegram_send_text(telegram, &telegram_help_text(), timeout)?;
            Ok(json!({ "ok": true, "action": "telegram_help", "sent": sent }))
        }
        TelegramInboundCommand::Back => {
            let state = set_away_mode(conn, false, now)?;
            let cleared = state
                .get("clearedPendingNotifications")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let sent = send_telegram_command_text(
                telegram,
                &format!("Remote Claude mode is off. Cleared {cleared} pending notification(s)."),
                timeout,
            )?;
            Ok(json!({ "ok": true, "action": "telegram_back", "state": state, "sent": sent }))
        }
        TelegramInboundCommand::Status => {
            let sent = telegram_send_text(telegram, &telegram_status_text(conn)?, timeout)?;
            Ok(json!({ "ok": true, "action": "telegram_status", "sent": sent }))
        }
        TelegramInboundCommand::Threads(raw_limit) => execute_threads_command(
            conn, telegram, raw_limit, now, timeout, deadline,
        )
        .or_else(|error| {
            let message = telegram_threads_failure_text(&error);
            let sent = telegram_send_text(telegram, &message, timeout)?;
            Ok(json!({
                "ok": false,
                "action": "telegram_threads_failed",
                "error": format!("{error:#}"),
                "sent": sent
            }))
        }),
        TelegramInboundCommand::Away => {
            execute_away_command(conn, telegram, now, timeout).or_else(|error| {
                telegram_backend_failure_result(
                    telegram,
                    "telegram_away_failed",
                    "Remote Claude mode could not start.",
                    error,
                    timeout,
                )
            })
        }
        TelegramInboundCommand::Repair => execute_repair_command(conn, telegram, now, timeout)
            .or_else(|error| {
                telegram_backend_failure_result(
                    telegram,
                    "telegram_repair_failed",
                    "Remote Claude mode could not repair.",
                    error,
                    timeout,
                )
            }),
        TelegramInboundCommand::NewThread(Some(prompt)) => {
            let config = load_daemon_config()?;
            let current_project =
                current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
            match resolve_new_thread_request(&config.projects, current_project, Some(&prompt)) {
                Ok(request) => {
                    if let Some(prompt) = request.prompt.as_deref() {
                        let result = start_new_thread_from_telegram(
                            conn,
                            &config,
                            request.project,
                            prompt,
                            now,
                        )?;
                        let confirmation = send_new_thread_confirmation(
                            conn,
                            telegram,
                            request.project,
                            &result,
                            timeout,
                            now,
                        )?;
                        Ok(json!({
                            "ok": true,
                            "action": "telegram_new_thread",
                            "projectId": request.project.id,
                            "result": result,
                            "confirmation": confirmation
                        }))
                    } else {
                        send_new_thread_prompt_for_project(
                            conn,
                            telegram,
                            request.project,
                            timeout,
                            now,
                        )
                    }
                }
                Err(error) => {
                    let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                    let sent = telegram_send_text(
                        telegram,
                        &format!(
                            "{}\n\n{}",
                            error,
                            telegram_projects_text(&config, current_project, &observed)
                        ),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "telegram_new_thread_needs_project",
                        "sent": sent
                    }))
                }
            }
        }
        TelegramInboundCommand::NewThread(None) => {
            let config = load_daemon_config()?;
            let current_project =
                current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
            match resolve_new_thread_request(&config.projects, current_project, None) {
                Ok(request) => send_new_thread_prompt_for_project(
                    conn,
                    telegram,
                    request.project,
                    timeout,
                    now,
                ),
                Err(error) => {
                    let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                    let sent = telegram_send_text(
                        telegram,
                        &format!(
                            "{}\n\n{}",
                            error,
                            telegram_projects_text(&config, current_project, &observed)
                        ),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "telegram_new_thread_needs_project",
                        "sent": sent
                    }))
                }
            }
        }
        TelegramInboundCommand::Project(Some(query)) => {
            let config = load_daemon_config()?;
            match resolve_project_query(&config.projects, &query) {
                Ok(project) => {
                    set_telegram_current_project_id(
                        conn,
                        &chat_id,
                        user_id.as_deref(),
                        &project.id,
                    )?;
                    let sent = telegram_send_text(
                        telegram,
                        &telegram_project_text(Some(project)),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "telegram_project_set",
                        "projectId": project.id,
                        "sent": sent
                    }))
                }
                Err(error) => {
                    let current_project =
                        current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
                    let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                    let sent = telegram_send_text(
                        telegram,
                        &format!(
                            "{}\n\n{}",
                            error,
                            telegram_projects_text(&config, current_project, &observed)
                        ),
                        timeout,
                    )?;
                    Ok(json!({
                        "ok": true,
                        "action": "telegram_project_not_found",
                        "sent": sent
                    }))
                }
            }
        }
        TelegramInboundCommand::Project(None) => {
            let config = load_daemon_config()?;
            let current_project =
                current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
            let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
            let sent = telegram_send_text(
                telegram,
                &telegram_projects_text(&config, current_project, &observed),
                timeout,
            )?;
            Ok(json!({ "ok": true, "action": "telegram_project", "sent": sent }))
        }
        TelegramInboundCommand::Unknown(command) => {
            let sent = telegram_send_text(
                telegram,
                &format!("I don't know {command} yet.\n\n{}", telegram_help_text()),
                timeout,
            )?;
            Ok(json!({
                "ok": true,
                "action": "telegram_unknown_command",
                "command": command,
                "sent": sent
            }))
        }
    }
}

fn execute_telegram_command_prompt_reply(
    conn: &Connection,
    telegram: &TelegramConfig,
    message: &Value,
    route: RoutedTelegramCommandPromptReply,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    let chat_id =
        telegram_chat_id(message).context("Telegram command prompt reply missing chat.id")?;
    let user_id = telegram_from_user_id(message);
    let reply_message_id = message
        .get("reply_to_message")
        .and_then(telegram_message_id)
        .context("Telegram command prompt reply missing reply_to_message.message_id")?;
    match route.kind {
        TelegramCommandRouteKind::NewThread => {
            let config = load_daemon_config()?;
            let current_project =
                current_project_for_identity(&config, conn, &chat_id, user_id.as_deref())?;
            let project = match route.project_id.as_deref() {
                Some(project_id) => match config
                    .projects
                    .iter()
                    .find(|project| project.id == project_id)
                {
                    Some(project) => Some(project),
                    None => {
                        let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                        let sent = telegram_send_text(
                            telegram,
                            &format!(
                                "That project is no longer available. Pick a project first, then start the session again.\n\n{}",
                                telegram_projects_text(&config, current_project, &observed)
                            ),
                            timeout,
                        )?;
                        mark_telegram_command_route_used(conn, &chat_id, reply_message_id, now)?;
                        return Ok(json!({
                            "ok": true,
                            "action": "telegram_new_thread_prompt_missing_project",
                            "sent": sent
                        }));
                    }
                },
                None => current_project,
            };
            let Some(project) = project else {
                let observed = observed_workspaces_from_db(conn, 5).unwrap_or_default();
                let sent = telegram_send_text(
                    telegram,
                    &format!(
                        "No project is selected for that prompt. Use /project <id> first, then try /new again.\n\n{}",
                        telegram_projects_text(&config, current_project, &observed)
                    ),
                    timeout,
                )?;
                mark_telegram_command_route_used(conn, &chat_id, reply_message_id, now)?;
                return Ok(json!({
                    "ok": true,
                    "action": "telegram_new_thread_prompt_needs_project",
                    "sent": sent
                }));
            };
            let result =
                start_new_thread_from_telegram(conn, &config, project, &route.message, now)?;
            mark_telegram_command_route_used(conn, &chat_id, reply_message_id, now)?;
            let confirmation =
                send_new_thread_confirmation(conn, telegram, project, &result, timeout, now)?;
            Ok(json!({
                "ok": true,
                "action": "telegram_new_thread_prompt_reply",
                "projectId": project.id,
                "result": result,
                "confirmation": confirmation
            }))
        }
    }
}

/// Turn a tapped button into a decision the blocked PreToolUse hook can read.
/// The write is conditional on the approval still being unanswered, so a
/// double tap (or the other button) cannot overturn the first answer.
fn record_callback_answer(
    conn: &Connection,
    route: &RoutedTelegramCallback,
    now: u64,
) -> Result<Value> {
    let Some(approval_id) = route.approval_id.as_deref() else {
        return Ok(json!({
            "ok": true,
            "action": "callback_without_approval",
            "toast": "这个按钮已经失效了。"
        }));
    };
    let decision = match route.action {
        TelegramCallbackAction::Approve => "allow",
        TelegramCallbackAction::ApproveSession => "allow_session",
        TelegramCallbackAction::Deny => "deny",
    };
    let outcome = crate::state::record_approval_decision(conn, approval_id, decision, now)?;
    let tool_name = crate::state::pending_approval_row(conn, approval_id)?
        .map(|(_, tool_name, _)| tool_name)
        .unwrap_or_else(|| "该工具".to_string());
    // The toast must not claim success for an answer that can no longer take
    // effect: once the waiting hook has given up, the session has already
    // fallen back to its own permission prompt.
    let toast = match outcome {
        ApprovalAnswer::Recorded => match route.action {
            TelegramCallbackAction::Approve => "已允许。".to_string(),
            TelegramCallbackAction::ApproveSession => {
                format!("已允许，本会话内 {tool_name} 不再询问。")
            }
            TelegramCallbackAction::Deny => "已拒绝。".to_string(),
        },
        ApprovalAnswer::AlreadyAnswered => "这条请求已经处理过了。".to_string(),
        ApprovalAnswer::Expired => "这条请求已超时，会话已回到终端等待处理。".to_string(),
        ApprovalAnswer::Unknown => "这个按钮已经失效了。".to_string(),
    };
    Ok(json!({
        "ok": true,
        "action": "telegram_approval_answer",
        "approvalId": approval_id,
        "threadId": route.thread_id,
        "decision": decision,
        "outcome": format!("{outcome:?}"),
        "recorded": outcome == ApprovalAnswer::Recorded,
        "toast": toast
    }))
}

pub(crate) fn process_telegram_updates(
    conn: &Connection,
    config: &DaemonConfig,
    now: u64,
    timeout: Duration,
    deadline: Option<Instant>,
) -> Result<Value> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Ok(json!({
            "ok": true,
            "transport": "telegram",
            "seen": 0,
            "skipped": "cycle_deadline"
        }));
    }
    let telegram = config
        .telegram
        .as_ref()
        .context("Telegram is not configured. Run setup first.")?;
    let bot_id = telegram_bot_id(&telegram.bot_token);
    let key = format!("telegram_offset:{bot_id}");
    let offset = get_setting_number(conn, &key)?.map(|value| value as i64 + 1);
    let updates = telegram_get_updates(&telegram.bot_token, offset, 0, timeout)?;
    let updates = telegram_updates_array(&updates)?;
    process_telegram_update_batch(
        conn, config, telegram, &bot_id, &key, updates, now, timeout, deadline,
    )
}

fn advance_telegram_ack_offset(max_acked: &mut Option<i64>, update_id: Option<i64>) {
    if let Some(update_id) = update_id {
        *max_acked = Some(max_acked.map_or(update_id, |current: i64| current.max(update_id)));
    }
}

#[allow(clippy::too_many_arguments)]
fn process_telegram_update_batch(
    conn: &Connection,
    config: &DaemonConfig,
    telegram: &TelegramConfig,
    bot_id: &str,
    offset_key: &str,
    updates: &[Value],
    now: u64,
    timeout: Duration,
    deadline: Option<Instant>,
) -> Result<Value> {
    let mut seen = 0usize;
    let mut replies = 0usize;
    let mut command_prompt_replies = 0usize;
    let mut commands = 0usize;
    let mut callbacks = 0usize;
    let mut duplicate = 0usize;
    let mut ignored = 0usize;
    let mut failed = 0usize;
    // The Telegram offset only advances past updates that were durably acked in
    // telegram_inbound_log. Unprocessed tail updates are left to the next cycle,
    // so a slow message or an expired batch budget can never skip later messages.
    let mut max_acked_update_id = None;
    for update in updates {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        seen += 1;
        let update_id = update.get("update_id").and_then(Value::as_i64);
        if let Some(update_id) = update_id {
            if telegram_inbound_processed(conn, bot_id, update_id)? {
                duplicate += 1;
                advance_telegram_ack_offset(&mut max_acked_update_id, Some(update_id));
                continue;
            }
        }
        let outcome: Result<()> = (|| {
            if let Some(message) = update.get("message") {
                let route_message_id = message
                    .get("reply_to_message")
                    .and_then(telegram_message_id);
                if let Some(route) = extract_telegram_reply_route(conn, message, telegram)? {
                    let result = send_claude_reply_to_thread(
                        conn,
                        config,
                        &route.thread_id,
                        &route.message,
                        now,
                    )?;
                    if let Some(update_id) = update_id {
                        record_telegram_inbound_processed(
                            conn,
                            bot_id,
                            update_id,
                            "telegram_reply",
                            &result,
                            backend_log_context_from_result(
                                &result,
                                Some(&route.thread_id),
                                route_message_id,
                            ),
                            now,
                        )?;
                    }
                    replies += 1;
                } else if let Some(route) =
                    extract_telegram_command_prompt_reply(conn, message, telegram)?
                {
                    let result = execute_telegram_command_prompt_reply(
                        conn, telegram, message, route, now, timeout,
                    )?;
                    if let Some(update_id) = update_id {
                        record_telegram_inbound_processed(
                            conn,
                            bot_id,
                            update_id,
                            "telegram_command_prompt_reply",
                            &result,
                            TelegramInboundLogContext {
                                thread_id: result
                                    .pointer("/result/threadId")
                                    .and_then(Value::as_str),
                                route_message_id,
                                result_action: result.get("action").and_then(Value::as_str),
                                backend_transport: result
                                    .pointer("/result/claude/transport")
                                    .and_then(Value::as_str),
                                backend_pid: result
                                    .pointer("/result/claude/pid")
                                    .and_then(Value::as_u64)
                                    .and_then(|value| {
                                        if value <= u32::MAX as u64 {
                                            Some(value as u32)
                                        } else {
                                            None
                                        }
                                    }),
                            },
                            now,
                        )?;
                    }
                    command_prompt_replies += 1;
                } else if let Some(command) = extract_telegram_command(message, telegram)? {
                    let result = execute_telegram_command(
                        conn, telegram, message, command, now, timeout, deadline,
                    )?;
                    if let Some(update_id) = update_id {
                        record_telegram_inbound_processed(
                            conn,
                            bot_id,
                            update_id,
                            "telegram_command",
                            &result,
                            TelegramInboundLogContext {
                                route_message_id,
                                result_action: result.get("action").and_then(Value::as_str),
                                ..TelegramInboundLogContext::default()
                            },
                            now,
                        )?;
                    }
                    commands += 1;
                } else {
                    // A message we cannot route must not vanish silently: the
                    // user typed it expecting an answer. The hint goes through
                    // the persistent outbox so a transient Telegram failure
                    // retries with backoff instead of dropping it.
                    let mut hinted = false;
                    if telegram_authorized(
                        telegram,
                        telegram_chat_id(message).as_deref(),
                        telegram_from_user_id(message).as_deref(),
                    ) {
                        let hint = unrouted_message_hint_text(message);
                        let discriminator = update_id
                            .map(|update_id| update_id.to_string())
                            .unwrap_or_else(|| {
                                format!(
                                    "{now}-{}",
                                    telegram_message_id(message).unwrap_or_default()
                                )
                            });
                        let event = json!({
                            "type": "bridge_notice",
                            "observedAt": now,
                            "message": hint,
                            "eventKey": format!("unrouted-hint:{bot_id}:{discriminator}")
                        });
                        hinted = crate::state::enqueue_outbound_event(conn, &event, now, "bridge")?;
                    }
                    if let Some(update_id) = update_id {
                        record_telegram_inbound_processed(
                            conn,
                            bot_id,
                            update_id,
                            "message_ignored",
                            &json!({ "ignored": true, "hinted": hinted }),
                            TelegramInboundLogContext {
                                route_message_id,
                                ..TelegramInboundLogContext::default()
                            },
                            now,
                        )?;
                    }
                    ignored += 1;
                }
            } else if let Some(callback_query) = update.get("callback_query") {
                // tinyCTB does not attach inline approval buttons; any residual
                // callback taps are answered with a pointer to the terminal.
                let route_message_id = callback_query
                    .get("message")
                    .and_then(|message| message.get("message_id"))
                    .and_then(Value::as_i64);
                match extract_telegram_callback_route(conn, callback_query, telegram)? {
                    Some(route) => {
                        let answer = record_callback_answer(conn, &route, now)?;
                        mark_telegram_callback_route_used(conn, &route.callback_id, now)?;
                        let _ = telegram_answer_callback_query(
                            telegram,
                            &route.callback_query_id,
                            answer
                                .get("toast")
                                .and_then(Value::as_str)
                                .unwrap_or("已收到"),
                            timeout,
                        );
                        if let Some(update_id) = update_id {
                            record_telegram_inbound_processed(
                                conn,
                                bot_id,
                                update_id,
                                "callback_query_answered",
                                &answer,
                                TelegramInboundLogContext {
                                    thread_id: Some(&route.thread_id),
                                    route_message_id,
                                    result_action: answer.get("action").and_then(Value::as_str),
                                    ..TelegramInboundLogContext::default()
                                },
                                now,
                            )?;
                        }
                        callbacks += 1;
                    }
                    None => {
                        if let Some(update_id) = update_id {
                            record_telegram_inbound_processed(
                                conn,
                                bot_id,
                                update_id,
                                "callback_query_ignored",
                                &json!({ "ignored": true }),
                                TelegramInboundLogContext {
                                    route_message_id,
                                    ..TelegramInboundLogContext::default()
                                },
                                now,
                            )?;
                        }
                        ignored += 1;
                    }
                }
            }
            Ok(())
        })();
        if let Err(error) = outcome {
            failed += 1;
            // Acknowledge the failing update so Telegram long polling advances past it.
            // Without this, the daemon re-processes the same update forever and every
            // message behind it is blocked.
            if let Some(update_id) = update_id {
                record_telegram_inbound_processed(
                    conn,
                    bot_id,
                    update_id,
                    "update_error",
                    &json!({ "error": format!("{error:#}") }),
                    TelegramInboundLogContext::default(),
                    now,
                )?;
                advance_telegram_ack_offset(&mut max_acked_update_id, Some(update_id));
            }
            if update.get("message").is_some() {
                let _ = telegram_send_text(
                    telegram,
                    &format!("Your message could not be processed: {error:#}"),
                    timeout,
                );
            }
        } else {
            advance_telegram_ack_offset(&mut max_acked_update_id, update_id);
        }
    }
    if let Some(update_id) = max_acked_update_id {
        set_setting(conn, offset_key, update_id as u64)?;
    }
    Ok(json!({
        "ok": true,
        "transport": "telegram",
        "seen": seen,
        "replies": replies,
        "commandPromptReplies": command_prompt_replies,
        "commands": commands,
        "callbacks": callbacks,
        "duplicate": duplicate,
        "ignored": ignored,
        "failed": failed
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use crate::{write_daemon_config, DaemonConfig, TelegramConfig};

    fn config_test_lock() -> &'static Mutex<()> {
        crate::state::test_env_lock()
    }

    struct CommandEnv {
        root: PathBuf,
        previous_state_dir: Option<String>,
        previous_claude_bin: Option<String>,
    }

    impl CommandEnv {
        /// Isolated state dir + a fake always-succeeding `claude` binary so
        /// backend checks pass without Claude Code installed.
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "tinyctb-telegram-command-{name}-{}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create command state dir");
            let fake_claude = root.join("claude");
            fs::write(&fake_claude, "#!/bin/sh\nexit 0\n").expect("write fake claude");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&fake_claude, fs::Permissions::from_mode(0o755))
                    .expect("chmod fake claude");
            }
            let previous_state_dir = std::env::var("TINYCTB_STATE_DIR").ok();
            let previous_claude_bin = std::env::var("CLAUDE_BIN").ok();
            std::env::set_var("TINYCTB_STATE_DIR", &root);
            std::env::set_var("CLAUDE_BIN", &fake_claude);
            std::env::set_var(
                "TINYCTB_CLAUDE_SETTINGS_PATH",
                root.join("claude-settings.json").display().to_string(),
            );
            Self {
                root,
                previous_state_dir,
                previous_claude_bin,
            }
        }
    }

    impl Drop for CommandEnv {
        fn drop(&mut self) {
            if let Some(previous_state_dir) = &self.previous_state_dir {
                std::env::set_var("TINYCTB_STATE_DIR", previous_state_dir);
            } else {
                std::env::remove_var("TINYCTB_STATE_DIR");
            }
            if let Some(previous_claude_bin) = &self.previous_claude_bin {
                std::env::set_var("CLAUDE_BIN", previous_claude_bin);
            } else {
                std::env::remove_var("CLAUDE_BIN");
            }
            std::env::remove_var("TINYCTB_CLAUDE_SETTINGS_PATH");
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn test_daemon_config() -> DaemonConfig {
        DaemonConfig {
            version: 1,
            bridge_command: "tinyctb".to_string(),
            events: crate::DEFAULT_NOTIFICATION_EVENTS.to_string(),
            telegram: Some(TelegramConfig {
                bot_token: "123:secret".to_string(),
                chat_id: "456".to_string(),
                allowed_user_id: Some("789".to_string()),
            }),
            claude: Some(ClaudeConfig::default()),
            projects: Vec::new(),
        }
    }

    #[test]
    fn telegram_setup_dry_run_writes_redacted_daemon_shape() {
        let _guard = config_test_lock().lock().expect("config lock");
        let _env = CommandEnv::new("setup-dry-run");

        let result = telegram_setup_result(TelegramSetupOptions {
            bot_token: Some("123:secret"),
            chat_id: Some("456"),
            allowed_user_id: Some("789"),
            events: crate::DEFAULT_NOTIFICATION_EVENTS,
            bridge_command: "tinyctb",
            dry_run: true,
            pair_timeout_ms: 1000,
        })
        .expect("telegram setup dry run");

        assert_eq!(result["action"], "telegram_setup");
        assert_eq!(result["dryRun"], true);
        assert_eq!(result["telegram"]["configured"], true);
        assert_eq!(result["telegram"]["botToken"], "<redacted>");
        assert_eq!(result["config"]["telegram"]["botToken"], "<redacted>");
        assert_eq!(result["config"]["telegram"]["chatId"], "456");
        assert_eq!(result["config"]["telegram"]["allowedUserId"], "789");
        assert_eq!(result["daemonCommand"], "tinyctb daemon run");
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("123:secret"),
            "setup output must not leak Telegram bot token"
        );
    }

    #[test]
    fn telegram_command_parser_supports_core_commands() {
        assert_eq!(
            parse_telegram_command_text("/away"),
            Some(TelegramInboundCommand::Away)
        );
        assert_eq!(
            parse_telegram_command_text("/back@tinyctb_bot"),
            Some(TelegramInboundCommand::Back)
        );
        assert_eq!(
            parse_telegram_command_text("/repair"),
            Some(TelegramInboundCommand::Repair)
        );
        assert_eq!(
            parse_telegram_command_text("/new Fix the formatter"),
            Some(TelegramInboundCommand::NewThread(Some(
                "Fix the formatter".to_string()
            )))
        );
        assert_eq!(
            parse_telegram_command_text("/new"),
            Some(TelegramInboundCommand::NewThread(None))
        );
        assert_eq!(
            parse_telegram_command_text("/project bridge"),
            Some(TelegramInboundCommand::Project(Some("bridge".to_string())))
        );
        assert_eq!(
            parse_telegram_command_text("/unknown"),
            Some(TelegramInboundCommand::Unknown("/unknown".to_string()))
        );
    }

    #[test]
    fn telegram_command_extraction_requires_standalone_authorized_message() {
        let telegram = TelegramConfig {
            bot_token: "123:secret".to_string(),
            chat_id: "456".to_string(),
            allowed_user_id: Some("789".to_string()),
        };

        let command = extract_telegram_command(
            &json!({
                "chat": { "id": "456" },
                "from": { "id": "789" },
                "text": "/status"
            }),
            &telegram,
        )
        .expect("extract command");
        assert_eq!(command, Some(TelegramInboundCommand::Status));

        let reply_command = extract_telegram_command(
            &json!({
                "chat": { "id": "456" },
                "from": { "id": "789" },
                "text": "/status",
                "reply_to_message": { "message_id": 1 }
            }),
            &telegram,
        )
        .expect("extract reply command");
        assert_eq!(reply_command, None);

        let unauthorized = extract_telegram_command(
            &json!({
                "chat": { "id": "999" },
                "from": { "id": "789" },
                "text": "/status"
            }),
            &telegram,
        )
        .expect("extract unauthorized command");
        assert_eq!(unauthorized, None);
    }

    #[test]
    fn telegram_reply_spawns_headless_resume_without_waiting() {
        // test_spawn::RECORDED is a process-global; without the shared env
        // lock this test races claude.rs's spawn tests under parallel runs.
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let config = test_daemon_config();
        let _ = crate::claude::test_spawn::take();
        if crate::claude::resolve_claude_binary().is_err() {
            return;
        }

        let result =
            send_claude_reply_to_thread(&conn, &config, "sess-1", "continue", 1000).expect("reply");

        assert_eq!(result["action"], "telegram_reply");
        assert_eq!(
            result.pointer("/claude/transport").and_then(Value::as_str),
            Some("headless-cli")
        );
        assert_eq!(
            result.pointer("/delivery/status").and_then(Value::as_str),
            Some("turn_started")
        );
        let spawned = crate::claude::test_spawn::take();
        assert_eq!(spawned.len(), 1);
        let (_, args, _) = &spawned[0];
        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"sess-1".to_string()));
        assert!(
            args.contains(&"telegram：continue".to_string()),
            "telegram replies must be prefixed in the session transcript: {args:?}"
        );
        let turns = crate::state::list_running_bridge_turns(&conn).expect("bridge turns");
        assert_eq!(
            turns.len(),
            1,
            "telegram replies must register a bridge turn so its log is watched"
        );
        assert_eq!(turns[0].thread_id, "sess-1");
        assert!(turns[0]
            .log_path
            .ends_with(&format!("{}.log", turns[0].turn_id)));
    }

    /// A reply to a LIVE session goes in over its socket — no headless
    /// process is spawned, so the transcript never forks.
    #[test]
    #[cfg(target_os = "linux")]
    fn telegram_reply_injects_into_live_session_instead_of_forking() {
        use std::io::{BufRead, BufReader};
        use std::os::unix::net::UnixListener;

        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let config = test_daemon_config();
        let dir = std::env::temp_dir().join(format!("tinyctb-live-reply-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("dir");
        let path = dir.join(format!("{}.sock", std::process::id()));
        let listener = UnixListener::bind(&path).expect("bind");
        let accepted = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line).expect("read");
            line
        });
        let socket_path = path.display().to_string();
        let (inode, boot_id) = crate::claude::socket_identity(&socket_path);
        crate::state::record_session_messaging_socket(
            &conn,
            "sess-live",
            &crate::claude::SessionSocket {
                path: socket_path,
                inode,
                boot_id,
            },
            1000,
        )
        .expect("record socket");
        let _ = crate::claude::test_spawn::take();

        let result = send_claude_reply_to_thread(&conn, &config, "sess-live", "在跑什么", 1000)
            .expect("reply");

        assert_eq!(result["action"], "telegram_reply");
        assert_eq!(
            result.pointer("/claude/transport").and_then(Value::as_str),
            Some("live-session-socket")
        );
        assert!(
            crate::claude::test_spawn::take().is_empty(),
            "a live session must not be forked with a headless resume"
        );
        assert!(
            crate::state::list_running_bridge_turns(&conn)
                .expect("turns")
                .is_empty(),
            "injected replies produce no headless turn to watch"
        );
        let line = accepted.join().expect("join");
        let parsed: Value = serde_json::from_str(line.trim()).expect("json line");
        assert_eq!(parsed["message"]["content"], "telegram：在跑什么");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The toast the user actually sees for each outcome — the failure this
    /// covers is a timed-out request reporting "已经处理过了" (or worse,
    /// "已允许") when the session has already fallen back to its terminal.
    #[test]
    fn approval_toasts_tell_the_truth_about_each_outcome() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let route = |action: TelegramCallbackAction, approval_id: &str| RoutedTelegramCallback {
            callback_query_id: "cq".to_string(),
            callback_id: "cb".to_string(),
            thread_id: "sess-toast".to_string(),
            action,
            approval_id: Some(approval_id.to_string()),
        };

        crate::state::create_pending_approval(
            &conn,
            "ap-ok",
            "sess-toast",
            "Bash",
            "Bash: ls",
            1000,
            9000,
        )
        .expect("create");
        let first = record_callback_answer(
            &conn,
            &route(TelegramCallbackAction::Approve, "ap-ok"),
            2000,
        )
        .expect("first tap");
        assert_eq!(first["outcome"], "Recorded");
        assert_eq!(first["toast"], "已允许。");
        let second =
            record_callback_answer(&conn, &route(TelegramCallbackAction::Deny, "ap-ok"), 2100)
                .expect("second tap");
        assert_eq!(second["toast"], "这条请求已经处理过了。");

        // A request the hook gave up on must say so, not "already handled".
        crate::state::create_pending_approval(
            &conn,
            "ap-late",
            "sess-toast",
            "Bash",
            "Bash: ls",
            1000,
            9000,
        )
        .expect("create");
        crate::state::expire_or_take_decision(&conn, "ap-late", 9500).expect("expire");
        let late = record_callback_answer(
            &conn,
            &route(TelegramCallbackAction::Approve, "ap-late"),
            9600,
        )
        .expect("late tap");
        assert_eq!(late["outcome"], "Expired");
        assert!(
            late["toast"].as_str().expect("toast").contains("超时"),
            "a timed-out request must say so: {late}"
        );
        assert_eq!(late["recorded"], false);

        let unknown = record_callback_answer(
            &conn,
            &route(TelegramCallbackAction::Approve, "ap-missing"),
            9700,
        )
        .expect("unknown");
        assert_eq!(unknown["outcome"], "Unknown");
    }

    #[test]
    fn remote_commands_toggle_away_mode_and_manage_hooks() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _env = CommandEnv::new("away-repair");
        write_daemon_config(&test_daemon_config()).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = test_daemon_config().telegram.expect("telegram");
        let message = json!({
            "chat": { "id": "456" },
            "from": { "id": "789" }
        });

        let away = execute_telegram_command(
            &conn,
            &telegram,
            &message,
            TelegramInboundCommand::Away,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("away result");
        assert_eq!(away["ok"], true);
        assert_eq!(away["action"], "telegram_away");
        assert_eq!(away["away"]["away"], true);
        assert!(away["sent"]["result"]["text"]
            .as_str()
            .expect("away text")
            .contains("Remote Claude mode is on."));

        let repair = execute_telegram_command(
            &conn,
            &telegram,
            &message,
            TelegramInboundCommand::Repair,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("repair result");
        assert_eq!(repair["ok"], true);
        assert_eq!(repair["action"], "telegram_repair");
        assert_eq!(repair["backend"]["action"], "repaired");
        assert_eq!(repair["away"]["away"], true);

        let back = execute_telegram_command(
            &conn,
            &telegram,
            &message,
            TelegramInboundCommand::Back,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("back result");
        assert_eq!(back["ok"], true);
        assert_eq!(back["action"], "telegram_back");
        assert_eq!(back["state"]["away"], false);
        assert!(back["sent"]["result"]["text"]
            .as_str()
            .expect("back text")
            .contains("Remote Claude mode is off."));
    }

    #[test]
    fn away_command_failure_is_reported_to_telegram() {
        let _guard = crate::state::test_env_lock().lock().expect("env lock");
        let _env = CommandEnv::new("away-failure");
        // Point CLAUDE_BIN at a missing binary so the backend check fails.
        std::env::set_var("CLAUDE_BIN", "/nonexistent/claude-binary");
        write_daemon_config(&test_daemon_config()).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = test_daemon_config().telegram.expect("telegram");
        let message = json!({
            "chat": { "id": "456" },
            "from": { "id": "789" }
        });

        let away = execute_telegram_command(
            &conn,
            &telegram,
            &message,
            TelegramInboundCommand::Away,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("away failure response");

        assert_eq!(away["ok"], false);
        assert_eq!(away["action"], "telegram_away_failed");
        assert!(away["sent"]["result"]["text"]
            .as_str()
            .expect("failure text")
            .contains("Try /repair"));
    }

    #[test]
    fn failing_telegram_update_is_acked_and_does_not_block_later_updates() {
        let _guard = config_test_lock().lock().expect("config lock");
        let _env = CommandEnv::new("failing-update-batch");
        // Break the backend so routed replies fail.
        std::env::set_var("CLAUDE_BIN", "/nonexistent/claude-binary");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = config.telegram.clone().expect("telegram config");
        let bot_id = telegram_bot_id(&telegram.bot_token);
        let key = format!("telegram_offset:{bot_id}");
        crate::state::insert_telegram_message_route(
            &conn,
            "456",
            10,
            "sess-failing",
            "test-event",
            0,
        )
        .expect("insert route");
        let updates = vec![
            json!({
                "update_id": 1,
                "message": {
                    "message_id": 11,
                    "chat": { "id": "456" },
                    "from": { "id": "789" },
                    "reply_to_message": { "message_id": 10 },
                    "text": "continue please"
                }
            }),
            json!({
                "update_id": 2,
                "message": {
                    "message_id": 12,
                    "chat": { "id": "456" },
                    "from": { "id": "789" },
                    "text": "plain message"
                }
            }),
        ];

        let first = process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            &key,
            &updates,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("first batch");

        assert_eq!(first["failed"], 1);
        assert_eq!(first["replies"], 0);
        assert_eq!(first["ignored"], 1);
        assert_eq!(
            get_setting_number(&conn, &key)
                .expect("offset lookup")
                .expect("offset set"),
            2
        );

        let second = process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            &key,
            &updates,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("second batch");
        assert_eq!(second["duplicate"], 2);
        assert_eq!(second["failed"], 0);
    }

    /// A plain text message ("麻将的训练进度如何了？") matches no route; it must
    /// get a loud hint back instead of vanishing silently.
    #[test]
    fn unrouted_plain_text_gets_a_routing_hint() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let config = test_daemon_config();
        let telegram = config.telegram.clone().expect("telegram");
        let bot_id = telegram_bot_id(&telegram.bot_token);
        let key = format!("telegram_offset:{bot_id}");
        let updates = vec![
            json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": { "id": "456" },
                    "from": { "id": "789" },
                    "text": "麻将的训练进度如何了？"
                }
            }),
            // Unauthorized chat: stays silent (no hint leaks to strangers).
            json!({
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "chat": { "id": "999" },
                    "from": { "id": "789" },
                    "text": "hello"
                }
            }),
        ];

        let result = process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            &key,
            &updates,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("batch");
        assert_eq!(result["ignored"], 2);
        // The hint sits in the persistent outbox (retry/backoff on transient
        // Telegram failures) rather than being fired-and-forgotten.
        assert_eq!(
            crate::state::pending_outbound_count(&conn).expect("pending"),
            1
        );
        let payload: String = conn
            .query_row("SELECT payload_json FROM outbound_events", [], |row| {
                row.get(0)
            })
            .expect("hint row");
        assert!(payload.contains("isn't routed"), "payload: {payload}");

        let hinted: Vec<Option<bool>> = [1i64, 2]
            .iter()
            .map(|update_id| {
                let raw: String = conn
                    .query_row(
                        "SELECT result_json FROM telegram_inbound_log
                         WHERE bot_id = ?1 AND update_id = ?2",
                        rusqlite::params![bot_id, update_id],
                        |row| row.get(0),
                    )
                    .expect("log row");
                serde_json::from_str::<Value>(&raw)
                    .expect("log json")
                    .get("hinted")
                    .and_then(Value::as_bool)
            })
            .collect();
        assert_eq!(
            hinted[0],
            Some(true),
            "authorized unrouted text must be answered with a hint"
        );
        assert_eq!(
            hinted[1],
            Some(false),
            "unauthorized chats must not receive hints"
        );

        let hint = unrouted_message_hint_text(&updates[0]["message"]);
        assert!(hint.contains("/new 麻将的训练进度如何了？"));
        assert!(hint.contains("Reply action"));
    }

    #[test]
    fn parses_threads_command_with_optional_limit() {
        assert_eq!(
            parse_telegram_command_text("/threads"),
            Some(TelegramInboundCommand::Threads(None))
        );
        assert_eq!(
            parse_telegram_command_text("/threads 12"),
            Some(TelegramInboundCommand::Threads(Some("12".to_string())))
        );
        assert_eq!(
            parse_telegram_command_text("/threads@tinyctb_bot 3"),
            Some(TelegramInboundCommand::Threads(Some("3".to_string())))
        );
    }

    #[test]
    fn parses_threads_limit_with_default_and_bounds() {
        assert_eq!(parse_telegram_threads_limit(None).expect("default"), 5);
        assert_eq!(
            parse_telegram_threads_limit(Some("10")).expect("explicit"),
            10
        );
        assert!(parse_telegram_threads_limit(Some("0")).is_err());
        assert!(parse_telegram_threads_limit(Some("26")).is_err());
        assert!(parse_telegram_threads_limit(Some("two")).is_err());
    }

    #[test]
    fn telegram_batch_deadline_does_not_advance_offset_past_unacked_updates() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let config = test_daemon_config();
        let telegram = config.telegram.clone().expect("telegram");
        let bot_id = telegram_bot_id(&telegram.bot_token);
        let key = format!("telegram_offset:{bot_id}");
        let updates = vec![
            json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": { "id": "456" },
                    "from": { "id": "789" },
                    "text": "plain message"
                }
            }),
            json!({
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "chat": { "id": "456" },
                    "from": { "id": "789" },
                    "text": "plain message"
                }
            }),
        ];

        let expired = process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            &key,
            &updates,
            0,
            Duration::from_secs(1),
            Some(Instant::now() - Duration::from_secs(1)),
        )
        .expect("expired batch");
        assert_eq!(expired["seen"], 0);
        assert_eq!(
            get_setting_number(&conn, &key).expect("offset lookup"),
            None
        );

        let full = process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            &key,
            &updates,
            0,
            Duration::from_secs(1),
            None,
        )
        .expect("full batch");
        assert_eq!(full["ignored"], 2);
        assert_eq!(
            get_setting_number(&conn, &key)
                .expect("offset lookup")
                .expect("offset set"),
            2
        );
    }

    #[test]
    fn telegram_callback_route_is_consumed_after_use() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = TelegramConfig {
            bot_token: "123:secret".to_string(),
            chat_id: "456".to_string(),
            allowed_user_id: Some("789".to_string()),
        };
        crate::state::insert_telegram_callback_route(
            &conn,
            &crate::state::TelegramCallbackRoute {
                callback_id: "cb_1".to_string(),
                chat_id: "456".to_string(),
                message_id: None,
                thread_id: "thr_1".to_string(),
                action: TelegramCallbackAction::Approve,
                approval_id: None,
            },
            1000,
        )
        .expect("insert route");
        let callback_query = json!({
            "id": "cq_1",
            "from": { "id": 789 },
            "message": { "message_id": 10, "chat": { "id": "456" } },
            "data": "claude:cb_1"
        });

        let route =
            extract_telegram_callback_route(&conn, &callback_query, &telegram).expect("route");
        assert!(route.is_some());

        mark_telegram_callback_route_used(&conn, "cb_1", 2000).expect("mark used");
        let after =
            extract_telegram_callback_route(&conn, &callback_query, &telegram).expect("after");
        assert!(after.is_none());
    }
}
