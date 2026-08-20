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
    telegram_bot_commands, telegram_chat_id, telegram_delete_webhook, telegram_from_user_id,
    telegram_get_updates, telegram_message_id, telegram_send_chat_action, telegram_send_message,
    telegram_send_text, telegram_send_text_message_id, telegram_updates_array,
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
    Stop(Option<String>),
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
    pub(crate) question_id: Option<String>,
    pub(crate) answer: Option<String>,
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
        "/stop" => Some(TelegramInboundCommand::Stop(rest.map(str::to_string))),
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

/// What a callback tap resolved to. The distinction the caller must not
/// collapse: a SPENT button still deserves an `answerCallbackQuery` — without
/// one the Telegram client spins on the tap for ~30s and then shows nothing,
/// which reads as "the bridge hung" (observed live, 2026-08-13). Only taps
/// that are not ours to answer stay silent.
#[derive(Debug)]
pub(crate) enum TelegramCallbackLookup {
    /// An unused route: record the answer and consume it.
    Route(RoutedTelegramCallback),
    /// Ours and authorized, but already used (or pruned): toast the truth.
    Spent {
        callback_query_id: String,
        toast: &'static str,
    },
    /// Unauthorized or not our data format: ignore silently, as ever.
    Foreign,
}

pub(crate) fn extract_telegram_callback_route(
    conn: &Connection,
    callback_query: &Value,
    telegram: &TelegramConfig,
) -> Result<TelegramCallbackLookup> {
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
        return Ok(TelegramCallbackLookup::Foreign);
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
        return Ok(TelegramCallbackLookup::Foreign);
    };
    let route = conn
        .query_row(
            "SELECT thread_id, action, approval_id, question_id, answer, used_at
             FROM telegram_callback_routes
             WHERE callback_id = ?1",
            params![callback_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((thread_id, action, approval_id, question_id, answer, used_at)) = route else {
        // Never existed or pruned away: the button outlived its row.
        return Ok(TelegramCallbackLookup::Spent {
            callback_query_id: callback_query_id.to_string(),
            toast: "这个按钮已经失效了。",
        });
    };
    if used_at.is_some() {
        // One answer per button (used_at) is a security rule; saying so is
        // not optional politeness but the only feedback the tap gets.
        return Ok(TelegramCallbackLookup::Spent {
            callback_query_id: callback_query_id.to_string(),
            toast: "这个按钮已经处理过了。",
        });
    }
    match TelegramCallbackAction::from_str(&action) {
        Some(action) => Ok(TelegramCallbackLookup::Route(RoutedTelegramCallback {
            callback_query_id: callback_query_id.to_string(),
            callback_id: callback_id.to_string(),
            thread_id,
            action,
            approval_id,
            question_id,
            answer,
        })),
        None => Ok(TelegramCallbackLookup::Spent {
            callback_query_id: callback_query_id.to_string(),
            toast: "这个按钮已经失效了。",
        }),
    }
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
        // Remember which message carries a question, so a text reply to it
        // is recognised as the answer.
        // Record EVERY chunk, so a reply to any part of a long dialog is
        // recognised as belonging to it.
        if let Some(question_id) = prepared.question_id.as_deref() {
            crate::state::attach_question_message_id(conn, question_id, message_id)?;
            crate::state::record_dialog_message(
                conn,
                &telegram.chat_id,
                message_id,
                "question",
                question_id,
                now,
            )?;
        }
        if let Some(approval_id) = prepared.approval_id.as_deref() {
            crate::state::attach_approval_message_id(conn, approval_id, message_id)?;
            crate::state::record_dialog_message(
                conn,
                &telegram.chat_id,
                message_id,
                "approval",
                approval_id,
                now,
            )?;
        }
        message_ids.push(message_id);
    }
    let first_message_id = *message_ids
        .first()
        .context("Telegram delivery did not send any messages")?;
    // The keyboard rides on the LAST chunk, so that is the message a button
    // belongs to.
    let keyboard_message_id = *message_ids.last().unwrap_or(&first_message_id);
    for route in &mut prepared.callback_routes {
        route.message_id = Some(keyboard_message_id);
        update_telegram_callback_message_id(conn, &route.callback_id, keyboard_message_id)?;
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

/// Clear the typing bubble for a thread, for callers outside this module
/// that do not hold a `TelegramConfig` (the daemon's stop recovery).
pub(crate) fn clear_typing_for_thread(conn: &Connection, thread_id: &str) -> Result<()> {
    let Some(config) = load_daemon_config().ok().and_then(|config| config.telegram) else {
        return Ok(());
    };
    clear_telegram_typing_indicator(conn, &config, thread_id)
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
            // Registration happens inside the spawn (before it, in fact) so
            // the turn's first tool call already finds its bridge_turns row.
            send_user_message(conn, config, thread_id, &prefixed, cwd_hint.as_deref(), now)?
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
    // The bridge_turns row is written by the spawn itself, before the
    // process exists, so the daemon watches the log (and the headless gate
    // recognises the turn) from the first instant.
    let result = start_thread_in_cwd(conn, config, Some(&project.cwd), Some(message), now)?;
    let thread_id = result
        .get("threadId")
        .and_then(Value::as_str)
        .context("new Claude session result missing threadId")?
        .to_string();
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

/// How many recent sessions compete for the /threads list before the limit
/// cut. Big enough that a live terminal older than a screenful of idle
/// churn still makes the list.
const THREADS_CLASSIFY_POOL: u64 = 50;

/// Assemble and classify the /threads candidates.
///
/// The pool is the recent `threads_cache` rows UNIONED with every running
/// bridge turn: a turn started moments ago (CLI `tinyctb new`, a fresh
/// Telegram task) has a `bridge_turns` row before its transcript or first
/// hook produces a snapshot, and a cache-only pool made exactly those
/// in-flight tasks vanish from the list. Missing snapshots get a minimal
/// placeholder — better a sparse row than an invisible running task.
/// One `list_running_bridge_turns` query serves both the union and the
/// per-candidate headless flag.
/// The /threads row for a session whose prompt is still open: shaped exactly
/// like the gate's original approval/question event, so the whole delivery
/// machinery (inline keyboard, route stamping, dialog registration for text
/// replies) applies unchanged.
fn reoffer_prompt_event(
    conn: &Connection,
    telegram: &TelegramConfig,
    snapshot: &crate::state::BridgeThreadSnapshot,
    liveness: render::ThreadLiveness,
    prompt: &crate::state::OpenPrompt,
    now: u64,
) -> Result<Value> {
    let short_id = snapshot.thread_id.chars().take(8).collect::<String>();
    let display_name = match snapshot.name.as_deref() {
        Some(name) if !name.trim().is_empty() => format!("{name} · {short_id}"),
        _ => short_id.clone(),
    };
    let thread = json!({
        "threadId": snapshot.thread_id,
        "displayName": display_name,
        "project": crate::projects::derive_project_label(snapshot.cwd.as_deref()),
        "cwd": snapshot.cwd,
    });
    match prompt {
        crate::state::OpenPrompt::Approval {
            approval_id,
            summary,
            headless,
        } => {
            let buttons = crate::approvals::approval_answer_buttons(
                conn,
                &telegram.chat_id,
                &snapshot.thread_id,
                approval_id,
                now,
            )?;
            Ok(json!({
                "type": "approval_request",
                "threadId": snapshot.thread_id,
                "approvalId": approval_id,
                "observedAt": now,
                "eventKey": format!("approval-reoffer:{approval_id}:{now}"),
                "lastPreview": summary,
                "buttons": buttons,
                // The gate kind persisted at creation, NOT the session's
                // current look: what a timeout does was fixed when the hook
                // started waiting, and the hint must not lie about it.
                "headless": headless,
                "statusLine": liveness.status_line(),
                "thread": thread
            }))
        }
        crate::state::OpenPrompt::Question {
            question_id,
            question,
            options,
            multi_select,
        } => {
            let buttons = if *multi_select {
                Vec::new()
            } else {
                crate::approvals::question_answer_buttons(
                    conn,
                    &telegram.chat_id,
                    &snapshot.thread_id,
                    question_id,
                    options,
                    now,
                )?
            };
            let body = crate::approvals::question_body(question, options, *multi_select);
            Ok(json!({
                "type": "question_request",
                "threadId": snapshot.thread_id,
                "questionId": question_id,
                "observedAt": now,
                "eventKey": format!("question-reoffer:{question_id}:{now}"),
                "lastPreview": body,
                "buttons": buttons,
                "statusLine": liveness.status_line(),
                "thread": thread
            }))
        }
    }
}

type ClassifiedThread = (
    crate::state::BridgeThreadSnapshot,
    render::ThreadLiveness,
    Option<crate::state::OpenPrompt>,
);

fn classify_recent_threads(
    conn: &Connection,
    classify_pool: u64,
    now: u64,
) -> Result<Vec<ClassifiedThread>> {
    let mut pool = list_recent_thread_snapshots_from_db(conn, classify_pool)?;
    let running_turns = crate::state::list_running_bridge_turns(conn)?;
    // Aggregate BY SESSION first: one session can have several running turns
    // (concurrent replies), and one placeholder per TURN would list the
    // session twice, double it in the census, and register duplicate reply
    // routes. The newest start stamps the placeholder's recency.
    let mut latest_running = std::collections::HashMap::new();
    for turn in &running_turns {
        let started = latest_running
            .entry(turn.thread_id.clone())
            .or_insert(turn.started_at);
        *started = (*started).max(turn.started_at);
    }
    let running = latest_running
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let known = pool
        .iter()
        .map(|snapshot| snapshot.thread_id.clone())
        .collect::<std::collections::HashSet<_>>();
    for (thread_id, started_at) in &latest_running {
        if known.contains(thread_id) {
            continue;
        }
        pool.push(crate::state::BridgeThreadSnapshot {
            thread_id: thread_id.clone(),
            name: None,
            cwd: None,
            updated_at: Some(*started_at),
            status_type: "active".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: Some("（任务刚启动，还没有可显示的输出）".to_string()),
            pending_prompt: None,
            event_uid: None,
        });
    }
    // Open prompts ride along: a waiting session's /threads row re-offers
    // the very question, so a missed notification stops mattering. And they
    // UNION into the pool like running turns do — an open ask on a session
    // outside the recent-50 cache (or with no cache row at all) is the most
    // urgent row of the whole list, not one to silently drop.
    let mut prompts = crate::state::open_prompts(conn, now)?;
    let mut known = pool
        .iter()
        .map(|snapshot| snapshot.thread_id.clone())
        .collect::<std::collections::HashSet<_>>();
    for thread_id in prompts.keys() {
        if known.contains(thread_id) {
            continue;
        }
        known.insert(thread_id.clone());
        pool.push(crate::state::BridgeThreadSnapshot {
            thread_id: thread_id.clone(),
            name: None,
            cwd: None,
            // `now`: waiting rows sort by their own tier anyway; recency only
            // breaks ties between several waiting sessions.
            updated_at: Some(now),
            status_type: "active".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        });
    }
    // Sessions stuck at a TERMINAL prompt union in too — such a session may
    // have waited longer than the whole recent-cache window (a dialog that
    // sat for eight hours was exactly how this gap was found), and falling
    // off the pool would deny it the waiting tier it exists to occupy.
    for (thread_id, prompt, created_at) in
        crate::state::threads_with_pending_terminal_prompts(conn)?
    {
        if known.contains(&thread_id) {
            continue;
        }
        known.insert(thread_id.clone());
        pool.push(crate::state::BridgeThreadSnapshot {
            thread_id,
            name: None,
            cwd: None,
            updated_at: Some(created_at),
            status_type: "active".to_string(),
            status_flags: Vec::new(),
            last_turn_status: None,
            last_preview: None,
            pending_prompt: Some(prompt),
            event_uid: None,
        });
    }
    Ok(pool
        .into_iter()
        .map(|snapshot| {
            let liveness = classify_thread_liveness(conn, &snapshot.thread_id, &running);
            let prompt = prompts.remove(&snapshot.thread_id);
            (snapshot, liveness, prompt)
        })
        .collect())
}

/// Classify one candidate. A terminal-state read failure becomes the
/// `unknown` condition — displayed as exactly that; calling it "idle" would
/// be a silent lie about a session that may be live. Loud on stderr as well.
/// The headless flag comes from the batch set either way: the socket being
/// unreadable does not un-know that a turn is running.
fn classify_thread_liveness(
    conn: &Connection,
    thread_id: &str,
    running: &std::collections::HashSet<String>,
) -> render::ThreadLiveness {
    match crate::claude::session_terminal_presence(conn, thread_id) {
        Ok(presence) => render::ThreadLiveness {
            presence,
            headless: running.contains(thread_id),
            unknown: false,
        },
        Err(error) => {
            eprintln!("tinyctb: liveness read failed for {thread_id}: {error:#}");
            render::ThreadLiveness {
                presence: crate::claude::TerminalPresence::Gone,
                headless: running.contains(thread_id),
                unknown: true,
            }
        }
    }
}

/// What is waiting on the user comes first — that is the list's whole
/// point. Two waiting flavours, remote-answerable ahead of terminal-bound:
/// 0 = an OPEN gate prompt (/threads re-offers it with buttons);
/// 1 = a terminal dialog observed by the hooks (`pending_prompt`) on a
///     session whose terminal is verifiably ALIVE — a session that has sat
///     waiting for hours is exactly what the user runs /threads to discover
///     (measured live: an 8-hour-old dialog was ranked third and read as a
///     bug). The liveness requirement keeps ghosts out: a `pending` row can
///     outlive its session (crash, plain non-gate notification), and a dead
///     session's stale prompt must not squat on the top of the list;
/// 2 = everything else.
fn waiting_rank(
    snapshot: &crate::state::BridgeThreadSnapshot,
    liveness: render::ThreadLiveness,
    prompt: Option<&crate::state::OpenPrompt>,
) -> u8 {
    if prompt.is_some() {
        0
    } else if liveness.presence == crate::claude::TerminalPresence::Window
        && snapshot
            .pending_prompt
            .as_ref()
            .is_some_and(|pending| pending.status == "pending")
    {
        1
    } else {
        2
    }
}

/// Order by liveness class (alive first), newest first inside each class,
/// THEN cut to the display limit.
///
/// Recency is part of the sort KEY, not an assumption about input order: the
/// pool mixes cache rows (DB returns newest first) with running-turn
/// placeholders (hash-map order), and a stable sort over that mix would keep
/// whatever accident it was handed — truncation could then keep an old task
/// and drop a new one.
fn order_threads_for_display(
    mut threads: Vec<ClassifiedThread>,
    limit: usize,
) -> Vec<ClassifiedThread> {
    threads.sort_by_key(|(snapshot, liveness, prompt)| {
        (
            waiting_rank(snapshot, *liveness, prompt.as_ref()),
            liveness.order(),
            std::cmp::Reverse(snapshot.updated_at.unwrap_or(0)),
        )
    });
    threads.truncate(limit);
    threads
}

/// `/stop <id>` needs at least this many characters. Session ids are UUIDs
/// and this is the short form `/threads` prints; anything shorter is asking
/// the bridge to guess, and a wrong guess kills work the user wanted.
const STOP_TARGET_MIN_CHARS: usize = 8;

/// A prefix long enough to tell this id apart from the others offered.
/// Printing the same truncated 8 characters twice ("abcd1234、abcd1234") is
/// worse than useless: it asks for a longer prefix while hiding the
/// characters that would make one.
fn disambiguating_prefix(id: &str, others: &std::collections::BTreeSet<&str>) -> String {
    let chars: Vec<char> = id.chars().collect();
    for take in STOP_TARGET_MIN_CHARS..=chars.len() {
        let candidate: String = chars.iter().take(take).collect();
        if others
            .iter()
            .filter(|other| other.starts_with(&candidate))
            .count()
            == 1
        {
            return candidate;
        }
    }
    id.to_string()
}

pub(crate) fn short_thread_id(thread_id: &str) -> String {
    thread_id.chars().take(8).collect()
}

/// `/stop` — the emergency brake for a headless turn that is running away.
///
/// Scope is deliberately narrow: only rows in `bridge_turns` with status
/// `running`, i.e. turns THIS bridge started for a Telegram message. A
/// session the user opened in their own terminal has no such row and cannot
/// be reached from here at all.
fn execute_stop_command(
    conn: &Connection,
    telegram: &TelegramConfig,
    target: Option<&str>,
    invocation_id: &str,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    // A redelivered update resumes its RECORDED operation — never a fresh
    // interpretation over whatever happens to be running by now. Without
    // this, a crash after the stops committed replayed into "nothing is
    // running" while the outbox was saying "stopped"; and an early exit
    // ("nothing to stop", "ambiguous") replayed after new turns started
    // would reinterpret into stopping work that did not exist when the
    // user sent the command.
    if let Some(stored) = crate::state::stop_operation(conn, invocation_id)? {
        return resume_stop_operation(conn, telegram, &stored, invocation_id, now, timeout);
    }

    let target = target.map(str::trim).filter(|value| !value.is_empty());
    let running = crate::state::list_running_bridge_turns(conn)?;

    if let Some(prefix) = target {
        if prefix.chars().count() < STOP_TARGET_MIN_CHARS {
            let text = format!(
                "`{prefix}` 太短了，至少要 {STOP_TARGET_MIN_CHARS} 位（/threads 显示的短 ID 就是这个长度）。"
            );
            // Frozen BEFORE the reply: this resolution is what a redelivery
            // of the same update must repeat, whatever is running by then.
            crate::state::record_stop_operation(
                conn,
                invocation_id,
                "too_short",
                &[],
                Some(&text),
                now,
            )?;
            let sent = send_telegram_command_text(telegram, &text, timeout).ok();
            return Ok(json!({
                "ok": false, "action": "telegram_stop", "reason": "target_too_short",
                "stopped": 0, "sent": sent
            }));
        }
    }

    // Match the SESSION id only. `turn_id` is an internal handle the user
    // never sees, and matching it too would let one prefix mean two things.
    let selected = running
        .iter()
        .filter(|turn| match target {
            Some(prefix) => turn.thread_id.starts_with(prefix),
            None => true,
        })
        .collect::<Vec<_>>();

    if let Some(prefix) = target {
        let distinct = selected
            .iter()
            .map(|turn| turn.thread_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if distinct.len() > 1 {
            // Refuse rather than fan out: a `/stop` that kills more sessions
            // than the user meant is exactly the damage this command can do.
            let text = format!(
                "`{prefix}` 匹配到 {} 个会话：{}。请给出更长的前缀——我不会替你猜该停哪个。",
                distinct.len(),
                distinct
                    .iter()
                    .map(|id| disambiguating_prefix(id, &distinct))
                    .collect::<Vec<_>>()
                    .join("、")
            );
            // Frozen: a replay after one candidate finished must NOT
            // reinterpret and kill the survivor the user never confirmed.
            crate::state::record_stop_operation(
                conn,
                invocation_id,
                "ambiguous",
                &[],
                Some(&text),
                now,
            )?;
            let sent = send_telegram_command_text(telegram, &text, timeout).ok();
            return Ok(json!({
                "ok": false, "action": "telegram_stop", "reason": "ambiguous_target",
                "matched": distinct.len(), "stopped": 0, "sent": sent
            }));
        }
    }

    if selected.is_empty() {
        let text = match target {
            Some(prefix) if !running.is_empty() => format!(
                "没有匹配 `{prefix}` 的运行中回合。当前运行中：{}",
                running
                    .iter()
                    .map(|turn| short_thread_id(&turn.thread_id))
                    .collect::<Vec<_>>()
                    .join("、")
            ),
            Some(prefix) => format!("没有匹配 `{prefix}` 的运行中回合，当前也没有无头回合在跑。"),
            None => "没有正在运行的无头回合。".to_string(),
        };
        // Frozen: "nothing to stop" is this update's answer FOREVER — a
        // replay must never become "stop everything" just because new turns
        // started before the redelivery.
        crate::state::record_stop_operation(conn, invocation_id, "empty", &[], Some(&text), now)?;
        // A failed reply must not fail the command: nothing was stopped, but
        // erroring here would ack the update while reporting "not handled".
        let sent = send_telegram_command_text(telegram, &text, timeout).ok();
        return Ok(json!({
            "ok": sent.is_some(), "action": "telegram_stop", "stopped": 0,
            "running": running.len(), "sent": sent
        }));
    }

    // ONE transaction for the whole ACCEPTANCE: the operation record and,
    // for every selected turn, its `stopping` CAS, its dialog sweep, and
    // its REQUESTED receipt. Any failure rolls the entire command back to
    // "never happened". There is NO automatic retry — the dispatcher
    // records the error and advances the offset — but because nothing
    // committed, the user's next /stop interprets a clean world; what the
    // rollback rules out is the half-accepted state. After the commit, the
    // daemon owns completion for every turn even if none of the signalling
    // below ever runs. Committing the acceptance piecemeal was the
    // review's P1: an intent error midway left earlier turns stopped,
    // later ones running forever, receipts delivered and the offset
    // advanced past the update.
    let turn_ids: Vec<String> = selected.iter().map(|turn| turn.turn_id.clone()).collect();
    let tx = conn.unchecked_transaction()?;
    crate::state::record_stop_operation(&tx, invocation_id, "turns", &turn_ids, None, now)?;
    let mut swept = Vec::with_capacity(selected.len());
    for turn in &selected {
        swept.push(stop_intent(&tx, turn, invocation_id, now)?);
    }
    tx.commit()?;

    let mut lines = Vec::new();
    let mut stopped = 0usize;
    for (turn, settled_prompts) in selected.iter().zip(swept) {
        let outcome = stop_execute(conn, telegram, turn, invocation_id, settled_prompts, now)?;
        if outcome.claimed {
            stopped += 1;
        }
        lines.push(outcome.summary);
    }
    // No direct send of the outcomes: every summary is already an OUTBOX
    // row committed with its turn's state change (see `stop_bridge_turn`),
    // so a crash from here on can neither lose a story nor need a receipt
    // fallback. The fast lane delivers them within a cycle.
    Ok(json!({
        "ok": true, "action": "telegram_stop", "stopped": stopped,
        "attempted": selected.len(), "queuedSummaries": lines.len(),
        "summaries": lines
    }))
}

/// Resume a `/stop` whose Telegram update was redelivered — strictly from
/// its RECORDED interpretation. An early-exit resolution replays its stored
/// reply verbatim. A turn resolution handles each recorded turn by what the
/// DATABASE says about it now: still running → the stop is performed (the
/// crash interrupted it); `stopping` → the intent already committed, and
/// the requested receipt committed with the operation itself, so the daemon
/// owns the rest; settled → its terminal receipt already committed. Nothing
/// here re-reads the running set.
fn resume_stop_operation(
    conn: &Connection,
    telegram: &TelegramConfig,
    operation: &crate::state::StopOperation,
    invocation_id: &str,
    now: u64,
    timeout: Duration,
) -> Result<Value> {
    if operation.kind != "turns" {
        // The frozen answer, repeated verbatim; best-effort like the
        // original (nothing was stopped then, nothing is stopped now).
        let sent = operation
            .reply
            .as_deref()
            .and_then(|reply| send_telegram_command_text(telegram, reply, timeout).ok());
        return Ok(json!({
            "ok": true, "action": "telegram_stop", "replayed": true,
            "kind": operation.kind, "stopped": 0, "sent": sent
        }));
    }
    let mut lines = Vec::new();
    let mut stopped = 0usize;
    for turn_id in &operation.turn_ids {
        let Some(turn) = crate::state::bridge_turn_by_id(conn, turn_id)? else {
            lines.push(format!("回合 {turn_id} 已不在记录中"));
            continue;
        };
        match crate::state::bridge_turn_status(conn, turn_id)?.as_deref() {
            Some("running") => {
                let outcome = stop_bridge_turn(conn, telegram, &turn, invocation_id, now)?;
                if outcome.claimed {
                    stopped += 1;
                }
                lines.push(outcome.summary);
            }
            Some("stopping") => lines.push(format!(
                "🧵 {} — 停止中，daemon 在继续确认",
                short_thread_id(&turn.thread_id)
            )),
            status => lines.push(format!(
                "🧵 {} — 已结束（{}）",
                short_thread_id(&turn.thread_id),
                status.unwrap_or("unknown")
            )),
        }
    }
    Ok(json!({
        "ok": true, "action": "telegram_stop", "replayed": true,
        "stopped": stopped, "attempted": operation.turn_ids.len(), "summaries": lines
    }))
}

struct StopOutcome {
    claimed: bool,
    summary: String,
}

/// Kill one turn and settle it, in the order crash-consistency demands.
///
/// The kill happens BEFORE anything commits: a daemon dying in between
/// leaves a row still marked `running`, which the next cycle re-examines and
/// kills again (idempotent). Committing first and crashing before the kill
/// would instead leave a live process that nothing tracks any more.
/// The per-turn summary is enqueued into the OUTBOX inside the same
/// transaction as the turn's state change — never direct-sent. A crash
/// anywhere after that commit can therefore not stop a turn and lose the
/// story; the fast lane delivers within a cycle, and per-turn messages stay
/// far below Telegram's length limit (joint bodies were what once forced
/// chunking and per-chunk receipts here). The key carries the originating
/// command, so two `/stop`s against one turn keep both their receipts.
/// The INTENT half of stopping one turn, run inside the CALLER's
/// transaction: the CAS to `stopping`, the sweep of the dialogs the turn
/// owns (from the moment the user said stop, no owned button may accept an
/// answer), and the REQUESTED receipt. The fresh command path calls this
/// for every selected turn inside ONE transaction — a failure on any turn
/// rolls back all of them plus the operation record, because the
/// alternative left an accepted operation half-executed forever: offset
/// advanced, receipts delivered, later turns still `running` with no
/// scanner ever coming back for them.
fn stop_intent(
    conn: &Connection,
    turn: &crate::state::BridgeTurn,
    invocation_id: &str,
    now: u64,
) -> Result<usize> {
    let label = short_thread_id(&turn.thread_id);
    crate::state::mark_bridge_turn_stopping(conn, &turn.turn_id, now)?;
    let settled_prompts = crate::state::settle_prompts_for_turn(conn, &turn.turn_id, now)?;
    let requested = format!(
        "🧵 {label} — 停止请求已受理，正在终止…{}",
        if settled_prompts > 0 {
            format!("（已关闭 {settled_prompts} 个等待中的对话框）")
        } else {
            String::new()
        }
    );
    enqueue_stop_summary(conn, turn, invocation_id, "requested", &requested, now)?;
    Ok(settled_prompts)
}

/// The SIGNALLING half, strictly AFTER the intent committed. From that
/// commit on the daemon's recovery loop owns completion: an error here (or
/// a crash) leaves the turn `stopping` — re-killed with backoff, settled
/// with its terminal receipt — never a `running` row nobody scans.
///
/// Settle only what we can PROVE is gone. `kill_turn_process` declines to
/// signal when the recorded identity no longer matches the pid (restarted
/// daemon, recycled pid), and reports separately when it signalled but
/// could not reap in the bound. Recording either as `stopped` would drop a
/// live process out of every later scan.
fn stop_execute(
    conn: &Connection,
    telegram: &TelegramConfig,
    turn: &crate::state::BridgeTurn,
    invocation_id: &str,
    settled_prompts: usize,
    now: u64,
) -> Result<StopOutcome> {
    use crate::claude::KillOutcome;
    let label = short_thread_id(&turn.thread_id);
    // The command's own kill is attempt zero — unrecorded, the daemon's
    // next tick would fire a second kill within a second of this one.
    crate::state::record_stop_attempt(conn, &turn.turn_id, now)?;
    // `exited` only means the LEADER was reaped. A grandchild can outlive
    // it and keep doing exactly what the turn was doing, so the kill path
    // runs regardless and the group is what decides.
    let outcome = crate::claude::kill_turn_process(turn);
    if outcome != KillOutcome::Terminated {
        let reason = match outcome {
            KillOutcome::Unverified => {
                "无法确认进程身份（daemon 重启过或 pid 已被复用），拒绝发信号"
            }
            KillOutcome::Undetermined => "已发送终止信号，但未能在限期内确认退出",
            KillOutcome::Terminated => unreachable!(),
        };
        let dialog = if settled_prompts > 0 {
            format!("已关闭其 {settled_prompts} 个等待中的对话框；")
        } else {
            String::new()
        };
        let summary = format!(
            "🧵 {label} — 未确认停止：{reason}。{dialog}回合已标记为停止中，daemon 会持续确认直到整组退出。"
        );
        // Best-effort detail: the durable story is already complete without
        // it (the requested receipt committed with the operation, and the
        // daemon's recovery settle enqueues the final confirmation) — but a
        // failure to record it must still be heard somewhere. The SAME
        // invocation's undelivered "requested" is withdrawn in the same
        // transaction: retried out of order it would arrive after this,
        // promising a termination this message already reported on.
        let queued = (|| -> Result<()> {
            let tx = conn.unchecked_transaction()?;
            crate::state::cancel_pending_push_inner(
                &tx,
                &format!("stop-summary:{invocation_id}:{}:requested", turn.turn_id),
                now,
            )?;
            enqueue_stop_summary(&tx, turn, invocation_id, "outcome", &summary, now)?;
            tx.commit()?;
            Ok(())
        })();
        if let Err(err) = queued {
            eprintln!(
                "tinyctb: failed to queue the stop outcome detail for {}: {err:#}",
                turn.turn_id
            );
        }
        return Ok(StopOutcome {
            claimed: false,
            summary,
        });
    }

    // The log tail is read BEFORE the settle transaction so the summary can
    // commit inside it; the process is dead, the log is stable.
    let tail = crate::claude::turn_log_tail(std::path::Path::new(&turn.log_path), 300);
    let ran_for = now.saturating_sub(turn.started_at) / 1000;

    let tx = conn.unchecked_transaction()?;
    let claimed = crate::state::settle_stopping_turn(&tx, &turn.turn_id, now)?;
    // The dialogs were already swept in the intent transaction; re-running
    // is an idempotent no-op that also catches any prompt that raced its
    // way in between (creation refuses non-running owners, so this is belt
    // and braces, not load-bearing).
    let late_prompts = crate::state::settle_prompts_for_turn(&tx, &turn.turn_id, now)?;
    let settled_prompts = settled_prompts + late_prompts;
    let summary = if !claimed {
        format!("🧵 {label} — 已经结束了，无需停止")
    } else {
        let dialog = if settled_prompts > 0 {
            format!("\n同时关闭了 {settled_prompts} 个等待中的对话框")
        } else {
            String::new()
        };
        format!(
            "🧵 {label} — 已停止（跑了 {ran_for} 秒）{dialog}\n日志尾部：\n{}",
            if tail.is_empty() {
                "(空日志)".to_string()
            } else {
                tail
            }
        )
    };
    // The terminal receipt supersedes every UNDELIVERED "正在终止"/"未确认"
    // still in the queue — for any invocation: row ordering cannot keep a
    // failed-and-retried pre-phase from arriving after this one, so the
    // stale chatter is withdrawn instead. Delivered history stays.
    crate::state::withdraw_undelivered_stop_chatter(&tx, &turn.turn_id, now)?;
    enqueue_stop_summary(&tx, turn, invocation_id, "final", &summary, now)?;
    tx.commit()?;

    if claimed {
        if let Some(dir) = turn
            .cgroup_path
            .as_deref()
            .and_then(|path| crate::claude::turn_cgroup::validated(path, &turn.turn_id))
        {
            // Proven empty and settled: the ownership object has served.
            let _ = crate::claude::turn_cgroup::remove(&dir);
        }
        // Only once NOTHING is still running for this session. Clearing per
        // stopped turn would cancel the bubble for a sibling turn that is
        // still working — including one whose kill could not be confirmed.
        let still_running = crate::state::list_running_bridge_turns(conn)?
            .iter()
            .any(|other| other.thread_id == turn.thread_id);
        if !still_running {
            // Otherwise the "typing…" bubble keeps promising an answer that
            // is never coming, for the rest of its TTL.
            let _ = clear_telegram_typing_indicator(conn, telegram, &turn.thread_id);
        }
    }
    Ok(StopOutcome { claimed, summary })
}

/// One turn end to end — the RESUME path and direct tests: its own intent
/// transaction, then the signalling half. The fresh command path does NOT
/// come through here; its intents batch into a single transaction across
/// every selected turn.
fn stop_bridge_turn(
    conn: &Connection,
    telegram: &TelegramConfig,
    turn: &crate::state::BridgeTurn,
    invocation_id: &str,
    now: u64,
) -> Result<StopOutcome> {
    let tx = conn.unchecked_transaction()?;
    let settled_prompts = stop_intent(&tx, turn, invocation_id, now)?;
    tx.commit()?;
    stop_execute(conn, telegram, turn, invocation_id, settled_prompts, now)
}

/// One durable receipt per (invocation, turn, phase). The invocation keeps
/// two distinct `/stop`s from deduplicating each other's story away while a
/// redelivery of the SAME update stays idempotent; the PHASE keeps a later
/// "final" from colliding with the "requested" receipt of an earlier pass —
/// with a shared key, `INSERT OR IGNORE` kept the stale unconfirmed text
/// and the final outcome was lost.
fn enqueue_stop_summary(
    conn: &Connection,
    turn: &crate::state::BridgeTurn,
    invocation_id: &str,
    phase: &str,
    summary: &str,
    now: u64,
) -> Result<()> {
    let event = json!({
        "type": "bridge_notice",
        "threadId": turn.thread_id,
        "observedAt": now,
        "eventKey": format!("stop-summary:{invocation_id}:{}:{phase}", turn.turn_id),
        // Structured duplicates of what the key encodes: the terminal
        // withdrawal matches on THESE, exactly — parsing the key with LIKE
        // made `_`/`%` in a turn id act as wildcards.
        "stopTurn": turn.turn_id,
        "stopPhase": phase,
        "message": summary,
    });
    crate::state::enqueue_outbound_event(conn, &event, now, "bridge")?;
    Ok(())
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
    let classify_pool = limit.max(THREADS_CLASSIFY_POOL);
    sync_state_from_sessions(conn, &config, now, classify_pool, false)?;
    // Classify BEFORE cutting to `limit`: the limit bounds what is shown,
    // not what competes. Cutting by recency first let a fresh idle session
    // push a slightly older LIVE terminal off the default list — the exact
    // sessions the grouping exists to surface.
    let classified = classify_recent_threads(conn, classify_pool, now)?;
    let snapshots = order_threads_for_display(classified, limit as usize);
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

    // A one-line census up front, so the grouping reads as intentional. The
    // window/background split is the census's whole point: the window count
    // must match what the user can actually see on screen. Counted by
    // primary class (the same key the ordering uses), so the numbers add up
    // to the total even for a live session that also runs a headless turn.
    let count = |wanted: u8| {
        snapshots
            .iter()
            .filter(|(_, liveness, _)| liveness.order() == wanted)
            .count()
    };
    let waiting = snapshots
        .iter()
        .filter(|(_, _, prompt)| prompt.is_some())
        .count();
    let terminal_waiting = snapshots
        .iter()
        .filter(|(snapshot, liveness, prompt)| {
            waiting_rank(snapshot, *liveness, prompt.as_ref()) == 1
        })
        .count();
    let mut census = format!(
        "🧵 {} 个会话：🖥 终端 {} · 🫥 后台 {} · ⚙️ 无头 {} · 💤 空闲 {}",
        snapshots.len(),
        count(0),
        count(1),
        count(2),
        count(3)
    );
    let unknown = count(4);
    if unknown > 0 {
        census.push_str(&format!(" · ❓ 未知 {unknown}"));
    }
    if terminal_waiting > 0 {
        census = format!("🔐 {terminal_waiting} 个会话在终端等你作答\n{census}");
    }
    if waiting > 0 {
        census = format!("⏳ {waiting} 个会话在等你作答！\n{census}");
    }
    telegram_send_text(telegram, &census, timeout)?;

    let mut sent = Vec::with_capacity(snapshots.len());
    let mut render_failed = 0usize;
    for (snapshot, liveness, prompt) in &snapshots {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        // A waiting session's row IS the ask, buttons included — answered
        // through the same pending row the original notification feeds, so
        // whichever message the user answers first wins and the other gets
        // an honest "already handled".
        let prepared = match prompt {
            Some(prompt) => {
                let event = reoffer_prompt_event(conn, telegram, snapshot, *liveness, prompt, now)?;
                prepare_telegram_delivery(&telegram.chat_id, &event)
            }
            None => {
                prepare_telegram_thread_snapshot_delivery(&telegram.chat_id, snapshot, *liveness)
            }
        };
        // ONLY a rendering failure is isolated: it is specific to this one
        // snapshot while the transport still works, so report it in place and
        // keep listing the rest.
        let mut prepared = match prepared {
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
    // Stable and unique per command: the same message replayed keys the same
    // (so a retry deduplicates correctly), while two distinct `/stop`s never
    // collide however similar their effects.
    let command_invocation_id = format!(
        "{chat_id}:{}",
        telegram_message_id(message)
            .map(|id| id.to_string())
            .unwrap_or_else(|| now.to_string())
    );
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
        TelegramInboundCommand::Stop(target) => execute_stop_command(
            conn,
            telegram,
            target.as_deref(),
            &command_invocation_id,
            now,
            timeout,
        ),
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
    // A question button carries the chosen option rather than a permission
    // decision; the blocked hook is waiting on the answer text.
    if let (Some(question_id), Some(answer)) =
        (route.question_id.as_deref(), route.answer.as_deref())
    {
        let outcome = crate::state::record_question_answer(conn, question_id, answer, now)?;
        let toast = match outcome {
            ApprovalAnswer::Recorded => format!("已作答：{answer}"),
            ApprovalAnswer::AlreadyAnswered => "这个问题已经回答过了。".to_string(),
            ApprovalAnswer::Expired => "这个问题已超时，会话已回到终端等待作答。".to_string(),
            ApprovalAnswer::Unknown => "这个按钮已经失效了。".to_string(),
        };
        return Ok(json!({
            "ok": true,
            "action": "telegram_question_answer",
            "questionId": question_id,
            "threadId": route.thread_id,
            "answer": answer,
            "outcome": format!("{outcome:?}"),
            "recorded": outcome == ApprovalAnswer::Recorded,
            "toast": toast
        }));
    }
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
        // Reached only if a question button lost its answer text; refuse
        // rather than guess a permission decision from it.
        TelegramCallbackAction::AnswerQuestion => {
            return Ok(json!({
                "ok": true,
                "action": "telegram_callback_incomplete",
                "toast": "这个按钮已经失效了。"
            }))
        }
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
            TelegramCallbackAction::Deny | TelegramCallbackAction::AnswerQuestion => {
                "已拒绝。".to_string()
            }
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

/// Handle a reply that targets something the session is blocked on: a
/// question (the text IS the answer) or an approval (text is refused —
/// permission needs a button). Returns None for an ordinary reply so normal
/// routing continues.
fn answer_pending_question_from_reply(
    conn: &Connection,
    message: &Value,
    telegram: &TelegramConfig,
    now: u64,
    timeout: Duration,
) -> Result<Option<Value>> {
    let chat_id = telegram_chat_id(message);
    let user_id = telegram_from_user_id(message);
    if !telegram_authorized(telegram, chat_id.as_deref(), user_id.as_deref()) {
        return Ok(None);
    }
    let Some(reply_to) = message
        .get("reply_to_message")
        .and_then(telegram_message_id)
    else {
        return Ok(None);
    };
    let Some(text) = message
        .get("text")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let Some(chat_id) = chat_id else {
        return Ok(None);
    };
    // Recognition is by message, NOT by pending state: a reply to a dialog
    // that has already been answered or timed out is still a reply to that
    // dialog. Treating it as ordinary chat would inject it into the session
    // and leave the user thinking they answered something.
    let Some((kind, ref_id)) = crate::state::dialog_for_message(conn, &chat_id, reply_to)? else {
        return Ok(None);
    };

    if kind == "approval" {
        // Granting permission must stay a deliberate, unambiguous act: a
        // button. Free text can mean anything ("ok", "算了", "不要删"), and
        // mis-reading it as consent is exactly the mistake this feature must
        // never make.
        let notice = match crate::state::approval_decision(conn, &ref_id)? {
            None => "这条是审批请求，文字回复不算授权。请点消息下面的按钮作答（允许 / 本会话都允许 / 拒绝）。",
            Some(decision) if decision == "expired" => {
                "这条审批已超时，会话已回到终端等待处理。文字回复不算授权。"
            }
            Some(_) => "这条审批已经处理过了。文字回复不算授权。",
        };
        let sent = send_telegram_command_text(telegram, notice, timeout)?;
        return Ok(Some(json!({
            "ok": true,
            "action": "telegram_approval_needs_button",
            "approvalId": ref_id,
            "sent": sent
        })));
    }

    let question_id = ref_id;
    let outcome = crate::state::record_question_answer(conn, &question_id, text, now)?;
    let notice = match outcome {
        ApprovalAnswer::Recorded => format!("已作答：{text}"),
        ApprovalAnswer::AlreadyAnswered => "这个问题已经回答过了。".to_string(),
        ApprovalAnswer::Expired => "这个问题已超时，会话已回到终端等待作答。".to_string(),
        ApprovalAnswer::Unknown => "这个问题已经失效了。".to_string(),
    };
    let sent = send_telegram_command_text(telegram, &notice, timeout)?;
    Ok(Some(json!({
        "ok": true,
        "action": "telegram_question_reply",
        "questionId": question_id,
        "answer": text,
        "outcome": format!("{outcome:?}"),
        "sent": sent
    })))
}

/// answerCallbackQuery gets its own SHORT timeout. The tap's validity is
/// ~30s total on Telegram's side, so inheriting the general API timeout
/// (10s) would let three attempts run to ~30.4s worst case — past the very
/// window the retries exist to hit, while dragging the whole update batch
/// behind one tap. Two seconds is generous for this tiny call; three
/// attempts plus backoff bound the worst case at ~6.4s.
const CALLBACK_ACK_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);

/// Answer a callback tap, retrying transient API failures WITHIN the tap's
/// own validity window (a callback id dies on Telegram's side after ~30s —
/// `CALLBACK_ACK_ATTEMPT_TIMEOUT` is what keeps the retries inside it).
///
/// Deliberately NOT retried across cycles: parking the failed update for a
/// later retry would either wedge the whole queue behind one poison update
/// (the batch design acks failures precisely to avoid that) or be skipped by
/// the batch offset anyway — and by the next cycle the id is likely a corpse
/// no answer can reach. So: three spaced attempts here, and past that the
/// failure goes into the inbound log next to the update instead of vanishing
/// into a discarded Result.
fn acknowledge_callback_tap(
    telegram: &TelegramConfig,
    callback_query_id: &str,
    toast: &str,
    timeout: Duration,
) -> std::result::Result<(), String> {
    let attempt_timeout = timeout.min(CALLBACK_ACK_ATTEMPT_TIMEOUT);
    let mut last_error = String::new();
    for attempt in 0..3 {
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(200));
        }
        match send_callback_ack(telegram, callback_query_id, toast, attempt_timeout) {
            Ok(()) => return Ok(()),
            Err(err) => last_error = format!("{err:#}"),
        }
    }
    eprintln!("tinyctb: callback ack failed after retries: {last_error}");
    Err(last_error)
}

fn send_callback_ack(
    telegram: &TelegramConfig,
    callback_query_id: &str,
    toast: &str,
    timeout: Duration,
) -> Result<()> {
    #[cfg(test)]
    {
        let _ = telegram;
        test_callback_acks::attempt(callback_query_id, toast, timeout)
    }
    #[cfg(not(test))]
    {
        self::api::telegram_answer_callback_query(telegram, callback_query_id, toast, timeout)
            .map(|_| ())
    }
}

/// Observation seam for the dispatch layer: tests assert that a tap actually
/// SENT an ack (and what it said), and can inject persistent API failure.
/// Without this, deleting the send call would leave every toast test green.
#[cfg(test)]
pub(crate) mod test_callback_acks {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    pub(crate) static FAIL: AtomicBool = AtomicBool::new(false);
    pub(crate) static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    pub(crate) static RECORDED: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());
    pub(crate) static TIMEOUTS: Mutex<Vec<std::time::Duration>> = Mutex::new(Vec::new());

    /// RAII failure switch: `FAIL` resets on drop even when the test body
    /// panics, so one red test cannot leak `FAIL=true` into every later
    /// test that sends an ack.
    pub(crate) struct FailGuard;

    impl FailGuard {
        pub(crate) fn engage() -> Self {
            FAIL.store(true, Ordering::SeqCst);
            FailGuard
        }
    }

    impl Drop for FailGuard {
        fn drop(&mut self) {
            FAIL.store(false, Ordering::SeqCst);
        }
    }

    pub(crate) fn attempt(
        callback_query_id: &str,
        toast: &str,
        timeout: std::time::Duration,
    ) -> anyhow::Result<()> {
        ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        TIMEOUTS.lock().expect("ack lock").push(timeout);
        if FAIL.load(Ordering::SeqCst) {
            anyhow::bail!("injected ack failure");
        }
        RECORDED
            .lock()
            .expect("ack lock")
            .push((callback_query_id.to_string(), toast.to_string()));
        Ok(())
    }

    pub(crate) fn take_timeouts() -> Vec<std::time::Duration> {
        std::mem::take(&mut TIMEOUTS.lock().expect("ack lock"))
    }

    pub(crate) fn take() -> Vec<(String, String)> {
        std::mem::take(&mut RECORDED.lock().expect("ack lock"))
    }

    pub(crate) fn reset_attempts() -> usize {
        ATTEMPTS.swap(0, Ordering::SeqCst)
    }
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
                // A reply to a question the session is blocked on IS the
                // answer — injecting it as a new user message would leave
                // the question hanging.
                if let Some(result) =
                    answer_pending_question_from_reply(conn, message, telegram, now, timeout)?
                {
                    if let Some(update_id) = update_id {
                        record_telegram_inbound_processed(
                            conn,
                            bot_id,
                            update_id,
                            "telegram_question_reply",
                            &result,
                            TelegramInboundLogContext {
                                route_message_id,
                                result_action: result.get("action").and_then(Value::as_str),
                                ..TelegramInboundLogContext::default()
                            },
                            now,
                        )?;
                    }
                    replies += 1;
                } else if let Some(route) = extract_telegram_reply_route(conn, message, telegram)? {
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
                    TelegramCallbackLookup::Route(route) => {
                        let mut answer = record_callback_answer(conn, &route, now)?;
                        mark_telegram_callback_route_used(conn, &route.callback_id, now)?;
                        let ack = acknowledge_callback_tap(
                            telegram,
                            &route.callback_query_id,
                            answer
                                .get("toast")
                                .and_then(Value::as_str)
                                .unwrap_or("已收到"),
                            timeout,
                        );
                        // The update stays acked even when the toast could
                        // not be delivered (queue protection), but the
                        // failure is recorded, not discarded.
                        if let Some(object) = answer.as_object_mut() {
                            object.insert("ackDelivered".to_string(), json!(ack.is_ok()));
                            if let Err(error) = &ack {
                                object.insert("ackError".to_string(), json!(error));
                            }
                        }
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
                    TelegramCallbackLookup::Spent {
                        callback_query_id,
                        toast,
                    } => {
                        // The tap gets its toast even though nothing changed;
                        // silence here left the client spinning for ~30s and
                        // then showing nothing at all.
                        let ack =
                            acknowledge_callback_tap(telegram, &callback_query_id, toast, timeout);
                        if let Some(update_id) = update_id {
                            record_telegram_inbound_processed(
                                conn,
                                bot_id,
                                update_id,
                                "callback_query_spent",
                                &json!({
                                    "toast": toast,
                                    "ackDelivered": ack.is_ok(),
                                    "ackError": ack.err()
                                }),
                                TelegramInboundLogContext {
                                    route_message_id,
                                    ..TelegramInboundLogContext::default()
                                },
                                now,
                            )?;
                        }
                        ignored += 1;
                    }
                    TelegramCallbackLookup::Foreign => {
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

    use crate::{write_daemon_config, DaemonConfig, TelegramConfig};

    fn config_test_lock() -> std::sync::MutexGuard<'static, ()> {
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
        let _guard = config_test_lock();
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

    fn stop_test_config() -> TelegramConfig {
        TelegramConfig {
            bot_token: "123:secret".to_string(),
            chat_id: "456".to_string(),
            allowed_user_id: Some("789".to_string()),
        }
    }

    fn register_running_turn(conn: &Connection, turn: &str, thread: &str, pid: u32, at: u64) {
        crate::state::register_bridge_turn(
            conn,
            turn,
            thread,
            "/tmp/turn.log",
            Some(pid),
            None,
            None,
            None,
            None,
            None,
            at,
        )
        .expect("register turn");
    }

    #[test]
    fn stop_command_parses_with_and_without_a_target() {
        assert_eq!(
            parse_telegram_command_text("/stop"),
            Some(TelegramInboundCommand::Stop(None))
        );
        assert_eq!(
            parse_telegram_command_text("/stop 3ddecbe8"),
            Some(TelegramInboundCommand::Stop(Some("3ddecbe8".to_string())))
        );
        assert_eq!(
            parse_telegram_command_text("/stop@tinyctb_bot"),
            Some(TelegramInboundCommand::Stop(None))
        );
    }

    #[test]
    fn stop_with_nothing_running_says_so() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let result = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:1",
            1000,
            Duration::from_secs(1),
        )
        .expect("stop");
        assert_eq!(result["ok"], true);
        assert_eq!(result["stopped"], 0);
    }

    /// The core path: kill, settle as `stopped` (not `failed`, which would
    /// make the daemon push an error for something the user chose), close
    /// the dialogs THIS turn owned, and withdraw their queued buttons.
    #[test]
    fn stop_kills_settles_and_withdraws_the_button() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        register_running_turn(&conn, "turn-1", "sess-runaway", 4_000_001, 1000);
        crate::state::create_pending_approval(
            &conn,
            "ap-1",
            "sess-runaway",
            "Bash",
            "rm -rf /",
            true,
            1000,
            9_000_000_000,
        )
        .expect("approval");
        crate::state::record_approval_turn_owner(&conn, "ap-1", "turn-1").expect("owner");
        crate::state::enqueue_outbound_event(
            &conn,
            &json!({
                "type": "approval_request", "threadId": "sess-runaway",
                "eventKey": "approval:ap-1", "updatedAt": 1000
            }),
            1000,
            "bridge",
        )
        .expect("queue button");

        let result = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:1",
            5000,
            Duration::from_secs(1),
        )
        .expect("stop");

        assert_eq!(result["stopped"], 1);
        assert_eq!(
            crate::claude::test_kill::take(),
            vec![4_000_001],
            "the turn's process group must actually be signalled"
        );
        let status: String = conn
            .query_row(
                "SELECT status FROM bridge_turns WHERE turn_id = 'turn-1'",
                [],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(status, "stopped", "a deliberate stop is not a failure");
        let decision: Option<String> = conn
            .query_row(
                "SELECT decision FROM pending_approvals WHERE approval_id = 'ap-1'",
                [],
                |row| row.get(0),
            )
            .expect("approval");
        assert_eq!(decision.as_deref(), Some("expired"));
        let button: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events
                 WHERE json_extract(payload_json, '$.eventKey') = 'approval:ap-1'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(
            button, 0,
            "the button must be withdrawn, not delivered after the turn is gone"
        );
        let summary: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events
                 WHERE json_extract(payload_json, '$.eventKey') = 'stop-summary:456:1:turn-1:final'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(
            summary, 1,
            "the outcome must be a durable outbox row, committed with the settle"
        );
    }

    /// Ownership is by TURN. A concurrent turn's approval, and a question
    /// from the user's own terminal, must both survive — and an approval
    /// with no recorded owner fails OPEN rather than being swept up.
    #[test]
    fn stop_only_closes_the_dialogs_its_own_turn_owned() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        register_running_turn(&conn, "turn-doomed", "sess-shared", 4_000_010, 1000);
        register_running_turn(&conn, "turn-other", "sess-shared", 4_000_011, 1000);
        for (id, owner) in [
            ("ap-mine", Some("turn-doomed")),
            ("ap-sibling", Some("turn-other")),
            ("ap-orphan", None),
        ] {
            crate::state::create_pending_approval(
                &conn,
                id,
                "sess-shared",
                "Bash",
                "x",
                true,
                1000,
                9_000_000_000,
            )
            .expect("approval");
            if let Some(owner) = owner {
                crate::state::record_approval_turn_owner(&conn, id, owner).expect("owner");
            }
        }
        crate::state::create_pending_question(
            &conn,
            "q-terminal",
            "sess-shared",
            "选哪个？",
            &["A".to_string()],
            false,
            1000,
            9_000_000_000,
        )
        .expect("question");

        // Stop only the doomed turn (both share a session, so target by turn
        // is impossible — this is the multi-turn-per-session case).
        let turn = crate::state::list_running_bridge_turns(&conn)
            .expect("turns")
            .into_iter()
            .find(|turn| turn.turn_id == "turn-doomed")
            .expect("doomed turn");
        stop_bridge_turn(&conn, &stop_test_config(), &turn, "456:1", 5000).expect("stop");

        let open = |id: &str| -> Option<String> {
            conn.query_row(
                "SELECT decision FROM pending_approvals WHERE approval_id = ?1",
                params![id],
                |row| row.get(0),
            )
            .expect("row")
        };
        assert_eq!(open("ap-mine").as_deref(), Some("expired"));
        assert_eq!(
            open("ap-sibling"),
            None,
            "a concurrent turn's approval must survive"
        );
        assert_eq!(
            open("ap-orphan"),
            None,
            "unknown ownership must fail open, not be swept up"
        );
        let answered: Option<String> = conn
            .query_row(
                "SELECT answer FROM pending_questions WHERE question_id = 'q-terminal'",
                [],
                |row| row.get(0),
            )
            .expect("question");
        assert_eq!(
            answered, None,
            "questions are never headless, so /stop must not touch them"
        );
    }

    /// A kill that could not be CONFIRMED must not settle the turn. Marking
    /// it `stopped` would drop a live process out of every later scan, and
    /// nothing would ever reap it — the failure mode is a runaway turn that
    /// the bridge has forgotten about.
    #[test]
    fn an_unconfirmed_kill_leaves_the_turn_running() {
        let _guard = crate::state::test_env_lock();
        for outcome in [
            crate::claude::KillOutcome::Unverified,
            crate::claude::KillOutcome::Undetermined,
        ] {
            let conn = crate::state::create_state_db_in_memory().expect("db");
            let _ = crate::claude::test_kill::take();
            let _kill_guard = crate::claude::test_kill::OutcomeGuard::set(outcome);
            register_running_turn(&conn, "turn-x", "sess-x", 4_000_030, 1000);

            let result = execute_stop_command(
                &conn,
                &stop_test_config(),
                None,
                "456:1",
                5000,
                Duration::from_secs(1),
            )
            .expect("stop");

            assert_eq!(result["stopped"], 0, "{outcome:?} must not count as a stop");
            let status: String = conn
                .query_row(
                    "SELECT status FROM bridge_turns WHERE turn_id = 'turn-x'",
                    [],
                    |row| row.get(0),
                )
                .expect("row");
            assert_eq!(
                status, "stopping",
                "{outcome:?}: an unconfirmed kill leaves the INTENT, never `stopped`"
            );
            assert!(
                crate::state::list_running_bridge_turns(&conn)
                    .expect("turns")
                    .iter()
                    .any(|turn| turn.turn_id == "turn-x"),
                "{outcome:?}: and it must stay tracked — its process may still be alive"
            );
        }
    }

    /// A prefix that matches two sessions must be refused, not fanned out.
    #[test]
    fn stop_refuses_a_short_or_ambiguous_target() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        register_running_turn(&conn, "turn-a", "abcd1234-aaaa", 4_000_020, 1000);
        register_running_turn(&conn, "turn-b", "abcd1234-bbbb", 4_000_021, 1000);

        let short = execute_stop_command(
            &conn,
            &stop_test_config(),
            Some("abcd"),
            "456:1",
            5000,
            Duration::from_secs(1),
        )
        .expect("short");
        assert_eq!(short["reason"], "target_too_short");

        let ambiguous = execute_stop_command(
            &conn,
            &stop_test_config(),
            Some("abcd1234"),
            "456:2",
            5000,
            Duration::from_secs(1),
        )
        .expect("ambiguous");
        assert_eq!(ambiguous["reason"], "ambiguous_target");
        // The SAME update redelivered repeats the frozen refusal — even
        // though the target still matches two turns it could reinterpret.
        let replay = execute_stop_command(
            &conn,
            &stop_test_config(),
            Some("abcd1234"),
            "456:2",
            6000,
            Duration::from_secs(1),
        )
        .expect("replay");
        assert_eq!(replay["replayed"], true);
        assert_eq!(replay["kind"], "ambiguous");
        assert_eq!(ambiguous["matched"], 2);
        assert!(
            crate::claude::test_kill::take().is_empty(),
            "an ambiguous target must kill nothing at all"
        );
        let running: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM bridge_turns WHERE status = 'running'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(running, 2, "both turns must survive an ambiguous /stop");
    }

    /// Mixed outcome: one turn confirmed dead, a sibling in the same session
    /// still running. The typing bubble must SURVIVE — cancelling it would
    /// tell the user nothing is working while something still is.
    #[test]
    fn typing_survives_while_a_sibling_turn_is_still_running() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        let telegram = stop_test_config();
        register_running_turn(&conn, "turn-dead", "sess-shared", 4_000_040, 1000);
        register_running_turn(&conn, "turn-alive", "sess-shared", 4_000_041, 1000);
        register_telegram_typing_indicator(&conn, &telegram, "sess-shared", 1000)
            .expect("typing on");
        let typing_key = telegram_typing_key(&telegram.chat_id, "sess-shared");
        assert!(
            crate::state::get_setting_text(&conn, &typing_key)
                .expect("read")
                .is_some(),
            "the bubble must be on before we start"
        );

        let dead = crate::state::list_running_bridge_turns(&conn)
            .expect("turns")
            .into_iter()
            .find(|turn| turn.turn_id == "turn-dead")
            .expect("turn");
        let _kill_guard =
            crate::claude::test_kill::OutcomeGuard::set(crate::claude::KillOutcome::Terminated);
        stop_bridge_turn(&conn, &telegram, &dead, "456:1", 5000).expect("stop");

        assert!(
            crate::state::get_setting_text(&conn, &typing_key)
                .expect("read")
                .is_some(),
            "a sibling is still running, so the bubble must stay"
        );

        // Now stop the sibling too: with nothing left, the bubble goes.
        let alive = crate::state::list_running_bridge_turns(&conn)
            .expect("turns")
            .into_iter()
            .find(|turn| turn.turn_id == "turn-alive")
            .expect("turn");
        let _kill_guard =
            crate::claude::test_kill::OutcomeGuard::set(crate::claude::KillOutcome::Terminated);
        stop_bridge_turn(&conn, &telegram, &alive, "456:2", 6000).expect("stop");
        assert!(
            crate::state::get_setting_text(&conn, &typing_key)
                .expect("read")
                .is_none(),
            "with no turn left the bubble must be cleared"
        );
    }

    /// The intent is persisted BEFORE the signal, so a crash in between
    /// still says what happened. Without it the row stays `running` and the
    /// daemon reports a deliberate stop as "exited without producing an
    /// answer".
    #[test]
    fn a_stop_records_its_intent_before_signalling() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        register_running_turn(&conn, "turn-1", "sess-1", 4_000_050, 1000);
        // The kill cannot be confirmed, so nothing promotes the row to
        // `stopped` — exactly the window where only the intent survives.
        let _kill_guard =
            crate::claude::test_kill::OutcomeGuard::set(crate::claude::KillOutcome::Undetermined);
        let turn = crate::state::list_running_bridge_turns(&conn)
            .expect("turns")
            .into_iter()
            .next()
            .expect("turn");
        stop_bridge_turn(&conn, &stop_test_config(), &turn, "456:1", 5000).expect("stop");

        assert_eq!(
            crate::state::bridge_turn_status(&conn, "turn-1").expect("status"),
            Some("stopping".to_string()),
            "an unconfirmed kill must leave the INTENT behind, not a bare running row"
        );
        assert!(
            crate::state::list_running_bridge_turns(&conn)
                .expect("turns")
                .iter()
                .any(|turn| turn.turn_id == "turn-1"),
            "and a stopping turn must stay tracked — its process may still be alive"
        );
        assert_eq!(
            crate::state::stop_attempt_state(&conn, "turn-1").expect("state"),
            (1, 5000),
            "the command's own kill must count as attempt zero, or the daemon's \
             next tick re-kills within a second"
        );
    }

    /// The stop OPERATION and its receipts are all-or-nothing, proven by
    /// sabotage: when the receipts cannot be queued, the whole command must
    /// fail before any state changes or signals — a turn must never go
    /// `stopping` while the user holds no durable record of asking, and a
    /// half-recorded operation must not exist for a replay to trust.
    #[test]
    fn a_stop_is_all_or_nothing_with_its_receipt() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        register_running_turn(&conn, "turn-1", "sess-1", 4_000_058, 1000);
        conn.execute_batch(
            "CREATE TRIGGER outbox_broken BEFORE INSERT ON outbound_events
             BEGIN SELECT RAISE(ABORT, 'outbox broken'); END;",
        )
        .expect("trigger");

        let result = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:31",
            5000,
            Duration::from_secs(1),
        );

        assert!(result.is_err(), "a broken outbox must fail the command");
        assert_eq!(
            crate::state::bridge_turn_status(&conn, "turn-1").expect("status"),
            Some("running".to_string()),
            "no receipt → no operation: the turn must stay running for the user's next /stop"
        );
        assert!(
            crate::claude::test_kill::take().is_empty(),
            "and nothing may have been signalled"
        );
        let operations: i64 = conn
            .query_row("SELECT COUNT(*) FROM stop_operations", [], |row| row.get(0))
            .expect("count");
        assert_eq!(
            operations, 0,
            "a rolled-back operation must not exist for a replay to trust"
        );
    }

    /// The ACCEPTANCE is one transaction across every selected turn: an
    /// intent failure on any turn rolls back ALL of them plus the operation
    /// record and its receipts. Committed piecemeal, an intent error midway
    /// left earlier turns stopped and later ones running forever — with the
    /// receipts delivered, the offset advanced, and no scanner ever coming
    /// back for rows still `running`.
    #[test]
    fn a_stop_batch_is_atomic_across_turns() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        register_running_turn(&conn, "turn-1", "sess-a", 4_000_072, 1000);
        register_running_turn(&conn, "turn-2", "sess-b", 4_000_073, 2000);
        conn.execute_batch(
            "CREATE TRIGGER intent_broken BEFORE UPDATE OF status ON bridge_turns
             WHEN NEW.status = 'stopping' AND NEW.turn_id = 'turn-2'
             BEGIN SELECT RAISE(ABORT, 'intent broken'); END;",
        )
        .expect("trigger");

        let result = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:51",
            5000,
            Duration::from_secs(1),
        );

        assert!(
            result.is_err(),
            "a broken intent must fail the whole command"
        );
        for turn in ["turn-1", "turn-2"] {
            assert_eq!(
                crate::state::bridge_turn_status(&conn, turn).expect("status"),
                Some("running".to_string()),
                "{turn} must roll back with the batch — no half-accepted operation"
            );
        }
        assert!(
            crate::claude::test_kill::take().is_empty(),
            "and nothing may have been signalled"
        );
        let operations: i64 = conn
            .query_row("SELECT COUNT(*) FROM stop_operations", [], |row| row.get(0))
            .expect("count");
        assert_eq!(operations, 0, "no operation row for a replay to trust");
        let receipts: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(
            receipts, 0,
            "and no receipt promising work that never began"
        );
    }

    /// The exact counterexample from review: `/stop` sees nothing running
    /// and says so; the daemon dies before the ack; a NEW turn starts; the
    /// same update is redelivered. The frozen resolution must repeat
    /// "nothing to stop" — reinterpreting would kill a turn that did not
    /// exist when the user sent the command.
    #[test]
    fn a_replayed_empty_stop_never_kills_later_turns() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();

        let first = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:41",
            5000,
            Duration::from_secs(1),
        )
        .expect("first");
        assert_eq!(first["stopped"], 0);

        register_running_turn(&conn, "turn-late", "sess-late", 4_000_066, 6000);

        let replay = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:41",
            7000,
            Duration::from_secs(1),
        )
        .expect("replay");
        assert_eq!(replay["replayed"], true, "{replay}");
        assert_eq!(replay["kind"], "empty");
        assert!(
            crate::claude::test_kill::take().is_empty(),
            "the frozen empty answer must not become a kill"
        );
        assert_eq!(
            crate::state::bridge_turn_status(&conn, "turn-late").expect("status"),
            Some("running".to_string()),
            "the turn that started after the command must be untouched"
        );

        // A genuinely new command still reaches it.
        let _kill_guard =
            crate::claude::test_kill::OutcomeGuard::set(crate::claude::KillOutcome::Terminated);
        let fresh = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:42",
            8000,
            Duration::from_secs(1),
        )
        .expect("fresh");
        assert_eq!(fresh["stopped"], 1);
    }

    /// A redelivered `/stop` update resumes its RECORDED operation instead
    /// of reinterpreting the command over the present: after the stop
    /// committed, a replay used to answer "nothing is running" while the
    /// outbox was saying "stopped".
    #[test]
    fn a_replayed_stop_resumes_its_operation_not_the_present() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        register_running_turn(&conn, "turn-1", "sess-1", 4_000_055, 1000);
        let _kill_guard =
            crate::claude::test_kill::OutcomeGuard::set(crate::claude::KillOutcome::Terminated);

        let first = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:21",
            5000,
            Duration::from_secs(1),
        )
        .expect("stop");
        assert_eq!(first["stopped"], 1);
        let events_after_first: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");

        // The SAME update again (crash-before-ack redelivery).
        let replay = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:21",
            6000,
            Duration::from_secs(1),
        )
        .expect("replay");
        assert_eq!(replay["replayed"], true, "{replay}");
        assert_eq!(
            replay["attempted"], 1,
            "the replay must speak about the RECORDED turn, not the empty present"
        );
        let line = replay["summaries"][0].as_str().expect("line");
        assert!(
            line.contains("已结束") && line.contains("stopped"),
            "the replay reports the recorded turn's fate: {line}"
        );
        let events_after_replay: i64 = conn
            .query_row("SELECT COUNT(*) FROM outbound_events", [], |row| row.get(0))
            .expect("count");
        assert_eq!(
            events_after_first, events_after_replay,
            "a replay must not mint new receipts for work already receipted"
        );

        // A genuinely NEW command with nothing running still says so.
        let fresh = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:22",
            7000,
            Duration::from_secs(1),
        )
        .expect("fresh");
        assert_eq!(fresh["stopped"], 0);
    }

    /// Every outcome is a durable receipt COMMITTED WITH the state change
    /// itself, keyed by the invocation — deleting the production enqueue in
    /// `stop_bridge_turn` turns this red. Two `/stop`s against the same
    /// turn keep both receipts (no deduplication), while a redelivery of
    /// the SAME update stays idempotent by key.
    #[test]
    fn stop_summaries_are_durable_and_keyed_by_invocation() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        register_running_turn(&conn, "turn-1", "sess-1", 4_000_070, 1000);
        // The kill stays unconfirmed, so the turn survives as `stopping`
        // and the second `/stop` has the same turn to act on.
        let _kill_guard =
            crate::claude::test_kill::OutcomeGuard::set(crate::claude::KillOutcome::Undetermined);

        let first = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:11",
            5000,
            Duration::from_secs(1),
        )
        .expect("stop");
        assert_eq!(first["queuedSummaries"], 1);
        let second = execute_stop_command(
            &conn,
            &stop_test_config(),
            None,
            "456:12",
            6000,
            Duration::from_secs(1),
        )
        .expect("stop again");
        assert_eq!(second["queuedSummaries"], 1);

        let keys: Vec<String> = {
            let mut stmt = conn
                .prepare(
                    "SELECT json_extract(payload_json, '$.eventKey') FROM outbound_events
                     ORDER BY 1",
                )
                .expect("stmt");
            let rows = stmt
                .query_map([], |row| row.get(0))
                .expect("query")
                .collect::<rusqlite::Result<Vec<String>>>()
                .expect("rows");
            rows
        };
        assert_eq!(
            keys,
            vec![
                "stop-summary:456:11:turn-1:outcome".to_string(),
                "stop-summary:456:12:turn-1:outcome".to_string(),
            ],
            "each invocation keeps its own LATEST receipt; its superseded \
             undelivered `requested` is withdrawn so a retry cannot deliver \
             it after the outcome it precedes"
        );
    }

    /// The buttons die at the INTENT: even a kill that cannot be confirmed
    /// must leave no live dialog behind. Until this was transactional with
    /// `stopping`, an Undetermined kill kept the buttons alive and a later
    /// tap handed the blocked headless gate an `allow` for a turn the user
    /// had already ended.
    #[test]
    fn a_stop_that_cannot_confirm_still_kills_the_buttons() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        register_running_turn(&conn, "turn-1", "sess-1", 4_000_060, 1000);
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
        crate::state::enqueue_outbound_event(
            &conn,
            &json!({
                "type": "approval_request", "threadId": "sess-1",
                "eventKey": "approval:ap-1", "updatedAt": 1000
            }),
            1000,
            "bridge",
        )
        .expect("queue button");
        let _kill_guard =
            crate::claude::test_kill::OutcomeGuard::set(crate::claude::KillOutcome::Undetermined);
        let turn = crate::state::list_running_bridge_turns(&conn)
            .expect("turns")
            .into_iter()
            .next()
            .expect("turn");

        stop_bridge_turn(&conn, &stop_test_config(), &turn, "456:1", 5000).expect("stop");

        assert_eq!(
            crate::state::bridge_turn_status(&conn, "turn-1").expect("status"),
            Some("stopping".to_string()),
            "an unconfirmed kill leaves the turn stopping"
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
            "the dialog must die with the INTENT, not with the settle"
        );
        let button: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outbound_events
                 WHERE json_extract(payload_json, '$.eventKey') = 'approval:ap-1'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(
            button, 0,
            "and its queued button must be withdrawn even though the kill is unconfirmed"
        );
    }

    /// The ambiguity message has to be ACTIONABLE. Printing the same
    /// truncated eight characters twice asks for a longer prefix while
    /// hiding the very characters that would make one.
    #[test]
    fn the_ambiguity_message_shows_distinguishing_prefixes() {
        let ids: std::collections::BTreeSet<&str> =
            ["abcd1234-aaaa", "abcd1234-bbbb"].into_iter().collect();
        let first = disambiguating_prefix("abcd1234-aaaa", &ids);
        let second = disambiguating_prefix("abcd1234-bbbb", &ids);
        assert_ne!(
            first, second,
            "two candidates must not print identically: {first} vs {second}"
        );
        assert!(first.starts_with("abcd1234-a"), "got {first}");
        assert!(second.starts_with("abcd1234-b"), "got {second}");
        // And a unique id still gets the short form.
        let alone: std::collections::BTreeSet<&str> = ["ffff0000-zzzz"].into_iter().collect();
        assert_eq!(disambiguating_prefix("ffff0000-zzzz", &alone), "ffff0000");
    }

    /// /stop can only reach rows this bridge created. A session opened in the
    /// user's own terminal has no `bridge_turns` row at all.
    #[test]
    fn stop_never_touches_a_session_without_a_bridge_turn() {
        let _guard = crate::state::test_env_lock();
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_kill::take();
        let result = execute_stop_command(
            &conn,
            &stop_test_config(),
            Some("sess-terminal"),
            "456:1",
            5000,
            Duration::from_secs(1),
        )
        .expect("stop");
        assert_eq!(result["stopped"], 0);
        assert!(crate::claude::test_kill::take().is_empty());
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
        let _guard = crate::state::test_env_lock();
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

        let _guard = crate::state::test_env_lock();
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
            question_id: None,
            answer: None,
        };

        crate::state::create_pending_approval(
            &conn,
            "ap-ok",
            "sess-toast",
            "Bash",
            "Bash: ls",
            false,
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
            false,
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

    /// Permission is granted by a button and nothing else. A text reply to
    /// an approval must NOT count as consent, and must not be injected into
    /// the session either — that would look like an answer to the user while
    /// the approval quietly timed out.
    #[test]
    fn text_reply_never_authorises_an_approval() {
        let _guard = crate::state::test_env_lock();
        let _env = CommandEnv::new("text-not-consent");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("config");
        let telegram = config.telegram.clone().expect("telegram");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::create_pending_approval(
            &conn,
            "ap-text",
            "sess-x",
            "Bash",
            "Bash: rm -rf /",
            false,
            1000,
            9000,
        )
        .expect("create");
        crate::state::attach_approval_message_id(&conn, "ap-text", 77).expect("attach");
        crate::state::record_dialog_message(&conn, "456", 77, "approval", "ap-text", 1000)
            .expect("dialog message");
        // The approval message is also a routable thread message, which is
        // exactly how a text reply used to get injected instead.
        crate::state::insert_telegram_message_route(&conn, "456", 77, "sess-x", "e", 1000)
            .expect("route");
        let _ = crate::claude::test_spawn::take();

        let reply = json!({
            "message_id": 78,
            "chat": { "id": "456" },
            "from": { "id": "789" },
            "reply_to_message": { "message_id": 77 },
            "text": "允许"
        });
        let result = answer_pending_question_from_reply(
            &conn,
            &reply,
            &telegram,
            2000,
            Duration::from_secs(1),
        )
        .expect("handled")
        .expect("an approval reply must be intercepted");

        assert_eq!(result["action"], "telegram_approval_needs_button");
        assert_eq!(
            crate::state::approval_decision(&conn, "ap-text").expect("decision"),
            None,
            "text must never grant permission"
        );
        assert!(
            crate::claude::test_spawn::take().is_empty(),
            "and must not be injected into the session as a message"
        );
    }

    /// The same text, replying to a QUESTION, is a legitimate answer — it
    /// carries information, never permission.
    #[test]
    fn text_reply_answers_a_question_but_only_ever_as_content() {
        let _guard = crate::state::test_env_lock();
        let _env = CommandEnv::new("text-is-answer");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("config");
        let telegram = config.telegram.clone().expect("telegram");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::create_pending_question(
            &conn,
            "q-text",
            "sess-x",
            "按优先级排列这几项",
            &[],
            false,
            1000,
            9000,
        )
        .expect("create");
        crate::state::attach_question_message_id(&conn, "q-text", 88).expect("attach");
        crate::state::record_dialog_message(&conn, "456", 88, "question", "q-text", 1000)
            .expect("dialog message");

        let reply = json!({
            "message_id": 89,
            "chat": { "id": "456" },
            "from": { "id": "789" },
            "reply_to_message": { "message_id": 88 },
            "text": "3,1,2"
        });
        let result = answer_pending_question_from_reply(
            &conn,
            &reply,
            &telegram,
            2000,
            Duration::from_secs(1),
        )
        .expect("handled")
        .expect("a question reply must be taken as the answer");

        assert_eq!(result["action"], "telegram_question_reply");
        assert_eq!(
            crate::state::question_answer(&conn, "q-text").expect("answer"),
            Some("3,1,2".to_string()),
            "free text is how an ordering is given"
        );
    }

    /// A dialog that is already settled is still a dialog: replying to it
    /// must be consumed with an honest status, never leak into the session
    /// as chat.
    #[test]
    fn reply_to_a_settled_dialog_is_still_recognised() {
        let _guard = crate::state::test_env_lock();
        let _env = CommandEnv::new("settled-dialog");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("config");
        let telegram = config.telegram.clone().expect("telegram");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let _ = crate::claude::test_spawn::take();

        // An approval that already timed out, and an answered question.
        crate::state::create_pending_approval(
            &conn, "ap-done", "sess-x", "Bash", "Bash: ls", false, 1000, 5000,
        )
        .expect("create");
        crate::state::record_dialog_message(&conn, "456", 10, "approval", "ap-done", 1000)
            .expect("dialog");
        crate::state::insert_telegram_message_route(&conn, "456", 10, "sess-x", "e", 1000)
            .expect("route");
        crate::state::expire_or_take_decision(&conn, "ap-done", 6000).expect("expire");

        crate::state::create_pending_question(
            &conn,
            "q-done",
            "sess-x",
            "哪个?",
            &[],
            false,
            1000,
            9000,
        )
        .expect("create");
        crate::state::record_dialog_message(&conn, "456", 20, "question", "q-done", 1000)
            .expect("dialog");
        crate::state::insert_telegram_message_route(&conn, "456", 20, "sess-x", "e", 1000)
            .expect("route");
        crate::state::record_question_answer(&conn, "q-done", "SQLite", 2000).expect("answer");

        for (message_id, expected, needle) in [
            (10, "telegram_approval_needs_button", "超时"),
            (20, "telegram_question_reply", ""),
        ] {
            let reply = json!({
                "message_id": message_id + 1,
                "chat": { "id": "456" },
                "from": { "id": "789" },
                "reply_to_message": { "message_id": message_id },
                "text": "随便说点什么"
            });
            let result = answer_pending_question_from_reply(
                &conn,
                &reply,
                &telegram,
                7000,
                Duration::from_secs(1),
            )
            .expect("handled")
            .expect("a settled dialog must still be recognised");
            assert_eq!(result["action"], expected, "{result}");
            if !needle.is_empty() {
                assert!(
                    result["sent"]["result"]["text"]
                        .as_str()
                        .unwrap_or_default()
                        .contains(needle),
                    "the notice must state the real status: {result}"
                );
            }
        }
        // The answered question keeps its first answer.
        assert_eq!(
            crate::state::question_answer(&conn, "q-done").expect("answer"),
            Some("SQLite".to_string())
        );
        assert!(
            crate::claude::test_spawn::take().is_empty(),
            "no reply to a dialog may be injected into the session"
        );
    }

    /// A long question is split across several Telegram messages; replying to
    /// ANY chunk must still count as replying to that dialog.
    #[test]
    fn reply_to_any_chunk_of_a_split_dialog_is_recognised() {
        let _guard = crate::state::test_env_lock();
        let _env = CommandEnv::new("split-dialog");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("config");
        let telegram = config.telegram.clone().expect("telegram");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::create_pending_question(
            &conn,
            "q-split",
            "sess-x",
            "很长的问题",
            &[],
            false,
            1000,
            9000,
        )
        .expect("create");
        // Delivery records every chunk; the keyboard rides the last one.
        for message_id in [31, 32, 33] {
            crate::state::record_dialog_message(
                &conn, "456", message_id, "question", "q-split", 1000,
            )
            .expect("dialog");
            crate::state::insert_telegram_message_route(
                &conn, "456", message_id, "sess-x", "e", 1000,
            )
            .expect("route");
        }

        // Reply to the FIRST chunk — the one that used to fall through.
        let reply = json!({
            "message_id": 40,
            "chat": { "id": "456" },
            "from": { "id": "789" },
            "reply_to_message": { "message_id": 31 },
            "text": "3,1,2"
        });
        let result = answer_pending_question_from_reply(
            &conn,
            &reply,
            &telegram,
            2000,
            Duration::from_secs(1),
        )
        .expect("handled")
        .expect("a reply to the first chunk must answer the dialog");
        assert_eq!(result["action"], "telegram_question_reply");
        assert_eq!(
            crate::state::question_answer(&conn, "q-split").expect("answer"),
            Some("3,1,2".to_string())
        );
    }

    /// A reply from the wrong chat or user answers nothing.
    #[test]
    fn unauthorised_reply_cannot_answer_anything() {
        let _guard = crate::state::test_env_lock();
        let _env = CommandEnv::new("text-unauthorised");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("config");
        let telegram = config.telegram.clone().expect("telegram");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::create_pending_question(
            &conn,
            "q-auth",
            "sess-x",
            "哪个?",
            &[],
            false,
            1000,
            9000,
        )
        .expect("create");
        crate::state::attach_question_message_id(&conn, "q-auth", 90).expect("attach");
        crate::state::record_dialog_message(&conn, "456", 90, "question", "q-auth", 1000)
            .expect("dialog message");

        for (chat, user) in [("999", "789"), ("456", "111")] {
            let reply = json!({
                "message_id": 91,
                "chat": { "id": chat },
                "from": { "id": user },
                "reply_to_message": { "message_id": 90 },
                "text": "SQLite"
            });
            assert!(
                answer_pending_question_from_reply(
                    &conn,
                    &reply,
                    &telegram,
                    2000,
                    Duration::from_secs(1)
                )
                .expect("handled")
                .is_none(),
                "chat {chat} / user {user} must not be able to answer"
            );
        }
        assert_eq!(
            crate::state::question_answer(&conn, "q-auth").expect("answer"),
            None
        );
    }

    #[test]
    fn remote_commands_toggle_away_mode_and_manage_hooks() {
        let _guard = crate::state::test_env_lock();
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
        let _guard = crate::state::test_env_lock();
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
        let _guard = config_test_lock();
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

    /// The dispatch layer must actually SEND the ack for both branches —
    /// the lookup-level test alone would stay green if the send call were
    /// deleted. Drives a real double-tap through the update batch.
    #[test]
    fn every_callback_tap_gets_an_ack_through_the_dispatch() {
        let _guard = config_test_lock();
        let _env = CommandEnv::new("ack-dispatch");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = config.telegram.clone().expect("telegram");
        let bot_id = telegram_bot_id(&telegram.bot_token);
        let _ = test_callback_acks::take();
        test_callback_acks::reset_attempts();
        crate::state::insert_telegram_callback_route(
            &conn,
            &crate::state::TelegramCallbackRoute {
                callback_id: "cb_ack".to_string(),
                chat_id: "456".to_string(),
                message_id: None,
                thread_id: "thr_ack".to_string(),
                action: TelegramCallbackAction::Approve,
                approval_id: None,
                question_id: None,
                answer: None,
            },
            1000,
        )
        .expect("insert route");
        let tap = |update_id: i64, cq: &str| {
            json!({
                "update_id": update_id,
                "callback_query": {
                    "id": cq,
                    "from": { "id": 789 },
                    "message": { "message_id": 20, "chat": { "id": "456" } },
                    "data": "claude:cb_ack"
                }
            })
        };
        let updates = vec![tap(1, "cq_first"), tap(2, "cq_second")];
        process_telegram_update_batch(
            &conn,
            &config,
            &telegram,
            &bot_id,
            "telegram_offset:test",
            &updates,
            2000,
            Duration::from_secs(1),
            None,
        )
        .expect("batch");

        let acks = test_callback_acks::take();
        assert_eq!(acks.len(), 2, "both taps must be answered: {acks:?}");
        assert_eq!(acks[0].0, "cq_first");
        // First tap consumed the route (no approval behind it in this
        // fixture, so its toast is the invalid-button one) …
        assert_eq!(acks[1].0, "cq_second");
        // … and the SECOND tap on the now-used button gets the spent toast.
        assert_eq!(acks[1].1, "这个按钮已经处理过了。");
    }

    /// A failing Telegram API must not fail silently: the ack is retried,
    /// the update still advances (one poison tap must not wedge the queue),
    /// and the failure is written into the inbound log next to the update.
    #[test]
    fn failed_ack_is_retried_and_logged_loudly() {
        let _guard = config_test_lock();
        let _env = CommandEnv::new("ack-fail");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = config.telegram.clone().expect("telegram");
        let bot_id = telegram_bot_id(&telegram.bot_token);
        let _ = test_callback_acks::take();
        test_callback_acks::reset_attempts();
        // A used route: the tap resolves to Spent, whose ack will fail.
        crate::state::insert_telegram_callback_route(
            &conn,
            &crate::state::TelegramCallbackRoute {
                callback_id: "cb_dead".to_string(),
                chat_id: "456".to_string(),
                message_id: None,
                thread_id: "thr_dead".to_string(),
                action: TelegramCallbackAction::Approve,
                approval_id: None,
                question_id: None,
                answer: None,
            },
            1000,
        )
        .expect("insert route");
        mark_telegram_callback_route_used(&conn, "cb_dead", 1500).expect("use");
        let updates = vec![json!({
            "update_id": 7,
            "callback_query": {
                "id": "cq_dead",
                "from": { "id": 789 },
                "message": { "message_id": 21, "chat": { "id": "456" } },
                "data": "claude:cb_dead"
            }
        })];
        let _ = test_callback_acks::take_timeouts();
        let result = {
            let _fail = test_callback_acks::FailGuard::engage();
            process_telegram_update_batch(
                &conn,
                &config,
                &telegram,
                &bot_id,
                "telegram_offset:test",
                &updates,
                2000,
                Duration::from_secs(10),
                None,
            )
        };
        result.expect("batch must not error");

        assert_eq!(
            test_callback_acks::reset_attempts(),
            3,
            "the ack must be retried before giving up"
        );
        // Each attempt must run under the ack-specific cap: with the general
        // 10s API timeout, three attempts could take ~30s — past the tap's
        // own validity and stalling the rest of the batch.
        for timeout in test_callback_acks::take_timeouts() {
            assert!(
                timeout <= Duration::from_secs(2),
                "ack attempts must use the short ack timeout, got {timeout:?}"
            );
        }
        assert!(
            telegram_inbound_processed(&conn, &bot_id, 7).expect("processed"),
            "a poison tap must not wedge the queue"
        );
        assert_eq!(
            crate::state::get_setting_number(&conn, "telegram_offset:test").expect("offset"),
            Some(7),
            "the persisted offset must advance past the poison tap"
        );
        let result_json: String = conn
            .query_row(
                "SELECT result_json FROM telegram_inbound_log WHERE update_id = 7",
                [],
                |row| row.get(0),
            )
            .expect("log row");
        assert!(
            result_json.contains("injected ack failure"),
            "the failure must be recorded, not discarded: {result_json}"
        );
        assert!(
            result_json.contains("\"ackDelivered\":false"),
            "{result_json}"
        );
    }

    /// The REAL error chain, not a hand-built `unknown: true`: a database
    /// whose schema is missing makes `session_terminal_presence` genuinely
    /// fail, and the production classifier must turn that into the unknown
    /// condition rather than "idle" (or a crash). Also pins the prune rule
    /// that keeps the running-turns set correct: settled history is
    /// removable, running rows never are.
    #[test]
    fn a_real_read_failure_classifies_as_unknown() {
        // No schema at all: every table lookup errors.
        let broken = rusqlite::Connection::open_in_memory().expect("raw db");
        let running = std::collections::HashSet::new();
        let liveness = classify_thread_liveness(&broken, "thr-broken", &running);
        assert!(liveness.unknown, "a query error must classify as unknown");
        assert!(
            !liveness.headless && liveness.presence == crate::claude::TerminalPresence::Gone,
            "and must not invent liveness facts: {liveness:?}"
        );
        // But an unreadable socket must not ERASE facts either: the batch
        // set already knows this thread has a running turn.
        let mut known_running = std::collections::HashSet::new();
        known_running.insert("thr-broken".to_string());
        let liveness = classify_thread_liveness(&broken, "thr-broken", &known_running);
        assert!(liveness.unknown);
        assert!(
            liveness.headless,
            "unknown terminal state must keep the known running turn: {liveness:?}"
        );

        // The healthy path on a real db, driven through the same classifier.
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::register_bridge_turn(
            &conn,
            "turn-c",
            "sess-c",
            "/tmp/c.log",
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        let classified = classify_recent_threads(&conn, 50, 2000).expect("classify");
        let (_, liveness, _) = classified
            .iter()
            .find(|(snapshot, _, _)| snapshot.thread_id == "sess-c")
            .expect("a running turn with NO threads_cache row must still be listed");
        assert!(!liveness.unknown);
        assert!(
            liveness.headless,
            "the batch set must feed the classifier: {liveness:?}"
        );

        // Pruning: a settled turn far past retention goes, the running one
        // stays whatever its age.
        // A pid at registration: a turn that reached `done` in production
        // always paid its birth cleanup debt off (identity persisted), and
        // an indebted row is deliberately unprunable.
        crate::state::register_bridge_turn(
            &conn,
            "turn-old",
            "sess-old",
            "/tmp/o.log",
            Some(4_100),
            None,
            None,
            Some(4_100),
            None,
            None,
            500,
        )
        .expect("register old");
        crate::state::mark_bridge_turn_finished(&conn, "turn-old", "done", 600).expect("finish");
        let far_future = 500 + 31 * 24 * 60 * 60 * 1000;
        crate::state::prune_state_logs(&conn, far_future).expect("prune");
        assert!(
            crate::state::list_running_bridge_turns(&conn)
                .expect("after prune")
                .iter()
                .any(|turn| turn.thread_id == "sess-c"),
            "a running turn must survive pruning regardless of age"
        );
        let turns: i64 = conn
            .query_row("SELECT COUNT(*) FROM bridge_turns", [], |row| row.get(0))
            .expect("count");
        assert_eq!(turns, 1, "the settled turn must be pruned");
    }

    /// The live bug replayed: a session stuck at a TERMINAL dialog (remote
    /// window expired hours ago, `pending_prompt` still pending) must rank
    /// above ordinary live terminals — below only the remote-answerable
    /// re-offers. Idle recency must not bury it.
    #[test]
    fn terminal_waiting_sessions_rank_between_reoffers_and_the_rest() {
        use crate::claude::TerminalPresence;
        let snapshot = |id: &str, updated: u64, waiting: bool| crate::state::BridgeThreadSnapshot {
            thread_id: id.to_string(),
            name: None,
            cwd: None,
            updated_at: Some(updated),
            status_type: "active".to_string(),
            status_flags: vec![],
            last_turn_status: None,
            last_preview: None,
            pending_prompt: waiting.then(|| crate::state::PendingPrompt {
                prompt_id: "notify:1".to_string(),
                kind: "approval".to_string(),
                status: "pending".to_string(),
                question: Some("Claude needs your permission".to_string()),
                transcript_bytes: None,
                notification_type: None,
            }),
            event_uid: None,
        };
        let live = |presence| render::ThreadLiveness {
            presence,
            headless: false,
            unknown: false,
        };
        let pool = vec![
            (
                snapshot("terminal-busy", 900, false),
                live(TerminalPresence::Window),
                None,
            ),
            (
                snapshot("terminal-dialog", 100, true),
                live(TerminalPresence::Window),
                None,
            ),
            (
                snapshot("reoffer", 50, false),
                live(TerminalPresence::Gone),
                Some(crate::state::OpenPrompt::Approval {
                    approval_id: "ap".to_string(),
                    summary: "Bash: x".to_string(),
                    headless: false,
                }),
            ),
        ];
        let shown = order_threads_for_display(pool, 3);
        let ids = shown
            .iter()
            .map(|(snapshot, _, _)| snapshot.thread_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec!["reoffer", "terminal-dialog", "terminal-busy"],
            "answerable ask first, terminal dialog second, ordinary sessions last"
        );
    }

    /// A waiting session outranks even a live terminal: the list exists so a
    /// missed ask gets seen.
    #[test]
    fn waiting_sessions_lead_the_list() {
        use crate::claude::TerminalPresence;
        let snapshot = |id: &str, updated: u64| crate::state::BridgeThreadSnapshot {
            thread_id: id.to_string(),
            name: None,
            cwd: None,
            updated_at: Some(updated),
            status_type: "active".to_string(),
            status_flags: vec![],
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        };
        let live = |presence| render::ThreadLiveness {
            presence,
            headless: false,
            unknown: false,
        };
        let pool = vec![
            (
                snapshot("terminal-new", 900),
                live(TerminalPresence::Window),
                None,
            ),
            (
                snapshot("waiting-idle", 100),
                live(TerminalPresence::Gone),
                Some(crate::state::OpenPrompt::Approval {
                    approval_id: "ap-w".to_string(),
                    summary: "Bash: rm -rf".to_string(),
                    headless: false,
                }),
            ),
        ];
        let shown = order_threads_for_display(pool, 2);
        assert_eq!(
            shown[0].0.thread_id, "waiting-idle",
            "an open ask must lead even from the idle class"
        );
    }

    /// The full reoffer chain for an approval: /threads builds a fresh
    /// buttons event for the OPEN approval, and tapping one of those fresh
    /// buttons answers the very row the blocked hook is polling.
    #[test]
    fn threads_reoffers_an_open_approval_and_its_buttons_answer_it() {
        let _guard = config_test_lock();
        let _env = CommandEnv::new("reoffer-approval");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = config.telegram.clone().expect("telegram");
        // A running headless turn whose approval is waiting: registered in
        // bridge_turns (so the union lists it) with an open approval row.
        crate::state::register_bridge_turn(
            &conn,
            "turn-w",
            "sess-wait",
            "/tmp/w.log",
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
            "ap-wait",
            "sess-wait",
            "Bash",
            "Bash: touch x",
            false,
            1000,
            9_000,
        )
        .expect("approval");

        let classified = classify_recent_threads(&conn, 50, 2000).expect("classify");
        let (snapshot, liveness, prompt) = classified
            .iter()
            .find(|(snapshot, _, _)| snapshot.thread_id == "sess-wait")
            .expect("waiting session listed");
        let prompt = prompt.as_ref().expect("open approval must ride along");
        let event = reoffer_prompt_event(&conn, &telegram, snapshot, *liveness, prompt, 2000)
            .expect("reoffer event");
        assert_eq!(event["type"], "approval_request", "{event}");
        assert_eq!(event["approvalId"], "ap-wait", "{event}");
        let buttons = event["buttons"].as_array().expect("buttons");
        assert_eq!(buttons.len(), 3, "允许/本会话/拒绝 must all be offered");

        // Tap the freshly minted deny button: the ORIGINAL pending row is
        // answered — exactly what the blocked hook polls.
        let deny = buttons
            .iter()
            .find(|button| button["action"] == "deny")
            .expect("deny button");
        let callback_query = json!({
            "id": "cq_reoffer",
            "from": { "id": 789 },
            "message": { "message_id": 42, "chat": { "id": "456" } },
            "data": format!("claude:{}", deny["callbackId"].as_str().expect("id"))
        });
        let route = match extract_telegram_callback_route(&conn, &callback_query, &telegram)
            .expect("extract")
        {
            TelegramCallbackLookup::Route(route) => route,
            other => panic!("fresh reoffer button must route: {other:?}"),
        };
        let answer = record_callback_answer(&conn, &route, 3000).expect("record");
        assert_eq!(answer["recorded"], true, "{answer}");
        assert_eq!(
            crate::state::approval_decision(&conn, "ap-wait").expect("decision"),
            Some("deny".to_string()),
            "the hook-visible row carries the answer"
        );
    }

    /// A multi-select question reoffered by /threads keeps the no-buttons
    /// rule and spells out the options with the comma instruction — the
    /// stored multi_select flag is what makes that possible.
    #[test]
    fn threads_reoffers_a_multiselect_question_without_buttons() {
        let _guard = config_test_lock();
        let _env = CommandEnv::new("reoffer-multi");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = config.telegram.clone().expect("telegram");
        crate::state::register_bridge_turn(
            &conn,
            "turn-q",
            "sess-q",
            "/tmp/q.log",
            None,
            None,
            None,
            None,
            None,
            None,
            1000,
        )
        .expect("register");
        crate::state::create_pending_question(
            &conn,
            "q-multi",
            "sess-q",
            "选哪些库？",
            &["serde".to_string(), "tokio".to_string()],
            true,
            1000,
            9_000,
        )
        .expect("question");

        let classified = classify_recent_threads(&conn, 50, 2000).expect("classify");
        let (snapshot, liveness, prompt) = classified
            .iter()
            .find(|(snapshot, _, _)| snapshot.thread_id == "sess-q")
            .expect("listed");
        let event = reoffer_prompt_event(
            &conn,
            &telegram,
            snapshot,
            *liveness,
            prompt.as_ref().expect("open question"),
            2000,
        )
        .expect("event");
        assert_eq!(event["type"], "question_request", "{event}");
        assert!(
            event["buttons"].as_array().expect("buttons").is_empty(),
            "multi-select must never get one-tap buttons: {event}"
        );
        let body = event["lastPreview"].as_str().expect("body");
        assert!(body.contains("A. serde"), "{body}");
        assert!(body.contains("逗号分隔"), "{body}");
    }

    /// A session whose ONLY trace is an open prompt — no cache row, no
    /// running turn — must still be listed and led with. It is the most
    /// urgent row of the list, not one to silently drop.
    #[test]
    fn a_prompt_only_session_still_makes_the_list() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::create_pending_approval(
            &conn,
            "ap-orphan",
            "sess-orphan",
            "Bash",
            "Bash: make deploy",
            false,
            1000,
            9_000,
        )
        .expect("approval");
        let classified = classify_recent_threads(&conn, 50, 2000).expect("classify");
        let shown = order_threads_for_display(classified, 5);
        assert!(
            !shown.is_empty(),
            "an open prompt with no other trace must not vanish"
        );
        let (snapshot, _, prompt) = &shown[0];
        assert_eq!(
            snapshot.thread_id, "sess-orphan",
            "and it must lead the list"
        );
        assert!(prompt.is_some(), "carrying its prompt for the reoffer");
    }

    /// The timeout hint replays the gate kind persisted at CREATION. The
    /// session's current look must not rewrite it: a headless approval's
    /// timeout denies whatever the session is doing now.
    #[test]
    fn reoffer_replays_the_persisted_gate_kind() {
        let _guard = config_test_lock();
        let _env = CommandEnv::new("reoffer-kind");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = config.telegram.clone().expect("telegram");
        crate::state::create_pending_approval(
            &conn, "ap-h", "sess-h", "Bash", "Bash: x", true, 1000, 9_000,
        )
        .expect("approval");
        let classified = classify_recent_threads(&conn, 50, 2000).expect("classify");
        let (snapshot, liveness, prompt) = classified
            .iter()
            .find(|(snapshot, _, _)| snapshot.thread_id == "sess-h")
            .expect("listed");
        // The session no longer looks headless (no running turn), but the
        // approval was born at the headless gate.
        assert!(!liveness.headless);
        let event = reoffer_prompt_event(
            &conn,
            &telegram,
            snapshot,
            *liveness,
            prompt.as_ref().expect("prompt"),
            2000,
        )
        .expect("event");
        assert_eq!(
            event["headless"], true,
            "the hint must describe the approval, not today's session: {event}"
        );
    }

    /// Rows from before the `multi_select` column existed have NULL there.
    /// Guessing "single-select" would mint one-tap buttons for what may
    /// really be a multi-select — one tap would submit a single option and
    /// silently drop the rest. NULL must render the no-buttons shape.
    #[test]
    fn legacy_questions_without_multiselect_flag_get_no_buttons() {
        let _guard = config_test_lock();
        let _env = CommandEnv::new("reoffer-legacy");
        let config = test_daemon_config();
        write_daemon_config(&config).expect("write daemon config");
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let telegram = config.telegram.clone().expect("telegram");
        // A pre-upgrade row: written without the multi_select column.
        conn.execute(
            "INSERT INTO pending_questions(
                question_id, thread_id, question, options_json, created_at, expires_at
             ) VALUES ('q-old', 'sess-old', '选哪个？', '[\"甲\",\"乙\"]', 1000, 9000)",
            [],
        )
        .expect("legacy row");
        let classified = classify_recent_threads(&conn, 50, 2000).expect("classify");
        let (snapshot, liveness, prompt) = classified
            .iter()
            .find(|(snapshot, _, _)| snapshot.thread_id == "sess-old")
            .expect("listed");
        let event = reoffer_prompt_event(
            &conn,
            &telegram,
            snapshot,
            *liveness,
            prompt.as_ref().expect("prompt"),
            2000,
        )
        .expect("event");
        assert!(
            event["buttons"].as_array().expect("buttons").is_empty(),
            "unknown select-mode must not mint one-tap buttons: {event}"
        );
        let body = event["lastPreview"].as_str().expect("body");
        assert!(body.contains("A. 甲"), "options stay visible: {body}");
    }

    /// A session waiting at a TERMINAL dialog beyond the recent-cache window
    /// must still make the pool — driven through the real database, past the
    /// 50-row recency cut. (An 8-hour-old dialog fell off exactly this way.)
    #[test]
    fn a_terminal_waiting_session_beyond_the_cache_window_is_still_listed() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        // 50 fresher cache rows fill the recency window completely.
        for index in 0..50 {
            conn.execute(
                "INSERT INTO threads_cache(thread_id, updated_at, last_seen_at, status_type, status_flags_json)
                 VALUES (?1, ?2, ?2, 'active', '[]')",
                rusqlite::params![format!("filler-{index}"), 10_000 + index as i64],
            )
            .expect("filler");
        }
        // The waiting session: OLDEST of all, terminal prompt still pending.
        conn.execute(
            "INSERT INTO threads_cache(thread_id, updated_at, last_seen_at, status_type, status_flags_json)
             VALUES ('sess-stuck', 100, 100, 'active', '[]')",
            [],
        )
        .expect("stuck cache row");
        conn.execute(
            "INSERT INTO pending_prompts(thread_id, prompt_id, prompt_kind, prompt_status, question, created_at)
             VALUES ('sess-stuck', 'notify:1', 'approval', 'pending', 'Claude needs your permission', 200)",
            [],
        )
        .expect("pending prompt");

        let classified = classify_recent_threads(&conn, 50, 20_000).expect("classify");
        let stuck = classified
            .iter()
            .find(|(snapshot, _, _)| snapshot.thread_id == "sess-stuck")
            .expect("the waiting session must be in the pool despite the recency cut");
        assert!(
            stuck.0.pending_prompt.is_some(),
            "and must carry its prompt for the waiting tier"
        );
    }

    /// The waiting tier is for LIVE terminals only: a stale pending-prompt
    /// row whose session is gone must not squat on the top of the list.
    #[test]
    fn a_ghost_sessions_stale_prompt_does_not_lead() {
        use crate::claude::TerminalPresence;
        let snapshot = crate::state::BridgeThreadSnapshot {
            thread_id: "ghost".to_string(),
            name: None,
            cwd: None,
            updated_at: Some(100),
            status_type: "active".to_string(),
            status_flags: vec![],
            last_turn_status: None,
            last_preview: None,
            pending_prompt: Some(crate::state::PendingPrompt {
                prompt_id: "notify:9".to_string(),
                kind: "approval".to_string(),
                status: "pending".to_string(),
                question: Some("Claude needs your permission".to_string()),
                transcript_bytes: None,
                notification_type: None,
            }),
            event_uid: None,
        };
        let gone = render::ThreadLiveness {
            presence: TerminalPresence::Gone,
            headless: false,
            unknown: false,
        };
        let window = render::ThreadLiveness {
            presence: TerminalPresence::Window,
            headless: false,
            unknown: false,
        };
        assert_eq!(
            waiting_rank(&snapshot, gone, None),
            2,
            "a dead session's prompt is history, not a wait"
        );
        assert_eq!(
            waiting_rank(&snapshot, window, None),
            1,
            "the same prompt on a live terminal IS a wait"
        );
        // Background is explicitly "no window": nothing is waiting where the
        // user could see it, so no waiting tier either.
        let background = render::ThreadLiveness {
            presence: TerminalPresence::Background,
            headless: false,
            unknown: false,
        };
        assert_eq!(
            waiting_rank(&snapshot, background, None),
            2,
            "a windowless session cannot have a terminal waiting on anyone"
        );
    }

    /// Prompt history is pruned, open prompts are untouchable — whatever
    /// their age claims. /threads scans these tables on every call, so
    /// without pruning its cost grows with every approval ever made.
    #[test]
    fn prompt_history_is_pruned_but_open_prompts_never() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        let retention: u64 = 30 * 24 * 60 * 60 * 1000;
        let now = retention + 100_000;
        // Ancient settled approval and ancient expired question: history.
        crate::state::create_pending_approval(
            &conn,
            "ap-ancient",
            "sess-1",
            "Bash",
            "x",
            false,
            1000,
            5000,
        )
        .expect("approval");
        crate::state::record_approval_decision(&conn, "ap-ancient", "allow", 2000).expect("decide");
        crate::state::create_pending_question(
            &conn,
            "q-ancient",
            "sess-2",
            "?",
            &[],
            false,
            1000,
            5000,
        )
        .expect("question");
        // Ancient but OPEN approval (window still ahead of `now`): whatever
        // created_at says, an open prompt must survive pruning.
        crate::state::create_pending_approval(
            &conn,
            "ap-open",
            "sess-3",
            "Bash",
            "y",
            false,
            1000,
            now + 60_000,
        )
        .expect("open approval");

        crate::state::prune_state_logs(&conn, now).expect("prune");
        let approvals: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT approval_id FROM pending_approvals")
                .expect("stmt");
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .expect("rows");
            rows.collect::<rusqlite::Result<_>>().expect("collect")
        };
        assert_eq!(
            approvals,
            vec!["ap-open".to_string()],
            "settled history pruned, the open row untouched"
        );
        let questions: i64 = conn
            .query_row("SELECT COUNT(*) FROM pending_questions", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(questions, 0, "the expired question is history too");
    }

    /// A pre-upgrade approval has NULL for the gate kind. The reoffer must
    /// claim HEADLESS — the urgency-safe direction: "timeout denies" gets
    /// answered promptly either way, while "terminal will catch it" invites
    /// ignoring an approval whose task actually dies on timeout.
    #[test]
    fn legacy_approvals_claim_the_urgent_gate_kind() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        conn.execute(
            "INSERT INTO pending_approvals(
                approval_id, thread_id, tool_name, summary, created_at, expires_at
             ) VALUES ('ap-legacy', 'sess-legacy', 'Bash', 'Bash: x', 1000, 9000)",
            [],
        )
        .expect("legacy row");
        let prompts = crate::state::open_prompts(&conn, 2000).expect("prompts");
        match prompts.get("sess-legacy") {
            Some(crate::state::OpenPrompt::Approval { headless, .. }) => {
                assert!(*headless, "unknown gate kind must claim the urgent one")
            }
            other => panic!("legacy approval must be open: {other:?}"),
        }
    }

    /// Settled and expired prompts are NOT reoffered: their windows are
    /// closed, and buttons pointing at them would only harvest refusals.
    #[test]
    fn settled_and_expired_prompts_are_not_reoffered() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        crate::state::create_pending_approval(
            &conn, "ap-done", "sess-a", "Bash", "x", false, 1000, 9_000,
        )
        .expect("approval");
        crate::state::record_approval_decision(&conn, "ap-done", "allow", 1500).expect("decide");
        crate::state::create_pending_question(
            &conn,
            "q-late",
            "sess-b",
            "?",
            &[],
            false,
            1000,
            1500,
        )
        .expect("question");
        // At exactly expires_at the answer side still accepts (expiry there
        // is `now > expires_at`), so the open side must still offer.
        let prompts = crate::state::open_prompts(&conn, 1500).expect("prompts");
        assert!(
            prompts.contains_key("sess-b"),
            "the boundary instant is still answerable: {prompts:?}"
        );
        // now=2000: the question's window (1500) has passed.
        let prompts = crate::state::open_prompts(&conn, 2000).expect("prompts");
        assert!(
            prompts.is_empty(),
            "neither a decided approval nor an expired question is open: {prompts:?}"
        );
    }

    /// One session, one row — however many running turns it has. A
    /// per-turn placeholder would list the session twice, double the census
    /// and register duplicate reply routes.
    #[test]
    fn concurrent_turns_of_one_session_collapse_to_one_row() {
        let conn = crate::state::create_state_db_in_memory().expect("db");
        for (turn_id, started) in [("turn-a", 1000), ("turn-b", 3000), ("turn-c", 2000)] {
            crate::state::register_bridge_turn(
                &conn,
                turn_id,
                "sess-multi",
                "/tmp/m.log",
                None,
                None,
                None,
                None,
                None,
                None,
                started,
            )
            .expect("register");
        }
        let classified = classify_recent_threads(&conn, 50, 2000).expect("classify");
        let rows = classified
            .iter()
            .filter(|(snapshot, _, _)| snapshot.thread_id == "sess-multi")
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 1, "one session must render once: {rows:?}");
        assert_eq!(
            rows[0].0.updated_at,
            Some(3000),
            "the placeholder carries the NEWEST turn's start"
        );
    }

    /// Placeholders and cache rows mix in one pool; within a class the sort
    /// key (not input order) must put the newest first, or truncation keeps
    /// an old task and drops a new one.
    #[test]
    fn newest_task_survives_truncation_regardless_of_input_order() {
        use crate::claude::TerminalPresence;
        let entry = |id: &str, updated: u64| {
            (
                crate::state::BridgeThreadSnapshot {
                    thread_id: id.to_string(),
                    name: None,
                    cwd: None,
                    updated_at: Some(updated),
                    status_type: "active".to_string(),
                    status_flags: vec![],
                    last_turn_status: None,
                    last_preview: None,
                    pending_prompt: None,
                    event_uid: None,
                },
                render::ThreadLiveness {
                    presence: TerminalPresence::Gone,
                    headless: true,
                    unknown: false,
                },
            )
        };
        // Oldest-first input — the exact order `list_running_bridge_turns`
        // hands back.
        let pool = vec![
            {
                let (snapshot, liveness) = entry("task-old", 100);
                (snapshot, liveness, None)
            },
            {
                let (snapshot, liveness) = entry("task-new", 900);
                (snapshot, liveness, None)
            },
        ];
        let shown = order_threads_for_display(pool, 1);
        assert_eq!(
            shown[0].0.thread_id, "task-new",
            "truncation must keep the newest task"
        );
    }

    /// The display limit cuts AFTER liveness ordering: an older live
    /// terminal must survive a screenful of newer idle sessions. Cutting by
    /// recency first (the original shape) evicted exactly the sessions the
    /// grouping exists to surface.
    #[test]
    fn display_limit_keeps_live_sessions_over_newer_idle_ones() {
        use crate::claude::TerminalPresence;
        let snapshot = |id: &str, updated: u64| crate::state::BridgeThreadSnapshot {
            thread_id: id.to_string(),
            name: None,
            cwd: None,
            updated_at: Some(updated),
            status_type: "active".to_string(),
            status_flags: vec![],
            last_turn_status: None,
            last_preview: None,
            pending_prompt: None,
            event_uid: None,
        };
        let live = |presence| render::ThreadLiveness {
            presence,
            headless: false,
            unknown: false,
        };
        // Recency order, as the DB returns them: three fresh idle sessions,
        // then an older live terminal, then an older running headless turn.
        let pool = vec![
            (
                snapshot("idle-newest", 500),
                live(TerminalPresence::Gone),
                None,
            ),
            (snapshot("idle-2", 400), live(TerminalPresence::Gone), None),
            (snapshot("idle-3", 300), live(TerminalPresence::Gone), None),
            (
                snapshot("terminal-old", 200),
                live(TerminalPresence::Window),
                None,
            ),
            (
                snapshot("headless-old", 100),
                render::ThreadLiveness {
                    presence: TerminalPresence::Gone,
                    headless: true,
                    unknown: false,
                },
                None,
            ),
        ];
        let shown = order_threads_for_display(pool, 3);
        let ids = shown
            .iter()
            .map(|(snapshot, _, _)| snapshot.thread_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec!["terminal-old", "headless-old", "idle-newest"],
            "alive sessions outrank newer idle ones; recency breaks ties"
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
                question_id: None,
                answer: None,
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
        assert!(matches!(route, TelegramCallbackLookup::Route(_)));

        // A second tap on the SAME button must resolve to Spent so the tap
        // can be acknowledged — resolving to silence left the Telegram
        // client spinning for ~30s and then showing nothing (live bug,
        // 2026-08-13). One-answer-per-button itself stays intact.
        mark_telegram_callback_route_used(&conn, "cb_1", 2000).expect("mark used");
        match extract_telegram_callback_route(&conn, &callback_query, &telegram).expect("after") {
            TelegramCallbackLookup::Spent {
                callback_query_id,
                toast,
            } => {
                assert_eq!(callback_query_id, "cq_1");
                assert_eq!(toast, "这个按钮已经处理过了。");
            }
            _ => panic!("a used route must be Spent, not silent or re-routable"),
        }

        // A tap on a button whose row was pruned entirely gets the other
        // honest answer.
        let orphan = json!({
            "id": "cq_2",
            "from": { "id": 789 },
            "message": { "message_id": 10, "chat": { "id": "456" } },
            "data": "claude:cb_gone"
        });
        match extract_telegram_callback_route(&conn, &orphan, &telegram).expect("orphan") {
            TelegramCallbackLookup::Spent { toast, .. } => {
                assert_eq!(toast, "这个按钮已经失效了。")
            }
            _ => panic!("a pruned route must be Spent"),
        }

        // An unauthorized tap stays silent — Spent toasts are for the owner.
        let stranger = json!({
            "id": "cq_3",
            "from": { "id": 666 },
            "message": { "message_id": 10, "chat": { "id": "456" } },
            "data": "claude:cb_1"
        });
        assert!(matches!(
            extract_telegram_callback_route(&conn, &stranger, &telegram).expect("stranger"),
            TelegramCallbackLookup::Foreign
        ));
    }
}
