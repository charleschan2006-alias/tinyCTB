use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::time::Duration;

use crate::config::TelegramConfig;

/// One shared agent per timeout value. Building an agent per call meant a
/// fresh TLS handshake for every request — measured at ~9% of a core once
/// the daemon polled Telegram at 4Hz. A cached agent keeps its connection
/// pool, so repeat calls to api.telegram.org reuse the socket.
fn shared_agent(timeout: Duration) -> ureq::Agent {
    use std::collections::HashMap;
    use std::sync::Mutex;
    static AGENTS: Mutex<Option<HashMap<u128, ureq::Agent>>> = Mutex::new(None);
    let mut agents = AGENTS.lock().expect("agent cache lock");
    let agents = agents.get_or_insert_with(HashMap::new);
    agents
        .entry(timeout.as_millis())
        .or_insert_with(|| {
            ureq::Agent::config_builder()
                .timeout_global(Some(timeout))
                .build()
                .new_agent()
        })
        .clone()
}

/// Records what the bridge tried to SAY, and answers for it, so a test can
/// check that a user was told something without a request leaving the
/// machine. Thread-local with an RAII guard, and inert until armed.
#[cfg(test)]
pub(crate) mod outbound_probe {
    use serde_json::{json, Value};

    type During = Box<dyn Fn()>;

    thread_local! {
        static SENT: std::cell::RefCell<Option<Vec<(String, Value)>>> =
            const { std::cell::RefCell::new(None) };
        static DURING: std::cell::RefCell<Option<During>> = const { std::cell::RefCell::new(None) };
    }

    pub(crate) struct Armed;

    impl Drop for Armed {
        fn drop(&mut self) {
            SENT.with(|sent| *sent.borrow_mut() = None);
            DURING.with(|during| *during.borrow_mut() = None);
        }
    }

    /// Arm it so every call FAILS, which is how a test asks what happens
    /// when Telegram is unreachable at the exact moment something had to be
    /// said. Answering successfully is not offered: a probe that only ever
    /// says "sent" would let a fire-and-forget send look like delivery,
    /// which is the defect this exists to catch.
    pub(crate) fn arm_failing() -> Armed {
        SENT.with(|sent| *sent.borrow_mut() = Some(Vec::new()));
        DURING.with(|during| *during.borrow_mut() = None);
        Armed
    }

    /// Answer successfully, and let the world MOVE while the call is in
    /// flight — which is what talking to Telegram does in production, and
    /// what makes a batch test able to ask about anything but its first
    /// update.
    pub(crate) fn arm_acting(action: impl Fn() + 'static) -> Armed {
        SENT.with(|sent| *sent.borrow_mut() = Some(Vec::new()));
        DURING.with(|during| *during.borrow_mut() = Some(Box::new(action)));
        Armed
    }

    /// What has been sent on this thread since the probe was armed.
    pub(crate) fn sent() -> Vec<(String, Value)> {
        SENT.with(|sent| sent.borrow().clone().unwrap_or_default())
    }

    pub(crate) fn observe(method: &str, payload: &Value) -> Option<super::Result<Value>> {
        SENT.with(|sent| {
            let mut sent = sent.borrow_mut();
            let log = sent.as_mut()?;
            log.push((method.to_string(), payload.clone()));
            let calls = log.len();
            drop(sent);
            let acting = DURING.with(|during| {
                if let Some(action) = during.borrow().as_ref() {
                    action();
                    true
                } else {
                    false
                }
            });
            if acting {
                return Some(Ok(json!({ "ok": true, "result": { "message_id": calls } })));
            }
            Some(Err(anyhow::anyhow!(
                "Telegram API {method} request failed: probe armed to fail"
            )))
        })
    }
}

pub(crate) fn telegram_api_post(
    bot_token: &str,
    method: &str,
    payload: &Value,
    timeout: Duration,
) -> Result<Value> {
    #[cfg(test)]
    if let Some(canned) = outbound_probe::observe(method, payload) {
        return canned;
    }
    let agent = shared_agent(timeout);
    let url = format!(
        "https://api.telegram.org/bot{}/{}",
        bot_token.trim(),
        method.trim()
    );
    let mut response = agent
        .post(&url)
        .send_json(payload.clone())
        .map_err(|error| {
            anyhow!(
                "Telegram API {method} request failed: {}",
                crate::redact_secret_text(&error.to_string(), bot_token)
            )
        })?;
    let value: Value = response
        .body_mut()
        .read_json()
        .with_context(|| format!("Telegram API {method} returned invalid JSON"))?;
    if value.get("ok").and_then(Value::as_bool) != Some(true) {
        bail!("Telegram API {method} returned error: {value}");
    }
    Ok(value)
}

pub(crate) fn telegram_delete_webhook(bot_token: &str, timeout: Duration) -> Result<Value> {
    telegram_api_post(
        bot_token,
        "deleteWebhook",
        &json!({ "drop_pending_updates": false }),
        timeout,
    )
}

pub(crate) fn telegram_get_updates(
    bot_token: &str,
    offset: Option<i64>,
    timeout_seconds: u64,
    timeout: Duration,
) -> Result<Value> {
    let mut body = serde_json::Map::new();
    if let Some(offset) = offset {
        body.insert("offset".to_string(), json!(offset));
    }
    body.insert("timeout".to_string(), json!(timeout_seconds));
    body.insert(
        "allowed_updates".to_string(),
        json!(["message", "callback_query"]),
    );
    telegram_api_post(bot_token, "getUpdates", &Value::Object(body), timeout)
}

pub(crate) fn telegram_send_message(
    telegram: &TelegramConfig,
    payload: &Value,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(&telegram.bot_token, "sendMessage", payload, timeout)
}

pub(crate) fn telegram_send_text(
    telegram: &TelegramConfig,
    text: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_send_message(
        telegram,
        &json!({
            "chat_id": telegram.chat_id,
            "text": text,
            "disable_web_page_preview": true
        }),
        timeout,
    )
}

pub(crate) fn telegram_send_text_message_id(
    telegram: &TelegramConfig,
    text: &str,
    timeout: Duration,
) -> Result<i64> {
    telegram_send_text(telegram, text, timeout)?
        .pointer("/result/message_id")
        .and_then(Value::as_i64)
        .context("Telegram sendMessage response missing result.message_id")
}

#[cfg(not(test))]
pub(crate) fn telegram_send_chat_action(
    telegram: &TelegramConfig,
    action: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(
        &telegram.bot_token,
        "sendChatAction",
        &json!({
            "chat_id": telegram.chat_id,
            "action": action
        }),
        timeout,
    )
}

#[cfg(test)]
pub(crate) fn telegram_send_chat_action(
    telegram: &TelegramConfig,
    action: &str,
    _timeout: Duration,
) -> Result<Value> {
    Ok(json!({
        "ok": true,
        "result": true,
        "chat_id": telegram.chat_id,
        "action": action
    }))
}

/// The command list Telegram shows the moment the user types `/`. Array
/// order IS display order, and the top of that list is prime real estate on
/// a phone: `/away`, `/back` and `/threads` are the three used mid-errand,
/// one-handed, so they lead. Setup-time and diagnostic commands (`/start`,
/// `/repair`, `/help`) are typed once and sink to the bottom.
///
/// Changing this order changes what the user's thumb lands on — treat it as
/// UI, not as a list of capabilities.
pub(crate) fn telegram_bot_commands() -> Vec<Value> {
    vec![
        json!({ "command": "away", "description": "Start remote Claude mode" }),
        json!({ "command": "back", "description": "Stop remote Claude mode" }),
        json!({ "command": "threads", "description": "Show recent Claude sessions" }),
        json!({ "command": "stop", "description": "Stop ALL running headless turns (or one by id)" }),
        json!({ "command": "new", "description": "Start a new Claude session" }),
        json!({ "command": "project", "description": "Show or switch the current project" }),
        json!({ "command": "status", "description": "Show remote Claude status" }),
        json!({ "command": "start", "description": "Pair and show remote control help" }),
        json!({ "command": "repair", "description": "Fix remote Claude mode" }),
        json!({ "command": "help", "description": "Show Telegram remote control commands" }),
    ]
}

pub(crate) fn telegram_set_my_commands(
    telegram: &TelegramConfig,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(
        &telegram.bot_token,
        "setMyCommands",
        &json!({ "commands": telegram_bot_commands() }),
        timeout,
    )
}

#[cfg_attr(test, allow(dead_code))] // test builds ack through the seam instead
pub(crate) fn telegram_answer_callback_query(
    telegram: &TelegramConfig,
    callback_query_id: &str,
    text: &str,
    timeout: Duration,
) -> Result<Value> {
    telegram_api_post(
        &telegram.bot_token,
        "answerCallbackQuery",
        &json!({
            "callback_query_id": callback_query_id,
            "text": text,
            "show_alert": false
        }),
        timeout,
    )
}

pub(crate) fn telegram_updates_array(updates: &Value) -> Result<&[Value]> {
    updates
        .get("result")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .context("Telegram getUpdates response did not contain result array")
}

pub(crate) fn telegram_chat_id(message: &Value) -> Option<String> {
    message
        .get("chat")
        .and_then(|chat| chat.get("id"))
        .and_then(|value| {
            value
                .as_i64()
                .map(|id| id.to_string())
                .or_else(|| value.as_str().map(str::to_string))
        })
}

pub(crate) fn telegram_from_user_id(message: &Value) -> Option<String> {
    message
        .get("from")
        .and_then(|from| from.get("id"))
        .and_then(|value| {
            value
                .as_i64()
                .map(|id| id.to_string())
                .or_else(|| value.as_str().map(str::to_string))
        })
}

pub(crate) fn telegram_message_id(message: &Value) -> Option<i64> {
    message.get("message_id").and_then(Value::as_i64)
}

pub(crate) fn telegram_bot_id(bot_token: &str) -> String {
    crate::sha256_hex(bot_token.as_bytes())[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telegram_bot_commands_are_registered_for_core_remote_actions() {
        let commands = telegram_bot_commands();
        let names = commands
            .iter()
            .filter_map(|command| command.get("command").and_then(Value::as_str))
            .collect::<Vec<_>>();

        // Order is the phone UI: the three one-handed commands lead, and a
        // reshuffle that buries them must fail here rather than surprise the
        // user's thumb.
        assert_eq!(
            &names[..3],
            &["away", "back", "threads"],
            "away/back/threads must be the first three entries of the / menu"
        );
        assert_eq!(
            names,
            vec![
                "away", "back", "threads", "stop", "new", "project", "status", "start", "repair",
                "help",
            ]
        );
        for removed in [
            "away_on",
            "away_off",
            "live_on",
            "live_reset",
            "new_thread",
            "projects",
            "inbox",
            "waiting",
            "recent",
            "settings",
        ] {
            assert!(
                !names.contains(&removed),
                "removed telegram bot command is still advertised: {removed}"
            );
        }
        for required in [
            "start", "help", "away", "back", "repair", "status", "threads", "new", "project",
        ] {
            assert!(
                names.contains(&required),
                "missing required telegram bot command {required}"
            );
        }
        for command in commands {
            assert!(
                command["command"].as_str().expect("command").len() <= 32,
                "Telegram command names must fit BotCommand limits"
            );
            assert!(
                !command["description"]
                    .as_str()
                    .expect("description")
                    .trim()
                    .is_empty(),
                "Telegram commands must include human-readable descriptions"
            );
        }
    }
}
