use anyhow::{bail, Result};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::path::Path;

use crate::claude::get_away_mode;
use crate::config::{DaemonConfig, RegisteredProject};
use crate::projects::derive_project_label;
use crate::state::{
    derive_thread_display_name, list_waiting_from_db, pending_outbound_count, BridgeThreadSnapshot,
    ObservedWorkspace, PendingPrompt, TelegramCallbackRoute,
};

const TELEGRAM_CONTINUE_THREAD_HINT: &str =
    "💬 To continue this session, use Telegram's Reply action on this message.";
const TELEGRAM_ANSWER_THREAD_HINT: &str =
    "💬 To answer Claude, use Telegram's Reply action on this message.";
const TELEGRAM_APPROVAL_HINT: &str =
    "⌨️ Approve or deny this in the terminal where the session is running. Replying here sends a follow-up message instead.";
const TELEGRAM_MESSAGE_CHAR_LIMIT: usize = 4096;
/// Shared budget for a /threads snapshot body (question + preview together),
/// leaving room for the title, name, project and reply-hint lines inside one
/// Telegram message.
const TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT: usize = 3000;
/// The preview never shrinks below this, so a very long question cannot
/// squeeze out Claude's actual answer entirely.
const TELEGRAM_THREAD_SNAPSHOT_MIN_PREVIEW: usize = 600;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedTelegramDelivery {
    pub(crate) payloads: Vec<Value>,
    pub(crate) thread_id: Option<String>,
    pub(crate) event_id: String,
    pub(crate) callback_routes: Vec<TelegramCallbackRoute>,
}

fn telegram_event_title(event_type: &str, event: &Value) -> &'static str {
    if telegram_event_is_approval(event) {
        return "🔐 Claude needs approval";
    }
    match event_type {
        "thread_waiting" => "🟡 Claude needs you",
        "thread_completed" => "✅ Claude finished",
        "thread_status_changed" => "🔄 Claude changed",
        "thread_error" => "⚠️ Bridge error",
        "bridge_notice" => "ℹ️ tinyCTB",
        "approval_request" => "🔐 需要你批准",
        _ => "🧵 Claude update",
    }
}

fn telegram_event_reply_hint(event_type: &str, event: &Value) -> &'static str {
    if telegram_event_is_approval(event) {
        TELEGRAM_APPROVAL_HINT
    } else {
        match event_type {
            "thread_waiting" => TELEGRAM_ANSWER_THREAD_HINT,
            _ => TELEGRAM_CONTINUE_THREAD_HINT,
        }
    }
}

fn telegram_event_display_name(event: &Value) -> String {
    event
        .pointer("/thread/displayName")
        .and_then(Value::as_str)
        .or_else(|| event.pointer("/thread/name").and_then(Value::as_str))
        .or_else(|| event.get("threadId").and_then(Value::as_str))
        .unwrap_or("Claude session")
        .to_string()
}

/// Body of a notification. A waiting prompt carries two different things and
/// needs BOTH: the hook's question says why Claude wants you ("Claude is
/// waiting for your input", or a permission request plus its tool detail),
/// while the preview is what Claude actually last said. Showing only the
/// question — which is what a plain priority chain does — renders a
/// contentless one-liner and hides the answer sitting right next to it.
fn telegram_event_detail(event: &Value) -> Option<String> {
    let question = event
        .pointer("/thread/pendingPrompt/question")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let preview = event
        .pointer("/thread/lastPreview")
        .and_then(Value::as_str)
        .or_else(|| event.get("lastPreview").and_then(Value::as_str))
        .or_else(|| event.get("message").and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let detail = match (question, preview) {
        (Some(question), Some(preview)) => match redundant_pair(question, preview) {
            // One side is the other's full text (or its truncated prefix):
            // render the complete one only.
            Some(complete) => complete.to_string(),
            None => format!("{question}\n\n{preview}"),
        },
        (Some(value), None) | (None, Some(value)) => value.to_string(),
        (None, None) => return None,
    };
    Some(sanitize_telegram_detail(&detail))
}

fn telegram_event_is_approval(event: &Value) -> bool {
    event
        .pointer("/thread/pendingPrompt/promptKind")
        .and_then(Value::as_str)
        == Some("approval")
        || event
            .pointer("/thread/pendingPrompt/kind")
            .and_then(Value::as_str)
            == Some("approval")
}

/// Returns the text to show alone when two bodies really are the same text:
/// either they are equal, or the shorter one is an EXPLICITLY TRUNCATED copy
/// (ends in `…` / `...`) whose stem prefixes the longer one.
///
/// Both restrictions are load-bearing. Substring containment is not enough —
/// a short answer ("no") often appears inside its own question ("Please
/// answer yes or no"). Nor is a bare prefix — "No" is a genuine answer to
/// "No changes are required", not a truncation of it. Without a truncation
/// marker, only exact equality counts as redundant.
fn redundant_pair<'a>(left: &'a str, right: &'a str) -> Option<&'a str> {
    if left == right {
        return Some(left);
    }
    let (shorter, longer) = if left.chars().count() <= right.chars().count() {
        (left, right)
    } else {
        (right, left)
    };
    let stem = shorter
        .strip_suffix('…')
        .or_else(|| shorter.strip_suffix("..."))?
        .trim_end();
    (!stem.is_empty() && longer.starts_with(stem)).then_some(longer)
}

fn split_telegram_text(text: &str, max_chars: usize) -> Vec<String> {
    assert!(
        max_chars > 0,
        "Telegram message chunk size must be non-zero"
    );

    let mut chunks = Vec::new();
    let mut chunk = String::new();
    let mut chunk_chars = 0;

    for ch in text.chars() {
        if chunk_chars == max_chars {
            chunks.push(std::mem::take(&mut chunk));
            chunk_chars = 0;
        }
        chunk.push(ch);
        chunk_chars += 1;
    }

    if !chunk.is_empty() {
        chunks.push(chunk);
    }

    if chunks.is_empty() {
        chunks.push(String::new());
    }

    chunks
}

fn sanitize_telegram_detail(detail: &str) -> String {
    let mut sanitized = String::with_capacity(detail.len());
    let mut rest = detail;
    while let Some(start) = rest.find('[') {
        let (before, candidate_start) = rest.split_at(start);
        sanitized.push_str(before);
        let Some(end_offset) = candidate_start.find(']') else {
            sanitized.push_str(candidate_start);
            return sanitized;
        };
        let candidate = &candidate_start[1..end_offset];
        if let Some(replacement) = compact_telegram_file_reference(candidate) {
            sanitized.push_str(&replacement);
            rest = &candidate_start[end_offset + 1..];
        } else {
            sanitized.push('[');
            rest = &candidate_start[1..];
        }
    }
    sanitized.push_str(rest);
    sanitized
}

fn compact_telegram_file_reference(candidate: &str) -> Option<String> {
    let normalized = candidate
        .strip_prefix("F:")
        .or_else(|| candidate.strip_prefix("f:"))
        .unwrap_or(candidate);
    if !normalized.starts_with('/') {
        return None;
    }
    let (path, line_ref) = normalized.split_once('†').unwrap_or((normalized, ""));
    let file_name = Path::new(path).file_name()?.to_string_lossy();
    let line_ref = line_ref.trim();
    if line_ref.is_empty() {
        Some(file_name.into_owned())
    } else {
        Some(format!("{file_name} {line_ref}"))
    }
}

pub(crate) fn prepare_telegram_delivery(
    chat_id: &str,
    event: &Value,
) -> Result<PreparedTelegramDelivery> {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("bridge_event");
    let event_id = crate::notification_event_id(event);
    let thread_id = crate::event_thread_id(event);
    let mut lines = vec![
        telegram_event_title(event_type, event).to_string(),
        format!("🧵 {}", telegram_event_display_name(event)),
    ];
    if let Some(project) = event.pointer("/thread/project").and_then(Value::as_str) {
        lines.push(format!("📁 {project}"));
    }
    if let Some(detail) = telegram_event_detail(event) {
        lines.push(String::new());
        lines.push(detail);
    }
    if thread_id.is_some() {
        lines.push(String::new());
        lines.push(telegram_event_reply_hint(event_type, event).to_string());
    }

    let mut payloads = split_telegram_text(&lines.join("\n"), TELEGRAM_MESSAGE_CHAR_LIMIT)
        .into_iter()
        .map(|text| {
            json!({
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": true
            })
        })
        .collect::<Vec<_>>();

    // An approval request carries its answer buttons. The callback routes were
    // registered by the hook that is blocking on the answer; they are returned
    // here so delivery can stamp them with the message id.
    let mut callback_routes = Vec::new();
    if event_type == "approval_request" {
        let buttons = event
            .get("buttons")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut keyboard_row = Vec::new();
        for button in &buttons {
            let (Some(text), Some(callback_id)) = (
                button.get("text").and_then(Value::as_str),
                button.get("callbackId").and_then(Value::as_str),
            ) else {
                continue;
            };
            keyboard_row.push(json!({
                "text": text,
                "callback_data": format!("claude:{callback_id}")
            }));
            if let Some(action) = button
                .get("action")
                .and_then(Value::as_str)
                .and_then(crate::state::TelegramCallbackAction::from_str)
            {
                callback_routes.push(TelegramCallbackRoute {
                    callback_id: callback_id.to_string(),
                    chat_id: chat_id.to_string(),
                    message_id: None,
                    thread_id: thread_id.clone().unwrap_or_default(),
                    action,
                    approval_id: event
                        .get("approvalId")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                });
            }
        }
        if !keyboard_row.is_empty() {
            // Buttons ride on the LAST chunk so they sit under the full text.
            if let Some(last) = payloads.last_mut().and_then(Value::as_object_mut) {
                last.insert(
                    "reply_markup".to_string(),
                    json!({ "inline_keyboard": [keyboard_row] }),
                );
            }
        }
    }

    // Notifications about a terminal session's own permission dialog still
    // carry no buttons: that dialog can only be answered in its terminal.
    // Only `approval_request` events (raised by the PreToolUse gate, which is
    // blocking on the answer) get an inline keyboard.
    Ok(PreparedTelegramDelivery {
        payloads,
        thread_id,
        event_id,
        callback_routes,
    })
}

fn trim_for_telegram_detail(value: &str, max_chars: usize) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.chars().count() <= max_chars {
        return Some(value.to_string());
    }
    let take = max_chars.saturating_sub(3);
    Some(format!(
        "{}...",
        value.chars().take(take).collect::<String>().trim_end()
    ))
}

fn thread_snapshot_event_type(snapshot: &BridgeThreadSnapshot) -> &'static str {
    if snapshot.pending_prompt.is_some() {
        "thread_waiting"
    } else if snapshot.last_turn_status.as_deref() == Some("completed") {
        "thread_completed"
    } else {
        "thread_status_changed"
    }
}

fn pending_prompt_value(prompt: &PendingPrompt, question_budget: usize) -> Value {
    json!({
        "promptId": prompt.prompt_id,
        "promptKind": prompt.kind,
        "promptStatus": prompt.status,
        "kind": prompt.kind,
        "status": prompt.status,
        "question": prompt
            .question
            .as_deref()
            .and_then(|question| trim_for_telegram_detail(question, question_budget))
    })
}

fn thread_snapshot_event(snapshot: &BridgeThreadSnapshot) -> Value {
    let project = derive_project_label(snapshot.cwd.as_deref());
    let display_name = derive_thread_display_name(
        snapshot.name.as_deref(),
        project.as_deref(),
        snapshot
            .pending_prompt
            .as_ref()
            .and_then(|prompt| prompt.question.as_deref()),
        &snapshot.thread_id,
    );
    let display_name = trim_for_telegram_line(&display_name, 160);
    // The rendered body concatenates question AND preview, so the two share
    // one budget — /threads snapshots must stay within a single Telegram
    // message, and two independent 3000-char fields would overflow it.
    // The question (why Claude wants you) keeps priority; the preview takes
    // whatever is left, and always keeps a readable minimum.
    let question_len = snapshot
        .pending_prompt
        .as_ref()
        .and_then(|prompt| prompt.question.as_deref())
        .map(|question| {
            question
                .chars()
                .count()
                .min(TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT)
        })
        .unwrap_or(0);
    let preview_budget = TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT
        .saturating_sub(question_len)
        .max(TELEGRAM_THREAD_SNAPSHOT_MIN_PREVIEW);
    let question_budget =
        TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT.saturating_sub(TELEGRAM_THREAD_SNAPSHOT_MIN_PREVIEW);
    let last_preview = snapshot
        .last_preview
        .as_deref()
        .and_then(|preview| trim_for_telegram_detail(preview, preview_budget));

    json!({
        "type": thread_snapshot_event_type(snapshot),
        "threadId": snapshot.thread_id,
        "updatedAt": snapshot.updated_at,
        "lastPreview": last_preview,
        "thread": {
            "threadId": snapshot.thread_id,
            "name": snapshot.name,
            "displayName": display_name,
            "project": project,
            "cwd": snapshot.cwd,
            "updatedAt": snapshot.updated_at,
            "statusType": snapshot.status_type,
            "statusFlags": snapshot.status_flags,
            "lastTurnStatus": snapshot.last_turn_status,
            "lastPreview": last_preview,
            "pendingPrompt": snapshot
                .pending_prompt
                .as_ref()
                .map(|prompt| pending_prompt_value(prompt, question_budget))
        }
    })
}

pub(crate) fn prepare_telegram_thread_snapshot_delivery(
    chat_id: &str,
    snapshot: &BridgeThreadSnapshot,
) -> Result<PreparedTelegramDelivery> {
    let prepared = prepare_telegram_delivery(chat_id, &thread_snapshot_event(snapshot))?;
    if prepared.payloads.len() != 1 {
        bail!("thread snapshot Telegram delivery exceeded one message");
    }
    Ok(prepared)
}

pub(crate) fn telegram_projects_text(
    config: &DaemonConfig,
    current_project: Option<&RegisteredProject>,
    observed: &[ObservedWorkspace],
) -> String {
    let mut lines = vec!["Projects".to_string(), String::new()];
    match current_project {
        Some(project) => lines.push(format!("Current: {} ({})", project.id, project.label)),
        None => lines.push("Current: none selected".to_string()),
    }
    if config.projects.is_empty() {
        lines.push(String::new());
        lines.push("No projects are configured yet.".to_string());
    } else {
        lines.push(String::new());
        lines.push("Configured:".to_string());
        for project in &config.projects {
            let current = current_project
                .map(|current| current.id == project.id)
                .unwrap_or(false);
            let marker = if current { "•" } else { "-" };
            lines.push(format!(
                "{marker} {} - {}",
                project.id,
                trim_for_telegram_line(&project.label, 80)
            ));
            lines.push(format!("  {}", project.cwd));
        }
    }
    if !observed.is_empty() {
        lines.push(String::new());
        lines.push("Observed from recent Claude history:".to_string());
        for workspace in observed.iter().take(5) {
            lines.push(format!(
                "- {} - {}",
                workspace.label,
                trim_for_telegram_line(&workspace.cwd, 90)
            ));
        }
        lines.push(
            "Run `tinyctb projects import` locally to promote observed workspaces into the curated registry."
                .to_string(),
        );
    }
    lines.push(String::new());
    lines.push("Use /project <id> to switch the current project.".to_string());
    lines.join("\n")
}

pub(crate) fn telegram_project_text(project: Option<&RegisteredProject>) -> String {
    match project {
        Some(project) => format!(
            "Current project\n\n{} ({})\n{}\n\nNew Telegram sessions will start here until you switch again.",
            project.id, project.label, project.cwd
        ),
        None => "No current project is selected.\n\nUse /project to inspect the registry, then /project <id> to choose one."
            .to_string(),
    }
}

pub(crate) fn telegram_help_text() -> String {
    [
        "Claude remote is ready.",
        "",
        "Use Telegram's Reply action on a Claude notification to continue that exact session.",
        "",
        "/away - start remote Claude mode",
        "/back - stop remote Claude mode",
        "/repair - re-check hooks and the claude binary",
        "/status - show remote status",
        "/threads - show the 5 most recent Claude sessions",
        "/threads <count> - show that many recent Claude sessions",
        "/new <prompt> - start a new Claude session",
        "/new - ask for a prompt in a reply",
        "/project - list projects",
        "/project <id> - switch the current project",
    ]
    .join("\n")
}

fn telegram_backend_status_line() -> String {
    let hooks = match crate::hooks::hooks_status() {
        Ok(status) => status,
        Err(error) => {
            return format!("Hooks: status unavailable ({error:#}). Run `tinyctb doctor` locally.")
        }
    };
    let binary = crate::claude::resolve_claude_binary();
    let binary_label = match &binary {
        Ok(resolved) => format!("claude binary ok ({})", resolved.path.display()),
        Err(_) => "claude binary NOT FOUND".to_string(),
    };
    if hooks.get("installed").and_then(Value::as_bool) == Some(true) && binary.is_ok() {
        format!("Backend: hooks installed, {binary_label}.")
    } else {
        let missing = hooks
            .get("missing")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "none".to_string());
        format!("Backend: hooks missing [{missing}], {binary_label}. Use /repair to fix.")
    }
}

pub(crate) fn trim_for_telegram_line(value: &str, max_chars: usize) -> String {
    let mut trimmed = value.trim().replace('\n', " ");
    if trimmed.chars().count() <= max_chars {
        return trimmed;
    }
    trimmed = trimmed.chars().take(max_chars.saturating_sub(1)).collect();
    trimmed.push_str("...");
    trimmed
}

pub(crate) fn telegram_status_text(conn: &Connection) -> Result<String> {
    let away = get_away_mode(conn)?;
    let pending = pending_outbound_count(conn)?;
    let waiting = list_waiting_from_db(conn, None, 5)?;
    let away_label = if away["away"].as_bool() == Some(true) {
        "on"
    } else {
        "off"
    };
    Ok(format!(
        "Claude remote status\n\nRemote mode: {away_label}\n{}\nPending Telegram notifications: {pending}\nSessions waiting for you: {}\n\nUse /away before you leave. Use /repair if hooks look broken. Use /back when you return.",
        telegram_backend_status_line(),
        waiting.summary.count
    ))
}

pub(crate) fn telegram_new_thread_confirmation_text(
    project: &RegisteredProject,
    result: &Value,
) -> Result<String> {
    let cwd = result.get("cwd").and_then(Value::as_str);
    Ok(match cwd {
        Some(cwd) if !cwd.trim().is_empty() => format!(
            "Started a new Claude session in {}.\n{cwd}\n\nClaude is working on the first answer now. I will send the answer here when it finishes.\n\nUse Telegram's Reply action on this message to continue it.",
            project.label
        ),
        _ => format!(
            "Started a new Claude session in {} with no explicit working directory reported back.\n\nClaude is working on the first answer now. I will send the answer here when it finishes.\n\nUse Telegram's Reply action on this message to continue it.",
            project.label
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telegram_message_payload_preserves_full_multiline_claude_body() {
        let full_message =
            "Done.\n\nFirst line stays.\nSecond line also stays.\n\nThird paragraph remains intact.";
        let event = json!({
            "type": "thread_completed",
            "threadId": "thr_done",
            "updatedAt": 42,
            "thread": {
                "displayName": "LinkedIn Network",
                "project": "growth",
                "lastPreview": full_message
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text");
        assert!(text.starts_with("✅ Claude finished\n🧵 LinkedIn Network\n📁 growth"));
        assert!(text.contains(full_message));
        assert!(text
            .contains("💬 To continue this session, use Telegram's Reply action on this message."));
    }

    #[test]
    fn telegram_approval_payload_has_no_buttons_and_points_to_terminal() {
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_approval",
            "updatedAt": 42,
            "thread": {
                "displayName": "Deploy request",
                "project": "infra",
                "pendingPrompt": {
                    "kind": "approval",
                    "question": "Claude needs your permission to use Bash"
                }
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text");
        assert!(text.starts_with("🔐 Claude needs approval"));
        assert!(text.contains("Claude needs your permission to use Bash"));
        assert!(text.contains("Approve or deny this in the terminal"));
        assert!(prepared.callback_routes.is_empty());
        assert!(prepared.payloads[0].get("reply_markup").is_none());
    }

    /// Regression for the "🟡 Claude needs you with no content" report: the
    /// idle-notification question carries no information, so the body must
    /// also show what Claude last said.
    #[test]
    fn waiting_payload_shows_last_answer_next_to_the_generic_question() {
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_idle",
            "updatedAt": 42,
            "lastPreview": "Here is the answer you asked for.",
            "thread": {
                "displayName": "Mahjong training",
                "lastPreview": "Here is the answer you asked for.",
                "pendingPrompt": {
                    "promptKind": "reply",
                    "question": "Claude is waiting for your input"
                }
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.starts_with("🟡 Claude needs you"));
        assert!(
            text.contains("Claude is waiting for your input"),
            "the reason must stay: {text}"
        );
        assert!(
            text.contains("Here is the answer you asked for."),
            "the actual answer must be visible, not just the generic prompt: {text}"
        );
    }

    /// Approval prompts keep the permission line and its tool detail, and
    /// still gain the surrounding context.
    #[test]
    fn approval_payload_keeps_tool_detail_and_adds_context() {
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_approval",
            "updatedAt": 42,
            "thread": {
                "displayName": "Deploy",
                "lastPreview": "I will clean the build directory next.",
                "pendingPrompt": {
                    "promptKind": "approval",
                    "question": "Claude needs your permission to use Bash\n\n⚙️ Bash: rm -rf build/"
                }
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.starts_with("🔐 Claude needs approval"));
        assert!(text.contains("⚙️ Bash: rm -rf build/"), "{text}");
        assert!(
            text.contains("I will clean the build directory next."),
            "{text}"
        );
    }

    /// Regression: the body concatenates question AND preview, so a snapshot
    /// with both fields long must still fit one Telegram message — otherwise
    /// prepare_telegram_thread_snapshot_delivery bails and /threads dies.
    #[test]
    fn snapshot_with_long_question_and_long_preview_stays_one_message() {
        let snapshot = crate::state::BridgeThreadSnapshot {
            thread_id: "thr_long".to_string(),
            name: Some("Very chatty session".to_string()),
            cwd: Some("/home/user/projects/tinyCTB".to_string()),
            updated_at: Some(42),
            status_type: "active".to_string(),
            status_flags: vec!["waitingOnUserInput".to_string()],
            last_turn_status: None,
            last_preview: Some("答".repeat(9_000)),
            pending_prompt: Some(PendingPrompt {
                prompt_id: "notify:1".to_string(),
                kind: "approval".to_string(),
                status: "pending".to_string(),
                question: Some("问".repeat(9_000)),
            }),
            event_uid: None,
        };

        let prepared = prepare_telegram_thread_snapshot_delivery("999", &snapshot)
            .expect("long snapshot must not fail to render");
        assert_eq!(
            prepared.payloads.len(),
            1,
            "snapshot must stay single-message"
        );
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(
            text.chars().count() <= TELEGRAM_MESSAGE_CHAR_LIMIT,
            "rendered {} chars",
            text.chars().count()
        );
        // Both parts survive: the question keeps priority, the answer keeps a
        // guaranteed minimum rather than being squeezed out entirely.
        assert!(text.contains("问问问"), "question missing: {text}");
        assert!(text.contains("答答答"), "preview squeezed out: {text}");
    }

    /// A short answer often appears verbatim inside its own question; that
    /// must NOT count as redundant, or the real answer disappears.
    #[test]
    fn short_answer_contained_in_question_is_still_shown() {
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_yesno",
            "updatedAt": 42,
            "thread": {
                "displayName": "Confirm",
                "lastPreview": "no",
                "pendingPrompt": {
                    "promptKind": "reply",
                    "question": "Please answer yes or no"
                }
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.contains("Please answer yes or no"), "{text}");
        assert!(
            text.contains("\nno"),
            "the actual answer must survive containment dedup: {text}"
        );
    }

    /// The prefix case, distinct from the containment case above: a short
    /// answer that happens to START the question ("No" / "No changes are
    /// required") carries no truncation marker, so it is real content and
    /// must survive.
    #[test]
    fn short_answer_prefixing_question_without_ellipsis_is_still_shown() {
        let event = json!({
            "type": "thread_waiting",
            "threadId": "thr_prefix",
            "updatedAt": 42,
            "thread": {
                "displayName": "Review",
                "lastPreview": "No",
                "pendingPrompt": {
                    "promptKind": "reply",
                    "question": "No changes are required"
                }
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.contains("No changes are required"), "{text}");
        assert!(
            text.contains("\nNo") && text.trim_end().ends_with("Reply action on this message."),
            "an un-truncated short answer must not be swallowed as a prefix: {text}"
        );
        // Belt and braces at the unit level.
        assert_eq!(redundant_pair("No", "No changes are required"), None);
        assert_eq!(
            redundant_pair("No changes…", "No changes are required"),
            Some("No changes are required")
        );
        assert_eq!(redundant_pair("same", "same"), Some("same"));
    }

    /// A truncated preview IS redundant against the full text: show the
    /// complete one only.
    #[test]
    fn truncated_prefix_is_deduped_to_the_complete_text() {
        let full = "Done: the parser is fixed and all tests pass.";
        let event = json!({
            "type": "thread_completed",
            "threadId": "thr_trunc",
            "updatedAt": 42,
            "thread": {
                "displayName": "Parser",
                "lastPreview": "Done: the parser is fixed…",
                "pendingPrompt": { "promptKind": "reply", "question": full }
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.contains(full), "{text}");
        assert!(
            !text.contains("fixed…"),
            "the truncated copy must not be rendered too: {text}"
        );
    }

    /// A completed turn whose preview and question are the same text must not
    /// be rendered twice.
    #[test]
    fn duplicate_question_and_preview_render_once() {
        let answer = "Done: the parser is fixed.";
        let event = json!({
            "type": "thread_completed",
            "threadId": "thr_done",
            "updatedAt": 42,
            "lastPreview": answer,
            "thread": {
                "displayName": "Parser",
                "lastPreview": answer,
                "pendingPrompt": { "promptKind": "reply", "question": answer }
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert_eq!(text.matches(answer).count(), 1, "duplicated body: {text}");
    }

    #[test]
    fn telegram_message_payload_shortens_app_file_reference_tokens() {
        let preview = "Updated the docs. [F:/home/user/project/README.md†L1-L24]";
        let event = json!({
            "type": "thread_completed",
            "threadId": "thr_docs",
            "updatedAt": 42,
            "thread": {
                "displayName": "Docs updated",
                "project": "proj",
                "lastPreview": preview
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text");
        assert!(text.contains("README.md L1-L24"));
        assert!(!text.contains("[F:/home/user/project/README.md"));
    }

    #[test]
    fn telegram_message_payload_splits_without_truncating_claude_body() {
        let long_preview = "x".repeat(10_000);
        let event = json!({
            "type": "thread_completed",
            "threadId": "thr_split",
            "updatedAt": 42,
            "thread": {
                "displayName": "Long answer",
                "project": "ops",
                "lastPreview": long_preview
            }
        });

        let prepared = prepare_telegram_delivery("999", &event).expect("prepared telegram event");
        assert!(
            prepared.payloads.len() > 1,
            "long telegram messages should split"
        );
        for payload in &prepared.payloads {
            let text = payload["text"].as_str().expect("telegram text");
            assert!(text.chars().count() <= TELEGRAM_MESSAGE_CHAR_LIMIT);
        }
        let combined = prepared
            .payloads
            .iter()
            .map(|payload| payload["text"].as_str().expect("telegram text"))
            .collect::<Vec<_>>()
            .join("");
        assert!(combined.contains("Long answer"));
        assert!(combined.contains("💬 To continue this session"));
        assert!(combined.contains(&"x".repeat(5000)));
    }

    #[test]
    fn telegram_thread_snapshot_payload_uses_update_template_as_one_message() {
        let snapshot = crate::state::BridgeThreadSnapshot {
            thread_id: "thr_done".to_string(),
            name: Some("Release checklist".to_string()),
            cwd: Some("/home/user/projects/tinyCTB".to_string()),
            updated_at: Some(42),
            status_type: "idle".to_string(),
            status_flags: Vec::new(),
            last_turn_status: Some("completed".to_string()),
            last_preview: Some("x".repeat(10_000)),
            pending_prompt: None,
            event_uid: None,
        };

        let prepared = prepare_telegram_thread_snapshot_delivery("999", &snapshot)
            .expect("prepared thread snapshot");

        assert_eq!(prepared.thread_id.as_deref(), Some("thr_done"));
        assert_eq!(
            prepared.payloads.len(),
            1,
            "explicit /threads results must be one Telegram message per thread"
        );
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text");
        assert!(text.starts_with("✅ Claude finished\n🧵 Release checklist"));
        assert!(text.contains("📁 tinyCTB"));
        assert!(text.contains("💬 To continue this session"));
        assert!(
            text.chars().count() <= TELEGRAM_MESSAGE_CHAR_LIMIT,
            "snapshot messages must fit Telegram's single-message limit"
        );
    }

    #[test]
    fn telegram_help_text_uses_reduced_remote_command_set() {
        let text = telegram_help_text();
        for expected in [
            "/away - start remote Claude mode",
            "/back - stop remote Claude mode",
            "/status - show remote status",
            "/threads - show the 5 most recent Claude sessions",
            "/new <prompt> - start a new Claude session",
            "/project <id> - switch the current project",
        ] {
            assert!(
                text.contains(expected),
                "help text missing expected command: {expected}"
            );
        }
    }
}
