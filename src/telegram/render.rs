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
const TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT: usize = 3000;

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

fn telegram_event_detail(event: &Value) -> Option<String> {
    event
        .pointer("/thread/pendingPrompt/question")
        .and_then(Value::as_str)
        .or_else(|| event.pointer("/thread/lastPreview").and_then(Value::as_str))
        .or_else(|| event.get("lastPreview").and_then(Value::as_str))
        .or_else(|| event.get("message").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
        .map(sanitize_telegram_detail)
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

    let payloads = split_telegram_text(&lines.join("\n"), TELEGRAM_MESSAGE_CHAR_LIMIT)
        .into_iter()
        .map(|text| {
            json!({
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": true
            })
        })
        .collect::<Vec<_>>();

    // Claude Code permission prompts belong to the interactive terminal
    // session; there is no remote approval channel, so no inline keyboard is
    // attached. The callback route table stays in the schema for future use.
    Ok(PreparedTelegramDelivery {
        payloads,
        thread_id,
        event_id,
        callback_routes: Vec::new(),
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

fn pending_prompt_value(prompt: &PendingPrompt) -> Value {
    json!({
        "promptId": prompt.prompt_id,
        "promptKind": prompt.kind,
        "promptStatus": prompt.status,
        "kind": prompt.kind,
        "status": prompt.status,
        "question": prompt
            .question
            .as_deref()
            .and_then(|question| trim_for_telegram_detail(
                question,
                TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT
            ))
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
    let last_preview = snapshot.last_preview.as_deref().and_then(|preview| {
        trim_for_telegram_detail(preview, TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT)
    });

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
            "pendingPrompt": snapshot.pending_prompt.as_ref().map(pending_prompt_value)
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
