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
/// An interactive session's gate: not answering costs nothing, because the
/// terminal dialog is still there waiting.
const TELEGRAM_TERMINAL_APPROVAL_HINT: &str =
    "👆 点按钮作答。不答复也不会放行——超时后回落到终端里的权限弹窗。";
/// A headless turn's gate: there is no terminal behind it, so silence is a
/// refusal that stops the task. Say so, or ignoring the message looks free.
const TELEGRAM_HEADLESS_APPROVAL_HINT: &str =
    "👆 点按钮作答。这是你从 Telegram 发起的任务，背后没有终端可回落——超时不答复=拒绝，任务就停在这里。";
/// A BACKGROUND session's gate. Claude Code does render its dialog for these,
/// but onto a pty host nobody watches, so "it falls back to the terminal" is
/// a promise the user cannot cash: measured 2026-08-17, a background session
/// sat blocked for 7h09m behind exactly that invisible dialog.
///
/// NO deadline is named here, and no end EVENT either. The wait was fixed
/// when the gate published (config timeout for a session that had a window,
/// a day for one that did not), while this line may be rendered by a
/// /threads re-offer measuring the session as it is TODAY — a 30-second
/// question backgrounded afterwards would otherwise be advertised as good
/// for 24 hours. "Stuck until it expires" is no better: after the phone
/// window closes Claude may still be blocked at its own permission dialog,
/// which on a background session is just as invisible. So: present tense,
/// current fact, nothing about what happens next.
const TELEGRAM_WINDOWLESS_APPROVAL_HINT: &str =
    "👆 点按钮作答。这个会话在后台运行、没有终端窗口——现在没有任何看得见的弹窗能兜底，只能在这里作答。";
/// Same session shape, question gate: the terminal dialog is invisible, so
/// this message is the only place the question can be answered. Same rule
/// about durations.
const TELEGRAM_WINDOWLESS_QUESTION_HINT: &str =
    "💬 这个会话在后台运行、没有终端窗口——终端里看不到这道题，只能在这里作答。";
/// Neither confirmed: the session has no verified live socket, or reading it
/// failed. "It falls back to the terminal dialog" would be a guess, and the
/// status line right above may already be saying 💤 idle or ❓ unreadable —
/// the message must not contradict itself.
const TELEGRAM_UNVERIFIED_APPROVAL_HINT: &str =
    "👆 点按钮作答。现在确认不了这个会话有没有可见终端——别指望终端弹窗兜底。";
/// Stands in for the liveness line when a gate reports its own session as
/// windowless. /threads rows carry a freshly measured `statusLine` and keep
/// it; a gate's push has no such row behind it, and used to arrive with no
/// hint at all that the terminal it names cannot show anything.
const TELEGRAM_WINDOWLESS_STATUS_LINE: &str = "🫥 后台会话（无终端窗口）· 只能在手机作答";
const TELEGRAM_MESSAGE_CHAR_LIMIT: usize = 4096;
/// Shared budget for a /threads snapshot body (question + preview together),
/// leaving room for the title, name, project and reply-hint lines inside one
/// Telegram message.
const TELEGRAM_THREAD_SNAPSHOT_DETAIL_LIMIT: usize = 3000;
/// The preview never shrinks below this, so a very long question cannot
/// squeeze out Claude's actual answer entirely.
const TELEGRAM_THREAD_SNAPSHOT_MIN_PREVIEW: usize = 600;
/// Roughly how much label a Telegram row shows before buttons get squeezed.
/// Wide (CJK/emoji) characters count double, which is what actually fills the
/// row.
const BUTTON_ROW_WIDTH_BUDGET: usize = 32;
const MAX_BUTTONS_PER_ROW: usize = 3;

fn display_width(text: &str) -> usize {
    text.chars()
        .map(|ch| {
            if (ch as u32) > 0x1100 && !ch.is_ascii() {
                2
            } else {
                1
            }
        })
        .sum()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedTelegramDelivery {
    pub(crate) payloads: Vec<Value>,
    pub(crate) thread_id: Option<String>,
    pub(crate) event_id: String,
    pub(crate) callback_routes: Vec<TelegramCallbackRoute>,
    /// Set for a question: delivery records which message carries it so a
    /// text reply can be matched back to the waiting hook.
    pub(crate) question_id: Option<String>,
    /// Same, for an approval request — used to catch a text reply that was
    /// meant as an answer and tell the user to use the buttons.
    pub(crate) approval_id: Option<String>,
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
        "question_request" => "❓ Claude 在问你",
        _ => "🧵 Claude update",
    }
}

fn telegram_event_reply_hint(event_type: &str, event: &Value) -> &'static str {
    // Where an answer can actually land, as measured by whoever raised this
    // event: the gate for its own session, /threads at list time.
    let visibility = telegram_event_terminal_visibility(event);
    if event_type == "approval_request" {
        // What silence costs differs between the gates, and it is the one
        // thing the user needs to know before deciding to ignore the message.
        // Headless outranks the rest: a `-p` turn has no terminal process
        // behind it at all, which is a stronger statement than "the terminal
        // is there but hidden".
        return if event.get("headless").and_then(Value::as_bool) == Some(true) {
            TELEGRAM_HEADLESS_APPROVAL_HINT
        } else {
            match visibility {
                TerminalVisibility::Background => TELEGRAM_WINDOWLESS_APPROVAL_HINT,
                TerminalVisibility::Unverified => TELEGRAM_UNVERIFIED_APPROVAL_HINT,
                // Unstated keeps the wording it has always had: an older
                // event says nothing, and a headless one never reaches here.
                TerminalVisibility::Window | TerminalVisibility::Unstated => {
                    TELEGRAM_TERMINAL_APPROVAL_HINT
                }
            }
        };
    }
    if telegram_event_is_approval(event) {
        TELEGRAM_APPROVAL_HINT
    } else if event_type == "question_request" && visibility == TerminalVisibility::Background {
        // Deliberately not "点按钮": a multi-select question ships without
        // buttons and is answered by replying, and this hint covers both.
        TELEGRAM_WINDOWLESS_QUESTION_HINT
    } else {
        // A question's generic hint promises no terminal, so `Unverified`
        // needs nothing special here.
        match event_type {
            "thread_waiting" => TELEGRAM_ANSWER_THREAD_HINT,
            _ => TELEGRAM_CONTINUE_THREAD_HINT,
        }
    }
}

/// What the event says about the session's terminal — deliberately NOT a
/// boolean. Flattening it once already produced a message that showed
/// "💤 空闲" or "❓ 状态读取失败" above a hint promising the terminal dialog
/// would catch the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalVisibility {
    /// A terminal window exists behind the session.
    Window,
    /// Alive under the background pty host: a dialog there is invisible.
    Background,
    /// Probed and could not tell, or no verified live socket — nothing to
    /// promise either way.
    Unverified,
    /// The event says nothing: a headless turn (whose `headless` flag is the
    /// stronger statement anyway) or one queued by an older build. Renders
    /// exactly as those messages always have; kept distinct from `Window` so
    /// no code can mistake silence for a measurement.
    Unstated,
}

fn telegram_event_terminal_visibility(event: &Value) -> TerminalVisibility {
    // PRESENT, whatever its shape, means the writer meant to say something.
    // A null / number / object is a value we cannot read — treating it as
    // "absent" would fall through to the legacy branch and hand back the
    // terminal-fallback promise on an event that never claimed a window.
    if let Some(raw) = event.get("terminalVisibility") {
        return match raw.as_str() {
            Some(TERMINAL_VISIBILITY_BACKGROUND) => TerminalVisibility::Background,
            Some(TERMINAL_VISIBILITY_UNVERIFIED) => TerminalVisibility::Unverified,
            Some(TERMINAL_VISIBILITY_WINDOW) => TerminalVisibility::Window,
            // A value from some future build, or one that is not even a
            // string: unknown is not "fine".
            _ => TerminalVisibility::Unverified,
        };
    }
    // The boolean this field replaced shipped in a deployed build, and the
    // outbox keeps whole payloads across restarts and retries — an upgrade
    // must not re-render a background event as a windowed one.
    match event.get("windowless").and_then(Value::as_bool) {
        Some(true) => TerminalVisibility::Background,
        Some(false) => TerminalVisibility::Window,
        None => TerminalVisibility::Unstated,
    }
}

pub(crate) const TERMINAL_VISIBILITY_WINDOW: &str = "window";
pub(crate) const TERMINAL_VISIBILITY_BACKGROUND: &str = "background";
pub(crate) const TERMINAL_VISIBILITY_UNVERIFIED: &str = "unverified";

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
    // A snapshot's liveness line: what is behind the session right now, i.e.
    // where a reply to this very message would land. A measured line always
    // wins; the windowless stand-in only fills the gap a gate's own push
    // leaves, so a /threads row keeps saying what it actually observed.
    if let Some(status_line) = event.get("statusLine").and_then(Value::as_str) {
        lines.push(status_line.to_string());
    } else if telegram_event_terminal_visibility(event) == TerminalVisibility::Background {
        lines.push(TELEGRAM_WINDOWLESS_STATUS_LINE.to_string());
    }
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
    if event_type == "approval_request" || event_type == "question_request" {
        let buttons = event
            .get("buttons")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        // Layout follows label length, not button count: two or three short
        // labels read well side by side (two especially), while a long label
        // gets squeezed until the row stops looking tappable. So pack
        // greedily within a width budget, at most three per row.
        let mut rows: Vec<Value> = Vec::new();
        let mut current_row: Vec<Value> = Vec::new();
        let mut current_width = 0usize;
        for button in &buttons {
            let (Some(text), Some(callback_id)) = (
                button.get("text").and_then(Value::as_str),
                button.get("callbackId").and_then(Value::as_str),
            ) else {
                continue;
            };
            let width = display_width(text);
            if !current_row.is_empty()
                && (current_row.len() >= MAX_BUTTONS_PER_ROW
                    || current_width + width > BUTTON_ROW_WIDTH_BUDGET)
            {
                rows.push(json!(std::mem::take(&mut current_row)));
                current_width = 0;
            }
            current_row.push(json!({
                "text": text,
                "callback_data": format!("claude:{callback_id}")
            }));
            current_width += width;
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
                    question_id: event
                        .get("questionId")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    answer: button
                        .get("answer")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                });
            }
        }
        if !current_row.is_empty() {
            rows.push(json!(current_row));
        }
        if !rows.is_empty() {
            // Buttons ride on the LAST chunk so they sit under the full text.
            if let Some(last) = payloads.last_mut().and_then(Value::as_object_mut) {
                last.insert(
                    "reply_markup".to_string(),
                    json!({ "inline_keyboard": rows }),
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
        question_id: event
            .get("questionId")
            .and_then(Value::as_str)
            .map(str::to_string),
        approval_id: event
            .get("approvalId")
            .and_then(Value::as_str)
            .map(str::to_string),
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
    // Two sessions doing similar work get identical derived names; the short
    // id is what makes them tell apart (and names the exact session a Reply
    // will continue).
    let display_name = format!(
        "{display_name} · {}",
        snapshot.thread_id.chars().take(8).collect::<String>()
    );
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

/// What is behind a session RIGHT NOW — the fact that decides where a reply
/// to it lands, plus whether the user can SEE it. Four states, not two:
/// most sessions are dormant (Idle), and a live session may have no window
/// at all (hosted by Claude's background pty host).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThreadLiveness {
    /// Verified live messaging socket, and where its window is (or isn't).
    pub(crate) presence: crate::claude::TerminalPresence,
    /// A running bridge turn: a Telegram-started task is working now.
    pub(crate) headless: bool,
    /// The state could not be READ (database error). Shown as its own
    /// condition: a read failure dressed up as "idle" would be a silent lie.
    pub(crate) unknown: bool,
}

/// The /threads classes, in list order. Sorting and the census read this
/// same list, so a new state cannot shift one without the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LivenessClass {
    /// 🖥 a terminal window the user can look at.
    Window,
    /// 🫥 alive under the background pty host, no window.
    Background,
    /// 🔌 alive and reachable, window unreadable.
    Unverified,
    /// ⚙️ a running headless turn (whatever its terminal state).
    Headless,
    /// 💤 no live socket and nothing running.
    Idle,
    /// ❓ the row itself could not be read.
    Unknown,
}

impl LivenessClass {
    /// Census label. Every class has one, so the buckets always sum to the
    /// session count.
    pub(crate) fn census_label(self) -> &'static str {
        match self {
            LivenessClass::Window => "🖥 终端",
            LivenessClass::Background => "🫥 后台",
            LivenessClass::Unverified => "🔌 在线",
            LivenessClass::Headless => "⚙️ 无头",
            LivenessClass::Idle => "💤 空闲",
            LivenessClass::Unknown => "❓ 未知",
        }
    }

    /// The classes a census always names, even at zero, so the shape of the
    /// line stays stable between calls. The rest are printed only when
    /// someone is actually in them.
    pub(crate) const ALWAYS_SHOWN: [LivenessClass; 4] = [
        LivenessClass::Window,
        LivenessClass::Background,
        LivenessClass::Headless,
        LivenessClass::Idle,
    ];

    pub(crate) const ALL: [LivenessClass; 6] = [
        LivenessClass::Window,
        LivenessClass::Background,
        LivenessClass::Unverified,
        LivenessClass::Headless,
        LivenessClass::Idle,
        LivenessClass::Unknown,
    ];
}

impl ThreadLiveness {
    /// What this session counts AS: one name that drives BOTH the /threads
    /// sort and the census tally. They were a bare `u8` and a set of literal
    /// comparisons for one release, and inserting a state into the middle
    /// silently relabelled every bucket below it — `Unverified` sessions were
    /// counted as headless, headless as idle, idle as unknown, and plain
    /// unknown fell out of the tally entirely.
    pub(crate) fn class(self) -> LivenessClass {
        use crate::claude::TerminalPresence::*;
        if self.unknown {
            // A known running turn is a fact even when the terminal state is
            // not, so it is still counted as headless.
            return if self.headless {
                LivenessClass::Headless
            } else {
                LivenessClass::Unknown
            };
        }
        match (self.presence, self.headless) {
            (Window, _) => LivenessClass::Window,
            (Background, _) => LivenessClass::Background,
            // Alive (the socket checked out) but the window could not be
            // read: still ahead of a session with no socket at all.
            (Unverified, _) => LivenessClass::Unverified,
            (Gone, true) => LivenessClass::Headless,
            (Gone, false) => LivenessClass::Idle,
        }
    }

    /// Sort key for /threads: what is alive comes before what is dormant,
    /// and what has a window comes before what runs unseen. Declaration
    /// order of `LivenessClass` IS that order.
    pub(crate) fn order(self) -> u8 {
        self.class() as u8
    }

    pub(crate) fn status_line(self) -> String {
        use crate::claude::TerminalPresence::*;
        if self.unknown {
            // The socket being unreadable does not un-know a running turn.
            return if self.headless {
                "⚙️ 无头任务运行中 + ❓ 终端状态读取失败".to_string()
            } else {
                "❓ 状态读取失败（无法判断会话在做什么；回复仍会尝试送达）".to_string()
            };
        }
        let base = match self.presence {
            Window => "🖥 终端活跃（回复直达终端）",
            Background => "🫥 后台运行（无终端窗口，回复仍直达会话）",
            // The routing half IS known here — the socket was verified — so
            // it is still said; only the window claim is withheld.
            Unverified => "🔌 在线（读不到终端状态，回复仍直达会话）",
            // A running `-p` turn accepts no input: a Reply here does NOT
            // join it, it spawns a SECOND concurrent resume. Say so — the
            // status line's one job is where a reply actually lands.
            Gone if self.headless => "⚙️ 无头任务运行中（回复不会进入它，而是另起一个续跑）",
            Gone => "💤 空闲（回复会无头续跑）",
        };
        // A live session can also be running a headless turn; hiding either
        // side would lie.
        if self.headless && self.presence != Gone {
            format!("{base} + ⚙️ 无头任务")
        } else {
            base.to_string()
        }
    }

    /// The same liveness, for a message that carries an OPEN prompt.
    ///
    /// `status_line` ends every state with where a plain Reply lands, and on
    /// a prompt message every one of those clauses is false: a reply here is
    /// taken by the dialog handler — it becomes the question's answer, or is
    /// refused for an approval — and never starts a headless resume, whatever
    /// the session looks like. So this variant states presence and stops.
    /// Where the answer goes is the reply hint's job.
    pub(crate) fn prompt_status_line(self) -> String {
        use crate::claude::TerminalPresence::*;
        if self.unknown {
            return if self.headless {
                "⚙️ 无头任务运行中 + ❓ 终端状态读取失败".to_string()
            } else {
                "❓ 会话状态读取失败".to_string()
            };
        }
        let base = match self.presence {
            Window => "🖥 终端活跃",
            Background => "🫥 后台运行（无终端窗口）",
            Unverified => "🔌 在线（读不到终端状态）",
            Gone if self.headless => "⚙️ 无头任务运行中",
            Gone => "💤 未检测到活跃会话",
        };
        if self.headless && self.presence != Gone {
            format!("{base} + ⚙️ 无头任务")
        } else {
            base.to_string()
        }
    }
}

pub(crate) fn prepare_telegram_thread_snapshot_delivery(
    chat_id: &str,
    snapshot: &BridgeThreadSnapshot,
    liveness: ThreadLiveness,
) -> Result<PreparedTelegramDelivery> {
    let mut event = thread_snapshot_event(snapshot);
    if let Some(object) = event.as_object_mut() {
        // A pending terminal dialog is THE fact about this session — but
        // claim it only for a verifiably live terminal (a stale prompt row
        // can outlive its session), and claim no more than we know: the
        // prompt may never have had a remote window at all, so the line
        // says where it must be answered and nothing about history.
        let status_line = if liveness.presence == crate::claude::TerminalPresence::Window
            && snapshot
                .pending_prompt
                .as_ref()
                .is_some_and(|prompt| prompt.status == "pending")
        {
            format!(
                "⏳ 终端有对话框在等你（需要在终端处理）\n{}",
                liveness.status_line()
            )
        } else {
            liveness.status_line()
        };
        object.insert("statusLine".to_string(), json!(status_line));
    }
    let prepared = prepare_telegram_delivery(chat_id, &event)?;
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
        "/threads - show the 5 most recent Claude sessions",
        "/threads <count> - show that many recent Claude sessions",
        "/stop - stop every running headless turn",
        "/stop <id> - stop only the session whose id starts with that",
        "/new <prompt> - start a new Claude session",
        "/new - ask for a prompt in a reply",
        "/project - list projects",
        "/project <id> - switch the current project",
        "/status - show remote status",
        "/repair - re-check hooks and the claude binary",
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

    /// A gate on a background session pushes a question no terminal can
    /// show. The message has to say so where the reader looks for liveness,
    /// and its hint must not send them to a dialog that is invisible.
    #[test]
    fn a_windowless_question_says_the_terminal_cannot_show_it() {
        let hidden = json!({
            "type": "question_request",
            "threadId": "thr_bg",
            "questionId": "q1",
            "lastPreview": "现在发车？",
            "terminalVisibility": "background",
            "buttons": [{"text": "🔴A 发车", "callbackId": "c1", "action": "answer_question", "answer": "发车"}]
        });
        let text = prepare_telegram_delivery("999", &hidden)
            .expect("prepared")
            .payloads[0]["text"]
            .as_str()
            .expect("telegram text")
            .to_string();
        assert!(text.contains("🫥 后台会话（无终端窗口）"), "{text}");
        assert!(text.contains("终端里看不到这道题"), "{text}");

        // The same question from a session with a window keeps the old shape:
        // no stand-in line, no background claim.
        let visible = json!({
            "type": "question_request",
            "threadId": "thr_tty",
            "questionId": "q2",
            "lastPreview": "现在发车？",
            "buttons": [{"text": "🔴A 发车", "callbackId": "c1", "action": "answer_question", "answer": "发车"}]
        });
        let text = prepare_telegram_delivery("999", &visible)
            .expect("prepared")
            .payloads[0]["text"]
            .as_str()
            .expect("telegram text")
            .to_string();
        assert!(!text.contains("后台会话"), "{text}");
        assert!(!text.contains("终端里看不到这道题"), "{text}");
    }

    /// An approval from a background session must not promise the terminal
    /// fallback that makes silence look free — while a headless turn keeps
    /// its own, stronger warning.
    #[test]
    fn a_windowless_approval_drops_the_terminal_fallback_promise() {
        let approval = |visibility: &str, headless: bool| {
            let event = json!({
                "type": "approval_request",
                "threadId": "thr_bg",
                "approvalId": "a1",
                "lastPreview": "Bash: ls",
                "terminalVisibility": visibility,
                "headless": headless,
                "buttons": [{"text": "✅ 允许", "callbackId": "d1", "action": "approve"}]
            });
            prepare_telegram_delivery("999", &event)
                .expect("prepared")
                .payloads[0]["text"]
                .as_str()
                .expect("telegram text")
                .to_string()
        };
        let hidden = approval("background", false);
        assert!(hidden.contains("没有终端窗口"), "{hidden}");
        assert!(
            !hidden.contains("回落到终端里的权限弹窗"),
            "an invisible dialog is not a fallback: {hidden}"
        );
        let windowed = approval("window", false);
        assert!(windowed.contains("回落到终端里的权限弹窗"), "{windowed}");
        // Headless outranks: no terminal process exists at all, and silence
        // there is a refusal rather than a wait.
        let headless = approval("background", true);
        assert!(headless.contains("超时不答复=拒绝"), "{headless}");

        // Neither confirmed (dead socket, or the liveness read failed): the
        // status line above may already say 💤 or ❓, so the hint must not
        // invent a waiting terminal — nor claim the session is backgrounded.
        let unverified = approval("unverified", false);
        assert!(
            !unverified.contains("回落到终端里的权限弹窗"),
            "an unverifiable terminal is not a fallback: {unverified}"
        );
        assert!(
            unverified.contains("确认不了这个会话有没有可见终端"),
            "{unverified}"
        );
        assert!(!unverified.contains("后台运行"), "{unverified}");

        // A pre-0.2.7 event queued before the field existed keeps the wording
        // it was written for.
        let legacy = json!({
            "type": "approval_request",
            "threadId": "thr_old",
            "approvalId": "a2",
            "lastPreview": "Bash: ls",
            "buttons": []
        });
        let legacy = prepare_telegram_delivery("999", &legacy)
            .expect("prepared")
            .payloads[0]["text"]
            .as_str()
            .expect("telegram text")
            .to_string();
        assert!(legacy.contains("回落到终端里的权限弹窗"), "{legacy}");
    }

    /// No hint may name a wait. The window was fixed when the gate published
    /// (a config timeout for a session that had a window, a day for one that
    /// did not) while these lines are also rendered by /threads re-offers,
    /// which measure the session as it is TODAY: a 30-second question that
    /// was backgrounded afterwards would otherwise advertise 24 hours.
    #[test]
    fn no_hint_promises_a_fixed_wait() {
        for hint in [
            TELEGRAM_WINDOWLESS_APPROVAL_HINT,
            TELEGRAM_WINDOWLESS_QUESTION_HINT,
            TELEGRAM_UNVERIFIED_APPROVAL_HINT,
            TELEGRAM_TERMINAL_APPROVAL_HINT,
            TELEGRAM_HEADLESS_APPROVAL_HINT,
        ] {
            assert!(
                !hint.contains("24") && !hint.contains("小时") && !hint.contains("分钟"),
                "the deadline is not knowable from the hint: {hint}"
            );
        }
        // Stronger for the two "no visible terminal" hints: they must not
        // name an END EVENT either. "Stuck until this request expires" reads
        // as "then it stops being stuck", but the phone window closing only
        // hands the call to a native dialog that, on a background session,
        // is exactly as invisible.
        for hint in [
            TELEGRAM_WINDOWLESS_APPROVAL_HINT,
            TELEGRAM_WINDOWLESS_QUESTION_HINT,
            TELEGRAM_UNVERIFIED_APPROVAL_HINT,
        ] {
            assert!(
                !hint.contains("到期") && !hint.contains("超时"),
                "no terminal to fall back to means no end to promise: {hint}"
            );
        }
    }

    /// R1 of this version shipped a boolean `windowless` for one afternoon,
    /// and the outbox keeps whole payloads across restarts and retries. An
    /// upgrade must not re-render those queued events as windowed sessions —
    /// that would put the terminal-fallback promise back on exactly the
    /// messages this version exists to correct.
    #[test]
    fn a_legacy_boolean_payload_still_renders_as_a_background_session() {
        let approval = |extra: Value| {
            let mut event = json!({
                "type": "approval_request",
                "threadId": "thr_old",
                "approvalId": "a1",
                "lastPreview": "Bash: ls",
                "buttons": []
            });
            if let (Some(object), Some(extra)) = (event.as_object_mut(), extra.as_object()) {
                for (key, value) in extra {
                    object.insert(key.clone(), value.clone());
                }
            }
            prepare_telegram_delivery("999", &event)
                .expect("prepared")
                .payloads[0]["text"]
                .as_str()
                .expect("telegram text")
                .to_string()
        };
        let legacy_background = approval(json!({"windowless": true}));
        assert!(
            legacy_background.contains("🫥 后台会话（无终端窗口）"),
            "{legacy_background}"
        );
        assert!(
            !legacy_background.contains("回落到终端里的权限弹窗"),
            "{legacy_background}"
        );
        let legacy_windowed = approval(json!({"windowless": false}));
        assert!(
            legacy_windowed.contains("回落到终端里的权限弹窗"),
            "{legacy_windowed}"
        );
        // The new field wins when both are present (a mixed payload could
        // only come from a build that writes both, and the newer one is the
        // one this version measures).
        let both = approval(json!({"windowless": true, "terminalVisibility": "window"}));
        assert!(both.contains("回落到终端里的权限弹窗"), "{both}");
        // A value from some later build is not a promise of anything.
        let future = approval(json!({"terminalVisibility": "teleported"}));
        assert!(
            future.contains("确认不了这个会话有没有可见终端"),
            "an unknown state must not be read as a window: {future}"
        );
        // Neither is a value that is not even a string. Treating those as
        // "absent" would fall through to the legacy branch and hand back the
        // terminal promise on an event that never claimed a window.
        for malformed in [json!(null), json!(7), json!({"state": "window"}), json!([])] {
            let text = approval(json!({"terminalVisibility": malformed}));
            assert!(
                text.contains("确认不了这个会话有没有可见终端"),
                "a malformed field is not a window claim: {text}"
            );
            assert!(!text.contains("回落到终端里的权限弹窗"), "{text}");
        }
    }

    /// The live-but-unreadable session, in both status lines: it may say
    /// where a reply lands (the socket was verified) and must not say the
    /// user can see anything.
    #[test]
    fn an_unverified_presence_reports_reachability_without_a_window_claim() {
        let liveness = ThreadLiveness {
            presence: crate::claude::TerminalPresence::Unverified,
            headless: false,
            unknown: false,
        };
        let generic = liveness.status_line();
        assert!(generic.contains("读不到终端状态"), "{generic}");
        assert!(generic.contains("回复仍直达会话"), "{generic}");
        assert!(!generic.contains("终端活跃"), "{generic}");
        let prompt = liveness.prompt_status_line();
        assert!(prompt.contains("读不到终端状态"), "{prompt}");
        assert!(!prompt.contains("终端活跃"), "{prompt}");
        for claim in ["续跑", "送达", "回复"] {
            assert!(!prompt.contains(claim), "{prompt}");
        }
    }

    /// The generic liveness line ends with where a plain Reply lands. On a
    /// message carrying an OPEN prompt every one of those clauses is false:
    /// the dialog handler takes the reply (answer / refusal) and no headless
    /// resume is started. The prompt variant states presence and stops.
    #[test]
    fn a_prompt_status_line_never_says_where_a_reply_lands() {
        use crate::claude::TerminalPresence::*;
        let line = |presence, headless, unknown| {
            ThreadLiveness {
                presence,
                headless,
                unknown,
            }
            .prompt_status_line()
        };
        for text in [
            line(Window, false, false),
            line(Background, false, false),
            line(Gone, false, false),
            line(Gone, true, false),
            line(Window, true, false),
            line(Window, false, true),
        ] {
            for claim in ["续跑", "送达", "回复"] {
                assert!(
                    !text.contains(claim),
                    "a prompt line must not route replies: {text}"
                );
            }
        }
        // It still says the thing the reader needs: which of the three
        // shapes the session is in.
        assert!(line(Background, false, false).contains("无终端窗口"));
        assert!(line(Gone, true, false).contains("无头任务"));
        assert!(line(Window, false, true).contains("读取失败"));
        // And the generic line keeps its routing clause — /threads rows
        // without an open prompt still answer "where does a reply go".
        assert!(ThreadLiveness {
            presence: Gone,
            headless: false,
            unknown: false
        }
        .status_line()
        .contains("续跑"));
    }

    /// A multi-select question ships with NO buttons — it is answered by
    /// replying with letters. Rendered whole, the windowless message must
    /// still tell the reader both facts and never say "点按钮".
    #[test]
    fn a_windowless_multi_select_question_renders_without_button_wording() {
        let event = json!({
            "type": "question_request",
            "threadId": "thr_bg",
            "questionId": "q1",
            "lastPreview": "选哪几项?\n\nA. 甲\nB. 乙\n\n（多选）回复本消息，逗号分隔，例如 A,C",
            "terminalVisibility": "background",
            "buttons": []
        });
        let prepared = prepare_telegram_delivery("999", &event).expect("prepared");
        let text = prepared.payloads[0]["text"]
            .as_str()
            .expect("telegram text")
            .to_string();
        assert!(text.contains("🫥 后台会话（无终端窗口）"), "{text}");
        assert!(text.contains("终端里看不到这道题"), "{text}");
        assert!(text.contains("（多选）回复本消息"), "{text}");
        assert!(
            !text.contains("点按钮"),
            "there are no buttons to point at: {text}"
        );
        assert!(
            prepared.payloads.last().expect("payload")["reply_markup"].is_null(),
            "a multi-select question must ship no keyboard: {:?}",
            prepared.payloads
        );
    }

    /// The stand-in only fills a gap. A /threads row measures liveness for
    /// real and says more (where a reply lands), so its line must survive.
    #[test]
    fn a_measured_status_line_outranks_the_windowless_stand_in() {
        let event = json!({
            "type": "question_request",
            "threadId": "thr_bg",
            "questionId": "q1",
            "lastPreview": "现在发车？",
            "terminalVisibility": "background",
            "statusLine": "🫥 后台运行（无终端窗口，回复仍直达会话）",
            "buttons": []
        });
        let text = prepare_telegram_delivery("999", &event)
            .expect("prepared")
            .payloads[0]["text"]
            .as_str()
            .expect("telegram text")
            .to_string();
        assert!(
            text.contains("🫥 后台运行（无终端窗口，回复仍直达会话）"),
            "{text}"
        );
        assert!(
            !text.contains("🫥 后台会话（无终端窗口）"),
            "the measured line must not be doubled by the stand-in: {text}"
        );
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

    /// Layout follows label length: short labels share a row (two side by
    /// side reads best), long ones get a row each so they stay legible.
    #[test]
    fn button_rows_follow_label_length() {
        let question = json!({
            "type": "question_request",
            "threadId": "t",
            "questionId": "q1",
            "lastPreview": "选哪个?",
            "buttons": [
                {"text": "A. 交给 Sol 再审", "callbackId": "c1", "action": "answer_question", "answer": "交给 Sol 再审"},
                {"text": "B. 先关 away 休息", "callbackId": "c2", "action": "answer_question", "answer": "先关 away 休息"},
                {"text": "C. 继续做 /stop 中断", "callbackId": "c3", "action": "answer_question", "answer": "继续做"}
            ]
        });
        let prepared = prepare_telegram_delivery("999", &question).expect("prepared");
        let keyboard = prepared.payloads.last().expect("payload")["reply_markup"]
            ["inline_keyboard"]
            .as_array()
            .expect("keyboard")
            .clone();
        assert_eq!(
            keyboard.len(),
            3,
            "long options get a row each: {keyboard:?}"
        );
        for row in &keyboard {
            assert_eq!(row.as_array().expect("row").len(), 1, "{keyboard:?}");
        }

        // Two short options belong side by side.
        let short = json!({
            "type": "question_request",
            "threadId": "t",
            "questionId": "q2",
            "lastPreview": "继续吗?",
            "buttons": [
                {"text": "🔴A 是", "callbackId": "s1", "action": "answer_question", "answer": "是"},
                {"text": "🟠B 否", "callbackId": "s2", "action": "answer_question", "answer": "否"}
            ]
        });
        let prepared = prepare_telegram_delivery("999", &short).expect("prepared");
        let keyboard = prepared.payloads.last().expect("payload")["reply_markup"]
            ["inline_keyboard"]
            .as_array()
            .expect("keyboard")
            .clone();
        assert_eq!(
            keyboard.len(),
            1,
            "two short options share a row: {keyboard:?}"
        );
        assert_eq!(keyboard[0].as_array().expect("row").len(), 2);

        let approval = json!({
            "type": "approval_request",
            "threadId": "t",
            "approvalId": "a1",
            "lastPreview": "Bash: ls",
            "buttons": [
                {"text": "✅ 允许", "callbackId": "d1", "action": "approve"},
                {"text": "🔁 本会话都允许", "callbackId": "d2", "action": "approve_session"},
                {"text": "❌ 拒绝", "callbackId": "d3", "action": "deny"}
            ]
        });
        let prepared = prepare_telegram_delivery("999", &approval).expect("prepared");
        let keyboard = prepared.payloads.last().expect("payload")["reply_markup"]
            ["inline_keyboard"]
            .as_array()
            .expect("keyboard")
            .clone();
        assert_eq!(keyboard.len(), 1, "approval buttons stay on one row");
        assert_eq!(keyboard[0].as_array().expect("row").len(), 3);
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
    /// A snapshot with a pending terminal dialog carries the loud prefix —
    /// the user must learn it waits AND that only the terminal can answer.
    #[test]
    fn terminal_dialog_snapshot_carries_the_waiting_prefix() {
        let snapshot = BridgeThreadSnapshot {
            thread_id: "thr-dialog".to_string(),
            name: Some("s".to_string()),
            cwd: None,
            updated_at: Some(42),
            status_type: "active".to_string(),
            status_flags: vec![],
            last_turn_status: None,
            last_preview: None,
            pending_prompt: Some(PendingPrompt {
                prompt_id: "notify:9".to_string(),
                kind: "approval".to_string(),
                status: "pending".to_string(),
                question: Some("Claude needs your permission".to_string()),
                transcript_bytes: None,
                notification_type: None,
            }),
            event_uid: None,
        };
        let prepared = prepare_telegram_thread_snapshot_delivery(
            "999",
            &snapshot,
            ThreadLiveness {
                presence: crate::claude::TerminalPresence::Window,
                headless: false,
                unknown: false,
            },
        )
        .expect("render");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.contains("⏳ 终端有对话框在等你"), "{text}");
        assert!(text.contains("需要在终端处理"), "{text}");

        // A DEAD session's stale prompt row claims nothing: no live
        // terminal, no "waiting for you" line.
        let prepared = prepare_telegram_thread_snapshot_delivery(
            "999",
            &snapshot,
            ThreadLiveness {
                presence: crate::claude::TerminalPresence::Gone,
                headless: false,
                unknown: false,
            },
        )
        .expect("render");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(
            !text.contains("⏳ 终端有对话框"),
            "a ghost session must not claim a waiting terminal: {text}"
        );

        // Background = no window: claiming "终端有对话框在等你" while the
        // same message says 无终端窗口 would contradict itself.
        let prepared = prepare_telegram_thread_snapshot_delivery(
            "999",
            &snapshot,
            ThreadLiveness {
                presence: crate::claude::TerminalPresence::Background,
                headless: false,
                unknown: false,
            },
        )
        .expect("render");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(
            !text.contains("⏳ 终端有对话框"),
            "a windowless session must not claim a waiting terminal: {text}"
        );
    }

    /// The liveness line tells the user where a reply to THIS message will
    /// land — the one fact that differs between the three session states.
    #[test]
    fn snapshot_message_carries_its_liveness_line() {
        let base = BridgeThreadSnapshot {
            thread_id: "thr-live".to_string(),
            name: Some("session".to_string()),
            cwd: Some("/home/user/x".to_string()),
            updated_at: Some(42),
            status_type: "active".to_string(),
            status_flags: vec![],
            last_turn_status: None,
            last_preview: Some("最近的回答".to_string()),
            pending_prompt: None,
            event_uid: None,
        };
        let case = |presence: crate::claude::TerminalPresence, headless: bool, needle: &str| {
            let prepared = prepare_telegram_thread_snapshot_delivery(
                "999",
                &base,
                ThreadLiveness {
                    presence,
                    headless,
                    unknown: false,
                },
            )
            .expect("render");
            let text = prepared.payloads[0]["text"]
                .as_str()
                .expect("text")
                .to_string();
            assert!(text.contains(needle), "missing {needle:?} in: {text}");
            // The short id keeps same-named sessions apart.
            assert!(text.contains("thr-live"), "short id missing: {text}");
        };
        use crate::claude::TerminalPresence::*;
        case(Window, false, "🖥 终端活跃");
        case(Background, false, "🫥 后台运行");
        case(Gone, true, "另起一个续跑");
        case(Gone, false, "💤 空闲");
        case(Window, true, "+ ⚙️ 无头任务");
        // A read failure must not masquerade as idleness.
        let prepared = prepare_telegram_thread_snapshot_delivery(
            "999",
            &base,
            ThreadLiveness {
                presence: Gone,
                headless: false,
                unknown: true,
            },
        )
        .expect("render");
        let text = prepared.payloads[0]["text"].as_str().expect("text");
        assert!(text.contains("❓ 状态读取失败"), "{text}");
        assert!(
            !text.contains("💤"),
            "unknown must not display as idle: {text}"
        );
    }

    /// /threads groups by what is alive: terminals, then running headless
    /// turns, then the idle rest.
    #[test]
    fn liveness_orders_alive_before_dormant() {
        use crate::claude::TerminalPresence::*;
        let window = ThreadLiveness {
            presence: Window,
            headless: false,
            unknown: false,
        };
        let background = ThreadLiveness {
            presence: Background,
            headless: false,
            unknown: false,
        };
        let headless = ThreadLiveness {
            presence: Gone,
            headless: true,
            unknown: false,
        };
        let idle = ThreadLiveness {
            presence: Gone,
            headless: false,
            unknown: false,
        };
        // Alive, but nobody could read whether it has a window: still a
        // live session, so ahead of everything dormant.
        let unverified = ThreadLiveness {
            presence: Unverified,
            headless: false,
            unknown: false,
        };
        assert!(window.order() < background.order());
        assert!(background.order() < unverified.order());
        assert!(unverified.order() < headless.order());
        assert!(headless.order() < idle.order());
        let unknown = ThreadLiveness {
            presence: Gone,
            headless: false,
            unknown: true,
        };
        assert!(idle.order() < unknown.order());
    }

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
                transcript_bytes: None,
                notification_type: None,
            }),
            event_uid: None,
        };

        let prepared = prepare_telegram_thread_snapshot_delivery(
            "999",
            &snapshot,
            ThreadLiveness {
                presence: crate::claude::TerminalPresence::Gone,
                headless: false,
                unknown: false,
            },
        )
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

        let prepared = prepare_telegram_thread_snapshot_delivery(
            "999",
            &snapshot,
            ThreadLiveness {
                presence: crate::claude::TerminalPresence::Gone,
                headless: false,
                unknown: false,
            },
        )
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
