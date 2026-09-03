//! Answering a background fork's NATIVE prompt from two surfaces at once.
//!
//! A background fork (`claude --bg`, or a daemon fork) runs its
//! AskUserQuestion and approval dialogs on a hidden pty. `claude attach
//! <id>` opens a window onto that pty, and — proven 2026-09-02 on a
//! throwaway session — attach is BIDIRECTIONAL: keystrokes written to the
//! attach client's pty reach the fork's dialog and answer it. So one
//! native dialog serves both surfaces the user's law demands
//! ("双向推送，谁先抢答算谁的"):
//!   - LOCAL: a `gnome-terminal` running `claude attach <id>` — real keys.
//!   - PHONE: this module drives a headless `claude attach <id>` in a pty
//!     it owns and injects the chosen option's keystrokes.
//!
//! Whoever completes an answer first wins; the dialog is gone for the loser.
//!
//! This never stops or signals the fork or any TUI — it only opens a new
//! window and speaks to the fork the exact way a person at the keyboard
//! would. That is what makes it safe where the SIGSTOP takeover was not
//! (see the memory note `terminal-takeover-impossible`).

use anyhow::{anyhow, Context, Result};
use std::os::unix::io::RawFd;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// A fork id shortened for a window title, the way `claude` prints it.
fn short_id(session_id: &str) -> String {
    session_id.chars().take(8).collect()
}

/// The digits a person TYPES to pick 0-based `index`. Claude Code's
/// AskUserQuestion selector NUMBERS its options (`1. RED`, `2. GREEN`, …) and
/// typing the option's 1-based number selects it — VERIFIED live 2026-09-03 on
/// a real `claude attach` dialog (injected the digit for index 2 and the fork
/// answered "BLUE", the 3rd option). Arrow keys also navigate it, but typing
/// the number is ABSOLUTE: it does not depend on where any other attached
/// client left the highlight, which is exactly what the two-surface race
/// needs. An AskUserQuestion never has more than a handful of options, so the
/// number is a single digit.
pub(crate) fn option_digits(index: usize) -> Vec<u8> {
    (index + 1).to_string().into_bytes()
}

/// The full keystrokes to pick option `index`: its 1-based number, then
/// Enter, which submits the typed number. The phone injects single-select
/// only (approvals included — two options, allow/deny); multi-select keeps
/// the comma-reply path.
pub(crate) fn option_keystrokes(index: usize) -> Vec<u8> {
    let mut keys = option_digits(index);
    keys.push(b'\r');
    keys
}

// ---- the visible local window --------------------------------------------

/// The `gnome-terminal` argv that opens the fork's native dialog in a
/// window. Pure, so the wiring is testable without a display.
///
/// `claude attach` takes the SHORT 8-char job id (what `claude agents` prints as
/// `id`), NOT the full session UUID — passing the full UUID fails with "No job
/// matching …" and the attach exits immediately (the on-machine cause of both
/// the "flashing" local window and the phone-inject "no chrome" failure). So the
/// attach argument is `short_id(session_id)`.
pub(crate) fn attach_window_argv(session_id: &str, claude_bin: &str) -> Vec<String> {
    vec![
        "--title".to_string(),
        format!("tinyCTB · 后台任务 {} 待答", short_id(session_id)),
        "--".to_string(),
        claude_bin.to_string(),
        "attach".to_string(),
        short_id(session_id),
    ]
}

/// A hook or daemon may run under an environment that lost the X session;
/// fill in this machine's defaults for whatever is missing so the window
/// can reach the display.
fn fill_x_env(cmd: &mut Command) {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR")
        .unwrap_or_else(|_| format!("/run/user/{}", unsafe { libc::getuid() }));
    if std::env::var_os("DISPLAY").is_none() {
        cmd.env("DISPLAY", ":1");
    }
    if std::env::var_os("XAUTHORITY").is_none() {
        cmd.env("XAUTHORITY", format!("{runtime_dir}/gdm/Xauthority"));
    }
    if std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_none() {
        cmd.env(
            "DBUS_SESSION_BUS_ADDRESS",
            format!("unix:path={runtime_dir}/bus"),
        );
    }
}

/// The terminal emulator to pop. Tests point this at a stub via
/// TINYCTB_TEST_TERMINAL; production uses `gnome-terminal` (verified
/// available and able to launch from a clean daemon environment).
fn terminal_bin() -> PathBuf {
    #[cfg(test)]
    return PathBuf::from(
        std::env::var("TINYCTB_TEST_TERMINAL")
            .unwrap_or_else(|_| "/nonexistent/tinyctb-test-terminal-unset".to_string()),
    );
    #[cfg(not(test))]
    PathBuf::from("gnome-terminal")
}

/// Open the fork's native dialog in a desktop window. The window closes when
/// `claude attach` exits (when the viewer detaches or the session ends).
/// Fire-and-forget: a failure to pop the window is not fatal — the phone
/// remains the other surface.
pub(crate) fn pop_attach_window(session_id: &str) -> Result<()> {
    // The window runs the SAME claude the daemon resolves (CLAUDE_BIN / a
    // wrapper), never a bare PATH lookup. An invalid CLAUDE_BIN is an ERROR, not
    // a fallback — better no window than one driving the wrong binary; the
    // caller logs it and the phone remains the other surface.
    let claude_bin = crate::claude::resolve_claude_binary()
        .context("resolve claude for attach window")?
        .path
        .to_string_lossy()
        .into_owned();
    let mut cmd = Command::new(terminal_bin());
    cmd.args(attach_window_argv(session_id, &claude_bin))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    fill_x_env(&mut cmd);
    cmd.spawn()
        .with_context(|| format!("pop attach window for {session_id}"))?;
    Ok(())
}

// ---- the headless phone-side injection ------------------------------------

/// How long to WAIT for `claude attach` to connect and the fork's dialog to
/// render before giving up, and how long to read the fork's next state after
/// the Enter. The connect budget is a CEILING, not a fixed sleep — the presence
/// check returns the instant the dialog's chrome appears (see `wait_for_chrome`),
/// so a generous ceiling only bounds a dialog that never shows. It is generous
/// because the real `claude attach` can take a second or more to paint (proven
/// on-machine 2026-09-03: the chrome surfaced ~2s after connect), and a check
/// that raced that render was the 0.2.11 phone-inject failure.
const ATTACH_CONNECT_WAIT: Duration = Duration::from_secs(8);
const ATTACH_SETTLE_WAIT: Duration = Duration::from_secs(3);

/// The selector's own footer hint, drawn ONLY while an AskUserQuestion dialog
/// is live (`Enter to select · ↑/↓ to navigate · Esc to cancel`) — once
/// answered the dialog collapses to `User answered Claude's questions: …`, so
/// this line is gone, which is what makes it a reliable "the dialog is still
/// up" signature (unlike the option labels, which echo into scrollback). It is
/// English and fixed, regardless of the question's own language. VERIFIED live
/// on a real `claude attach` dialog 2026-09-03 — the earlier `Select with
/// numbers` guess (read from a different, non-interactive selector component)
/// never matched the interactive dialog.
const SELECTOR_CHROME: &[u8] = b"Enter to select";

/// A person keys "3 ⏎" with a beat between the digit and Enter; leave the
/// same beat so the selector's input buffer holds the digit before Enter
/// reads it — Enter on an empty buffer would submit the DEFAULTS.
const SELECTOR_KEY_GAP: Duration = Duration::from_millis(200);

/// The default (connect, settle) waits — overridable in tests so a stub
/// attach need not burn the real seconds (up to an 8s connect ceiling plus a
/// 3s settle). The connect value is a CEILING: `wait_for_chrome` returns the
/// instant the dialog appears, so a live dialog costs only its render time.
fn default_waits() -> (Duration, Duration) {
    #[cfg(test)]
    if let Ok(ms) = std::env::var("TINYCTB_TEST_ATTACH_WAIT_MS") {
        let d = Duration::from_millis(ms.parse().unwrap_or(600));
        return (d, d);
    }
    (ATTACH_CONNECT_WAIT, ATTACH_SETTLE_WAIT)
}

/// What an injection attempt did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InjectOutcome {
    /// The keystrokes were put in front of a LIVE dialog: the selector chrome was
    /// present, and the option number + Enter were written to it. This does NOT
    /// claim the fork RECORDED them — the PostToolUse "answered" hook records the
    /// fork's OWN result authoritatively when the turn completes (whichever
    /// surface won the "谁先抢答" race, and exactly what it chose). It only says
    /// "delivered to a live dialog", so the phone button is consumed and need not
    /// retry. This is what lets the inject side stop scraping the pty to GUESS the
    /// outcome — the guess (and its unrecoverable false-positive) is gone.
    Delivered,
    /// The dialog was never found: the selector chrome did not appear within the
    /// budget (already answered/closed, still connecting, or the attach failed).
    /// NOTHING was typed. The phone reports this as retryable and records nothing;
    /// the row stays open for another surface, and the hook settles it if an
    /// answer lands, else it expires.
    Unreachable,
}

/// Answer a background fork's native single-select dialog by option index,
/// but — when `verify_present` — ONLY if the selector is still showing AND
/// `still_authorized()` says THIS question is still the fork's open one, the
/// safety the "谁先抢答" design rests on. A pty child execs `claude attach
/// <id>`; the parent waits for the selector chrome, re-checks `still_authorized`
/// (the generic chrome cannot tell one question's dialog from the next's, so the
/// DB row's status is the identity check), and only then types the option's
/// number + Enter. With `verify_present` false it always injects — for the
/// manual command, where the caller vouches the dialog is up.
pub(crate) fn inject_option(
    session_id: &str,
    index: usize,
    verify_present: bool,
    still_authorized: impl Fn() -> bool,
) -> Result<InjectOutcome> {
    let (connect, settle) = default_waits();
    inject_option_timed(
        session_id,
        index,
        verify_present,
        connect,
        SELECTOR_KEY_GAP,
        settle,
        still_authorized,
    )
}

/// The manual-command path: inject keystrokes with no presence check.
pub(crate) fn inject_via_attach(session_id: &str, keystrokes: &[u8]) -> Result<()> {
    let (connect, settle) = default_waits();
    with_attach_pty(session_id, |master| {
        drain_capture(master, connect);
        write_all(master, keystrokes)?;
        drain_capture(master, settle);
        Ok(())
    })
}

pub(crate) fn inject_option_timed(
    session_id: &str,
    index: usize,
    verify_present: bool,
    connect_wait: Duration,
    key_gap: Duration,
    settle_wait: Duration,
    still_authorized: impl Fn() -> bool,
) -> Result<InjectOutcome> {
    with_attach_pty(session_id, |master| {
        if !verify_present {
            // The manual/vouched path: let attach paint, type the number +
            // Enter (the caller vouches the dialog is up).
            drain_capture(master, connect_wait);
            write_all(master, &option_digits(index))?;
            drain_capture(master, key_gap);
            write_all(master, b"\r")?;
            drain_capture(master, settle_wait);
            return Ok(InjectOutcome::Delivered);
        }
        // WAIT for the fork's dialog to render, then DELIVER the keystrokes into
        // it. We do NOT scrape the screen afterwards to guess whether the fork
        // "took" the answer — the PostToolUse hook records the fork's OWN result.
        // `wait_for_chrome` accumulates until the selector chrome appears — the
        // real `claude attach` can take a second or more to connect and paint
        // (on-machine 2026-09-03: ~2s), and the chrome is scrollback-safe on a
        // fresh forkpty, so accumulating cannot be fooled by history. No chrome
        // within the budget ⇒ no live dialog ⇒ Unreachable (retryable, nothing
        // typed).
        let (intro, present) = wait_for_chrome(master, connect_wait);
        if !present {
            ilog(format!(
                "inject {}: intro {}B no chrome -> Unreachable",
                short_id(session_id),
                intro.len(),
            ));
            return Ok(InjectOutcome::Unreachable);
        }
        // A dialog is up — but is it OURS? `wait_for_chrome` may have waited
        // seconds, during which this fork could have ANSWERED our question and
        // moved on to the NEXT one, whose generic `Enter to select` chrome is
        // indistinguishable. The gate settles our row the instant a new native
        // question opens (`settle_stale_native_questions`), so re-checking the
        // DB row's status HERE — after the chrome is up, right before we commit
        // any key — REVOKES an in-flight injection whose question is gone: we
        // must NOT drive a different question's dialog. This DB status IS the
        // question-instance identity the chrome cannot give us. Nothing has been
        // typed yet, so this bail is a clean, retryable Unreachable.
        //
        // ACCEPTED RESIDUAL (documented inherent limit, not chased): there is a
        // microsecond window between this status read and the digit write below
        // in which — only if the fork answered THIS question locally AND the
        // next question's create/settle transaction committed AND that new dialog
        // rendered AND the OS descheduled us across all of it — a key could still
        // reach a different question. Closing it fully needs a cross-process
        // SQLite writer guard held across the pty writes; for a single-user tool
        // that theoretical race is out of scope. The authoritative hook still
        // records only what the fork actually took.
        if !still_authorized() {
            ilog(format!(
                "inject {}: question no longer the fork's open one -> Unreachable",
                short_id(session_id),
            ));
            return Ok(InjectOutcome::Unreachable);
        }
        // Committed: type the number, a beat (`key_gap`, so the selector buffers
        // the digit before Enter reads it — Enter on an empty buffer submits the
        // defaults), then Enter. We do NOT re-check and bail AFTER the digit: a
        // withheld Enter would strand the digit in the buffer and a retry would
        // append another. The residual — a LOCAL keyboard answer during the
        // digit→Enter beat — can send a stray Enter into THIS same fork's next
        // state, which a working fork ignores; it can no longer drive a DIFFERENT
        // question (the identity check above already ruled that out).
        write_all(master, &option_digits(index))?;
        drain_capture(master, key_gap);
        // The digit is now in the selector's buffer. If the Enter write fails
        // HERE (e.g. the attach closed → EIO), do NOT surface a retryable error —
        // a retry would append a SECOND digit to a buffer that may still hold the
        // first. Treat a post-digit failure as Delivered (NON-retryable): the
        // digit reached the live dialog and the authoritative hook settles the row
        // when the fork completes; if nothing ever submits it, the row expires. (A
        // failure of the DIGIT write above types nothing, so its `?` → retryable
        // is correct.)
        if let Err(err) = write_all(master, b"\r") {
            ilog(format!(
                "inject {}: enter write failed after digit ({err}) -> Delivered (non-retryable)",
                short_id(session_id),
            ));
            return Ok(InjectOutcome::Delivered);
        }
        drain_capture(master, settle_wait);
        ilog(format!(
            "inject {}: intro {}B chrome present, authorized -> Delivered",
            short_id(session_id),
            intro.len(),
        ));
        Ok(InjectOutcome::Delivered)
    })
}

/// Whether the AskUserQuestion selector is LIVE on the captured screen: its
/// prompt chrome (`SELECTOR_CHROME`) appears as a byte substring. Unlike the
/// option labels — which also echo into scrollback — this line is drawn only
/// while the dialog is up, so it tells a live dialog apart from history.
fn dialog_present(screen: &[u8]) -> bool {
    screen
        .windows(SELECTOR_CHROME.len())
        .any(|window| window == SELECTOR_CHROME)
}

/// Run `claude attach <id>` on a pty and hand the master fd to `drive`,
/// then always reap the child and close the fd.
fn with_attach_pty<T>(session_id: &str, drive: impl FnOnce(RawFd) -> Result<T>) -> Result<T> {
    // Everything the child needs is built HERE, in the parent, BEFORE
    // forkpty: after the fork the child may call only async-signal-safe
    // functions. This daemon is multithreaded, so a `malloc` in the child —
    // a `CString` allocation, or the PATH search `execvp` does — can deadlock
    // on an allocator lock some other thread was holding at fork time. So
    // resolve the program to an absolute path now and hand the child a ready
    // argv it only has to `execv`.
    let prog = std::ffi::CString::new(resolve_program(&attach_program())?)
        .map_err(|_| anyhow!("attach program path has an interior NUL"))?;
    let arg_attach = std::ffi::CString::new("attach").expect("literal has no NUL");
    // `claude attach` takes the SHORT 8-char job id (what `claude agents` prints
    // as `id`), NOT the full session UUID — the full UUID fails with "No job
    // matching …" and the attach exits at once (the real cause of the phone
    // inject's "no chrome → Unreachable" on-machine).
    let arg_id = std::ffi::CString::new(short_id(session_id))
        .map_err(|_| anyhow!("session id has an interior NUL"))?;
    let argv: [*const libc::c_char; 4] = [
        prog.as_ptr(),
        arg_attach.as_ptr(),
        arg_id.as_ptr(),
        std::ptr::null(),
    ];

    // Build the child's ENVIRONMENT here too, so the child only `execve`s — no
    // allocation after the fork. The daemon runs under systemd with NO `TERM`,
    // and without it `claude attach` renders a DEGRADED view with no
    // AskUserQuestion selector (no chrome) — the on-machine cause of the
    // phone-inject "no chrome → Unreachable" failure. So carry the parent's
    // environment through and ensure a `TERM` is present so the interactive
    // dialog actually paints.
    use std::os::unix::ffi::OsStrExt as _;
    let mut env_cstrings: Vec<std::ffi::CString> = Vec::new();
    let mut has_term = false;
    for (key, value) in std::env::vars_os() {
        if key.as_bytes() == b"TERM" {
            has_term = true;
        }
        let mut kv = Vec::with_capacity(key.as_bytes().len() + value.as_bytes().len() + 1);
        kv.extend_from_slice(key.as_bytes());
        kv.push(b'=');
        kv.extend_from_slice(value.as_bytes());
        if let Ok(cs) = std::ffi::CString::new(kv) {
            env_cstrings.push(cs);
        }
    }
    if !has_term {
        env_cstrings
            .push(std::ffi::CString::new("TERM=xterm-256color").expect("literal has no NUL"));
    }
    let mut envp: Vec<*const libc::c_char> = env_cstrings.iter().map(|c| c.as_ptr()).collect();
    envp.push(std::ptr::null());

    let mut master: RawFd = 0;
    // A real window size so the dialog lays out normally and the arrow-key
    // navigation lands where `option_keystrokes` expects.
    let winsize = libc::winsize {
        ws_row: 50,
        ws_col: 200,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pid = unsafe {
        libc::forkpty(
            &mut master,
            std::ptr::null_mut(),
            std::ptr::null(),
            &winsize,
        )
    };
    if pid < 0 {
        return Err(anyhow!("forkpty: {}", std::io::Error::last_os_error()));
    }
    if pid == 0 {
        // Child: the pty slave is already our controlling terminal and stdio.
        // Async-signal-safe calls ONLY from here. `execve` (not `execvp`):
        // `prog` is absolute (no PATH search, no allocation) and `envp` (built
        // above with a guaranteed `TERM`) is passed explicitly so the dialog
        // renders even under the daemon's TERM-less environment. On failure, die
        // loudly.
        unsafe {
            libc::execve(prog.as_ptr(), argv.as_ptr(), envp.as_ptr());
            libc::_exit(127);
        }
    }
    let result = drive(master);
    unsafe {
        libc::kill(pid, libc::SIGKILL);
        let mut status = 0;
        // Reap, retrying only on EINTR so the child never lingers as a zombie.
        while libc::waitpid(pid, &mut status, 0) < 0 {
            if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
        libc::close(master);
    }
    result
}

/// The attach client binary. Tests point this at a stub that records the
/// keystrokes it received; production uses the real `claude`.
fn attach_program() -> String {
    #[cfg(test)]
    return std::env::var("TINYCTB_TEST_ATTACH")
        .unwrap_or_else(|_| "/nonexistent/tinyctb-test-attach-unset".to_string());
    #[cfg(not(test))]
    "claude".to_string()
}

/// Resolve a program name to an absolute path in the PARENT (allocation is
/// fine here), so the forked child can `execv` with no PATH search. A name
/// that already contains a slash is taken as-is; an unresolved bare name is
/// returned unchanged so `execv` fails loudly into `_exit(127)`.
fn resolve_program(name: &str) -> Result<String> {
    // The attach client IS claude, so honour tinyCTB's authoritative resolver
    // (CLAUDE_BIN override, then discovery) — the same binary the rest of the
    // daemon spawns. An INVALID `CLAUDE_BIN` is an ERROR here, never a silent
    // fallback to some other PATH claude (that is the resolver's contract).
    // This runs in the PARENT, before forkpty, so its allocation and
    // `--version` probe are safe. Tests point `attach_program` at a stub path
    // (which has a slash), so they never reach this branch.
    #[cfg(not(test))]
    if name == "claude" {
        let resolved = crate::claude::resolve_claude_binary()?;
        return Ok(resolved.path.to_string_lossy().into_owned());
    }
    if name.contains('/') {
        return Ok(name.to_string());
    }
    if let Ok(path) = std::env::var("PATH") {
        for dir in path.split(':').filter(|dir| !dir.is_empty()) {
            let candidate = std::path::Path::new(dir).join(name);
            if candidate.is_file() {
                return Ok(candidate.to_string_lossy().into_owned());
            }
        }
    }
    Ok(name.to_string())
}

fn write_all(master: RawFd, bytes: &[u8]) -> Result<()> {
    // A pty master can accept a short write; loop until every byte is in, and
    // treat EINTR as a retry rather than a lost keystroke.
    let mut offset = 0;
    while offset < bytes.len() {
        let n = unsafe {
            libc::write(
                master,
                bytes[offset..].as_ptr() as *const libc::c_void,
                bytes.len() - offset,
            )
        };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(anyhow!("write keystrokes: {err}"));
        }
        if n == 0 {
            return Err(anyhow!("write keystrokes: zero-length write to pty"));
        }
        offset += n as usize;
    }
    Ok(())
}

/// Read the pty for `dur`, returning what it emitted (the attach client's
/// TUI frames) so the caller can look for the dialog. Also keeps the pipe
/// from filling.
fn drain_capture(master: RawFd, dur: Duration) -> Vec<u8> {
    let deadline = Instant::now() + dur;
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    while Instant::now() < deadline {
        let mut pfd = libc::pollfd {
            fd: master,
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut pfd, 1, 200) };
        if ready > 0 {
            let n = unsafe { libc::read(master, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if n < 0 {
                // EINTR is a retryable interruption, not end-of-stream; only a
                // real error ends the capture. Treating EINTR as EOF would cut a
                // frame short and could read a false "chrome gone".
                if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                break;
            }
            if n == 0 {
                break; // EOF: the attach client closed the pty.
            }
            out.extend_from_slice(&buf[..n as usize]);
        }
    }
    out
}

/// Nudge the pty window size so the fork's TUI takes a SIGWINCH and redraws the
/// CURRENT screen, then restore it. Two changes so the final size still matches
/// the layout the attach client was given. Best-effort — a failed ioctl just
/// means the following capture may be empty, which the caller treats as "not
/// present".
fn force_repaint(master: RawFd) {
    let nudged = libc::winsize {
        ws_row: 50,
        ws_col: 199,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let normal = libc::winsize {
        ws_row: 50,
        ws_col: 200,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    unsafe {
        libc::ioctl(master, libc::TIOCSWINSZ, &nudged);
        libc::ioctl(master, libc::TIOCSWINSZ, &normal);
    }
}

/// Poll the pty until the selector chrome appears, up to `budget`, returning the
/// ACCUMULATED frames and whether the chrome was seen. The real `claude attach`
/// can take a second or more to connect and paint, so a single short capture
/// races the render; accumulating until the chrome shows tolerates a slow
/// attach. The chrome is scrollback-safe (drawn only while the dialog is live)
/// and a fresh forkpty carries no prior attach's bytes, so this cannot be fooled
/// by history. Returns the instant the chrome appears, so a generous budget
/// never slows the success path — it only bounds the wait for a dialog that will
/// never show. A read of 0 (EOF: attach exited) ends the wait early with
/// whatever was seen; EINTR is retried, not mistaken for EOF.
fn wait_for_chrome(master: RawFd, budget: Duration) -> (Vec<u8>, bool) {
    let deadline = Instant::now() + budget;
    let mut acc = Vec::new();
    let mut buf = [0u8; 8192];
    // Nudge one repaint in case the dialog painted before we began draining.
    force_repaint(master);
    while Instant::now() < deadline {
        let mut pfd = libc::pollfd {
            fd: master,
            events: libc::POLLIN,
            revents: 0,
        };
        if unsafe { libc::poll(&mut pfd, 1, 150) } > 0 {
            let n = unsafe { libc::read(master, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if n < 0 {
                if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                break;
            }
            if n == 0 {
                break; // EOF: the attach client exited.
            }
            acc.extend_from_slice(&buf[..n as usize]);
            if dialog_present(&acc) {
                return (acc, true);
            }
        }
    }
    let present = dialog_present(&acc);
    (acc, present)
}

/// A diagnostic breadcrumb into the daemon log (`daemon.err.log`), so a
/// real-machine injection failure is TRACEABLE instead of silent — the 0.2.11
/// inject path logged nothing, which is why its failure had to be reverse
/// engineered from the DB and a hand-rolled pty capture. It records only a
/// short session id, byte counts, booleans and the outcome — never the option
/// index (which would leak the answer for a two-option allow/deny), the
/// question, or the option text. A no-op in tests to keep their output clean.
#[cfg(not(test))]
fn ilog(msg: String) {
    eprintln!("tinyctb: {msg}");
}
#[cfg(test)]
fn ilog(_msg: String) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_number_is_typed_one_based_then_enter() {
        assert_eq!(option_digits(0), b"1".to_vec());
        assert_eq!(option_digits(2), b"3".to_vec());
        assert_eq!(option_keystrokes(0), b"1\r".to_vec());
        assert_eq!(option_keystrokes(2), b"3\r".to_vec());
    }

    #[test]
    fn the_attach_window_runs_claude_attach_on_the_forks_short_id() {
        // `claude attach` takes the SHORT 8-char id, not the full session UUID.
        let argv = attach_window_argv("c8bac5f4-16c1-4392-8366-57b28b1997b6", "claude");
        assert_eq!(
            argv,
            vec![
                "--title",
                "tinyCTB · 后台任务 c8bac5f4 待答",
                "--",
                "claude",
                "attach",
                "c8bac5f4",
            ]
        );
    }

    #[test]
    fn dialog_presence_is_the_selector_chrome_not_the_option_text() {
        // The selector's own footer hint — present only while the dialog is up.
        let live = b"\x1b[2m Enter to select \xc2\xb7 up/down to navigate \xc2\xb7 Esc \x1b[0m";
        assert!(dialog_present(live));
        // The option label alone is NOT the signal: it also echoes in
        // scrollback, so matching it could not tell a live dialog from history.
        let gone = b"\x1b[2m APPLE  \xe9\xa6\x99\xe8\x95\x89  (the session moved on)\x1b[0m";
        assert!(!dialog_present(gone));
    }

    /// A stub `claude attach` that keeps a background printer redrawing `prints`
    /// (as a live TUI does) UNTIL it is answered, captures the first `keys`
    /// bytes typed at it, then redraws `after` — the fork's NEXT state once the
    /// dialog closed. Pass `after` empty to model attach dying without redrawing
    /// (an EMPTY post-Enter frame, which must NOT count as a successful close).
    /// (A SIGWINCH trap is unreliable while a foreground command blocks, so the
    /// redraw is timer-driven; `force_repaint` is still exercised — it just is
    /// not what makes the chrome appear here.) Raw mode so the CR is not mangled.
    fn live_attach_stub(
        dir: &std::path::Path,
        prints: &str,
        after: &str,
        capture: &std::path::Path,
        keys: usize,
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let stub = dir.join("attach-stub.sh");
        let done = dir.join("answered.flag");
        std::fs::write(
            &stub,
            format!(
                "#!/bin/sh\nstty raw -echo 2>/dev/null\n( while true; do if [ -f '{done}' ]; then printf '%s' '{after}'; else printf '%s' '{prints}'; fi; sleep 0.03; done ) &\nprinter=$!\nhead -c {keys} > '{capture}' 2>/dev/null\n: > '{done}'\nsleep 0.4\nkill \"$printer\" 2>/dev/null\n",
                done = done.display(),
                capture = capture.display()
            ),
        )
        .expect("write stub");
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        stub
    }

    /// A stub `claude attach` that renders NOTHING and exits — attach failed to
    /// bring up a screen at all.
    fn dead_attach_stub(dir: &std::path::Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;
        let stub = dir.join("attach-dead.sh");
        std::fs::write(&stub, "#!/bin/sh\nexit 0\n").expect("write stub");
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        stub
    }

    /// End to end against a stub: the selector chrome appears, and the number for
    /// option index 2 ("3") + Enter are typed into the live dialog — so the
    /// outcome is `Delivered`. The inject side no longer scrapes the screen to
    /// judge whether the fork "took" it (the PostToolUse hook records the fork's
    /// own result); `Delivered` means only "the keys were put in front of a live
    /// dialog", so `after` here is irrelevant.
    #[test]
    fn a_present_dialog_gets_the_option_number() {
        let _guard = crate::state::test_env_lock();
        let dir = std::env::temp_dir().join(format!("tinyctb-inject-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let capture = dir.join("keys.bin");
        let stub = live_attach_stub(&dir, "Enter to select", "continuing…", &capture, 2);
        std::env::set_var("TINYCTB_TEST_ATTACH", &stub);
        let outcome = inject_option_timed(
            "sess-x",
            2,
            true,
            Duration::from_millis(400),
            Duration::from_millis(250),
            Duration::from_millis(250),
            || true,
        )
        .expect("inject");
        std::env::remove_var("TINYCTB_TEST_ATTACH");
        assert_eq!(outcome, InjectOutcome::Delivered);
        let got = std::fs::read(&capture).unwrap_or_default();
        assert!(got.starts_with(&option_digits(2)), "stub received {got:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The dialog is up, but by the time it renders THIS question is no longer
    /// the fork's open one (it was answered and the fork moved on, so the gate
    /// settled the row): `still_authorized()` returns false, so NOTHING is typed
    /// and the outcome is `Unreachable`. This is the question-instance identity
    /// the generic chrome cannot give — it stops a stale button, whose injection
    /// is already in flight, from driving a LATER question's dialog.
    #[test]
    fn a_superseded_question_is_not_driven() {
        let _guard = crate::state::test_env_lock();
        let dir = std::env::temp_dir().join(format!("tinyctb-superseded-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let capture = dir.join("keys.bin");
        // The selector chrome is present the whole time (a live dialog is up)...
        let stub = live_attach_stub(&dir, "Enter to select", "Enter to select", &capture, 1);
        std::env::set_var("TINYCTB_TEST_ATTACH", &stub);
        // ...but the row is no longer authorized (answered/superseded/settled).
        let outcome = inject_option_timed(
            "sess-x",
            0,
            true,
            Duration::from_millis(400),
            Duration::from_millis(200),
            Duration::from_millis(200),
            || false,
        )
        .expect("inject");
        std::env::remove_var("TINYCTB_TEST_ATTACH");
        assert_eq!(outcome, InjectOutcome::Unreachable);
        assert!(
            std::fs::read(&capture).unwrap_or_default().is_empty(),
            "nothing typed once the question is no longer authorized"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A real screen is up but it is NOT the selector (answered at the keyboard,
    /// still connecting, or errored — the pty cannot tell): nothing is typed and
    /// the outcome is `Unreachable`, never a claimed local answer.
    #[test]
    fn a_non_selector_screen_is_unreachable() {
        let _guard = crate::state::test_env_lock();
        let dir = std::env::temp_dir().join(format!("tinyctb-gone-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let capture = dir.join("keys.bin");
        let stub = live_attach_stub(&dir, "the session is busy elsewhere", "gone", &capture, 2);
        std::env::set_var("TINYCTB_TEST_ATTACH", &stub);
        let outcome = inject_option_timed(
            "sess-x",
            1,
            true,
            Duration::from_millis(400),
            Duration::from_millis(200),
            Duration::from_millis(200),
            || true,
        )
        .expect("inject");
        std::env::remove_var("TINYCTB_TEST_ATTACH");
        assert_eq!(outcome, InjectOutcome::Unreachable);
        assert!(
            std::fs::read(&capture).unwrap_or_default().is_empty(),
            "no keystrokes to a non-selector screen"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An attach that never renders a screen is `Unreachable` — a connection
    /// failure must not be reported as a person answering locally.
    #[test]
    fn an_attach_that_never_renders_is_unreachable() {
        let _guard = crate::state::test_env_lock();
        let dir = std::env::temp_dir().join(format!("tinyctb-dead-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let stub = dead_attach_stub(&dir);
        std::env::set_var("TINYCTB_TEST_ATTACH", &stub);
        let outcome = inject_option_timed(
            "sess-x",
            0,
            true,
            Duration::from_millis(400),
            Duration::from_millis(100),
            Duration::from_millis(100),
            || true,
        )
        .expect("inject");
        std::env::remove_var("TINYCTB_TEST_ATTACH");
        assert_eq!(outcome, InjectOutcome::Unreachable);
        std::fs::remove_dir_all(&dir).ok();
    }
}
