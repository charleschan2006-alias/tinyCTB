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
pub(crate) fn attach_window_argv(session_id: &str, claude_bin: &str) -> Vec<String> {
    vec![
        "--title".to_string(),
        format!("tinyCTB · 后台任务 {} 待答", short_id(session_id)),
        "--".to_string(),
        claude_bin.to_string(),
        "attach".to_string(),
        session_id.to_string(),
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

/// How long to let `claude attach` connect and the fork's dialog render
/// before injecting, and how long to hold the pty afterwards so the keys
/// are consumed.
const ATTACH_CONNECT_WAIT: Duration = Duration::from_secs(4);
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
/// attach need not burn the real seven seconds.
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
    /// POSITIVE delivery: the selector was live right up to the Enter, and it
    /// then CLOSED — strong evidence the fork consumed the number. Only this
    /// finalises the row on the phone side.
    Injected,
    /// The answer could NOT be positively delivered: the selector was not
    /// found on the current screen, it did not close after the Enter, or the
    /// attach never rendered / failed. Whether the dialog was already answered
    /// at the keyboard, is still connecting, or errored, the phone cannot tell
    /// them apart over a pty — so it claims NOTHING here (no record, no
    /// settle), reports it as retryable, and lets the authoritative
    /// PostToolUse hook close the row when the fork's answer actually lands.
    Unreachable,
}

/// Answer a background fork's native single-select dialog by option index,
/// but — when `verify_present` — ONLY if the selector is still showing, the
/// safety the "谁先抢答" design rests on. A pty child execs `claude attach
/// <id>`; the parent reads the fork's screen, and only if the selector's
/// prompt chrome is present does it type the option's number + Enter. With
/// `verify_present` false it always injects — for the manual command, where
/// the caller vouches the dialog is up.
pub(crate) fn inject_option(
    session_id: &str,
    index: usize,
    verify_present: bool,
) -> Result<InjectOutcome> {
    let (connect, settle) = default_waits();
    inject_option_timed(
        session_id,
        index,
        verify_present,
        connect,
        SELECTOR_KEY_GAP,
        settle,
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
) -> Result<InjectOutcome> {
    with_attach_pty(session_id, |master| {
        // Let attach connect and paint. NO frames at all means the client never
        // rendered — an exec/connect failure. Cannot deliver.
        let intro = drain_capture(master, connect_wait);
        if verify_present && intro.is_empty() {
            return Ok(InjectOutcome::Unreachable);
        }
        if !verify_present {
            // The manual/vouched path: type the number + Enter, no checks.
            write_all(master, &option_digits(index))?;
            drain_capture(master, key_gap);
            write_all(master, b"\r")?;
            drain_capture(master, settle_wait);
            return Ok(InjectOutcome::Injected);
        }
        // Presence on the CURRENT screen: flush queued bytes, force a repaint,
        // and check chrome ONLY in that fresh frame. Absent is AMBIGUOUS — the
        // dialog may be answered at the keyboard, still connecting, or errored,
        // and a pty cannot tell them apart — so claim NOTHING here (report
        // Unreachable, let the PostToolUse hook close the row on the real
        // answer).
        if !dialog_present(&repaint_and_capture(master, key_gap)) {
            return Ok(InjectOutcome::Unreachable);
        }
        // Type the number, but do NOT submit yet.
        write_all(master, &option_digits(index))?;
        // Re-confirm the selector is STILL live before the Enter that submits
        // it: this withholds the only harmful key (Enter) once the dialog is
        // gone, shrinking the check→submit race. Its residual (a deschedule
        // between this check and the write) is a documented best-effort limit
        // of driving a live TUI over a pty.
        if !dialog_present(&repaint_and_capture(master, key_gap)) {
            return Ok(InjectOutcome::Unreachable);
        }
        write_all(master, b"\r")?;
        // Did the fork TAKE the number? Force a repaint and read the current
        // screen. `Injected` requires POSITIVE evidence: a NON-EMPTY frame with
        // the chrome GONE — i.e. the fork redrew its NEXT state after the dialog
        // closed. `claude attach` mirrors the still-running session, so a
        // successful answer leaves the fork WORKING and the attach producing
        // output; an EMPTY frame means the attach produced nothing, i.e. it
        // DIED/crashed — not a success. Chrome still present ⇒ the number was
        // not taken (partial buffer, stuck attach). So EMPTY or CHROME-PRESENT
        // ⇒ Unreachable; only NON-EMPTY-and-CHROME-GONE ⇒ Injected.
        //
        // The two-sided ambiguity Sol raised is resolved by ASYMMETRIC HARM:
        // claiming Injected on an empty frame RECORDS an answer the fork may
        // never have taken — UNRECOVERABLE (the row is finalised, the hook's
        // `answer IS NULL` settle can no longer fix it, the question is silently
        // dropped). Reporting Unreachable on a true-but-invisible success is
        // RECOVERABLE (retryable button; the PostToolUse hook still settles the
        // row when the fork's answer actually lands; /threads re-offers). So we
        // take the recoverable side. (The premise that attach EXITS the instant
        // one question is answered is rejected: it mirrors the running session
        // and only exits when the session ends or the viewer detaches.)
        drain_capture(master, key_gap);
        let after = repaint_and_capture(master, settle_wait);
        if after.is_empty() || dialog_present(&after) {
            return Ok(InjectOutcome::Unreachable);
        }
        Ok(InjectOutcome::Injected)
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
    let arg_id = std::ffi::CString::new(session_id)
        .map_err(|_| anyhow!("session id has an interior NUL"))?;
    let argv: [*const libc::c_char; 4] = [
        prog.as_ptr(),
        arg_attach.as_ptr(),
        arg_id.as_ptr(),
        std::ptr::null(),
    ];

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
        // Child: the pty slave is already our controlling terminal and
        // stdio. Async-signal-safe calls ONLY from here. `execv` (not
        // `execvp`): `prog` is already absolute, so there is no PATH search
        // and no allocation. On failure, die loudly.
        unsafe {
            libc::execv(prog.as_ptr(), argv.as_ptr());
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
            if n <= 0 {
                break;
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

/// Read and DISCARD everything already queued on the pty, without waiting —
/// so a following capture reflects the repaint we are about to force, not
/// bytes that scrolled past before it.
fn flush_pending(master: RawFd) {
    let mut buf = [0u8; 8192];
    loop {
        let mut pfd = libc::pollfd {
            fd: master,
            events: libc::POLLIN,
            revents: 0,
        };
        // Zero timeout: only drain what is IMMEDIATELY available.
        if unsafe { libc::poll(&mut pfd, 1, 0) } <= 0 {
            break;
        }
        let n = unsafe { libc::read(master, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n <= 0 {
            break;
        }
    }
}

/// Flush queued bytes, force a fresh repaint, then return what the fork redrew
/// within `dur` — the CURRENT screen, so a presence check reflects now rather
/// than anything that scrolled past earlier in the stream.
fn repaint_and_capture(master: RawFd, dur: Duration) -> Vec<u8> {
    flush_pending(master);
    force_repaint(master);
    drain_capture(master, dur)
}

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
    fn the_attach_window_runs_claude_attach_on_the_fork() {
        let argv = attach_window_argv("c8bac5f4-16c1-4392-8366-57b28b1997b6", "claude");
        assert_eq!(
            argv,
            vec![
                "--title",
                "tinyCTB · 后台任务 c8bac5f4 待答",
                "--",
                "claude",
                "attach",
                "c8bac5f4-16c1-4392-8366-57b28b1997b6",
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

    /// End to end against a stub: the selector chrome is on the current screen
    /// through both liveness checks, the number for option index 2 ("3") is
    /// typed, and the selector then closes (the stub stops redrawing once
    /// answered) — so the outcome is `Injected`.
    #[test]
    fn a_present_dialog_gets_the_option_number() {
        let _guard = crate::state::test_env_lock();
        let dir = std::env::temp_dir().join(format!("tinyctb-inject-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let capture = dir.join("keys.bin");
        // After the answer the fork redraws its next state ("continuing…") —
        // a non-empty frame WITHOUT the selector chrome, the positive evidence
        // of a real close.
        let stub = live_attach_stub(&dir, "Enter to select", "continuing…", &capture, 2);
        std::env::set_var("TINYCTB_TEST_ATTACH", &stub);
        let outcome = inject_option_timed(
            "sess-x",
            2,
            true,
            Duration::from_millis(400),
            Duration::from_millis(250),
            Duration::from_millis(250),
        )
        .expect("inject");
        std::env::remove_var("TINYCTB_TEST_ATTACH");
        assert_eq!(outcome, InjectOutcome::Injected);
        let got = std::fs::read(&capture).unwrap_or_default();
        assert!(got.starts_with(&option_digits(2)), "stub received {got:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// attach passes both liveness checks, then goes SILENT after the Enter —
    /// an EMPTY post-Enter frame. A live attach mirroring a WORKING fork would
    /// keep producing output, so silence means it DIED before the key was
    /// consumed. It is `Unreachable` (retryable, recoverable), NOT a claimed
    /// success that permanently records an answer the fork never took.
    #[test]
    fn an_empty_frame_after_enter_is_unreachable() {
        let _guard = crate::state::test_env_lock();
        let dir = std::env::temp_dir().join(format!("tinyctb-emptyframe-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let capture = dir.join("keys.bin");
        // Chrome through both checks, then SILENCE after the answer.
        let stub = live_attach_stub(&dir, "Enter to select", "", &capture, 2);
        std::env::set_var("TINYCTB_TEST_ATTACH", &stub);
        let outcome = inject_option_timed(
            "sess-x",
            2,
            true,
            Duration::from_millis(400),
            Duration::from_millis(250),
            Duration::from_millis(250),
        )
        .expect("inject");
        std::env::remove_var("TINYCTB_TEST_ATTACH");
        assert_eq!(outcome, InjectOutcome::Unreachable);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The selector is STILL up after the Enter (the number was not taken — a
    /// stuck attach or a partial buffer): chrome persists, so it is
    /// `Unreachable`, never a claimed success.
    #[test]
    fn a_stuck_selector_after_enter_is_unreachable() {
        let _guard = crate::state::test_env_lock();
        let dir = std::env::temp_dir().join(format!("tinyctb-stuck-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let capture = dir.join("keys.bin");
        // Chrome BEFORE and AFTER the answer — the dialog never closes.
        let chrome = "Enter to select";
        let stub = live_attach_stub(&dir, chrome, chrome, &capture, 2);
        std::env::set_var("TINYCTB_TEST_ATTACH", &stub);
        let outcome = inject_option_timed(
            "sess-x",
            2,
            true,
            Duration::from_millis(400),
            Duration::from_millis(250),
            Duration::from_millis(250),
        )
        .expect("inject");
        std::env::remove_var("TINYCTB_TEST_ATTACH");
        assert_eq!(outcome, InjectOutcome::Unreachable);
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
        )
        .expect("inject");
        std::env::remove_var("TINYCTB_TEST_ATTACH");
        assert_eq!(outcome, InjectOutcome::Unreachable);
        std::fs::remove_dir_all(&dir).ok();
    }
}
