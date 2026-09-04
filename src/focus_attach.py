#!/usr/bin/env python3
"""Pop a terminal window for the fork's native dialog AND pull it to the
foreground with keyboard focus.

Invoked by the tinyCTB daemon as:

    python3 focus_attach.py <terminal-bin> <arg> <arg> ...

i.e. argv[1:] is the exact command to launch (e.g. `gnome-terminal --title …
-- <claude> attach <short>`). This helper — not the daemon — spawns it, so the
`_NET_CLIENT_LIST` snapshot is taken strictly BEFORE the launch and the new
window can be identified by diffing the list afterwards. gnome-terminal is a
client/server: the visible window is owned by gnome-terminal-server, so its
title and `_NET_WM_PID` are unreliable for matching — the before/after diff is
not, and is terminal-agnostic.

Focus is requested with `_NET_ACTIVE_WINDOW` **source indication 2 (pager)**.
EWMH defines 2 as "pager/direct user action"; Mutter normally brings such a
window to the foreground even past its focus-stealing prevention, though the
spec explicitly permits a WM to refuse, so this is a best effort, not a hard
guarantee. (Upstream `wmctrl -a` actually sends source=0 then `XMapRaised`;
source=2 here is the equivalent direct-focus request.) The message layout was
verified on-machine 2026-09-04 (python-xlib 0.33, X11/Mutter).

Best-effort by contract: the window MUST appear even if X, python-xlib, or the
activation is unavailable, so the terminal is launched unconditionally and every
X step is guarded. Always exits 0 — focus is a nice-to-have, never load-bearing.
"""
import os
import subprocess
import sys
import time

# The daemon fills these before exec, but default them defensively so a stray
# invocation still reaches this machine's display.
os.environ.setdefault("DISPLAY", ":1")
os.environ.setdefault(
    "XAUTHORITY", "/run/user/%d/gdm/Xauthority" % os.getuid()
)

TERMINAL_ARGV = sys.argv[1:]
# Windows that appear but are NOT the terminal we launched (class-filtered so a
# coincidental unrelated window opening in the same instant is not grabbed).
TERMINAL_WM_CLASS_HINTS = ("gnome-terminal", "terminal")
FIND_WINDOW_BUDGET = 6.0   # seconds to wait for the new window to map
MAP_SETTLE = 0.3           # let it paint before we activate


_SPAWNED = False


def _spawn_terminal():
    global _SPAWNED
    if _SPAWNED or not TERMINAL_ARGV:
        return
    try:
        subprocess.Popen(
            TERMINAL_ARGV,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception:
        # Leave _SPAWNED False so a later call (the __main__ tail) can retry — a
        # transient failure (e.g. EAGAIN under load) then still gets a window.
        # A genuinely unrunnable terminal fails both times, the same no-window
        # outcome as before. Setting the guard only AFTER a successful spawn is
        # what makes this retry possible (Sol review: guard-before-attempt bug).
        return
    _SPAWNED = True


def main():
    # Connect to X first so the snapshot is taken BEFORE the launch. If any of
    # this fails, fall back to a bare launch with no activation.
    try:
        from Xlib import X, display, protocol
    except Exception:
        _spawn_terminal()
        return

    try:
        d = display.Display()
        root = d.screen().root
        atom = d.get_atom

        def client_list():
            p = root.get_full_property(atom("_NET_CLIENT_LIST"), X.AnyPropertyType)
            return set(p.value) if p else set()

        def wm_class(win):
            try:
                w = d.create_resource_object("window", win)
                p = w.get_full_property(atom("WM_CLASS"), X.AnyPropertyType)
                if not p:
                    return ""
                raw = p.value
                text = raw.decode("latin-1") if isinstance(raw, (bytes, bytearray)) else str(raw)
                return text.lower()
            except Exception:
                return ""

        def activate(win):
            ev = protocol.event.ClientMessage(
                window=win,
                client_type=atom("_NET_ACTIVE_WINDOW"),
                # data.l[0]=2 → source is a pager/direct user action, which a WM
                # normally (best-effort, not guaranteed) honours past focus-
                # stealing prevention; l[1]=CurrentTime; rest 0.
                data=(32, [2, X.CurrentTime, 0, 0, 0]),
            )
            root.send_event(
                ev,
                event_mask=X.SubstructureRedirectMask | X.SubstructureNotifyMask,
            )
            d.flush()

        before = client_list()
    except Exception:
        _spawn_terminal()
        return

    _spawn_terminal()
    if not _SPAWNED:
        return  # terminal never launched; the __main__ tail retries — nothing to focus

    # Wait for the new window to join the client list.
    target = None
    deadline = time.time() + FIND_WINDOW_BUDGET
    while time.time() < deadline:
        try:
            new = client_list() - before
        except Exception:
            break
        if new:
            terminals = [w for w in new if any(h in wm_class(w) for h in TERMINAL_WM_CLASS_HINTS)]
            if terminals:
                target = max(terminals)
                break
            if len(new) == 1:
                # No class match but exactly one new window — take it.
                target = next(iter(new))
                break
        time.sleep(0.1)

    if target is None:
        return  # nothing to activate (opened as a tab, or never mapped in time)

    try:
        time.sleep(MAP_SETTLE)
        activate(target)
    except Exception:
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception:
        pass  # never let a helper crash swallow the window
    if not _SPAWNED:
        # Last-ditch: main() never got the terminal up (a crash, or a transient
        # spawn failure that left _SPAWNED False). Try once more — the window is
        # the one thing that MUST appear; focus was already best-effort.
        try:
            _spawn_terminal()
        except Exception:
            pass
    sys.exit(0)
