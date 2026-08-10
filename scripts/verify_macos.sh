#!/usr/bin/env bash
#
# tinyCTB — macOS LaunchAgent service-layer verification (A段)
#
# Verifies the launchd integration end to end WITHOUT needing Claude Code
# installed or logged in: build, plist generation, plutil lint, and the full
# bootstrap → status → reload → stop → uninstall lifecycle.
#
# It uses a dedicated label (tinyctb-verify) and a throwaway fake config so it
# never touches a real `tinyctb` service or your real Telegram setup, and it
# restores/cleans everything on exit.
#
# Run on the Mac from the repo:  bash scripts/verify_macos.sh
#
set -u

LABEL="tinyctb-verify"
PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
# A dedicated throwaway state dir: the plist embeds TINYCTB_STATE_DIR, so the
# verify daemon reads/writes only here and the user's real ~/.tinyctb (config,
# state.db, hook spool, logs) is never touched.
STATE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tinyctb-verify-state.XXXXXX")"
export TINYCTB_STATE_DIR="$STATE_DIR"
CONFIG="$STATE_DIR/config.json"

pass=0
fail=0
ok()   { printf '  \033[32m✅ %s\033[0m\n' "$1"; pass=$((pass + 1)); }
bad()  { printf '  \033[31m❌ %s\033[0m\n' "$1"; fail=$((fail + 1)); }
info() { printf '  · %s\n' "$1"; }
step() { printf '\n▶ %s\n' "$1"; }

# --- platform + location guards -------------------------------------------
if [ "$(uname)" != "Darwin" ]; then
    echo "This script verifies the macOS LaunchAgent path and must run on macOS." >&2
    exit 2
fi
cd "$(dirname "$0")/.." || { echo "cannot cd to repo root" >&2; exit 2; }
if [ ! -f Cargo.toml ]; then
    echo "run this from the tinyCTB repo (Cargo.toml not found)" >&2
    exit 2
fi
BIN="./target/release/tinyctb"

# Refuse to run if a real tinyctb agent is already loaded — its shared daemon
# lock would block our verify daemon and give a false failure.
if launchctl print "gui/$(id -u)/tinyctb" >/dev/null 2>&1; then
    echo "A real 'tinyctb' LaunchAgent is already loaded." >&2
    echo "Stop it first (tinyctb daemon stop) so the verification can run cleanly." >&2
    exit 2
fi

# --- cleanup ----------------------------------------------------------------
cleanup() {
    step "Cleanup"
    "$BIN" daemon uninstall --label "$LABEL" >/dev/null 2>&1
    launchctl bootout "gui/$(id -u)/$LABEL" >/dev/null 2>&1
    rm -f "$PLIST"
    rm -rf "$STATE_DIR"
    info "removed the throwaway verify state dir"
    info "done"
}
trap cleanup EXIT

# A throwaway config so `daemon run` stays alive (bogus token → Telegram 401,
# which the daemon cycle handles gracefully without exiting).
cat > "$CONFIG" <<'JSON'
{"version":1,"bridgeCommand":"tinyctb","events":"thread_waiting,thread_completed","telegram":{"botToken":"0:verify","chatId":"0"},"claude":{"permissionMode":"bypassPermissions","sessionScanLimit":10},"projects":[]}
JSON
chmod 600 "$CONFIG"

echo "tinyCTB macOS LaunchAgent verification (service layer, no Claude login needed)"
echo "label=$LABEL  plist=$PLIST"

# --- 1. build --------------------------------------------------------------
step "1. cargo build --release (also confirms it compiles on macOS)"
if cargo build --release >/tmp/tinyctb-verify-build.log 2>&1; then
    ok "release build succeeded"
else
    bad "release build FAILED — see /tmp/tinyctb-verify-build.log"
    tail -20 /tmp/tinyctb-verify-build.log
    exit 1
fi

# --- 2. dry-run install shows the generated plist --------------------------
step "2. daemon install --dry-run (inspect plist without writing)"
if "$BIN" daemon install --label "$LABEL" --dry-run >/dev/null 2>&1; then
    ok "dry-run install returned ok"
else
    bad "dry-run install failed"
fi

# --- 3. real install + plutil lint -----------------------------------------
step "3. daemon install + plutil -lint"
if "$BIN" daemon install --label "$LABEL" >/dev/null 2>&1 && [ -f "$PLIST" ]; then
    ok "plist written to $PLIST"
else
    bad "plist was not written"
fi
if plutil -lint "$PLIST" 2>/dev/null | grep -q "OK"; then
    ok "plutil -lint: plist is valid"
else
    bad "plutil -lint reported the plist invalid"
    plutil -lint "$PLIST"
fi
if grep -q "<key>RunAtLoad</key>" "$PLIST" && grep -q "<key>KeepAlive</key>" "$PLIST"; then
    ok "plist declares RunAtLoad + KeepAlive"
else
    bad "plist missing RunAtLoad/KeepAlive"
fi
# Load-bearing isolation check: the verify daemon must be pinned to the
# throwaway state dir, otherwise it would consume the real hook spool/db.
if grep -q "<key>TINYCTB_STATE_DIR</key>" "$PLIST" && grep -qF "$STATE_DIR" "$PLIST"; then
    ok "plist pins TINYCTB_STATE_DIR to the throwaway dir"
else
    bad "plist does not pin TINYCTB_STATE_DIR — verify daemon would touch real state"
fi

# --- 4. start + verify running ---------------------------------------------
step "4. daemon start (launchctl bootout→bootstrap) + verify running"
if "$BIN" daemon start --label "$LABEL" >/dev/null 2>&1; then
    ok "daemon start reached running state (verified within grace window)"
else
    bad "daemon start did not reach running — check $STATE_DIR/logs/daemon.err.log"
fi
if "$BIN" daemon status --label "$LABEL" 2>/dev/null | grep -q '"running":true'; then
    ok "daemon status reports running"
else
    bad "daemon status does not report running"
fi

# --- 5. reload (the P1-2 fix: an already-loaded agent must re-register) -----
step "5. reload: install + start again while already loaded"
"$BIN" daemon install --label "$LABEL" >/dev/null 2>&1
if "$BIN" daemon start --label "$LABEL" >/dev/null 2>&1 &&
    "$BIN" daemon status --label "$LABEL" 2>/dev/null | grep -q '"running":true'; then
    ok "second start on an already-loaded agent still reaches running (bootout→bootstrap works)"
else
    bad "reload failed — an already-loaded agent did not re-register"
fi

# --- 6. stop ---------------------------------------------------------------
step "6. daemon stop"
"$BIN" daemon stop --label "$LABEL" >/dev/null 2>&1
sleep 1
if "$BIN" daemon status --label "$LABEL" 2>/dev/null | grep -q '"running":true'; then
    bad "daemon still running after stop"
else
    ok "daemon stopped"
fi

# --- 7. uninstall removes the agent ----------------------------------------
step "7. daemon uninstall + confirm the agent is gone"
"$BIN" daemon uninstall --label "$LABEL" >/dev/null 2>&1
if [ ! -f "$PLIST" ] && ! launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1; then
    ok "plist removed and agent booted out"
else
    bad "uninstall left the plist or a loaded agent behind"
fi

# --- verdict ---------------------------------------------------------------
printf '\n────────────────────────────────────────\n'
printf 'PASS: %d   FAIL: %d\n' "$pass" "$fail"
if [ "$fail" -eq 0 ]; then
    printf '\033[32mService-layer verification PASSED.\033[0m\n'
    echo "Still manual: log out & back in to confirm RunAtLoad restarts the agent,"
    echo "then B段 (full hook→Telegram→reply loop) needs Claude Code installed + logged in."
    exit 0
else
    printf '\033[31mVerification FAILED — see the ❌ lines above.\033[0m\n'
    exit 1
fi
