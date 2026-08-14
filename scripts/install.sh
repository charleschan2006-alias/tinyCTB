#!/usr/bin/env bash
# tinyCTB installer: downloads the latest prebuilt release binary — no Rust
# toolchain, no source checkout. Usage:
#
#   curl -fsSL https://raw.githubusercontent.com/charleschan2006-alias/tinyCTB/main/scripts/install.sh | bash
#
# Installs to ~/.local/bin/tinyctb (override with TINYCTB_INSTALL_DIR).
# The binary is a static musl build: it runs on any Linux x86_64, no
# system libraries required.
set -euo pipefail

REPO="charleschan2006-alias/tinyCTB"
INSTALL_DIR="${TINYCTB_INSTALL_DIR:-$HOME/.local/bin}"

fail() {
    echo "install.sh: $*" >&2
    exit 1
}

[ "$(uname -s)" = "Linux" ] || fail "prebuilt binaries are Linux-only (hooks and live-session \
delivery depend on /proc and unix sockets). On other systems build from source: cargo install --git https://github.com/$REPO"
[ "$(uname -m)" = "x86_64" ] || fail "prebuilt binaries are x86_64-only for now. \
On $(uname -m) build from source: cargo install --git https://github.com/$REPO"
command -v curl >/dev/null || fail "curl is required"

# Resolve the latest tag from the release redirect — no API token, no jq.
latest_url=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest") ||
    fail "could not reach github.com"
tag="${latest_url##*/}"
case "$tag" in
v*) ;;
*) fail "no release found yet at https://github.com/$REPO/releases — build from source: cargo install --git https://github.com/$REPO" ;;
esac

name="tinyctb-$tag-x86_64-linux-musl"
base="https://github.com/$REPO/releases/download/$tag"
tmp=$(mktemp -d)
staged=""
# `rm -f --` for the staged file, never -r: in a shared or attacker-writable
# install dir a predictable name swapped for a directory must not become a
# recursive delete. mktemp's random name closes the prediction hole itself.
trap 'rm -rf "$tmp"; [ -n "$staged" ] && rm -f -- "$staged"' EXIT

echo "Downloading tinyctb $tag ..."
curl -fsSL -o "$tmp/$name.tar.gz" "$base/$name.tar.gz" ||
    fail "download failed: $base/$name.tar.gz"
curl -fsSL -o "$tmp/$name.tar.gz.sha256" "$base/$name.tar.gz.sha256" ||
    fail "checksum download failed"
(cd "$tmp" && sha256sum -c "$name.tar.gz.sha256" >/dev/null) ||
    fail "checksum verification FAILED — refusing to install"

tar xzf "$tmp/$name.tar.gz" -C "$tmp"
mkdir -p "$INSTALL_DIR"
# Stage in the TARGET directory, then rename: mv within one filesystem is
# atomic, so an interrupted install can never leave a truncated binary at
# the final path (a staging file in /tmp could cross filesystems). mktemp
# gives the stage an unpredictable name; -T refuses to travel INTO a
# directory if someone planted one (or a symlink to one) at the final path.
staged=$(mktemp "$INSTALL_DIR/.tinyctb.install.XXXXXXXX") ||
    fail "cannot create staging file in $INSTALL_DIR"
install -m 755 "$tmp/$name/tinyctb" "$staged"
# Prove the binary RUNS before it takes the final path — inside an echo's
# command substitution a crash would vanish into echo's exit code.
version=$("$staged" --version) || fail "downloaded binary failed to execute"
mv -fT -- "$staged" "$INSTALL_DIR/tinyctb"
staged=""

echo "Installed: $INSTALL_DIR/tinyctb ($version)"
case ":$PATH:" in
*":$INSTALL_DIR:"*) ;;
*)
    echo
    echo "NOTE: $INSTALL_DIR is not on your PATH. Add to your shell profile:"
    echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
    ;;
esac
echo
echo "Next: create a Telegram bot with @BotFather, then run"
echo "  tinyctb setup --bot-token <telegram-bot-token>"
echo "which pairs the chat, installs the Claude Code hooks, and starts the daemon."
