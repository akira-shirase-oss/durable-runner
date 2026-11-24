#!/usr/bin/env bash
# droplet-build.sh <commit> — on the droplet, as root: a release build of durable-server and
# durable-cli at exactly that commit, in /opt/durable-build. direct/deploy.sh uploads this file
# from the commit itself and starts it detached (systemd-run), so a dropped SSH can't stop it.
#
# - The commit must be on GitHub's main (checked here too, not only on the deploying machine).
# - Incremental: the checkout (/opt/durable-build/src) and target dir are kept between builds, so
#   only what changed recompiles. nice 15 / idle I/O, and CARGO_BUILD_JOBS (2): the live servers
#   keep their CPU.
# - 2 GB can't hold two Rust builds: it waits for any other cargo on the host (Colophon's direct
#   deploy builds here too) and holds /opt/durable-build/.lock against a second of its own.
# - Never a lab build: no features, RUSTFLAGS cleared, and lab-build-guard.sh on the result.
# - On success it records "<commit> <sha256 server> <sha256 cli>" in /opt/durable-build/built;
#   droplet-deploy.sh installs only binaries matching that record.
# The build cache is capped by droplet-deploy.sh after a successful switch.
set -euo pipefail
die() { echo "droplet-build: error: $*" >&2; exit 1; }
commit=${1:-}
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || die "usage: droplet-build.sh <full 40-character commit id>"
BUILD=${DURABLE_BUILD_DIR:-/opt/durable-build}
REPO_URL=${DURABLE_REPO_URL:-https://github.com/akira-shirase-oss/durable-runner.git}
JOBS=${DURABLE_BUILD_JOBS:-2}
WAIT=${DURABLE_BUILD_WAIT_SECS:-1200}
MIN_FREE_GB=${DURABLE_BUILD_MIN_FREE_GB:-3}
for v in JOBS WAIT MIN_FREE_GB; do [[ "${!v}" =~ ^[0-9]+$ ]] || die "$v must be a whole number"; done
(( JOBS >= 1 )) || die "DURABLE_BUILD_JOBS must be at least 1"
export HOME=${HOME:-/root}
export CARGO_HOME=${CARGO_HOME:-$HOME/.cargo} RUSTUP_HOME=${RUSTUP_HOME:-$HOME/.rustup}
export PATH=$CARGO_HOME/bin:$PATH CARGO_TARGET_DIR=$BUILD/target CARGO_BUILD_JOBS=$JOBS
unset RUSTFLAGS CARGO_BUILD_RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER
start=$SECONDS

mkdir -p "$BUILD"
exec 9>"$BUILD/.lock"
flock -n 9 || die "another Durable Runner build or deploy holds $BUILD/.lock"
rm -f "$BUILD/built"

deadline=$((SECONDS + WAIT))
while pgrep -x cargo >/dev/null 2>&1; do
  (( SECONDS < deadline )) || die "another cargo build has run for over $WAIT s on this host; not starting a second (2 GB of memory)"
  echo "waiting for another cargo build on this host to finish ($(pgrep -x cargo | tr '\n' ' '))"
  sleep 15
done
free_kb=$(df -Pk "$BUILD" | awk 'NR==2 {print $4}')
[[ "$free_kb" =~ ^[0-9]+$ ]] || die "can't read the free space under $BUILD"
(( free_kb >= MIN_FREE_GB * 1024 * 1024 )) || die "only $((free_kb / 1024)) MB free under $BUILD (want ${MIN_FREE_GB} GB): clear old build dirs first"
command -v rustup >/dev/null || die "no rustup in $CARGO_HOME/bin (Colophon's direct deploy installs it; or https://rustup.rs as root)"

src=$BUILD/src
if [[ ! -d "$src/.git" ]]; then
  git clone -q --filter=blob:none --no-checkout "$REPO_URL" "$src"
fi
git -C "$src" fetch -q origin "+refs/heads/main:refs/remotes/origin/main"
git -C "$src" cat-file -e "$commit^{commit}" 2>/dev/null || git -C "$src" fetch -q origin "$commit" \
  || die "$commit isn't on GitHub"
git -C "$src" merge-base --is-ancestor "$commit" origin/main || die "$commit isn't on GitHub's main: refusing"
git -C "$src" -c advice.detachedHead=false checkout -q -f --detach "$commit"
git -C "$src" clean -q -ffdx
[[ "$(git -C "$src" rev-parse HEAD)" == "$commit" ]] || die "the checkout isn't at $commit"

channel=$(sed -n 's/^channel *= *"\([^"]*\)".*/\1/p' "$src/rust-toolchain.toml" | head -1)
[[ "$channel" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || die "rust-toolchain.toml's channel '$channel' isn't a pinned version"
if ! rustup toolchain list | grep -q "^$channel-"; then
  echo "installing Rust $channel (once per toolchain bump)"
  rustup toolchain install "$channel" --profile minimal --component clippy,rustfmt >/dev/null
fi

echo "building $commit with Rust $channel ($JOBS jobs)"
cd "$src"
io=(); command -v ionice >/dev/null && io=(ionice -c 3)
if ! "${io[@]+"${io[@]}"}" nice -n 15 cargo +"$channel" build --release --locked -p durable-server -p durable-cli >"$BUILD/cargo.log" 2>&1; then
  tail -n 40 "$BUILD/cargo.log" >&2
  die "cargo build failed (the whole log: $BUILD/cargo.log)"
fi
tail -n 3 "$BUILD/cargo.log"
rel=$CARGO_TARGET_DIR/release
[[ -x "$rel/durable-server" && -x "$rel/durable-cli" ]] || die "the build left no durable-server/durable-cli in $rel"
"$src/deploy/lab-build-guard.sh" "$rel/durable-server" || die "the build is a LAB build: refusing to record it"
sum() { sha256sum "$1" | cut -d' ' -f1; }
printf '%s %s %s\n' "$commit" "$(sum "$rel/durable-server")" "$(sum "$rel/durable-cli")" >"$BUILD/built.tmp"
mv -f "$BUILD/built.tmp" "$BUILD/built"
echo "built in $((SECONDS - start)) s"
