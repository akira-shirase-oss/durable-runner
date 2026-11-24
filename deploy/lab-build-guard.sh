#!/usr/bin/env bash
# lab-build-guard.sh BINARY — exit 0 only for a production durable-server.
# A lab build (`cargo build --features lab`, which run-workspaces.sh and
# the load workflow make) lists --allow-local-tenant-handlers in --help;
# that flag turns the tenant SSRF guard off. It shares target/release
# with the production build, so install.sh, upgrade.sh and smoke.sh ask
# this script before trusting a binary (S3 re-review 3). A binary whose
# --help fails or is not durable-server's is refused too: unknown is no.
#   deploy/lab-build-guard.sh target/release/durable-server
set -uo pipefail
bin=${1:?usage: lab-build-guard.sh /path/to/durable-server}
if ! help=$("$bin" --help 2>&1); then
  echo "refusing $bin: --help failed, so it cannot be told apart from a lab build" >&2
  exit 1
fi
if grep -q -- '--allow-local-tenant-handlers' <<<"$help"; then
  echo "refusing $bin: a LAB build (--allow-local-tenant-handlers) — rebuild with a plain cargo build --release" >&2
  exit 1
fi
if ! grep -q -- '--journal' <<<"$help"; then
  echo "refusing $bin: its --help is not durable-server's" >&2
  exit 1
fi
