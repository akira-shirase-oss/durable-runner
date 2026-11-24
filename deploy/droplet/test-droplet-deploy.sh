#!/usr/bin/env bash
# direct/droplet-deploy.sh end to end, through the real upgrade.sh, rollback.sh, format guard, lab
# guard and droplet smoke, as root in a throwaway Ubuntu 24.04 container (the droplet's release):
# testdata/deploy-scenario.sh has the steps. This checkout's deploy/ is mounted read-only; nothing
# outside the container is written. Skipped, and said so, when Docker isn't running.
#
# The test image (Ubuntu 24.04 + curl, git, python3, iproute2) is built once and reused:
# durable-runner-deploy-test:24.04-1.
#
#   deploy/droplet/test-droplet-deploy.sh
set -euo pipefail
deploy_dir=$(cd "$(dirname "$0")/.." && pwd)
IMAGE=durable-runner-deploy-test:24.04-1
if ! docker info >/dev/null 2>&1; then
  echo "  SKIP  Docker isn't running: droplet-deploy.sh was not exercised"
  exit 0
fi
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "building $IMAGE (once)"
  docker build -q -t "$IMAGE" - >/dev/null <<'EOF'
FROM ubuntu:24.04
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
      curl git python3 iproute2 ca-certificates >/dev/null && rm -rf /var/lib/apt/lists/*
EOF
fi
docker run --rm -v "$deploy_dir:/kit/deploy:ro" "$IMAGE" bash /kit/deploy/droplet/testdata/deploy-scenario.sh
