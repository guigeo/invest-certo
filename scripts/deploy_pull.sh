#!/usr/bin/env bash

set -e
set -o pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

export UV_CACHE_DIR="${UV_CACHE_DIR:-$PROJECT_DIR/.uv-cache}"

UV="${UV:-uv}"
DASHBOARD_SERVICE="${DASHBOARD_SERVICE:-invest-certo-dashboard.service}"
PIPELINE_SERVICE="${PIPELINE_SERVICE:-invest-certo-pipeline.service}"
WITH_DEV=0
RUN_TESTS=0
RUN_PIPELINE=0

usage() {
  cat <<'EOF'
Usage: scripts/deploy_pull.sh [options]

Options:
  --with-dev       Sync development dependencies too.
  --test           Run pytest after syncing dependencies.
  --run-pipeline   Trigger the pipeline service after deployment.
  -h, --help       Show this help.

Environment:
  UV                         uv executable name/path. Default: uv
  UV_CACHE_DIR               uv cache path. Default: <repo>/.uv-cache
  DASHBOARD_SERVICE          systemd dashboard service name.
  PIPELINE_SERVICE           systemd pipeline service name.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --with-dev)
      WITH_DEV=1
      ;;
    --test)
      RUN_TESTS=1
      WITH_DEV=1
      ;;
    --run-pipeline)
      RUN_PIPELINE=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

systemctl_cmd() {
  if [ "${EUID:-$(id -u)}" -eq 0 ]; then
    systemctl "$@"
  else
    sudo systemctl "$@"
  fi
}

echo "Deploy directory: $PROJECT_DIR"

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Local code changes detected. Commit or discard them before deploying." >&2
  exit 1
fi

echo "Pulling latest code..."
git pull --ff-only

echo "Syncing dependencies..."
if [ "$WITH_DEV" -eq 1 ]; then
  "$UV" sync --extra dev
else
  "$UV" sync
fi

if [ "$RUN_TESTS" -eq 1 ]; then
  echo "Running tests..."
  "$UV" run python -m pytest
fi

echo "Restarting dashboard service..."
systemctl_cmd restart "$DASHBOARD_SERVICE"

if [ "$RUN_PIPELINE" -eq 1 ]; then
  echo "Starting pipeline service..."
  systemctl_cmd start "$PIPELINE_SERVICE"
fi

echo "Deployment completed."
