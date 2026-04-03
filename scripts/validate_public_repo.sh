#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
DIST_DIR="$(mktemp -d)"
trap 'rm -rf "$DIST_DIR"' EXIT

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "error: required command not found: $name" >&2
    exit 1
  fi
}

require_cmd git
require_cmd go
require_cmd rg
require_cmd "$PYTHON_BIN"

"$ROOT_DIR/scripts/check_public_surface.sh"

pick_free_port() {
  "$PYTHON_BIN" - <<'PY'
import socket

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

(
  cd "$ROOT_DIR"
  go mod tidy
  git diff --exit-code go.mod go.sum
  go build ./...
  make test
  "$PYTHON_BIN" -m pip install --upgrade pip build
  "$PYTHON_BIN" -m pip install -e "sdk/python[async,dev]"
  "$PYTHON_BIN" -m build sdk/python --sdist --wheel --outdir "$DIST_DIR"
  make docs-lint
  make docs-check
)

DIST_DIR="$DIST_DIR" "$PYTHON_BIN" - <<'PY'
from pathlib import Path
import os
import sys
import tarfile
import zipfile

dist = Path(os.environ["DIST_DIR"])
whls = sorted(dist.glob("*.whl"))
sdists = sorted(dist.glob("*.tar.gz"))

if not whls or not sdists:
    raise SystemExit("missing built wheel or sdist")

wheel_has_license = False
with zipfile.ZipFile(whls[0]) as zf:
    wheel_has_license = any(name.endswith("LICENSE") or "/licenses/LICENSE" in name for name in zf.namelist())

sdist_has_license = False
with tarfile.open(sdists[0], "r:gz") as tf:
    sdist_has_license = any(name.endswith("/LICENSE") for name in tf.getnames())

if not wheel_has_license:
    raise SystemExit("wheel is missing LICENSE")
if not sdist_has_license:
    raise SystemExit("sdist is missing LICENSE")
PY

if [[ "${SKIP_DOCKER:-0}" == "1" ]]; then
  echo "Skipping Docker-backed validation because SKIP_DOCKER=1"
else
  (
    cd "$ROOT_DIR"
    cleanup() {
      make down >/dev/null 2>&1 || true
      docker compose -f examples/fault-tolerance/docker-compose.yml down -v --remove-orphans >/dev/null 2>&1 || true
    }
    trap cleanup EXIT
    export POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-$(pick_free_port)}"
    export AGENT_SPACE_PORT="${AGENT_SPACE_PORT:-$(pick_free_port)}"
    make doctor
    make up
    make smoke
    make down
    make example-fault-tolerance
  )
fi

echo "Public repo validation passed"
