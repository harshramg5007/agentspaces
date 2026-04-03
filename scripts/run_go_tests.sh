#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO_BIN:-go}"
GENERATED_LINKS=()

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "error: required command not found: $name" >&2
    exit 1
  fi
}

relative_path() {
  python3 - "$1" "$2" <<'PY'
import os
import sys

print(os.path.relpath(sys.argv[2], sys.argv[1]))
PY
}

cleanup() {
  local idx path
  for ((idx=${#GENERATED_LINKS[@]} - 1; idx >= 0; idx--)); do
    path="${GENERATED_LINKS[idx]}"
    if [[ -L "$path" ]]; then
      rm -f "$path"
    fi
  done
}

trap cleanup EXIT
trap 'exit 1' INT TERM

require_cmd "$GO_BIN"
require_cmd find
require_cmd python3

materialize_test_links() {
  local source target target_dir expected existing
  local count=0

  while IFS= read -r source; do
    ((count += 1))
    target="$ROOT_DIR/${source#tests/go/}"
    target_dir="$(dirname "$target")"

    mkdir -p "$target_dir"

    expected="$(relative_path "$target_dir" "$ROOT_DIR/$source")"

    if [[ -L "$target" ]]; then
      existing="$(readlink "$target")"
      if [[ "$existing" != "$expected" ]]; then
        echo "error: refusing to reuse unexpected symlink at ${target#$ROOT_DIR/}" >&2
        exit 1
      fi
      GENERATED_LINKS+=("$target")
      continue
    fi

    if [[ -e "$target" ]]; then
      echo "error: refusing to overwrite existing file at ${target#$ROOT_DIR/}" >&2
      exit 1
    fi

    ln -s "$expected" "$target"
    GENERATED_LINKS+=("$target")
  done < <(cd "$ROOT_DIR" && find tests/go -type f -name '*_test.go' | sort)

  if [[ "$count" -eq 0 ]]; then
    echo "error: no Go tests found under tests/go" >&2
    exit 1
  fi
}

cd "$ROOT_DIR"
materialize_test_links

if [[ "$#" -eq 0 ]]; then
  set -- ./...
fi

"$GO_BIN" test -v -race "$@"
