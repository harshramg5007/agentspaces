#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FORBIDDEN_PATHS_FILE="$ROOT_DIR/scripts/public_forbidden_paths.txt"

if ! command -v rg >/dev/null 2>&1; then
  echo "error: rg is required" >&2
  exit 1
fi

missing=0

if [[ ! -f "$ROOT_DIR/.github/CODEOWNERS" ]]; then
  echo "error: .github/CODEOWNERS is required" >&2
  missing=1
fi

if [[ -e "$ROOT_DIR/CODEOWNERS" ]]; then
  echo "error: root CODEOWNERS must not be present" >&2
  missing=1
fi

while IFS= read -r rel; do
  [[ -z "$rel" ]] && continue
  if [[ -e "$ROOT_DIR/$rel" ]]; then
    echo "error: forbidden public path present: $rel" >&2
    missing=1
  fi
done <"$FORBIDDEN_PATHS_FILE"

if [[ "$missing" -ne 0 ]]; then
  exit 1
fi

cd "$ROOT_DIR"

TARGETS=(
  README.md
  CHANGELOG.md
  CONTRIBUTING.md
  SECURITY.md
  COMMERCIAL-LICENSING.md
  api
  docs
  examples
  .github
  Makefile
  docker-compose.yml
  Dockerfile
  Dockerfile.python
  sdk/python
)

common_excludes=(
  --glob
  !LICENSE
  --glob
  !sdk/python/LICENSE
)

if rg -n "${common_excludes[@]}" '\bMIT\b|\bAGPL\b|opensource\.org/licenses/MIT|Tuple Spaces|support@agentspace\.io|api\.agentspace\.io|experimental/|benchmarks/' "${TARGETS[@]}"; then
  echo "error: forbidden public strings found" >&2
  exit 1
fi

if rg -n "${common_excludes[@]}" 'open source' "${TARGETS[@]}"; then
  echo "error: 'open source' wording is not allowed outside BUSL license files" >&2
  exit 1
fi

echo "Public surface check passed"
