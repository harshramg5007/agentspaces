#!/usr/bin/env python3
"""Sanity-check repo-relative paths used in fenced shell blocks in markdown files."""

import pathlib
import re
import shlex
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def git_tracked_markdown() -> list[pathlib.Path]:
    out = subprocess.check_output(["git", "ls-files", "*.md"], text=True, cwd=ROOT)
    files = []
    for rel in out.splitlines():
        p = ROOT / rel
        if p.exists():
            files.append(p)
    return files


CHECKABLE_PREFIXES = (
    "./scripts/",
    "./examples/",
    "./docs/",
    "./api/",
    "./cmd/",
    "./pkg/",
    "./sdk/",
    "scripts/",
    "examples/",
    "docs/",
    "api/",
    "cmd/",
    "pkg/",
    "sdk/",
)


def should_check_token(token: str) -> bool:
    if token.startswith(("http://", "https://")):
        return False
    if any(ch in token for ch in "*<>"):
        return False
    if token.startswith("$"):
        return False
    if token.endswith("/...") or token == "./..." or token == "../...":
        return False
    if token.endswith("/"):
        return False

    if token.startswith(CHECKABLE_PREFIXES):
        return True

    if token.startswith(("./", "../")):
        return False
    return False


def resolve_token(cwd: pathlib.Path, token: str) -> pathlib.Path:
    if token.startswith(("./", "../")):
        return (cwd / token).resolve()
    return (ROOT / token).resolve()


def main() -> int:
    files = git_tracked_markdown()
    errors: list[str] = []

    for path in files:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        in_code = False
        shell_block = False
        cwd = ROOT

        for lineno, raw in enumerate(lines, start=1):
            line = raw.strip()

            if line.startswith("```"):
                if not in_code:
                    in_code = True
                    lang = line[3:].strip().lower()
                    shell_block = lang in ("", "bash", "sh", "shell", "zsh")
                    cwd = ROOT
                else:
                    in_code = False
                    shell_block = False
                continue

            if not in_code or not shell_block:
                continue
            if not line or line.startswith("#"):
                continue

            if line.startswith("$ "):
                line = line[2:].strip()

            try:
                parts = shlex.split(line)
            except ValueError:
                continue
            if not parts:
                continue

            if parts[0] == "cd" and len(parts) >= 2:
                target = parts[1]
                if target.startswith("/"):
                    continue
                next_cwd = (cwd / target).resolve()
                try:
                    next_cwd.relative_to(ROOT)
                except ValueError:
                    continue
                if next_cwd.exists() and next_cwd.is_dir():
                    cwd = next_cwd
                else:
                    errors.append(
                        f"{path.relative_to(ROOT)}:{lineno}: missing cd target '{target}'"
                    )
                continue

            tokens: list[str] = []
            for tok in parts:
                if should_check_token(tok):
                    tokens.append(tok)

            # go run ./path
            if parts[0] == "go" and len(parts) >= 3 and parts[1] == "run":
                run_target = parts[2]
                if should_check_token(run_target):
                    tokens.append(run_target)

            for tok in tokens:
                resolved = resolve_token(cwd, tok)
                try:
                    resolved.relative_to(ROOT)
                except ValueError:
                    continue

                # Generated binary paths are build outputs, not tracked inputs.
                if str(resolved).startswith(str((ROOT / "bin").resolve())):
                    continue

                if not resolved.exists():
                    errors.append(
                        f"{path.relative_to(ROOT)}:{lineno}: missing path '{tok}'"
                    )

    if errors:
        print("Markdown command-path check failed:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    print("Markdown command-path check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
