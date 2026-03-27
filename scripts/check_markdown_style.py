#!/usr/bin/env python3
"""Lightweight markdown style lint for tracked markdown files."""

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def tracked_markdown() -> list[pathlib.Path]:
    out = subprocess.check_output(["git", "ls-files", "*.md"], text=True, cwd=ROOT)
    files = []
    for rel in out.splitlines():
        p = ROOT / rel
        if p.exists():
            files.append(p)
    return files


def lint_file(path: pathlib.Path) -> list[str]:
    errors: list[str] = []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    in_code = False
    h1_count = 0
    prev_heading_level = None

    for idx, line in enumerate(lines, start=1):
        if re.search(r"[ \t]+$", line):
            errors.append(f"{path.relative_to(ROOT)}:{idx}: trailing whitespace")

        if "\t" in line:
            errors.append(f"{path.relative_to(ROOT)}:{idx}: tab character found")

        if line.strip().startswith("```"):
            in_code = not in_code
            continue

        if in_code:
            continue

        m = re.match(r"^(#{1,6})\s+\S", line)
        if not m:
            continue

        level = len(m.group(1))
        if level == 1:
            h1_count += 1
        if prev_heading_level is not None and level > prev_heading_level + 1:
            errors.append(
                f"{path.relative_to(ROOT)}:{idx}: heading jump from H{prev_heading_level} to H{level}"
            )
        prev_heading_level = level

    if in_code:
        errors.append(f"{path.relative_to(ROOT)}: unclosed fenced code block")

    if h1_count > 1:
        errors.append(f"{path.relative_to(ROOT)}: multiple H1 headings ({h1_count})")

    return errors


def main() -> int:
    all_errors: list[str] = []
    for file_path in tracked_markdown():
        all_errors.extend(lint_file(file_path))

    if all_errors:
        print("Markdown style lint failed:", file=sys.stderr)
        for err in all_errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    print("Markdown style lint passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
