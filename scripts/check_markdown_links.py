#!/usr/bin/env python3
"""Validate markdown relative links and local anchors for tracked markdown files."""

import pathlib
import re
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


def slugify(text: str) -> str:
    slug = text.strip().lower()
    slug = re.sub(r"[^\w\- ]", "", slug)
    slug = slug.replace(" ", "-")
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def heading_anchors(path: pathlib.Path) -> set[str]:
    anchors: set[str] = set()
    in_code = False
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.rstrip("\n")
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        m = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if m:
            anchors.add(slugify(m.group(2)))
    return anchors


def parse_target(raw_target: str) -> tuple[str, str]:
    target = raw_target.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    # strip optional title: path "title"
    if " " in target and not target.startswith("./") and not target.startswith("../"):
        target = target.split(" ", 1)[0]
    path, frag = target, ""
    if "#" in target:
        path, frag = target.split("#", 1)
    path = path.split("?", 1)[0]
    return path, frag


def main() -> int:
    files = git_tracked_markdown()
    anchors_by_file = {p: heading_anchors(p) for p in files}

    link_re = re.compile(r"(?<!\!)\[[^\]]*\]\(([^)]+)\)")
    errors: list[str] = []

    for src in files:
        text = src.read_text(encoding="utf-8", errors="replace")
        for m in link_re.finditer(text):
            raw_target = m.group(1).strip()
            if not raw_target:
                continue
            if raw_target.startswith(("http://", "https://", "mailto:", "tel:")):
                continue

            path_part, frag = parse_target(raw_target)

            if path_part == "" and raw_target.startswith("#"):
                anchor = slugify(frag)
                if anchor and anchor not in anchors_by_file[src]:
                    errors.append(
                        f"{src.relative_to(ROOT)}: missing local anchor '#{frag}'"
                    )
                continue

            if path_part == "":
                continue

            target_path = (src.parent / path_part).resolve()
            try:
                target_path.relative_to(ROOT)
            except ValueError:
                # External/absolute paths outside repo are ignored
                continue

            if not target_path.exists():
                errors.append(
                    f"{src.relative_to(ROOT)}: missing target '{path_part}'"
                )
                continue

            if frag and target_path.suffix.lower() == ".md":
                anchor = slugify(frag)
                if target_path not in anchors_by_file:
                    anchors_by_file[target_path] = heading_anchors(target_path)
                if anchor and anchor not in anchors_by_file[target_path]:
                    errors.append(
                        f"{src.relative_to(ROOT)}: missing anchor '#{frag}' in "
                        f"{target_path.relative_to(ROOT)}"
                    )

    if errors:
        print("Markdown link check failed:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    print("Markdown link check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
