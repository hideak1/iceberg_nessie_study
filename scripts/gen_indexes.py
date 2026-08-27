"""Regenerate each part's contents table from the chapters on disk.

Several agents wrote these tables by hand at different times, so their formats
drifted. The prose at the top of each page is authored and is preserved; only
the table below it is rebuilt.

    uv run python scripts/gen_indexes.py          # rewrite
    uv run python scripts/gen_indexes.py --check  # fail if any table is stale
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"


def chapter_key(path: Path) -> tuple[int, int]:
    m = re.search(r"chapter_(\d+)\.(\d+)", path.name)
    return (int(m.group(1)), int(m.group(2))) if m else (999, 999)


def parse_h1(path: Path) -> tuple[str, str]:
    """Return (number, title) from '# Chapter 3.3 — Foo'."""
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("# "):
            h1 = line[2:].strip()
            m = re.match(r"Chapter\s+([\d.]+)\s*[—:-]\s*(.+)", h1)
            if m:
                return m.group(1), m.group(2).strip().rstrip(".")
            return "", h1
    return "", path.stem


def render(part: Path) -> str | None:
    index = part / "index.md"
    if not index.exists():
        return None
    text = index.read_text(encoding="utf-8")

    # keep everything above the contents table
    cut = text.find("| # |")
    head = (text[:cut] if cut != -1 else text).rstrip()

    rows = ["| # | Chapter | Status |", "| --- | --- | --- |"]
    for ch in sorted(part.glob("chapter_*.md"), key=chapter_key):
        num, title = parse_h1(ch)
        rows.append(
            f"| **{num}** | [{title}]({ch.name}) | :material-check-circle: written |"
        )
    if len(rows) == 2:
        return None
    return head + "\n\n" + "\n".join(rows) + "\n"


def main() -> int:
    check = "--check" in sys.argv
    stale, written = [], 0
    for part in sorted(DOCS.glob("part*")):
        new = render(part)
        if new is None:
            continue
        index = part / "index.md"
        if index.read_text(encoding="utf-8").rstrip() == new.rstrip():
            continue
        if check:
            stale.append(part.name)
        else:
            index.write_text(new, encoding="utf-8")
            written += 1

    if check:
        if stale:
            print("stale contents tables: " + ", ".join(stale))
            return 1
        print("all part contents tables are current.")
        return 0
    print(f"rewrote {written} part contents tables.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
