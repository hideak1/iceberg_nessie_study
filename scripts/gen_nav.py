"""Regenerate the mkdocs nav from what is actually on disk.

Hand-maintaining 51 nav entries drifts from the filenames the moment anyone
renames a chapter. This reads docs/, pulls each chapter's title out of its H1,
and rewrites the nav block in place.

    uv run python scripts/gen_nav.py          # rewrite
    uv run python scripts/gen_nav.py --check  # fail if the nav is stale
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
MKDOCS = ROOT / "mkdocs.yml"

PART_TITLES = {
    "part01_foundations": "Part 1 · Foundations",
    "part02_table_spec": "Part 2 · Table Spec",
    "part03_iceberg_core": "Part 3 · Iceberg Core",
    "part04_read_path": "Part 4 · Read Path",
    "part05_write_path": "Part 5 · Write Path",
    "part06_catalogs": "Part 6 · Catalogs",
    "part07_nessie_architecture": "Part 7 · Nessie Architecture",
    "part08_version_store": "Part 8 · Version Store",
    "part09_branching": "Part 9 · Branching",
    "part10_ecosystem": "Part 10 · Ecosystem",
    "part11_spark_practice": "Part 11 · Spark in Practice",
}


def chapter_key(path: Path) -> tuple[int, int]:
    """Sort 4.10 after 4.9, which a plain string sort would not."""
    m = re.search(r"chapter_(\d+)\.(\d+)", path.name)
    return (int(m.group(1)), int(m.group(2))) if m else (999, 999)


def title_of(path: Path) -> str:
    """Nav label from the H1, e.g. '# Chapter 3.3 — Foo' -> '3.3 Foo'."""
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("# "):
            h1 = line[2:].strip()
            m = re.match(r"Chapter\s+([\d.]+)\s*[—:-]\s*(.+)", h1)
            if m:
                title = m.group(2).strip().rstrip(".")
                # nav labels are plain text -- markdown in them renders literally,
                # so `metadata.json` reaches the sidebar with its backticks showing
                title = title.replace("`", "")
                return f"{m.group(1)} {title}"
            return h1.replace("`", "")
    return path.stem


def build_nav() -> str:
    lines = ["nav:", "  - Home: index.md"]
    for slug, label in PART_TITLES.items():
        part = DOCS / slug
        if not (part / "index.md").exists():
            continue
        lines.append(f'  - "{label}":')
        lines.append(f"      - {slug}/index.md")
        for ch in sorted(part.glob("chapter_*.md"), key=chapter_key):
            label_text = title_of(ch).replace('"', "'")
            lines.append(f'      - "{label_text}": {slug}/{ch.name}')
    return "\n".join(lines) + "\n"


def main() -> int:
    text = MKDOCS.read_text(encoding="utf-8")
    nav = build_nav()

    # nav is the final top-level block in this file
    start = text.index("\nnav:\n")
    updated = text[: start + 1] + nav

    count = nav.count("chapter_")
    if "--check" in sys.argv:
        if text.rstrip() != updated.rstrip():
            print(f"nav is stale ({count} chapters on disk). Run: make nav")
            return 1
        print(f"nav is current ({count} chapters).")
        return 0

    MKDOCS.write_text(updated, encoding="utf-8")
    print(f"nav rewritten: {count} chapters across "
          f"{sum(1 for s in PART_TITLES if (DOCS / s / 'index.md').exists())} parts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
