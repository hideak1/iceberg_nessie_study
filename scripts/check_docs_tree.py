"""Fail if anything unexpected is sitting in docs/.

Everything under docs/ is published. An agent that writes a working file with a
relative path while its working directory is docs/ puts an internal document on
the public site, and nothing else notices: the build succeeds, the page is simply
absent from the nav. That happened here -- a 59-line review fragment still marked
IN PROGRESS was served at /design/reviews/coherence/.

The tree is small and completely regular, so state its shape and enforce it.

    uv run python scripts/check_docs_tree.py
    uv run python scripts/check_docs_tree.py --strict
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

PART_DIR = re.compile(r"^part\d{2}_\w+$")
CHAPTER = re.compile(r"^chapter_\d+\.\d+_\w+\.md$")

# Everything the site is allowed to contain, by shape.
ALLOWED_TOP = {"index.md"}
ALLOWED_DIRS = {"assets"}


def main() -> int:
    strays: list[str] = []

    for path in sorted(DOCS.rglob("*")):
        rel = path.relative_to(DOCS)
        parts = rel.parts

        if path.is_dir():
            if len(parts) == 1 and not (PART_DIR.match(parts[0]) or parts[0] in ALLOWED_DIRS):
                strays.append(f"{rel}/  (unexpected directory)")
            continue

        if len(parts) == 1:
            if parts[0] not in ALLOWED_TOP:
                strays.append(f"{rel}  (unexpected file at the top level)")
        elif parts[0] in ALLOWED_DIRS:
            continue
        elif PART_DIR.match(parts[0]):
            if len(parts) != 2:
                strays.append(f"{rel}  (nested too deep inside a part)")
            elif parts[1] != "index.md" and not CHAPTER.match(parts[1]):
                strays.append(f"{rel}  (not an index or a chapter)")
        else:
            strays.append(f"{rel}  (outside the expected tree)")

    if strays:
        print(f"{len(strays)} unexpected entries under docs/ -- these would be published:")
        for s in strays:
            print(f"  {s}")
        print("\nWorking files belong outside docs/. Everything here goes on the site.")
        return 1 if "--strict" in sys.argv else 0

    print("docs/ contains only the index, the part directories and their chapters.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
