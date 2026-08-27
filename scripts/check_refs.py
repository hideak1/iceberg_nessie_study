"""Check the book's internal cross-references.

Eleven agents wrote 51 chapters in parallel and five more reviewed them part by
part, so every promise one chapter makes about another was written by someone who
could not see the other side. There are 529 such references. Reading them all is
the kind of job that gets abandoned halfway; counting them is not.

This checks what can be checked mechanically:
  - every "Chapter X.Y" names a chapter that exists
  - every "Next:" points at the following chapter, or the next part's opener
  - every "Prerequisites:" names chapters that exist and come earlier
  - no chapter cites itself
  - which chapters nothing ever points at

What it cannot check -- whether a promise was kept, whether a term drifted,
whether a concept was assumed but never taught -- is left to a reader.

    uv run python scripts/check_refs.py
    uv run python scripts/check_refs.py --strict
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

REF = re.compile(r"Chapter\s+(\d+\.\d+)")
NEXT_LINE = re.compile(r"^\*\*Next:\*\*\s*(.+)$", re.MULTILINE)
PREREQ = re.compile(r"\*\*Prerequisites:\*\*\s*(.+)")


def num_of(path: Path) -> str:
    m = re.search(r"chapter_(\d+\.\d+)", path.name)
    return m.group(1) if m else ""


def sort_key(n: str) -> tuple[int, int]:
    a, b = n.split(".")
    return int(a), int(b)


def main() -> int:
    chapters = {}
    for md in DOCS.rglob("chapter_*.md"):
        chapters[num_of(md)] = md
    order = sorted(chapters, key=sort_key)

    dangling, self_ref, bad_next, bad_prereq = [], [], [], []
    cited = defaultdict(set)

    for i, num in enumerate(order):
        md = chapters[num]
        text = md.read_text(encoding="utf-8")
        rel = f"{md.parent.name}/{md.name}"
        # The H1 is "# Chapter X.Y - Title"; a chapter naming itself there is
        # its own title, not a self-reference.
        body = re.sub(r"^#\s+Chapter[^\n]*$", "", text, count=1, flags=re.MULTILINE)

        for m in REF.finditer(body):
            target = m.group(1)
            line = body[: m.start()].count("\n") + 1
            if target not in chapters:
                dangling.append((rel, line, target))
            elif target == num:
                self_ref.append((rel, line))
            else:
                cited[target].add(num)

        # Next: should lead to the following chapter in reading order
        nxt = NEXT_LINE.search(text)
        if nxt and i + 1 < len(order):
            expected = order[i + 1]
            named = REF.findall(nxt.group(1))
            # At a part boundary, handing off to "Part N" reads better than
            # naming its first chapter, and says the same thing.
            crossing = expected.split(".")[0] != num.split(".")[0]
            part_named = re.search(rf"\bPart\s+{expected.split('.')[0]}\b", nxt.group(1))
            if crossing and part_named:
                pass
            elif named and expected not in named:
                bad_next.append((rel, named[0], expected))
            elif not named and not (crossing and part_named):
                bad_next.append((rel, "names neither the chapter nor the part", expected))

        pre = PREREQ.search(text)
        if pre:
            for target in REF.findall(pre.group(1)):
                if target not in chapters:
                    bad_prereq.append((rel, target, "does not exist"))
                elif sort_key(target) > sort_key(num):
                    bad_prereq.append((rel, target, "comes later in the book"))

    print(f"{len(chapters)} chapters, "
          f"{sum(len(v) for v in cited.values())} resolved cross-references\n")

    def report(title: str, rows, fmt):
        print(f"== {title}: {len(rows)} ==")
        for r in rows:
            print("  " + fmt(r))
        if not rows:
            print("  none")
        print()

    report("References to a chapter that does not exist", dangling,
           lambda r: f"{r[0]}:{r[1]} -> Chapter {r[2]}")
    report("Chapters citing themselves", self_ref,
           lambda r: f"{r[0]}:{r[1]}")
    report("Next: not pointing at the following chapter", bad_next,
           lambda r: f"{r[0]} -> {r[1]}, expected {r[2]}")
    report("Prerequisites that do not exist or come later", bad_prereq,
           lambda r: f"{r[0]} -> Chapter {r[1]} ({r[2]})")

    orphans = [n for n in order if not cited.get(n)]
    print(f"== Chapters nothing points at: {len(orphans)} ==")
    print("   (not an error -- but a chapter no other chapter needs is worth a look)")
    print("  " + (", ".join(orphans) if orphans else "none"))

    broken = len(dangling) + len(self_ref) + len(bad_next) + len(bad_prereq)
    return 1 if (broken and "--strict" in sys.argv) else 0


if __name__ == "__main__":
    raise SystemExit(main())
