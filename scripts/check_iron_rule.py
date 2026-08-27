"""Catch hand-typed source that bypassed the injection mechanism.

`hooks/snippets.py` proves that every {% snip %} resolves. It says nothing about
code that never went through a snip at all -- a plain ```java fence types
straight into the page and builds perfectly green. A review found seven such
blocks across five chapters, one of them reformatted from the original, which is
exactly the drift the injection mechanism exists to prevent.

The BRIEF allows one narrow exception: re-quoting a few lines of code already
injected on the same page, as a focus device. This script encodes that rule --
a fenced code block is legal only if its content also appears in what the page's
own snippets resolve to.

    uv run python scripts/check_iron_rule.py          # report
    uv run python scripts/check_iron_rule.py --strict # non-zero exit on violations
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "hooks"))
import snippets  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

# Languages that mean "this is source from the vendored repos".
SOURCE_LANGS = {"java", "kotlin", "scala"}

# A fence with no language is neither source nor prose: it is data, output or a
# listing. It cannot be matched against the page's snippets the way code can, so
# it is reported for a human to source or justify. An audit found one such block
# in this book presenting a directory listing with invented UUIDs -- data typed
# by hand, in a book whose whole claim is that it does not do that. It was
# invisible here because the check only looked at java, kotlin and scala.
# The obvious way this regresses is to tag the fence ```text instead of leaving
# it bare. These tags all mean the same thing -- "this is data or output, not
# source" -- and all of them need a provenance or a stated reason.
UNSOURCED_LANGS = {"", "text", "txt", "plain", "plaintext", "output", "console"}

# `\w*` not `\w+`: a fence with no language was never matched at all, which
# is why an unsourced data block sat in this book undetected.
FENCE = re.compile(r"^```(\w*)[^\n]*\n(.*?)^```", re.MULTILINE | re.DOTALL)


def normalise(code: str) -> list[str]:
    """Code lines, stripped of indentation, blanks and comments.

    Comments are dropped on both sides so that dropping one while re-quoting is
    not treated as tampering. Omitting a comment to keep a quote tight is an
    editorial choice; changing a line of code is the thing this check exists to
    catch, and that still shows up.
    """
    out = []
    in_block = False
    for raw in code.splitlines():
        ln = raw.strip()
        if in_block:
            if "*/" in ln:
                in_block = False
                ln = ln.split("*/", 1)[1].strip()
                if not ln:
                    continue
            else:
                continue
        if ln.startswith("/*"):
            if "*/" not in ln:
                in_block = True
            continue
        if ln.startswith("//") or ln.startswith("*"):
            continue
        if not ln:
            continue
        out.append(ln)
    return out


def page_injected_blocks(text: str) -> list[list[str]]:
    """Each snip on the page, resolved and normalised, kept separate.

    Kept separate on purpose. Merging them into one set lets a block of foreign
    code pass whenever its lines happen to appear individually somewhere on the
    page -- and lines like `}` or a bare `return x;` appear everywhere. A
    re-quote is a contiguous run of one block, so that is what to look for.
    """
    blocks = []
    for m in snippets.SNIP_RE.finditer(text):
        alias, path, locator, _title = m.groups()
        try:
            code, _a, _b, _u = snippets._resolve(alias, path, locator)
            blocks.append(normalise(code))
        except snippets.SnippetError:
            pass  # hooks/snippets.py already reports unresolvable locators
    return blocks


def best_contiguous_match(lines: list[str], blocks: list[list[str]]) -> float:
    """Longest run of `lines` appearing consecutively in any one block."""
    best = 0
    for block in blocks:
        for start in range(len(block)):
            run = 0
            while (
                run < len(lines)
                and start + run < len(block)
                and block[start + run] == lines[run]
            ):
                run += 1
            best = max(best, run)
    return best / len(lines) if lines else 1.0


def main() -> int:
    snippets._load_pins()
    violations: list[tuple[str, int, str, int, float]] = []
    unsourced: list[tuple[str, int, str]] = []
    requotes = 0
    checked = 0

    for md in sorted(DOCS.rglob("*.md")):
        text = md.read_text(encoding="utf-8")
        if "{% snip" not in text and "```java" not in text:
            continue
        blocks = page_injected_blocks(text)

        for m in FENCE.finditer(text):
            lang, body = m.group(1), m.group(2)
            if lang in UNSOURCED_LANGS and body.strip():
                unsourced.append(
                    (str(md.relative_to(ROOT)),
                     text[: m.start()].count("\n") + 1,
                     next((l.strip() for l in body.splitlines() if l.strip()), "")[:58])
                )
                continue
            if lang not in SOURCE_LANGS:
                continue
            checked += 1
            lines = normalise(body)
            if not lines:
                continue
            ratio = best_contiguous_match(lines, blocks)
            line_no = text[: m.start()].count("\n") + 1
            if ratio >= 0.85:
                requotes += 1
            else:
                rel = str(md.relative_to(ROOT))
                violations.append((rel, line_no, lang, len(lines), ratio))

    for rel, line_no, lang, n, ratio in violations:
        print(f"HAND-TYPED  {rel}:{line_no}  ({lang}, {n} lines, "
              f"longest contiguous run found in this page's snippets: {ratio:.0%})")

    for rel, line_no, first in unsourced:
        print(f"UNSOURCED   {rel}:{line_no}  (fence with no language)")
        print(f"              {first}")

    print()
    print(f"{checked} source fences: {requotes} are re-quotes of injected code, "
          f"{len(violations)} are hand-typed.")
    if unsourced:
        print(f"{len(unsourced)} unsourced data blocks: neither injected nor prose. "
              f"Each needs a source or a stated reason.")
    if violations:
        print("Fix by widening an existing locator or adding a new one.")
    if (violations or unsourced) and "--strict" in sys.argv:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
