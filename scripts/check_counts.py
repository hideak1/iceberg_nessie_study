"""Check counting claims against the code they describe.

Reviewers found the same error in seven different chapters: prose states a count
that the snippet on the same page refutes. "Fourteen lines, nine of them comment"
above a fourteen-line block with four comment lines. "Eleven overloads" where
there are eight. Each sits within a screen of its own evidence, and every human
reviewer had to catch it by hand.

Claims of the form "N lines" are mechanically checkable: resolve the nearest
snippet on the page and count. This script does that, and separately lists every
other numeric claim as a candidate for a reader to verify, since counting method
overloads needs judgement a regex does not have.

    uv run python scripts/check_counts.py            # verify line claims, list candidates
    uv run python scripts/check_counts.py --strict   # non-zero exit on a contradicted claim
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "hooks"))
import snippets  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
    "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
    "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16,
    "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
    "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60, "seventy": 70,
    "eighty": 80, "ninety": 90,
}
NUM = r"(\d+|" + "|".join(WORDS) + r")"

# "Fourteen lines", "a 68-line method", "twelve lines of it"
LINE_CLAIM = re.compile(rf"(?<![-\w]){NUM}[- ]lines?\b", re.IGNORECASE)
# any other numeric adjective, for the candidate list
OTHER_CLAIM = re.compile(
    rf"(?<![-\w]){NUM}\s+(\w+(?:\s+\w+)?)\b", re.IGNORECASE
)

# tolerated: a claim within this many lines of the true count is not flagged,
# because prose legitimately rounds ("about forty lines")
HEDGES = ("about", "roughly", "around", "some", "nearly", "almost", "~",
          "over", "under", "first", "last", "next", "final")


def to_int(token: str) -> int | None:
    token = token.lower()
    if token.isdigit():
        return int(token)
    return WORDS.get(token)


# Only *unscoped absolute* claims rot silently. An agent checking 15 candidates
# found 3 real, and both misses were of this shape -- "on the first line", "the
# first line of apply()" -- sitting above a snippet whose first rendered line is
# an annotation. Every claim naming a *statement* was correct (3 of 3), as was
# every relative one ("the line below the lookup") and every scoped one ("the
# first line of the method"): those describe the code, which does not move when
# a locator widens. So the discriminator is not the phrase, it is whether the
# claim is anchored to something other than the excerpt's own edges.
POSITIONAL = re.compile(
    r"\b(?:the (?:first|last|final|opening|closing) (?:line|member|field|entry|row)"
    r"|on the (?:first|last) line"
    r"|at the (?:top|bottom))\b",
    re.IGNORECASE,
)

# A claim that goes on to name what it is scoped to is describing the code, not
# the excerpt: "the first line of the method", "at the bottom of the file".
SCOPED_AFTER = re.compile(r"^\s*(?:of|in|inside|within)\b", re.IGNORECASE)

# A snippet that opens on code makes "the first line" true no matter how the
# locator moves. The trap is a locator opening on an annotation, comment or a
# signature the prose does not count as a line.
def opens_on_noncode(first_line: str) -> bool:
    s = first_line.strip()
    return (
        s.startswith("@")
        or s.startswith("//")
        or s.startswith("/*")
        or s.startswith("*")
        or bool(re.match(r"^(?:public|private|protected|static|final|abstract|default)\b", s))
    )


def snippet_bodies(text: str) -> list[tuple[int, str]]:
    """(character offset, resolved code) for each snip, for positional checks."""
    out = []
    for m in snippets.SNIP_RE.finditer(text):
        alias, path, locator, _ = m.groups()
        try:
            code, _a, _b, _u = snippets._resolve(alias, path, locator)
            out.append((m.start(), code))
        except snippets.SnippetError:
            pass
    return out


def snippet_spans(text: str) -> list[tuple[int, int]]:
    """(character offset, resolved line count) for each snip on the page."""
    out = []
    for m in snippets.SNIP_RE.finditer(text):
        alias, path, locator, _ = m.groups()
        try:
            code, a, b, _u = snippets._resolve(alias, path, locator)
            out.append((m.start(), b - a + 1))
        except snippets.SnippetError:
            pass
    return out


def main() -> int:
    snippets._load_pins()
    contradicted = []
    candidates = []

    positional = []
    for md in sorted(DOCS.rglob("chapter_*.md")):
        text = md.read_text(encoding="utf-8")
        spans = snippet_spans(text)
        bodies = snippet_bodies(text)
        rel = str(md.relative_to(ROOT))

        for m in POSITIONAL.finditer(text):
            if SCOPED_AFTER.match(text[m.end(): m.end() + 12]):
                continue
            before = [b for b in bodies if b[0] < m.start()]
            if not before or m.start() - before[-1][0] > 900:
                continue
            code = before[-1][1].splitlines()
            if not code or not opens_on_noncode(code[0]):
                continue
            line_no = text[: m.start()].count("\n") + 1
            positional.append(
                (rel, line_no, m.group(0).strip(), code[0].strip(), code[-1].strip())
            )

        for m in LINE_CLAIM.finditer(text):
            claimed = to_int(m.group(1))
            if claimed is None or not spans:
                continue
            # "in one line", "a two-line helper" are idiom far more often than
            # measurement. Only a count large enough to have been counted is a
            # claim worth checking.
            if claimed < 5:
                continue
            context = text[max(0, m.start() - 60): m.start()].lower()
            if any(h in context for h in HEDGES):
                continue
            # A sentence that names a file is measuring that file, not the
            # snippet on the page. "CONTRIBUTING.md is a 23-line stub" is a
            # claim about a file this page never injects.
            # A sentence boundary is a period followed by whitespace. Splitting
            # on a bare "." puts the boundary inside "CONTRIBUTING.md" and hides
            # the very filename this check is looking for.
            window = text[max(0, m.start() - 400): m.start() + 120]
            parts = re.split(r"(?<=\.)\s", window)
            sentence = parts[-1] if len(parts) > 1 else window
            if re.search(r"\b[\w.-]+\.(md|json|yaml|yml|txt|gradle|kts|avsc)\b", sentence):
                continue
            # The claim must sit just after a snippet, describing it. A snippet
            # further down the page is a different subject.
            before = [s for s in spans if s[0] < m.start()]
            if not before:
                continue
            offset, actual = before[-1]
            if m.start() - offset > 900:
                continue
            if claimed != actual and abs(claimed - actual) <= 20:
                line_no = text[: m.start()].count("\n") + 1
                contradicted.append((rel, line_no, m.group(0), actual))

        for m in OTHER_CLAIM.finditer(text):
            n = to_int(m.group(1))
            noun = m.group(2)
            if n is None or n > 60 or "line" in noun.lower():
                continue
            line_no = text[: m.start()].count("\n") + 1
            candidates.append((rel, line_no, f"{m.group(1)} {noun}"))

    print("== Line claims contradicted by the nearest snippet ==")
    if contradicted:
        for rel, line_no, claim, actual in contradicted:
            print(f"  {rel}:{line_no}  \"{claim}\" -> nearest snippet is {actual} lines")
    else:
        print("  none")

    print(f"\n== Other numeric claims: {len(candidates)} to verify by hand ==")
    print("   (counting overloads, cases, checks -- needs judgement, not a regex)")

    print("\n== Positional claims about a snippet ==")
    print("   (\"the last member\", \"on the first line\" -- a snippet that gains a")
    print("    leading annotation silently invalidates these)")
    for rel, line_no, phrase, first, last in positional:
        print(f"  {rel}:{line_no}  \"{phrase}\"")
        print(f"      snippet begins: {first[:64]}")
        print(f"      snippet ends:   {last[:64]}")

    if "--strict" in sys.argv and contradicted:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
