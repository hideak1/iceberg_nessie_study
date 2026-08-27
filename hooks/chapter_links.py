"""Turn every "Chapter X.Y" into a link to that chapter.

The book leans on cross-references -- 583 of them -- and every one was plain
text. A reader who follows "the audit in Chapter 6.2" gets nothing. Writing them
as markdown links by hand would mean every author knowing every other chapter's
filename, which is exactly the coupling that produced the errors this book spent
so long correcting.

So the prose stays as prose and this resolves it at build time. A chapter that
moves or is renamed re-links itself; a reference to a chapter that does not exist
fails the build rather than rendering as dead text.
"""

from __future__ import annotations

import re
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent / "docs"

# "Chapter 3.4", "Chapters 3.4 and 3.5", "Chapter 3.4's"
REF = re.compile(r"\bChapters?\s+(\d+\.\d+)((?:\s*(?:,|and|&|to|-|–)\s*\d+\.\d+)*)")

_index: dict[str, str] | None = None


def _build_index() -> dict[str, str]:
    out = {}
    for md in DOCS.rglob("chapter_*.md"):
        m = re.search(r"chapter_(\d+\.\d+)", md.name)
        if m:
            out[m.group(1)] = f"{md.parent.name}/{md.name}"
    return out


class UnknownChapter(Exception):
    pass


def _relative(from_page: str, to_doc: str) -> str:
    """A path from one docs page to another, for mkdocs to resolve."""
    depth = from_page.count("/")
    return "../" * depth + to_doc


def on_page_markdown(markdown, page=None, config=None, files=None, **kwargs):
    global _index
    if _index is None:
        _index = _build_index()

    src = getattr(getattr(page, "file", None), "src_path", "")
    # index pages already link their own chapters in a table
    self_num = None
    m = re.search(r"chapter_(\d+\.\d+)", src)
    if m:
        self_num = m.group(1)

    missing: list[str] = []

    def link_one(num: str, label: str) -> str:
        if num == self_num:
            return label
        target = _index.get(num)
        if not target:
            missing.append(num)
            return label
        return f"[{label}]({_relative(src, target)})"

    def replace(match: re.Match) -> str:
        head, tail = match.group(1), match.group(2) or ""
        word = match.group(0)[: match.group(0).index(head)]  # "Chapter " / "Chapters "
        nums = [head] + re.findall(r"\d+\.\d+", tail)
        if len(nums) == 1:
            # link the whole phrase -- "Chapter 3.4" is what a reader aims at,
            # and a bare "3.4" is a small target
            return link_one(head, word + head)
        # keep the connective text, link each number
        out = word + link_one(nums[0], nums[0])
        rest = tail
        for n in nums[1:]:
            rest = rest.replace(n, "\x00", 1)
        for n in nums[1:]:
            rest = rest.replace("\x00", link_one(n, n), 1)
        return out + rest

    result = REF.sub(replace, markdown)

    if missing:
        raise UnknownChapter(
            f"{src}: reference to chapter(s) that do not exist: {', '.join(sorted(set(missing)))}"
        )
    return result
