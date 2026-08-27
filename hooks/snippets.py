"""Inject real upstream source into the book at build time.

Not one line of Java in this book is typed by hand. Pages carry locators:

    {% snip ice:core/src/main/java/org/apache/iceberg/SnapshotProducer.java#method:apply() %}

and this hook replaces each with the actual code read out of `vendor/`, at the
tag pinned in SOURCES.lock, carrying the real upstream line numbers and a
permalink back to GitHub.

If a locator cannot be resolved -- method renamed, file moved, tag bumped --
the build fails. A snippet that silently goes stale is worse than no snippet.

Locator forms after `#`:
    method:name          first member named `name`
    method:name()        the no-arg overload
    method:name@2        the 2nd overload, in file order
    class:Name           a class/interface/enum body
    L120-L168            a raw line range
Append `+doc` to keep the leading javadoc: `method:apply()+doc`
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENDOR = ROOT / "vendor"

# repo alias -> (vendor dir, github org/repo, pinned tag)
REPOS = {
    "ice": ("iceberg", "apache/iceberg", None),
    "nes": ("nessie", "projectnessie/nessie", None),
}

SNIP_RE = re.compile(r"\{\%\s*snip\s+(\w+):([^#\s]+)#(\S+?)\s*(?:\|\s*([^%]*?))?\s*\%\}")

LANG_BY_SUFFIX = {
    ".java": "java",
    ".json": "json",
    ".avsc": "json",
    ".py": "python",
    ".kt": "kotlin",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".sql": "sql",
    ".xml": "xml",
    ".md": "markdown",
}

# Java keywords that can precede `name(` on a line that is a *call*, not a declaration.
_NOT_A_DECL = r"(?!\s*(?:return|throw|new|if|while|for|switch|catch|else|do|assert|synchronized)\b)"


class SnippetError(Exception):
    """Raised when a locator cannot be resolved. Fails the build."""


def _load_pins() -> None:
    """Read the tag pins out of SOURCES.lock so links point at the right tree."""
    lock = ROOT / "SOURCES.lock"
    if not lock.exists():
        raise SnippetError("SOURCES.lock is missing -- cannot pin source links")
    current = None
    tags: dict[str, str] = {}
    for line in lock.read_text().splitlines():
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            current = line[1:-1]
        elif "=" in line and not line.startswith("#") and current:
            key, _, value = line.partition("=")
            if key.strip() == "tag":
                tags[current] = value.strip()
    for alias, (vendor_dir, gh, _) in REPOS.items():
        tag = tags.get(vendor_dir)
        if not tag:
            raise SnippetError(f"SOURCES.lock has no tag for [{vendor_dir}]")
        REPOS[alias] = (vendor_dir, gh, tag)


def _scan_braces(lines: list[str], start: int) -> int | None:
    """Index of the line closing the member that opens at or after `start`.

    Walks characters so that braces inside strings, chars, text blocks and
    comments do not throw off the depth count. Returns the line index of the
    matching `}`, or of the `;` for a body-less declaration.
    """
    depth = 0
    opened = False
    in_block_comment = False
    in_text_block = False

    for i in range(start, len(lines)):
        line = lines[i]
        j = 0
        while j < len(line):
            rest = line[j:]
            c = line[j]

            if in_block_comment:
                if rest.startswith("*/"):
                    in_block_comment = False
                    j += 2
                    continue
                j += 1
                continue

            if in_text_block:
                if rest.startswith('"""'):
                    in_text_block = False
                    j += 3
                    continue
                j += 1
                continue

            if rest.startswith("/*"):
                in_block_comment = True
                j += 2
                continue
            if rest.startswith("//"):
                break
            if rest.startswith('"""'):
                in_text_block = True
                j += 3
                continue

            if c == '"' or c == "'":
                quote = c
                j += 1
                while j < len(line):
                    if line[j] == "\\":
                        j += 2
                        continue
                    if line[j] == quote:
                        j += 1
                        break
                    j += 1
                continue

            if c == "{":
                depth += 1
                opened = True
            elif c == "}":
                depth -= 1
                if opened and depth == 0:
                    return i
            elif c == ";" and not opened:
                return i
            j += 1

    return None


def _find_member(lines: list[str], name: str, want_noarg: bool, nth: int) -> int:
    """Line index of the declaration of member `name`."""
    ident = re.escape(name)
    # A type may contain spaces (`List<Foo> `, `Map<K, V>`) but must begin and
    # end on a real character -- otherwise whitespace alone satisfies "has a
    # return type" and every bare call statement parses as a declaration.
    tc = r"[\w$.<>\[\],?&]"
    type_expr = rf"{tc}(?:[\w$.<>\[\],?&\s]*?{tc})?"
    modifiers = (
        r"(?:public|protected|private|static|final|abstract|synchronized|"
        r"default|transient|volatile|native|strictfp)"
    )
    # A declaration carries at least a modifier or a return type. A bare
    # `runValidations(parent);` call statement has neither -- that distinction
    # is the only thing separating a declaration from a call site here.
    decl = re.compile(
        rf"^(\s+){_NOT_A_DECL}"
        rf"(?:@\w+(?:\([^)]*\))?\s+)*"
        rf"(?:"
        rf"(?:{modifiers}\s+)+(?:<[^>]*>\s*)?(?:{type_expr}\s+)?"
        rf"|"
        rf"(?:<[^>]*>\s*)?{type_expr}\s+"
        rf")"
        rf"{ident}\s*\("
    )

    hits = []
    for i, line in enumerate(lines):
        m = decl.match(line)
        if not m:
            continue
        after = line[m.end() - 1:]
        if want_noarg:
            # Require empty parens -- distinguishes apply() from apply(a, b).
            if not re.match(r"\(\s*\)", after):
                continue
        hits.append(i)

    if not hits:
        raise SnippetError(f"no declaration of `{name}` found")
    if nth > len(hits):
        raise SnippetError(
            f"asked for overload #{nth} of `{name}` but only {len(hits)} found "
            f"(at lines {[h + 1 for h in hits]})"
        )
    return hits[nth - 1]


def _find_type(lines: list[str], name: str) -> int:
    ident = re.escape(name)
    pat = re.compile(rf"^\s*(?:[\w@]+\s+)*(?:class|interface|enum|record)\s+{ident}\b")
    for i, line in enumerate(lines):
        if pat.match(line):
            return i
    raise SnippetError(f"no type declaration of `{name}` found")


def _extend_upwards(lines: list[str], start: int, with_doc: bool) -> int:
    """Pull in annotations above a declaration, and the javadoc if asked."""
    i = start
    while i > 0:
        prev = lines[i - 1].strip()
        if prev.startswith("@") and not prev.startswith("@ "):
            i -= 1
            continue
        break
    if with_doc and i > 0 and lines[i - 1].strip().endswith("*/"):
        j = i - 1
        while j > 0 and not lines[j].strip().startswith("/*"):
            j -= 1
        if lines[j].strip().startswith("/*"):
            i = j
    return i


def _resolve(alias: str, path: str, locator: str) -> tuple[str, int, int, str]:
    """Return (code, first_line, last_line, github_url) for one locator."""
    if alias not in REPOS:
        raise SnippetError(f"unknown repo alias `{alias}` (known: {', '.join(REPOS)})")
    vendor_dir, gh, tag = REPOS[alias]
    src = VENDOR / vendor_dir / path
    if not src.exists():
        raise SnippetError(
            f"{vendor_dir}/{path} does not exist at {tag}. "
            f"Run `make vendor`; if that succeeds the file moved upstream."
        )

    text = src.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    with_doc = locator.endswith("+doc")
    if with_doc:
        locator = locator[: -len("+doc")]

    m = re.fullmatch(r"L(\d+)-L(\d+)", locator)
    if m:
        first, last = int(m.group(1)), int(m.group(2))
        if last > len(lines):
            raise SnippetError(
                f"{path} has {len(lines)} lines, cannot take L{first}-L{last}"
            )
        start, end = first - 1, last - 1
    else:
        if locator.startswith("class:"):
            start = _find_type(lines, locator[len("class:"):])
        else:
            name = locator[len("method:"):] if locator.startswith("method:") else locator
            nth = 1
            if "@" in name:
                name, _, n = name.partition("@")
                nth = int(n)
            want_noarg = name.endswith("()")
            if want_noarg:
                name = name[:-2]
            start = _find_member(lines, name, want_noarg, nth)
            start = _extend_upwards(lines, start, with_doc)

        end = _scan_braces(lines, start)
        if end is None:
            raise SnippetError(f"unbalanced braces resolving `{locator}` in {path}")

    body = "\n".join(lines[start: end + 1])
    body = _dedent(body)
    url = f"https://github.com/{gh}/blob/{tag}/{path}#L{start + 1}-L{end + 1}"
    return body, start + 1, end + 1, url


def _dedent(block: str) -> str:
    rows = [r for r in block.splitlines() if r.strip()]
    if not rows:
        return block
    pad = min(len(r) - len(r.lstrip()) for r in rows)
    if pad == 0:
        return block
    return "\n".join(r[pad:] if r.strip() else r for r in block.splitlines())


def _render(alias: str, path: str, locator: str, title: str | None) -> str:
    code, first, last, url = _resolve(alias, path, locator)
    lang = LANG_BY_SUFFIX.get(Path(path).suffix, "text")
    label = title or f"{Path(path).name} · {locator}"
    _, gh, tag = REPOS[alias]
    return (
        f'```{lang} title="{label}" linenums="{first}"\n'
        f"{code}\n"
        "```\n"
        f'<p class="snip-src">:material-source-branch: '
        f"[`{gh}` · `{path}` L{first}–L{last}]({url}) "
        f"<span>@ {tag}</span></p>"
    )


def on_page_markdown(markdown, page=None, config=None, files=None, **kwargs):
    """MkDocs hook: swap every locator for real upstream source."""
    if not REPOS["ice"][2]:
        _load_pins()

    errors: list[str] = []

    def replace(match: re.Match) -> str:
        alias, path, locator, title = match.groups()
        try:
            return _render(alias, path, locator, title)
        except SnippetError as exc:
            src = getattr(page, "file", None)
            where = getattr(src, "src_path", "?")
            errors.append(f"  {where}: {alias}:{path}#{locator}\n      -> {exc}")
            return match.group(0)

    out = SNIP_RE.sub(replace, markdown)

    if errors:
        raise SnippetError(
            "unresolved source locators:\n" + "\n".join(errors)
        )
    return out


def _check(scope: str | None = None) -> int:
    """Standalone validation -- used by `make check`.

    `scope` narrows to a subtree so that agents writing different parts in
    parallel do not fail on each other's half-finished pages.
    """
    _load_pins()
    target = (ROOT / scope) if scope else (ROOT / "docs")
    if not target.exists():
        print(f"no such path: {target}")
        return 1
    total = 0
    bad = 0
    sources = [target] if target.is_file() else sorted(target.rglob("*.md"))
    for md in sources:
        text = md.read_text(encoding="utf-8")
        for match in SNIP_RE.finditer(text):
            alias, path, locator, _ = match.groups()
            total += 1
            try:
                _, first, last, _url = _resolve(alias, path, locator)
            except SnippetError as exc:
                bad += 1
                rel = md.relative_to(ROOT)
                print(f"FAIL {rel}\n     {alias}:{path}#{locator}\n     -> {exc}")
            else:
                pass
    ok = total - bad
    print(f"\n{ok}/{total} source locators resolved.")
    if bad:
        print(f"{bad} broken. The pinned tree no longer matches the text.")
        return 1
    return 0


def _resolve_one(spec: str) -> int:
    """Check a single locator: `ice:path/To.java#method:name`.

    Used while designing a chapter, before any page exists to scan.
    """
    _load_pins()
    if ":" not in spec or "#" not in spec:
        print("usage: snippets.py --resolve ice:path/To.java#method:name")
        return 2
    alias, _, rest = spec.partition(":")
    path, _, locator = rest.partition("#")
    try:
        code, first, last, url = _resolve(alias, path, locator)
    except SnippetError as exc:
        print(f"FAIL {spec}\n  -> {exc}")
        return 1
    lines = code.splitlines()
    print(f"OK  {spec}")
    print(f"    L{first}-L{last}  ({last - first + 1} lines)")
    print(f"    {url}")
    print(f"    first: {lines[0].strip()[:90]}")
    print(f"    last:  {lines[-1].strip()[:90]}")
    return 0


if __name__ == "__main__":
    import sys as _sys

    if len(_sys.argv) > 2 and _sys.argv[1] == "--resolve":
        raise SystemExit(_resolve_one(_sys.argv[2]))
    _scope = _sys.argv[1] if len(_sys.argv) > 1 else None
    raise SystemExit(_check(_scope))
