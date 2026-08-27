# Iceberg & Nessie Internals

A source-level walkthrough of [Apache Iceberg](https://github.com/apache/iceberg) and
[Project Nessie](https://github.com/projectnessie/nessie) — the table format, and the
catalog that gives it Git semantics.

This is not a usage guide. It assumes you can already create a table and run a query,
and goes after the layer underneath: the bytes on disk, the commit protocol, the
pruning algorithms, the storage engine behind Nessie's branches.

## Every code block is real

No Java in this book is typed by hand. Pages carry locators like

```
{% snip ice:core/src/main/java/org/apache/iceberg/SnapshotProducer.java#method:apply() %}
```

and a MkDocs hook replaces each with the actual source read out of `vendor/` at a
pinned tag, carrying **the real upstream line numbers** and a permalink back to GitHub.

If a method is renamed or a file moves, `make check` and the build **fail** rather than
render code that no longer exists.

| Project | Tag | Released |
| --- | --- | --- |
| `apache/iceberg` | `apache-iceberg-1.11.0` | 2026-05-20 |
| `projectnessie/nessie` | `nessie-0.108.4` | 2026-07-31 |

Pins live in [`SOURCES.lock`](SOURCES.lock).

## Quick start

```bash
make install   # python deps via uv
make vendor    # shallow-clone both repos at their pinned tags (~120 MB)
make serve     # http://localhost:8000
```

| Target | What it does |
| --- | --- |
| `make install` | `uv sync` |
| `make vendor` | Clone/refresh `vendor/iceberg` and `vendor/nessie` at the pinned tags |
| `make serve` | Live-reloading site on :8000 |
| `make build` | Static build into `site/` with `--strict` |
| `make check` | Resolve every source locator; non-zero exit if any broke |
| `make deploy` | Publish to GitHub Pages |

## Outline

51 chapters across 11 parts, bottom-up: the on-disk format first, then the code that
manipulates it, then the catalog layer, then Nessie, then where the two meet -- and
finally the one part that gives advice, on driving all of it from Spark.

| Part | Chapters | Focus |
| --- | :--: | --- |
| 1 · Foundations | 4 | Why table formats exist; the two core ideas; navigating the codebases |
| 2 · The Table Spec | 5 | `metadata.json`, manifest lists, manifests, V1→V2→V3 |
| 3 · Iceberg Core | 5 | `TableMetadata`, `SnapshotProducer`, the commit protocol, conflict detection |
| 4 · The Read Path | 5 | `planFiles`, manifest/file pruning, residuals, split planning |
| 5 · The Write Path | 5 | Writers, appends, deletes, CoW vs MoR, maintenance |
| 6 · Catalogs | 4 | The `Catalog` SPI, where atomicity leaks, the REST spec |
| 7 · Nessie Architecture | 5 | Service layers, `Reference`/`Content`, the request path |
| 8 · The Version Store | 5 | `Persist`, the commit DAG, key indexes, CAS, backends |
| 9 · Branching Algorithms | 4 | Commit, merge, transplant, GC |
| 10 · Integration & Ecosystem | 4 | `NessieCatalog`, multi-table commits, the catalog landscape |
| 11 · Spark in Practice | 5 | Catalog wiring, importing existing tables, write distribution, `MERGE INTO`, maintenance |

**Written so far:** Chapter 3.3, `SnapshotProducer`: the life of a commit — the
reference chapter that fixes the format for the rest.

## Locator syntax

| Form | Meaning |
| --- | --- |
| `#method:name` | First declaration of `name` |
| `#method:name()` | The no-arg overload specifically |
| `#method:name@2` | The 2nd overload, in file order |
| `#class:Name` | A class / interface / enum body |
| `#L120-L168` | A raw line range |
| `…+doc` | Keep the leading javadoc |

Repo aliases: `ice` → `apache/iceberg`, `nes` → `projectnessie/nessie`.

## License

MIT for the prose. The injected code belongs to its upstream projects, both Apache 2.0.
