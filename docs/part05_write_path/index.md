# Part 5 · The Write Path

Turning rows into data files, deletes into delete files, and both into a snapshot the read path can prune efficiently.

| # | Chapter | Status |
| --- | --- | --- |
| **5.1** | [The writer family and metrics collection](chapter_5.1_writers.md) | :material-check-circle: written |
| **5.2** | [`FastAppend` vs `MergeAppend`](chapter_5.2_append_strategies.md) | :material-check-circle: written |
| **5.3** | [Position deletes, equality deletes, and `RowDelta`](chapter_5.3_deletes.md) | :material-check-circle: written |
| **5.4** | [Copy-on-write, merge-on-read, and V3 deletion vectors](chapter_5.4_cow_mor.md) | :material-check-circle: written |
| **5.5** | [Maintenance: compaction, snapshot expiry, orphan files](chapter_5.5_maintenance.md) | :material-check-circle: written |
