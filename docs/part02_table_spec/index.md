# Part 2 · The Table Spec, byte by byte

The on-disk format, read directly. Every structure here is what the source in Parts 3-5 manipulates, so this is the vocabulary the rest of the book assumes.

| # | Chapter | Status |
| --- | --- | --- |
| **2.1** | [Anatomy of a table directory after a real write](chapter_2.1_table_directory_anatomy.md) | :material-check-circle: written |
| **2.2** | [`metadata.json`, field by field](chapter_2.2_metadata_json.md) | :material-check-circle: written |
| **2.3** | [The manifest list: snapshot-level pruning data](chapter_2.3_manifest_list.md) | :material-check-circle: written |
| **2.4** | [The manifest file: data files, column metrics, field IDs](chapter_2.4_manifest_file.md) | :material-check-circle: written |
| **2.5** | [V1 → V2 → V3: row-level deletes, deletion vectors, row lineage](chapter_2.5_format_versions.md) | :material-check-circle: written |
| **2.6** | [`PartitionSpec`: transforms, hidden partitioning, partition evolution](chapter_2.6_partitioning.md) | :material-check-circle: written |
