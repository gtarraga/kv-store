# Segmented storage with compaction

Append-only file-based KV store with segmented logs. Instead of a single large file, data is split across multiple segment files. Each segment is immutable once closed, and segments can be compacted to remove duplicates and tombstones.

## Architecture

**Segments**: Data is split across multiple numbered segment files (e.g., `segment_0001.db`, `segment_0002.db`). When the active segment reaches a size threshold, it's closed and a new active segment is created for writes.

**Compaction**: Background process that rewrites individual segments to remove:

- Duplicate keys (keeping only the latest value)
- Tombstone records for deleted keys

This keeps segment sizes bounded and improves read performance.

## Methods

1. **Add**: Appends to the active segment. If segment reaches threshold, close it and create new segment.
2. **Search**: Scans segments from newest to oldest, stopping at the first match. Still O(n) within segments, but returns newer data faster.
3. **Update**: Appends a new record to the active segment with the updated value. Generates duplicate keys across segments.
4. **Delete**: Appends a tombstone record (`null` value) to mark the key as deleted.

## Improvements

Version 2 left us with 3 tradeoffs: total db size, duplicate keys and slow reads. With version 3 we are now solving the first two.

**Segmenting** allows us to keep db file sizes constrained. On it's own it just allows us to have split files but this means accessing data is often faster since there's less to look through.

On top of that, **compaction** of segments allow us to prune the extra records caused by our append-only approach. Unused bloat will get removed and it will reduce the total db size.
