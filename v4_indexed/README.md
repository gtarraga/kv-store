# Segmented storage with compaction

Append-only file-based KV store with segmented logs and in-memory hash index. Instead of a single large file, data is split across multiple segment files. Each segment is immutable once closed, and segments can be compacted to remove duplicates and tombstones. The index provides O(1) reads.

## Architecture

**Index**: In memory hash table that tracks where each key is stored. We save the offset and the segment file for each record. This makes writes slower but much faster reads.

**Segments**: Data is split across multiple numbered segment files (e.g., `segment_0001.db`, `segment_0002.db`). When the active segment reaches a size threshold, it's closed and a new active segment is created for writes.

**Compaction**: Background process that rewrites individual segments to reduce disk size. It aims to remove:

- Duplicate keys (keeping only the latest value)
- Tombstone records for deleted keys

## Methods

1. **Add**: Appends to the active segment, adds the offset and segment id to the index. If segment reaches size threshold, close it and create new segment.
2. **Search**: Looks up for the key in the index O(1), then reads from the corresponding segment file.
3. **Update**: Appends a new record to the active segment and updates the index entry. Generates duplicate keys across segments.
4. **Delete**: Appends a tombstone record (`null` value) and updates the index.

## Improvements

With version 4 we are now solving the undbounded file growth that version 2 introduced along with the speedy reads from v3 (index).

**Segmenting** allows us to keep db file sizes constrained. On it's own it just allows us to have split files but this means accessing data is often faster since there's less to look through.

On top of that, **compaction** of segments allow us to prune the extra records caused by our append-only approach. Unused bloat will get removed and it will reduce the total db size.

There's some things that could be improved on this implementation that we will tackle on the next version:

- All keys must fit in the available RAM since the entire index is in-memory.
- Slow startups, since we lose the whole index we need to rebuild it by reading all segments.
