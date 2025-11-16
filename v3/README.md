# Indexed database

Append-only file-based KV store with an in-memory hash index to speed up reads from O(n) to O(1).

## Architecture

**Index**: In-memory hash table that tracks where each key is stored. We save the byte offset for each record in a single log file. This adds some overhead to writes but allows much faster reads.

**Storage**: Single append-only log file. All operations append to this file, and the index points to the location of the latest value for each key.

## Methods

1. **Add**: Appends to the log file and adds the byte offset to the in-memory index.
2. **Search**: Looks up the key in the index O(1), then seeks directly to that byte offset in the file.
3. **Update**: Appends a new record to the log file and updates the index entry with the new offset. Generates duplicate keys in the file.
4. **Delete**: Appends a tombstone record (`null` value) and updates the index.

## Tradeoffs and limitations

With version 3 we've now solved the slow reads that version 2 had. By using an in-memory hash index, we can find any key in O(1) time instead of scanning the entire file.

- File grows unbounded since we never remove old/duplicate records.
- All keys must fit in available RAM since the entire index is in-memory.
- Slow startups, since we lose the index on restart and need to rebuild it by reading the entire log file.
