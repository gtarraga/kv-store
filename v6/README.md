# LSM-Tree Storage with MemTable, SSTables, and Bloom Filters

Append only KV store using an LSM-Tree architecture. Segments are sorted alphabetically by key in sorted string tables (SSTables). These are merged and compacted into different tiers, the lower the tier, the more data and older each file is. Writing is done in a memtable that gets flushed when we reach the max size per segment, then it gets merged with other SSTables if possible. Tiers are tracked via a `MANIFEST` file.

## Architecture

**Memtable**: We no longer use a segment we are actively writing to. We now use an in-memory data structure, a skiplist in this case, that allows us to keep new KVs sorted. It uses a WAL to be crash resilient and it fliushes to an SSTable when the threshold is reached.

**Write Ahead Log (WAL)**: Everytime we add a new value, we write in this file on disk. We keep this file to recover from crashes or similar. If we were to lose our memtable, we can restore it by reading this file.

**SSTables (Sorted String Tables)**: This is what we now use for segments. They are files that store KVs sorted alphabetically. Each SSTable also contains:

- Its own Bloom filter
- Its own Sparse index
- Metadata footer with offsets and min/max keys

Storing sorted KVs allow us to do range queries. This allows us to keep a sparse index that will take up less memory but still allow us to know where to start searching from. If we are looking for `5` and we have `3`, `8` and `23` in our index, we know we could find `5` between `3` and `8`. We keep min/max keys for the same purpose.

**Bloom Filters**: This data structure allows us to know with certainty if a key is NOT in that SSTable. Allows us to skip entire files without reading the file entries.

**LSM Tree Structure**:

- 3 tiers. From newest files to oldest files:
  - Level 0: Newest SSTables (flushed from memtable)
  - Level 1-2: Progressively older, compacted data
- Background compaction merges and deduplicates across levels
- When the memtable gets flushed to an SSTable, it gets placed in the Tier 0, we then decide if we should move that and other SSTables to a lower tier through merges.
- We track which SSTables is in which tier with our `MANIFEST` file.

## Methods

**Add**: Writes to memtable (and WAL). Flushes to SSTable when memtable is full.

**Search**: This is now O(log n)

1. Checks the memtable
2. Goes through Tier 0 from newest to oldest SSTables and does the following:

   - Checks key range. Is this key between the min and max keys?
   - Checks the bloom filter to see if it can skip this table
   - Checks sparse index
   - Reads the file between the nearest entries in the index
   - Returns the first match found.

3. Checks lower tiers one by one in the same way.

**Update**: Does the same as add, but instead of appending, the memtable will overwrite the key in memory (WAL will still append)

**Delete**: Writes tombstone (`null` value).
