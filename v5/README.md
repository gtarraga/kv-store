# Tiered Storage with Manifest

An append-only KV store introducing a Tier based Compacted Architecture. This version implements merging segments by organizing them into tiers, managed by the `SegmentManager` and tracked via a `MANIFEST` file.

## Architecture

**Segment Indexes & idx files**: Each segment now has its own separate hash table that tracks where each key is stored. These indexes are also saved as a snapshot on `.idx` companion file with the same name as the segment. They speed up rebuilding the indexes, we don't need to read the db files anymore.

**Merging**: There's multiple merging strategies, this is a tiering strategy. When a segment rotates, we check if theres 4 files in the tier. If there is, we get the 4 segments and merge and compact them into a new segment. We can cascade into lower tier merges.

**Tiers**:

- 3 tiers. From newest files to oldest files, 0 (NEWEST) -> 2 (OLDEST).
- When an active segment gets rotated, it gets placed in the Tier 0, we then decide if we should move that and other segments to a lower tier through merges.
- We merge and compact the files at the lowest tier to deduplicate keys and remove tombstones.
- We track which segment is in which tier with our `MANIFEST` file.

**Manifest File**: Single source of truth that indicates which segments belong to which tier. Makes the db resilient to crashes since it allows it to read the last state.

## Methods

1. **Add**: Appends to the active segment and updates the segment's index with the offset. If segment reaches size threshold, rotate the segment and create new one.

- **Search**: Looks up for the key in the indexes O(1), then reads from the corresponding segment file. But it still needs to go through each index, so O(N) again... but a small N at least.
  1.  Checks the active segment's index.
  2.  Goes through Tier 0 from newest to oldest segments and checks their indexes.
  3.  Checks lower tiers one by one in the same way.
  4.  Returns the first match found.

3. **Update**: Appends a new record to the active segment and updates the index entry. Generates duplicate keys across segments.
4. **Delete**: Appends a tombstone record (`null` value) and updates the index. They eventually get dropped on the lowest tier, only when segments get merged.

## Improvements

**Merging and tiers** reclaims space and reduces search times by reducing the number of files. This also makes sure the oldest data eventually gets compacted.

Adding a `MANIFEST` file and `.idx` compantion files makes **crash recovery** way easier. We know exactly what state the db was in adn we can restore it.

Unfortunately we didnt fix the previous version's issues, I lied lol. We improved startup times since we only need to read the index file, not the actual data but all keys still need to fit into ram.

I expect bloom filters and SSTables to help out next version.
