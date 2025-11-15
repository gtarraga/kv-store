# Append-only storage

Append-only file-based KV store. Storage files are on-disk and immutable, we only append new lines to the file. This means we never modify or delete existing data, only add new entries.

Updates create a new KV pair at the end of file with the updated value. Deletes also create a new pair but use tombstone records (`null` values) to mark the key as deleted.

## Methods

1. **Add**: simple append to the end of the db file
2. **Search**: scans line by line through the file until it finds the last match. This can be very slow now.
3. **Update**: Appends a new record to the end of the file with the updated value. This generates duplicate keys since the old value remains in the file.
4. **Delete**: Appends a tombstone record for the key to mark it as deleted. The actual data remains in the file but the tombstone indicates the key should be treated as deleted. This implementation uses `null` as a value for the tombstone records.

Compared to version 1, `update` and `delete` are way faster. They now perform as fast as `add`. However, this comes with a couple tradeoffs:

- Since we keep adding lines to our db, the file can get very large.
- Duplicate records make it so we need to read the entire file before returning the value for searches.
- `Update` and `delete` don't care if the record exists since they don't read the file. They will append either a new record regardless.

These things can cause the `search` method to become very slow. Reads are O(n) so the longer the file the longer it will take.
