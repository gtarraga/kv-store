# Basic storage

Basic file-based KV store. Stores everything in a plain text file (`db.txt`) with one KV pair per line.

It lacks any of the optimizations that we expect from a KV store nowadays, just data persistence storing it on-disk.

## Methods

1. **Add**: simple append to the end of the db file
2. **Search**: scans line by line through the file until it finds a match
3. **Update**: Searches the db line by line and creates a new temp file. This temp file will copy each line one by one. If it finds a match, that line will get copied over to the temp file but with the new updated value. Once its done, it overwrites the original db file with this new temp file.
4. **Delete**: It does the same as above but skips copying the line to the temp filee if it finds a match.

`Search`, `update` and `delete` are really badly performant since it isn't really leveraging in-memory storage. `Update` and `delete` are specially slow since its actually rewriting an entire file every time we run either of those.
