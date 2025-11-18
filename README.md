# KV Database from Scratch

Building a key-value database from the ground up, starting simple and adding complexity step by step.

I wanted to explore implementing the different storage systems; building optimizations on top of the previous version. I thought it'd make a good exercise to understand how databases work under the hood.

Got inspired while reading [Designing Data-Intensive Applications](https://dataintensive.net/) by Martin Kleppmann (specifically Chapter 3 "Data Structures that Power Your Database"). Highly recommend!

## How it works

Each version (`v1`, `v2`, etc.) is a completely independent implementation with its own tradeoffs:

- `v1` - **[The Basic One](https://github.com/gtarraga/kv-store/tree/main/v1)**: Basic disk-only storage. Every operation hits the file. Slow but simple.

- `v2` - **[Append only](https://github.com/gtarraga/kv-store/tree/main/v2)**: Storage files are on-disk and immutable, we only append new lines to the file. Updates create a new KV at EOF with the updated value. Deletes also create a new line but we are using tombstone records. Search now looks for the last matching key in the file.

- `v3` - **[It's indexed now](https://github.com/gtarraga/kv-store/tree/main/v3)**: This adds an in-memory hashmap index to keep the location of each of the values. Allows us to read at O(1) instead of O(n).

- `v4` - **[Segmenting and compacting](https://github.com/gtarraga/kv-store/tree/main/v4)**: When db files get too big, we close them and create a new segment. Compaction removes duplicate keys and tombstones for old segments, keeping the size smaller.

- `v4_idx` - **[Indexed Segments](https://github.com/gtarraga/kv-store/tree/main/v4_indexed)**: Added the global index to the segmented db. Searching now checks the index to find the KV's segment file and offset and reads that line directly.

- `v5` - **[Tiering and merges](https://github.com/gtarraga/kv-store/tree/main/v5)**: Each segment has its own index and idx compantion file. Added a manifest file to keep track of the state of segments and tiers. Merges and tiers now compact data and help enhance search speeds since less files.

Each version lives in its own folder with its own README explaining the approach.

## Usage

Build and run:

```bash
go build -o kvdb
```

### Single Version Mode

Interactive mode:

```bash
./kvdb --version v1
```

Or run single commands:

```bash
./kvdb --version v1 add name will
./kvdb --version v1 search name
./kvdb --version v1 update name john
./kvdb --version v1 delete name
```

### Performance Comparison Mode

Compare all versions side-by-side with the `--compare` flag:

Interactive comparison mode:

```bash
./kvdb --compare
```

Or run single command comparisons:

```bash
./kvdb --compare add name will
./kvdb --compare search name
./kvdb --compare update name john
./kvdb --compare delete name
```

This mode runs the same command on all available versions and displays the execution time (in milliseconds) for each version, making it easy to see performance differences between implementations.

Commands: `add`, `search`, `update`, `delete`

## Project structure

```
kv-store/
├── main.go              # TUI interface (version selector + REPL)
└── <db-version>/        # DB version directory
    ├── <db-version>.go  # DB version runnable
    ├── data/            # Data directory
    │   ├── segment-001.txt
    │   └── db.txt
    └── <other files>
```
