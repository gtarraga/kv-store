# KV Database from Scratch

Building a key-value database from the ground up, starting simple and adding complexity step by step.

I wanted to explore implementing the different storage systems; building optimizations on top of the previous version. I thought it'd make a good exercise to understand how databases work under the hood.

Got inspired while reading [Designing Data-Intensive Applications](https://dataintensive.net/) by Martin Kleppmann (specifically Chapter 3 "Data Structures that Power Your Database"). Highly recommend!

## How it works

Each version (`v1`, `v2`, etc.) is a completely independent implementation with its own tradeoffs:

- **v1**: Basic disk-only storage. Every operation hits the file. Slow but simple.

Each version lives in its own folder with its own README explaining the approach.

## Usage

Build and run:

```bash
go build -o kvdb
```

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

Commands: `add`, `search`, `update`, `delete`

## Project structure

```
kv-database/
├── main.go              # TUI interface (version selector + REPL)
└── <db-version>/        # DB version directory
    ├── <db-version>.go  # DB version runnable
    ├── data/            # Data directory
    │   ├── segment-001.txt
    │   └── db.txt
    └── <other files>
```
