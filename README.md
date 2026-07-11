# lsm-engine

A write-optimized storage engine built from scratch in Go, implementing the core architecture of production systems like RocksDB and LevelDB. Designed around the Log-Structured Merge-tree (LSM) pattern to maximize write throughput while bounding read amplification through bloom filters and sparse indexes.

---

## Architecture

```
Write path
──────────────────────────────────────────────────────
  Put(key, value)
       │
       ▼
  Write-Ahead Log          ← crash-safe sequential append, CRC32 per record
       │
       ▼
  MemTable (skip-list)     ← concurrent, in-memory sorted structure
       │
       │  (when full, ~64 MB)
       ▼
  SSTable flush            ← sorted 4 KB blocks + sparse index + bloom filter
       │
       ▼
  Compaction Manager       ← size-tiered: k-way heap merge when L0 hits 4 files

Read path
──────────────────────────────────────────────────────
  Get(key)
       │
       ├──► MemTable            (O(log n) skip-list lookup)
       │
       └──► SSTables, newest → oldest
                 │
                 ├── Bloom filter    → skip table with ~99% probability if absent
                 ├── Sparse index    → binary search to the right 4 KB block
                 └── Block scan      → linear scan within block
```

---

## Components

### Write-Ahead Log (`wal/`)
Sequential append-only log for crash recovery. Every write is persisted to disk before touching the in-memory structure. Each record is framed as:

```
[ type: 1B | key_len: 4B | val_len: 4B | key | value | crc32: 4B ]
```

On startup, `Recover()` replays all valid records, stopping cleanly at a truncated record from a mid-write crash.

### MemTable (`memtable/`, `skiplist/`)
An in-memory write buffer backed by a concurrent skip-list. Reads take a shared `RWMutex` lock; writes take an exclusive lock. Deletion writes a tombstone rather than removing the key, ensuring deletes propagate correctly through the LSM levels. When the memtable exceeds 64 MB it is frozen and flushed to disk.

### SSTable (`sstable/`)
Immutable on-disk files storing sorted key-value pairs. Format:

```
┌──────────────────────────────────────┐
│  Data blocks (4 KB each)             │
│  [type:1][keylen:4][vallen:4][k][v]  │
├──────────────────────────────────────┤
│  Sparse index                        │
│  First key + offset + size per block │
├──────────────────────────────────────┤
│  Bloom filter                        │
│  Encoded bit array (1% FPR)          │
├──────────────────────────────────────┤
│  Footer (40 bytes)                   │
│  index_offset | index_size           │
│  bloom_offset | bloom_size | magic   │
└──────────────────────────────────────┘
```

Point lookups: check bloom filter → binary search sparse index → scan one block.

### Bloom Filter (`bloom/`)
Per-SSTable probabilistic set membership filter. Uses double-hashing (FNV-1a + FNV) to simulate k independent hash functions without k separate passes. Sized for a 1% false positive rate, eliminating ~99% of unnecessary disk reads for absent keys.

### Compaction (`compaction/`)
Size-tiered compaction merges SSTables within a level when it accumulates too many files. Uses a k-way min-heap merge to produce a single sorted output SSTable, deduplicating keys and preserving the most recent value. A fresh bloom filter is built over the merged output. Compaction cascades upward through levels automatically.

---

## Usage

```go
package main

import (
    "fmt"
    "github.com/devfiqi/lsm-engine/db"
)

func main() {
    engine, err := db.Open("./data")
    if err != nil {
        panic(err)
    }
    defer engine.Close()

    engine.Put([]byte("hello"), []byte("world"))
    engine.Put([]byte("foo"),   []byte("bar"))

    val, err := engine.Get([]byte("hello"))
    if err == nil {
        fmt.Println(string(val)) // world
    }

    engine.Delete([]byte("foo"))
}
```

---

## Getting Started

```bash
git clone https://github.com/devfiqi/lsm-engine.git
cd lsm-engine
go build ./...
```

Requires Go 1.21+. No external dependencies.

---

## Design Decisions

**Why a skip-list over a red-black tree?**
Skip-lists offer comparable O(log n) average performance with simpler concurrent access patterns. The probabilistic structure makes it straightforward to reason about lock granularity.

**Why size-tiered over leveled compaction?**
Size-tiered compaction minimizes write amplification — ideal for write-heavy workloads. The tradeoff is slightly higher space amplification. Leveled compaction would reduce space amplification at the cost of more frequent, smaller merges.

**Why sparse index over a full index?**
A full index would require loading all keys into memory. The sparse index (one entry per 4 KB block) provides O(log n) block-level lookup with memory proportional to the number of blocks, not the number of keys.

---

## Project Structure

```
lsm-engine/
├── types.go            — Entry, Iterator, Storage interfaces
├── errors.go           — sentinel errors
├── wal/
│   ├── wal.go          — append writes with CRC32
│   └── recover.go      — crash recovery replay
├── skiplist/
│   └── skiplist.go     — concurrent probabilistic sorted structure
├── memtable/
│   └── memtable.go     — in-memory write buffer
├── bloom/
│   └── bloom.go        — probabilistic set membership filter
├── sstable/
│   ├── writer.go       — SSTable encoder and memtable flush
│   └── reader.go       — SSTable decoder with scan iterator
├── compaction/
│   └── compaction.go   — size-tiered k-way merge
└── db/
    └── db.go           — top-level engine interface
```

---

## Implementation Stats

~1,450 lines of Go across 10 commits. No external dependencies — only the Go standard library.
