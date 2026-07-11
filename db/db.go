package db

import (
	"fmt"
	"os"
	"sync"

	lsm "github.com/devfiqi/lsm-engine"
	"github.com/devfiqi/lsm-engine/compaction"
	"github.com/devfiqi/lsm-engine/memtable"
	"github.com/devfiqi/lsm-engine/sstable"
	"github.com/devfiqi/lsm-engine/wal"
)

// DB is the top-level storage engine: WAL → memtable → SSTables → compaction.
type DB struct {
	dir      string
	mu       sync.RWMutex
	mem      *memtable.MemTable
	compact  *compaction.Manager
	walSeq   int
	sstSeq   int
	closed   bool
}

/*
Opens or creates a storage engine at dir, replaying the WAL for crash recovery.
*/
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db := &DB{
		dir:     dir,
		compact: compaction.NewManager(dir),
	}

	if err := db.openWAL(); err != nil {
		return nil, err
	}
	return db, nil
}

// openWAL opens the current WAL and replays any existing records into a fresh memtable.
func (db *DB) openWAL() error {
	db.walSeq++
	walPath := fmt.Sprintf("%s/wal-%d.log", db.dir, db.walSeq)

	records, err := wal.Recover(walPath)
	if err != nil {
		return err
	}

	w, err := wal.NewWAL(walPath)
	if err != nil {
		return err
	}
	db.mem = memtable.New(w, 0)

	// replay recovered records into the fresh memtable
	for _, rec := range records {
		switch rec.Type {
		case wal.RecordTypePut:
			db.mem.Put(rec.Entry.Key, rec.Entry.Value)
		case wal.RecordTypeDelete:
			db.mem.Delete(rec.Entry.Key)
		}
	}
	return nil
}

/*
Writes a key-value pair, flushing the memtable to an SSTable if it is full.
*/
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return lsm.ErrEngineClosed
	}
	if err := db.mem.Put(key, value); err != nil {
		return err
	}
	if db.mem.Full() {
		return db.flush()
	}
	return nil
}

/*
Writes a deletion tombstone, flushing the memtable if it is full.
*/
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return lsm.ErrEngineClosed
	}
	if err := db.mem.Delete(key); err != nil {
		return err
	}
	if db.mem.Full() {
		return db.flush()
	}
	return nil
}

/*
Looks up key in the memtable first, then searches SSTables newest-to-oldest.
*/
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, lsm.ErrEngineClosed
	}

	// check active memtable
	if val, found, tomb := db.mem.Get(key); found {
		if tomb {
			return nil, lsm.ErrKeyNotFound
		}
		return val, nil
	}

	// search SSTables from newest level to oldest
	for _, meta := range db.compact.All() {
		r, err := sstable.OpenReader(meta.Path)
		if err != nil {
			continue
		}
		val, tomb, err := r.Get(key)
		r.Close()
		if err == nil {
			if tomb {
				return nil, lsm.ErrKeyNotFound
			}
			return val, nil
		}
		if err != lsm.ErrKeyNotFound {
			return nil, err
		}
	}
	return nil, lsm.ErrKeyNotFound
}

// flush writes the active memtable to disk as an SSTable and rotates the WAL.
func (db *DB) flush() error {
	iter := db.mem.NewIterator()
	db.sstSeq++
	outPath := fmt.Sprintf("%s/sst-0-%d.sst", db.dir, db.sstSeq)

	meta, err := sstable.Flush(outPath, iter, 0)
	if err != nil {
		return err
	}
	if err := db.compact.Add(meta); err != nil {
		return err
	}

	// rotate to a fresh WAL so the old one can be discarded
	return db.openWAL()
}

/*
Flushes the active memtable and syncs the WAL before closing.
*/
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true

	// flush any remaining memtable data
	if db.mem.Size() > 0 {
		if err := db.flush(); err != nil {
			return err
		}
	}
	return nil
}
