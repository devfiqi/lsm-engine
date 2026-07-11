package memtable

import (
	"sync/atomic"

	lsm "github.com/devfiqi/lsm-engine"
	"github.com/devfiqi/lsm-engine/skiplist"
	"github.com/devfiqi/lsm-engine/wal"
)

const DefaultMaxSize = 64 << 20 // 64 MB

type MemTable struct {
	sl      *skiplist.SkipList
	w       *wal.WAL
	size    atomic.Int64
	maxSize int64
}

/*
Creates a MemTable backed by the given WAL, with a configurable size cap.
*/
func New(w *wal.WAL, maxSize int64) *MemTable {
	if maxSize <= 0 {
		maxSize = DefaultMaxSize
	}
	return &MemTable{sl: skiplist.New(), w: w, maxSize: maxSize}
}

/*
Writes the entry to the WAL then inserts it into the skip-list.
*/
func (m *MemTable) Put(key, value []byte) error {
	if err := m.w.Write(lsm.Entry{Key: key, Value: value}); err != nil {
		return err
	}
	m.sl.Set(key, value, false)
	m.size.Add(int64(len(key) + len(value)))
	return nil
}

/*
Writes a tombstone to the WAL then marks the key deleted in the skip-list.
*/
func (m *MemTable) Delete(key []byte) error {
	if err := m.w.Delete(key); err != nil {
		return err
	}
	m.sl.Set(key, nil, true)
	m.size.Add(int64(len(key)))
	return nil
}

/*
Returns the value for a key, whether it was found, and whether it is a tombstone.
*/
func (m *MemTable) Get(key []byte) (value []byte, found bool, tomb bool) {
	return m.sl.Get(key)
}

/*
Returns current memory usage in bytes.
*/
func (m *MemTable) Size() int64 {
	return m.size.Load()
}

/*
Reports whether the MemTable has exceeded its size cap and should be flushed.
*/
func (m *MemTable) Full() bool {
	return m.size.Load() >= m.maxSize
}

/*
Returns a sorted iterator over all entries including tombstones.
*/
func (m *MemTable) NewIterator() *skiplist.Iterator {
	return m.sl.NewIterator()
}
