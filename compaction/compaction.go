package compaction

import (
	"bytes"
	"container/heap"
	"fmt"
	"os"
	"sync"

	"github.com/devfiqi/lsm-engine/bloom"
	"github.com/devfiqi/lsm-engine/sstable"
)

const (
	L0Threshold = 4  // merge L0 when it reaches this many SSTables
	sizeRatio   = 10 // each level allows sizeRatio × more SSTables than the level above
)

// Manager tracks SSTables across levels and drives size-tiered compaction.
type Manager struct {
	dir    string
	levels [][]*sstable.Meta
	mu     sync.Mutex
	nextID int
}

/*
Creates a compaction manager rooted at dir.
*/
func NewManager(dir string) *Manager {
	return &Manager{dir: dir, levels: make([][]*sstable.Meta, 1)}
}

/*
Adds a flushed SSTable to level 0 and triggers compaction if the threshold is reached.
*/
func (m *Manager) Add(meta *sstable.Meta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta.Level = 0
	m.levels[0] = append(m.levels[0], meta)
	if len(m.levels[0]) >= L0Threshold {
		return m.compact(0)
	}
	return nil
}

/*
Returns all SSTables across all levels, from newest (highest) to oldest (L0).
*/
func (m *Manager) All() []*sstable.Meta {
	m.mu.Lock()
	defer m.mu.Unlock()
	var all []*sstable.Meta
	for i := len(m.levels) - 1; i >= 0; i-- {
		all = append(all, m.levels[i]...)
	}
	return all
}

// compact merges all SSTables in level l into one SSTable at level l+1.
func (m *Manager) compact(l int) error {
	if len(m.levels[l]) == 0 {
		return nil
	}
	for len(m.levels) <= l+1 {
		m.levels = append(m.levels, nil)
	}

	m.nextID++
	outPath := fmt.Sprintf("%s/sst-%d-%d.sst", m.dir, l+1, m.nextID)

	merged, err := mergeSSTables(outPath, m.levels[l])
	if err != nil {
		return err
	}
	merged.Level = l + 1

	for _, s := range m.levels[l] {
		os.Remove(s.Path)
	}
	m.levels[l] = nil
	m.levels[l+1] = append(m.levels[l+1], merged)

	// propagate compaction upward if next level overflows
	threshold := L0Threshold * pow(sizeRatio, l+1)
	if len(m.levels[l+1]) >= threshold {
		return m.compact(l + 1)
	}
	return nil
}

func pow(base, exp int) int {
	r := 1
	for i := 0; i < exp; i++ {
		r *= base
	}
	return r
}

// heapItem holds one pending entry from a ScanIterator during k-way merge.
type heapItem struct {
	key, value []byte
	tomb       bool
	it         *sstable.ScanIterator
}

type minHeap []*heapItem

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return bytes.Compare(h[i].key, h[j].key) < 0 }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(*heapItem)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

/*
Merges multiple SSTables into one via a k-way heap merge.
When the same key appears in multiple tables the first (lowest-key) occurrence wins,
which is correct when inputs are ordered newest-first.
*/
func mergeSSTables(outPath string, inputs []*sstable.Meta) (*sstable.Meta, error) {
	readers := make([]*sstable.Reader, 0, len(inputs))
	for _, m := range inputs {
		r, err := sstable.OpenReader(m.Path)
		if err != nil {
			return nil, err
		}
		readers = append(readers, r)
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	h := &minHeap{}
	for _, r := range readers {
		it := r.NewScanIterator()
		if it.Next() {
			heap.Push(h, &heapItem{key: it.Key(), value: it.Value(), tomb: it.Tombstone(), it: it})
		}
	}
	heap.Init(h)

	w, err := sstable.NewWriter(outPath)
	if err != nil {
		return nil, err
	}

	var lastKey []byte
	var allKeys [][]byte

	for h.Len() > 0 {
		item := heap.Pop(h).(*heapItem)

		// deduplicate: only write the first occurrence of each key
		if lastKey == nil || !bytes.Equal(item.key, lastKey) {
			lastKey = append([]byte(nil), item.key...)
			allKeys = append(allKeys, lastKey)
			if err := w.Append(item.key, item.value, item.tomb); err != nil {
				return nil, err
			}
		}

		if item.it.Next() {
			heap.Push(h, &heapItem{key: item.it.Key(), value: item.it.Value(), tomb: item.it.Tombstone(), it: item.it})
		}
	}

	// build and embed bloom filter over all merged keys
	bf := bloom.New(max(len(allKeys), 1), 0.01)
	for _, k := range allKeys {
		bf.Add(k)
	}
	return w.FinishWithBloom(bf.Encode())
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
