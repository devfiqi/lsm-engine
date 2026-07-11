package skiplist

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel = 16
	p        = 0.25
)

type node struct {
	key   []byte
	value []byte
	tomb  bool // true if this is a deletion tombstone
	next  [maxLevel]*node
}

type SkipList struct {
	head  *node
	level int
	count int
	mu    sync.RWMutex
	rng   *rand.Rand
}

/*
Creates a new empty skip-list.
*/
func New() *SkipList {
	return &SkipList{
		head:  &node{},
		level: 1,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel promotes a new node with probability p up to maxLevel.
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && sl.rng.Float64() < p {
		level++
	}
	return level
}

/*
Inserts or updates a key. Pass tomb=true to write a deletion tombstone.
*/
func (sl *SkipList) Set(key, value []byte, tomb bool) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	var update [maxLevel]*node
	curr := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// update in place if key already exists
	if next := curr.next[0]; next != nil && bytes.Equal(next.key, key) {
		next.value = value
		next.tomb = tomb
		return
	}

	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}

	n := &node{key: key, value: value, tomb: tomb}
	for i := 0; i < level; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	sl.count++
}

/*
Returns the value for a key, whether it exists, and whether it is a tombstone.
*/
func (sl *SkipList) Get(key []byte) (value []byte, found bool, tomb bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
	}

	if n := curr.next[0]; n != nil && bytes.Equal(n.key, key) {
		return n.value, true, n.tomb
	}
	return nil, false, false
}

/*
Returns the number of entries including tombstones.
*/
func (sl *SkipList) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.count
}

// Iterator walks the level-0 linked list in sorted key order.
// Safe to use only when the skip-list is frozen (no concurrent writes).
type Iterator struct {
	curr *node
}

/*
Returns an iterator positioned before the first entry.
*/
func (sl *SkipList) NewIterator() *Iterator {
	return &Iterator{curr: sl.head}
}

/*
Advances to the next entry and reports whether one exists.
*/
func (it *Iterator) Next() bool {
	if it.curr == nil {
		return false
	}
	it.curr = it.curr.next[0]
	return it.curr != nil
}

/*
Reports whether the iterator is on a valid entry.
*/
func (it *Iterator) Valid() bool { return it.curr != nil && it.curr.key != nil }

func (it *Iterator) Key() []byte       { return it.curr.key }
func (it *Iterator) Value() []byte     { return it.curr.value }
func (it *Iterator) Tombstone() bool   { return it.curr.tomb }
