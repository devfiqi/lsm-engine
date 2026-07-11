package lsm

// Entry is a key-value pair written to the engine.
type Entry struct {
	Key   []byte
	Value []byte
}

// Iterator walks over a sorted sequence of entries.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Close() error
}

// Storage defines the read/write surface of the engine.
type Storage interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Close() error
}
