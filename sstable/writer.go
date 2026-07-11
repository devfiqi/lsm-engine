package sstable

import (
	"bufio"
	"encoding/binary"
	"os"

	"github.com/devfiqi/lsm-engine/bloom"
	"github.com/devfiqi/lsm-engine/skiplist"
)

const (
	RecordTypePut    byte = 1
	RecordTypeDelete byte = 2

	targetBlockSize = 4 * 1024 // 4 KB per data block
	FooterSize      = 40       // 5 × uint64
	Magic           = uint64(0x6c736d656e67696e)
)

// Meta describes an on-disk SSTable.
type Meta struct {
	Path   string
	Level  int
	MinKey []byte
	MaxKey []byte
	Size   int64
}

type indexEntry struct {
	firstKey []byte
	offset   int64
	size     int64
}

// Writer encodes a sorted entry stream into an SSTable file.
type Writer struct {
	f           *os.File
	buf         *bufio.Writer
	offset      int64
	index       []indexEntry
	blockOffset int64
	blockBytes  int
	blockOpen   bool
	minKey      []byte
	maxKey      []byte
}

/*
Creates a new SSTable writer at path.
*/
func NewWriter(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, buf: bufio.NewWriterSize(f, 64*1024)}, nil
}

/*
Appends an entry; keys must arrive in sorted ascending order.
*/
func (w *Writer) Append(key, value []byte, tomb bool) error {
	if w.minKey == nil {
		w.minKey = append([]byte(nil), key...)
	}
	w.maxKey = append([]byte(nil), key...)

	if !w.blockOpen {
		w.blockOffset = w.offset
		w.blockOpen = true
		w.blockBytes = 0
		w.index = append(w.index, indexEntry{firstKey: append([]byte(nil), key...), offset: w.blockOffset})
	}

	recType := RecordTypePut
	if tomb {
		recType = RecordTypeDelete
		value = nil
	}

	// type(1) + keylen(4) + vallen(4) + key + value
	var hdr [9]byte
	hdr[0] = recType
	binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(value)))

	for _, b := range [][]byte{hdr[:], key, value} {
		n, err := w.buf.Write(b)
		w.offset += int64(n)
		w.blockBytes += n
		if err != nil {
			return err
		}
	}

	if w.blockBytes >= targetBlockSize {
		w.index[len(w.index)-1].size = w.offset - w.blockOffset
		w.blockOpen = false
	}
	return nil
}

/*
Writes the sparse index and footer, closes the file, and returns table metadata.
*/
func (w *Writer) Finish() (*Meta, error) {
	if w.blockOpen && len(w.index) > 0 {
		w.index[len(w.index)-1].size = w.offset - w.blockOffset
	}

	indexOffset := w.offset
	if err := w.writeIndex(); err != nil {
		return nil, err
	}
	indexSize := w.offset - indexOffset

	// bloom offset/size are 0 until commit 7 integrates the bloom filter
	if err := w.writeFooter(indexOffset, indexSize, 0, 0); err != nil {
		return nil, err
	}
	w.offset += FooterSize

	if err := w.buf.Flush(); err != nil {
		return nil, err
	}
	if err := w.f.Close(); err != nil {
		return nil, err
	}

	return &Meta{
		Path:   w.f.Name(),
		MinKey: w.minKey,
		MaxKey: w.maxKey,
		Size:   w.offset,
	}, nil
}

func (w *Writer) writeIndex() error {
	var numBuf [4]byte
	binary.LittleEndian.PutUint32(numBuf[:], uint32(len(w.index)))
	n, err := w.buf.Write(numBuf[:])
	w.offset += int64(n)
	if err != nil {
		return err
	}
	for _, e := range w.index {
		var klenBuf [4]byte
		binary.LittleEndian.PutUint32(klenBuf[:], uint32(len(e.firstKey)))
		var offBuf, sizBuf [8]byte
		binary.LittleEndian.PutUint64(offBuf[:], uint64(e.offset))
		binary.LittleEndian.PutUint64(sizBuf[:], uint64(e.size))
		for _, b := range [][]byte{klenBuf[:], e.firstKey, offBuf[:], sizBuf[:]} {
			n, err := w.buf.Write(b)
			w.offset += int64(n)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Writer) writeFooter(indexOffset, indexSize, bloomOffset, bloomSize int64) error {
	var footer [FooterSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(indexSize))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(bloomSize))
	binary.LittleEndian.PutUint64(footer[32:40], Magic)
	_, err := w.buf.Write(footer[:])
	return err
}

/*
Flushes a frozen memtable iterator to a new SSTable file at path.
Builds a bloom filter over all keys and embeds it before the footer.
*/
func Flush(path string, iter *skiplist.Iterator, expectedKeys int) (*Meta, error) {
	// collect entries so we can build the bloom filter before writing
	type kv struct {
		key, value []byte
		tomb       bool
	}
	var entries []kv
	for iter.Next() {
		entries = append(entries, kv{
			key:   append([]byte(nil), iter.Key()...),
			value: append([]byte(nil), iter.Value()...),
			tomb:  iter.Tombstone(),
		})
	}

	if expectedKeys <= 0 {
		expectedKeys = len(entries)
	}
	if expectedKeys == 0 {
		expectedKeys = 1
	}

	bf := bloom.New(expectedKeys, 0.01)
	for _, e := range entries {
		bf.Add(e.key)
	}
	bloomBytes := bf.Encode()

	w, err := NewWriter(path)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if err := w.Append(e.key, e.value, e.tomb); err != nil {
			return nil, err
		}
	}
	return w.FinishWithBloom(bloomBytes)
}

/*
Writes the sparse index, bloom filter, and footer, then closes the file.
*/
func (w *Writer) FinishWithBloom(bloomBytes []byte) (*Meta, error) {
	if w.blockOpen && len(w.index) > 0 {
		w.index[len(w.index)-1].size = w.offset - w.blockOffset
	}

	indexOffset := w.offset
	if err := w.writeIndex(); err != nil {
		return nil, err
	}
	indexSize := w.offset - indexOffset

	bloomOffset := w.offset
	n, err := w.buf.Write(bloomBytes)
	w.offset += int64(n)
	if err != nil {
		return nil, err
	}
	bloomSize := int64(len(bloomBytes))

	if err := w.writeFooter(indexOffset, indexSize, bloomOffset, bloomSize); err != nil {
		return nil, err
	}
	w.offset += FooterSize

	if err := w.buf.Flush(); err != nil {
		return nil, err
	}
	if err := w.f.Close(); err != nil {
		return nil, err
	}

	return &Meta{
		Path:   w.f.Name(),
		MinKey: w.minKey,
		MaxKey: w.maxKey,
		Size:   w.offset,
	}, nil
}
