package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	lsm "github.com/devfiqi/lsm-engine"
	"github.com/devfiqi/lsm-engine/bloom"
)

// Reader opens an SSTable for point lookups.
type Reader struct {
	f     *os.File
	index []indexEntry
	bf    *bloom.Filter // nil if the file has no bloom section
	size  int64
}

/*
Opens an SSTable file, loads its sparse index and bloom filter into memory.
*/
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := info.Size()

	// read footer from the last FooterSize bytes
	footer := make([]byte, FooterSize)
	if _, err := f.ReadAt(footer, size-FooterSize); err != nil {
		f.Close()
		return nil, err
	}

	if binary.LittleEndian.Uint64(footer[32:40]) != Magic {
		f.Close()
		return nil, lsm.ErrCorrupted
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexSize := int64(binary.LittleEndian.Uint64(footer[8:16]))
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[16:24]))
	bloomSize := int64(binary.LittleEndian.Uint64(footer[24:32]))

	index, err := readIndex(f, indexOffset, indexSize)
	if err != nil {
		f.Close()
		return nil, err
	}

	var bf *bloom.Filter
	if bloomSize > 0 {
		bloomData := make([]byte, bloomSize)
		if _, err := f.ReadAt(bloomData, bloomOffset); err != nil {
			f.Close()
			return nil, err
		}
		bf = bloom.Decode(bloomData)
	}

	return &Reader{f: f, index: index, bf: bf, size: size}, nil
}

func readIndex(f *os.File, offset, size int64) ([]indexEntry, error) {
	raw := make([]byte, size)
	if _, err := f.ReadAt(raw, offset); err != nil {
		return nil, err
	}
	r := bytes.NewReader(raw)

	var numEntries uint32
	if err := binary.Read(r, binary.LittleEndian, &numEntries); err != nil {
		return nil, err
	}

	entries := make([]indexEntry, numEntries)
	for i := range entries {
		var klen uint32
		if err := binary.Read(r, binary.LittleEndian, &klen); err != nil {
			return nil, err
		}
		key := make([]byte, klen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, err
		}
		var blockOffset, blockSize uint64
		binary.Read(r, binary.LittleEndian, &blockOffset)
		binary.Read(r, binary.LittleEndian, &blockSize)
		entries[i] = indexEntry{firstKey: key, offset: int64(blockOffset), size: int64(blockSize)}
	}
	return entries, nil
}

/*
Looks up key using the bloom filter then the sparse index to find the right block.
Returns value, whether it is a tombstone, and any error.
*/
func (r *Reader) Get(key []byte) (value []byte, tomb bool, err error) {
	// bloom filter eliminates most unnecessary block reads
	if r.bf != nil && !r.bf.MayContain(key) {
		return nil, false, lsm.ErrKeyNotFound
	}

	// binary search the sparse index for the last block whose firstKey <= key
	blockIdx := r.findBlock(key)
	if blockIdx < 0 {
		return nil, false, lsm.ErrKeyNotFound
	}

	return r.scanBlock(key, r.index[blockIdx])
}

// findBlock binary-searches the index for the last entry with firstKey <= key.
func (r *Reader) findBlock(key []byte) int {
	lo, hi, result := 0, len(r.index)-1, -1
	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(r.index[mid].firstKey, key)
		if cmp <= 0 {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result
}

// scanBlock reads the block and scans for key linearly.
func (r *Reader) scanBlock(key []byte, e indexEntry) ([]byte, bool, error) {
	block := make([]byte, e.size)
	if _, err := r.f.ReadAt(block, e.offset); err != nil {
		return nil, false, err
	}

	br := bytes.NewReader(block)
	var hdr [9]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			break
		}
		recType := hdr[0]
		klen := binary.LittleEndian.Uint32(hdr[1:5])
		vlen := binary.LittleEndian.Uint32(hdr[5:9])

		k := make([]byte, klen)
		if _, err := io.ReadFull(br, k); err != nil {
			break
		}
		v := make([]byte, vlen)
		if vlen > 0 {
			if _, err := io.ReadFull(br, v); err != nil {
				break
			}
		}

		if bytes.Equal(k, key) {
			return v, recType == RecordTypeDelete, nil
		}
		// keys are sorted; stop if we've passed the target
		if bytes.Compare(k, key) > 0 {
			break
		}
	}
	return nil, false, lsm.ErrKeyNotFound
}

/*
Closes the underlying file handle.
*/
func (r *Reader) Close() error {
	return r.f.Close()
}
