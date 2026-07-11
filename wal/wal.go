package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"

	lsm "github.com/devfiqi/lsm-engine"
)

const (
	RecordTypePut    byte = 1
	RecordTypeDelete byte = 2

	// type(1) + keyLen(4) + valLen(4)
	headerSize = 9
)

type WAL struct {
	file *os.File
	buf  *bufio.Writer
}

/*
Opens or creates a WAL file at path, appending to existing records.
*/
func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, buf: bufio.NewWriter(f)}, nil
}

/*
Appends a put record to the WAL with a CRC32 checksum.
*/
func (w *WAL) Write(e lsm.Entry) error {
	return w.append(RecordTypePut, e.Key, e.Value)
}

/*
Appends a delete tombstone record to the WAL.
*/
func (w *WAL) Delete(key []byte) error {
	return w.append(RecordTypeDelete, key, nil)
}

/*
Encodes and buffers a single WAL record: header | key | value | crc32.
*/
func (w *WAL) append(recType byte, key, value []byte) error {
	var header [headerSize]byte
	header[0] = recType
	binary.LittleEndian.PutUint32(header[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(value)))

	// checksum covers the full record payload
	crc := crc32.NewIEEE()
	crc.Write(header[:])
	crc.Write(key)
	crc.Write(value)

	var sum [4]byte
	binary.LittleEndian.PutUint32(sum[:], crc.Sum32())

	for _, b := range [][]byte{header[:], key, value, sum[:]} {
		if _, err := w.buf.Write(b); err != nil {
			return err
		}
	}
	return nil
}

/*
Flushes buffered data and fsyncs to disk.
*/
func (w *WAL) Sync() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

/*
Syncs and closes the WAL file.
*/
func (w *WAL) Close() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

/*
Returns the underlying file path for this WAL.
*/
func (w *WAL) Path() string {
	return w.file.Name()
}
