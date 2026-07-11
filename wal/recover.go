package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	lsm "github.com/devfiqi/lsm-engine"
)

// Record pairs a WAL entry with its operation type.
type Record struct {
	Type  byte
	Entry lsm.Entry
}

/*
Replays all valid records from a WAL file, stopping at EOF or a truncated record.
Returns ErrCorrupted if a checksum fails.
*/
func Recover(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		// no WAL file means a clean start
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var records []Record
	r := bufio.NewReader(f)

	for {
		var header [headerSize]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			// clean EOF or partial write at crash boundary — stop replaying
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		recType := header[0]
		keyLen := binary.LittleEndian.Uint32(header[1:5])
		valLen := binary.LittleEndian.Uint32(header[5:9])

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		value := make([]byte, valLen)
		if valLen > 0 {
			if _, err := io.ReadFull(r, value); err != nil {
				if err == io.ErrUnexpectedEOF {
					break
				}
				return nil, err
			}
		}

		var sum [4]byte
		if _, err := io.ReadFull(r, sum[:]); err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		// recompute and verify checksum
		crc := crc32.NewIEEE()
		crc.Write(header[:])
		crc.Write(key)
		crc.Write(value)
		if crc.Sum32() != binary.LittleEndian.Uint32(sum[:]) {
			return nil, lsm.ErrCorrupted
		}

		records = append(records, Record{
			Type:  recType,
			Entry: lsm.Entry{Key: key, Value: value},
		})
	}

	return records, nil
}
