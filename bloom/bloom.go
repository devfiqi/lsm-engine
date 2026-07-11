package bloom

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// Filter is a space-efficient probabilistic set membership structure.
type Filter struct {
	bits []byte
	m    uint64 // total bits
	k    uint64 // number of hash probes per key
}

/*
Creates a filter sized for n expected keys at false-positive rate p.
*/
func New(n int, p float64) *Filter {
	m := uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2))))
	k := uint64(math.Round(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		k = 1
	}
	return &Filter{bits: make([]byte, (m+7)/8), m: m, k: k}
}

// hashes returns two independent 64-bit hashes used for double-hashing.
func hashes(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write(key)
	h2 := fnv.New64()
	h2.Write(key)
	return h1.Sum64(), h2.Sum64()
}

/*
Adds key to the filter.
*/
func (f *Filter) Add(key []byte) {
	h1, h2 := hashes(key)
	for i := uint64(0); i < f.k; i++ {
		pos := (h1 + i*h2) % f.m
		f.bits[pos/8] |= 1 << (pos % 8)
	}
}

/*
Reports whether key might be in the set. False positives are possible; false negatives are not.
*/
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := hashes(key)
	for i := uint64(0); i < f.k; i++ {
		pos := (h1 + i*h2) % f.m
		if f.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

/*
Serializes the filter to bytes for embedding in an SSTable file.
*/
func (f *Filter) Encode() []byte {
	buf := make([]byte, 16+len(f.bits))
	binary.LittleEndian.PutUint64(buf[0:8], f.m)
	binary.LittleEndian.PutUint64(buf[8:16], f.k)
	copy(buf[16:], f.bits)
	return buf
}

/*
Deserializes a filter from bytes read out of an SSTable file.
*/
func Decode(data []byte) *Filter {
	if len(data) < 16 {
		return nil
	}
	m := binary.LittleEndian.Uint64(data[0:8])
	k := binary.LittleEndian.Uint64(data[8:16])
	bits := make([]byte, len(data)-16)
	copy(bits, data[16:])
	return &Filter{bits: bits, m: m, k: k}
}
