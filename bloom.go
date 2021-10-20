package go_bloomfilter

import (
	"github.com/spaolacci/murmur3"
	"math"
	"sync"
)

const (
	mod7       = 1<<3 - 1
	bitPerByte = 8
)

type Filter struct {
	lock  *sync.RWMutex
	m     uint64 // bit array of m bits, m will be ceiling to power of 2
	log2m uint64 // log_2 of m
	n     uint64 // number of inserted elements
	k     uint64 // the number of hash function
	keys  []byte // byte array to store hash value
}

// baseHash returns the murmur3 128-bit hash
func baseHash(data []byte) []uint64 {
	a1 := []byte{1}
	hasher := murmur3.New128()
	hasher.Write(data)
	v1, v2 := hasher.Sum128()
	hasher.Write(a1)
	v3, v4 := hasher.Sum128()
	return []uint64{
		v1, v2, v3, v4,
	}
}

// location returns the ith hashed location using the four base hash values
func location(h []uint64, i uint64) uint64 {
	// return h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
	return h[i&1] + i*h[2+(((i+(i&1))&3)/2)]
}

// New is function of creating a bloom filter
// k is number of hash function,
// m is the size of filter
// race is sync or not
func New(size uint64, k uint64) *Filter {
	log2 := uint64(math.Ceil(math.Log2(float64(size))))
	filter := &Filter{
		m:     1 << log2,
		log2m: log2,
		k:     k,
		keys:  make([]byte, 1<<log2),
		lock:  &sync.RWMutex{},
	}
	return filter
}


// Add adds byte array to bloom filter
func (f *Filter) Add(data []byte) *Filter {
	f.lock.Lock()
	defer f.lock.Unlock()
	h := baseHash(data)
	for i := uint64(0); i < f.k; i++ {
		loc := location(h, i)
		slot, mod := f.location(loc)
		f.keys[slot] |= 1 << mod
	}
	f.n++
	return f
}


// Test check if byte array may exist in bloom filter
func (f *Filter) Exists(data []byte) bool {
	f.lock.RLock()
	defer f.lock.RUnlock()
	h := baseHash(data)
	for i := uint64(0); i < f.k; i++ {
		loc := location(h, i)
		slot, mod := f.location(loc)
		if f.keys[slot]&(1<<mod) == 0 {
			return false
		}
	}
	return true
}

// location returns the bit position in byte array
// & (f.m - 1) is the quick way for mod operation
func (f *Filter) location(h uint64) (uint64, uint64) {
	slot := (h / bitPerByte) & (f.m - 1)
	mod := h & mod7
	return slot, mod
}


// AddString adds string to filter
func (f *Filter) AddString(s string) *Filter {
	data := str2Bytes(s)
	return f.Add(data)
}

// TestString if string may exist in filter
func (f *Filter) ExistsString(s string) bool {
	data := str2Bytes(s)
	return f.Exists(data)
}