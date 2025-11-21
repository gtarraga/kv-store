package v6

import (
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bits			[]byte
	numBits		uint32
	numHashes	uint32
	numItems	uint32
}

func NewBloomFilter(n int, fpRate float64) *BloomFilter {
	//  optimal number of bits = -n * ln(p) / (ln(2)^2)
	numBits := uint32(math.Ceil(-float64(n) * math.Log(fpRate) / (math.Log(2) * math.Log(2))))

	// optimal num  of hash functions = (m/n) * ln(2)
	numHashes := uint32(math.Ceil((float64(numBits) / float64(n)) * math.Log(2)))

	if numHashes == 0 {
		numHashes = 1
	}

	return &BloomFilter{
		bits:				make([]byte, (numBits + 7) / 8),
		numBits:		numBits,
		numHashes:	numHashes,
		numItems:		0,
	}
}

// Inserts key into the filter
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bf.hash(key)
	
	for i := uint32(0); i < bf.numHashes; i++ {
		// Double hashing: hash_i(x) = hash1(x) + i * hash2(x)
		pos := (h1 + i*h2) % bf.numBits
		bf.setBit(pos)
	}
	
	bf.numItems++
}

// TRUE if key MIGHT be present (can be false positive)
// FALSE if key is NOT present
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := bf.hash(key)
	
	for i := uint32(0); i < bf.numHashes; i++ {
		pos := (h1 + i*h2) % bf.numBits
		if !bf.getBit(pos) {
			return false
		}
	}
	
	return true
}

func (bf *BloomFilter) hash(key []byte) (uint32, uint32) {
	// First hash: FNV-1a
	h1 := fnv.New32a()
	h1.Write(key)
	hash1 := h1.Sum32()
	
	// Second hash: FNV-1a with different seed
	h2 := fnv.New32a()
	seed := []byte{0xde, 0xad, 0xbe, 0xef}
	h2.Write(seed)
	h2.Write(key)
	hash2 := h2.Sum32()
	
	// Ensure hash2 is odd (for better distribution with double hashing)
	if hash2%2 == 0 {
		hash2++
	}
	
	return hash1, hash2
}

// Sets bit at the given position
func (bf *BloomFilter) setBit(pos uint32) {
	byteIdx := pos / 8
	bitIdx := pos % 8
	bf.bits[byteIdx] |= (1 << bitIdx)
}

// Gets bit at the given position
func (bf *BloomFilter) getBit(pos uint32) bool {
	byteIdx := pos / 8
	bitIdx := pos % 8
	return (bf.bits[byteIdx] & (1 << bitIdx)) != 0
}

// Returns bytes
func (bf *BloomFilter) Size() int {
	return len(bf.bits)
}

func (bf *BloomFilter) NumItems() uint32 {
	return bf.numItems
}

// Calculates the false positive rate
func (bf *BloomFilter) EstimatedFPR() float64 {
	if bf.numItems == 0 {
		return 0
	}
	
	// FPR ≈ (1 - e^(-k*n/m))^k
	k := float64(bf.numHashes)
	n := float64(bf.numItems)
	m := float64(bf.numBits)
	
	return math.Pow(1-math.Exp(-k*n/m), k)
}

// Serializes the Bloom filter to bytes
func (bf *BloomFilter) Marshal() []byte {
	// Format: [numBits:4][numHashes:4][numItems:4][bits...]
	result := make([]byte, 12+len(bf.bits))
	
	result[0] = byte(bf.numBits >> 24)
	result[1] = byte(bf.numBits >> 16)
	result[2] = byte(bf.numBits >> 8)
	result[3] = byte(bf.numBits)
	
	result[4] = byte(bf.numHashes >> 24)
	result[5] = byte(bf.numHashes >> 16)
	result[6] = byte(bf.numHashes >> 8)
	result[7] = byte(bf.numHashes)
	
	result[8] = byte(bf.numItems >> 24)
	result[9] = byte(bf.numItems >> 16)
	result[10] = byte(bf.numItems >> 8)
	result[11] = byte(bf.numItems)
	
	copy(result[12:], bf.bits)
	
	return result
}

// Deserializes a Bloom filter from bytes
func UnmarshalBloomFilter(data []byte) *BloomFilter {
	if len(data) < 12 {
		return nil
	}
	
	numBits := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	numHashes := uint32(data[4])<<24 | uint32(data[5])<<16 | uint32(data[6])<<8 | uint32(data[7])
	numItems := uint32(data[8])<<24 | uint32(data[9])<<16 | uint32(data[10])<<8 | uint32(data[11])
	
	bits := make([]byte, len(data)-12)
	copy(bits, data[12:])
	
	return &BloomFilter{
		bits:      bits,
		numBits:   numBits,
		numHashes: numHashes,
		numItems:  numItems,
	}
}