package v6

import (
	"encoding/binary"
	"fmt"
	"os"
)

type SSTableWriter struct {
	file				*os.File
	dataOffset	int64
	index				[]IndexEntry
	bloom				*BloomFilter
}

type IndexEntry struct {
	Key    []byte
	Offset int64
	Size   int64
}

func NewSSTableWriter(path string, expectedKeys int) (*SSTableWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// 1% false positive rate
	bloom := NewBloomFilter(expectedKeys, 0.01)

	return &SSTableWriter{
		file:       file,
		dataOffset: 0,
		index:      make([]IndexEntry, 0, expectedKeys/100), // Sparse index
		bloom:      bloom,
	}, nil
}

// Keys to be added in sorted order
func (w *SSTableWriter) Append(key, value []byte) error {
	// Add the key to the bloom filter
	w.bloom.Add(key)

	keyLen := uint32(len(key))
	valueLen := uint32(len(value))

	// Write key and key length
	if err := binary.Write(w.file, binary.BigEndian, keyLen); err != nil {
		return err
	}
	if _, err := w.file.Write(key); err != nil {
		return err
	}

	// Write value and key value
	if err := binary.Write(w.file, binary.BigEndian, valueLen); err != nil {
		return err
	}
	if _, err := w.file.Write(value); err != nil {
		return err
	}

	// add to sparse index (every so often for more efficiency)
	if len(w.index) == 0 || len(w.index)%16 == 0 {
		w.index = append(w.index, IndexEntry{
			Key:    append([]byte(nil), key...), // Copy key
			Offset: w.dataOffset,
			Size:   int64(8 + keyLen + valueLen),
		})
	}

	w.dataOffset += int64(8 + keyLen + valueLen)
	return nil
}

func (w *SSTableWriter) Finalize() error {
	dataEnd := w.dataOffset
	
	indexOffset := dataEnd
	indexSize := 0

	// Write index
	for _, entry := range w.index {
		// [keyLen:4][key][offset:8][size:8]
		keyLen := uint32(len(entry.Key))
		
		if err := binary.Write(w.file, binary.BigEndian, keyLen); err != nil {
			return err
		}
		if _, err := w.file.Write(entry.Key); err != nil {
			return err
		}
		if err := binary.Write(w.file, binary.BigEndian, entry.Offset); err != nil {
			return err
		}
		if err := binary.Write(w.file, binary.BigEndian, entry.Size); err != nil {
			return err
		}
		
		indexSize += int(4 + keyLen + 16)
	}

	// Write Bloom filter
	bloomOffset := indexOffset + int64(indexSize)
	bloomData := w.bloom.Marshal()
	
	bloomSize := uint32(len(bloomData))
	if err := binary.Write(w.file, binary.BigEndian, bloomSize); err != nil {
		return err
	}
	if _, err := w.file.Write(bloomData); err != nil {
		return err
	}

	// Footer with metadata
	// [indexOffset:8][indexSize:4][bloomOffset:8][bloomSize:4][magic:4]
	footer := make([]byte, 28)
	
	binary.BigEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.BigEndian.PutUint32(footer[8:12], uint32(indexSize))
	binary.BigEndian.PutUint64(footer[12:20], uint64(bloomOffset))
	binary.BigEndian.PutUint32(footer[20:24], bloomSize)
	
	// Magic number to identify SSTable files
	copy(footer[24:28], []byte("SST1"))


	// Write and sync to disk
	if _, err := w.file.Write(footer); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	
	return w.file.Close()
}

func (w *SSTableWriter) Stats() string {
	return fmt.Sprintf("Data: %d bytes, Index entries: %d, Bloom FPR: %.2f%%",
		w.dataOffset, len(w.index), w.bloom.EstimatedFPR()*100)
}