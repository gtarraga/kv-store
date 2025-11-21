package v6

import (
	"bufio"
	"fmt"
	"os"
)

type SSTableWriter struct {
	file				*os.File
	writer			*bufio.Writer
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

	writer := bufio.NewWriter(file)

	return &SSTableWriter{
		file:       file,
		writer:     writer,
		dataOffset: 0,
		index:      make([]IndexEntry, 0, expectedKeys/100), // Sparse index
		bloom:      bloom,
	}, nil
}

// Keys to be added in sorted order
func (w *SSTableWriter) Append(key, value []byte) error {
	// Add the key to the bloom filter
	w.bloom.Add(key)

	// Plain text: key:value\n
	line := fmt.Sprintf("%s:%s\n", string(key), string(value))
	n, err := w.writer.WriteString(line)
	if err != nil {
		return err
	}

	// add to sparse index (every so often for more efficiency)
	if len(w.index) == 0 || len(w.index)%16 == 0 {
		w.index = append(w.index, IndexEntry{
			Key:    append([]byte(nil), key...), // Copy key
			Offset: w.dataOffset,
			Size:   int64(n),
		})
	}

	w.dataOffset += int64(n)
	return nil
}

// SSTables have the index and bloom filters embedded in the same file, with a metadata footer to find those sections quickly
func (w *SSTableWriter) Finalize() error {
	dataEnd := w.dataOffset
	
	// Ondex section marker
	if _, err := w.writer.WriteString("\n--- INDEX ---\n"); err != nil {
		return err
	}
	indexOffset := dataEnd + 15 // +15 for "\n--- INDEX ---\n"
	indexSize := 0

	// Write index
	for _, entry := range w.index {
		line := fmt.Sprintf("%s@%d:%d\n", string(entry.Key), entry.Offset, entry.Size)
		n, err := w.writer.WriteString(line)
		if err != nil {
			return err
		}
		indexSize += n
	}

	// Bloom filter section marker
	if _, err := w.writer.WriteString("\n--- BLOOM ---\n"); err != nil {
		return err
	}
	bloomOffset := indexOffset + int64(indexSize) + 15 // +15 for "\n--- BLOOM ---\n"
	
	// Bloom filter encoded
	bloomData := w.bloom.Marshal()
	if _, err := w.writer.Write(bloomData); err != nil {
		return err
	}
	bloomSize := len(bloomData)

	// Footer with metadata
	footer := fmt.Sprintf("\n--- FOOTER ---\nindex_offset:%d\nindex_size:%d\nbloom_offset:%d\nbloom_size:%d\nmagic:SST1\n",
		indexOffset, indexSize, bloomOffset, bloomSize)
	
	if _, err := w.writer.WriteString(footer); err != nil {
		return err
	}

	// Flush and sync to disk
	if err := w.writer.Flush(); err != nil {
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