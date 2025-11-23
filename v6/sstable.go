package v6

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type SSTableWriter struct {
	file				*os.File
	writer			*bufio.Writer
	dataOffset	int64
	index				[]IndexEntry
	bloom				*BloomFilter
	minKey			[]byte
	maxKey			[]byte
}

type IndexEntry struct {
	Key    []byte
	Offset int64
	Size   int64
}

type SSTableReader struct {
	Path         	string
	file         	*os.File
	indexOffset  	int64
	indexSize    	int64
	bloomOffset  	int64
	bloomSize    	int64
	index        	[]IndexEntry
	bloom        	*BloomFilter
	minKey       	[]byte
	maxKey       	[]byte
	Id						int
}

type FooterMetadata struct {
	IndexOffset int64
	IndexSize   int64
	BloomOffset int64
	BloomSize   int64
	Magic       string
	MinKey      []byte
	MaxKey      []byte
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
		minKey:     []byte{},
		maxKey:     []byte{},
	}, nil
}

// Keys to be added in sorted order
func (w *SSTableWriter) Append(key, value []byte) error {
	// Udate min and max keys
	if len(w.minKey) == 0 {
		w.minKey = append([]byte(nil), key...)
	}
	w.maxKey = append([]byte(nil), key...)

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
	footer := fmt.Sprintf(
		"\n--- FOOTER ---\n" +
		"index_offset:%d\n" +
		"index_size:%d\n" +
		"bloom_offset:%d\n" +
		"bloom_size:%d\n" +
		"min_key:%s\n" +
		"max_key:%s\n" +
		"magic:SST1\n",
		indexOffset, indexSize, bloomOffset, bloomSize, w.minKey, w.maxKey)
	
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

func LoadSSTable(path string) (*SSTableReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	
	fileSize := stat.Size()
	
	// Read the last 256 bytes, the footer should be smaller
	seekPos := fileSize - int64(256)
	if seekPos < 0 {
		seekPos = 0
	}
	if _, err := file.Seek(seekPos, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to footer: %w", err)
	}

	footerBytes := make([]byte, fileSize - seekPos)
	if _, err := io.ReadFull(file, footerBytes); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	footer, err := parseFooter(footerBytes)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to parse footer: %w", err)
	}

	reader := &SSTableReader{
		Path: path,
		file: file,
		indexOffset: footer.IndexOffset,
		indexSize: footer.IndexSize,
		bloomOffset: footer.BloomOffset,
		bloomSize: footer.BloomSize,
		index: make([]IndexEntry, 0),
		minKey: footer.MinKey,
		maxKey: footer.MaxKey,
	}

	if err := reader.loadBloomFilter(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to load bloom filter: %w", err)
	}

	if err := reader.loadIndex(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to load index: %w", err)
	}
	return reader, nil
}

func parseFooter(data []byte) (*FooterMetadata, error) {
	footerMarker := []byte("\n--- FOOTER ---\n")
	idx := bytes.Index(data, footerMarker)
	if idx == -1 {
		return nil, fmt.Errorf("footer marker not found")
	}

	footerContent := string(data[idx + len(footerMarker):])
	
	var metadata FooterMetadata
	scanner := bufio.NewScanner(strings.NewReader(footerContent))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		
		switch parts[0] {
		case "index_offset":
			fmt.Sscanf(parts[1], "%d", &metadata.IndexOffset)
		case "index_size":
			fmt.Sscanf(parts[1], "%d", &metadata.IndexSize)
		case "bloom_offset":
			fmt.Sscanf(parts[1], "%d", &metadata.BloomOffset)
		case "bloom_size":
			fmt.Sscanf(parts[1], "%d", &metadata.BloomSize)
		case "min_key":
			metadata.MinKey = []byte(parts[1])
		case "max_key":
			metadata.MaxKey = []byte(parts[1])
		case "magic":
			metadata.Magic = parts[1]
			if metadata.Magic != "SST1" {
				return nil, fmt.Errorf("invalid magic number: %s", metadata.Magic)
			}
		}
	}
	return &metadata, nil
}

func (r *SSTableReader) loadBloomFilter() error {
	if r.bloomOffset == 0 || r.bloomSize == 0 {
		return nil
	}

	if _, err := r.file.Seek(r.bloomOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to bloom filter: %w", err)
	}

	bloomData := make([]byte, r.bloomSize)
	if _, err := io.ReadFull(r.file, bloomData); err != nil {
		return fmt.Errorf("failed to read bloom filter: %w", err)
	}
	r.bloom = UnmarshalBloomFilter(bloomData)
	return nil
}

func (r *SSTableReader) loadIndex() error {
	if r.indexOffset == 0 || r.indexSize == 0 {
		return nil
	}

	if _, err := r.file.Seek(r.indexOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to index: %w", err)
	}

	indexData := make([]byte, r.indexSize)
	if _, err := io.ReadFull(r.file, indexData); err != nil {
		return fmt.Errorf("failed to read index: %w", err)
	}
	r.index = make([]IndexEntry, 0)
	scanner := bufio.NewScanner(bytes.NewReader(indexData))

	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.SplitN(line, "@", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		var offset, size int64
		if _, err := fmt.Sscanf(parts[1], "%d:%d", &offset, &size); err != nil {
			continue
		}
		r.index = append(r.index, IndexEntry{
			Key:    []byte(key),
			Offset: offset,
			Size:   size,
		})
	}
	return nil
}

func (r *SSTableReader) Get(key []byte) ([]byte, error) {
	// Range check
	if bytes.Compare(key, r.minKey) < 0 || bytes.Compare(key, r.maxKey) > 0 {
		return nil, fmt.Errorf("key is out of range")
	}

	// Check bloom filter
	if r.bloom != nil && !r.bloom.MayContain(key) {
		return nil, fmt.Errorf("key not found")
	}

	
	// Search the nearest entry in the idx
	idx := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].Key, key) >= 0
	})

	// Sparse index navigation
	if idx >= len(r.index) {
		// Past the end, start from last entry
		idx = len(r.index) - 1
	}
	if idx > 0 && bytes.Compare(r.index[idx].Key, key) > 0 {
		// Key is between index entries, go back one
		idx--
	}

	startOffset := r.index[idx].Offset
	var endOffset int64
	if idx+1 < len(r.index) {
		endOffset = r.index[idx+1].Offset
	} else {
		endOffset = r.indexOffset
	}

	sr := io.NewSectionReader(r.file, startOffset, endOffset - startOffset)
	scanner := bufio.NewScanner(sr)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		if parts[0] == string(key) {
			return []byte(parts[1]), nil
		}

		// If we passed it, it doesnt exist since the file is sorted
		if parts[0] > string(key) {
			break
		}
	}
	return nil, fmt.Errorf("key not found")
}

// Reads all KV pairs from the segment still reads the entire file line by line
// this will be used when compacting segments after rotation
func (r *SSTableReader) ReadAllRecords() (map[string][]byte, error) {
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	entries := make(map[string][]byte)
	scanner := bufio.NewScanner(r.file)
	
	for scanner.Scan() {
		line := scanner.Text()
		
		// Stop at index marker
		if strings.HasPrefix(line, "\n--- INDEX ---") || strings.HasPrefix(line, "--- INDEX ---") {
			break
		}
		
		// Skip empty lines
		if line == "" {
			continue
		}
		
		// Parse key:value
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		
		entries[parts[0]] = []byte(parts[1])
	}
	
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading entries: %w", err)
	}
	
	return entries, nil
}

func (r *SSTableReader) Close() error {
	return r.file.Close()
}