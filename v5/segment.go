package v5

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Segment struct {
	Id			int
	Path		string
	DataDir	string
	Index		map[string]int64
	mu			sync.RWMutex
}

type SegmentEntry struct {
	Key		 	string
	Value 	string
	Offset	int64
}

func NewSegment(dataDir string, id int) *Segment {
	filename := fmt.Sprintf("segment_%04d.db", id)
	return &Segment{
		Id:				id,
		Path:			filepath.Join(dataDir, filename),
		DataDir:	dataDir,
		Index: 		make(map[string]int64),
	}
}

func (seg *Segment) Exists() bool {
	_, err := os.Stat(seg.Path)
	return err == nil
}

func (seg *Segment) Size() (int64, error) {
	info, err := os.Stat(seg.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

func (seg *Segment) SetReadOnly() error {
	return os.Chmod(seg.Path, 0o444)
}

func (seg *Segment) Append(key, value string) error {
	offset, err := seg.Size()
	if err != nil {
		return err
	}
	
	file, err := os.OpenFile(seg.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	
	_, err = fmt.Fprintf(file, "%s:%s\n", key, value)
	if err!= nil {
		return err
	}

	seg.mu.Lock()
	seg.Index[key] = offset // Here we need to write tombstones too, otherwise we risk reading older entries
	seg.mu.Unlock()

	return nil
}

// Writes all records to the segment file, also used for compaction
func (seg *Segment) WriteRecords(records map[string]string) (map[string]int64, error) {
	tempPath := seg.Path + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	newOffsets := make(map[string]int64)
	var offset int64 = 0

	// Write compacted records to temp file and keep track of new offsets
	for key, value := range records {
		newOffsets[key] = offset
		line := fmt.Sprintf("%s:%s\n", key, value)

		if _, err := tempFile.WriteString(line); err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return nil, fmt.Errorf("write line: %w", err)
		}

		offset += int64(len(line))
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
    return nil, fmt.Errorf("close temp file: %w", err)
    
	}

	// Replace old segment file with the new compacted one
	if err := os.Rename(tempPath, seg.Path); err != nil {
		os.Remove(tempPath)
		return nil, fmt.Errorf("replace segment: %w", err)
	}

	return newOffsets, nil
}

// Reads all KV pairs from the segment and returns the KV pair and the offset
// This is generally used to build indexes
func (seg *Segment) ReadAllEntries() ([]SegmentEntry, error) {
	file, err := os.Open(seg.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return []SegmentEntry{}, nil // Empty segment
		}
		return nil, err
	}
	defer file.Close()
	
	// Hashmap to track each entry info and the current offset
	var entries []SegmentEntry
	var offset int64 = 0

	// Read the entire file and map it onto a hashmap
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if k, v, err := parseEntry(line); err == nil {
			entries = append(entries, SegmentEntry{
				Key: 		k,
				Value: 	v,
				Offset: offset,
			})
		}
		offset += int64(len(line)) + 1 // Make sure to add the newline value ('\n' == 1) to the offset 
	}

	return entries, scanner.Err()
}

// Returns entire line on offset
func (seg *Segment) Read(offset int64) (string, error) {
	file, err := os.Open(seg.Path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Move reader pointer to expected offset in file
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek failed: %w", err)
	}
	
	line, err := bufio.NewReader(file).ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read line at offset %d: %w", offset, err)
	}

	return strings.TrimSpace(line), nil // Returning the line without the newline char
}

func (seg *Segment) LookupKey(key string) (string, bool) {
	seg.mu.RLock()
	defer seg.mu.RUnlock()
	
	offset, found := seg.Index[key]
	if !found {
		return "", false
	}

	// Call line reader on the expected offset
	line, err := seg.Read(offset)
	if err != nil {
		return "", false
	}

	// Make sure it matches the expected key
	k, v, err := parseEntry(line)
	if err != nil || k != key {
		return "", false
	}

	return v, true
}

// ═════════════════════════════════════════════════════
// Segment index management methods
// ═════════════════════════════════════════════════════
func (seg *Segment) IndexPath() string {
	return strings.TrimSuffix(seg.Path, ".db") + ".idx"
}

func (seg *Segment) SaveIndex() error {
	indexPath := seg.IndexPath()

	file, err := os.Create(indexPath)
	if err != nil {
		return fmt.Errorf("create index file: %w", err)
	}
	defer file.Close()

	seg.mu.RLock()
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(seg.Index)
	seg.mu.RUnlock()

	if err != nil {
		return fmt.Errorf("encode index: %w", err)
	}

	return nil
}

// Loads index to memory from idx file
func (seg *Segment) LoadIndex() error {
	indexPath := seg.IndexPath()

	file, err := os.Open(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("index file not found")
		}
		return fmt.Errorf("open index file: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&seg.Index); err != nil {
		// Corrupt index, rebuild from log file
		if rebuildErr := seg.RebuildIndex(); rebuildErr != nil {
			return fmt.Errorf("decode failed and rebuild failed: %w", rebuildErr)
		}
	}

	return nil
}

// Goes over the actual segment log file to build an index in memory
// This is used for the newest segment (it might have been active when process crashed)
func (seg *Segment) RebuildIndex() error {
	seg.Index = make(map[string]int64)

	entries, err := seg.ReadAllEntries()
	if err != nil {
		return fmt.Errorf("read entries: %w", err)
	}

	for _, entry := range entries {
		seg.Index[entry.Key] = entry.Offset
	}

	return nil
}


