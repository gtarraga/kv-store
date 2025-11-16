package v3

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Segment struct {
	ID			int
	Path		string
	DataDir	string	
}

type SegmentSearchResult struct {
	Value 	string
	Found 	bool 		// true if key exists in this segment
	Deleted	bool 		// true if found but is a tombstone
}


func NewSegment(dataDir string, id int) *Segment {
	filename := fmt.Sprintf("segment_%04d.db", id)
	return &Segment{
		ID:				id,
		Path:			filepath.Join(dataDir, filename),
		DataDir:	dataDir,
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
	file, err := os.OpenFile(seg.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	
	if _, err = fmt.Fprintf(file, "%s:%s\n", key, value); err != nil {
		return err
	}
	return file.Sync()
}

// Writes all records to the segment file, also used for compaction
func (seg *Segment) WriteRecords(records map[string]string) error {
	tempPath := seg.Path + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	// Write compacted records to temp file 
	for key, value := range records {
		if _, err := fmt.Fprintf(tempFile, "%s:%s\n", key, value); err != nil {
			tempFile.Close()
			os.Remove(tempPath)
			return fmt.Errorf("write record: %w", err)
		}
	}

	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
    return fmt.Errorf("close temp file: %w", err)
    
	}

	// Replace old segment file with the new compacted one
	if err := os.Rename(tempPath, seg.Path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("replace segment: %w", err)
	}

	return nil
}

// Reads all KV pairs from the segment still reads the entire file line by line
// this will be used when compacting segments after rotation
func (seg *Segment) ReadAllRecords() (map[string]string, error) {
	file, err := os.Open(seg.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]string), nil // Empty segment
		}
		return nil, err
	}
	defer file.Close()
	
	// Hashmap to read the segment and deduplicate
	records := make(map[string]string)

	// Read the entire file and map it onto a hashmap
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			records[parts[0]] = parts[1] // Last write wins, deduplicates but keeps tombstones
		}
	}

	return records, scanner.Err()
}

// Basic implementation of a get method on an append only file (like in v2)
// It returns a found and deleted booleans so we can stop searching segments asap
func (seg *Segment) FindKey(key string) (SegmentSearchResult, error) {
	file, err := os.Open(seg.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return SegmentSearchResult{}, nil
		}
		return SegmentSearchResult{}, err
	}
	defer file.Close()

	var foundValue string
	found := false

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 && parts[0] == key {
			foundValue = parts[1]	// Keep track of the last value for this key
			found = true
		}
	}
	if err := scanner.Err(); err != nil {
    return SegmentSearchResult{}, err
	}

  if !found {
		return SegmentSearchResult{}, nil // Not found in this segment
	}
	
	if foundValue == tombstoneValue {
		return SegmentSearchResult{Found: true, Deleted: true}, nil // Found but is a tombstone
	}

	return SegmentSearchResult{Value: foundValue, Found: true}, nil
}