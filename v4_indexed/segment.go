package v4_idx

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Segment struct {
	Id			int
	Path		string
	DataDir	string	
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
	
	_, err = fmt.Fprintf(file, "%s:%s\n", key, value)
	return err
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