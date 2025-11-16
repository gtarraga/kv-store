package v3

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type V3Store struct {
	mu 				sync.RWMutex
	filePath 	string
	index 		map[string]int64
}

func NewV3Store() *V3Store {
	dataDir := filepath.Join("v3", "data")
	os.MkdirAll(dataDir, 0755)

	filePath := filepath.Join(dataDir, "db.txt")
	index := make(map[string]int64)
	if _, err := os.Stat(filePath); err == nil {
		index = rebuildIndex(filePath) // If we have a file at startup, we build the index
	}

	return &V3Store{
		filePath: filePath,
		index: index,
	}
}

func (s *V3Store) Close() error {
	return nil
}

// Appends to the file and updates the index
// For tombstone values, it updates the index instead of deleting the record
func (s *V3Store) Set(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the current offset, which is actually the size before write
	var byteOffset int64 = 0
	info, err := os.Stat(s.filePath)
	if err == nil {
		byteOffset = info.Size()
	}

	file, err := os.OpenFile(s.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write to file
	if _, err = fmt.Fprintf(file, "%s:%s\n", key, value); err != nil {
		return err
	}

	// Update index after successful write. If tombstone, remove from index
	if value == "null" {
		delete(s.index, key)
	} else {
		s.index[key] = byteOffset 
	}
	return nil
}

// Checks index offset, reads that line in file and returns value.
// This will still read the file for deleted records as Set() 
// updates the index instead of removing the key once deleted.
func (s *V3Store) Get(key string) (string, error) {
	s.mu.RLock()
	offset, exists := s.index[key]
	s.mu.RUnlock()
	
	if !exists {
		return "", fmt.Errorf("key not found: %s", key)
	}

	file, err := os.Open(s.filePath)
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

	k, v, err := parseEntry(line)
	if err != nil || k != key {
		return "", fmt.Errorf("corrupted data at offset %d", offset)
	}

	// Handle tombstones
	if v == "null" {
		return "", fmt.Errorf("key not found: %s", key)
	}

	return v, nil
}

// Updates a key-value pair in the database by appending an updated value to the file
func (s *V3Store) Update(key, value string) error {
	return s.Set(key, value)
}

// Deletes a key-value pair in the database by appending a tombstone record (`null` value) to the file
func (s *V3Store) Delete(key string) error {
	return s.Set(key, "null")
}

// Goes over the file to rebuild the index with each key's offset
func rebuildIndex(filePath string) map[string]int64 {
	keyValues := make(map[string]int64)

	file, err := os.Open(filePath)
	if err != nil {
		return keyValues
	}
	defer file.Close()

	var offset int64 = 0	// Track the cumulative offset

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if k, _, err := parseEntry(line); err == nil {
			keyValues[k] = offset	// Store the offset where the record starts
		}
		offset += int64(len(line)) + 1 // Make sure to add the newline value ('\n' == 1) to the offset 
	}

	return keyValues
}

// "key:value\n" parser
func parseEntry(line string) (key, value string, err error) {
	parts := strings.SplitN(strings.TrimSpace(line), ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid entry format")
	}
	return parts[0], parts[1], nil
}