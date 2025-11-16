package v3

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type V3Store struct {
	mu								sync.RWMutex
	segmentIds				[]int
	activeSegmentId		int
	activeSegmentPath	string
	maxSegmentSize		int64
}

func NewV3Store() *V3Store {
	dataDir := filepath.Join("v3", "data")
	os.MkdirAll(dataDir, 0755)

	segmentIds := getExistingSegments(dataDir)

	// If theres no existing files we start the IDs at 1
	if len(segmentIds) == 0 {
		segmentIds = []int{1}
	}

	// Active segment should always be the newest one
	activeSegmentId := segmentIds[len(segmentIds)-1]
	
	return &V3Store{
		segmentIds: segmentIds,
		activeSegmentId: activeSegmentId,
		activeSegmentPath: segmentPath(dataDir, activeSegmentId),
		maxSegmentSize: 75, // Small max size ~8 lines
	}
}

// Sets a key-value pair in the database by appending to the file
// If the active segment is over the max size, rotate the segment and append on the new one
func (s *V3Store) Set(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// File size
	info, err := os.Stat(s.activeSegmentPath)
	currentSize := int64(0)
	if err == nil {
		currentSize = info.Size()
	}
	
	// Write size of the new KV pair
	writeSize := int64(len(key) + len(value) + 2) // key:value\n

	// If current size + new pair is bigger than max, rotate the segment
	if currentSize + writeSize >= s.maxSegmentSize {
		if err := rotateSegment(s); err != nil {
			return err
		}
	}

	// Append on active segment as usual
	file, err := os.OpenFile(s.activeSegmentPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	
	_, err = fmt.Fprintf(file, "%s:%s\n", key, value)
	return err
}

// Reads segments from newest to oldest to find a key
// It still has to read the entire file but segmenting keeps them small
func (s *V3Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()


	// Iterating segments in reverse (newest first)
	for i := len(s.segmentIds) - 1; i >= 0; i-- {
		segmentId := s.segmentIds[i]
		segmentPath := segmentPath(filepath.Dir(s.activeSegmentPath), segmentId)
		
		value, found, deleted, err := searchSegmentForKey(segmentPath, key)
		if err != nil {
			return "", err
		}

		if !found {
			continue
		}

		if deleted {
			return "", fmt.Errorf("key not found: %s", key)
		}

		return value, nil
	}

	return "", fmt.Errorf("key not found: %s", key)
}

// Updates a key-value pair in the database by appending an updated value to the file
func (s *V3Store) Update(key, value string) error {
	return s.Set(key, value)
}

// Deletes a key-value pair in the database by appending a tombstone record (`null` value) to the file
func (s *V3Store) Delete(key string) error {
	return s.Set(key, "null")
}

func (s *V3Store) Close() error {
	return nil
}