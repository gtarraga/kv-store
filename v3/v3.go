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

	// Compaction
	compactCh					chan int
	stopCompaction		chan struct{}
	compactionDone		sync.WaitGroup
}

func NewV3Store() *V3Store {
	dataDir := filepath.Join("v3", "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
    panic(fmt.Sprintf("failed to create data directory: %v", err))
	}

	segmentIds := getExistingSegments(dataDir)

	// If theres no existing files we start the IDs at 1
	if len(segmentIds) == 0 {
		segmentIds = []int{1}
	}

	// Active segment should always be the newest one
	activeSegmentId := segmentIds[len(segmentIds)-1]
	
	store := &V3Store{
		segmentIds:					segmentIds,
		activeSegmentId:		activeSegmentId,
		activeSegmentPath:	segmentPath(dataDir, activeSegmentId),
		maxSegmentSize:			75, // Small max size ~8 lines
		compactCh:					make(chan int, 10), // Up to 10 segments can be queued for compaction
		stopCompaction:			make(chan struct{}),
	}

	store.compactionDone.Add(1)
	go store.compactionWorker()

	return store
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
	
	if _, err = fmt.Fprintf(file, "%s:%s\n", key, value); err != nil {
		return err
	}
	return file.Sync()
}

// Reads segments from newest to oldest to find a key
// It still has to read the entire file but segmenting keeps them small
func (s *V3Store) Get(key string) (string, error) {
	s.mu.RLock()
	// Copying the array to release the lock asap
	segmentIds := make([]int, len(s.segmentIds))
	copy(segmentIds, s.segmentIds)
	dataDir := filepath.Dir(s.activeSegmentPath)
	s.mu.RUnlock()


	// Iterating segments in reverse (newest first)
	for i := len(segmentIds) - 1; i >= 0; i-- {
		segmentPath := segmentPath(dataDir, segmentIds[i])
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
	close(s.stopCompaction)
	s.compactionDone.Wait()
	return nil
}