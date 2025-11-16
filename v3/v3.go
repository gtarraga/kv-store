package v3

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const tombstoneValue = "null"

type V3Store struct {
	mu             sync.RWMutex
	dataDir        string
	segments       []*Segment
	activeSegment  *Segment
	maxSegmentSize int64
	manager        *SegmentManager

	// Compaction
	compactCh      chan int
	stopCompaction chan struct{}
	compactionDone sync.WaitGroup
}

func NewV3Store() *V3Store {
	dataDir := filepath.Join("v3", "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
    panic(fmt.Sprintf("failed to create data directory: %v", err))
	}

	manager := NewSegmentManager(dataDir)
	segments, err := manager.DiscoverSegments()
	if err != nil {
		panic(fmt.Sprintf("failed to discover segments: %v", err))
	}

	// If theres no existing segments we initialize them on 1
	if len(segments) == 0 {
		segments = []*Segment{NewSegment(dataDir, 1)}
	}

	// Active segment should always be the newest one
	activeSegment := segments[len(segments)-1]
	
	store := &V3Store{
		dataDir:						dataDir,
		segments:						segments,
		activeSegment:			activeSegment,
		maxSegmentSize:			75, // Small max size ~8 lines
		manager: 						manager,
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

	currentSize, err := s.activeSegment.Size()
	if err != nil {
		currentSize = 0
	}
	
	// Write size of the new KV pair
	writeSize := int64(len(key) + len(value) + 2) // key:value\n

	// If current size + new pair is bigger than max, rotate the segment
	if currentSize + writeSize >= s.maxSegmentSize {
		if err := s.rotateSegment(); err != nil {
			return err
		}
	}

	return s.activeSegment.Append(key, value)
}

// Reads segments from newest to oldest to find a key
// It still has to read the entire file but segmenting keeps them small
func (s *V3Store) Get(key string) (string, error) {
	s.mu.RLock()
	segments := make([]*Segment, len(s.segments)) // Copying the array to release the lock asap
	copy(segments, s.segments)
	s.mu.RUnlock()

	// Search segments from newest to oldest
	for i := len(segments) - 1; i >= 0; i-- {
		result, err := segments[i].FindKey(key)
		if err != nil {
			return "", err
		}

		if !result.Found {
			continue
		}

		if result.Deleted {
			return "", fmt.Errorf("key not found: %s", key)
		}

		return result.Value, nil
	}

	return "", fmt.Errorf("key not found: %s", key)
}

// Updates a key-value pair in the database by appending an updated value to the file
func (s *V3Store) Update(key, value string) error {
	return s.Set(key, value)
}

// Deletes a key-value pair in the database by appending a tombstone record (`null` value) to the file
func (s *V3Store) Delete(key string) error {
	return s.Set(key, tombstoneValue)
}

func (s *V3Store) Close() error {
	close(s.stopCompaction)
	s.compactionDone.Wait()
	return nil
}

// Handles segment rotation when Set() calls it
func (s *V3Store) rotateSegment() error {
	oldSegment := s.activeSegment

	// Create new segment with next id
	newSegment, err := s.manager.CreateSegment(oldSegment.ID + 1)
	if err != nil {
		return fmt.Errorf("failed to create new segment: %w", err)
	}

	// Update state
	s.activeSegment = newSegment
	s.segments = append(s.segments, newSegment)
	
	// Make old segment readonly
	if err := oldSegment.SetReadOnly(); err != nil {
		fmt.Printf("Warning: failed to set segment %d readonly: %v\n", oldSegment.ID, err)
	}

	// Send old segment to the compaction background runner
	select {
	case s.compactCh <- oldSegment.ID:
	default:
		// Channel is full so we skip compaction
	}
	
	return nil
}