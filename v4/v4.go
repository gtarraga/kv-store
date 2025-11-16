package v4

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const tombstoneValue = "null"

type V4Store struct {
	mu             sync.RWMutex
	dataDir        string
	segments       []*Segment
	activeSegment  *Segment
	maxSegmentSize int64
	manager        *SegmentManager
}

func NewV4Store() *V4Store {
	dataDir := filepath.Join("v4", "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
    panic(fmt.Sprintf("failed to create data directory: %v", err))
	}

	manager := NewSegmentManager(dataDir)
	segments, err := manager.DiscoverSegments()
	if err != nil {
		panic(fmt.Sprintf("failed to discover segments: %v", err))
	}

	// If theres no existing segments we create an empty segment #1 on disk
	if len(segments) == 0 {
		seg := NewSegment(dataDir, 1)
		file, err := os.Create(seg.Path)
		if err != nil {
			panic(fmt.Sprintf("failed to create initial segment: %v", err))
		}
		file.Close()
		segments = []*Segment{seg}
	}

	// Active segment should always be the newest one
	activeSegment := segments[len(segments)-1]
	
	store := &V4Store{
		dataDir:						dataDir,
		segments:						segments,
		activeSegment:			activeSegment,
		maxSegmentSize:			75, // Small max size ~8 lines
		manager: 						manager,
	}

	return store
}

func (s *V4Store) Close() error {
	s.manager.Close()
	return nil
}

// Sets a key-value pair in the database by appending to the file
// If the active segment is over the max size, rotate the segment and append on the new one
func (s *V4Store) Set(key string, value string) error {
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
		oldSegment := s.activeSegment

		// Send it off to the manager for segment rotation and compaction
		newSegment, err := s.manager.RotateSegment(oldSegment, tombstoneValue)
		if err != nil {
			return err
		}

		// Update state
		s.activeSegment = newSegment
		s.segments = append(s.segments, newSegment)
	}

	return s.activeSegment.Append(key, value)
}

// Reads segments from newest to oldest to find a key
// It still has to read the entire file but segmenting keeps them small
func (s *V4Store) Get(key string) (string, error) {
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
func (s *V4Store) Update(key, value string) error {
	return s.Set(key, value)
}

// Deletes a key-value pair in the database by appending a tombstone record (`null` value) to the file
func (s *V4Store) Delete(key string) error {
	return s.Set(key, tombstoneValue)
}
