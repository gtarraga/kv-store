package v5

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const tombstoneValue = "null"

type V5Store struct {
	mu             sync.RWMutex
	dataDir        string
	segments       []*Segment // oldest to newest
	activeSegment  *Segment
	maxSegmentSize int64
	manager        *SegmentManager
}

func NewV5Store() *V5Store {
	dataDir := filepath.Join("v5", "data")
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

	// Initialize all indexes for existing segments
	for _, seg := range segments {
		if err := seg.LoadIndex(); err != nil {
			panic(fmt.Sprintf("failed to build index for segment %d: %v", seg.Id, err))
		}
	}

	// Active segment always rebuilds the index (maybe it crashed)
	activeSegment := segments[len(segments)-1]
	if err := activeSegment.RebuildIndex(); err != nil {
		panic(fmt.Sprintf("failed to build index for active segment: %v", err))
	}
	
	store := &V5Store{
		dataDir:						dataDir,
		segments:						segments,
		activeSegment:			activeSegment,
		maxSegmentSize:			75, // Small max size ~8 lines
		manager: 						manager,
	}

	return store
}

func (s *V5Store) Close() error {
	s.manager.Close()
	return nil
}

// Sets a key-value pair in the database by appending to the file
// If the active segment is over the max size, rotate the segment and append on the new one
func (s *V5Store) Set(key string, value string) error {
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

func (s *V5Store) Get(key string) (string, error) {
	s.mu.RLock()
	segments := s.segments
	s.mu.RUnlock()

	// Search segment indexes from newest to oldest
	for i := len(segments) -1; i >= 0; i-- {
		seg := segments[i]
		if value, found := seg.LookupKey(key); found {
			if value == tombstoneValue {
				return "", fmt.Errorf("key not found: %s", key)
			}
			return value, nil
		}
	}

	return "", fmt.Errorf("key not found: %s", key)
}

// Updates a key-value pair in the database by appending an updated value to the file
func (s *V5Store) Update(key, value string) error {
	return s.Set(key, value)
}

// Deletes a key-value pair in the database by appending a tombstone record (`null` value) to the file
func (s *V5Store) Delete(key string) error {
	return s.Set(key, tombstoneValue)
}

// "key:value\n" parser
func parseEntry(line string) (key, value string, err error) {
	parts := strings.SplitN(strings.TrimSpace(line), ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid entry format")
	}
	return parts[0], parts[1], nil
}
