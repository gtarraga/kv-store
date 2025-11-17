package v4_idx

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const tombstoneValue = "null"

type SegmentLocation struct {
	SegmentId	int
	Offset		int64
}
type V4IdxStore struct {
	mu             sync.RWMutex
	dataDir        string
	segments       []*Segment
	activeSegment  *Segment
	maxSegmentSize int64
	manager        *SegmentManager
	index					 map[string]SegmentLocation
}

func NewV4Store() *V4IdxStore {
	dataDir := filepath.Join("v4_indexed", "data")
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
	
	store := &V4IdxStore{
		dataDir:						dataDir,
		segments:						segments,
		activeSegment:			activeSegment,
		maxSegmentSize:			75, // Small max size ~8 lines
		manager: 						manager,
		index: 							make(map[string]SegmentLocation),
	}

	if err := store.rebuildGlobalIndex(); err != nil {
		panic(fmt.Sprintf("failed to rebuild index %v", err))
	}

	go store.indexUpdateListener()

	return store
}

func (s *V4IdxStore) Close() error {
	s.manager.Close()
	return nil
}

// Sets a key-value pair in the database by appending to the file
// If the active segment is over the max size, rotate the segment and append on the new one
func (s *V4IdxStore) Set(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentSize, err := s.activeSegment.Size()
	if err != nil {
		currentSize = 0
	}
	
	// Write size of the new KV pair
	writeSize := int64(len(key) + len(value) + 2) // key:value\n

	// Get the current offset, which is actually the size before write
	offset := currentSize

	// If current size + new pair is bigger than max, rotate the segment
	if currentSize + writeSize >= s.maxSegmentSize {
		oldSegment := s.activeSegment

		indexSnapshot := make(map[string]SegmentLocation)
		for k, v := range s.index {
			indexSnapshot[k] = v
		}

		// Send it off to the manager for segment rotation and compaction
		newSegment, err := s.manager.RotateSegment(oldSegment, tombstoneValue, indexSnapshot)
		if err != nil {
			return err
		}

		// Update offset after rotation
		offset = 0

		// Update state
		s.activeSegment = newSegment
		s.segments = append(s.segments, newSegment)
	}

	// Write to file
	if err = s.activeSegment.Append(key, value); err != nil {
		return err
	}

	// Update index after successful write. If tombstone, remove from index
	if value == "null" {
		delete(s.index, key)
	} else {
		s.index[key] = SegmentLocation{
			SegmentId: s.activeSegment.Id,
			Offset: offset,
		} 
	}
	return nil
}

func (s *V4IdxStore) Get(key string) (string, error) {
	s.mu.RLock()
	location, exists := s.index[key]
	s.mu.RUnlock()
	
	if !exists {
		return "", fmt.Errorf("key not found: %s", key)
	}

	// Find the segment
	s.mu.RLock()
	var targetSegment *Segment
	for _, seg := range s.segments {
		if seg.Id == location.SegmentId {
			targetSegment = seg
			break
		}
	}
	s.mu.RUnlock()

	if targetSegment == nil {
		return "", fmt.Errorf("segment %d not found", location.SegmentId)
	}

	// Call line reader on the target segment
	line, err := targetSegment.Read(location.Offset)
	if err != nil {
		return "", err
	}

	// Make sure it matches the expected key
	k, v, err := parseEntry(line)
	if err != nil || k != key {
		return "", fmt.Errorf("corrupted data at offset %d", location.Offset)
	}

	return v, nil
}

// Updates a key-value pair in the database by appending an updated value to the file
func (s *V4IdxStore) Update(key, value string) error {
	return s.Set(key, value)
}

// Deletes a key-value pair in the database by appending a tombstone record (`null` value) to the file
func (s *V4IdxStore) Delete(key string) error {
	return s.Set(key, tombstoneValue)
}

func (s *V4IdxStore) indexUpdateListener() {
	for update := range s.manager.IndexUpdateCh {
		s.mu.Lock()
		for key, offset := range update.NewOffsets {
			s.index[key] = SegmentLocation{
				SegmentId:	update.SegmentId,
				Offset:			offset,
			}
		}
		s.mu.Unlock()
	}
}

func (s *V4IdxStore) rebuildGlobalIndex() error {
	s.index = make(map[string]SegmentLocation)

	for _, seg := range s.segments {
		entries, err := seg.ReadAllEntries()
		if err != nil {
			return fmt.Errorf("failed to index segment %d: %w", seg.Id, err)
		}

		for _, entry := range entries {
			// Older segments might have KV pairs that have been deleted in newer ones
			if entry.Value == tombstoneValue {
        delete(s.index, entry.Key) // Remove from index if tombstone
        continue
    	}
			s.index[entry.Key] = SegmentLocation{
				SegmentId: 	seg.Id,
				Offset: 		entry.Offset,
			}
		}
	}

	return nil
}

// "key:value\n" parser
func parseEntry(line string) (key, value string, err error) {
	parts := strings.SplitN(strings.TrimSpace(line), ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid entry format")
	}
	return parts[0], parts[1], nil
}
