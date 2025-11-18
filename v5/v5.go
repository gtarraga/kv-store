package v5

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const TOMBSTONE_VALUE = "null"
const MAX_LEVEL = 2

type V5Store struct {
	mu							sync.RWMutex
	dataDir					string

	// Flattened slice of all segments from all tiers
	// Ordered newest to oldest to read faster
	readSegments		[]*Segment

	activeSegment		*Segment
	maxSegmentSize	int64
	manager					*SegmentManager
}

func NewV5Store() *V5Store {
	dataDir := filepath.Join("v5", "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
    panic(fmt.Sprintf("failed to create data directory: %v", err))
	}

	manager := NewSegmentManager(dataDir)

	store := &V5Store{
		dataDir:						dataDir,
		maxSegmentSize:			75, // Small max size ~8 lines
		manager: 						manager,
	}

	manager.onMergeComplete = func(tiers []Tier, activeSegment *Segment) {
		store.updateReadView(tiers, activeSegment)
	}

	tierSegments, activeSegment, err := manager.InitState()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize db state: %v", err))
	}

	// If theres no existing segments we create an empty segment file and a manifest file
	if activeSegment == nil {
		activeSegment, err = manager.CreateSegment(1)
		if err != nil {
			panic(fmt.Sprintf("failed to create initial segment: %v", err))
		}
		manager.WriteInitialManifest(activeSegment)
	}
	store.activeSegment = activeSegment

	// Initialize all indexes for existing segments
	allSegments := append(tierSegments, activeSegment)
	for _, seg := range allSegments {
		if err := seg.LoadIndex(); err != nil {
			panic(fmt.Sprintf("failed to build index for segment %d: %v", seg.Id, err))
		}
	}

	store.updateReadView(manager.GetTiers(), activeSegment)
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
	writeSize := int64(len(key) + len(value) + 2) // key:value\n, the +2 is for ":" and "\n"

	// If current size + new pair is bigger than max, rotate the segment
	if currentSize + writeSize >= s.maxSegmentSize {
		oldSegment := s.activeSegment

		// Send it off to the manager for segment rotation and compaction
		newSegment, err := s.manager.RotateSegment(oldSegment)
		if err != nil {
			return err
		}

		// Update state
		s.activeSegment = newSegment
		// CANT WE JUST APPEND IT TO THE START INSTEAD OF REBUILDING THE WHOLE THING???
		s.readSegments = append(s.readSegments, newSegment)
	}

	return s.activeSegment.Append(key, value)
}

func (s *V5Store) Get(key string) (string, error) {
	s.mu.RLock()
	segments := s.readSegments
	s.mu.RUnlock()



	for _, seg := range segments {
		if value, found := seg.LookupKey(key); found {
			if value == TOMBSTONE_VALUE {
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
	return s.Set(key, TOMBSTONE_VALUE)
}

// Creates a flattened slice from all the segments in
// the db, ordered from newest to oldest to help out reads
func (s *V5Store) updateReadView(tiers []Tier, activeSeg *Segment) {
	newReadSegments := []*Segment{activeSeg}

	// Order all segments from newest to oldest:
	// Tier 0 -> Tier 1 -> Tier 2
	// And from last in the tier array to first, since its newest last
	for _, tier := range tiers {
		for i := len(tier.Segments) - 1; i >= 0; i-- {
			newReadSegments = append(newReadSegments, tier.Segments[i])
		}
	}

	s.mu.Lock()
	s.readSegments = newReadSegments
	s.mu.Unlock()
}

// "key:value\n" parser
func parseEntry(line string) (key, value string, err error) {
	parts := strings.SplitN(strings.TrimSpace(line), ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid entry format")
	}
	return parts[0], parts[1], nil
}
