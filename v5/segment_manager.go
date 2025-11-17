package v5

import (
	"fmt"
	"os"
	"sync"
)

type SegmentManager struct {
	DataDir string
	
	// Compaction
	compactCh      	chan compactionRequest
	stopCompaction 	chan struct{}
	compactionDone 	sync.WaitGroup
}

type compactionRequest struct {
	segment					*Segment
	tombstoneValue	string
}

func NewSegmentManager(dataDir string) *SegmentManager {
	sm := &SegmentManager{
		DataDir:        dataDir,
		compactCh:      make(chan compactionRequest, 10), // Up to 10 segments can be queued for compaction
		stopCompaction: make(chan struct{}),
	}
	
	sm.compactionDone.Add(1)
	go sm.compactionWorker()
	
	return sm
}

func (sm *SegmentManager) Close() {
	close(sm.stopCompaction)
	sm.compactionDone.Wait()
}

// Reading the data dir for existing segments
func (sm *SegmentManager) DiscoverSegments() ([]*Segment, error) {
	entries, err := os.ReadDir(sm.DataDir) // Already sorted ascendingly
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	// Building the available segments from the existing files
	// ONly processes files matching the segment_XXXX.db pattern
	var segments []*Segment
	for _, e := range entries {
		name := e.Name()
		var id int
		if n, err := fmt.Sscanf(name, "segment_%d.db", &id); err == nil && n == 1 {
			segments = append(segments, NewSegment(sm.DataDir, id))
		}
	}

	return segments, nil
}

func (sm *SegmentManager) CreateSegment(id int) (*Segment, error) {
	seg := NewSegment(sm.DataDir, id)
	if seg.Exists() {
		return nil, fmt.Errorf("segment %d already exists", id)
	}
	return seg, nil
}

// Creates a new segment and sets the old one to readonly when Set() calls it
func (sm *SegmentManager) RotateSegment(
	oldSegment *Segment,
	tombstoneValue string,
) (*Segment, error) {
	// Create new segment with next id
	newSegment, err := sm.CreateSegment(oldSegment.Id + 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create new segment: %w", err)
	}

	// Make old segment readonly
	if err := oldSegment.SetReadOnly(); err != nil {
		fmt.Printf("Warning: failed to set segment %d readonly: %v\n", oldSegment.Id, err)
	}

	// Save index to file
	// We will be saving it again during compaction since the queue might 
	// be full and we always want an index file for rotated segments
	if err := oldSegment.SaveIndex(); err != nil {
		fmt.Printf("failed to save index for segment  %d: %v", oldSegment.Id, err)
	}

	// Send old segment to the compaction background runner
	select {
	case sm.compactCh <- compactionRequest{
		segment: oldSegment,
		tombstoneValue: tombstoneValue,
	}:
	default:
		// Channel is full so we skip compaction
	}

	return newSegment, nil
}