package v3

import (
	"fmt"
	"os"
)

type SegmentManager struct {
	DataDir string
}

func NewSegmentManager(dataDir string) *SegmentManager {
	return &SegmentManager{DataDir: dataDir}
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