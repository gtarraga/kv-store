package v4

import (
	"fmt"
)

func (sm *SegmentManager) compactionWorker() {
	defer sm.compactionDone.Done()

	for {
		select {
		case <-sm.stopCompaction:
			return
		case req := <-sm.compactCh:
			if err := sm.compactSegment(req.segmentID); err != nil {
				fmt.Printf("Compaction error for segment %d: %v\n", req.segmentID, err)
			}
		}
	}
}

// This function loads the old segment records onto an in-memory hashmap to deduplicate
// after that, it removes the deleted KVs (tombstone values) and writes a temp file
// finally it replaces the original file with the newly compacted temp file
func (sm *SegmentManager) compactSegment(segmentId int) error {
	seg := NewSegment(sm.DataDir, segmentId)

	// Read all records from the segment (already handles deduplication)
	keyValues, err := seg.ReadAllRecords()
	if err != nil {
		return fmt.Errorf("read segment: %w", err)
	}

	// Write compacted records back
	if err := seg.WriteRecords(keyValues); err != nil {
		return fmt.Errorf("write compacted segment: %w", err)
	}

	// Back to readonly perms
	if err := seg.SetReadOnly(); err != nil {
		fmt.Printf("Failed to set segment %d to readonly: %v\n", segmentId, err)
	}

	return nil
}