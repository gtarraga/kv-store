package v4_idx

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
			if err := sm.compactSegment(req.segmentId, req.tombstoneValue, req.indexSnapshot); err != nil {
				fmt.Printf("Compaction error for segment %d: %v\n", req.segmentId, err)
			}
		}
	}
}

// This function loads the old segment records onto an in-memory hashmap to deduplicate
// after that, it removes the deleted KVs (tombstone values) and writes a temp file
// finally it replaces the original file with the newly compacted temp file
func (sm *SegmentManager) compactSegment(segId int, tombstoneValue string, globalIndex map[string]SegmentLocation) error {
	seg := NewSegment(sm.DataDir, segId)

	// Read all records from the segment (already handles deduplication)
	segmentEntries, err := seg.ReadAllEntries()
	if err != nil {
		return err
	}

	// Filters the segment entries against the global index and skips tombstones
	compactedRecords := make(map[string]string)
	for _, entry := range segmentEntries {
		if loc, exists := globalIndex[entry.Key]; exists && loc.SegmentId == segId {
			if entry.Value != tombstoneValue {
				compactedRecords[entry.Key] = entry.Value
			}
		}
	}

	// Write compacted records back
	newOffsets, err := seg.WriteRecords(compactedRecords)
	if err != nil {
		return fmt.Errorf("write compacted segment: %w", err)
	}

	sm.IndexUpdateCh <- IndexUpdate{
		SegmentId: 	segId,
		NewOffsets: newOffsets,
	}

	// Back to readonly perms
	if err := seg.SetReadOnly(); err != nil {
		fmt.Printf("Failed to set segment %d to readonly: %v\n", segId, err)
	}

	return nil
}