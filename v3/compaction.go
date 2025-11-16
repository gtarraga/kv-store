package v3

import (
	"fmt"
)

func (s *V3Store) compactionWorker() {
	defer s.compactionDone.Done()

	for {
		select {
		case <-s.stopCompaction:
			return
		case segmentId := <-s.compactCh:
			if err := s.compactSegment(segmentId); err != nil {
				fmt.Printf("Compaction error for segment %d: %v\n", segmentId, err)
			}
		}
	}
}

// This function loads the old segment records onto an in-memory hashmap to deduplicate
// after that, it removes the deleted KVs (tombstone values) and writes a temp file
// finally it replaces the original file with the newly compacted temp file
func (s *V3Store) compactSegment(segmentId int) error {
	seg := NewSegment(s.dataDir, segmentId)

	// Read all records from the segment (already handles deduplication)
	keyValues, err := seg.ReadAllRecords()
	if err != nil {
		return fmt.Errorf("read segment: %w", err)
	}

	// Remove tombstones (deleted keys)
	// This could be done more efficiently skipping the lines on the write
	// but doing it separately to illustrate the tombstone handling
	for key, value := range keyValues {
		if value == tombstoneValue {
			delete(keyValues, key)
		}
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