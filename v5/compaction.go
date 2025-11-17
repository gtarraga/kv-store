package v5

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func (sm *SegmentManager) compactionWorker() {
	defer sm.compactionDone.Done()

	for {
		select {
		case <-sm.stopCompaction:
			return
		case req := <-sm.compactCh:
			if err := sm.compactSegment(req.segment, req.tombstoneValue); err != nil {
				fmt.Printf("Compaction error for segment %d: %v\n", req.segment.Id, err)
			}
		}
	}
}

// This function crossreferences the segment index with the log's values
// it removes the deleted KVs (tombstone values) and writes a temp file
// finally it replaces the original files with the newly compacted temp files
func (sm *SegmentManager) compactSegment(seg *Segment, tombstoneValue string) error {
	file, err := os.Open(seg.Path)
	if err != nil {
		return fmt.Errorf("could not open segment %d for compaction: %w", seg.Id, err)
	}
	defer file.Close()

	// Filters the segment entries against the global index and skips tombstones
	seg.mu.RLock()
	compactedRecords := make(map[string]string)
	for key, offset := range seg.Index {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			fmt.Printf("Compaction seek error for segment %d, key %s: %v\n", seg.Id, key, err)
			continue
		}

		line, err := bufio.NewReader(file).ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Compaction read error for segment %d, key %s: %v\n", seg.Id, key, err)
				continue
			}
		}

		k, value, err := parseEntry(line)
		if err != nil || k != key {
			continue // Skip corrupted
		}

		// Skip tombstones
		if value != tombstoneValue {
			compactedRecords[key] = value
		}
	}
	seg.mu.RUnlock()


	// Write compacted records back
	newOffsets, err := seg.WriteRecords(compactedRecords)
	if err != nil {
		return fmt.Errorf("write compacted segment: %w", err)
	}

	// Update memory index 
	seg.mu.Lock()
	seg.Index = newOffsets
	seg.mu.Unlock()	

	// Save updated index file
	if err := seg.SaveIndex(); err != nil {
		return  fmt.Errorf("failed to save index after compaction: %w", err)
	}

	// Back to readonly perms
	if err := seg.SetReadOnly(); err != nil {
		fmt.Printf("Failed to set segment %d to readonly: %v\n", seg.Id, err)
	}

	return nil
}