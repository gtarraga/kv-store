package v5

import (
	"fmt"
	"os"
)

func (sm *SegmentManager) mergerWorker() {
	defer sm.mergerDone.Done()

	for {
		select {
		case <-sm.stopMerger:
			return
		case segmentsToMerge := <-sm.mergeCh:
			if err := sm.runMergeCycle(0, segmentsToMerge); err != nil {
				fmt.Printf("Compaction error for segments %v\n", err)
			}
		}
	}
}

// Handles merge for a tier and checks if we need to cascade into merging the next tier
func (sm *SegmentManager) runMergeCycle(level int, segmentsToMerge []*Segment) error {
	// Guarding against merging on our max level
	if level >= sm.maxLevels {
		return nil
	}

	newMergedSegment, err := sm.performMerge(level, segmentsToMerge)
	if err != nil {
		return fmt.Errorf("failed merge IO for tier %d: %w", level, err)
	}
	
	// Locking to handle manifest (our single source of truth)
	sm.mu.Lock()
	
	nextLevel := level + 1
	
	// Make sure next tier exists
	for len(sm.tiers) <= nextLevel {
		sm.tiers = append(sm.tiers, Tier{Level: nextLevel, Segments: []*Segment{}})
	}
	
	// APPEND merged segment, the newest in the next tier
	sm.tiers[nextLevel].Segments = append(sm.tiers[nextLevel].Segments, newMergedSegment)

	// Atomic commit of the new state to the manifest
	manifest := sm.buildManifestFromState()
	if err := sm.writeManifest(manifest); err != nil {
		sm.mu.Unlock()
		return fmt.Errorf("failed to commit merge for tier %d: %w", level, err)
	}

	// Check if this merge will fill next tier, we cascade into another merge if it does
	var nextSegmentsToMerge []*Segment
	if len(sm.tiers[nextLevel].Segments) >= sm.mergeThreshold {
		nextSegmentsToMerge = sm.tiers[nextLevel].Segments
		sm.tiers[nextLevel].Segments = []*Segment{} // Clean up tier for the cascading merge
	}

	sm.mu.Unlock() // Manifest is OK

	// Cleanup old fles
	for _, seg := range segmentsToMerge {
		os.Remove(seg.Path)
		os.Remove(seg.IndexPath())
	}

	// If we cascaded, run another merge with the newer segments
	if nextSegmentsToMerge != nil && level < sm.maxLevels {
		return sm.runMergeCycle(nextLevel, nextSegmentsToMerge)
	}

	return nil
}

// Goes over each segment and writes a new merged and compacted segment
func (sm *SegmentManager) performMerge(level int, segments []*Segment) (*Segment, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("cannot merge zero segments")
	}

	// Starts from oldest seg and overwrites with the newer seg entries
	// Deduplicates and merges the data
	// Tombstones act as a "shield" in upper tiers, they block us from going down a tier 
	// to search for a missing key. This wont be a problem in v6 when we add bloom filters
	mergedData := make(map[string]string)
	for _, seg := range segments {
		entries, err := seg.ReadAllEntries()
		if err != nil {
			return nil, fmt.Errorf("could not read entries from segment %d: %w", seg.Id, err)
		}
		for _, entry := range entries {
			mergedData[entry.Key] = entry.Value
		}
	}

	// New segment for the merged data
	sm.mu.Lock()
	newMergedSegment, err := sm.CreateSegment()
	sm.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("could not create new merged segment: %w", err)
	}

	// Write records and index
	newOffsets, err := newMergedSegment.WriteRecords(mergedData)
	if err != nil {
		return nil, fmt.Errorf("could not write records to new segment: %w", err)
	}
	newMergedSegment.Index = newOffsets
	if err := newMergedSegment.SaveIndex(); err != nil {
		return nil, fmt.Errorf("could not save index for new segment: %w", err)
	}
	newMergedSegment.SetReadOnly()

	return newMergedSegment, nil
}