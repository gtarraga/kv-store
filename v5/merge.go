package v5

import (
	"fmt"
	"os"
	"path/filepath"
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
	if level >= MAX_LEVEL {
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
	sm.tiers[nextLevel].Segments = append([]*Segment{newMergedSegment}, sm.tiers[nextLevel].Segments...)

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
		sm.tiers[nextLevel].Segments = []*Segment{}	// Clean up tier for the cascading merge
	}

	currentTiers := sm.tiers
	currentActiveSegment := sm.activeSegment
	sm.mu.Unlock() // Manifest is OK

	// Cleanup old fles
	for _, seg := range segmentsToMerge {
		os.Remove(seg.Path)
		os.Remove(seg.IndexPath())
	}

	// Notify the store to update the read slice with the new state
	if sm.onMergeComplete != nil {
		sm.onMergeComplete(currentTiers, currentActiveSegment)
	}

	// If we cascaded, run another merge with the newer segments
	if nextSegmentsToMerge != nil && level < MAX_LEVEL {
		return sm.runMergeCycle(nextLevel, nextSegmentsToMerge)
	}

	return nil
}


// Goes over each segment and writes a new merged and compacted segment
func (sm *SegmentManager) performMerge(level int, segments []*Segment) (*Segment, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("cannot merge zero segments")
	}

	isMaxLevel := level >= MAX_LEVEL-1

	// Starts from oldest seg and overwrites with the newer seg entries
	// Deduplicates and merges the data. We only remove tombstones if we create a level 2 segment.
	// Tombstones act as a "shield" in upper tiers, they block us from going down
	// a tier to search for a missing key. This wont be a problem in v6 when we add bloom filters
	mergedData := make(map[string]string)
	for _, seg := range segments {
		entries, err := seg.ReadAllEntries()
		if err != nil {
			return nil, fmt.Errorf("could not read entries from segment %d: %w", seg.Id, err)
		}
		for _, entry := range entries {
			isTombstone := entry.Value == TOMBSTONE_VALUE
			
			// Keep tombstones unless we are creating a max level segment
			if isMaxLevel && isTombstone {
				delete(mergedData, entry.Key)
			} else {
				mergedData[entry.Key] = entry.Value
			}
		}
	}

	// Run the creation twice, the first one might fail if the file already exists on disk
	// This could be due to a crash so we delete it and rewrite it
	newestSegmentId := segments[len(segments)-1].Id
	newMergedSegment, err := sm.CreateSegment(newestSegmentId)
	if err != nil {
		if newMergedSegment != nil {
			os.Remove(newMergedSegment.Path)
		} else {
			filename := fmt.Sprintf("segment_%04d.db", newestSegmentId)
			os.Remove(filepath.Join(sm.dataDir, filename))
		}
		
		newMergedSegment, err = sm.CreateSegment(newestSegmentId)
		if err != nil {
			return nil, fmt.Errorf("could not create new merged segment: %w", err)
		}
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