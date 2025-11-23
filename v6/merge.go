package v6

import (
	"fmt"
	"os"
	"path/filepath"
)

func (lsm *LSMManager) mergerWorker() {
	defer lsm.mergerDone.Done()

	for {
		select {
		case <-lsm.stopMerger:
			return
		case sstables := <-lsm.mergeCh:
			// Collect all segments to delete after the merge
			toDelete := make([]*SSTableReader, 0)
			lsm.runMergeCycle(0, sstables, &toDelete)
			
			// Delete old segments
			for _, seg := range toDelete {
				seg.Close()
				os.Remove(seg.Path)
			}
		}
	}
}


// Handles merge for a tier and checks if we need to cascade into merging the next tier
func (lsm *LSMManager) runMergeCycle(level int, segmentsToMerge []*SSTableReader, toDelete *[]*SSTableReader) error {
	newMergedSegment, err := lsm.performMerge(level, segmentsToMerge)
	if err != nil {
		return fmt.Errorf("failed merge IO for tier %d: %w", level, err)
	}
	
	// Locking to handle manifest (our single source of truth)
	lsm.mu.Lock()
	
	// Merge files at bottom level to avoid it becoming a graveyard
	targetLevel := level + 1
	if level >= lsm.maxLevels {
		targetLevel = lsm.maxLevels
	}

	// Make sure next tier exists
	for len(lsm.tiers) <= targetLevel {
		lsm.tiers = append(lsm.tiers, Tier{Level: len(lsm.tiers), Segments: []*SSTableReader{}})
	}
	
	// Remove old segments
	if level < len(lsm.tiers) {
		mergedIDs := make(map[int]bool)
		for _, seg := range segmentsToMerge {
			mergedIDs[seg.Id] = true
		}
		
		// Keep segments that weren't merged
		var remaining []*SSTableReader
		for _, seg := range lsm.tiers[level].Segments {
			if !mergedIDs[seg.Id] {
				remaining = append(remaining, seg)
			}
		}
		lsm.tiers[level].Segments = remaining
	}
	
	// APPEND merged segment, the newest in the next tier
	lsm.tiers[targetLevel].Segments = append(lsm.tiers[targetLevel].Segments, newMergedSegment)

	// Check if this merge will fill next tier, we cascade into another merge if it does
	var nextSegmentsToMerge []*SSTableReader
	if len(lsm.tiers[targetLevel].Segments) >= lsm.mergeThreshold {
		nextSegmentsToMerge = make([]*SSTableReader, len(lsm.tiers[targetLevel].Segments))
		copy(nextSegmentsToMerge, lsm.tiers[targetLevel].Segments)
	}

	// Atomic commit of the new state to the manifest
	manifest := lsm.buildManifestFromState()
	if err := lsm.writeManifest(manifest); err != nil {
		lsm.mu.Unlock()
		return fmt.Errorf("failed to commit merge for tier %d: %w", level, err)
	}

	lsm.mu.Unlock() // Manifest is OK

	// Add segments to delete after the merge and cascading merges
	*toDelete = append(*toDelete, segmentsToMerge...)

	// If we cascaded, run another merge with the newer segments
	if nextSegmentsToMerge != nil && targetLevel < lsm.maxLevels {
		return lsm.runMergeCycle(targetLevel, nextSegmentsToMerge, toDelete)
	}

	return nil
}

// Goes over each segment and writes a new merged and compacted segment
func (lsm *LSMManager) performMerge(level int, segments []*SSTableReader) (*SSTableReader, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("cannot merge zero segments")
	}

	isMaxLevel := level >= lsm.maxLevels

	// Temp memtable for merging
	tempMemTable := NewMemTableWithoutWAL()

	// Read all segments and insert
	for _, seg := range segments {
		entries, err := seg.ReadAllRecords()
		if err != nil {
			return nil, fmt.Errorf("could not read entries from segment %d: %w", seg.Id, err)
		}
		
		for key, value := range entries {
			isTombstone := string(value) == TOMBSTONE_VALUE
		
			// Keep tombstones unless we are merging max level segments
			// At max level, we can remove tombstones
			if isMaxLevel && isTombstone {
				continue // Skip tombstones at max level
			}
			
			// Insert into memtable, will overwrite if key exists
			tempMemTable.InsertWithoutWAL([]byte(key), value)
		}
	}

	sstPath := lsm.CreateSSTablePath()

	// Flush memtable to sst
	if err := tempMemTable.Flush(sstPath); err != nil {
		return nil, fmt.Errorf("could not flush merged data to SSTable: %w", err)
	}

	newSSTable, err := LoadSSTable(sstPath)
	if err != nil {
		return nil, fmt.Errorf("could not load merged SSTable: %w", err)
	}

	filename := filepath.Base(sstPath)
	var id int
	fmt.Sscanf(filename, "sst_%04d.db", &id)
	newSSTable.Id = id

	return newSSTable, nil
}