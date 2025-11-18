package v5

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Manifest struct {
	Tiers					[]ManifestTier	`json:"tiers"`
	ActiveSegment	string					`json:"active_segment"`
}

type ManifestTier struct {
	Level 		int 			`json:"level"`
	Segments	[]string 	`json:"segments"`
}

// Live tiers struct
type Tier struct {
	Level			int
	Segments	[]*Segment
}

type SegmentManager struct {
	dataDir	string
	mu			sync.RWMutex

	// In memory tiers parsed from the manifest
	tiers					[]Tier 		// Contains all rotated segments
	activeSegment	*Segment	// active segment is only here, not in tiers
	maxLevels			int

	// Merging
	mergeThreshold	int
	mergeCh					chan []*Segment
	stopMerger			chan struct{}
	mergerDone			sync.WaitGroup

	// Callback to Store to update the state after merge
	onMergeComplete	func(tiers []Tier, activeSegment *Segment)
}

func NewSegmentManager(dataDir string) *SegmentManager {
	sm := &SegmentManager{
		dataDir:        dataDir,
		mergeCh:      	make(chan []*Segment, 10), // Up to 10 segments can be queued for compaction
		stopMerger:			make(chan struct{}),
		mergeThreshold:	4, // Merge when Tier 0 has 4 segments
		maxLevels: 			MAX_LEVEL,
	}
	
	sm.mergerDone.Add(1)
	go sm.mergerWorker()
	
	return sm
}

func (sm *SegmentManager) Close() {
	close(sm.stopMerger)
	sm.mergerDone.Wait()
}

// Loads the db structure from the MANIFEST file or initializes a new one
func (sm *SegmentManager) InitState() ([]*Segment, *Segment, error) {
	manifestPath := filepath.Join(sm.dataDir, "MANIFEST")
	f, err := os.Open(manifestPath)
	if os.IsNotExist(err) {
		return sm.initializeFromDirectory()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open manifest: %w", err)
	}
	defer f.Close()

	// It exists so we error out
	var manifest Manifest
	if err := json.NewDecoder(f).Decode(&manifest); err != nil {
		return nil, nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	allTierSegments := []*Segment{}
	validFiles := make(map[string]bool)
	for _, mt := range manifest.Tiers {
		tierSegments := []*Segment{}
		for _, segName := range mt.Segments {
			id, _ := parseSegmentID(segName)
			seg := NewSegment(sm.dataDir, id)
			tierSegments = append(tierSegments, seg)
			allTierSegments = append(allTierSegments, seg)
			validFiles[segName] = true
		}
		
		// Build the in memory segments for the segment manager
		sm.tiers = append(sm.tiers, Tier{
			Level: mt.Level,
			Segments: tierSegments,
		})
	}

	var activeSegment *Segment
	if manifest.ActiveSegment != "" {
		id, _ := parseSegmentID(manifest.ActiveSegment)
		activeSegment = NewSegment(sm.dataDir, id)
		validFiles[manifest.ActiveSegment] = true
	}
	sm.activeSegment = activeSegment

	if err := sm.cleanupDirectory(validFiles); err != nil {
		fmt.Printf("failed to reconcile directory: %v\n", err)
	}

	return allTierSegments, activeSegment, nil
}

// Only used when MANIFEST doesn't exist
func (sm *SegmentManager) initializeFromDirectory() ([]*Segment, *Segment, error) {
	segments, err := sm.DiscoverSegments()
	if err != nil {
		return nil, nil, err
	}
	// New db!
	if len(segments) == 0 {
		return nil, nil, nil
	}

	// Assign the last segment as active and add everything else to tierSegments
	activeSegment := segments[len(segments)-1]
	tierSegments := segments[:len(segments)-1]
	sm.activeSegment = activeSegment

	sm.tiers = []Tier{{Level: 0, Segments: tierSegments}}

	// Fallback to writing manifest line by line
	manifest := sm.buildManifestFromState()
	if err := sm.writeManifest(manifest); err != nil {
		return nil, nil, fmt.Errorf("failed to write initial manifest: %w", err)
	}

	return tierSegments, activeSegment, nil
}


// Scans the dir for db files to bootstrap the db
func (sm *SegmentManager) DiscoverSegments() ([]*Segment, error) {
	entries, err := os.ReadDir(sm.dataDir) // Already sorted ascendingly
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
		if id, ok := parseSegmentID(name); ok {
			segments = append(segments, NewSegment(sm.dataDir, id))
		}
	}

	return segments, nil
}

// Removes orphan files not in the manifest
func (sm *SegmentManager) cleanupDirectory(validFiles map[string]bool) error {
	entries, err := os.ReadDir(sm.dataDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !validFiles[name] && (strings.HasSuffix(name, ".db") || strings.HasSuffix(name, ".idx") || strings.HasSuffix(name, ".tmp")) {
			os.Remove(filepath.Join(sm.dataDir, name))
		}
	}
	return nil
}

// Builds a manifest format from the one SegmentManager has in memory
func (sm *SegmentManager) buildManifestFromState() Manifest {
	manifest := Manifest{
		ActiveSegment:	filepath.Base(sm.activeSegment.Path),
		Tiers: 					[]ManifestTier{},
	}

	// TODO: This feels like the same we are doing in InitState but optimized,
	// maybe we should look to optimize that or reuse this code
	for _, tier := range sm.tiers {
		mt := ManifestTier{Level: tier.Level, Segments: []string{}}
		for _, seg := range tier.Segments {
			mt.Segments = append(mt.Segments, filepath.Base(seg.Path))
		}
		manifest.Tiers = append(manifest.Tiers, mt)
	}
	return manifest
}

// Reading a manifest in memory and rewriting it to the file
func (sm *SegmentManager) writeManifest(manifest Manifest) error {
	tempPath := filepath.Join(sm.dataDir, "MANIFEST.tmp")
	filePath := filepath.Join(sm.dataDir, "MANIFEST")

	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := json.NewEncoder(file).Encode(manifest); err != nil {
		os.Remove(tempPath)
		return err
	}
	if err := file.Sync(); err != nil {
		os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, filePath); err != nil {
		os.Remove(tempPath)
		return err
	}
	return nil
}

func (sm *SegmentManager) WriteInitialManifest(activeSegment *Segment) error {
	sm.activeSegment = activeSegment
	manifest := sm.buildManifestFromState()
	return sm.writeManifest(manifest)
}

func (sm *SegmentManager) GetTiers() []Tier {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.tiers
}

func (sm *SegmentManager) CreateSegment(id int) (*Segment, error) {
	seg := NewSegment(sm.dataDir, id)
	if seg.Exists() {
		return nil, fmt.Errorf("segment %d already exists", id)
	}
	return seg, nil
}

// Creates a new segment and sets the old one to readonly when Set() calls it
func (sm *SegmentManager) RotateSegment(oldSegment *Segment) (*Segment, error) {
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

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// if there's no tiers yet, add the tier 0
	if len(sm.tiers) == 0 {
		sm.tiers = append(sm.tiers, Tier{Level: 0, Segments: []*Segment{}})
	}
	// Append the segment to the tier list
	sm.tiers[0].Segments = append(sm.tiers[0].Segments, oldSegment)

	// IF WE DO THIS WE COULD REUSE THE WriteInitialManifest FUNCTION??
	sm.activeSegment = newSegment
	manifest := sm.buildManifestFromState()
	if err := sm.writeManifest(manifest); err != nil {
		return nil, fmt.Errorf("FATAL: failed to write manifest after segment rotation: %w", err)
	}

	// Check if we have enough segments to merge and send it to the merger
	if len(sm.tiers[0].Segments) >= sm.mergeThreshold {
		segmentsToMerge := sm.tiers[0].Segments
		sm.tiers[0].Segments = []*Segment{}
		sm.mergeCh <- segmentsToMerge
	}

	return newSegment, nil
}

func parseSegmentID(filename string) (int, bool) {
	var id int
	if n, err := fmt.Sscanf(filename, "segment_%d.db", &id); err == nil && n == 1 {
		return id, true
	}
	// Add other formats like merged segments if needed
	return 0, false
}