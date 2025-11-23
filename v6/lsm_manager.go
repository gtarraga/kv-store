package v6

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
	ActiveWAL			string					`json:"active_wal"`
	NextEntryID		int							`json:"next_entry_id"`
}

type ManifestTier struct {
	Level 		int 			`json:"level"`
	Segments	[]string 	`json:"segments"`
}

// Live tiers struct
type Tier struct {
	Level			int
	Segments	[]*SSTableReader
}

type LSMManager struct {
	dataDir	string
	mu			sync.RWMutex

	// In memory tiers parsed from the manifest
	tiers					[]Tier 		// Contains all rotated segments
	maxLevels			int
	nextEntryID		int

	// Merging
	mergeThreshold	int
	mergeCh					chan []*SSTableReader
	stopMerger			chan struct{}
	mergerDone			sync.WaitGroup
}

func NewLSMManager(dataDir string) *LSMManager {
	lsm := &LSMManager{
		dataDir:        dataDir,
		mergeCh:      	make(chan []*SSTableReader, 10), // Up to 10 segments can be queued for compaction
		stopMerger:			make(chan struct{}),
		mergeThreshold:	4, // Merge when Tier 0 has 4 segments
		maxLevels: 			MAX_LEVEL,
	}
	
	lsm.mergerDone.Add(1)
	go lsm.mergerWorker()
	
	return lsm
}

func (lsm *LSMManager) Close() {
	close(lsm.stopMerger)
	lsm.mergerDone.Wait()

	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	for _, tier := range lsm.tiers {
		for _, sst := range tier.Segments {
			sst.Close()
		}
	}
}

// Get searches for a key in all older segments
// It returns (value, found)
func (lsm *LSMManager) Get(key []byte) ([]byte, bool) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	// Search tiers from newest to oldest
	// Tier 0 (Newest) -> Tier N (Oldest)
	for _, tier := range lsm.tiers {
		// Within a tier, search segments from newest to oldest
		// Newest are at the end so we iterate in reverse:
		// Newest (LAST) -> Oldest (FIRST)
		for i := len(tier.Segments) - 1; i >= 0; i-- {
			sst := tier.Segments[i]

			// This already handles the bloom filter check and range check
			val, err := sst.Get(key)
			if err == nil {
				return val, true
			}
		}
	}
	return nil, false
}

// Loads the db structure from the MANIFEST file or initializes a new one
func (lsm *LSMManager) InitState() (*Manifest, error) {
	manifestPath := filepath.Join(lsm.dataDir, "MANIFEST")
	f, err := os.Open(manifestPath)
	if os.IsNotExist(err) {
		return lsm.initializeFromDirectory()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open manifest: %w", err)
	}
	defer f.Close()

	// It exists so we error out
	var manifest Manifest
	if err := json.NewDecoder(f).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	lsm.nextEntryID = manifest.NextEntryID
	
	validFiles := make(map[string]bool)

	// Load Tiers
	for _, mt := range manifest.Tiers {
		var tierSegments []*SSTableReader
		for _, segName := range mt.Segments {
			seg, _ := LoadSSTable(filepath.Join(lsm.dataDir, segName))
			tierSegments = append(tierSegments, seg)
			validFiles[segName] = true
		}
		
		// Build the in memory segments for the segment manager
		lsm.tiers = append(lsm.tiers, Tier{
			Level: mt.Level,
			Segments: tierSegments,
		})
	}

	if manifest.ActiveWAL != "" {
		validFiles[manifest.ActiveWAL] = true
	}

	lsm.cleanupDirectory(validFiles)
	return &manifest, nil
}

// Only used when MANIFEST doesn't exist
func (lsm *LSMManager) initializeFromDirectory() (*Manifest, error) {
	segments, err := lsm.DiscoverSegments()
	if err != nil {
		return nil, err
	}
	// New db!
	if len(segments) == 0 {
		return &Manifest{
			ActiveWAL: "wal_0000.log",
			NextEntryID: 0,
		}, nil
	}

	// Add all SSTables to tierSegments
	tierSegments := segments
	lsm.tiers = []Tier{{Level: 0, Segments: tierSegments}}
	lsm.nextEntryID = segments[len(segments)-1].Id + 1

	// Fallback to writing manifest line by line
	manifest := lsm.buildManifestFromState()
	if err := lsm.writeManifest(manifest); err != nil {
    return nil, fmt.Errorf("failed to write initial manifest: %w", err)
  }
	
	return &manifest, nil
}

// Scans the dir for db files to bootstrap the db
func (lsm *LSMManager) DiscoverSegments() ([]*SSTableReader, error) {
	entries, err := os.ReadDir(lsm.dataDir) // Already sorted ascendingly
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	// Building the available segments from the existing files
	// ONly processes files matching the segment_XXXX.db pattern
	var segments []*SSTableReader
	for _, e := range entries {
		name := e.Name()
		if _, ok := parseSegmentID(name); ok {
			SSTablePath := filepath.Join(lsm.dataDir, name)
			SSTable, _ := LoadSSTable(SSTablePath)
			segments = append(segments, SSTable)
		}
	}

	return segments, nil
}

// Builds a manifest format from the one SegmentManager has in memory
func (lsm *LSMManager) buildManifestFromState() Manifest {
	manifest := Manifest{
		NextEntryID:	 lsm.nextEntryID,
	}

	for _, tier := range lsm.tiers {
		mt := ManifestTier{Level: tier.Level}
		for _, seg := range tier.Segments {
			mt.Segments = append(mt.Segments, filepath.Base(seg.Path))
		}
		manifest.Tiers = append(manifest.Tiers, mt)
	}
	return manifest
}

// Reading a manifest in memory and rewriting it to the file
func (lsm *LSMManager) writeManifest(manifest Manifest) error {
	tempPath := filepath.Join(lsm.dataDir, "MANIFEST.tmp")
	filePath := filepath.Join(lsm.dataDir, "MANIFEST")

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

// Removes orphan files not in the manifest
func (lsm *LSMManager) cleanupDirectory(validFiles map[string]bool) {
	entries, _ := os.ReadDir(lsm.dataDir)
	for _, entry := range entries {
		name := entry.Name()
		if !validFiles[name] && (strings.HasSuffix(name, ".db") || strings.HasSuffix(name, ".idx") || strings.HasSuffix(name, ".log")) {
			os.Remove(filepath.Join(lsm.dataDir, name))
		}
	}
}

func (lsm *LSMManager) UpdateActiveWAL(walName string) error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	manifest := lsm.buildManifestFromState()
	manifest.ActiveWAL = walName
	return lsm.writeManifest(manifest)
}

// Creates a new SSTable path and increments the nextEntryID
func (lsm *LSMManager) CreateSSTablePath() string {
	lsm.mu.Lock()
	id := lsm.nextEntryID
	lsm.nextEntryID++
	lsm.mu.Unlock()

	filename := fmt.Sprintf("sst_%04d.db", id)
	return filepath.Join(lsm.dataDir, filename)
}

// Adds a new SSTable to the manifest
func (lsm *LSMManager) AddSSTable(sstPath string) error {
	// Load
	sst, err := LoadSSTable(sstPath)
	if err != nil {
		return fmt.Errorf("failed to load SSTable: %w", err)
	}
	
	lsm.mu.Lock()

	// Add to Tier 0
	if len(lsm.tiers) == 0 {
		lsm.tiers = []Tier{{Level: 0, Segments: []*SSTableReader{}}}
	}
	lsm.tiers[0].Segments = append(lsm.tiers[0].Segments, sst)

	manifest := lsm.buildManifestFromState()
	if err := lsm.writeManifest(manifest); err != nil {
		lsm.mu.Unlock()
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	// Check if we need merges
	var shouldMerge bool
	var toMerge []*SSTableReader
	if len(lsm.tiers[0].Segments) >= lsm.mergeThreshold {
		shouldMerge = true
		toMerge = make([]*SSTableReader, len(lsm.tiers[0].Segments))
		copy(toMerge, lsm.tiers[0].Segments)
	}

	lsm.mu.Unlock()

	// Send it to the merge channel, skip if the channel is full
	if shouldMerge {
		select{
			case lsm.mergeCh <- toMerge:
			default:
				fmt.Printf("merge channel is full, skipping merge\n")
		}
	}

	return nil
}

func parseSegmentID(filename string) (int, bool) {
	var id int
	if _, err := fmt.Sscanf(filename, "sst_%04d.db", &id); err == nil {
		return id, true
	}
	return 0, false
}