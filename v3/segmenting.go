package v3

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func segmentFileName(id int) string {
	return fmt.Sprintf("segment_%04d.db", id)
}

func segmentPath(dataDir string, id int) string {
	return filepath.Join(dataDir, segmentFileName(id))
}

func parseSegmentID(filename string) (int, bool) {
	name := filepath.Base(filename)

	// Parse using the same format pattern as segmentFileName
	var id int
	n, err := fmt.Sscanf(name, "segment_%d.db", &id)
	if err != nil || n != 1 {
		return 0, false
	}

	return id, true
}

func getExistingSegments(dataDir string) []int {
	// Reading the data dir for existing segments
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil
	}

	// Building the available IDs from the existing files
	var segmentIds []int
	for _, e := range entries {
		parsed, ok := parseSegmentID(e.Name())
		if ok {
			segmentIds = append(segmentIds, parsed)
		}
	}

	return segmentIds
}

func rotateSegment(s *V3Store) error {
	oldSegmentPath := s.activeSegmentPath
	newSegmentId := s.activeSegmentId + 1
	newSegmentPath := segmentPath(filepath.Dir(oldSegmentPath), newSegmentId)
	
	// Check if new segment already exists
	if _, err := os.Stat(newSegmentPath); err == nil {
		return fmt.Errorf("segment %d already exists, possible corruption", newSegmentId)
	}
	
	// Update state
	s.activeSegmentId = newSegmentId
	s.activeSegmentPath = newSegmentPath
	s.segmentIds = append(s.segmentIds, newSegmentId)
	
	// Compact old segment and make it readonly
	if oldSegmentPath != "" {
		if err := os.Chmod(oldSegmentPath, 0o444); err != nil {
			fmt.Printf("Warning: failed to set %s readonly: %v\n", oldSegmentPath, err)
		}

		// Send old file to the compaction background runner
		select {
		case s.compactCh <- newSegmentId - 1:
		default:
			// Channel is full so we skip compaction
		}
	}
	
	return nil
}

// Basic implementation of a get method on an append only file (like in v2)
// It returns a found and deleted booleans so we can stop searching segments asap
func searchSegmentForKey(segmentPath, key string) (string, bool, bool, error) {
	file, err := os.Open(segmentPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, false, nil
		}
		return "", false, false, err
	}
	defer file.Close()

	var foundValue string
	found := false

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 && parts[0] == key {
			// Keep track of the last value for this key
			foundValue = parts[1]
			found = true
		}
	}
	if err := scanner.Err(); err != nil {
    return "", false, false, err
	}

  if !found {
		return "", false, false, nil
	}

	// If the value is "null" this means it was deleted ("null" is our tombstone record)
	if foundValue == "null" {
		return "", true, true, nil
	}

	return foundValue, true, false, nil
}