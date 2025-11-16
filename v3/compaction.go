package v3

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func (s *V3Store) compactionWorker() {
	defer s.compactionDone.Done()

	for {
		select {
		case <-s.stopCompaction:
			return
		case segmentId := <-s.compactCh:
			s.compactSegment(segmentId)
		}
	}
}


// This function loads the old segment records onto an in-memory hashmap to deduplicate
// after that, it removes the deleted KVs (tombstone values) and writes a temp file
// finally it replaces the original file with the newly compacted temp file
func (s *V3Store) compactSegment(segmentId int) {
	segmentPath := segmentPath(s.dataDir, segmentId)

	file, err := os.Open(segmentPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		fmt.Printf("Compaction error reading segment %d: %v\n", segmentId, err)
		return
	}
	defer file.Close()
	
	// Hashmap to read the segment and deduplicate
	keyValues := make(map[string]string)

	// Read the entire file and map it onto a hashmap
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			// This will update values removing duplicates
			// last write wins but still keeps tombstones
			keyValues[parts[0]] = parts[1]
		}
	}
	if err := scanner.Err(); err != nil {
    fmt.Printf("Compaction error scanning segment %d: %v\n", segmentId, err)
    return
	}

	// Remove tombstones (deleted keys)
	// This could be done more efficiently skipping the lines on the write
	// but doing it separately to illustrate the tombstone handling
	for key, value := range keyValues {
		if value == tombstoneValue {
			delete(keyValues, key)
		}
	}
	
	tempPath := segmentPath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		fmt.Printf("Compaction error creating temp file for segment %d: %v\n", segmentId, err)
		return
	}

	// Write compacted records to temp file 
	for key, value := range keyValues {
		fmt.Fprintf(tempFile, "%s:%s\n", key, value)
	}
	if err := tempFile.Close(); err != nil {
    fmt.Printf("Compaction error closing temp file for segment %d: %v\n", segmentId, err)
    os.Remove(tempPath)
    return
	}

	// Replace old segment file with the new compacted one
	if err := os.Rename(tempPath, segmentPath); err != nil {
		fmt.Printf("Compaction error replacing segment %d: %v\n", segmentId, err)
		os.Remove(tempPath)
		return
	}

	// Back to readonly perms
	os.Chmod(segmentPath, 0o444)
}