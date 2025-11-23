package v6

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TOMBSTONE_VALUE = "null"
const MAX_LEVEL = 2

type V6Store struct {
	mu             sync.RWMutex
	dataDir        string
	memtable       *MemTable
	immutable      *MemTable	// Rotated memtable that is being flushed to an SSTable
	manager        *LSMManager
	maxMemSize     int64
	flushWg        sync.WaitGroup
}

func NewV6Store() *V6Store {
	dataDir := filepath.Join("v6", "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
    panic(fmt.Sprintf("failed to create data directory: %v", err))
	}

	manager := NewLSMManager(dataDir)

	// The manager already initializes the segments, indexes and bloom filters
	manifest, err := manager.InitState()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize db state: %v", err))
	}


	var walPath string
	if manifest != nil && manifest.ActiveWAL != "" {
		walPath = filepath.Join(dataDir, manifest.ActiveWAL)
	} else {
		walPath = filepath.Join(dataDir, "wal_0000.log")
		if err := manager.UpdateActiveWAL("wal_0000.log"); err != nil {
			panic(fmt.Sprintf("failed to update active WAL: %v", err))
		}
	}
	// Create memtable, it automatically replays the previous WAL if exists
	memtable, err := NewMemTable(walPath)
	if err != nil {
		panic(fmt.Sprintf("failed to create memtable: %v", err))
	}

	return &V6Store{
		dataDir:        dataDir,
		memtable:       memtable,
		immutable:      nil,
		manager:        manager,
		maxMemSize:			300, // Small max size ~8 lines
	}
}

func (s *V6Store) Close() error {
	// Wait for all flushes to complete
	s.flushWg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.memtable != nil {
		s.memtable.Close()
	}

	if s.immutable != nil {
		s.immutable.Close()
	}

	s.manager.Close()
	return nil
}

func (s *V6Store) Set(key, value string) error {
	s.mu.Lock()

	// Insert KV into memtable
	if err := s.memtable.Insert([]byte(key), []byte(value)); err != nil {
		s.mu.Unlock()
		return err
	}

	// After inserting, check if the memtable should be flushed
	shouldFlush := s.memtable.ShouldFlush(s.maxMemSize)
	s.mu.Unlock()

	// If the memtable should be flushed, rotate it
	if shouldFlush {
		return s.rotateMemTable()
	}

	return nil
}

func (s *V6Store) Get(key string) (string, error) {
	s.mu.RLock()

	// Check active memtable first
	if val, err := s.memtable.Find([]byte(key)); err == nil {
		s.mu.RUnlock()
		if string(val) == TOMBSTONE_VALUE {
			return "", fmt.Errorf("key not found")
		}
		return string(val), nil
	}

	// Check immutable memtable
	if s.immutable != nil {
		if val, err := s.immutable.Find([]byte(key)); err == nil {
			s.mu.RUnlock()
			if string(val) == TOMBSTONE_VALUE {
				return "", fmt.Errorf("key not found")
			}
			return string(val), nil
		}
	}

	s.mu.RUnlock()

	// Check LSM (already checks through range, bloom filter and entries)
	if val, found := s.manager.Get([]byte(key)); found {
		if string(val) == TOMBSTONE_VALUE {
			return "", fmt.Errorf("key not found")
		}
		return string(val), nil
	}

	return "", fmt.Errorf("key not found")
}

func (s *V6Store) Update(key, value string) error {
	return s.Set(key, value)
}

func (s *V6Store) Delete(key string) error {
	return s.Set(key, TOMBSTONE_VALUE)
}

func (s *V6Store) rotateMemTable() error {
	s.mu.Lock()

	// Double check if the memtable should be flushed
	if !s.memtable.ShouldFlush(s.maxMemSize){
		s.mu.Unlock()
		return nil
	}

	for s.immutable != nil {
		s.mu.Unlock()
		time.Sleep(10 * time.Millisecond) // Wait for the immutable to be flushed
		s.mu.Lock()
	}

	// Create new memtable
	walName := fmt.Sprintf("wal_%04d.log", s.manager.nextEntryID)
	walPath := filepath.Join(s.dataDir, walName)
	newMemtable, err := NewMemTable(walPath)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to rotate memtable: %v", err)
	}

	// Move current memtable to immutable
	s.immutable = s.memtable
	s.immutable.MakeReadOnly()
	s.memtable = newMemtable

	if err := s.manager.UpdateActiveWAL(walName); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to update active WAL: %v", err)
	}

	toFlush := s.immutable
	s.mu.Unlock()
	
	s.flushWg.Add(1)
	go func() {
		defer s.flushWg.Done()
		s.flushMemTable(toFlush)
	}()
	return nil
}

func (s *V6Store) flushMemTable(mt *MemTable) {
	// Create paht
	sstPath := s.manager.CreateSSTablePath()

	// Flush memtable to SSTable
	if err := mt.Flush(sstPath); err != nil {
		return
	}

	// Add SSTable to manager
	if err := s.manager.AddSSTable(sstPath); err != nil {
		return
	}

	// Delete WAL
	if mt.wal != nil {
		walPath := mt.wal.path
		mt.Close()
		if err := DeleteWAL(walPath); err != nil {
			fmt.Printf("failed to delete WAL: %v", err)
		}
	}

	// Update immutable
	s.mu.Lock()
	if s.immutable == mt {
		s.immutable = nil
	}
	s.mu.Unlock()
}