package v6

import (
	"fmt"
	"sync"
)

type MemTable struct {
	mu				sync.RWMutex
	skiplist	*SkipList
	size			int64
	count			int
	readOnly	bool
	wal				*WAL
}

func NewMemTable(walPath string) (*MemTable, error) {
	mt := &MemTable{
		skiplist: NewSkipList(),
		size: 		0,
		count:		0,
		readOnly: false,
	}

	// Replay if it exists
	if err := ReplayWAL(walPath, mt); err != nil {
		return nil, err
	}

	// Open the WAL
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}
	mt.wal = wal

	return mt, nil
}

func (mt *MemTable) NewIterator() *Iterator {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.NewIterator()
}

func (mt *MemTable) Size() int64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.size
}

func (mt *MemTable) Count() int64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return int64(mt.count)
}

func (mt *MemTable) MakeReadOnly() {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.readOnly = true
}

func (mt *MemTable) IsReadOnly() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.readOnly
}

func (mt *MemTable) Find(key []byte) ([]byte, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.Find(key)
}

// Inserts or updates the KV and updates the size
func (mt *MemTable) Insert(key, value []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.readOnly {
		panic("readonly memtable")
	}


	// Write to WAL first (for durability)
	if mt.wal != nil {
		if err := mt.wal.WritePut(key, value); err != nil {
			return err
		}
	}

	// Check if the key exists, to update or add the new KV size to the memtable size
	existing, err := mt.skiplist.Find(key)
	if err == nil {
		mt.size -= int64(len(existing))
		mt.size += int64(len(value))
	} else {
		mt.size += int64(len(key) + len(value))
		mt.count++
	}

	mt.skiplist.Insert(key, value)
	return nil
}

// Inserts or updates the KV and updates the size
func (mt *MemTable) Delete(key []byte) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.readOnly {
		panic("readonly memtable")
	}
	
	// Write to WAL first (for durability)
	if mt.wal != nil {
		if err := mt.wal.WriteDelete(key); err != nil {
			return false
		}
	}

	// Check if the key exists, to update or add the new KV size to the memtable size
	value, err := mt.skiplist.Find(key)
	if err == nil {
		mt.size -= int64(len(key) + len(value))
		mt.count--
	}

	return mt.skiplist.Delete(key)
}

func (mt *MemTable) ShouldFlush(threshold int64) bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.size >= threshold
}

func (mt *MemTable) Flush(outputPath string) error {
	mt.mu.Lock()
	if !mt.readOnly {
		mt.MakeReadOnly()
	}
	count := mt.count
	mt.mu.Unlock()

	writer, err := NewSSTableWriter(outputPath, count)
	if err != nil {
		return fmt.Errorf("failed to create SSTable writer: %w", err)
	}

	iter := mt.skiplist.NewIterator()
	entryCount := 0
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if err := writer.Append(key, value); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
		entryCount++
	}

	if err := writer.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize SSTable: %w", err)
	}

	return nil
}

func (mt *MemTable) ForEach(fn func(key, value []byte) bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	iter := mt.skiplist.NewIterator()
	for iter.Next() {
		if !fn(iter.Key(), iter.Value()) {
			break
		}
	}
}

// Resets the memtable
func (mt *MemTable) Clear() {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.readOnly {
		panic("readonly memtable")
	}
	
	mt.skiplist = NewSkipList()
	mt.size = 0
	mt.count = 0
}

func (mt *MemTable) Close() error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	
	if mt.wal != nil {
		return mt.wal.Close()
	}
	return nil
}