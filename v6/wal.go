package v6

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type WAL struct {
	file		*os.File
	writer	*bufio.Writer
	path		string
}

const (
	WALEntryPut			byte = 1
	WALEntryDelete	byte = 2
)

// New write ahead log
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	
	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
		path:   path,
	}, nil
}

func (w *WAL) WriteEntry(entryType byte, key, value []byte) error {
	var line string
	
	switch entryType {
	case WALEntryPut:
		line = fmt.Sprintf("PUT %s:%s\n", string(key), string(value))
	case WALEntryDelete:
		line = fmt.Sprintf("DEL %s\n", string(key))
	default:
		return fmt.Errorf("unknown entry type: %d", entryType)
	}

	if _, err := w.writer.WriteString(line); err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *WAL) WritePut(key, value []byte) error {
	return w.WriteEntry(WALEntryPut, key, value)
}

func (w *WAL) WriteDelete(key []byte) error {
	return w.WriteEntry(WALEntryDelete, key, nil)
}

func (w *WAL) Close() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Forces a sync to disk
func (w *WAL) Sync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Replay reads and entries from WAL
func ReplayWAL(path string, mt *MemTable) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL file is fine
		}
		return err
	}
	defer file.Close()
	
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		
		if strings.HasPrefix(line, "PUT ") {
			// Format: PUT key:value
			parts := strings.SplitN(line[4:], ":", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid PUT entry: %s", line)
			}
			key := []byte(parts[0])
			value := []byte(parts[1])
			mt.skiplist.Insert(key, value)
			mt.size += int64(len(key) + len(value))
			mt.count++
		} else if strings.HasPrefix(line, "DEL ") {
			// Format: DEL key
			key := []byte(line[4:])
			value, err := mt.skiplist.Find(key)
			if err == nil {
				mt.size -= int64(len(key) + len(value))
				mt.count--
				mt.skiplist.Delete(key)
			}
		} else {
			return fmt.Errorf("unknown entry type in line: %s", line)
		}
	}
	
	if err := scanner.Err(); err != nil {
		return err
	}
	
	return nil
}

// Removes the WAL
func DeleteWAL(path string) error {
	return os.Remove(path)
}