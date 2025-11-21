package v6

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
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
	keyLen := uint32(len(key))
	valueLen := uint32(len(value))

	// Build entry and reserve space for checksum
	buf := make([]byte, 0, 13+keyLen+valueLen)
	buf = append(buf, 0, 0, 0, 0)

	// Type
	buf = append(buf, entryType)
	
	// Key
	buf = binary.BigEndian.AppendUint32(buf, keyLen)
	buf = append(buf, key...)

	// Value
	buf = binary.BigEndian.AppendUint32(buf, valueLen)
	buf = append(buf, value...)

	// Checksum
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], checksum)

	if _, err := w.writer.Write(buf); err != nil {
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
	
	reader := bufio.NewReader(file)
	
	for {
		// Read checksum
		var checksumBuf [4]byte
		if _, err := io.ReadFull(reader, checksumBuf[:]); err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			return err
		}
		expectedChecksum := binary.BigEndian.Uint32(checksumBuf[:])
		
		// Read entry type
		entryType, err := reader.ReadByte()
		if err != nil {
			return err
		}
		
		// Read key length
		var keyLenBuf [4]byte
		if _, err := io.ReadFull(reader, keyLenBuf[:]); err != nil {
			return err
		}
		keyLen := binary.BigEndian.Uint32(keyLenBuf[:])
		
		// Read key
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return err
		}
		
		// Read value length
		var valueLenBuf [4]byte
		if _, err := io.ReadFull(reader, valueLenBuf[:]); err != nil {
			return err
		}
		valueLen := binary.BigEndian.Uint32(valueLenBuf[:])
		
		// Read value
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			return err
		}
		
		// Verify checksum
		checksumData := make([]byte, 0, 1+8+keyLen+valueLen)
		checksumData = append(checksumData, entryType)
		checksumData = append(checksumData, keyLenBuf[:]...)
		checksumData = append(checksumData, key...)
		checksumData = append(checksumData, valueLenBuf[:]...)
		checksumData = append(checksumData, value...)
		
		actualChecksum := crc32.ChecksumIEEE(checksumData)
		if actualChecksum != expectedChecksum {
			return fmt.Errorf("checksum mismatch: expected %d, got %d", expectedChecksum, actualChecksum)
		}
		
		// Replay entry
		switch entryType {
		case WALEntryPut:
			mt.Insert(key, value)
		case WALEntryDelete:
			mt.Delete(key)
		default:
			return fmt.Errorf("unknown entry type: %d", entryType)
		}
	}
	
	return nil
}

// Removes the WAL
func DeleteWAL(path string) error {
	return os.Remove(path)
}