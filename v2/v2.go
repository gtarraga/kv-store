package v2

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type V2Store struct {
	filePath string
}

func NewV2Store() *V2Store {
	dataDir := filepath.Join("v2", "data")
	os.MkdirAll(dataDir, 0755)

	return &V2Store{
		filePath: filepath.Join(dataDir, "db.txt"),
	}
}

// Sets a key-value pair in the database by appending to the file
func (s *V2Store) Set(key string, value string) error {
	file, err := os.OpenFile(s.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	
	_, err = fmt.Fprintf(file, "%s:%s\n", key, value)
	return err
}

// Reads the entire file to find the last match
func (s *V2Store) Get(key string) (string, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("key not found: %s", key)
		}
		return "", err
	}
	defer file.Close()

	var lastValue *string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 && parts[0] == key {
			// We keep track of the last value of the key we are looking for
			val := parts[1]
			lastValue = &val
		}
	}

  if lastValue == nil {
		return "", fmt.Errorf("key not found: %s", key)
	}

	// If the value is "null" this means it was deleted ("null" is our tombstone record)
	if *lastValue == "null" {
		return "", fmt.Errorf("key not found: %s", key)
	}

	return *lastValue, nil
}

// Updates a key-value pair in the database by appending an updated value to the file
func (s *V2Store) Update(key, value string) error {
	return s.Set(key, value)
}

// Deletes a key-value pair in the database by appending a tombstone record (`null` value) to the file
func (s *V2Store) Delete(key string) error {
	return s.Set(key, "null")
}

func (s *V2Store) Close() error {
	return nil
}