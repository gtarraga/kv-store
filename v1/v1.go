package v1

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type V1Store struct {
	filePath string
}

func NewV1Store() *V1Store {
	dataDir := filepath.Join("v1", "data")
	os.MkdirAll(dataDir, 0755)

	return &V1Store{
		filePath: filepath.Join(dataDir, "db.txt"),
	}
}

// Sets a key-value pair in the database by appending to the file
func (s *V1Store) Set(key, value string) error {
	file, err := os.OpenFile(s.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	
	_, err = fmt.Fprintf(file, "%s:%s\n", key, value)
	return err
}

// Gets a value from the database reading line by line until it finds a match
func (s *V1Store) Get(key string) (string, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("key not found: %s", key)
		}
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 && parts[0] == key {
			return parts[1], nil
		}
	}

	return "", fmt.Errorf("key not found: %s", key)
}

// Updates a key-value pair in the database by rewriting the file
func (s *V1Store) Update(key, value string) error {
	return s.modifyKey(key, &value)
}

// Deletes a key-value pair in the database by rewriting the file
func (s *V1Store) Delete(key string) error {
	return s.modifyKey(key, nil)
}

// Creates a temp file and dumps the contents of the db file
// except the modified KV pair, then it overwrites the original
func (s *V1Store) modifyKey(key string, value *string) error {
	input, err := os.Open(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("key not found: %s", key)
		}
		return err
	}
	defer input.Close()

	tempFile := s.filePath + ".tmp"
	output, err := os.Create(tempFile)
	if err != nil {
		return err
	}
	defer output.Close()

	found := false
	scanner := bufio.NewScanner(input)

	// Copy the original db file line by line into the new temp file
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)

		if len(parts) == 2 && parts[0] == key && !found {
			found = true

			// Only copies matching key if we are updating it, otherwise it skips it
			if value != nil {
				fmt.Fprintf(output, "%s:%s\n", key, *value)
			}
		} else if line != "" {
			fmt.Fprintln(output, line)
		}
	}

	if !found {
		os.Remove(tempFile)
		return fmt.Errorf("key not found: %s", key)
	}

	output.Close()
	input.Close()

	return os.Rename(tempFile, s.filePath)
}

func (s *V1Store) Close() error {
	return nil
}