package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"
)

// PerformanceResult stores execution time and error for a single operation
type PerformanceResult struct {
	Version  string
	Duration time.Duration
	Error    error
	Value    string
}

// runCompareCommand executes a single command on all versions and compares performance
func runCompareCommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	// Initialize all versions
	dbs := make(map[string]KVStore)
	var versions []string
	
	for version := range dbRegistry {
		versions = append(versions, version)
		db, err := initDB(version)
		if err != nil {
			return fmt.Errorf("failed to initialize %s: %v", version, err)
		}
		dbs[version] = db
		defer db.Close()
	}

	results := make([]PerformanceResult, 0, len(versions))
	
	for _, version := range versions {
		db := dbs[version]
		
		start := time.Now()
		value, err := executeCommandWithResult(db, args)
		duration := time.Since(start)
		
		results = append(results, PerformanceResult{
			Version:  version,
			Duration: duration,
			Error:    err,
			Value:    value,
		})
	}

	printComparisonResults(args, results)
	
	return nil
}

// executeCommandWithResult executes a command and returns the result value
func executeCommandWithResult(db KVStore, args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("no command provided")
	}

	command := strings.ToLower(args[0])
	
	switch command {
	case "add", "set":
		if len(args) != 3 {
			return "", fmt.Errorf("usage: set <key> <value>")
		}
		err := db.Set(args[1], args[2])
		return "", err

	case "search", "get":
		if len(args) != 2 {
			return "", fmt.Errorf("usage: search <key>")
		}
		return db.Get(args[1])

	case "update", "u":
		if len(args) != 3 {
			return "", fmt.Errorf("usage: update <key> <value>")
		}
		err := db.Update(args[1], args[2])
		return "", err

	case "delete", "del", "d":
		if len(args) != 2 {
			return "", fmt.Errorf("usage: delete <key>")
		}
		err := db.Delete(args[1])
		return "", err

	default:
		return "", fmt.Errorf("unknown command '%s'. Available commands: add, search, update, delete", command)
	}
}

// printComparisonResults displays the performance comparison in a nice table format
func printComparisonResults(args []string, results []PerformanceResult) {
	command := strings.Join(args, " ")
	
	fmt.Printf("\nPerformance Comparison for: %s\n", command)
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("%-10s %-15s %s\n", "Version", "Time (ms)", "Status")
	fmt.Println(strings.Repeat("-", 60))
	
	for _, result := range results {
		ms := float64(result.Duration.Microseconds()) / 1000.0
		status := "✓ OK"
		
		if result.Error != nil {
			status = fmt.Sprintf("✗ %v", result.Error)
		} else if result.Value != "" {
			status = fmt.Sprintf("✓ %s", result.Value)
		}
		
		fmt.Printf("%-10s %-15.3f %s\n", result.Version, ms, status)
	}
	fmt.Println(strings.Repeat("=", 60))
}

// runCompareInteractive runs an interactive session comparing all versions
func runCompareInteractive() {
	fmt.Println("KV Database - Performance Comparison Mode")
	fmt.Println("Commands: add <key> <value> | search <key> | update <key> <value> | delete <key> | exit | help")
	fmt.Println()

	// Initialize all versions
	dbs := make(map[string]KVStore)
	var versions []string
	
	for version := range dbRegistry {
		versions = append(versions, version)
		db, err := initDB(version)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to initialize %s: %v\n", version, err)
			return
		}
		dbs[version] = db
		defer db.Close()
	}

	scanner := bufio.NewScanner(os.Stdin)
	
	for {
		fmt.Print("compare> ")
		
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		args := strings.Fields(line)
		command := strings.ToLower(args[0])

		switch command {
		case "exit", "quit":
			return

		case "help":
			printHelp()
			continue

		case "version":
			fmt.Printf("Comparing versions: %v\n", versions)
			continue
		}

		// Execute on all versions and measure performance
		results := make([]PerformanceResult, 0, len(versions))
		
		for _, version := range versions {
			db := dbs[version]
			
			start := time.Now()
			value, err := executeCommandWithResult(db, args)
			duration := time.Since(start)
			
			results = append(results, PerformanceResult{
				Version:  version,
				Duration: duration,
				Error:    err,
				Value:    value,
			})
		}

		printComparisonResults(args, results)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
	}
}

