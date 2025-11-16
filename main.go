package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	v1 "kv-store/v1"
	v2 "kv-store/v2"
	v3 "kv-store/v3"
	v4 "kv-store/v4"
)

type KVStore interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Update(key, value string) error
	Delete(key string) error
	Close() error
}

// Available database versions
var dbRegistry = map[string]func() (KVStore, error){
	"v1": func() (KVStore, error) { return v1.NewV1Store(), nil },
	"v2": func() (KVStore, error) { return v2.NewV2Store(), nil },
	"v3": func() (KVStore, error) { return v3.NewV3Store(), nil },
	"v4": func() (KVStore, error) { return v4.NewV4Store(), nil },
}

const defaultVersion = "v4"

func main() {
	version := flag.String("version", defaultVersion, "Database version to use (v1, v2, v3, etc.)")
	compare := flag.Bool("compare", false, "Run in comparison mode to benchmark all versions")
	flag.Parse()

	args := flag.Args()

	// Comparison mode
	if *compare {
		if len(args) == 0 {
			runCompareInteractive()
			return
		}
		
		if err := runCompareCommand(args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Standard single-version mode
	db, err := initDB(*version)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// If no command provided, enter interactive mode
	if len(args) == 0 {
		runInteractive(db, *version)
		return
	}

	// Single command
	if err := executeCommand(db, args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// Initializes db with a specific version
func initDB(version string) (KVStore, error) {
	constructor, exists := dbRegistry[version]
	if !exists {
		available := make([]string, 0, len(dbRegistry))
		for v := range dbRegistry {
			available = append(available, v)
		}
		return nil, fmt.Errorf("unknown version '%s'. Available versions: %v", version, available)
	}

	return constructor()
}

// Single command runner
func executeCommand(db KVStore, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	command := strings.ToLower(args[0])
	
	switch command {
	case "add", "set":
		if len(args) != 3 {
			return fmt.Errorf("usage: set <key> <value>")
		}
		if err := db.Set(args[1], args[2]); err != nil {
			return err
		}
		return nil

	case "search", "get":
		if len(args) != 2 {
			return fmt.Errorf("usage: search <key>")
		}
		value, err := db.Get(args[1])
		if err != nil {
			return err
		}
		fmt.Println(value)
		return nil

	case "update", "u":
		if len(args) != 3 {
			return fmt.Errorf("usage: update <key> <value>")
		}
		return db.Update(args[1], args[2])

	case "delete", "del", "d":
		if len(args) != 2 {
			return fmt.Errorf("usage: delete <key>")
		}
		if err := db.Delete(args[1]); err != nil {
			return err
		}
		return nil

	default:
		return fmt.Errorf("unknown command '%s'. Available commands: add, search, update, delete", command)
	}
}

// Interactive REPL session
func runInteractive(db KVStore, version string) {
	fmt.Printf("KV Database %s - Interactive Mode\n", version)
	fmt.Println("Commands: add <key> <value> | search <key> | update <key> <value> | delete <key> | exit | help")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	
	for {
		fmt.Print("kv> ")
		
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
			fmt.Printf("Using database version: %s\n", version)
			continue
		}

		if err := executeCommand(db, args); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
	}
}

// printHelp displays help information
func printHelp() {
	fmt.Println("Available Commands:")
	fmt.Println("  add <key> <value>      - Add a new key with value")
	fmt.Println("  search <key>           - Get the value of a key")
	fmt.Println("  update <key> <value>   - Update an existing key")
	fmt.Println("  delete <key>           - Delete a key")
	fmt.Println("  help                   - Show this help message")
	fmt.Println("  version                - Show current database version")
	fmt.Println("  exit                   - Exit interactive mode")
}