package main

import (
	"fmt"
	"os"

	"github.com/revolyssup/barge/transport"
)

const defaultAddr = "localhost:50051"

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  barge-cli set <key> <value> [--addr <address>]
  barge-cli get <key> [--addr <address>]

Commands:
  set    Set a key-value pair on the barge cluster
  get    Get the value for a key from the barge cluster

Flags:
  --addr  Address of the barge node (default: %s)
`, defaultAddr)
}

func parseAddr(args []string) string {
	for i, arg := range args {
		if arg == "--addr" && i+1 < len(args) {
			return args[i+1]
		}
	}
	return defaultAddr
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	addr := parseAddr(os.Args[2:])

	client, err := transport.NewKVClient(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer client.Close()

	switch cmd {
	case "set":
		// Find key and value (skip --addr and its value)
		positional := filterPositional(os.Args[2:])
		if len(positional) < 2 {
			fmt.Fprintf(os.Stderr, "Error: set requires <key> and <value> arguments\n")
			usage()
			os.Exit(1)
		}
		key := positional[0]
		value := positional[1]

		resp, err := client.Set(key, value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if !resp.Success {
			fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
			os.Exit(1)
		}
		fmt.Printf("OK\n")

	case "get":
		positional := filterPositional(os.Args[2:])
		if len(positional) < 1 {
			fmt.Fprintf(os.Stderr, "Error: get requires a <key> argument\n")
			usage()
			os.Exit(1)
		}
		key := positional[0]

		resp, err := client.Get(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if !resp.Found {
			fmt.Fprintf(os.Stderr, "Key not found: %s\n", key)
			os.Exit(1)
		}
		fmt.Printf("%s\n", resp.Value)

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		usage()
		os.Exit(1)
	}
}

// filterPositional returns arguments that are not flags (--addr and its value).
func filterPositional(args []string) []string {
	var result []string
	skip := false
	for _, arg := range args {
		if skip {
			skip = false
			continue
		}
		if arg == "--addr" {
			skip = true
			continue
		}
		result = append(result, arg)
	}
	return result
}
