package main

import (
	"context"
	"fmt"
	"os"
	"time"

	pb "github.com/revolyssup/barge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultAddr = "localhost:50051"

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: barge-cli [options] <command> [arguments]

Commands:
  get <key>           Get the value for a key
  set <key> <value>   Set the value for a key

Options:
  --addr <address>    Address of the barge node (default: %s)
  --help              Show this help message

Examples:
  barge-cli --addr localhost:50051 set mykey myvalue
  barge-cli --addr localhost:50051 get mykey
  barge-cli get mykey
  barge-cli set foo bar
`, defaultAddr)
}

func main() {
	args := os.Args[1:]

	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	addr := defaultAddr

	// Parse flags
	i := 0
	for i < len(args) {
		switch args[i] {
		case "--addr":
			if i+1 >= len(args) {
				fmt.Fprintf(os.Stderr, "Error: --addr requires an argument\n")
				os.Exit(1)
			}
			addr = args[i+1]
			i += 2
		case "--help", "-h":
			usage()
			os.Exit(0)
		default:
			// Not a flag, must be the command
			goto parseCommand
		}
	}

	fmt.Fprintf(os.Stderr, "Error: no command specified\n")
	usage()
	os.Exit(1)

parseCommand:
	remaining := args[i:]

	if len(remaining) == 0 {
		fmt.Fprintf(os.Stderr, "Error: no command specified\n")
		usage()
		os.Exit(1)
	}

	command := remaining[0]
	cmdArgs := remaining[1:]

	// Connect to the barge node
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to connect to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewClientServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch command {
	case "get":
		if len(cmdArgs) != 1 {
			fmt.Fprintf(os.Stderr, "Error: 'get' requires exactly one argument: <key>\n")
			fmt.Fprintf(os.Stderr, "Usage: barge-cli get <key>\n")
			os.Exit(1)
		}
		key := cmdArgs[0]
		resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to get key %q: %v\n", key, err)
			os.Exit(1)
		}
		if !resp.Found {
			fmt.Fprintf(os.Stderr, "Key %q not found\n", key)
			os.Exit(1)
		}
		fmt.Println(resp.Value)

	case "set":
		if len(cmdArgs) != 2 {
			fmt.Fprintf(os.Stderr, "Error: 'set' requires exactly two arguments: <key> <value>\n")
			fmt.Fprintf(os.Stderr, "Usage: barge-cli set <key> <value>\n")
			os.Exit(1)
		}
		key := cmdArgs[0]
		value := cmdArgs[1]
		resp, err := client.Set(ctx, &pb.SetRequest{Key: key, Value: value})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to set key %q: %v\n", key, err)
			os.Exit(1)
		}
		if !resp.Success {
			fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
			os.Exit(1)
		}
		fmt.Printf("OK\n")

	default:
		fmt.Fprintf(os.Stderr, "Error: unknown command %q\n", command)
		usage()
		os.Exit(1)
	}
}
