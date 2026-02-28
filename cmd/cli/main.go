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

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: barge-cli [options] <command> [args]

Commands:
  get <key>           Get the value for a key
  set <key> <value>   Set a key-value pair

Options:
  --addr <address>    Address of the barge node (default: localhost:50051)
  --help              Show this help message

Examples:
  barge-cli set mykey myvalue
  barge-cli --addr localhost:50052 get mykey
`)
}

func main() {
	addr := "localhost:50051"
	args := os.Args[1:]

	// Parse flags
	var cmdArgs []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--addr":
			if i+1 >= len(args) {
				fmt.Fprintf(os.Stderr, "Error: --addr requires an argument\n")
				os.Exit(1)
			}
			i++
			addr = args[i]
		case "--help", "-h":
			usage()
			os.Exit(0)
		default:
			cmdArgs = append(cmdArgs, args[i])
		}
	}

	if len(cmdArgs) == 0 {
		fmt.Fprintf(os.Stderr, "Error: no command specified\n\n")
		usage()
		os.Exit(1)
	}

	// Connect to the barge node
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to connect to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)

	command := cmdArgs[0]
	switch command {
	case "get":
		if len(cmdArgs) < 2 {
			fmt.Fprintf(os.Stderr, "Error: get requires a key argument\n")
			fmt.Fprintf(os.Stderr, "Usage: barge-cli get <key>\n")
			os.Exit(1)
		}
		key := cmdArgs[1]
		doGet(client, key)

	case "set":
		if len(cmdArgs) < 3 {
			fmt.Fprintf(os.Stderr, "Error: set requires key and value arguments\n")
			fmt.Fprintf(os.Stderr, "Usage: barge-cli set <key> <value>\n")
			os.Exit(1)
		}
		key := cmdArgs[1]
		value := cmdArgs[2]
		doSet(client, key, value)

	default:
		fmt.Fprintf(os.Stderr, "Error: unknown command %q\n\n", command)
		usage()
		os.Exit(1)
	}
}

func doGet(client pb.RaftServiceClient, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ClientGet(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to get key %q: %v\n", key, err)
		os.Exit(1)
	}

	if resp.Error != "" {
		fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
		os.Exit(1)
	}

	if !resp.Found {
		fmt.Fprintf(os.Stderr, "Key %q not found\n", key)
		os.Exit(1)
	}

	fmt.Println(resp.Value)
}

func doSet(client pb.RaftServiceClient, key, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ClientSet(ctx, &pb.SetRequest{Key: key, Value: value})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to set key %q: %v\n", key, err)
		os.Exit(1)
	}

	if resp.Error != "" {
		fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
		os.Exit(1)
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Error: set operation was not successful\n")
		os.Exit(1)
	}

	fmt.Printf("OK\n")
}
