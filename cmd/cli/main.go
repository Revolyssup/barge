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
	fmt.Fprintf(os.Stderr, `Usage: barge-cli [options] <command> [arguments]

Options:
  --addr <host:port>    Address of the barge node (default: localhost:50051)

Commands:
  get <key>             Get the value for a key
  set <key> <value>     Set a key to a value

Examples:
  barge-cli --addr localhost:50051 set mykey myvalue
  barge-cli --addr localhost:50051 get mykey
  barge-cli get mykey
  barge-cli set foo bar
`)
	os.Exit(1)
}

func main() {
	addr := "localhost:50051"
	args := os.Args[1:]

	// Parse --addr flag manually to keep it simple
	for i := 0; i < len(args); i++ {
		if args[i] == "--addr" {
			if i+1 >= len(args) {
				fmt.Fprintf(os.Stderr, "Error: --addr requires an argument\n")
				os.Exit(1)
			}
			addr = args[i+1]
			// Remove --addr and its value from args
			args = append(args[:i], args[i+2:]...)
			break
		}
	}

	if len(args) < 1 {
		usage()
	}

	command := args[0]

	switch command {
	case "get":
		if len(args) != 2 {
			fmt.Fprintf(os.Stderr, "Usage: barge-cli get <key>\n")
			os.Exit(1)
		}
		key := args[1]
		doGet(addr, key)

	case "set":
		if len(args) != 3 {
			fmt.Fprintf(os.Stderr, "Usage: barge-cli set <key> <value>\n")
			os.Exit(1)
		}
		key := args[1]
		value := args[2]
		doSet(addr, key, value)

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		usage()
	}
}

func connect(addr string) (pb.RaftServiceClient, *grpc.ClientConn) {
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

	return pb.NewRaftServiceClient(conn), conn
}

func doGet(addr, key string) {
	client, conn := connect(addr)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ClientGet(ctx, &pb.ClientGetRequest{
		Key: key,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Error != "" {
		fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
		os.Exit(1)
	}

	if !resp.Found {
		fmt.Fprintf(os.Stderr, "Key not found: %s\n", key)
		os.Exit(1)
	}

	fmt.Println(resp.Value)
}

func doSet(addr, key, value string) {
	client, conn := connect(addr)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ClientSet(ctx, &pb.ClientSetRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resp.Error != "" {
		fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
		os.Exit(1)
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Error: set operation failed\n")
		os.Exit(1)
	}

	fmt.Printf("OK\n")
}
