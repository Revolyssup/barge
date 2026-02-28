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
	fmt.Fprintf(os.Stderr, `Usage:
  barge-cli [--addr <address>] <command> [arguments]

Commands:
  set <key> <value>   Set a key-value pair
  get <key>           Get the value for a key

Flags:
  --addr <address>    Address of the barge node (default: localhost:50051)
`)
	os.Exit(1)
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
	}

	addr := "localhost:50051"

	// Parse flags
	i := 0
	for i < len(args) {
		if args[i] == "--addr" {
			if i+1 >= len(args) {
				fmt.Fprintf(os.Stderr, "Error: --addr requires an argument\n")
				usage()
			}
			addr = args[i+1]
			i += 2
		} else {
			break
		}
	}

	remaining := args[i:]
	if len(remaining) == 0 {
		usage()
	}

	command := remaining[0]

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)

	switch command {
	case "set":
		if len(remaining) != 3 {
			fmt.Fprintf(os.Stderr, "Usage: barge-cli set <key> <value>\n")
			os.Exit(1)
		}
		key := remaining[1]
		value := remaining[2]
		doSet(ctx, client, key, value)
	case "get":
		if len(remaining) != 2 {
			fmt.Fprintf(os.Stderr, "Usage: barge-cli get <key>\n")
			os.Exit(1)
		}
		key := remaining[1]
		doGet(ctx, client, key)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		usage()
	}
}

func doSet(ctx context.Context, client pb.RaftServiceClient, key, value string) {
	req := &pb.ClientRequestMessage{
		Command: fmt.Sprintf("set %s %s", key, value),
	}
	resp, err := client.ClientRequest(ctx, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if !resp.Success {
		if resp.LeaderHint != "" {
			fmt.Fprintf(os.Stderr, "Not the leader. Try: %s\n", resp.LeaderHint)
		} else {
			fmt.Fprintf(os.Stderr, "Request failed (node may not be the leader)\n")
		}
		os.Exit(1)
	}
	fmt.Printf("OK\n")
}

func doGet(ctx context.Context, client pb.RaftServiceClient, key string) {
	req := &pb.ClientRequestMessage{
		Command: fmt.Sprintf("get %s", key),
	}
	resp, err := client.ClientRequest(ctx, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if !resp.Success {
		if resp.LeaderHint != "" {
			fmt.Fprintf(os.Stderr, "Not the leader. Try: %s\n", resp.LeaderHint)
		} else {
			fmt.Fprintf(os.Stderr, "Request failed (node may not be the leader)\n")
		}
		os.Exit(1)
	}
	fmt.Printf("%s\n", resp.Response)
}
