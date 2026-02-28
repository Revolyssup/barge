package main

import (
	"context"
	"fmt"
	"os"
	"time"

	pb "barge/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultAddr    = "localhost:50051"
	maxRedirects   = 5
	requestTimeout = 10 * time.Second
)

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: barge <command> [options] [args...]

Commands:
  get    Get a value by key
  set    Set a key-value pair

Examples:
  barge get --addr localhost:50051 mykey
  barge set --addr localhost:50051 mykey myvalue

Options:
  --addr <address>   Address of the barge node (default: %s)
`, defaultAddr)
	os.Exit(1)
}

func parseArgs(args []string) (addr string, remaining []string) {
	addr = defaultAddr
	i := 0
	for i < len(args) {
		if args[i] == "--addr" {
			if i+1 >= len(args) {
				fmt.Fprintf(os.Stderr, "Error: --addr requires an argument\n")
				os.Exit(1)
			}
			addr = args[i+1]
			i += 2
		} else {
			remaining = append(remaining, args[i])
			i++
		}
	}
	return
}

func connect(addr string) (pb.RaftServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	client := pb.NewRaftServiceClient(conn)
	return client, conn, nil
}

func doGet(addr string, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	currentAddr := addr
	for i := 0; i < maxRedirects; i++ {
		client, conn, err := connect(currentAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		resp, err := client.ClientGet(ctx, &pb.ClientGetRequest{Key: key})
		conn.Close()

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if resp.Error != "" && resp.LeaderAddr != "" {
			// Not the leader, follow redirect
			fmt.Fprintf(os.Stderr, "Redirecting to leader at %s...\n", resp.LeaderAddr)
			currentAddr = resp.LeaderAddr
			continue
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
		return
	}

	fmt.Fprintf(os.Stderr, "Error: too many redirects\n")
	os.Exit(1)
}

func doSet(addr string, key string, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	currentAddr := addr
	for i := 0; i < maxRedirects; i++ {
		client, conn, err := connect(currentAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		resp, err := client.ClientSet(ctx, &pb.ClientSetRequest{Key: key, Value: value})
		conn.Close()

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if resp.Error != "" && resp.LeaderAddr != "" {
			// Not the leader, follow redirect
			fmt.Fprintf(os.Stderr, "Redirecting to leader at %s...\n", resp.LeaderAddr)
			currentAddr = resp.LeaderAddr
			continue
		}

		if resp.Error != "" {
			fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
			os.Exit(1)
		}

		if resp.Success {
			fmt.Println("OK")
		} else {
			fmt.Fprintf(os.Stderr, "Error: set operation failed\n")
			os.Exit(1)
		}
		return
	}

	fmt.Fprintf(os.Stderr, "Error: too many redirects\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	command := os.Args[1]
	addr, remaining := parseArgs(os.Args[2:])

	switch command {
	case "get":
		if len(remaining) != 1 {
			fmt.Fprintf(os.Stderr, "Usage: barge get [--addr <address>] <key>\n")
			os.Exit(1)
		}
		doGet(addr, remaining[0])

	case "set":
		if len(remaining) != 2 {
			fmt.Fprintf(os.Stderr, "Usage: barge set [--addr <address>] <key> <value>\n")
			os.Exit(1)
		}
		doSet(addr, remaining[0], remaining[1])

	case "help", "--help", "-h":
		usage()

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		usage()
	}
}
