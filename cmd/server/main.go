package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"barge/consensus"
	"barge/storage"
	"barge/transport"
)

func main() {
	id := flag.String("id", "", "Node ID")
	addr := flag.String("addr", ":50051", "Listen address")
	peersStr := flag.String("peers", "", "Comma-separated list of peer id=addr pairs")
	flag.Parse()

	if *id == "" {
		log.Fatal("Node ID is required")
	}

	// Parse peers
	peers := make(map[string]string)
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) != 2 {
				log.Fatalf("Invalid peer format: %s", p)
			}
			peers[parts[0]] = parts[1]
		}
	}

	// Create components
	store := storage.NewStorage()
	grpcTransport := transport.NewGRPCTransport()

	// Create and register node
	node := consensus.NewNode(*id, *addr, peers, grpcTransport, store)
	grpcTransport.RegisterHandler(node)

	// Start transport
	if err := grpcTransport.Start(*addr); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	// Start consensus
	node.Start()

	fmt.Printf("Node %s started on %s\n", *id, *addr)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("Shutting down...")
	node.Stop()
	grpcTransport.Stop()
}
