package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/revolyssup/barge/consensus"
	"github.com/revolyssup/barge/storage"
	"github.com/revolyssup/barge/transport"
)

func main() {
	id := flag.String("id", "", "Node ID")
	addr := flag.String("addr", ":50051", "Listen address")
	peers := flag.String("peers", "", "Comma-separated list of peer addresses")
	flag.Parse()

	if *id == "" {
		log.Fatal("Node ID is required")
	}

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	store := storage.NewMemoryStorage()

	// Create the node first with nil transport, then set it up
	node := consensus.NewNode(*id, peerList, nil, store)

	// Create transport with node as handler
	t := transport.NewGRPCTransport(node)

	// Update node's transport
	node.SetTransport(t)

	// Start transport
	if err := t.Start(*addr); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	// Start the node
	node.Start()

	fmt.Printf("Node %s started on %s\n", *id, *addr)

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	node.Stop()
	t.Stop()
}
