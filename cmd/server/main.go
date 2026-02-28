package main

import (
	"flag"
	"fmt"
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
		fmt.Println("Error: -id is required")
		os.Exit(1)
	}

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	store := storage.NewStorage()
	t := transport.NewGRPCTransport(*addr)
	node := consensus.NewNode(*id, peerList, t, store)

	// Set the node on the transport so it can handle incoming RPCs
	t.SetNode(node)

	if err := t.Start(); err != nil {
		fmt.Printf("Failed to start transport: %v\n", err)
		os.Exit(1)
	}

	node.Start()
	fmt.Printf("Node %s started on %s\n", *id, *addr)

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	node.Stop()
	t.Stop()
}
