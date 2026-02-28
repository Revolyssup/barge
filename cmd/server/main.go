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

	grpcTransport := transport.NewGRPCTransport(*addr)
	client := transport.NewGRPCClient()

	node := consensus.NewNode(*id, peerList, client)
	grpcTransport.SetNode(node)

	if err := grpcTransport.Start(); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	node.Start()

	fmt.Printf("Node %s started on %s\n", *id, *addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	node.Stop()
	grpcTransport.Stop()
}
