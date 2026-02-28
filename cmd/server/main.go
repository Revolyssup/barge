package main

import (
	"flag"
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
	addr := flag.String("addr", "", "Node address (host:port)")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer id=addr pairs")
	flag.Parse()

	if *id == "" || *addr == "" {
		log.Fatal("id and addr are required")
	}

	peers := make(map[string]string)
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) == 2 {
				peers[parts[0]] = parts[1]
			}
		}
	}

	t := transport.NewGRPCTransport(*addr)
	s := storage.NewStorage()
	node := consensus.NewNode(*id, *addr, peers, t, s)

	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	log.Printf("Node %s started successfully", *id)

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	node.Stop()
}
