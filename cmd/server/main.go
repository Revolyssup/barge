// cmd/server/main.go wires all three layers together into a runnable Raft node.
//
// Usage:
//
//	go run ./cmd/server -id A -addr :9000 -http :8000 -peers :9001,:9002
//	go run ./cmd/server -id B -addr :9001 -http :8001 -peers :9000,:9002
//	go run ./cmd/server -id C -addr :9002 -http :8002 -peers :9000,:9001
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/example/raft/consensus"
	"github.com/example/raft/storage"
	"github.com/example/raft/transport"
)

func main() {
	id := flag.String("id", "A", "node identifier")
	addr := flag.String("addr", ":9000", "gRPC listen address")
	httpAddr := flag.String("http", ":8000", "HTTP API listen address")
	peersFlag := flag.String("peers", "", "comma-separated peer addresses")
	flag.Parse()

	peers := []string{}
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	// ── Storage layer ────────────────────────────────────────────────────
	kvStore := storage.NewInMemoryStorage()
	logStore := storage.NewInMemoryLogStorage()

	// ── Transport layer ──────────────────────────────────────────────────
	grpcTransport := transport.NewGRPCTransport()
	grpcServer := transport.NewGRPCServer()

	// ── Consensus layer ──────────────────────────────────────────────────
	cfg := consensus.DefaultConfig(*id, peers)
	applyCh := make(chan consensus.ApplyMsg, 256)
	node := consensus.NewNode(cfg, logStore, grpcTransport, applyCh)

	// Start the gRPC server BEFORE the node so incoming RPCs are handled
	// from the first moment the node tries to communicate.
	if err := grpcServer.Start(*addr, node); err != nil {
		log.Fatalf("grpc server: %v", err)
	}
	log.Printf("node %s listening on gRPC=%s, HTTP=%s, peers=%v", *id, *addr, *httpAddr, peers)

	node.Start()

	// ── Application state machine ────────────────────────────────────────
	// Consume committed entries and apply them to the KV store.
	go func() {
		for msg := range applyCh {
			// Very simple command format: "SET key value"
			parts := strings.SplitN(string(msg.Command), " ", 3)
			if len(parts) == 3 && parts[0] == "SET" {
				_ = kvStore.Set(parts[1], []byte(parts[2]))
				log.Printf("applied [%d] SET %s=%s", msg.Index, parts[1], parts[2])
			}
		}
	}()

	// ── HTTP API ─────────────────────────────────────────────────────────
	httpServer := startHTTPServer(*httpAddr, *id, node, kvStore)

	// ── Graceful shutdown ────────────────────────────────────────────────
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down…")
	httpServer.Close()
	node.Stop()
	grpcServer.Stop()
	grpcTransport.Close()
}

func startHTTPServer(addr, nodeID string, node *consensus.Node, kv *storage.InMemoryStorage) *http.Server {
	mux := http.NewServeMux()

	// GET /status - returns node status
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		role := "follower"
		if node.IsLeader() {
			role = "leader"
		}
		resp := map[string]interface{}{
			"id":     nodeID,
			"role":   role,
			"term":   node.CurrentTerm(),
			"leader": node.IsLeader(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	// GET /get?key=xxx - reads a key from the KV store
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, `{"error": "missing key parameter"}`, http.StatusBadRequest)
			return
		}
		val, err := kv.Get(key)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error": "key not found: %s"}`, key), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"key":   key,
			"value": string(val),
		})
	})

	// POST /set - sets a key-value pair via Raft consensus
	// Body: {"key": "foo", "value": "bar"}
	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error": "invalid JSON body"}`, http.StatusBadRequest)
			return
		}
		if req.Key == "" {
			http.Error(w, `{"error": "missing key"}`, http.StatusBadRequest)
			return
		}

		// Submit command to Raft
		cmd := fmt.Sprintf("SET %s %s", req.Key, req.Value)
		if !node.Submit([]byte(cmd)) {
			http.Error(w, `{"error": "not the leader, cannot accept writes"}`, http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status": "accepted",
			"key":    req.Key,
			"value":  req.Value,
		})
	})

	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()
	return server
}
