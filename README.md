# Raft Consensus – Minimal Go Implementation

A clean, minimal implementation of the [Raft consensus algorithm](https://raft.github.io/) in Go with fully decoupled layers.

---

## Architecture

```
┌──────────────────────────────────────────────┐
│              Application (cmd/server)         │
│   Reads ApplyMsg channel, updates KV store   │
└─────────────┬────────────────────────────────┘
              │  ApplyMsg channel
┌─────────────▼────────────────────────────────┐
│            Consensus Layer  (consensus/)      │
│  Node – election timer, log replication,     │
│  commit index advancement                    │
│  Interface: transport.Transport (send RPCs)  │
│             storage.LogStorage  (log store)  │
└──────┬────────────────────────────┬──────────┘
       │                            │
┌──────▼──────┐             ┌───────▼──────────┐
│  Transport  │             │    Storage        │
│  Layer      │             │    Layer          │
│ transport/  │             │  storage/         │
│             │             │                   │
│ Interface:  │             │ Interface:        │
│ Transport   │             │ Storage           │
│ Server      │             │ LogStorage        │
│             │             │                   │
│ Impl:       │             │ Impl:             │
│ GRPCTransp. │             │ InMemoryStorage   │
│ GRPCServer  │             │ InMemoryLogStorage│
│             │             │                   │
│ Test double:│             └───────────────────┘
│ mockTransp. │
└─────────────┘
```

### Interfaces at a glance

| Interface | Package | Responsibility |
|-----------|---------|----------------|
| `transport.Transport` | transport | Send RequestVote / AppendEntries to a peer |
| `transport.Server` | transport | Listen for inbound RPCs and forward to Handler |
| `transport.Handler` | transport | Implemented by `consensus.Node` |
| `storage.Storage` | storage | `Get / Set / Delete` for the application state machine |
| `storage.LogStorage` | storage | Append / read / truncate Raft log entries |

---

## Quick start

### Prerequisites
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Regenerate proto (if you modify raft.proto)
```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/raft.proto
```
> The repo ships `proto/raft.pb.go` so you don't need to run protoc to build.

### Build & run a 3-node cluster
```bash
go mod tidy

# Terminal 1
go run ./cmd/server -id A -addr :9000 -peers :9001,:9002

# Terminal 2
go run ./cmd/server -id B -addr :9001 -peers :9000,:9002

# Terminal 3
go run ./cmd/server -id C -addr :9002 -peers :9000,:9001
```

### Run tests
```bash
go test ./...
```
All consensus tests use the **mock transport** – no network is needed.

---

## Layer details

### Storage (`storage/`)
| Type | Description |
|------|-------------|
| `InMemoryStorage` | Thread-safe `map[string][]byte`, satisfies `Storage` |
| `InMemoryLogStorage` | Slice-backed log with a sentinel at index 0, satisfies `LogStorage` |

### Transport (`transport/`)
| Type | Description |
|------|-------------|
| `GRPCTransport` | Pooled gRPC client; implements `Transport` |
| `GRPCServer` | `net.Listen` + `grpc.Server`; implements `Server` |
| `mockTransport` (test only) | Synchronous in-process delivery; no serialization |

### Consensus (`consensus/`)
`Node` implements the Raft state machine:
- **Leader election** – randomised election timeouts, RequestVote RPCs, majority quorum
- **Log replication** – AppendEntries RPCs, `nextIndex` / `matchIndex` per peer, back-off on conflict
- **Commit advancement** – counts replicated entries across peers to advance `commitIndex`
- **Apply loop** – pushes committed entries to `applyCh` for the application layer

---

## What's intentionally left out (for brevity)
- Persistent storage (log / currentTerm / votedFor to disk)
- Log compaction / snapshotting
- Membership changes (joint consensus)
- Read-only optimisation (linearisable reads via lease or read index)
