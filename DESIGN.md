# Barge: Building a Raft Consensus Implementation

This document explains the thought process behind building Barge, a Raft consensus implementation in Go. It's intended as both documentation and a guide for approaching similar distributed systems projects.

## Table of Contents

1. [Understanding the Problem](#understanding-the-problem)
2. [Architecture Overview](#architecture-overview)
3. [Layer-by-Layer Design](#layer-by-layer-design)
4. [The Raft Algorithm](#the-raft-algorithm)
5. [Implementation Strategy](#implementation-strategy)
6. [Testing Approach](#testing-approach)
7. [Lessons Learned](#lessons-learned)

---

## Understanding the Problem

### What is Consensus?

In distributed systems, multiple servers need to agree on shared state. If you have 3 servers and a client writes `x=5`, all 3 servers should eventually agree that `x=5`. This is the **consensus problem**.

### Why Raft?

Before Raft, Paxos was the standard consensus algorithm. Paxos is notoriously difficult to understand and implement correctly. Raft was designed in 2014 specifically for understandability while providing the same guarantees:

- **Safety**: Never returns incorrect results (even with network partitions, delays, duplicates, reordering)
- **Availability**: Functional as long as majority of servers are operational
- **Consistency**: Doesn't depend on timing for consistency (clocks can be wrong)

### The Core Insight

Raft decomposes consensus into three sub-problems:

1. **Leader Election**: One server is elected leader; others are followers
2. **Log Replication**: Leader accepts commands, replicates to followers
3. **Safety**: If a server has applied a log entry, no other server will apply a different entry for that index

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      cmd/server/main.go                      │
│                    (Wiring + HTTP API)                       │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│    consensus/   │  │    transport/   │  │     storage/    │
│                 │  │                 │  │                 │
│  Raft State     │  │  gRPC Client    │  │  Log Storage    │
│  Machine        │  │  gRPC Server    │  │  KV Storage     │
│                 │  │                 │  │                 │
│  (Leader        │  │  (Network       │  │  (Persistence   │
│   Election,     │  │   Abstraction)  │  │   Abstraction)  │
│   Log Repl.)    │  │                 │  │                 │
└─────────────────┘  └─────────────────┘  └─────────────────┘
          │                   │                   │
          └───────────────────┴───────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │     proto/      │
                    │  (gRPC types)   │
                    └─────────────────┘
```

### Why This Layering?

**Separation of concerns** is critical in distributed systems:

1. **consensus/** - Pure algorithm logic. No network code, no disk I/O. This makes it testable and easier to reason about correctness.

2. **transport/** - Network abstraction. The consensus layer doesn't know if it's using gRPC, HTTP, or in-memory channels. This enables:
   - Unit testing with mock transport
   - Swapping protocols without touching consensus code

3. **storage/** - Persistence abstraction. Currently in-memory, but the interface allows swapping in disk-backed storage (BoltDB, SQLite, etc.) without changing consensus code.

4. **proto/** - Wire format definitions. Separate from business logic.

---

## Layer-by-Layer Design

### 1. Storage Layer (`storage/`)

**Start here.** Before implementing consensus, define what needs to be stored.

```go
// What the state machine needs
type Storage interface {
    Get(key string) ([]byte, error)
    Set(key string, value []byte) error
    Delete(key string) error
}

// What Raft needs for its log
type LogStorage interface {
    AppendLog(entry LogEntry) error
    GetLog(index uint64) (LogEntry, error)
    LastIndex() uint64
    LastTerm() uint64
    Entries(from, to uint64) ([]LogEntry, error)
    TruncateSuffix(from uint64) error  // For log conflicts
}
```

**Design decisions:**

- Separate KV storage from log storage (different access patterns)
- In-memory implementation first (simplest thing that works)
- Interface allows disk-backed implementation later
- Thread-safe with `sync.RWMutex`

### 2. Transport Layer (`transport/`)

**Define the network boundary.** What RPCs does Raft need?

```go
// Outbound: consensus calls these to reach peers
type Transport interface {
    SendRequestVote(ctx context.Context, addr string, args RequestVoteArgs) (RequestVoteReply, error)
    SendAppendEntries(ctx context.Context, addr string, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// Inbound: transport calls these when receiving RPCs
type Handler interface {
    HandleRequestVote(args RequestVoteArgs) RequestVoteReply
    HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply
}
```

**Design decisions:**

- Transport-agnostic types (`RequestVoteArgs`, not protobuf types)
- Context for timeout/cancellation
- Address as string (could be `host:port`, node ID, etc.)
- Handler interface so consensus node can receive RPCs

**gRPC Implementation:**

- Connection pooling (don't dial on every RPC)
- Address normalization (`:9000` → `localhost:9000`)
- Graceful shutdown

### 3. Consensus Layer (`consensus/`)

**The heart of Raft.** This is where correctness matters most.

```go
type Node struct {
    // Configuration
    cfg Config

    // Persistent state (survives restarts)
    currentTerm uint64
    votedFor    string
    log         storage.LogStorage

    // Volatile state (rebuilt after restart)
    commitIndex uint64
    lastApplied uint64

    // Leader-only state
    nextIndex  map[string]uint64
    matchIndex map[string]uint64

    // Runtime
    role      Role  // Follower, Candidate, or Leader
    transport transport.Transport
    applyCh   chan ApplyMsg
}
```

**Key insight:** The Raft paper is your specification. Implement exactly what it says.

### 4. Application Layer (`cmd/server/`)

**Wire everything together** and expose an API:

```go
func main() {
    // 1. Create storage
    kvStore := storage.NewInMemoryStorage()
    logStore := storage.NewInMemoryLogStorage()

    // 2. Create transport
    grpcTransport := transport.NewGRPCTransport()
    grpcServer := transport.NewGRPCServer()

    // 3. Create consensus node
    node := consensus.NewNode(cfg, logStore, grpcTransport, applyCh)

    // 4. Connect them
    grpcServer.Start(addr, node)  // node implements Handler
    node.Start()

    // 5. Apply committed entries to state machine
    go func() {
        for msg := range applyCh {
            // Apply to kvStore
        }
    }()

    // 6. HTTP API for clients
    startHTTPServer(...)
}
```

---

## The Raft Algorithm

### State Machine

```
                    times out,
                    starts election
         ┌──────────────────────────────┐
         │                              │
         ▼                              │
    ┌─────────┐                    ┌────┴─────┐
    │Follower │───────────────────▶│Candidate │
    └─────────┘   times out,       └──────────┘
         ▲        starts election       │
         │                              │
         │    discovers leader or       │ receives majority
         │    higher term               │ of votes
         │                              │
         │        ┌────────┐            │
         └────────│ Leader │◀───────────┘
                  └────────┘
```

### Leader Election

1. **Follower** waits for heartbeats from leader
2. If no heartbeat within election timeout → become **Candidate**
3. **Candidate** increments term, votes for self, requests votes from peers
4. If majority votes received → become **Leader**
5. If higher term discovered → become **Follower**

**Critical detail:** Randomized election timeout (150-300ms) prevents split votes.

### Log Replication

1. Client sends command to **Leader**
2. Leader appends to local log
3. Leader sends `AppendEntries` to all followers
4. When majority acknowledge → entry is **committed**
5. Leader notifies followers of commit in next heartbeat
6. All nodes apply committed entries to state machine

### Safety: The Election Restriction

A candidate's log must be "at least as up-to-date" as any other server to win election:

```go
logOK := args.LastLogTerm > lastTerm ||
    (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
```

This ensures the leader always has all committed entries.

---

## Implementation Strategy

### Phase 1: Interfaces First

Define all interfaces before writing implementations:

```go
// storage/storage.go - interfaces only
// transport/transport.go - interfaces only
```

This forces you to think about boundaries and enables testing.

### Phase 2: Simplest Implementation

```go
// storage/storage.go - add InMemoryStorage
// transport/grpc.go - real gRPC implementation
```

Don't optimize. Don't add features. Make it work.

### Phase 3: Core Algorithm

Implement in this order (each builds on previous):

1. **RequestVote RPC handler** - easiest, stateless decision
2. **AppendEntries RPC handler** - heartbeats first, then log entries
3. **Election timer** - follower timeout → candidate
4. **Leader election** - send RequestVote, count votes
5. **Heartbeats** - leader sends empty AppendEntries
6. **Log replication** - leader sends actual entries
7. **Commit advancement** - track what's replicated on majority

### Phase 4: Edge Cases

- Single-node cluster (should immediately become leader)
- Network partitions (leader in minority steps down)
- Log conflicts (truncate and re-replicate)

### Phase 5: Client Interface

Only add HTTP/CLI after consensus works:

```go
// cmd/server/main.go - HTTP API
```

---

## Testing Approach

### Unit Tests (Fast, Deterministic)

Test RPC handlers in isolation:

```go
func TestHandleRequestVote_GrantsVote(t *testing.T) {
    node := makeNode("A")
    reply := node.HandleRequestVote(transport.RequestVoteArgs{
        Term: 2, CandidateID: "B",
    })
    if !reply.VoteGranted {
        t.Fatal("expected vote granted")
    }
}
```

### Integration Tests (Mock Transport)

Test multi-node behavior without network:

```go
type mockTransport struct {
    handlers map[string]transport.Handler
}

func (m *mockTransport) SendRequestVote(...) {
    // Directly call handler - no network
    return m.handlers[addr].HandleRequestVote(args), nil
}
```

This lets you test election, replication, and failure scenarios quickly.

### Chaos Testing (Real Network)

For production readiness:
- Kill nodes randomly
- Partition networks
- Delay/reorder messages
- Run for hours looking for invariant violations

---

## Lessons Learned

### 1. The Paper is the Spec

The Raft paper (https://raft.github.io/raft.pdf) Figure 2 is your checklist. Implement it literally.

### 2. Logging is Essential

In distributed systems, `log.Printf` is your debugger:

```go
log.Printf("[%s] starting election for term %d", n.cfg.ID, term)
log.Printf("[%s] → Leader (term %d)", n.cfg.ID, n.currentTerm)
```

### 3. Time is the Enemy

- Never assume clocks are synchronized
- Always use timeouts
- Randomize to break symmetry

### 4. Concurrency is Hard

- Hold locks for minimum time
- Be careful about lock ordering
- Use channels for coordination, not data sharing

### 5. Test the Sad Path

Most bugs appear during:
- Leader failure during replication
- Network partition healing
- Multiple simultaneous elections

### 6. Start Simple

The first version of Barge:
- In-memory only (no persistence)
- No snapshots
- No membership changes
- No read optimization

These can be added later without changing the core algorithm.

---

## File Structure Summary

```
barge/
├── cmd/server/
│   └── main.go           # Wiring + HTTP API
├── consensus/
│   ├── node.go           # Raft state machine
│   ├── node_test.go      # Integration tests
│   └── mock_transport_test.go
├── storage/
│   ├── storage.go        # Interfaces + in-memory impl
│   └── storage_test.go
├── transport/
│   ├── transport.go      # Interfaces + types
│   └── grpc.go           # gRPC implementation
├── proto/
│   ├── raft.proto        # Protocol definition
│   └── raft.pb.go        # Generated code
├── Makefile
├── go.mod
└── README.md
```

---

## Further Reading

1. [Raft Paper](https://raft.github.io/raft.pdf) - The original paper
2. [Raft Visualization](https://raft.github.io/) - Interactive demo
3. [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/) - Common pitfalls
4. [TiKV's Raft Implementation](https://github.com/tikv/raft-rs) - Production-grade Rust implementation
5. [etcd's Raft](https://github.com/etcd-io/raft) - Production-grade Go implementation

---

## What's Next?

Barge is a minimal implementation. Production systems add:

1. **Persistence** - Write log to disk before acknowledging
2. **Snapshots** - Compact log to prevent unbounded growth
3. **Membership Changes** - Add/remove nodes at runtime
4. **Read Optimization** - Serve reads from followers (with lease)
5. **Batching** - Amortize RPC overhead
6. **Pipelining** - Don't wait for each entry to be acked

Each of these is a significant undertaking. The beauty of the layered architecture is that most can be added without touching the core consensus logic.
