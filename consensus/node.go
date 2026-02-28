// Package consensus implements the Raft consensus algorithm.
//
// Architecture:
//   - Node   – the main Raft state machine (leader election + log replication).
//   - Config – how to configure a node.
//
// The consensus layer depends only on the transport and storage interfaces;
// it never imports concrete implementations.
package consensus

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/example/raft/storage"
	"github.com/example/raft/transport"
)

// Role represents the Raft node role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

// Config holds all parameters needed to create a Raft node.
type Config struct {
	ID    string   // this node's identifier
	Peers []string // addresses of all OTHER nodes (used by transport)

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

func DefaultConfig(id string, peers []string) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		HeartbeatInterval:  150 * time.Millisecond,
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
	}
}

// ApplyMsg is delivered to the application state machine when a log entry is committed.
type ApplyMsg struct {
	Index   uint64
	Command []byte
}

// Node is a single Raft participant.
type Node struct {
	mu  sync.Mutex
	cfg Config

	// Persistent state (simplified – not actually persisted to disk here)
	currentTerm uint64
	votedFor    string
	log         storage.LogStorage

	// Volatile state
	commitIndex uint64
	lastApplied uint64

	// Leader-only volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	role     Role
	leaderID string

	transport transport.Transport

	// Channel on which committed entries are pushed to the application layer.
	applyCh chan ApplyMsg

	// Internal signals
	resetElectionTimer chan struct{}
	stopCh             chan struct{}
	wg                 sync.WaitGroup
}

// NewNode creates a Raft node.  Call Start() to begin participation.
func NewNode(cfg Config, logStore storage.LogStorage, tr transport.Transport, applyCh chan ApplyMsg) *Node {
	return &Node{
		cfg:                cfg,
		log:                logStore,
		transport:          tr,
		applyCh:            applyCh,
		role:               Follower,
		resetElectionTimer: make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		nextIndex:          make(map[string]uint64),
		matchIndex:         make(map[string]uint64),
	}
}

// Start launches background goroutines.
func (n *Node) Start() {
	n.wg.Add(1)
	go n.run()
}

// Stop shuts down the node gracefully.
func (n *Node) Stop() {
	close(n.stopCh)
	n.wg.Wait()
}

// Submit proposes a command to the cluster.  Returns false if this node is not the leader.
func (n *Node) Submit(command []byte) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return false
	}
	idx := n.log.LastIndex() + 1
	entry := storage.LogEntry{Index: idx, Term: n.currentTerm, Command: command}
	if err := n.log.AppendLog(entry); err != nil {
		return false
	}
	// Trigger immediate replication
	n.broadcastAppendEntries()
	return true
}

// IsLeader reports whether this node currently believes itself to be the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// CurrentTerm returns the node's current term.
func (n *Node) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// -------------------------------------------------------------------------
// transport.Handler implementation (called by the transport layer)
// -------------------------------------------------------------------------

func (n *Node) HandleRequestVote(args transport.RequestVoteArgs) transport.RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := transport.RequestVoteReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply // stale
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	// Grant vote if we haven't voted yet (or already voted for this candidate)
	// and the candidate's log is at least as up-to-date as ours.
	lastIdx := n.log.LastIndex()
	lastTerm := n.log.LastTerm()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOK {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.sendResetElection()
	}
	reply.Term = n.currentTerm
	return reply
}

func (n *Node) HandleAppendEntries(args transport.AppendEntriesArgs) transport.AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := transport.AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	// Recognize leader
	n.role = Follower
	n.leaderID = args.LeaderID
	n.sendResetElection()

	// Consistency check
	if args.PrevLogIndex > 0 {
		prevEntry, err := n.log.GetLog(args.PrevLogIndex)
		if err != nil || prevEntry.Term != args.PrevLogTerm {
			return reply // log doesn't match
		}
	}

	// Append new entries (overwriting conflicts)
	for _, e := range args.Entries {
		existing, err := n.log.GetLog(e.Index)
		if err == nil && existing.Term != e.Term {
			// Conflict: truncate from here
			_ = n.log.TruncateSuffix(e.Index)
		}
		if e.Index > n.log.LastIndex() {
			_ = n.log.AppendLog(storage.LogEntry{
				Index:   e.Index,
				Term:    e.Term,
				Command: e.Command,
			})
		}
	}

	// Advance commit index
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min64(args.LeaderCommit, n.log.LastIndex())
		n.applyLogs()
	}

	reply.Success = true
	return reply
}

// -------------------------------------------------------------------------
// Main run loop
// -------------------------------------------------------------------------

func (n *Node) run() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		role := n.role
		n.mu.Unlock()

		switch role {
		case Follower, Candidate:
			n.runElectionTimer()
		case Leader:
			n.runHeartbeat()
		}
	}
}

func (n *Node) runElectionTimer() {
	timeout := n.electionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-n.resetElectionTimer:
			// Received heartbeat or granted vote; reset timer.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.electionTimeout())
		case <-timer.C:
			// Timeout expired: start an election.
			n.startElection()
			return
		}
	}
}

func (n *Node) runHeartbeat() {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != Leader {
				n.mu.Unlock()
				return
			}
			n.broadcastAppendEntries()
			n.mu.Unlock()
		}
	}
}

// -------------------------------------------------------------------------
// Election
// -------------------------------------------------------------------------

func (n *Node) startElection() {
	n.mu.Lock()
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.cfg.ID
	term := n.currentTerm
	lastIdx := n.log.LastIndex()
	lastTerm := n.log.LastTerm()
	peers := n.cfg.Peers
	n.mu.Unlock()

	log.Printf("[%s] starting election for term %d", n.cfg.ID, term)

	votes := 1 // vote for self
	needed := (len(peers)+1)/2 + 1 // majority including self
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	// Single node cluster: already have majority
	if votes >= needed {
		n.mu.Lock()
		if n.role == Candidate && n.currentTerm == term {
			n.becomeLeader()
		}
		n.mu.Unlock()
		return
	}

	for _, peer := range peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			reply, err := n.transport.SendRequestVote(ctx, addr, transport.RequestVoteArgs{
				Term:         term,
				CandidateID:  n.cfg.ID,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				won := votes >= needed
				voteMu.Unlock()
				if won {
					n.mu.Lock()
					if n.role == Candidate && n.currentTerm == term {
						n.becomeLeader()
					}
					n.mu.Unlock()
				}
			}
		}(peer)
	}
	wg.Wait()
}

// -------------------------------------------------------------------------
// Log replication
// -------------------------------------------------------------------------

// broadcastAppendEntries sends AppendEntries to all peers.
// Must be called with n.mu held.
func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.cfg.Peers {
		go n.sendAppendEntries(peer)
	}
}

func (n *Node) sendAppendEntries(peer string) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peer]
	if nextIdx == 0 {
		nextIdx = 1
	}
	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 {
		if e, err := n.log.GetLog(prevLogIndex); err == nil {
			prevLogTerm = e.Term
		}
	}

	lastIdx := n.log.LastIndex()
	var entries []transport.LogEntry
	for i := nextIdx; i <= lastIdx; i++ {
		e, err := n.log.GetLog(i)
		if err != nil {
			break
		}
		entries = append(entries, transport.LogEntry{Index: e.Index, Term: e.Term, Command: e.Command})
	}

	args := transport.AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	reply, err := n.transport.SendAppendEntries(ctx, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		newMatch := prevLogIndex + uint64(len(entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitIndex()
	} else {
		// Back off
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

// advanceCommitIndex finds the highest index replicated on a majority.
// Must be called with n.mu held.
func (n *Node) advanceCommitIndex() {
	lastIdx := n.log.LastIndex()
	for idx := lastIdx; idx > n.commitIndex; idx-- {
		e, err := n.log.GetLog(idx)
		if err != nil || e.Term != n.currentTerm {
			continue
		}
		count := 1 // self
		for _, peer := range n.cfg.Peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		needed := (len(n.cfg.Peers)+1)/2 + 1
		if count >= needed {
			n.commitIndex = idx
			n.applyLogs()
			break
		}
	}
}

// -------------------------------------------------------------------------
// State transitions (must be called with n.mu held)
// -------------------------------------------------------------------------

func (n *Node) becomeFollower(term uint64) {
	log.Printf("[%s] → Follower (term %d)", n.cfg.ID, term)
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.sendResetElection()
}

func (n *Node) becomeLeader() {
	log.Printf("[%s] → Leader (term %d)", n.cfg.ID, n.currentTerm)
	n.role = Leader
	n.leaderID = n.cfg.ID
	lastIdx := n.log.LastIndex()
	for _, peer := range n.cfg.Peers {
		n.nextIndex[peer] = lastIdx + 1
		n.matchIndex[peer] = 0
	}
}

// -------------------------------------------------------------------------
// Apply loop
// -------------------------------------------------------------------------

// applyLogs delivers committed-but-not-yet-applied entries to the apply channel.
// Must be called with n.mu held.
func (n *Node) applyLogs() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e, err := n.log.GetLog(n.lastApplied)
		if err != nil {
			break
		}
		select {
		case n.applyCh <- ApplyMsg{Index: e.Index, Command: e.Command}:
		default:
		}
	}
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

func (n *Node) electionTimeout() time.Duration {
	lo := int64(n.cfg.ElectionTimeoutMin)
	hi := int64(n.cfg.ElectionTimeoutMax)
	return time.Duration(lo + rand.Int63n(hi-lo))
}

func (n *Node) sendResetElection() {
	select {
	case n.resetElectionTimer <- struct{}{}:
	default:
	}
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
