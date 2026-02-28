package consensus

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/revolyssup/barge/proto"
	"github.com/revolyssup/barge/storage"
	"github.com/revolyssup/barge/transport"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// KVCommand represents a key-value set command to be replicated.
type KVCommand struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Node struct {
	mu sync.RWMutex

	// Persistent state
	id          string
	currentTerm uint64
	votedFor    string
	log         []*pb.LogEntry

	// Volatile state
	commitIndex uint64
	lastApplied uint64
	state       State
	leaderID    string

	// Leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Cluster info
	peers []string

	// Transport layer
	transport transport.Transport

	// Storage engine
	storage storage.Storage

	// Channels
	heartbeatCh chan struct{}
	stopCh      chan struct{}

	// apply notifications: closed when new entries are committed
	applyNotifyCh chan struct{}
}

// NewNode creates a new Raft node
func NewNode(id string, peers []string, t transport.Transport, s storage.Storage) *Node {
	n := &Node{
		id:            id,
		currentTerm:   0,
		votedFor:      "",
		log:           make([]*pb.LogEntry, 0),
		commitIndex:   0,
		lastApplied:   0,
		state:         Follower,
		peers:         peers,
		transport:     t,
		storage:       s,
		heartbeatCh:   make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		applyNotifyCh: make(chan struct{}, 1),
	}
	return n
}

// Start begins the node's operation
func (n *Node) Start() {
	go n.run()
	go n.applyLoop()
}

// Stop gracefully stops the node
func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) run() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.RLock()
		state := n.state
		n.mu.RUnlock()

		switch state {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func (n *Node) runFollower() {
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		n.mu.Lock()
		n.state = Candidate
		n.mu.Unlock()
	case <-n.heartbeatCh:
		// Reset timer on heartbeat
	case <-n.stopCh:
		return
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	currentTerm := n.currentTerm
	lastLogIndex := uint64(len(n.log))
	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLogTerm = n.log[lastLogIndex-1].Term
	}
	n.mu.Unlock()

	votes := 1 // Vote for self
	total := len(n.peers) + 1
	majority := total/2 + 1

	voteCh := make(chan bool, len(n.peers))

	for _, peer := range n.peers {
		go func(peer string) {
			resp, err := n.transport.SendRequestVote(peer, &pb.VoteRequest{
				Term:         currentTerm,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				voteCh <- false
				return
			}

			if resp.Term > currentTerm {
				n.mu.Lock()
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				n.mu.Unlock()
				voteCh <- false
				return
			}

			voteCh <- resp.VoteGranted
		}(peer)
	}

	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for i := 0; i < len(n.peers); i++ {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
			}
			if votes >= majority {
				n.mu.Lock()
				n.state = Leader
				n.leaderID = n.id
				// Initialize leader state
				for _, peer := range n.peers {
					n.nextIndex[peer] = uint64(len(n.log)) + 1
					n.matchIndex[peer] = 0
				}
				n.mu.Unlock()
				log.Printf("Node %s became leader for term %d", n.id, currentTerm)
				return
			}
		case <-timer.C:
			return
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) runLeader() {
	n.sendHeartbeats()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	select {
	case <-ticker.C:
		n.sendHeartbeats()
	case <-n.stopCh:
		return
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	currentTerm := n.currentTerm
	commitIndex := n.commitIndex
	n.mu.RUnlock()

	for _, peer := range n.peers {
		go func(peer string) {
			n.mu.RLock()
			nextIdx := n.nextIndex[peer]
			var prevLogIndex uint64
			var prevLogTerm uint64

			if nextIdx > 1 {
				prevLogIndex = nextIdx - 1
				if int(prevLogIndex) <= len(n.log) {
					prevLogTerm = n.log[prevLogIndex-1].Term
				}
			}

			// Send any unsent entries
			var entries []*pb.LogEntry
			if int(nextIdx) <= len(n.log) {
				entries = n.log[nextIdx-1:]
			}
			n.mu.RUnlock()

			resp, err := n.transport.SendAppendEntries(peer, &pb.AppendEntriesRequest{
				Term:         currentTerm,
				LeaderId:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				return
			}

			if resp.Success {
				if len(entries) > 0 {
					n.nextIndex[peer] = entries[len(entries)-1].Index + 1
					n.matchIndex[peer] = entries[len(entries)-1].Index
				}
				// Try to advance commit index
				n.advanceCommitIndex()
			} else {
				if n.nextIndex[peer] > 1 {
					n.nextIndex[peer]--
				}
			}
		}(peer)
	}
}

func (n *Node) advanceCommitIndex() {
	for idx := n.commitIndex + 1; idx <= uint64(len(n.log)); idx++ {
		if n.log[idx-1].Term != n.currentTerm {
			continue
		}

		matches := 1 // Leader has the entry
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				matches++
			}
		}

		total := len(n.peers) + 1
		if matches > total/2 {
			n.commitIndex = idx
		}
	}

	// Notify apply loop
	select {
	case n.applyNotifyCh <- struct{}{}:
	default:
	}
}

// applyLoop applies committed entries to the state machine (storage).
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyNotifyCh:
			n.applyCommitted()
		}
	}
}

// applyCommitted applies all entries between lastApplied and commitIndex.
func (n *Node) applyCommitted() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		var cmd KVCommand
		if err := json.Unmarshal(entry.Command, &cmd); err != nil {
			log.Printf("Node %s: failed to unmarshal command at index %d: %v", n.id, n.lastApplied, err)
			continue
		}
		n.storage.Set(cmd.Key, []byte(cmd.Value))
		log.Printf("Node %s: applied entry index=%d key=%s value=%s", n.id, n.lastApplied, cmd.Key, cmd.Value)
	}
}

// HandleAppendEntries processes incoming AppendEntries RPCs
func (n *Node) HandleAppendEntries(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &pb.AppendEntriesResponse{
			Term:    n.currentTerm,
			Success: false,
		}, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
	}
	n.state = Follower
	n.leaderID = req.LeaderId

	// Signal heartbeat received
	select {
	case n.heartbeatCh <- struct{}{}:
	default:
	}

	// Log consistency check
	if req.PrevLogIndex > 0 {
		if int(req.PrevLogIndex) > len(n.log) {
			return &pb.AppendEntriesResponse{
				Term:    n.currentTerm,
				Success: false,
			}, nil
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// Delete conflicting entries
			n.log = n.log[:req.PrevLogIndex-1]
			return &pb.AppendEntriesResponse{
				Term:    n.currentTerm,
				Success: false,
			}, nil
		}
	}

	// Append new entries
	for _, entry := range req.Entries {
		if int(entry.Index) <= len(n.log) {
			if n.log[entry.Index-1].Term != entry.Term {
				n.log = n.log[:entry.Index-1]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	// Update commit index
	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := uint64(len(n.log))
		if req.LeaderCommit < lastNewIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		// Notify apply loop
		select {
		case n.applyNotifyCh <- struct{}{}:
		default:
		}
	}

	return &pb.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: true,
	}, nil
}

// HandleRequestVote processes incoming RequestVote RPCs
func (n *Node) HandleRequestVote(req *pb.VoteRequest) (*pb.VoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &pb.VoteResponse{
			Term:        n.currentTerm,
			VoteGranted: false,
		}, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.state = Follower
	}

	// Check if we can vote for this candidate
	if n.votedFor == "" || n.votedFor == req.CandidateId {
		// Check log freshness
		lastLogIndex := uint64(len(n.log))
		var lastLogTerm uint64
		if lastLogIndex > 0 {
			lastLogTerm = n.log[lastLogIndex-1].Term
		}

		if req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			n.votedFor = req.CandidateId
			return &pb.VoteResponse{
				Term:        n.currentTerm,
				VoteGranted: true,
			}, nil
		}
	}

	return &pb.VoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}, nil
}

// HandleGet handles a client get request by reading from local storage.
func (n *Node) HandleGet(req *pb.GetRequest) (*pb.GetResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	val, err := n.storage.Get(req.Key)
	if err != nil {
		return &pb.GetResponse{
			Found: false,
			Error: fmt.Sprintf("key not found: %s", req.Key),
		}, nil
	}

	return &pb.GetResponse{
		Value: string(val),
		Found: true,
	}, nil
}

// HandleSet handles a client set request by proposing it through Raft.
// Only the leader can accept writes; followers return the leader address.
func (n *Node) HandleSet(req *pb.SetRequest) (*pb.SetResponse, error) {
	n.mu.Lock()

	if n.state != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		if leader == "" {
			return &pb.SetResponse{
				Success: false,
				Error:   "no leader elected, try again later",
			}, nil
		}
		return &pb.SetResponse{
			Success: false,
			Error:   fmt.Sprintf("not leader, try node: %s", leader),
		}, nil
	}

	// Marshal the command
	cmd := KVCommand{Key: req.Key, Value: req.Value}
	data, err := json.Marshal(cmd)
	if err != nil {
		n.mu.Unlock()
		return &pb.SetResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to marshal command: %v", err),
		}, nil
	}

	// Append to leader's log
	entry := &pb.LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log) + 1),
		Command: data,
	}
	n.log = append(n.log, entry)
	entryIndex := entry.Index
	n.mu.Unlock()

	// Send heartbeats immediately to replicate the new entry
	n.sendHeartbeats()

	// Wait for the entry to be committed (with timeout)
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return &pb.SetResponse{
				Success: false,
				Error:   "timeout waiting for commit",
			}, nil
		case <-ticker.C:
			n.mu.RLock()
			committed := n.commitIndex >= entryIndex
			n.mu.RUnlock()
			if committed {
				return &pb.SetResponse{
					Success: true,
				}, nil
			}
		}
	}
}

// GetState returns the current state of the node (for testing)
func (n *Node) GetState() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// GetTerm returns the current term (for testing)
func (n *Node) GetTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// GetLeaderID returns the current leader ID
func (n *Node) GetLeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}
