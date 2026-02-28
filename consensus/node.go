package consensus

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "barge/proto"
	"barge/storage"
	"barge/transport"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Command represents a state machine command
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Node struct {
	mu sync.RWMutex

	// Node identity
	id    string
	addr  string
	peers map[string]string // id -> address

	// Persistent state
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

	// Components
	transport transport.Transport
	storage   *storage.Storage

	// Channels
	heartbeatCh chan struct{}
	stopCh      chan struct{}
}

// NewNode creates a new Raft node
func NewNode(id, addr string, peers map[string]string, t transport.Transport, s *storage.Storage) *Node {
	n := &Node{
		id:          id,
		addr:        addr,
		peers:       peers,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*pb.LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		state:       Follower,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		transport:   t,
		storage:     s,
		heartbeatCh: make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}

	return n
}

// Start begins the node's operation
func (n *Node) Start() {
	go n.run()
}

// Stop gracefully stops the node
func (n *Node) Stop() {
	close(n.stopCh)
}

// run is the main loop
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
	timeout := randomTimeout(150, 300)
	select {
	case <-n.heartbeatCh:
		return
	case <-timeout:
		n.mu.Lock()
		log.Printf("[%s] Election timeout, becoming candidate", n.id)
		n.state = Candidate
		n.mu.Unlock()
	case <-n.stopCh:
		return
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	currentTerm := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	n.mu.Unlock()

	log.Printf("[%s] Starting election for term %d", n.id, currentTerm)

	votes := 1 // Vote for self
	voteCh := make(chan bool, len(n.peers))

	for peerID, peerAddr := range n.peers {
		go func(id, addr string) {
			resp, err := n.transport.SendRequestVote(addr, &pb.RequestVoteRequest{
				Term:         currentTerm,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				log.Printf("[%s] RequestVote to %s failed: %v", n.id, id, err)
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
		}(peerID, peerAddr)
	}

	timeout := randomTimeout(150, 300)
	majority := (len(n.peers)+1)/2 + 1

	for i := 0; i < len(n.peers); i++ {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
				if votes >= majority {
					n.mu.Lock()
					log.Printf("[%s] Won election for term %d with %d votes", n.id, currentTerm, votes)
					n.state = Leader
					n.leaderID = n.id
					n.initLeaderState()
					n.mu.Unlock()
					return
				}
			}
		case <-timeout:
			return
		case <-n.stopCh:
			return
		}
	}

	// Did not win, go back to follower
	n.mu.Lock()
	n.state = Follower
	n.mu.Unlock()
}

func (n *Node) runLeader() {
	n.sendHeartbeats()

	heartbeatTicker := time.NewTicker(50 * time.Millisecond)
	defer heartbeatTicker.Stop()

	select {
	case <-heartbeatTicker.C:
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

	for peerID, peerAddr := range n.peers {
		go func(id, addr string) {
			n.mu.RLock()
			nextIdx := n.nextIndex[id]
			var prevLogIndex, prevLogTerm uint64
			if nextIdx > 0 {
				prevLogIndex = nextIdx - 1
				if prevLogIndex > 0 && int(prevLogIndex) <= len(n.log) {
					prevLogTerm = n.log[prevLogIndex-1].Term
				}
			}

			var entries []*pb.LogEntry
			if int(nextIdx) <= len(n.log) {
				if nextIdx == 0 {
					entries = n.log[:]
				} else {
					entries = n.log[nextIdx-1:]
				}
			}
			n.mu.RUnlock()

			resp, err := n.transport.SendAppendEntries(addr, &pb.AppendEntriesRequest{
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

			if resp.Term > currentTerm {
				n.mu.Lock()
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				n.mu.Unlock()
				return
			}

			if resp.Success {
				n.mu.Lock()
				if len(entries) > 0 {
					n.nextIndex[id] = entries[len(entries)-1].Index + 1
					n.matchIndex[id] = entries[len(entries)-1].Index
				}
				n.mu.Unlock()
				n.updateCommitIndex()
			} else {
				n.mu.Lock()
				if n.nextIndex[id] > 1 {
					n.nextIndex[id]--
				}
				n.mu.Unlock()
			}
		}(peerID, peerAddr)
	}
}

func (n *Node) initLeaderState() {
	lastIndex := uint64(len(n.log))
	for id := range n.peers {
		n.nextIndex[id] = lastIndex + 1
		n.matchIndex[id] = 0
	}
}

func (n *Node) updateCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for idx := n.commitIndex + 1; idx <= uint64(len(n.log)); idx++ {
		if n.log[idx-1].Term != n.currentTerm {
			continue
		}

		matches := 1 // Leader has it
		for id := range n.peers {
			if n.matchIndex[id] >= idx {
				matches++
			}
		}

		if matches > (len(n.peers)+1)/2 {
			n.commitIndex = idx
		}
	}

	n.applyEntries()
}

func (n *Node) applyEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]

		var cmd Command
		if err := json.Unmarshal(entry.Command, &cmd); err != nil {
			log.Printf("[%s] Failed to unmarshal command: %v", n.id, err)
			continue
		}

		switch cmd.Op {
		case "set":
			n.storage.Set(cmd.Key, cmd.Value)
			log.Printf("[%s] Applied: SET %s = %s", n.id, cmd.Key, cmd.Value)
		case "delete":
			n.storage.Delete(cmd.Key)
			log.Printf("[%s] Applied: DELETE %s", n.id, cmd.Key)
		}
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
		n.state = Follower
		n.votedFor = ""
	}

	n.leaderID = req.LeaderId

	// Signal heartbeat received
	select {
	case n.heartbeatCh <- struct{}{}:
	default:
	}

	// Check previous log entry
	if req.PrevLogIndex > 0 {
		if int(req.PrevLogIndex) > len(n.log) {
			return &pb.AppendEntriesResponse{
				Term:    n.currentTerm,
				Success: false,
			}, nil
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
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
		if req.LeaderCommit < uint64(len(n.log)) {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = uint64(len(n.log))
		}
		n.applyEntries()
	}

	return &pb.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: true,
	}, nil
}

// HandleRequestVote processes incoming RequestVote RPCs
func (n *Node) HandleRequestVote(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &pb.RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: false,
		}, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	// Check if we can grant the vote
	if n.votedFor == "" || n.votedFor == req.CandidateId {
		lastLogIndex, lastLogTerm := n.lastLogInfo()

		// Check if candidate's log is at least as up-to-date
		if req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			n.votedFor = req.CandidateId
			return &pb.RequestVoteResponse{
				Term:        n.currentTerm,
				VoteGranted: true,
			}, nil
		}
	}

	return &pb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}, nil
}

// HandleClientGet processes incoming client get requests
func (n *Node) HandleClientGet(req *pb.ClientGetRequest) (*pb.ClientGetResponse, error) {
	n.mu.RLock()
	state := n.state
	leaderID := n.leaderID
	n.mu.RUnlock()

	// If not the leader, redirect to leader
	if state != Leader {
		leaderAddr := ""
		if leaderID != "" {
			if addr, ok := n.peers[leaderID]; ok {
				leaderAddr = addr
			}
		}
		if leaderAddr != "" {
			return &pb.ClientGetResponse{
				Error:      "not the leader",
				LeaderAddr: leaderAddr,
			}, nil
		}
		return &pb.ClientGetResponse{
			Error: "no leader available",
		}, nil
	}

	value, found := n.storage.Get(req.Key)
	return &pb.ClientGetResponse{
		Value: value,
		Found: found,
	}, nil
}

// HandleClientSet processes incoming client set requests
func (n *Node) HandleClientSet(req *pb.ClientSetRequest) (*pb.ClientSetResponse, error) {
	n.mu.Lock()
	state := n.state
	leaderID := n.leaderID

	// If not the leader, redirect to leader
	if state != Leader {
		n.mu.Unlock()
		leaderAddr := ""
		if leaderID != "" {
			if addr, ok := n.peers[leaderID]; ok {
				leaderAddr = addr
			}
		}
		if leaderAddr != "" {
			return &pb.ClientSetResponse{
				Error:      "not the leader",
				LeaderAddr: leaderAddr,
			}, nil
		}
		return &pb.ClientSetResponse{
			Error: "no leader available",
		}, nil
	}

	// Create command
	cmd := Command{
		Op:    "set",
		Key:   req.Key,
		Value: req.Value,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		n.mu.Unlock()
		return &pb.ClientSetResponse{
			Error: fmt.Sprintf("failed to marshal command: %v", err),
		}, nil
	}

	// Append to log
	entry := &pb.LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log) + 1),
		Command: cmdBytes,
	}
	n.log = append(n.log, entry)
	entryIndex := entry.Index
	n.mu.Unlock()

	// Send to peers and wait for majority
	n.sendHeartbeats()

	// Wait for entry to be committed (with timeout)
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.RLock()
			committed := n.commitIndex >= entryIndex
			n.mu.RUnlock()
			if committed {
				return &pb.ClientSetResponse{
					Success: true,
				}, nil
			}
		case <-timeout:
			return &pb.ClientSetResponse{
				Error: "timeout waiting for commit",
			}, nil
		}
	}
}

func (n *Node) lastLogInfo() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func randomTimeout(min, max int) <-chan time.Time {
	d := time.Duration(min+rand.Intn(max-min)) * time.Millisecond
	return time.After(d)
}

// GetState returns the current state of the node (for testing/debugging)
func (n *Node) GetState() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// GetCurrentTerm returns the current term (for testing/debugging)
func (n *Node) GetCurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}
