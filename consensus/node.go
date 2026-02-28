package consensus

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/revolyssup/barge/proto"
	"github.com/revolyssup/barge/storage"
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

type Transport interface {
	SendRequestVote(addr string, req *pb.VoteRequest) (*pb.VoteResponse, error)
	SendAppendEntries(addr string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

type commitWaiter struct {
	index uint64
	ch    chan struct{}
}

type Node struct {
	mu sync.Mutex

	id    string
	state State

	currentTerm uint64
	votedFor    string
	log         []*pb.LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	peers     []string
	transport Transport

	electionTimeout *time.Timer
	heartbeatTicker *time.Ticker

	stopCh chan struct{}

	leaderID string

	storage *storage.Storage

	commitWaiters []commitWaiter
}

func NewNode(id string, peers []string, transport Transport) *Node {
	n := &Node{
		id:          id,
		state:       Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*pb.LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		peers:       peers,
		transport:   transport,
		stopCh:      make(chan struct{}),
		storage:     storage.NewStorage(),
	}
	return n
}

func (n *Node) Start() {
	n.resetElectionTimer()
	go n.run()
}

func (n *Node) Stop() {
	close(n.stopCh)
	if n.electionTimeout != nil {
		n.electionTimeout.Stop()
	}
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
	}
}

func (n *Node) run() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimeout.C:
			n.mu.Lock()
			if n.state != Leader {
				n.startElection()
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) resetElectionTimer() {
	timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond // random between 300ms and 500ms
	if n.electionTimeout == nil {
		n.electionTimeout = time.NewTimer(timeout)
	} else {
		n.electionTimeout.Reset(timeout)
	}
}

func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.resetElectionTimer()

	votes := 1
	term := n.currentTerm

	var lastLogIndex, lastLogTerm uint64
	if len(n.log) > 0 {
		lastEntry := n.log[len(n.log)-1]
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	}

	for _, peer := range n.peers {
		go func(peer string) {
			req := &pb.VoteRequest{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			resp, err := n.transport.SendRequestVote(peer, req)
			if err != nil {
				log.Printf("Node %s: Error sending RequestVote to %s: %v", n.id, peer, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				return
			}

			if resp.VoteGranted {
				votes++
				if votes > (len(n.peers)+1)/2 {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	log.Printf("Node %s became leader for term %d", n.id, n.currentTerm)
	n.state = Leader
	n.leaderID = n.id

	var lastLogIndex uint64
	if len(n.log) > 0 {
		lastLogIndex = n.log[len(n.log)-1].Index
	}

	for _, peer := range n.peers {
		n.nextIndex[peer] = lastLogIndex + 1
		n.matchIndex[peer] = 0
	}

	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
	}
	n.heartbeatTicker = time.NewTicker(100 * time.Millisecond)

	go n.sendHeartbeats()
}

func (n *Node) sendHeartbeats() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.heartbeatTicker.C:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			n.sendAppendEntriesToAll()
			n.mu.Unlock()
		}
	}
}

func (n *Node) sendAppendEntriesToAll() {
	for _, peer := range n.peers {
		go n.sendAppendEntriesToPeer(peer)
	}
}

func (n *Node) sendAppendEntriesToPeer(peer string) {
	n.mu.Lock()

	nextIdx := n.nextIndex[peer]
	var prevLogIndex, prevLogTerm uint64

	if nextIdx > 1 {
		prevEntry := n.log[nextIdx-2]
		prevLogIndex = prevEntry.Index
		prevLogTerm = prevEntry.Term
	}

	entries := make([]*pb.LogEntry, 0)
	if nextIdx <= uint64(len(n.log)) {
		entries = n.log[nextIdx-1:]
	}

	req := &pb.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	n.mu.Unlock()

	resp, err := n.transport.SendAppendEntries(peer, req)
	if err != nil {
		log.Printf("Node %s: Error sending AppendEntries to %s: %v", n.id, peer, err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > term {
		n.currentTerm = resp.Term
		n.state = Follower
		n.votedFor = ""
		if n.heartbeatTicker != nil {
			n.heartbeatTicker.Stop()
		}
		return
	}

	if resp.Success {
		if len(entries) > 0 {
			n.nextIndex[peer] = entries[len(entries)-1].Index + 1
			n.matchIndex[peer] = entries[len(entries)-1].Index
		}
		n.updateCommitIndex()
	} else {
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

func (n *Node) updateCommitIndex() {
	for i := n.commitIndex + 1; i <= uint64(len(n.log)); i++ {
		if n.log[i-1].Term != n.currentTerm {
			continue
		}

		matches := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= i {
				matches++
			}
		}

		if matches > (len(n.peers)+1)/2 {
			n.commitIndex = i
		}
	}

	n.applyCommitted()
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		n.storage.Set(entry.Key, entry.Value)
		log.Printf("Node %s: Applied entry %d: %s = %s", n.id, entry.Index, entry.Key, entry.Value)
	}

	// Notify any waiters whose index has been committed
	n.notifyCommitWaiters()
}

func (n *Node) notifyCommitWaiters() {
	remaining := make([]commitWaiter, 0, len(n.commitWaiters))
	for _, w := range n.commitWaiters {
		if w.index <= n.commitIndex {
			close(w.ch)
		} else {
			remaining = append(remaining, w)
		}
	}
	n.commitWaiters = remaining
}

func (n *Node) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	n.leaderID = req.LeaderId
	n.resetElectionTimer()

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(n.log)) {
			return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false}
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			n.log = n.log[:req.PrevLogIndex-1]
			return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false}
		}
	}

	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if idx <= uint64(len(n.log)) {
			if n.log[idx-1].Term != entry.Term {
				n.log = n.log[:idx-1]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := uint64(len(n.log))
		if req.LeaderCommit < lastNewIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		n.applyCommitted()
	}

	return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: true}
}

func (n *Node) HandleRequestVote(req *pb.VoteRequest) *pb.VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &pb.VoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	if n.votedFor == "" || n.votedFor == req.CandidateId {
		var lastLogIndex, lastLogTerm uint64
		if len(n.log) > 0 {
			lastEntry := n.log[len(n.log)-1]
			lastLogIndex = lastEntry.Index
			lastLogTerm = lastEntry.Term
		}

		if req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			n.votedFor = req.CandidateId
			n.resetElectionTimer()
			return &pb.VoteResponse{Term: n.currentTerm, VoteGranted: true}
		}
	}

	return &pb.VoteResponse{Term: n.currentTerm, VoteGranted: false}
}

// Get retrieves a value from the local storage.
func (n *Node) Get(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.storage.Get(key)
}

// Set proposes a new key-value entry to the Raft log.
// Only the leader can accept writes. Returns an error if this node is not the leader.
func (n *Node) Set(key, value string) error {
	n.mu.Lock()

	if n.state != Leader {
		leaderID := n.leaderID
		n.mu.Unlock()
		if leaderID == "" {
			return fmt.Errorf("no leader elected yet")
		}
		return fmt.Errorf("not the leader, leader is %s", leaderID)
	}

	// Append entry to local log
	newIndex := uint64(len(n.log)) + 1
	entry := &pb.LogEntry{
		Term:  n.currentTerm,
		Index: newIndex,
		Key:   key,
		Value: value,
	}
	n.log = append(n.log, entry)

	// Create a waiter for this entry to be committed
	waitCh := make(chan struct{})
	n.commitWaiters = append(n.commitWaiters, commitWaiter{
		index: newIndex,
		ch:    waitCh,
	})

	// Immediately send append entries to all peers
	n.sendAppendEntriesToAll()

	// For single-node cluster, update commit index immediately
	if len(n.peers) == 0 {
		n.commitIndex = newIndex
		n.applyCommitted()
	}

	n.mu.Unlock()

	// Wait for commit or timeout
	select {
	case <-waitCh:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for entry to be committed")
	}
}

// GetState returns the current state of the node.
func (n *Node) GetState() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// GetLeaderID returns the ID of the current known leader.
func (n *Node) GetLeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}
