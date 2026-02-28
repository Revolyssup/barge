package consensus

import (
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

type Node struct {
	mu sync.RWMutex

	// Node identity
	id    string
	addr  string
	peers map[string]string // id -> addr

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         []*pb.LogEntry

	// Volatile state
	state       State
	commitIndex uint64
	lastApplied uint64

	// Leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Components
	transport transport.Transport
	storage   *storage.Storage

	// Channels and timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	stopCh         chan struct{}

	// Commit notification
	commitCh chan struct{}
}

func NewNode(id, addr string, peers map[string]string, t transport.Transport, s *storage.Storage) *Node {
	n := &Node{
		id:          id,
		addr:        addr,
		peers:       peers,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*pb.LogEntry, 0),
		state:       Follower,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		transport:   t,
		storage:     s,
		stopCh:      make(chan struct{}),
		commitCh:    make(chan struct{}, 100),
	}

	// Register this node as the handler for incoming RPCs
	t.SetHandler(n)

	// Register this node as the client operator if the transport supports it
	if gt, ok := t.(*transport.GRPCTransport); ok {
		gt.SetClientOperator(n)
	}

	return n
}

func (n *Node) Start() error {
	if err := n.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	n.resetElectionTimer()
	go n.run()
	go n.applyCommitted()

	log.Printf("Node %s started at %s", n.id, n.addr)
	return nil
}

func (n *Node) Stop() {
	close(n.stopCh)
	n.transport.Stop()
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
}

// Get retrieves a value from the local storage.
func (n *Node) Get(key string) (string, bool) {
	return n.storage.Get(key)
}

// ProposeSet proposes a set command through Raft consensus.
// Only the leader can accept proposals; followers return an error.
func (n *Node) ProposeSet(key, value string) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return fmt.Errorf("not the leader")
	}

	entry := &pb.LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log) + 1),
		Command: fmt.Sprintf("SET %s %s", key, value),
	}
	n.log = append(n.log, entry)
	log.Printf("Leader %s: appended entry %d: %s", n.id, entry.Index, entry.Command)
	n.mu.Unlock()

	// Trigger immediate replication
	n.replicateToAll()

	// Wait for commit with timeout
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-n.commitCh:
			n.mu.RLock()
			committed := n.commitIndex >= entry.Index
			n.mu.RUnlock()
			if committed {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout waiting for commit")
		case <-n.stopCh:
			return fmt.Errorf("node stopped")
		}
	}
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
	select {
	case <-n.electionTimer.C:
		log.Printf("Node %s: election timeout, becoming candidate", n.id)
		n.mu.Lock()
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
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	n.mu.Unlock()

	n.resetElectionTimer()

	votes := 1 // Vote for self
	var votesMu sync.Mutex

	for peerID, peerAddr := range n.peers {
		go func(id, addr string) {
			resp, err := n.transport.SendRequestVote(addr, &pb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				log.Printf("Node %s: failed to send RequestVote to %s: %v", n.id, id, err)
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

			if resp.VoteGranted {
				votesMu.Lock()
				votes++
				if votes > (len(n.peers)+1)/2 && n.state == Candidate {
					log.Printf("Node %s: won election for term %d", n.id, n.currentTerm)
					n.state = Leader
					n.initLeaderState()
				}
				votesMu.Unlock()
			}
		}(peerID, peerAddr)
	}

	select {
	case <-n.electionTimer.C:
		log.Printf("Node %s: election timeout during candidacy", n.id)
	case <-n.stopCh:
		return
	}
}

func (n *Node) runLeader() {
	n.heartbeatTimer = time.NewTimer(50 * time.Millisecond)

	select {
	case <-n.heartbeatTimer.C:
		n.replicateToAll()
	case <-n.stopCh:
		return
	}
}

func (n *Node) replicateToAll() {
	for peerID, peerAddr := range n.peers {
		go n.replicateTo(peerID, peerAddr)
	}
}

func (n *Node) replicateTo(peerID, peerAddr string) {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}

	nextIdx := n.nextIndex[peerID]
	prevLogIndex := uint64(0)
	prevLogTerm := uint64(0)

	if nextIdx > 1 && int(nextIdx-1) <= len(n.log) {
		prevLogIndex = nextIdx - 1
		prevLogTerm = n.log[prevLogIndex-1].Term
	}

	var entries []*pb.LogEntry
	if int(nextIdx) <= len(n.log) {
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
	n.mu.RUnlock()

	resp, err := n.transport.SendAppendEntries(peerAddr, req)
	if err != nil {
		log.Printf("Node %s: failed to send AppendEntries to %s: %v", n.id, peerID, err)
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
			n.nextIndex[peerID] = entries[len(entries)-1].Index + 1
			n.matchIndex[peerID] = entries[len(entries)-1].Index
		}
		n.updateCommitIndex()
	} else {
		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
	}
}

func (n *Node) updateCommitIndex() {
	for i := uint64(len(n.log)); i > n.commitIndex; i-- {
		if n.log[i-1].Term != n.currentTerm {
			continue
		}

		matches := 1 // Leader has it
		for peerID := range n.peers {
			if n.matchIndex[peerID] >= i {
				matches++
			}
		}

		if matches > (len(n.peers)+1)/2 {
			n.commitIndex = i
			// Notify commit channel
			select {
			case n.commitCh <- struct{}{}:
			default:
			}
			break
		}
	}
}

func (n *Node) applyCommitted() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.commitCh:
			n.mu.Lock()
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				entry := n.log[n.lastApplied-1]
				n.applyEntry(entry)
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) applyEntry(entry *pb.LogEntry) {
	// Parse command: "SET key value"
	var cmd, key, value string
	fmt.Sscanf(entry.Command, "%s %s %s", &cmd, &key, &value)
	if cmd == "SET" {
		n.storage.Set(key, value)
		log.Printf("Node %s: applied entry %d: SET %s=%s", n.id, entry.Index, key, value)
	}
}

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
	n.resetElectionTimer()

	// Log consistency check
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

	if req.LeaderCommit > n.commitIndex {
		oldCommit := n.commitIndex
		if req.LeaderCommit < uint64(len(n.log)) {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = uint64(len(n.log))
		}
		if n.commitIndex > oldCommit {
			select {
			case n.commitCh <- struct{}{}:
			default:
			}
		}
	}

	return &pb.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: true,
	}, nil
}

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
		n.votedFor = ""
		n.state = Follower
	}

	lastLogIndex, lastLogTerm := n.lastLogInfo()
	logUpToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if (n.votedFor == "" || n.votedFor == req.CandidateId) && logUpToDate {
		n.votedFor = req.CandidateId
		n.resetElectionTimer()
		return &pb.RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: true,
		}, nil
	}

	return &pb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}, nil
}

func (n *Node) initLeaderState() {
	for peerID := range n.peers {
		n.nextIndex[peerID] = uint64(len(n.log) + 1)
		n.matchIndex[peerID] = 0
	}
}

func (n *Node) lastLogInfo() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) resetElectionTimer() {
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
	} else {
		n.electionTimer.Reset(timeout)
	}
}
