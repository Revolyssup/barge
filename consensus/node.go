package consensus

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/revolyssup/barge/storage"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    uint64
	Index   uint64
	Data    []byte
}

// KVCommand represents a key-value set command that is replicated through Raft.
type KVCommand struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Transport interface {
	SendAppendEntries(target string, req AppendEntriesRequest) (*AppendEntriesResponse, error)
	SendRequestVote(target string, req RequestVoteRequest) (*RequestVoteResponse, error)
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type Node struct {
	mu          sync.Mutex
	id          string
	state       State
	currentTerm uint64
	votedFor    string
	log         []LogEntry
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	peers       []string
	transport   Transport
	storage     *storage.Storage
	stopCh      chan struct{}
}

func NewNode(id string, peers []string, transport Transport, storage *storage.Storage) *Node {
	n := &Node{
		id:        id,
		state:     Follower,
		peers:     peers,
		transport: transport,
		storage:   storage,
		stopCh:    make(chan struct{}),
		log:       make([]LogEntry, 0),
	}
	return n
}

func (n *Node) Start() {
	go n.run()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) run() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
			n.mu.Lock()
			state := n.state
			n.mu.Unlock()

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
}

func (n *Node) runFollower() {
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	select {
	case <-time.After(timeout):
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
	currentTerm := n.currentTerm
	lastLogIndex := uint64(len(n.log))
	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLogTerm = n.log[lastLogIndex-1].Term
	}
	n.mu.Unlock()

	votes := 1
	for _, peer := range n.peers {
		resp, err := n.transport.SendRequestVote(peer, RequestVoteRequest{
			Term:         currentTerm,
			CandidateID:  n.id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		})
		if err != nil {
			continue
		}
		if resp.VoteGranted {
			votes++
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if votes > (len(n.peers)+1)/2 {
		n.state = Leader
		n.nextIndex = make(map[string]uint64)
		n.matchIndex = make(map[string]uint64)
		for _, peer := range n.peers {
			n.nextIndex[peer] = uint64(len(n.log)) + 1
			n.matchIndex[peer] = 0
		}
	}
}

func (n *Node) runLeader() {
	n.mu.Lock()
	for _, peer := range n.peers {
		go n.sendAppendEntries(peer)
	}
	n.mu.Unlock()

	select {
	case <-time.After(50 * time.Millisecond):
	case <-n.stopCh:
		return
	}
}

func (n *Node) sendAppendEntries(peer string) {
	n.mu.Lock()
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}

	var entries []LogEntry
	if nextIdx <= uint64(len(n.log)) {
		entries = n.log[nextIdx-1:]
	}

	req := AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	resp, err := n.transport.SendAppendEntries(peer, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Success {
		n.nextIndex[peer] = nextIdx + uint64(len(entries))
		n.matchIndex[peer] = n.nextIndex[peer] - 1
		n.updateCommitIndex()
	} else {
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

func (n *Node) updateCommitIndex() {
	for i := uint64(len(n.log)); i > n.commitIndex; i-- {
		count := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= i {
				count++
			}
		}
		if count > (len(n.peers)+1)/2 && n.log[i-1].Term == n.currentTerm {
			n.commitIndex = i
			n.applyEntries()
			break
		}
	}
}

func (n *Node) applyEntries() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		var cmd KVCommand
		if err := json.Unmarshal(entry.Data, &cmd); err != nil {
			log.Printf("failed to unmarshal log entry %d: %v", entry.Index, err)
			continue
		}
		n.storage.Set(cmd.Key, []byte(cmd.Value))
		log.Printf("applied entry %d: SET %s = %s", entry.Index, cmd.Key, cmd.Value)
	}
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
	}
	n.state = Follower

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(n.log)) {
			return AppendEntriesResponse{Term: n.currentTerm, Success: false}
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			return AppendEntriesResponse{Term: n.currentTerm, Success: false}
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
		n.commitIndex = req.LeaderCommit
		if uint64(len(n.log)) < n.commitIndex {
			n.commitIndex = uint64(len(n.log))
		}
		n.applyEntries()
	}

	return AppendEntriesResponse{Term: n.currentTerm, Success: true}
}

func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.state = Follower
	}

	if n.votedFor == "" || n.votedFor == req.CandidateID {
		lastLogIndex := uint64(len(n.log))
		var lastLogTerm uint64
		if lastLogIndex > 0 {
			lastLogTerm = n.log[lastLogIndex-1].Term
		}

		if req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			n.votedFor = req.CandidateID
			return RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}
		}
	}

	return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
}

// Get retrieves a value from the node's local storage.
func (n *Node) Get(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	val, err := n.storage.Get(key)
	if err != nil {
		return "", false
	}
	return string(val), true
}

// Propose proposes a key-value set command to the Raft cluster.
// Only the leader can accept proposals. Returns an error if this node is not the leader.
func (n *Node) Propose(key, value string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return fmt.Errorf("not the leader")
	}

	cmd := KVCommand{Key: key, Value: value}
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	entry := LogEntry{
		Term:  n.currentTerm,
		Index: uint64(len(n.log)) + 1,
		Data:  data,
	}
	n.log = append(n.log, entry)

	return nil
}

// IsLeader returns true if the node is currently the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// GetState returns the current state of the node.
func (n *Node) GetState() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}
