package consensus

import (
	pb "barge/proto"
	"fmt"
	"sync"
)

// MockTransport implements transport.Transport for testing
type MockTransport struct {
	mu       sync.Mutex
	nodes    map[string]*Node
	blocked  map[string]bool
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		nodes:   make(map[string]*Node),
		blocked: make(map[string]bool),
	}
}

func (t *MockTransport) RegisterNode(id string, node *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = node
}

func (t *MockTransport) BlockNode(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked[id] = true
}

func (t *MockTransport) UnblockNode(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.blocked, id)
}

func (t *MockTransport) SendAppendEntries(target string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	t.mu.Lock()
	node, ok := t.nodes[target]
	blocked := t.blocked[target]
	t.mu.Unlock()

	if blocked || !ok {
		return nil, fmt.Errorf("node %s is unreachable", target)
	}

	return node.HandleAppendEntries(req)
}

func (t *MockTransport) SendRequestVote(target string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	t.mu.Lock()
	node, ok := t.nodes[target]
	blocked := t.blocked[target]
	t.mu.Unlock()

	if blocked || !ok {
		return nil, fmt.Errorf("node %s is unreachable", target)
	}

	return node.HandleRequestVote(req)
}

func (t *MockTransport) Get(target string, req *pb.ClientGetRequest) (*pb.ClientGetResponse, error) {
	t.mu.Lock()
	node, ok := t.nodes[target]
	blocked := t.blocked[target]
	t.mu.Unlock()

	if blocked || !ok {
		return nil, fmt.Errorf("node %s is unreachable", target)
	}

	return node.HandleClientGet(req)
}

func (t *MockTransport) Set(target string, req *pb.ClientSetRequest) (*pb.ClientSetResponse, error) {
	t.mu.Lock()
	node, ok := t.nodes[target]
	blocked := t.blocked[target]
	t.mu.Unlock()

	if blocked || !ok {
		return nil, fmt.Errorf("node %s is unreachable", target)
	}

	return node.HandleClientSet(req)
}
