package consensus

import (
	"context"
	"sync"

	"github.com/example/raft/transport"
)

// mockTransport lets tests wire nodes together in-process without any network.
// Each node registers itself; messages are delivered synchronously.
type mockTransport struct {
	mu       sync.RWMutex
	handlers map[string]transport.Handler
}

func newMockTransport() *mockTransport {
	return &mockTransport{handlers: make(map[string]transport.Handler)}
}

func (m *mockTransport) register(addr string, h transport.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[addr] = h
}

func (m *mockTransport) SendRequestVote(_ context.Context, addr string, args transport.RequestVoteArgs) (transport.RequestVoteReply, error) {
	m.mu.RLock()
	h, ok := m.handlers[addr]
	m.mu.RUnlock()
	if !ok {
		return transport.RequestVoteReply{}, nil
	}
	return h.HandleRequestVote(args), nil
}

func (m *mockTransport) SendAppendEntries(_ context.Context, addr string, args transport.AppendEntriesArgs) (transport.AppendEntriesReply, error) {
	m.mu.RLock()
	h, ok := m.handlers[addr]
	m.mu.RUnlock()
	if !ok {
		return transport.AppendEntriesReply{}, nil
	}
	return h.HandleAppendEntries(args), nil
}
