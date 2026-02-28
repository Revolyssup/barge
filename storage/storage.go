// Package storage provides the persistence layer for the Raft node.
// The interface allows swapping in-memory storage for disk-backed implementations.
package storage

import (
	"fmt"
	"sync"
)

// Storage defines the key-value store interface used by the state machine.
type Storage interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Delete(key string) error
}

// LogStorage persists Raft log entries independent of the application state machine.
type LogStorage interface {
	AppendLog(entry LogEntry) error
	GetLog(index uint64) (LogEntry, error)
	LastIndex() uint64
	LastTerm() uint64
	Entries(from, to uint64) ([]LogEntry, error)
	TruncateSuffix(from uint64) error
}

// LogEntry is a single command in the Raft log.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// -------------------------------------------------------------------------
// InMemoryStorage – simple thread-safe key/value store.
// -------------------------------------------------------------------------

type InMemoryStorage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{data: make(map[string][]byte)}
}

func (s *InMemoryStorage) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found", key)
	}
	// Return a copy so callers can't mutate internal state.
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (s *InMemoryStorage) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	s.data[key] = cp
	return nil
}

func (s *InMemoryStorage) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

// -------------------------------------------------------------------------
// InMemoryLogStorage – stores Raft log entries in a slice.
// -------------------------------------------------------------------------

type InMemoryLogStorage struct {
	mu      sync.RWMutex
	entries []LogEntry // index 0 is a sentinel (index=0, term=0)
}

func NewInMemoryLogStorage() *InMemoryLogStorage {
	return &InMemoryLogStorage{
		// Sentinel at position 0 so real entries start at index 1.
		entries: []LogEntry{{Index: 0, Term: 0}},
	}
}

func (l *InMemoryLogStorage) AppendLog(entry LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
	return nil
}

func (l *InMemoryLogStorage) GetLog(index uint64) (LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= uint64(len(l.entries)) {
		return LogEntry{}, fmt.Errorf("log index %d out of range", index)
	}
	return l.entries[index], nil
}

func (l *InMemoryLogStorage) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint64(len(l.entries) - 1)
}

func (l *InMemoryLogStorage) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *InMemoryLogStorage) Entries(from, to uint64) ([]LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	n := uint64(len(l.entries))
	if from > n || to > n {
		return nil, fmt.Errorf("range [%d,%d) out of bounds (len=%d)", from, to, n)
	}
	result := make([]LogEntry, to-from)
	copy(result, l.entries[from:to])
	return result, nil
}

func (l *InMemoryLogStorage) TruncateSuffix(from uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if from < uint64(len(l.entries)) {
		l.entries = l.entries[:from]
	}
	return nil
}
