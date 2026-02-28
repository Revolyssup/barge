package storage

import (
	"fmt"
	"sync"
)

type Storage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewStorage() *Storage {
	return &Storage{
		data: make(map[string][]byte),
	}
}

func (s *Storage) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return val, nil
}

func (s *Storage) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
	return nil
}

func (s *Storage) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}
