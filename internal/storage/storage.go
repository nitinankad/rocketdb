package storage

import (
	"context"
	"fmt"
	"sync"
)

type Engine interface {
	Put(ctx context.Context, table, key string, value []byte) error
	Get(ctx context.Context, table, key string) ([]byte, bool, error)
	Delete(ctx context.Context, table, key string) error
}

type InMemory struct {
	mu     sync.RWMutex
	tables map[string]map[string][]byte
}

func NewInMemory() *InMemory {
	return &InMemory{
		tables: make(map[string]map[string][]byte),
	}
}

func (s *InMemory) Put(_ context.Context, table, key string, value []byte) error {
	if table == "" || key == "" {
		return fmt.Errorf("table and key are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tables[table]; !ok {
		s.tables[table] = make(map[string][]byte)
	}
	copied := make([]byte, len(value))
	copy(copied, value)
	s.tables[table][key] = copied

	return nil
}

func (s *InMemory) Get(_ context.Context, table, key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tableData, ok := s.tables[table]
	if !ok {
		return nil, false, nil
	}

	val, ok := tableData[key]
	if !ok {
		return nil, false, nil
	}

	copied := make([]byte, len(val))
	copy(copied, val)
	return copied, true, nil
}

func (s *InMemory) Delete(_ context.Context, table, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tableData, ok := s.tables[table]; ok {
		delete(tableData, key)
	}
	return nil
}
