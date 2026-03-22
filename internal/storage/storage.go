package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type Engine interface {
	Put(ctx context.Context, table, key string, value []byte) error
	Get(ctx context.Context, table, key string) ([]byte, bool, error)
	Delete(ctx context.Context, table, key string) error
	Scan(ctx context.Context, table, cursor string, limit int) ([]Row, string, error)
}

type Row struct {
	Key   string
	Value []byte
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

func (s *InMemory) Scan(_ context.Context, table, cursor string, limit int) ([]Row, string, error) {
	if limit <= 0 {
		return []Row{}, "", nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	tableData, ok := s.tables[table]
	if !ok || len(tableData) == 0 {
		return []Row{}, "", nil
	}

	keys := make([]string, 0, len(tableData))
	for key := range tableData {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	start := 0
	if cursor != "" {
		idx := sort.SearchStrings(keys, cursor)
		for idx < len(keys) && keys[idx] <= cursor {
			idx++
		}
		start = idx
	}
	if start >= len(keys) {
		return []Row{}, "", nil
	}

	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}

	rows := make([]Row, 0, end-start)
	for _, key := range keys[start:end] {
		val := tableData[key]
		copied := make([]byte, len(val))
		copy(copied, val)
		rows = append(rows, Row{
			Key:   key,
			Value: copied,
		})
	}

	nextCursor := ""
	if end < len(keys) {
		nextCursor = keys[end-1]
	}

	return rows, nextCursor, nil
}
