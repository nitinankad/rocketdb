package metadata

import (
	"fmt"
	"slices"
	"sync"
)

type Table struct {
	Name              string
	PartitionKey      string
	ReplicationFactor int
}

type Shard struct {
	ID        int
	Leader    string
	Followers []string
}

type Service struct {
	mu     sync.RWMutex
	Tables map[string]Table
	Shards []Shard
}

func DefaultBootstrap() *Service {
	return &Service{
		Tables: map[string]Table{
			"users": {
				Name:              "users",
				PartitionKey:      "id",
				ReplicationFactor: 2,
			},
		},
		Shards: []Shard{
			{ID: 0, Leader: "node-1", Followers: []string{"node-2"}},
			{ID: 1, Leader: "node-2", Followers: []string{"node-3"}},
			{ID: 2, Leader: "node-3", Followers: []string{"node-1"}},
		},
	}
}

func (s *Service) Table(name string) (Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.Tables[name]
	if !ok {
		return Table{}, fmt.Errorf("table not found: %s", name)
	}
	return t, nil
}

func (s *Service) ShardByID(id int) (Shard, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, shard := range s.Shards {
		if shard.ID == id {
			return shard, nil
		}
	}
	return Shard{}, fmt.Errorf("shard not found: %d", id)
}

func (s *Service) IsLeader(nodeID string, shardID int) bool {
	shard, err := s.ShardByID(shardID)
	if err != nil {
		return false
	}
	return shard.Leader == nodeID
}

func (s *Service) IsReplica(nodeID string, shardID int) bool {
	shard, err := s.ShardByID(shardID)
	if err != nil {
		return false
	}
	return shard.Leader == nodeID || slices.Contains(shard.Followers, nodeID)
}

func (s *Service) ShardsSnapshot() []Shard {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Shard, len(s.Shards))
	for i, shard := range s.Shards {
		out[i] = Shard{
			ID:        shard.ID,
			Leader:    shard.Leader,
			Followers: append([]string(nil), shard.Followers...),
		}
	}
	return out
}

func (s *Service) ReplaceShards(shards []Shard) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]Shard, len(shards))
	for i, shard := range shards {
		out[i] = Shard{
			ID:        shard.ID,
			Leader:    shard.Leader,
			Followers: append([]string(nil), shard.Followers...),
		}
	}
	s.Shards = out
}

func (s *Service) TableNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.Tables))
	for name := range s.Tables {
		names = append(names, name)
	}
	return names
}
