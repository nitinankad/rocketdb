package replication

import "context"

type Manager interface {
	Replicate(ctx context.Context, shardID int, table, key string, value []byte) error
}

type NoopManager struct{}

func NewNoopManager() *NoopManager {
	return &NoopManager{}
}

func (m *NoopManager) Replicate(_ context.Context, _ int, _ string, _ string, _ []byte) error {
	return nil
}
