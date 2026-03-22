package router

import (
	"hash/fnv"

	"github.com/nitinankad/rocketdb/internal/metadata"
)

type Router struct {
	meta *metadata.Service
}

type Route struct {
	ShardID int    `json:"shard_id"`
	Leader  string `json:"leader"`
}

func New(meta *metadata.Service) *Router {
	return &Router{meta: meta}
}

func (r *Router) RouteByPartitionKey(partitionKey string) Route {
	shardCount := len(r.meta.Shards)
	if shardCount == 0 {
		return Route{}
	}

	shardID := int(hashKey(partitionKey) % uint32(shardCount))
	shard := r.meta.Shards[shardID]

	return Route{
		ShardID: shard.ID,
		Leader:  shard.Leader,
	}
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
