# rocketdb
A toy database that uses PostgreSQL-like syntax goals with DynamoDB-like internals to learn distributed DB fundamentals.

## Current Baseline

Phase-A scaffold is in place:

- `cmd/rocketdb-gateway`: query gateway process
- `cmd/rocketdb-node`: shard node process
- `cmd/rocketdb-local`: single-process local cluster launcher (1 gateway + 3 nodes)
- `internal/metadata`: table and shard topology bootstrap
- `internal/router`: hash-based partition routing (`hash(key) % N`)
- `internal/storage`: shard-local storage interface + in-memory engine
- `internal/shard`: node HTTP handlers and leader-only write checks
- `internal/replication`: async replication interface (noop manager for now)

## Run

Run local cluster:

```bash
go run ./cmd/rocketdb-local
```

Run components separately:

```bash
go run ./cmd/rocketdb-gateway --addr :8080
go run ./cmd/rocketdb-node --node-id node-1 --addr :8081
```

## Quick API Smoke Test

Route a partition key:

```bash
curl -X POST localhost:8080/v1/route \
  -H "content-type: application/json" \
  -d '{"table":"users","partition_key":"user-123"}'
```

Write directly to a shard leader:

```bash
curl -X PUT localhost:8081/v1/kv \
  -H "content-type: application/json" \
  -d '{"table":"users","key":"user-123","value":"{\"name\":\"Ada\"}","shard_id":0}'
```
