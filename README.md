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
go run ./cmd/rocketdb-node --node-id node-1 --addr :8081 --data-dir ./data/single
```

Data persistence:

- local cluster writes shard data under `./data/local/*.json.wal` (append-only WAL)
- single node mode writes under `--data-dir` (default `./data/single`)
- restart the process and previously written keys remain on disk
- if a legacy `*.json` snapshot exists, it is loaded on startup before WAL replay

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

Full table scan via gateway (paginated):

```bash
curl -X POST localhost:8080/v1/scan \
  -H "content-type: application/json" \
  -d '{"table":"users","limit":50,"consistency":"strong"}'
```

Use `next_cursor` from the response to fetch the next page:

```bash
curl -X POST localhost:8080/v1/scan \
  -H "content-type: application/json" \
  -d '{"table":"users","limit":50,"cursor":"<next_cursor>","consistency":"strong"}'
```
