# rocketdb
A toy database that uses PostgreSQL-like syntax goals with DynamoDB-like internals to learn distributed DB fundamentals.

## Current Baseline

Phase-A scaffold is in place:

- `cmd/rocketdb-gateway`: query gateway process
- `cmd/rocketdb-node`: shard node process
- `cmd/rocketdb-local`: single-process local cluster launcher (1 gateway + 3 nodes)
- `cmd/rocketdb-cli`: simple CLI for gateway KV/scan APIs
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
go run ./cmd/rocketdb-cli --help
```

Data persistence:

- local cluster writes shard data under `./data/local/*.json.wal` (append-only WAL)
- single node mode writes under `--data-dir` (default `./data/single`)
- restart the process and previously written keys remain on disk
- if a legacy `*.json` snapshot exists, it is loaded on startup before WAL replay

## Quick API Smoke Test

Write through gateway (no manual node routing needed):

```bash
curl -X PUT localhost:8080/v1/kv \
  -H "content-type: application/json" \
  -d '{"table":"users","key":"user-123","item":{"name":"Ada","age":42,"city":"Austin"}}'
```

Read through gateway:

```bash
curl "localhost:8080/v1/kv?table=users&key=user-123&consistency=strong"
```

Delete through gateway:

```bash
curl -X DELETE localhost:8080/v1/kv \
  -H "content-type: application/json" \
  -d '{"table":"users","key":"user-123"}'
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

## CLI

All commands below go through the gateway (`:8080`) and auto-route to the right shard.

```bash
go run ./cmd/rocketdb-cli put  --table users --key user-123 --item '{"name":"Ada","age":42}'
go run ./cmd/rocketdb-cli get  --table users --key user-123
go run ./cmd/rocketdb-cli del  --table users --key user-123
go run ./cmd/rocketdb-cli scan --table users --limit 10
go run ./cmd/rocketdb-cli sql  "insert into users (key, name, age) values ('u1','Ada',42)"
go run ./cmd/rocketdb-cli sql  "select * from users where key = 'u1'"
```

## Auto Rebalance (Toy)

Gateway now supports topology changes with automatic data migration.

Add a node (gateway updates shard map + migrates existing records):

```bash
curl -X POST localhost:8080/v1/admin/nodes/add \
  -H "content-type: application/json" \
  -d '{"id":"node-4","address":":8084"}'
```

Remove a node (migrates records away first):

```bash
curl -X POST localhost:8080/v1/admin/nodes/remove \
  -H "content-type: application/json" \
  -d '{"id":"node-2"}'
```

Inspect current topology:

```bash
curl localhost:8080/v1/admin/topology
```
