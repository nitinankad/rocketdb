# Toy Distributed SQL-ish Database Plan (Go)

Goal: build a toy database that accepts Postgres-like syntax but uses DynamoDB-like internals:

- hash-based sharding/partitioning
- horizontal scalability
- asynchronous replication
- eventual consistency by default

## 1. Product Definition (Week 0)

Define a strict v1 contract so implementation stays small.

- Query language: a limited Postgres-like subset
  - `CREATE TABLE`
  - `INSERT`
  - `SELECT ... WHERE pk = ?`
  - `UPDATE ... WHERE pk = ?`
  - `DELETE ... WHERE pk = ?`
- Primary model: key-based access first, limited secondary-index support later
- Consistency:
  - writes acknowledged by leader shard
  - replicas updated asynchronously
  - reads can be `strong` (leader) or `eventual` (replica)

## 2. Core Architecture (Week 1)

Build these core modules in Go:

1. SQL Frontend
   - lexer/parser for the SQL subset (or use a parser library and enforce a subset)
   - AST and statement validator
2. Planner
   - converts AST into execution plans (`GetByPK`, `PutByPK`, `ScanRange`, etc.)
3. Partition Router
   - computes partition from partition key (`hash(key) % N`)
   - routes to shard leaders/replicas
4. Storage Engine API
   - shard-local interface (`Put`, `Get`, `Delete`, `QueryByPK`)
5. Shard Node
   - local storage + WAL + replication sender/receiver
6. Metadata Service
   - table schema, partition key, shard map, replica topology
7. Replication Manager
   - async log shipping / stream replication from leader to followers
8. Query Gateway
   - entrypoint clients talk to (HTTP, TCP, or simple wire protocol)

## 3. Data and Consistency Model

Use DynamoDB-like distributed behavior with SQL syntax on top.

- Partitioning:
  - every table defines a partition key
  - requests are routed by hash of partition key
- Replication:
  - each partition has 1 leader + N followers
  - leader appends to WAL, followers apply logs asynchronously
- Read modes:
  - `CONSISTENCY STRONG` -> leader read
  - `CONSISTENCY EVENTUAL` -> follower read allowed
- Conflict/versioning:
  - per-row version (`version` or `updated_at`) for last-write-wins in v1
- Failure behavior:
  - if follower lag is high, gateway can force leader reads

## 4. Build Phases

### Phase A: Skeleton + Local Cluster

- Go module and package layout
- run 3+ node processes locally (single machine)
- metadata bootstrap and shard map config
- health and metrics endpoints

### Phase B: SQL Subset Frontend

- implement parser for limited SQL grammar
- convert statements to AST
- semantic checks (table exists, required PK filter exists)

### Phase C: Single-Partition Execution

- implement local storage interface on one node
- support CRUD by partition key
- add WAL for durability and replay

### Phase D: Sharding and Routing

- implement partition hash and router
- send requests to correct shard leader
- add table metadata: `table`, `columns`, `partition_key`, `replication_factor`

### Phase E: Async Replication + Eventual Reads

- replicate WAL entries leader -> followers
- follower apply loop with ack tracking
- support read consistency modes (strong/eventual)
- expose replication lag metrics

### Phase F: Scaling and Rebalancing (Toy Version)

- support adding a node
- move selected partitions to new node
- update shard map and route traffic safely

## 5. Suggested Go Tech Choices

- Server transport: `chi` (HTTP JSON) for quick iteration
- SQL parsing:
  - option A: simple custom parser for limited grammar
  - option B: existing parser (e.g., pg_query bindings) then restrict supported syntax
- Storage:
  - start with `bbolt` or `badger` per shard
- Serialization: `protobuf` or `msgpack` for replication records
- Logging: `zap` or `zerolog`
- Testing: `testing` + `testify`
- Local orchestration: Docker Compose or simple multi-process launcher

## 6. Testing Strategy

- Unit tests:
  - parser correctness
  - partition routing deterministic behavior
  - WAL encode/decode
- Integration tests:
  - write on leader, read from follower before/after replication
  - node-down scenarios
  - stale-read behavior under eventual consistency
- Property checks (optional):
  - no key maps to two leaders simultaneously

## 7. v1 Milestone

Ship a small cluster that can:

1. accept limited Postgres-like SQL
2. route data by hashed partition key across shards
3. replicate data asynchronously to followers
4. provide selectable strong vs eventual read behavior
5. report replication lag and consistency status

## 8. Post-v1 Enhancements

- secondary indexes and global index routing
- richer SQL support (`ORDER BY`, limited joins)
- quorum writes/reads
- leader election
- automatic rebalancing
