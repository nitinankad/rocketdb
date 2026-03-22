package shard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/replication"
	"github.com/nitinankad/rocketdb/internal/storage"
)

const keySep = "\x1f"

type Node struct {
	id          string
	store       storage.Engine
	replication replication.Manager
	meta        *metadata.Service

	streamMu      sync.RWMutex
	streamCursor  int64
	streamEntries []streamEvent
}

type streamEvent struct {
	Cursor    int64          `json:"cursor"`
	Timestamp int64          `json:"timestamp_unix"`
	Op        string         `json:"op"`
	Table     string         `json:"table"`
	Key       string         `json:"key"`
	SortKey   string         `json:"sort_key,omitempty"`
	Item      map[string]any `json:"item,omitempty"`
}

func NewNode(id string, store storage.Engine, repl replication.Manager, meta *metadata.Service) *Node {
	n := &Node{
		id:          id,
		store:       store,
		replication: repl,
		meta:        meta,
	}
	go n.ttlLoop()
	return n
}

func (n *Node) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", n.handleHealth)
	mux.HandleFunc("/metrics", n.handleMetrics)
	mux.HandleFunc("/v1/kv", n.handleKV)
	mux.HandleFunc("/v1/query", n.handleQuery)
	mux.HandleFunc("/v1/query-gsi", n.handleQueryGSI)
	mux.HandleFunc("/v1/scan", n.handleScan)
	mux.HandleFunc("/v1/streams", n.handleStreams)
	mux.HandleFunc("/v1/admin/topology", n.handleTopology)
	mux.HandleFunc("/v1/admin/table/upsert", n.handleUpsertTable)
}

type writeCondition struct {
	AttributeExists *bool  `json:"attribute_exists,omitempty"`
	ExpectedVersion *int64 `json:"expected_version,omitempty"`
}

type kvRequest struct {
	Table     string          `json:"table"`
	Key       string          `json:"key"`
	SortKey   string          `json:"sort_key,omitempty"`
	Value     string          `json:"value,omitempty"`
	Item      map[string]any  `json:"item,omitempty"`
	Condition *writeCondition `json:"condition,omitempty"`
	ShardID   int             `json:"shard_id"`
}

type queryRequest struct {
	Table     string `json:"table"`
	Key       string `json:"key"`
	SortOp    string `json:"sort_op,omitempty"`
	SortValue string `json:"sort_value,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Cursor    string `json:"cursor,omitempty"`
	ShardID   int    `json:"shard_id"`
}

type gsiQueryRequest struct {
	Table   string `json:"table"`
	Index   string `json:"index,omitempty"`
	PKValue string `json:"pk_value"`
	SKOp    string `json:"sk_op,omitempty"`
	SKValue string `json:"sk_value,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	Cursor  string `json:"cursor,omitempty"`
	ShardID int    `json:"shard_id"`
}

type scanRequest struct {
	Table   string `json:"table"`
	ShardID int    `json:"shard_id"`
	Cursor  string `json:"cursor,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type scanRow struct {
	Key     string         `json:"key"`
	SortKey string         `json:"sort_key,omitempty"`
	Item    map[string]any `json:"item"`
}

type topologyRequest struct {
	Shards []metadata.Shard `json:"shards"`
}

type tableUpsertRequest struct {
	Table metadata.Table `json:"table"`
}

func (n *Node) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"node":   n.id,
	})
}

func (n *Node) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("rocketdb_node_up 1\n"))
}

func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		n.handleGetKV(w, r)
	case http.MethodPut:
		n.handlePutKV(w, r)
	case http.MethodDelete:
		n.handleDeleteKV(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (n *Node) handleGetKV(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	key := r.URL.Query().Get("key")
	sortKey := r.URL.Query().Get("sort_key")

	if table == "" || key == "" {
		writeError(w, http.StatusBadRequest, "table and key are required")
		return
	}

	physical := composePrimaryKey(key, sortKey)
	val, found, err := n.store.Get(r.Context(), table, physical)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "record not found")
		return
	}

	item, err := decodeStoredItem(val)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"table":    table,
		"key":      key,
		"sort_key": sortKey,
		"item":     item,
	})
}

func (n *Node) handlePutKV(w http.ResponseWriter, r *http.Request) {
	req, err := decodeKVBody(r.Context(), r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateWriteRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !n.meta.IsLeader(n.id, req.ShardID) {
		writeError(w, http.StatusConflict, "writes must go to leader for this shard")
		return
	}

	physical := composePrimaryKey(req.Key, req.SortKey)
	currentRaw, found, err := n.store.Get(r.Context(), req.Table, physical)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var currentItem map[string]any
	var currentVersion int64
	if found {
		currentItem, err = decodeStoredItem(currentRaw)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		currentVersion = extractVersion(currentItem)
	}

	if err := checkCondition(req.Condition, found, currentVersion); err != nil {
		writeError(w, http.StatusPreconditionFailed, err.Error())
		return
	}

	item := req.Item
	if item == nil {
		item = map[string]any{"value": req.Value}
	}
	item["version"] = currentVersion + 1

	payload, err := json.Marshal(item)
	if err != nil {
		writeError(w, http.StatusBadRequest, "item must be valid json object")
		return
	}

	if err := n.store.Put(r.Context(), req.Table, physical, payload); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := n.syncGSI(r.Context(), req.Table, physical, currentItem, item); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := n.replication.Replicate(r.Context(), req.ShardID, req.Table, physical, payload); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	n.appendEvent("put", req.Table, req.Key, req.SortKey, item)

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": currentVersion + 1,
	})
}

func (n *Node) handleDeleteKV(w http.ResponseWriter, r *http.Request) {
	req, err := decodeKVBody(r.Context(), r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateWriteRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !n.meta.IsLeader(n.id, req.ShardID) {
		writeError(w, http.StatusConflict, "deletes must go to leader for this shard")
		return
	}

	physical := composePrimaryKey(req.Key, req.SortKey)
	currentRaw, found, err := n.store.Get(r.Context(), req.Table, physical)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var currentItem map[string]any
	if found {
		currentItem, err = decodeStoredItem(currentRaw)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	if err := n.store.Delete(r.Context(), req.Table, physical); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := n.syncGSI(r.Context(), req.Table, physical, currentItem, nil); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := n.replication.Replicate(r.Context(), req.ShardID, req.Table, physical, nil); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	n.appendEvent("delete", req.Table, req.Key, req.SortKey, nil)

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (n *Node) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Table == "" || req.Key == "" {
		writeError(w, http.StatusBadRequest, "table and key are required")
		return
	}
	if req.ShardID < 0 {
		writeError(w, http.StatusBadRequest, "shard_id must be >= 0")
		return
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Limit > 1000 {
		req.Limit = 1000
	}
	if req.SortOp == "" {
		req.SortOp = "any"
	}

	if !n.meta.IsReplica(n.id, req.ShardID) {
		writeError(w, http.StatusConflict, "node does not own this shard")
		return
	}

	rows := make([]scanRow, 0, req.Limit)
	nextCursor := ""
	cursor := req.Cursor

	for len(rows) < req.Limit {
		batch, batchNext, err := n.store.Scan(r.Context(), req.Table, cursor, 500)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(batch) == 0 {
			break
		}

		for _, row := range batch {
			pk, sk := splitPrimaryKey(row.Key)
			if pk != req.Key {
				continue
			}
			if !matchSort(req.SortOp, sk, req.SortValue) {
				continue
			}

			item, err := decodeStoredItem(row.Value)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			rows = append(rows, scanRow{
				Key:     pk,
				SortKey: sk,
				Item:    item,
			})
			nextCursor = row.Key
			if len(rows) == req.Limit {
				break
			}
		}

		if batchNext == "" {
			nextCursor = ""
			break
		}
		cursor = batchNext
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"rows":        rows,
		"next_cursor": nextCursor,
	})
}

func (n *Node) handleQueryGSI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req gsiQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Table == "" || req.PKValue == "" {
		writeError(w, http.StatusBadRequest, "table and pk_value are required")
		return
	}
	if req.ShardID < 0 {
		writeError(w, http.StatusBadRequest, "shard_id must be >= 0")
		return
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Limit > 1000 {
		req.Limit = 1000
	}
	if !n.meta.IsReplica(n.id, req.ShardID) {
		writeError(w, http.StatusConflict, "node does not own this shard")
		return
	}

	tableMeta, err := n.meta.Table(req.Table)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if tableMeta.GSI1Name == "" || tableMeta.GSI1PKAttribute == "" {
		writeError(w, http.StatusBadRequest, "table has no gsi configured")
		return
	}
	if req.Index == "" {
		req.Index = tableMeta.GSI1Name
	}
	if req.Index != tableMeta.GSI1Name {
		writeError(w, http.StatusBadRequest, "unknown index name")
		return
	}
	if req.SKOp == "" {
		req.SKOp = "any"
	}

	indexTable := gsiIndexTable(req.Table, tableMeta.GSI1Name)
	prefix := req.PKValue + keySep
	cursor := req.Cursor
	out := make([]scanRow, 0, req.Limit)
	nextCursor := ""

	for len(out) < req.Limit {
		batch, batchNext, err := n.store.Scan(r.Context(), indexTable, cursor, 500)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(batch) == 0 {
			break
		}

		for _, idxRow := range batch {
			if !strings.HasPrefix(idxRow.Key, prefix) {
				continue
			}
			_, gsiSK, primaryKey := splitGSIIndexKey(idxRow.Key)
			if !matchSort(req.SKOp, gsiSK, req.SKValue) {
				continue
			}
			raw, found, err := n.store.Get(r.Context(), req.Table, primaryKey)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if !found {
				continue
			}
			item, err := decodeStoredItem(raw)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			pk, sk := splitPrimaryKey(primaryKey)
			out = append(out, scanRow{Key: pk, SortKey: sk, Item: item})
			nextCursor = idxRow.Key
			if len(out) == req.Limit {
				break
			}
		}

		if batchNext == "" {
			nextCursor = ""
			break
		}
		cursor = batchNext
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"rows":        out,
		"next_cursor": nextCursor,
	})
}

func (n *Node) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req scanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.Table == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}
	if req.ShardID < 0 {
		writeError(w, http.StatusBadRequest, "shard_id must be >= 0")
		return
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Limit > 1000 {
		req.Limit = 1000
	}

	if !n.meta.IsReplica(n.id, req.ShardID) {
		writeError(w, http.StatusConflict, "node does not own this shard")
		return
	}

	rows, next, err := n.store.Scan(r.Context(), req.Table, req.Cursor, req.Limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := make([]scanRow, 0, len(rows))
	for _, row := range rows {
		item, err := decodeStoredItem(row.Value)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		pk, sk := splitPrimaryKey(row.Key)
		out = append(out, scanRow{
			Key:     pk,
			SortKey: sk,
			Item:    item,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"rows":        out,
		"next_cursor": next,
	})
}

func (n *Node) handleStreams(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	cursor := int64(0)
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		cursor = parsed
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = parsed
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	n.streamMu.RLock()
	defer n.streamMu.RUnlock()

	events := make([]streamEvent, 0, limit)
	next := cursor
	for _, ev := range n.streamEntries {
		if ev.Cursor <= cursor {
			continue
		}
		events = append(events, ev)
		next = ev.Cursor
		if len(events) == limit {
			break
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"events":      events,
		"next_cursor": next,
	})
}

func (n *Node) handleTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req topologyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if len(req.Shards) == 0 {
		writeError(w, http.StatusBadRequest, "shards are required")
		return
	}

	n.meta.ReplaceShards(req.Shards)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (n *Node) handleUpsertTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req tableUpsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if err := n.meta.UpsertTable(req.Table); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (n *Node) ttlLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		_ = n.sweepExpired(context.Background())
	}
}

func (n *Node) sweepExpired(ctx context.Context) error {
	now := time.Now().Unix()
	for _, tableName := range n.meta.TableNames() {
		tableMeta, err := n.meta.Table(tableName)
		if err != nil || tableMeta.TTLAttribute == "" {
			continue
		}

		cursor := ""
		for {
			rows, next, err := n.store.Scan(ctx, tableName, cursor, 1000)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}

			for _, row := range rows {
				item, err := decodeStoredItem(row.Value)
				if err != nil {
					continue
				}
				ttl, ok := extractTTL(item, tableMeta.TTLAttribute)
				if !ok || ttl > now {
					continue
				}

				pk, sk := splitPrimaryKey(row.Key)
				shardID := n.shardForKey(pk)
				if !n.meta.IsLeader(n.id, shardID) {
					continue
				}

				_ = n.store.Delete(ctx, tableName, row.Key)
				_ = n.syncGSI(ctx, tableName, row.Key, item, nil)
				_ = n.replication.Replicate(ctx, shardID, tableName, row.Key, nil)
				n.appendEvent("expire", tableName, pk, sk, nil)
			}

			if next == "" {
				break
			}
			cursor = next
		}
	}
	return nil
}

func (n *Node) syncGSI(ctx context.Context, table, primaryKey string, oldItem, newItem map[string]any) error {
	tableMeta, err := n.meta.Table(table)
	if err != nil || tableMeta.GSI1Name == "" || tableMeta.GSI1PKAttribute == "" {
		return nil
	}

	oldPK, _ := stringAttr(oldItem, tableMeta.GSI1PKAttribute)
	newPK, _ := stringAttr(newItem, tableMeta.GSI1PKAttribute)
	oldSK := ""
	newSK := ""
	if tableMeta.GSI1SKAttribute != "" {
		oldSK, _ = stringAttr(oldItem, tableMeta.GSI1SKAttribute)
		newSK, _ = stringAttr(newItem, tableMeta.GSI1SKAttribute)
	}
	indexTable := gsiIndexTable(table, tableMeta.GSI1Name)

	oldIndexKey := composeGSIIndexKey(oldPK, oldSK, primaryKey)
	newIndexKey := composeGSIIndexKey(newPK, newSK, primaryKey)

	if oldPK != "" && oldIndexKey != newIndexKey {
		if err := n.store.Delete(ctx, indexTable, oldIndexKey); err != nil {
			return err
		}
	}
	if newPK != "" {
		if err := n.store.Put(ctx, indexTable, newIndexKey, []byte(primaryKey)); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) appendEvent(op, table, key, sortKey string, item map[string]any) {
	n.streamMu.Lock()
	defer n.streamMu.Unlock()

	n.streamCursor++
	n.streamEntries = append(n.streamEntries, streamEvent{
		Cursor:    n.streamCursor,
		Timestamp: time.Now().Unix(),
		Op:        op,
		Table:     table,
		Key:       key,
		SortKey:   sortKey,
		Item:      item,
	})
	if len(n.streamEntries) > 10000 {
		n.streamEntries = append([]streamEvent(nil), n.streamEntries[len(n.streamEntries)-10000:]...)
	}
}

func (n *Node) shardForKey(key string) int {
	shards := n.meta.ShardsSnapshot()
	if len(shards) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(shards)))
}

func decodeKVBody(ctx context.Context, r *http.Request) (kvRequest, error) {
	var req kvRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return kvRequest{}, fmt.Errorf("invalid json body: %w", err)
	}
	select {
	case <-ctx.Done():
		return kvRequest{}, ctx.Err()
	default:
	}
	return req, nil
}

func validateWriteRequest(req kvRequest) error {
	if req.Table == "" {
		return errors.New("table is required")
	}
	if req.Key == "" {
		return errors.New("key is required")
	}
	if req.ShardID < 0 {
		return errors.New("shard_id must be >= 0")
	}
	return nil
}

func checkCondition(cond *writeCondition, found bool, currentVersion int64) error {
	if cond == nil {
		return nil
	}
	if cond.AttributeExists != nil && *cond.AttributeExists != found {
		return errors.New("condition failed: attribute_exists")
	}
	if cond.ExpectedVersion != nil && *cond.ExpectedVersion != currentVersion {
		return errors.New("condition failed: expected_version")
	}
	return nil
}

func extractVersion(item map[string]any) int64 {
	if item == nil {
		return 0
	}
	v, ok := item["version"]
	if !ok {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	default:
		return 0
	}
}

func extractTTL(item map[string]any, attr string) (int64, bool) {
	v, ok := item[attr]
	if !ok {
		return 0, false
	}
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	default:
		return 0, false
	}
}

func stringAttr(item map[string]any, key string) (string, bool) {
	if item == nil {
		return "", false
	}
	v, ok := item[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func matchSort(op, actual, expected string) bool {
	switch strings.ToLower(op) {
	case "", "any":
		return true
	case "eq":
		return actual == expected
	case "begins_with":
		return strings.HasPrefix(actual, expected)
	case "gt":
		return actual > expected
	case "gte":
		return actual >= expected
	case "lt":
		return actual < expected
	case "lte":
		return actual <= expected
	default:
		return false
	}
}

func decodeStoredItem(raw []byte) (map[string]any, error) {
	var item map[string]any
	if err := json.Unmarshal(raw, &item); err != nil || item == nil {
		return nil, fmt.Errorf("stored record is not a valid json object")
	}
	return item, nil
}

func composePrimaryKey(key, sortKey string) string {
	if sortKey == "" {
		return key
	}
	return key + keySep + sortKey
}

func splitPrimaryKey(composite string) (string, string) {
	parts := strings.SplitN(composite, keySep, 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func gsiIndexTable(table, index string) string {
	return "__" + index + "_" + table
}

func composeGSIIndexKey(pk, sk, primaryKey string) string {
	return pk + keySep + sk + keySep + primaryKey
}

func splitGSIIndexKey(composite string) (string, string, string) {
	parts := strings.SplitN(composite, keySep, 3)
	if len(parts) < 3 {
		return "", "", ""
	}
	return parts[0], parts[1], parts[2]
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{
		"error": msg,
		"code":  strconv.Itoa(status),
	})
}
