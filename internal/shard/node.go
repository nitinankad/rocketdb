package shard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/replication"
	"github.com/nitinankad/rocketdb/internal/storage"
)

type Node struct {
	id          string
	store       storage.Engine
	replication replication.Manager
	meta        *metadata.Service
}

func NewNode(id string, store storage.Engine, repl replication.Manager, meta *metadata.Service) *Node {
	return &Node{
		id:          id,
		store:       store,
		replication: repl,
		meta:        meta,
	}
}

func (n *Node) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", n.handleHealth)
	mux.HandleFunc("/metrics", n.handleMetrics)
	mux.HandleFunc("/v1/kv", n.handleKV)
	mux.HandleFunc("/v1/scan", n.handleScan)
	mux.HandleFunc("/v1/admin/topology", n.handleTopology)
}

type kvRequest struct {
	Table   string         `json:"table"`
	Key     string         `json:"key"`
	Value   string         `json:"value,omitempty"`
	Item    map[string]any `json:"item,omitempty"`
	ShardID int            `json:"shard_id"`
}

type scanRequest struct {
	Table   string `json:"table"`
	ShardID int    `json:"shard_id"`
	Cursor  string `json:"cursor,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type scanRow struct {
	Key  string         `json:"key"`
	Item map[string]any `json:"item"`
}

type topologyRequest struct {
	Shards []metadata.Shard `json:"shards"`
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

	val, found, err := n.store.Get(r.Context(), table, key)
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
	resp := map[string]any{
		"table": table,
		"key":   key,
		"item":  item,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (n *Node) handlePutKV(w http.ResponseWriter, r *http.Request) {
	req, err := decodeBody(r.Context(), r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if !n.meta.IsLeader(n.id, req.ShardID) {
		writeError(w, http.StatusConflict, "writes must go to leader for this shard")
		return
	}

	item := req.Item
	if item == nil {
		item = map[string]any{"value": req.Value}
	}
	payload, err := json.Marshal(item)
	if err != nil {
		writeError(w, http.StatusBadRequest, "item must be valid json object")
		return
	}

	if err := n.store.Put(r.Context(), req.Table, req.Key, payload); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := n.replication.Replicate(r.Context(), req.ShardID, req.Table, req.Key, payload); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (n *Node) handleDeleteKV(w http.ResponseWriter, r *http.Request) {
	req, err := decodeBody(r.Context(), r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if !n.meta.IsLeader(n.id, req.ShardID) {
		writeError(w, http.StatusConflict, "deletes must go to leader for this shard")
		return
	}

	if err := n.store.Delete(r.Context(), req.Table, req.Key); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := n.replication.Replicate(r.Context(), req.ShardID, req.Table, req.Key, nil); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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
		entry := scanRow{
			Key:  row.Key,
			Item: item,
		}
		out = append(out, entry)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"rows":        out,
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

func decodeBody(ctx context.Context, r *http.Request) (kvRequest, error) {
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

func validateRequest(req kvRequest) error {
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

func decodeStoredItem(raw []byte) (map[string]any, error) {
	var item map[string]any
	if err := json.Unmarshal(raw, &item); err != nil || item == nil {
		return nil, fmt.Errorf("stored record is not a valid json object")
	}
	return item, nil
}
