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
}

type kvRequest struct {
	Table   string `json:"table"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
	ShardID int    `json:"shard_id"`
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

	writeJSON(w, http.StatusOK, map[string]string{
		"table": table,
		"key":   key,
		"value": string(val),
	})
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

	if err := n.store.Put(r.Context(), req.Table, req.Key, []byte(req.Value)); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := n.replication.Replicate(r.Context(), req.ShardID, req.Table, req.Key, []byte(req.Value)); err != nil {
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
