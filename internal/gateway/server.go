package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nitinankad/rocketdb/internal/config"
	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/router"
)

type Server struct {
	router     *router.Router
	meta       *metadata.Service
	cluster    config.Cluster
	httpClient *http.Client
}

func NewServer(rt *router.Router, meta *metadata.Service, cluster config.Cluster) *Server {
	return &Server{
		router:  rt,
		meta:    meta,
		cluster: cluster,
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (s *Server) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/route", s.handleRoute)
	mux.HandleFunc("/v1/scan", s.handleScan)
}

type routeRequest struct {
	Table        string `json:"table"`
	PartitionKey string `json:"partition_key"`
}

type scanRequest struct {
	Table       string `json:"table"`
	Limit       int    `json:"limit,omitempty"`
	Cursor      string `json:"cursor,omitempty"`
	Consistency string `json:"consistency,omitempty"`
}

type scanRow struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	ShardID int    `json:"shard_id"`
}

type nodeScanRequest struct {
	Table   string `json:"table"`
	ShardID int    `json:"shard_id"`
	Cursor  string `json:"cursor,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type nodeScanResponse struct {
	Rows []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"rows"`
	NextCursor string `json:"next_cursor"`
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("rocketdb_gateway_up 1\n"))
}

func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req routeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Table == "" || req.PartitionKey == "" {
		writeError(w, http.StatusBadRequest, "table and partition_key are required")
		return
	}
	if _, err := s.meta.Table(req.Table); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	route := s.router.RouteByPartitionKey(req.PartitionKey)
	writeJSON(w, http.StatusOK, route)
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req scanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Table == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}
	if _, err := s.meta.Table(req.Table); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Limit > 1000 {
		req.Limit = 1000
	}
	if req.Consistency == "" {
		req.Consistency = "strong"
	}
	if req.Consistency != "strong" && req.Consistency != "eventual" {
		writeError(w, http.StatusBadRequest, "consistency must be strong or eventual")
		return
	}

	startShardIdx, shardCursor, err := parseGatewayCursor(req.Cursor)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if startShardIdx < 0 || startShardIdx >= len(s.meta.Shards) {
		writeError(w, http.StatusBadRequest, "cursor shard index out of range")
		return
	}

	rows := make([]scanRow, 0, req.Limit)
	nextCursor := ""

	for i := startShardIdx; i < len(s.meta.Shards) && len(rows) < req.Limit; i++ {
		shard := s.meta.Shards[i]
		nodeID := shard.Leader
		if req.Consistency == "eventual" && len(shard.Followers) > 0 {
			nodeID = shard.Followers[0]
		}

		addr, err := s.cluster.AddressByNodeID(nodeID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		nodeReq := nodeScanRequest{
			Table:   req.Table,
			ShardID: shard.ID,
			Limit:   req.Limit - len(rows),
		}
		if i == startShardIdx {
			nodeReq.Cursor = shardCursor
		}

		nodeResp, err := s.scanNode(r.Context(), addr, nodeReq)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}

		for _, rr := range nodeResp.Rows {
			rows = append(rows, scanRow{
				Key:     rr.Key,
				Value:   rr.Value,
				ShardID: shard.ID,
			})
		}

		if len(rows) == req.Limit {
			if nodeResp.NextCursor != "" {
				nextCursor = fmt.Sprintf("%d|%s", i, nodeResp.NextCursor)
			} else if len(nodeResp.Rows) > 0 && i < len(s.meta.Shards)-1 {
				// Continue from the key we just returned, then move to next shard.
				lastKey := nodeResp.Rows[len(nodeResp.Rows)-1].Key
				nextCursor = fmt.Sprintf("%d|%s", i, lastKey)
			}
			break
		}

		if nodeResp.NextCursor != "" {
			nextCursor = fmt.Sprintf("%d|%s", i, nodeResp.NextCursor)
			break
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"rows":        rows,
		"next_cursor": nextCursor,
	})
}

func (s *Server) scanNode(ctx context.Context, addr string, req nodeScanRequest) (nodeScanResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nodeScanResponse{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+addr+"/v1/scan", bytes.NewReader(body))
	if err != nil {
		return nodeScanResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nodeScanResponse{}, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nodeScanResponse{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return nodeScanResponse{}, fmt.Errorf("node %s returned status %d: %s", addr, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var out nodeScanResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nodeScanResponse{}, err
	}
	return out, nil
}

func parseGatewayCursor(cursor string) (int, string, error) {
	if cursor == "" {
		return 0, "", nil
	}
	parts := strings.SplitN(cursor, "|", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid cursor format")
	}
	idx, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", fmt.Errorf("invalid cursor shard index")
	}
	return idx, parts[1], nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
