package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nitinankad/rocketdb/internal/config"
	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/router"
	"github.com/nitinankad/rocketdb/internal/transport"
)

type Server struct {
	router *router.Router
	meta   *metadata.Service
	mux    *http.ServeMux

	mu        sync.RWMutex
	nodeAddrs map[string]string
	nodeOrder []string
	clients   map[string]*transport.Client
}

func NewServer(rt *router.Router, meta *metadata.Service, cluster config.Cluster) *Server {
	nodeAddrs := make(map[string]string, len(cluster.Nodes))
	nodeOrder := make([]string, 0, len(cluster.Nodes))
	for _, n := range cluster.Nodes {
		nodeAddrs[n.ID] = n.Address
		nodeOrder = append(nodeOrder, n.ID)
	}

	srv := &Server{
		router: rt,
		meta:   meta,
		mux:    http.NewServeMux(),
		nodeAddrs: nodeAddrs,
		nodeOrder: nodeOrder,
		clients:   make(map[string]*transport.Client, len(cluster.Nodes)),
	}
	srv.RegisterHTTP(srv.mux)
	return srv
}

func (s *Server) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/route", s.handleRoute)
	mux.HandleFunc("/v1/kv", s.handleKV)
	mux.HandleFunc("/v1/query", s.handleQuery)
	mux.HandleFunc("/v1/query-gsi", s.handleQueryGSI)
	mux.HandleFunc("/v1/streams", s.handleStreams)
	mux.HandleFunc("/v1/scan", s.handleScan)
	mux.HandleFunc("/v1/admin/topology", s.handleTopology)
	mux.HandleFunc("/v1/admin/tables/upsert", s.handleUpsertTable)
	mux.HandleFunc("/v1/admin/nodes/add", s.handleAddNode)
	mux.HandleFunc("/v1/admin/nodes/remove", s.handleRemoveNode)
}

func (s *Server) HandleRPC(ctx context.Context, req transport.Request) (transport.Response, error) {
	return transport.DispatchHTTP(ctx, s.mux, req)
}

type routeRequest struct {
	Table        string `json:"table"`
	PartitionKey string `json:"partition_key"`
}

type kvRequest struct {
	Table       string         `json:"table"`
	Key         string         `json:"key"`
	SortKey     string         `json:"sort_key,omitempty"`
	Value       string         `json:"value,omitempty"`
	Item        map[string]any `json:"item,omitempty"`
	Condition   any            `json:"condition,omitempty"`
	Consistency string         `json:"consistency,omitempty"`
}

type scanRequest struct {
	Table       string `json:"table"`
	Limit       int    `json:"limit,omitempty"`
	Cursor      string `json:"cursor,omitempty"`
	Consistency string `json:"consistency,omitempty"`
}

type scanRow struct {
	Key     string         `json:"key"`
	SortKey string         `json:"sort_key,omitempty"`
	Item    map[string]any `json:"item,omitempty"`
	ShardID int            `json:"shard_id"`
}

type nodeScanRequest struct {
	Table   string `json:"table"`
	ShardID int    `json:"shard_id"`
	Cursor  string `json:"cursor,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type nodeScanResponse struct {
	Rows []struct {
		Key     string         `json:"key"`
		SortKey string         `json:"sort_key,omitempty"`
		Item    map[string]any `json:"item"`
	} `json:"rows"`
	NextCursor string `json:"next_cursor"`
}

type nodeStreamsResponse struct {
	Events []map[string]any `json:"events"`
}

type nodeUpsertRequest struct {
	Table     string         `json:"table"`
	Key       string         `json:"key"`
	SortKey   string         `json:"sort_key,omitempty"`
	Item      map[string]any `json:"item,omitempty"`
	Condition any            `json:"condition,omitempty"`
	ShardID   int            `json:"shard_id"`
}

type adminNodeRequest struct {
	ID      string `json:"id"`
	Address string `json:"address,omitempty"`
}

type topologyRequest struct {
	Shards []metadata.Shard `json:"shards"`
}

type queryRequest struct {
	Table       string `json:"table"`
	Key         string `json:"key"`
	SortOp      string `json:"sort_op,omitempty"`
	SortValue   string `json:"sort_value,omitempty"`
	Limit       int    `json:"limit,omitempty"`
	Cursor      string `json:"cursor,omitempty"`
	Consistency string `json:"consistency,omitempty"`
}

type gsiQueryRequest struct {
	Table       string `json:"table"`
	Index       string `json:"index,omitempty"`
	PKValue     string `json:"pk_value"`
	SKOp        string `json:"sk_op,omitempty"`
	SKValue     string `json:"sk_value,omitempty"`
	Limit       int    `json:"limit,omitempty"`
	Cursor      string `json:"cursor,omitempty"`
	Consistency string `json:"consistency,omitempty"`
}

type streamsRequest struct {
	Cursor      int64  `json:"cursor,omitempty"`
	Limit       int    `json:"limit,omitempty"`
	Consistency string `json:"consistency,omitempty"`
}

type tableUpsertRequest struct {
	Name              string `json:"name"`
	PartitionKey      string `json:"partition_key"`
	SortKey           string `json:"sort_key,omitempty"`
	GSI1Name          string `json:"gsi1_name,omitempty"`
	GSI1PKAttribute   string `json:"gsi1_pk_attribute,omitempty"`
	GSI1SKAttribute   string `json:"gsi1_sk_attribute,omitempty"`
	TTLAttribute      string `json:"ttl_attribute,omitempty"`
	ReplicationFactor int    `json:"replication_factor,omitempty"`
}

type nodeTableUpsertRequest struct {
	Table metadata.Table `json:"table"`
}

type rebalanceRecord struct {
	Table   string
	Key     string
	SortKey string
	Item    map[string]any
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

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetKV(w, r)
	case http.MethodPut:
		s.handlePutKV(w, r)
	case http.MethodDelete:
		s.handleDeleteKV(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleGetKV(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	key := r.URL.Query().Get("key")
	sortKey := r.URL.Query().Get("sort_key")
	consistency := r.URL.Query().Get("consistency")
	if consistency == "" {
		consistency = "strong"
	}
	if consistency != "strong" && consistency != "eventual" {
		writeError(w, http.StatusBadRequest, "consistency must be strong or eventual")
		return
	}
	if table == "" || key == "" {
		writeError(w, http.StatusBadRequest, "table and key are required")
		return
	}
	if _, err := s.meta.Table(table); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	addr, _, err := s.resolveNodeForKey(key, consistency)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	status, payload, err := s.forwardNode(r.Context(), addr, transport.Request{
		Method: http.MethodGet,
		Path:   "/v1/kv",
		Query: map[string]string{
			"table":    table,
			"key":      key,
			"sort_key": sortKey,
		},
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSONRaw(w, status, payload)
}

func (s *Server) handlePutKV(w http.ResponseWriter, r *http.Request) {
	var req kvRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Table == "" || req.Key == "" {
		writeError(w, http.StatusBadRequest, "table and key are required")
		return
	}
	if _, err := s.meta.Table(req.Table); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	addr, shardID, err := s.resolveNodeForKey(req.Key, "strong")
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	nodeBody, err := json.Marshal(nodeUpsertRequest{
		Table:     req.Table,
		Key:       req.Key,
		SortKey:   req.SortKey,
		Item:      chooseItem(req),
		Condition: req.Condition,
		ShardID:   shardID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	status, payload, err := s.forwardNode(r.Context(), addr, transport.Request{
		Method: http.MethodPut,
		Path:   "/v1/kv",
		Body:   nodeBody,
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSONRaw(w, status, payload)
}

func (s *Server) handleDeleteKV(w http.ResponseWriter, r *http.Request) {
	var req kvRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Table == "" || req.Key == "" {
		writeError(w, http.StatusBadRequest, "table and key are required")
		return
	}
	if _, err := s.meta.Table(req.Table); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	addr, shardID, err := s.resolveNodeForKey(req.Key, "strong")
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	nodeBody, err := json.Marshal(map[string]any{
		"table":    req.Table,
		"key":      req.Key,
		"sort_key": req.SortKey,
		"shard_id": shardID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	status, payload, err := s.forwardNode(r.Context(), addr, transport.Request{
		Method: http.MethodDelete,
		Path:   "/v1/kv",
		Body:   nodeBody,
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSONRaw(w, status, payload)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Table == "" || req.Key == "" {
		writeError(w, http.StatusBadRequest, "table and key are required")
		return
	}
	if req.Consistency == "" {
		req.Consistency = "strong"
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Limit > 1000 {
		req.Limit = 1000
	}

	addr, shardID, err := s.resolveNodeForKey(req.Key, req.Consistency)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	body, err := json.Marshal(map[string]any{
		"table":      req.Table,
		"key":        req.Key,
		"sort_op":    req.SortOp,
		"sort_value": req.SortValue,
		"limit":      req.Limit,
		"cursor":     req.Cursor,
		"shard_id":   shardID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	status, payload, err := s.forwardNode(r.Context(), addr, transport.Request{
		Method: http.MethodPost,
		Path:   "/v1/query",
		Body:   body,
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSONRaw(w, status, payload)
}

func (s *Server) handleQueryGSI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req gsiQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Table == "" || req.PKValue == "" {
		writeError(w, http.StatusBadRequest, "table and pk_value are required")
		return
	}
	if req.Consistency == "" {
		req.Consistency = "strong"
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.Limit > 1000 {
		req.Limit = 1000
	}

	shards := s.meta.ShardsSnapshot()
	startShardIdx, shardCursor, err := parseGatewayCursor(req.Cursor)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if startShardIdx < 0 || startShardIdx >= len(shards) {
		writeError(w, http.StatusBadRequest, "cursor shard index out of range")
		return
	}

	rows := make([]scanRow, 0, req.Limit)
	nextCursor := ""

	for i := startShardIdx; i < len(shards) && len(rows) < req.Limit; i++ {
		shard := shards[i]
		nodeID := shard.Leader
		if req.Consistency == "eventual" && len(shard.Followers) > 0 {
			nodeID = shard.Followers[0]
		}
		addr, err := s.addressByNodeID(nodeID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		body, err := json.Marshal(map[string]any{
			"table":    req.Table,
			"index":    req.Index,
			"pk_value": req.PKValue,
			"sk_op":    req.SKOp,
			"sk_value": req.SKValue,
			"limit":    req.Limit - len(rows),
			"cursor": func() string {
				if i == startShardIdx {
					return shardCursor
				}
				return ""
			}(),
			"shard_id": shard.ID,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		status, payload, err := s.forwardNode(r.Context(), addr, transport.Request{
			Method: http.MethodPost,
			Path:   "/v1/query-gsi",
			Body:   body,
		})
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		if status != http.StatusOK {
			writeJSONRaw(w, status, payload)
			return
		}

		var nodeResp nodeScanResponse
		if err := json.Unmarshal(payload, &nodeResp); err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		for _, rr := range nodeResp.Rows {
			rows = append(rows, scanRow{Key: rr.Key, SortKey: rr.SortKey, Item: rr.Item, ShardID: shard.ID})
		}
		if len(rows) == req.Limit {
			if nodeResp.NextCursor != "" {
				nextCursor = fmt.Sprintf("%d|%s", i, nodeResp.NextCursor)
			}
			break
		}
		if nodeResp.NextCursor != "" {
			nextCursor = fmt.Sprintf("%d|%s", i, nodeResp.NextCursor)
			break
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"rows": rows, "next_cursor": nextCursor})
}

func (s *Server) handleStreams(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	cursor := r.URL.Query().Get("cursor")
	if cursor == "" {
		cursor = "0"
	}
	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limit = "100"
	}

	out := make([]map[string]any, 0, 256)
	shards := s.meta.ShardsSnapshot()
	seen := map[string]bool{}
	for _, shard := range shards {
		nodeID := shard.Leader
		if seen[nodeID] {
			continue
		}
		seen[nodeID] = true
		addr, err := s.addressByNodeID(nodeID)
		if err != nil {
			continue
		}
		status, payload, err := s.forwardNode(r.Context(), addr, transport.Request{
			Method: http.MethodGet,
			Path:   "/v1/streams",
			Query: map[string]string{
				"cursor": cursor,
				"limit":  limit,
			},
		})
		if err != nil || status != http.StatusOK {
			continue
		}
		var resp nodeStreamsResponse
		if err := json.Unmarshal(payload, &resp); err != nil {
			continue
		}
		for _, ev := range resp.Events {
			ev["node_id"] = nodeID
			out = append(out, ev)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
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

	shards := s.meta.ShardsSnapshot()
	startShardIdx, shardCursor, err := parseGatewayCursor(req.Cursor)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if startShardIdx < 0 || startShardIdx >= len(shards) {
		writeError(w, http.StatusBadRequest, "cursor shard index out of range")
		return
	}

	rows := make([]scanRow, 0, req.Limit)
	nextCursor := ""

	for i := startShardIdx; i < len(shards) && len(rows) < req.Limit; i++ {
		shard := shards[i]
		nodeID := shard.Leader
		if req.Consistency == "eventual" && len(shard.Followers) > 0 {
			nodeID = shard.Followers[0]
		}

		addr, err := s.addressByNodeID(nodeID)
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
				SortKey: rr.SortKey,
				Item:    rr.Item,
				ShardID: shard.ID,
			})
		}

		if len(rows) == req.Limit {
			if nodeResp.NextCursor != "" {
				nextCursor = fmt.Sprintf("%d|%s", i, nodeResp.NextCursor)
			} else if len(nodeResp.Rows) > 0 && i < len(shards)-1 {
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

func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.mu.RLock()
	nodes := make(map[string]string, len(s.nodeAddrs))
	for id, addr := range s.nodeAddrs {
		nodes[id] = addr
	}
	order := append([]string(nil), s.nodeOrder...)
	s.mu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"nodes":       nodes,
		"node_order":  order,
		"shards":      s.meta.ShardsSnapshot(),
		"table_names": s.meta.TableNames(),
	})
}

func (s *Server) handleUpsertTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req tableUpsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Name == "" || req.PartitionKey == "" {
		writeError(w, http.StatusBadRequest, "name and partition_key are required")
		return
	}
	if req.ReplicationFactor == 0 {
		req.ReplicationFactor = 2
	}

	t := metadata.Table{
		Name:              req.Name,
		PartitionKey:      req.PartitionKey,
		SortKey:           req.SortKey,
		GSI1Name:          req.GSI1Name,
		GSI1PKAttribute:   req.GSI1PKAttribute,
		GSI1SKAttribute:   req.GSI1SKAttribute,
		TTLAttribute:      req.TTLAttribute,
		ReplicationFactor: req.ReplicationFactor,
	}
	if err := s.meta.UpsertTable(t); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.pushTableMetadata(r.Context(), t); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "table": t})
}

func (s *Server) handleAddNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req adminNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.ID == "" || req.Address == "" {
		writeError(w, http.StatusBadRequest, "id and address are required")
		return
	}

	oldAddrs, oldOrder := s.nodeStateSnapshot()
	if _, exists := oldAddrs[req.ID]; exists {
		writeError(w, http.StatusConflict, "node already exists")
		return
	}

	newAddrs := copyNodeAddrs(oldAddrs)
	newAddrs[req.ID] = req.Address
	newOrder := append(append([]string(nil), oldOrder...), req.ID)

	moved, shards, err := s.rebalanceToTopology(r.Context(), newAddrs, newOrder)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"action":        "add_node",
		"node_id":       req.ID,
		"moved_records": moved,
		"shards":        shards,
	})
}

func (s *Server) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req adminNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	oldAddrs, oldOrder := s.nodeStateSnapshot()
	if _, exists := oldAddrs[req.ID]; !exists {
		writeError(w, http.StatusNotFound, "node not found")
		return
	}
	if len(oldOrder) <= 1 {
		writeError(w, http.StatusBadRequest, "cannot remove last node")
		return
	}

	newAddrs := copyNodeAddrs(oldAddrs)
	delete(newAddrs, req.ID)

	newOrder := make([]string, 0, len(oldOrder)-1)
	for _, id := range oldOrder {
		if id != req.ID {
			newOrder = append(newOrder, id)
		}
	}

	moved, shards, err := s.rebalanceToTopology(r.Context(), newAddrs, newOrder)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"action":        "remove_node",
		"node_id":       req.ID,
		"moved_records": moved,
		"shards":        shards,
	})
}

func (s *Server) rebalanceToTopology(ctx context.Context, newAddrs map[string]string, newOrder []string) (int, []metadata.Shard, error) {
	if len(newOrder) == 0 {
		return 0, nil, fmt.Errorf("at least one node is required")
	}

	oldShards := s.meta.ShardsSnapshot()
	oldAddrs, _ := s.nodeStateSnapshot()

	records, err := s.collectAllRecords(ctx, oldShards, oldAddrs)
	if err != nil {
		return 0, nil, err
	}

	newShards := buildShards(newOrder)
	s.meta.ReplaceShards(newShards)
	s.setNodeState(newAddrs, newOrder)

	allAddrs := copyNodeAddrs(newAddrs)
	for id, addr := range oldAddrs {
		if _, exists := allAddrs[id]; !exists {
			allAddrs[id] = addr
		}
	}
	if err := s.pushTopology(ctx, newShards, allAddrs); err != nil {
		return 0, nil, err
	}

	moved, err := s.replayRecords(ctx, records)
	if err != nil {
		return moved, nil, err
	}

	return moved, newShards, nil
}

func (s *Server) collectAllRecords(ctx context.Context, shards []metadata.Shard, addrs map[string]string) ([]rebalanceRecord, error) {
	recordsByKey := make(map[string]rebalanceRecord)
	tables := s.meta.TableNames()

	for _, shard := range shards {
		addr, ok := addrs[shard.Leader]
		if !ok {
			return nil, fmt.Errorf("address not found for shard leader %s", shard.Leader)
		}

		for _, table := range tables {
			cursor := ""
			for {
				resp, err := s.scanNode(ctx, addr, nodeScanRequest{
					Table:   table,
					ShardID: shard.ID,
					Cursor:  cursor,
					Limit:   1000,
				})
				if err != nil {
					return nil, fmt.Errorf("scan shard %d table %s failed: %w", shard.ID, table, err)
				}

				for _, row := range resp.Rows {
					id := table + "\x00" + row.Key + "\x00" + row.SortKey
					recordsByKey[id] = rebalanceRecord{
						Table:   table,
						Key:     row.Key,
						SortKey: row.SortKey,
						Item:    row.Item,
					}
				}

				if resp.NextCursor == "" {
					break
				}
				cursor = resp.NextCursor
			}
		}
	}

	out := make([]rebalanceRecord, 0, len(recordsByKey))
	for _, rec := range recordsByKey {
		out = append(out, rec)
	}
	return out, nil
}

func (s *Server) replayRecords(ctx context.Context, records []rebalanceRecord) (int, error) {
	moved := 0
	for _, rec := range records {
		addr, shardID, err := s.resolveNodeForKey(rec.Key, "strong")
		if err != nil {
			return moved, err
		}

		body, err := json.Marshal(nodeUpsertRequest{
			Table:   rec.Table,
			Key:     rec.Key,
			SortKey: rec.SortKey,
			Item:    rec.Item,
			ShardID: shardID,
		})
		if err != nil {
			return moved, err
		}

		status, payload, err := s.forwardNode(ctx, addr, transport.Request{
			Method: http.MethodPut,
			Path:   "/v1/kv",
			Body:   body,
		})
		if err != nil {
			return moved, err
		}
		if status != http.StatusOK {
			return moved, fmt.Errorf("replay put failed status=%d body=%s", status, strings.TrimSpace(string(payload)))
		}

		moved++
	}
	return moved, nil
}

func (s *Server) pushTopology(ctx context.Context, shards []metadata.Shard, addrs map[string]string) error {
	body, err := json.Marshal(topologyRequest{Shards: shards})
	if err != nil {
		return err
	}

	for nodeID, addr := range addrs {
		status, payload, err := s.forwardNode(ctx, addr, transport.Request{
			Method: http.MethodPost,
			Path:   "/v1/admin/topology",
			Body:   body,
		})
		if err != nil {
			return fmt.Errorf("topology push failed for %s: %w", nodeID, err)
		}
		if status != http.StatusOK {
			return fmt.Errorf("topology push failed for %s status=%d body=%s", nodeID, status, strings.TrimSpace(string(payload)))
		}
	}
	return nil
}

func (s *Server) pushTableMetadata(ctx context.Context, table metadata.Table) error {
	body, err := json.Marshal(nodeTableUpsertRequest{Table: table})
	if err != nil {
		return err
	}
	addrs, _ := s.nodeStateSnapshot()
	for nodeID, addr := range addrs {
		status, payload, err := s.forwardNode(ctx, addr, transport.Request{
			Method: http.MethodPost,
			Path:   "/v1/admin/table/upsert",
			Body:   body,
		})
		if err != nil {
			return fmt.Errorf("table upsert push failed for %s: %w", nodeID, err)
		}
		if status != http.StatusOK {
			return fmt.Errorf("table upsert push failed for %s status=%d body=%s", nodeID, status, strings.TrimSpace(string(payload)))
		}
	}
	return nil
}

func (s *Server) scanNode(ctx context.Context, addr string, req nodeScanRequest) (nodeScanResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nodeScanResponse{}, err
	}

	status, payload, err := s.forwardNode(ctx, addr, transport.Request{
		Method: http.MethodPost,
		Path:   "/v1/scan",
		Body:   body,
	})
	if err != nil {
		return nodeScanResponse{}, err
	}
	if status != http.StatusOK {
		return nodeScanResponse{}, fmt.Errorf("node %s returned status %d: %s", addr, status, strings.TrimSpace(string(payload)))
	}

	var out nodeScanResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		return nodeScanResponse{}, err
	}
	return out, nil
}

func (s *Server) resolveNodeForKey(key, consistency string) (string, int, error) {
	route := s.router.RouteByPartitionKey(key)
	shard, err := s.meta.ShardByID(route.ShardID)
	if err != nil {
		return "", 0, err
	}

	nodeID := shard.Leader
	if consistency == "eventual" && len(shard.Followers) > 0 {
		nodeID = shard.Followers[0]
	}
	addr, err := s.addressByNodeID(nodeID)
	if err != nil {
		return "", 0, err
	}
	return addr, route.ShardID, nil
}

func (s *Server) addressByNodeID(nodeID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	addr, ok := s.nodeAddrs[nodeID]
	if !ok {
		return "", fmt.Errorf("unknown node id: %s", nodeID)
	}
	return addr, nil
}

func (s *Server) nodeStateSnapshot() (map[string]string, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return copyNodeAddrs(s.nodeAddrs), append([]string(nil), s.nodeOrder...)
}

func (s *Server) setNodeState(nodeAddrs map[string]string, nodeOrder []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for addr, client := range s.clients {
		if !containsAddr(nodeAddrs, addr) {
			client.Close()
			delete(s.clients, addr)
		}
	}
	s.nodeAddrs = copyNodeAddrs(nodeAddrs)
	s.nodeOrder = append([]string(nil), nodeOrder...)
}

func (s *Server) forwardNode(ctx context.Context, addr string, req transport.Request) (int, []byte, error) {
	client := s.clientForAddr(addr)
	resp, err := client.Do(ctx, req)
	if err != nil {
		return 0, nil, err
	}
	return resp.Status, resp.Body, nil
}

func (s *Server) clientForAddr(addr string) *transport.Client {
	s.mu.Lock()
	defer s.mu.Unlock()

	client, ok := s.clients[addr]
	if ok {
		return client
	}

	client = transport.NewClient(addr, 3*time.Second, 8)
	s.clients[addr] = client
	return client
}

func chooseItem(req kvRequest) map[string]any {
	if req.Item != nil {
		return req.Item
	}
	return map[string]any{"value": req.Value}
}

func buildShards(nodeOrder []string) []metadata.Shard {
	shards := make([]metadata.Shard, 0, len(nodeOrder))
	for i, nodeID := range nodeOrder {
		shard := metadata.Shard{
			ID:     i,
			Leader: nodeID,
		}
		if len(nodeOrder) > 1 {
			next := nodeOrder[(i+1)%len(nodeOrder)]
			if next != nodeID {
				shard.Followers = []string{next}
			}
		}
		shards = append(shards, shard)
	}
	return shards
}

func copyNodeAddrs(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func containsAddr(addrs map[string]string, target string) bool {
	for _, addr := range addrs {
		if addr == target {
			return true
		}
	}
	return false
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

func writeJSONRaw(w http.ResponseWriter, status int, payload []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
