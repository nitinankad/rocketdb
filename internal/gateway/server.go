package gateway

import (
	"encoding/json"
	"net/http"

	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/router"
)

type Server struct {
	router *router.Router
	meta   *metadata.Service
}

func NewServer(rt *router.Router, meta *metadata.Service) *Server {
	return &Server{
		router: rt,
		meta:   meta,
	}
}

func (s *Server) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/route", s.handleRoute)
}

type routeRequest struct {
	Table        string `json:"table"`
	PartitionKey string `json:"partition_key"`
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
