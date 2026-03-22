package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"

	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/replication"
	"github.com/nitinankad/rocketdb/internal/shard"
	"github.com/nitinankad/rocketdb/internal/storage"
)

func main() {
	addr := flag.String("addr", ":8081", "node listen address")
	nodeID := flag.String("node-id", "node-1", "node identifier")
	dataDir := flag.String("data-dir", filepath.Join("data", "single"), "node data directory")
	flag.Parse()

	storePath := filepath.Join(*dataDir, *nodeID+".json")
	store, err := storage.NewDisk(storePath)
	if err != nil {
		log.Fatalf("storage init failed: %v", err)
	}
	meta := metadata.DefaultBootstrap()
	repl := replication.NewNoopManager()
	node := shard.NewNode(*nodeID, store, repl, meta)

	mux := http.NewServeMux()
	node.RegisterHTTP(mux)

	log.Printf("rocketdb-node id=%s addr=%s data=%s", *nodeID, *addr, storePath)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("node server failed: %v", err)
	}
}
