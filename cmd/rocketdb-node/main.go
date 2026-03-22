package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/replication"
	"github.com/nitinankad/rocketdb/internal/shard"
	"github.com/nitinankad/rocketdb/internal/storage"
)

func main() {
	addr := flag.String("addr", ":8081", "node listen address")
	nodeID := flag.String("node-id", "node-1", "node identifier")
	flag.Parse()

	store := storage.NewInMemory()
	meta := metadata.DefaultBootstrap()
	repl := replication.NewNoopManager()
	node := shard.NewNode(*nodeID, store, repl, meta)

	mux := http.NewServeMux()
	node.RegisterHTTP(mux)

	log.Printf("rocketdb-node id=%s addr=%s", *nodeID, *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("node server failed: %v", err)
	}
}
