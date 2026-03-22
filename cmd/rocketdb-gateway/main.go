package main

import (
	"flag"
	"log"

	"github.com/nitinankad/rocketdb/internal/config"
	"github.com/nitinankad/rocketdb/internal/gateway"
	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/router"
	"github.com/nitinankad/rocketdb/internal/transport"
)

func main() {
	addr := flag.String("addr", ":8080", "gateway listen address")
	flag.Parse()

	cluster := config.DefaultCluster()
	meta := metadata.DefaultBootstrap()
	rt := router.New(meta)
	srv := gateway.NewServer(rt, meta, cluster)

	log.Printf("rocketdb-gateway addr=%s", *addr)
	if err := transport.NewServer(srv.HandleRPC).ListenAndServe(*addr); err != nil {
		log.Fatalf("gateway server failed: %v", err)
	}
}
