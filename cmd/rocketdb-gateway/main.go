package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/nitinankad/rocketdb/internal/gateway"
	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/router"
)

func main() {
	addr := flag.String("addr", ":8080", "gateway listen address")
	flag.Parse()

	meta := metadata.DefaultBootstrap()
	rt := router.New(meta)
	srv := gateway.NewServer(rt, meta)

	mux := http.NewServeMux()
	srv.RegisterHTTP(mux)

	log.Printf("rocketdb-gateway addr=%s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("gateway server failed: %v", err)
	}
}
