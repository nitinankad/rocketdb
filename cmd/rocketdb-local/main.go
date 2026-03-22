package main

import (
	"log"
	"net/http"
	"path/filepath"

	"github.com/nitinankad/rocketdb/internal/config"
	"github.com/nitinankad/rocketdb/internal/gateway"
	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/replication"
	"github.com/nitinankad/rocketdb/internal/router"
	"github.com/nitinankad/rocketdb/internal/shard"
	"github.com/nitinankad/rocketdb/internal/storage"
)

func main() {
	cluster := config.DefaultCluster()
	meta := metadata.DefaultBootstrap()
	rt := router.New(meta)

	gatewaySrv := gateway.NewServer(rt, meta, cluster)
	gatewayMux := http.NewServeMux()
	gatewaySrv.RegisterHTTP(gatewayMux)
	go func() {
		log.Printf("rocketdb-gateway addr=%s", cluster.GatewayAddress)
		if err := http.ListenAndServe(cluster.GatewayAddress, gatewayMux); err != nil {
			log.Fatalf("gateway server failed: %v", err)
		}
	}()

	for _, n := range cluster.Nodes {
		storePath := filepath.Join("data", "local", n.ID+".json")
		store, err := storage.NewDisk(storePath)
		if err != nil {
			log.Fatalf("storage init failed id=%s: %v", n.ID, err)
		}

		node := shard.NewNode(n.ID, store, replication.NewNoopManager(), meta)
		nodeMux := http.NewServeMux()
		node.RegisterHTTP(nodeMux)

		go func(nodeID, addr, dataPath string, mux *http.ServeMux) {
			log.Printf("rocketdb-node id=%s addr=%s data=%s", nodeID, addr, dataPath)
			if err := http.ListenAndServe(addr, mux); err != nil {
				log.Fatalf("node server failed id=%s: %v", nodeID, err)
			}
		}(n.ID, n.Address, storePath, nodeMux)
	}

	select {}
}
