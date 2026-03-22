package main

import (
	"log"
	"path/filepath"

	"github.com/nitinankad/rocketdb/internal/config"
	"github.com/nitinankad/rocketdb/internal/gateway"
	"github.com/nitinankad/rocketdb/internal/metadata"
	"github.com/nitinankad/rocketdb/internal/replication"
	"github.com/nitinankad/rocketdb/internal/router"
	"github.com/nitinankad/rocketdb/internal/shard"
	"github.com/nitinankad/rocketdb/internal/storage"
	"github.com/nitinankad/rocketdb/internal/transport"
)

func main() {
	cluster := config.DefaultCluster()
	meta := metadata.DefaultBootstrap()
	rt := router.New(meta)

	gatewaySrv := gateway.NewServer(rt, meta, cluster)
	go func() {
		log.Printf("rocketdb-gateway addr=%s", cluster.GatewayAddress)
		if err := transport.NewServer(gatewaySrv.HandleRPC).ListenAndServe(cluster.GatewayAddress); err != nil {
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
		go func(nodeID, addr, dataPath string) {
			log.Printf("rocketdb-node id=%s addr=%s data=%s", nodeID, addr, dataPath)
			if err := transport.NewServer(node.HandleRPC).ListenAndServe(addr); err != nil {
				log.Fatalf("node server failed id=%s: %v", nodeID, err)
			}
		}(n.ID, n.Address, storePath)
	}

	select {}
}
