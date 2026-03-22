package config

import "fmt"

type Node struct {
	ID      string
	Address string
}

type Cluster struct {
	GatewayAddress string
	Nodes          []Node
}

func DefaultCluster() Cluster {
	return Cluster{
		GatewayAddress: ":8080",
		Nodes: []Node{
			{ID: "node-1", Address: ":8081"},
			{ID: "node-2", Address: ":8082"},
			{ID: "node-3", Address: ":8083"},
		},
	}
}

func (c Cluster) AddressByNodeID(nodeID string) (string, error) {
	for _, n := range c.Nodes {
		if n.ID == nodeID {
			return n.Address, nil
		}
	}
	return "", fmt.Errorf("unknown node id: %s", nodeID)
}
