package config

// NodeConfig represents the address of a single node.
type NodeConfig struct {
	ID      int
	Address string // e.g., "localhost:8001"
}

// ClusterConfig holds the configuration for all nodes in the cluster.
type ClusterConfig struct {
	Nodes []NodeConfig
}

// DefaultCluster returns a default 3-node cluster configuration.
func DefaultCluster() *ClusterConfig {
	return &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 0, Address: "localhost:8001"},
			{ID: 1, Address: "localhost:8002"},
			{ID: 2, Address: "localhost:8003"},
		},
	}
}

// GetNode returns the config for a specific node ID.
func (c *ClusterConfig) GetNode(id int) *NodeConfig {
	for i := range c.Nodes {
		if c.Nodes[i].ID == id {
			return &c.Nodes[i]
		}
	}
	return nil
}

// PeerAddresses returns addresses of all nodes except the given one.
func (c *ClusterConfig) PeerAddresses(selfID int) []string {
	var peers []string
	for _, node := range c.Nodes {
		if node.ID != selfID {
			peers = append(peers, node.Address)
		}
	}
	return peers
}
