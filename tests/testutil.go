package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/dines/distributed-kv/raft"
)

// TestCluster manages an in-process cluster of Raft nodes for testing.
type TestCluster struct {
	t     *testing.T
	nodes []*raft.RaftNode
	mu    sync.Mutex
}

// NewTestCluster creates a cluster of n nodes for testing.
func NewTestCluster(t *testing.T, n int) *TestCluster {
	t.Helper()

	cluster := &TestCluster{
		t:     t,
		nodes: make([]*raft.RaftNode, n),
	}

	allIDs := make([]int, n)
	for i := 0; i < n; i++ {
		allIDs[i] = i
	}

	for i := 0; i < n; i++ {
		peers := make([]int, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, j)
			}
		}
		node := raft.NewNode(i, peers)
		cluster.nodes[i] = node
	}

	cluster.wireRPCs()

	return cluster
}

// wireRPCs connects the nodes so they can call each other's RPC handlers directly.
func (tc *TestCluster) wireRPCs() {
	for _, node := range tc.nodes {
		n := node
		n.SetSendRequestVote(func(peerID int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) {
			tc.nodes[peerID].HandleRequestVote(args, reply)
		})

		n.SetSendAppendEntries(func(peerID int, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) {
			tc.nodes[peerID].HandleAppendEntries(args, reply)
		})
	}
}

// Start starts all nodes in the cluster.
func (tc *TestCluster) Start() {
	for _, node := range tc.nodes {
		node.Start()
	}
}

// Stop stops all nodes in the cluster.
func (tc *TestCluster) Stop() {
	for _, node := range tc.nodes {
		node.Stop()
	}
}

// Node returns the node at the given index.
func (tc *TestCluster) Node(id int) *raft.RaftNode {
	return tc.nodes[id]
}

// WaitForLeader waits until a leader is elected and returns its ID.
func (tc *TestCluster) WaitForLeader(timeout time.Duration) int {
	tc.t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range tc.nodes {
			if node.IsLeader() {
				return node.ID()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return -1
}

// KillNode stops a node to simulate failure.
func (tc *TestCluster) KillNode(id int) {
	tc.nodes[id].Stop()
}
