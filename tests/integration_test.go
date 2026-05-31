package tests

import (
	"testing"
	"time"

	"github.com/dines/distributed-kv/raft"
)

func TestLeaderElection(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	cluster.Start()
	defer cluster.Stop()

	leaderID := cluster.WaitForLeader(2 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected within timeout")
	}

	t.Logf("Leader elected: node %d", leaderID)
}

func TestLeaderElectionSingleNode(t *testing.T) {
	cluster := NewTestCluster(t, 1)
	cluster.Start()
	defer cluster.Stop()

	leaderID := cluster.WaitForLeader(1 * time.Second)
	if leaderID == -1 {
		t.Fatal("single node should become leader")
	}

	if leaderID != 0 {
		t.Errorf("expected node 0 to be leader, got %d", leaderID)
	}
}

func TestLogReplication(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	cluster.Start()
	defer cluster.Stop()

	leaderID := cluster.WaitForLeader(2 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	leader := cluster.Node(leaderID)

	entry := leader.Propose(raft.Command{Op: "SET", Key: "name", Value: "dines"})
	if entry == nil {
		t.Fatal("leader should accept proposals")
	}

	time.Sleep(500 * time.Millisecond)

	for _, node := range cluster.nodes {
		if node.LastLogIndex() < 0 {
			t.Errorf("node %d has no log entries", node.ID())
		}
	}
}

func TestFaultToleranceLeaderDown(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	cluster.Start()
	defer cluster.Stop()

	leaderID := cluster.WaitForLeader(2 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	t.Logf("Initial leader: node %d", leaderID)

	cluster.KillNode(leaderID)

	time.Sleep(1 * time.Second)

	newLeaderID := -1
	for _, node := range cluster.nodes {
		if node.ID() != leaderID && node.IsLeader() {
			newLeaderID = node.ID()
			break
		}
	}

	if newLeaderID == -1 {
		t.Fatal("no new leader elected after leader failure")
	}

	t.Logf("New leader after failure: node %d", newLeaderID)
}

func TestFaultToleranceMinorityDown(t *testing.T) {
	cluster := NewTestCluster(t, 3)
	cluster.Start()
	defer cluster.Stop()

	leaderID := cluster.WaitForLeader(2 * time.Second)
	if leaderID == -1 {
		t.Fatal("no leader elected")
	}

	// Kill one follower (not the leader)
	followerID := -1
	for _, node := range cluster.nodes {
		if node.ID() != leaderID {
			followerID = node.ID()
			break
		}
	}
	cluster.KillNode(followerID)

	// Wait and verify the cluster still has a leader
	time.Sleep(1 * time.Second)

	// Check that some node is still leader (may or may not be the same one)
	hasLeader := false
	for _, node := range cluster.nodes {
		if node.ID() != followerID && node.IsLeader() {
			hasLeader = true
			break
		}
	}

	if !hasLeader {
		t.Error("cluster should still have a leader after minority failure")
	}
}
