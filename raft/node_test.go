package raft

import (
	"testing"
)

func TestNewNode(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	if node == nil {
		t.Fatal("NewNode returned nil")
	}
	if node.id != 0 {
		t.Errorf("expected id 0, got %d", node.id)
	}
	if node.state != Follower {
		t.Errorf("expected Follower, got %s", node.state)
	}
	if node.currentTerm != 0 {
		t.Errorf("expected term 0, got %d", node.currentTerm)
	}
	if node.votedFor != -1 {
		t.Errorf("expected votedFor -1, got %d", node.votedFor)
	}
}

func TestNodeBecomeFollower(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	node.becomeFollower(5)
	if node.state != Follower {
		t.Errorf("expected Follower, got %s", node.state)
	}
	if node.currentTerm != 5 {
		t.Errorf("expected term 5, got %d", node.currentTerm)
	}
	if node.votedFor != -1 {
		t.Errorf("expected votedFor -1 after term change, got %d", node.votedFor)
	}
}

func TestNodeBecomeLeader(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	node.becomeLeader()
	if node.state != Leader {
		t.Errorf("expected Leader, got %s", node.state)
	}
	for _, peerID := range node.peers {
		if node.nextIndex[peerID] != len(node.log) {
			t.Errorf("expected nextIndex[%d] = %d, got %d", peerID, len(node.log), node.nextIndex[peerID])
		}
	}
}

func TestNodeAppendLogEntry(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	cmd := Command{Op: "SET", Key: "foo", Value: "bar"}
	entry := node.appendLogEntry(cmd, 1)
	if entry.Term != 1 {
		t.Errorf("expected term 1, got %d", entry.Term)
	}
	if entry.Index != 0 {
		t.Errorf("expected index 0, got %d", entry.Index)
	}
	if len(node.log) != 1 {
		t.Errorf("expected log length 1, got %d", len(node.log))
	}
}

func TestNodeLastLogIndex(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	if node.lastLogIndex() != -1 {
		t.Errorf("expected -1 for empty log, got %d", node.lastLogIndex())
	}

	node.appendLogEntry(Command{Op: "SET", Key: "a", Value: "1"}, 1)
	if node.lastLogIndex() != 0 {
		t.Errorf("expected 0, got %d", node.lastLogIndex())
	}

	node.appendLogEntry(Command{Op: "SET", Key: "b", Value: "2"}, 1)
	if node.lastLogIndex() != 1 {
		t.Errorf("expected 1, got %d", node.lastLogIndex())
	}
}

func TestNodeLastLogTerm(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	if node.lastLogTerm() != 0 {
		t.Errorf("expected 0 for empty log, got %d", node.lastLogTerm())
	}

	node.appendLogEntry(Command{Op: "SET", Key: "a", Value: "1"}, 3)
	if node.lastLogTerm() != 3 {
		t.Errorf("expected 3, got %d", node.lastLogTerm())
	}
}
