package raft

import (
	"testing"
)

func TestAppendEntriesAsLeader(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	node.becomeLeader()
	node.currentTerm = 1

	entry := node.propose(Command{Op: "SET", Key: "foo", Value: "bar"})
	if entry == nil {
		t.Fatal("expected entry, got nil")
	}
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

func TestAppendEntriesRejectsFollower(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	entry := node.propose(Command{Op: "SET", Key: "foo", Value: "bar"})
	if entry != nil {
		t.Error("follower should not accept proposals")
	}
}

func TestHandleAppendEntries(t *testing.T) {
	node := NewNode(1, []int{0, 2})
	node.currentTerm = 1

	args := &AppendEntriesArgs{
		LeaderId:     0,
		Term:         1,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 1, Index: 0, Command: Command{Op: "SET", Key: "a", Value: "1"}},
		},
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}

	node.handleAppendEntries(args, reply)

	if !reply.Success {
		t.Error("expected AppendEntries to succeed")
	}
	if len(node.log) != 1 {
		t.Errorf("expected log length 1, got %d", len(node.log))
	}
	if node.log[0].Command.Key != "a" {
		t.Errorf("expected key 'a', got '%s'", node.log[0].Command.Key)
	}
}

func TestHandleAppendEntriesRejectsStaleTerm(t *testing.T) {
	node := NewNode(1, []int{0, 2})
	node.currentTerm = 3

	args := &AppendEntriesArgs{
		LeaderId:     0,
		Term:         2,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}

	node.handleAppendEntries(args, reply)

	if reply.Success {
		t.Error("expected AppendEntries to be rejected (stale term)")
	}
	if reply.Term != 3 {
		t.Errorf("expected reply term 3, got %d", reply.Term)
	}
}

func TestHandleAppendEntriesHigherTermStepsDown(t *testing.T) {
	node := NewNode(1, []int{0, 2})
	node.currentTerm = 1
	node.state = Leader

	args := &AppendEntriesArgs{
		LeaderId:     0,
		Term:         5,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}

	node.handleAppendEntries(args, reply)

	if node.state != Follower {
		t.Errorf("expected to step down to Follower, got %s", node.state)
	}
	if node.currentTerm != 5 {
		t.Errorf("expected term 5, got %d", node.currentTerm)
	}
}

func TestHandleAppendEntriesRejectsInconsistentLog(t *testing.T) {
	node := NewNode(1, []int{0, 2})
	node.currentTerm = 1

	args1 := &AppendEntriesArgs{
		LeaderId:     0,
		Term:         1,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 1, Index: 0, Command: Command{Op: "SET", Key: "a", Value: "1"}},
		},
		LeaderCommit: -1,
	}
	reply1 := &AppendEntriesReply{}
	node.handleAppendEntries(args1, reply1)
	if !reply1.Success {
		t.Fatal("first AppendEntries should succeed")
	}

	args2 := &AppendEntriesArgs{
		LeaderId:     0,
		Term:         1,
		PrevLogIndex: 0,
		PrevLogTerm:  2,
		Entries: []LogEntry{
			{Term: 1, Index: 1, Command: Command{Op: "SET", Key: "b", Value: "2"}},
		},
		LeaderCommit: 0,
	}
	reply2 := &AppendEntriesReply{}
	node.handleAppendEntries(args2, reply2)

	if reply2.Success {
		t.Error("expected AppendEntries to be rejected (inconsistent log)")
	}
}

func TestCommitIndexUpdate(t *testing.T) {
	node := NewNode(1, []int{0, 2})
	node.currentTerm = 1

	node.log = []LogEntry{
		{Term: 1, Index: 0, Command: Command{Op: "SET", Key: "a", Value: "1"}},
		{Term: 1, Index: 1, Command: Command{Op: "SET", Key: "b", Value: "2"}},
	}

	args := &AppendEntriesArgs{
		LeaderId:     0,
		Term:         1,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 1,
	}
	reply := &AppendEntriesReply{}
	node.handleAppendEntries(args, reply)

	if node.commitIndex != 1 {
		t.Errorf("expected commitIndex 1, got %d", node.commitIndex)
	}
}
