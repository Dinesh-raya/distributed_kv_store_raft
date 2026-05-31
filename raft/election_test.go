package raft

import (
	"testing"
)

func TestRequestVoteAsFollower(t *testing.T) {
	node := NewNode(0, []int{1, 2})

	// A candidate with higher term should get our vote
	args := &RequestVoteArgs{
		CandidateId:  1,
		Term:         1,
		LastLogIndex: -1,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{}

	node.handleRequestVote(args, reply)

	if !reply.VoteGranted {
		t.Error("expected vote to be granted")
	}
	if reply.Term != 1 {
		t.Errorf("expected term 1, got %d", reply.Term)
	}
	if node.votedFor != 1 {
		t.Errorf("expected votedFor 1, got %d", node.votedFor)
	}
}

func TestRequestVoteRejectsLowerTerm(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	node.currentTerm = 5

	args := &RequestVoteArgs{
		CandidateId:  1,
		Term:         3, // lower than our term
		LastLogIndex: -1,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{}

	node.handleRequestVote(args, reply)

	if reply.VoteGranted {
		t.Error("expected vote to be rejected (lower term)")
	}
	if reply.Term != 5 {
		t.Errorf("expected reply term 5, got %d", reply.Term)
	}
}

func TestRequestVoteRejectsAlreadyVoted(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	node.currentTerm = 1
	node.votedFor = 2 // already voted for node 2

	args := &RequestVoteArgs{
		CandidateId:  1,
		Term:         1,
		LastLogIndex: -1,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{}

	node.handleRequestVote(args, reply)

	if reply.VoteGranted {
		t.Error("expected vote to be rejected (already voted)")
	}
}

func TestRequestVoteHigherTermStepsDown(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	node.currentTerm = 3
	node.state = Leader

	args := &RequestVoteArgs{
		CandidateId:  1,
		Term:         5, // higher than our term
		LastLogIndex: -1,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{}

	node.handleRequestVote(args, reply)

	if node.state != Follower {
		t.Errorf("expected to step down to Follower, got %s", node.state)
	}
	if node.currentTerm != 5 {
		t.Errorf("expected term 5, got %d", node.currentTerm)
	}
}

func TestRequestVoteRejectsStaleLog(t *testing.T) {
	node := NewNode(0, []int{1, 2})
	node.currentTerm = 1
	// Our log has an entry from term 1
	node.log = []LogEntry{{Term: 1, Index: 0, Command: Command{Op: "SET", Key: "a", Value: "1"}}}

	// Candidate has log from term 0 (stale)
	args := &RequestVoteArgs{
		CandidateId:  1,
		Term:         2,
		LastLogIndex: -1,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{}

	node.handleRequestVote(args, reply)

	if reply.VoteGranted {
		t.Error("expected vote to be rejected (candidate log is stale)")
	}
}

func TestCountVotes(t *testing.T) {
	node := NewNode(0, []int{1, 2, 3})
	node.becomeCandidate()

	// Majority of 4 nodes is 3 (> 4/2). With 2 votes we don't have majority.
	if node.hasMajority(2) {
		t.Error("2 votes should NOT be majority for 4-node cluster")
	}

	// With 3 votes we have majority.
	if !node.hasMajority(3) {
		t.Error("3 votes should be majority for 4-node cluster")
	}
}
