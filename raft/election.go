package raft

import (
	"math/rand"
	"time"
)

const (
	// Election timeout range in milliseconds.
	// Randomized to prevent split votes.
	minElectionTimeout = 300
	maxElectionTimeout = 500

	// Heartbeat interval -- leader sends AppendEntries this often.
	heartbeatInterval = 100 * time.Millisecond
)

// randomElectionTimeout returns a random duration between min and max election timeout.
func randomElectionTimeout() time.Duration {
	ms := minElectionTimeout + rand.Intn(maxElectionTimeout-minElectionTimeout)
	return time.Duration(ms) * time.Millisecond
}

// handleRequestVote processes a RequestVote RPC from a candidate.
func (rn *RaftNode) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	// If candidate's term is lower, reject
	if args.Term < rn.currentTerm {
		return
	}

	// If candidate's term is higher, step down
	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
	}

	// Grant vote if we haven't voted for someone else AND candidate's log is at least as up-to-date
	if (rn.votedFor == -1 || rn.votedFor == args.CandidateId) &&
		rn.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rn.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rn.currentTerm
	}
}

// isLogUpToDate checks if the candidate's log is at least as up-to-date as ours.
// This ensures we only vote for candidates that have all committed entries.
func (rn *RaftNode) isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm int) bool {
	ourLastTerm := rn.lastLogTerm()
	ourLastIndex := rn.lastLogIndex()

	// Compare terms first -- higher term wins
	if candidateLastLogTerm != ourLastTerm {
		return candidateLastLogTerm > ourLastTerm
	}
	// Same term -- compare indices
	return candidateLastLogIndex >= ourLastIndex
}

// hasMajority checks if the given vote count is a majority.
func (rn *RaftNode) hasMajority(votes int) bool {
	total := len(rn.peers) + 1 // +1 for self
	return votes*2 > total
}

// startElection initiates a leader election.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.becomeCandidate()
	term := rn.currentTerm
	lastLogIdx := rn.lastLogIndex()
	lastLogTerm := rn.lastLogTerm()
	rn.mu.Unlock()

	votes := 1 // vote for self
	votesCh := make(chan bool, len(rn.peers))

	// Request votes from all peers
	for _, peerID := range rn.peers {
		go func(peer int) {
			args := &RequestVoteArgs{
				CandidateId:  rn.id,
				Term:         term,
				LastLogIndex: lastLogIdx,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			// Use the function field -- overridable in tests
			rn.sendRequestVote(peer, args, reply)
			votesCh <- reply.VoteGranted
		}(peerID)
	}

	// Collect votes
	for i := 0; i < len(rn.peers); i++ {
		vote := <-votesCh
		if vote {
			votes++
			if rn.hasMajority(votes) {
				rn.mu.Lock()
				if rn.state == Candidate && rn.currentTerm == term {
					rn.becomeLeader()
				}
				rn.mu.Unlock()
				return
			}
		}
	}
}
