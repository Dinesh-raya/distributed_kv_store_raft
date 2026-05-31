package raft

import (
	"sync"
)

// RaftNode holds all state for a single Raft node.
type RaftNode struct {
	mu sync.Mutex

	// Identity
	id     int
	peers  []int // peer node IDs
	state  State

	// Persistent state (survive crashes -- but we keep in memory for simplicity)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex int // highest log entry known to be committed
	lastApplied int // highest log entry applied to state machine

	// Leader state (reinitialized after election)
	nextIndex  map[int]int // for each peer, index of next log entry to send
	matchIndex map[int]int // for each peer, highest log entry known to be replicated

	// Channels for signaling
	applyCh chan ApplyMsg // committed entries sent here for state machine
	quitCh  chan struct{}

	// Function fields for testability (overridden in tests)
	sendRequestVote   func(int, *RequestVoteArgs, *RequestVoteReply)
	sendAppendEntries func(int, *AppendEntriesArgs, *AppendEntriesReply)
}

// ApplyMsg is sent on applyCh when a log entry is committed.
type ApplyMsg struct {
	Command Command
	Index   int
	Term    int
}

// NewNode creates a new Raft node.
func NewNode(id int, peers []int) *RaftNode {
	return &RaftNode{
		id:          id,
		peers:       peers,
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 0),
		commitIndex: -1,
		lastApplied: -1,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		applyCh:     make(chan ApplyMsg, 100),
		quitCh:      make(chan struct{}),
		sendRequestVote:   func(int, *RequestVoteArgs, *RequestVoteReply) {},
		sendAppendEntries: func(int, *AppendEntriesArgs, *AppendEntriesReply) {},
	}
}

// becomeFollower transitions the node to follower state for the given term.
func (rn *RaftNode) becomeFollower(term int) {
	rn.state = Follower
	rn.currentTerm = term
	rn.votedFor = -1
}

// becomeCandidate transitions the node to candidate state.
func (rn *RaftNode) becomeCandidate() {
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
}

// becomeLeader transitions the node to leader state.
func (rn *RaftNode) becomeLeader() {
	rn.state = Leader
	// Initialize nextIndex for all peers to one past the last log entry
	lastIdx := len(rn.log)
	for _, peerID := range rn.peers {
		rn.nextIndex[peerID] = lastIdx
		rn.matchIndex[peerID] = -1
	}
}

// appendLogEntry adds a new command to the log and returns the entry.
func (rn *RaftNode) appendLogEntry(cmd Command, term int) LogEntry {
	entry := LogEntry{
		Term:    term,
		Index:   len(rn.log),
		Command: cmd,
	}
	rn.log = append(rn.log, entry)
	return entry
}

// lastLogIndex returns the index of the last log entry, or -1 if empty.
func (rn *RaftNode) lastLogIndex() int {
	return len(rn.log) - 1
}

// lastLogTerm returns the term of the last log entry, or 0 if empty.
func (rn *RaftNode) lastLogTerm() int {
	if len(rn.log) == 0 {
		return 0
	}
	return rn.log[len(rn.log)-1].Term
}

// ID returns the node's ID.
func (rn *RaftNode) ID() int {
	return rn.id
}

// IsLeader returns true if this node is the leader.
func (rn *RaftNode) IsLeader() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state == Leader
}

// HandleRequestVote exposes handleRequestVote for testing.
func (rn *RaftNode) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rn.handleRequestVote(args, reply)
}

// HandleAppendEntries exposes handleAppendEntries for testing.
func (rn *RaftNode) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.handleAppendEntries(args, reply)
}

// Propose exposes propose for testing.
func (rn *RaftNode) Propose(cmd Command) *LogEntry {
	return rn.propose(cmd)
}

// LastLogIndex exposes lastLogIndex for testing.
func (rn *RaftNode) LastLogIndex() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.lastLogIndex()
}

// Stop signals the node to shut down.
func (rn *RaftNode) Stop() {
	close(rn.quitCh)
}

// handleAppendEntries handles an incoming AppendEntries RPC.
// Stub implementation -- will be fully implemented in the log replication task.
func (rn *RaftNode) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.Success = false

	// Reject if term is stale
	if args.Term < rn.currentTerm {
		return
	}

	// Step down if we see a higher term
	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
	}

	// Reject if log doesn't contain an entry at prevLogIndex with prevLogTerm
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(rn.log) {
			return
		}
		if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return
		}
	}

	// Append entries
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rn.log) {
			if rn.log[idx].Term != entry.Term {
				rn.log = rn.log[:idx]
				rn.log = append(rn.log, entry)
			}
		} else {
			rn.log = append(rn.log, entry)
		}
	}

	// Update commit index
	if args.LeaderCommit > rn.commitIndex {
		lastNewEntry := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewEntry {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNewEntry
		}
	}

	reply.Success = true
}

// propose adds a command to the leader's log.
// Returns nil if this node is not the leader.
func (rn *RaftNode) propose(cmd Command) *LogEntry {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return nil
	}

	entry := rn.appendLogEntry(cmd, rn.currentTerm)
	return &entry
}
