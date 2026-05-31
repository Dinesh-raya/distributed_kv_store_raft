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

// Stop signals the node to shut down.
func (rn *RaftNode) Stop() {
	close(rn.quitCh)
}
