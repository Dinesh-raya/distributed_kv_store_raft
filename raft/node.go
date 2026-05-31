package raft

import (
	"sync"
	"time"
)

// RaftNode holds all state for a single Raft node.
type RaftNode struct {
	mu       sync.Mutex
	stopOnce sync.Once

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
	applyCh     chan ApplyMsg // committed entries sent here for state machine
	quitCh      chan struct{}
	heartbeatCh chan struct{} // signal when heartbeat received

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
		heartbeatCh: make(chan struct{}, 100),
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
	rn.stopOnce.Do(func() {
		close(rn.quitCh)
	})
}

// Start begins the node's main run loop.
func (rn *RaftNode) Start() {
	go rn.run()
}

// SetSendRequestVote sets the function used to send RequestVote RPCs.
func (rn *RaftNode) SetSendRequestVote(fn func(int, *RequestVoteArgs, *RequestVoteReply)) {
	rn.sendRequestVote = fn
}

// SetSendAppendEntries sets the function used to send AppendEntries RPCs.
func (rn *RaftNode) SetSendAppendEntries(fn func(int, *AppendEntriesArgs, *AppendEntriesReply)) {
	rn.sendAppendEntries = fn
}

// run is the main loop for the Raft node.
func (rn *RaftNode) run() {
	for {
		select {
		case <-rn.quitCh:
			return
		default:
		}

		rn.mu.Lock()
		state := rn.state
		rn.mu.Unlock()

		if state == Leader {
			// Send heartbeats / replicate
			var wg sync.WaitGroup
			for _, peerID := range rn.peers {
				wg.Add(1)
				go func(pid int) {
					defer wg.Done()
					rn.replicateToPeer(pid)
				}(peerID)
			}
			wg.Wait()
			rn.advanceCommitIndex()
			rn.applyCommittedEntries()
			time.Sleep(heartbeatInterval)
		} else {
			// Wait for election timeout or heartbeat
			timeout := randomElectionTimeout()
			select {
			case <-rn.quitCh:
				return
			case <-rn.heartbeatCh:
				// Received heartbeat — reset timer, stay follower
				continue
			case <-time.After(timeout):
			}

			rn.mu.Lock()
			isFollower := rn.state == Follower || rn.state == Candidate
			rn.mu.Unlock()

			if isFollower {
				rn.startElection()
			}
		}
	}
}

// propose and handleAppendEntries are implemented in replication.go.
