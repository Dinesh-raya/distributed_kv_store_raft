package raft

// State represents the role of a Raft node.
type State int

const (
	Follower  State = iota // 0
	Candidate              // 1
	Leader                 // 2
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    int
	Index   int
	Command Command
}

// Command represents a KV operation.
type Command struct {
	Op    string // "SET", "GET", "DELETE"
	Key   string
	Value string // only used for SET
}

// --- RequestVote RPC ---

type RequestVoteArgs struct {
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// --- AppendEntries RPC ---

type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// --- Client RPC ---

type ClientRequest struct {
	Command Command
}

type ClientResponse struct {
	Success  bool
	Value    string // for GET responses
	LeaderId int    // if redirected, the leader's ID
	Error    string
}
