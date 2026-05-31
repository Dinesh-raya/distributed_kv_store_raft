package raft

// propose adds a command to the leader's log.
// Returns the log entry if this node is the leader, nil otherwise.
func (rn *RaftNode) propose(cmd Command) *LogEntry {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return nil
	}

	entry := rn.appendLogEntry(cmd, rn.currentTerm)
	return &entry
}

// handleAppendEntries processes an AppendEntries RPC from the leader.
func (rn *RaftNode) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.Term = rn.currentTerm
	reply.Success = false

	// If leader's term is lower, reject
	if args.Term < rn.currentTerm {
		return
	}

	// If leader's term is higher or equal, step down
	if args.Term >= rn.currentTerm {
		rn.becomeFollower(args.Term)
	}

	// Check log consistency at prevLogIndex
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(rn.log) {
			return
		}
		if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return
		}
	}

	// Append new entries (overwrite any conflicting entries)
	for i, entry := range args.Entries {
		idx := entry.Index
		if idx < len(rn.log) {
			if rn.log[idx].Term != entry.Term {
				rn.log = rn.log[:idx]
				rn.log = append(rn.log, args.Entries[i:]...)
				break
			}
		} else {
			rn.log = append(rn.log, args.Entries[i:]...)
			break
		}
	}

	// Update commit index
	if args.LeaderCommit > rn.commitIndex {
		newCommit := args.LeaderCommit
		if newCommit >= len(rn.log) {
			newCommit = len(rn.log) - 1
		}
		rn.commitIndex = newCommit
	}

	reply.Success = true

	// Signal heartbeat to reset election timer
	select {
	case rn.heartbeatCh <- struct{}{}:
	default:
	}
}

// replicateToPeer sends log entries to a single peer.
func (rn *RaftNode) replicateToPeer(peerID int) bool {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return false
	}

	nextIdx := rn.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(rn.log) {
		prevLogTerm = rn.log[prevLogIndex].Term
	}

	var entries []LogEntry
	if nextIdx < len(rn.log) {
		entries = make([]LogEntry, len(rn.log)-nextIdx)
		copy(entries, rn.log[nextIdx:])
	}

	args := &AppendEntriesArgs{
		LeaderId:     rn.id,
		Term:         rn.currentTerm,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rn.commitIndex,
	}
	rn.mu.Unlock()

	reply := &AppendEntriesReply{}
	rn.sendAppendEntries(peerID, args, reply)

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if reply.Term > rn.currentTerm {
		rn.becomeFollower(reply.Term)
		return false
	}

	if reply.Success {
		if len(entries) > 0 {
			lastEntry := entries[len(entries)-1]
			rn.nextIndex[peerID] = lastEntry.Index + 1
			rn.matchIndex[peerID] = lastEntry.Index
		}
		return true
	}

	if rn.nextIndex[peerID] > 0 {
		rn.nextIndex[peerID]--
	}
	return false
}

// advanceCommitIndex checks if any new entries can be committed.
func (rn *RaftNode) advanceCommitIndex() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return
	}

	for n := len(rn.log) - 1; n > rn.commitIndex; n-- {
		if rn.log[n].Term != rn.currentTerm {
			continue
		}

		count := 1 // count self
		for _, peerID := range rn.peers {
			if rn.matchIndex[peerID] >= n {
				count++
			}
		}

		if rn.hasMajority(count) {
			rn.commitIndex = n
			break
		}
	}
}

// applyCommittedEntries sends committed entries to the apply channel.
func (rn *RaftNode) applyCommittedEntries() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]
		msg := ApplyMsg{
			Command: entry.Command,
			Index:   entry.Index,
			Term:    entry.Term,
		}
		rn.applyCh <- msg
	}
}
