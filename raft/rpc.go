package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

// RaftRPC wraps the RaftNode for RPC exposure.
type RaftRPC struct {
	node *RaftNode
}

// RequestVote is the RPC handler for RequestVote.
func (r *RaftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.node.handleRequestVote(args, reply)
	return nil
}

// AppendEntries is the RPC handler for AppendEntries.
func (r *RaftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.node.handleAppendEntries(args, reply)
	return nil
}

// StartRPCServer starts the RPC server for this Raft node.
func (rn *RaftNode) StartRPCServer(address string) error {
	raftRPC := &RaftRPC{node: rn}
	err := rpc.Register(raftRPC)
	if err != nil {
		return fmt.Errorf("failed to register RPC: %v", err)
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", address, err)
	}

	log.Printf("Raft node %d listening on %s", rn.id, address)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-rn.quitCh:
					return
				default:
					log.Printf("Accept error: %v", err)
					continue
				}
			}
			go rpc.ServeConn(conn)
		}
	}()

	return nil
}
