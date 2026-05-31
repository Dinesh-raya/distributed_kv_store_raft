package server

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/dines/distributed-kv/raft"
	"github.com/dines/distributed-kv/store"
)

// KVServer is the client-facing server that routes commands through Raft.
type KVServer struct {
	node  *raft.RaftNode
	store *store.KVStateMachine

	// Pending commands: index -> channel to send result back
	pendingMu sync.Mutex
	pending   map[int]chan string
}

// NewKVServer creates a new client-facing server.
func NewKVServer(node *raft.RaftNode) *KVServer {
	return &KVServer{
		node:    node,
		store:   store.NewKVStateMachine(),
		pending: make(map[int]chan string),
	}
}

// Start begins the apply loop that processes committed entries.
func (s *KVServer) Start() {
	go s.applyLoop()
}

// applyLoop reads committed entries from Raft, applies them to the KV store,
// and notifies any waiting SubmitCommand calls.
func (s *KVServer) applyLoop() {
	for msg := range s.node.ApplyCh() {
		result := s.store.Apply(msg.Command)

		s.pendingMu.Lock()
		ch, ok := s.pending[msg.Index]
		if ok {
			delete(s.pending, msg.Index)
		}
		s.pendingMu.Unlock()

		if ok {
			ch <- result
		}
	}
}

// SubmitCommand sends a command through Raft and waits for it to be committed.
func (s *KVServer) SubmitCommand(cmd raft.Command, reply *raft.ClientResponse) error {
	if !s.node.IsLeader() {
		reply.Success = false
		reply.Error = "not the leader — connect to the leader node"
		return nil
	}

	// For GET operations, go through Raft for linearizability
	entry := s.node.Propose(cmd)
	if entry == nil {
		reply.Success = false
		reply.Error = "failed to propose — not the leader"
		return nil
	}

	// Wait for the entry to be committed and applied
	resultCh := make(chan string, 1)
	s.pendingMu.Lock()
	s.pending[entry.Index] = resultCh
	s.pendingMu.Unlock()

	// Wait for result (with timeout handled by RPC layer)
	result := <-resultCh

	reply.Success = true
	reply.Value = result
	return nil
}

// StartClientServer starts the client-facing RPC server.
func (s *KVServer) StartClientServer(address string) error {
	err := rpc.Register(s)
	if err != nil {
		return fmt.Errorf("failed to register client RPC: %v", err)
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", address, err)
	}

	log.Printf("Client server listening on %s", address)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Client accept error: %v", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	return nil
}
