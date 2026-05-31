package server

import (
	"fmt"
	"log"
	"net"
	"net/rpc"

	"github.com/dines/distributed-kv/raft"
)

// KVServer is the client-facing server that routes commands through Raft.
type KVServer struct {
	node     *raft.RaftNode
	leaderID int
}

// NewKVServer creates a new client-facing server.
func NewKVServer(node *raft.RaftNode) *KVServer {
	return &KVServer{
		node:     node,
		leaderID: -1,
	}
}

// SubmitCommand sends a command through Raft and waits for it to be committed.
func (s *KVServer) SubmitCommand(cmd raft.Command, reply *raft.ClientResponse) error {
	reply.Success = false
	reply.Error = "not implemented yet"
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
