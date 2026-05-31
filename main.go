package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"

	"github.com/dines/distributed-kv/config"
	"github.com/dines/distributed-kv/raft"
	"github.com/dines/distributed-kv/server"
)

func main() {
	nodeID := flag.Int("id", 0, "Node ID (0, 1, or 2)")
	flag.Parse()

	cfg := config.DefaultCluster()

	nodeConfig := cfg.GetNode(*nodeID)
	if nodeConfig == nil {
		log.Fatalf("Invalid node ID: %d", *nodeID)
	}

	// Get peer IDs
	var peerIDs []int
	for _, node := range cfg.Nodes {
		if node.ID != *nodeID {
			peerIDs = append(peerIDs, node.ID)
		}
	}

	// Create Raft node
	node := raft.NewNode(*nodeID, peerIDs)

	// Start Raft RPC server
	raftAddr := nodeConfig.Address
	err := node.StartRPCServer(raftAddr)
	if err != nil {
		log.Fatalf("Failed to start Raft RPC: %v", err)
	}

	// Wire up real RPC send functions
	node.SetSendRequestVote(func(peerID int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) {
		peerCfg := cfg.GetNode(peerID)
		if peerCfg == nil {
			return
		}
		client, err := rpc.Dial("tcp", peerCfg.Address)
		if err != nil {
			return
		}
		defer client.Close()
		client.Call("RaftRPC.RequestVote", args, reply)
	})

	node.SetSendAppendEntries(func(peerID int, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) {
		peerCfg := cfg.GetNode(peerID)
		if peerCfg == nil {
			return
		}
		client, err := rpc.Dial("tcp", peerCfg.Address)
		if err != nil {
			return
		}
		defer client.Close()
		client.Call("RaftRPC.AppendEntries", args, reply)
	})

	// Wire up real RPC send functions
	node.SetSendRequestVote(func(peerID int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) {
		peerCfg := cfg.GetNode(peerID)
		if peerCfg == nil {
			return
		}
		client, err := rpc.Dial("tcp", peerCfg.Address)
		if err != nil {
			return
		}
		defer client.Close()
		client.Call("RaftRPC.RequestVote", args, reply)
	})

	node.SetSendAppendEntries(func(peerID int, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) {
		peerCfg := cfg.GetNode(peerID)
		if peerCfg == nil {
			return
		}
		client, err := rpc.Dial("tcp", peerCfg.Address)
		if err != nil {
			return
		}
		defer client.Close()
		client.Call("RaftRPC.AppendEntries", args, reply)
	})

	// Start Raft node (election loop)
	node.Start()

	// Create and start client server
	kvServer := server.NewKVServer(node)
	kvServer.Start() // Start the apply loop
	clientAddr := fmt.Sprintf("localhost:%d", 9001+*nodeID)
	err = kvServer.StartClientServer(clientAddr)
	if err != nil {
		log.Fatalf("Failed to start client server: %v", err)
	}

	fmt.Printf("Node %d started\n", *nodeID)
	fmt.Printf("  Raft:   %s\n", raftAddr)
	fmt.Printf("  Client: %s\n", clientAddr)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Printf("\nShutting down node %d...\n", *nodeID)
	node.Stop()
}
