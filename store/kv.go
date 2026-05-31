package store

import (
	"github.com/dines/distributed-kv/raft"
)

// KVStateMachine is an in-memory key-value store.
// It knows nothing about Raft -- it just applies commands.
type KVStateMachine struct {
	store map[string]string
}

// NewKVStateMachine creates a new empty KV store.
func NewKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		store: make(map[string]string),
	}
}

// Apply executes a command against the store and returns the result.
func (kv *KVStateMachine) Apply(command raft.Command) string {
	switch command.Op {
	case "SET":
		kv.store[command.Key] = command.Value
		return "OK"
	case "GET":
		return kv.store[command.Key]
	case "DELETE":
		delete(kv.store, command.Key)
		return "OK"
	default:
		return "ERROR: unknown operation"
	}
}
