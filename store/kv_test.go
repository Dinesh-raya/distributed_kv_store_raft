package store

import (
	"testing"

	"github.com/dines/distributed-kv/raft"
)

func TestNewKVStateMachine(t *testing.T) {
	kv := NewKVStateMachine()
	if kv == nil {
		t.Fatal("NewKVStateMachine returned nil")
	}
}

func TestApplySET(t *testing.T) {
	kv := NewKVStateMachine()
	cmd := raft.Command{Op: "SET", Key: "name", Value: "dines"}
	result := kv.Apply(cmd)
	if result != "OK" {
		t.Errorf("expected OK, got %s", result)
	}
}

func TestApplyGET(t *testing.T) {
	kv := NewKVStateMachine()
	kv.Apply(raft.Command{Op: "SET", Key: "name", Value: "dines"})

	result := kv.Apply(raft.Command{Op: "GET", Key: "name"})
	if result != "dines" {
		t.Errorf("expected dines, got %s", result)
	}
}

func TestApplyGETMissing(t *testing.T) {
	kv := NewKVStateMachine()
	result := kv.Apply(raft.Command{Op: "GET", Key: "nonexistent"})
	if result != "" {
		t.Errorf("expected empty string, got %s", result)
	}
}

func TestApplyDELETE(t *testing.T) {
	kv := NewKVStateMachine()
	kv.Apply(raft.Command{Op: "SET", Key: "name", Value: "dines"})
	result := kv.Apply(raft.Command{Op: "DELETE", Key: "name"})
	if result != "OK" {
		t.Errorf("expected OK, got %s", result)
	}

	// Verify it's gone
	result = kv.Apply(raft.Command{Op: "GET", Key: "name"})
	if result != "" {
		t.Errorf("expected empty after delete, got %s", result)
	}
}

func TestApplyDELETEMissing(t *testing.T) {
	kv := NewKVStateMachine()
	result := kv.Apply(raft.Command{Op: "DELETE", Key: "nonexistent"})
	if result != "OK" {
		t.Errorf("expected OK even for missing key, got %s", result)
	}
}
