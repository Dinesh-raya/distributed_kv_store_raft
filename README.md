# Distributed Key-Value Store with Raft Consensus

A distributed key-value store built from scratch in Go using the Raft consensus algorithm. This project demonstrates leader election, log replication, and fault tolerance — the same concepts behind production systems like etcd, CockroachDB, and Consul.

## Overview

This system maintains a consistent key-value store across multiple nodes. When you write `SET name "dines"` on any node, all nodes eventually have that value. If the leader crashes, a new leader is elected automatically within ~1 second.

**Key features:**
- Leader election with randomized timeouts
- Log replication from leader to followers
- Fault tolerance (survives minority node failures)
- Linearizable reads (all operations go through Raft)
- Interactive CLI client
- Zero external dependencies (Go standard library only)

## Architecture

```
Client (CLI)
    |
    v
Client Server (RPC)
    |
    v
Raft Node
    |
    ├── Leader Election (RequestVote RPC)
    ├── Log Replication (AppendEntries RPC)
    └── KV State Machine (SET/GET/DELETE)
```

The system is split into two clean layers:
- **Raft Layer** — handles consensus (leader election, log replication)
- **KV Layer** — applies committed entries to an in-memory map

## Project Structure

```
distributed-kv/
├── main.go                    # Entry point
├── cmd/kvctl/main.go          # CLI client
├── raft/
│   ├── types.go               # Shared types (State, LogEntry, RPCs)
│   ├── node.go                # RaftNode core struct
│   ├── election.go            # Leader election logic
│   ├── replication.go         # Log replication logic
│   └── rpc.go                 # RPC server
├── store/
│   └── kv.go                  # KV state machine
├── server/
│   └── server.go              # Client-facing handler
├── config/
│   └── config.go              # Cluster configuration
└── tests/
    ├── testutil.go            # In-process test cluster
    └── integration_test.go    # Integration tests
```

## How to Run

### Prerequisites
- Go 1.21+

### Build
```bash
go build -o distributed-kv .
go build -o kvctl ./cmd/kvctl/
```

### Start a 3-Node Cluster

Open three terminals:

```bash
# Terminal 1
./distributed-kv -id 0

# Terminal 2
./distributed-kv -id 1

# Terminal 3
./distributed-kv -id 2
```

### Use the CLI

```bash
# Terminal 4
./kvctl localhost:9001
```

```
> SET name dines
OK
> GET name
dines
> DELETE name
OK
> GET name
(nil)
```

## Testing

### Run All Tests
```bash
go test ./... -v -timeout 30s
```

### Run with Race Detector
```bash
CGO_ENABLED=1 go test ./... -race -timeout 60s
```

### Test Coverage

| Package | Tests | What's Tested |
|---------|-------|---------------|
| `raft/` | 19 | Election, replication, state transitions |
| `store/` | 6 | SET, GET, DELETE operations |
| `tests/` | 5 | Leader election, fault tolerance, replication |
| **Total** | **30** | **All passing** |

## How Raft Works

### Leader Election
1. Every node starts as a **Follower**
2. If no heartbeat received within 300-500ms (randomized), node becomes a **Candidate**
3. Candidate requests votes from peers
4. If majority votes received, candidate becomes **Leader**
5. One leader per term; higher term wins conflicts

### Log Replication
1. Client sends command to leader
2. Leader appends to its log, sends `AppendEntries` to followers
3. Once majority confirm, entry is **committed**
4. Committed entries applied to the KV state machine

### Fault Tolerance
- **Leader failure**: Followers detect missing heartbeats, trigger new election
- **Minority failure**: Cluster continues with majority
- **Network partition**: Minority side becomes unavailable (CP system)

## Design Decisions

| Decision | Choice | Why |
|----------|--------|-----|
| Algorithm | Raft over Paxos | Understandable, well-documented |
| Language | Go | Goroutines, built-in RPC, industry standard |
| Communication | `net/rpc` | Zero dependencies, simple |
| Storage | In-memory map | Focus on consensus, not storage |
| Reads | Through Raft | Linearizable consistency |
| Cluster config | Static | Avoids complex membership changes |

## What's Not Included (Out of Scope)

- Persistence (WAL, snapshots)
- Dynamic cluster membership
- Log compaction
- TLS/security
- HTTP API
- Monitoring dashboard

These are well-defined extensions that build on the core Raft implementation.

## Real-World Systems Using Raft

| System | Use Case |
|--------|----------|
| **etcd** | Kubernetes cluster state |
| **CockroachDB** | Distributed SQL database |
| **Consul** | Service discovery and configuration |
| **TiKV** | Distributed key-value database |

## Interview Talking Points

1. **"How does leader election work?"** — Randomized timeouts prevent split votes; RequestVote RPC with term numbers and log freshness checks
2. **"What happens when a leader crashes?"** — Followers detect missing heartbeats, trigger election, new leader elected within ~1 second
3. **"How do you ensure consistency?"** — All operations go through Raft; committed entries applied in order
4. **"How would you scale this?"** — Each range of keys gets its own Raft group (like CockroachDB)
5. **"What would you add for production?"** — Persistence (WAL), snapshots, TLS, monitoring

## Documentation

- [Design Spec](docs/superpowers/specs/2026-05-31-distributed-kv-store-design.md)
- [Analysis Report](docs/superpowers/specs/2026-05-31-distributed-kv-store-analysis.md)
- [Implementation Plan](docs/superpowers/plans/2026-05-31-distributed-kv-store.md)

## License

This is a portfolio project built for learning distributed systems concepts.
