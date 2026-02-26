# APEX-KV Architecture

## Overview

APEX-KV is a production-grade distributed key-value store built from scratch in C++20. It provides strong consistency via the Raft consensus algorithm, sub-millisecond GET latencies, and horizontal scalability through consistent hashing.

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │                          CLIENT LAYER                                │
 │   Go SDK · Python SDK · Rust SDK · JS SDK · Interactive REPL        │
 └─────────────────────────────┬────────────────────────────────────────┘
                               │  TCP (binary protocol, port 7001–700N)
 ┌─────────────────────────────▼────────────────────────────────────────┐
 │                         NETWORK LAYER                                │
 │   Single-threaded epoll event loop (edge-triggered, ET)              │
 │   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
 │   │   Client conns   │  │   Peer conns     │  │   eventfd wakeup │  │
 │   │  (accept4, ET)   │  │  (outbound TCP)  │  │  (Raft/Gossip)   │  │
 │   └──────────────────┘  └──────────────────┘  └──────────────────┘  │
 └──────────┬──────────────────────┬────────────────────────────────────┘
            │                      │ MpscQueue (lock-free)
 ┌──────────▼──────────┐  ┌────────▼────────────────────────────────────┐
 │    KV STORE LAYER   │  │           RAFT CONSENSUS ENGINE              │
 │                     │  │                                              │
 │  64 shards, each:   │  │  Leader election · Log replication           │
 │  ┌───────────────┐  │  │  Majority commit · WAL persistence           │
 │  │ RobinHoodMap  │  │  │                                              │
 │  │  (lock-free   │  │  │  ┌─────────────┐  ┌──────────────────────┐  │
 │  │   reads)      │  │  │  │  Raft Log   │  │  Write-Ahead Log     │  │
 │  └───────────────┘  │  │  │  (in-mem)   │  │  (fdatasync/batch)   │  │
 │                     │  │  └─────────────┘  └──────────────────────┘  │
 │  SlabAllocator      │  │                                              │
 │  (mmap-backed,      │  │  Group-commit: N entries → 1 fdatasync      │
 │   MADV_HUGEPAGE)    │  │  Durability: unsafe / durable / strict      │
 └─────────────────────┘  └──────────────────────────────────────────────┘
                                      │ UDP (port 8001–800N)
 ┌────────────────────────────────────▼────────────────────────────────┐
 │                      SWIM GOSSIP PROTOCOL                           │
 │   Membership detection · Failure detection · Incarnation numbers    │
 │   Alive → Suspected (1s silence) → Dead (5s silence)               │
 └─────────────────────────────────────────────────────────────────────┘
                                      │
 ┌────────────────────────────────────▼────────────────────────────────┐
 │                    CONSISTENT HASHING RING                          │
 │   150 virtual nodes per physical node · wyhash routing             │
 │   Replica placement for replication factor N                        │
 └─────────────────────────────────────────────────────────────────────┘
```

## Component Details

### Network Layer (§17)

The network thread is the single owner of all file descriptors. No other thread may read or write sockets. This eliminates the fd-race present in many naive implementations.

- `epoll(EPOLLET)` — edge-triggered for zero wasted wakeups
- `accept4(SOCK_NONBLOCK)` — non-blocking accept in the event loop
- `eventfd` — Raft/Gossip post messages via a lock-free MPSC queue; the network thread drains and sends
- `TCP_NODELAY` on all sockets — eliminates Nagle buffering

### Raft Consensus (§15)

Full Raft implementation with:

| Property | Value |
|---|---|
| Heartbeat interval | 50 ms |
| Election timeout | 150–300 ms (randomized) |
| Log persistence | WAL with CRC32 + fdatasync |
| Commit rule | Majority quorum (⌊N/2⌋+1) |
| Peer identification | `sender_id` in every RPC header |

Key correctness properties:
- Followers persist entries to WAL **before** ACKing AppendEntries
- Leader advances commit index only after quorum confirmation
- AppendEntries responses credit the correct peer by `node_id`

### Write-Ahead Log — Group-Commit (§9)

```
Writer thread 1 ──┐
Writer thread 2 ──┤→ pending queue → flush thread: writev(N entries) → fdatasync → signal
Writer thread 3 ──┘
```

- Writers push serialized records and block on a `std::future`
- Flush thread batches up to `max_batch_size` entries per `fdatasync`
- One `fdatasync` per batch instead of one per entry → **10–50× throughput improvement** under write-heavy workloads

**Durability modes:**

| Mode | fsync | Behavior |
|---|---|---|
| `unsafe` | Never | Fastest, loses data on crash |
| `durable` | Per batch (default) | Correct Raft semantics |
| `strict` | Per follower append | Strongest guarantee |

### KV Store (§8)

Robin Hood open-addressing hash map:
- 32-byte slots for cache-friendly reads (2 per cache line)
- Backward-shift deletion (no tombstones)
- PSL-bounded probe sequences via Robin Hood displacement
- 64 shards, each with an independent mutex

### SWIM Gossip (§16)

- Fanout = 3 random members per gossip round
- Piggybacked membership updates for O(log N) dissemination
- Incarnation numbers for refuting false-dead claims

### Consistent Hashing Ring (§14)

- 150 virtual nodes per physical node (wyhash)
- `O(log N)` lookup via `std::lower_bound`
- Supports arbitrary replication factor N

## Performance Characteristics

All numbers are approximate single-machine benchmarks with 3 nodes, 8 client threads.

| Metric | Typical | Notes |
|---|---|---|
| GET p50 | ~100 µs | Local read path, shard lock |
| GET p99 | ~500 µs | Under contention |
| PUT p50 | ~1 ms | Leader → WAL → quorum |
| PUT p99 | ~5 ms | Group-commit batch latency |
| Throughput (GET) | ~500K ops/s | 8 threads, 100 key hot set |
| Throughput (PUT) | ~100K ops/s | Group-commit enabled |

## WAL Record Format

```
┌──────────────────────────────────────────────────────┐
│  MAGIC   │ DATA_LEN │ CRC32    │       DATA           │
│  4 bytes │ 4 bytes  │ 4 bytes  │   data_len bytes     │
│ 0xAEC0FFEE                                            │
├──────────────────────────────────────────────────────┤
│ DATA:                                                 │
│  term:8   index:8   op:1   key_len:4   key   val_len:4   val
└──────────────────────────────────────────────────────┘
```

## Wire Protocol

All messages use a 16-byte fixed header:

```
┌────────────────────────────────────────────────────────────┐
│ magic:4 (0xA9ECBD10) │ cmd:1 │ flags:1 │ sender_id:2      │
├────────────────────────────────────────────────────────────┤
│ payload_len:4 (big-endian)   │ seq:4 (big-endian)          │
└────────────────────────────────────────────────────────────┘
```

The `sender_id` field fixes the v1 bug where Raft couldn't identify which peer responded to an AppendEntries — it was guessing by fd, which was wrong under reconnection.

## Failure Scenarios

| Scenario | Behavior |
|---|---|
| Leader crash | Followers detect via election timeout (150–300 ms); new election |
| Follower crash | Leader reduces quorum count; cluster continues if majority alive |
| Network partition | Minority partition cannot commit (safety preserved) |
| WAL corruption | CRC mismatch detected at replay; node refuses to start |
| Slow follower | Leader retransmits from `next_index`; follower catches up |
