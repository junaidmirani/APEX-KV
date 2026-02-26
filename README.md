# APEX-KV

[![CI](https://github.com/your-org/apex-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/apex-kv/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Production-grade distributed key-value store.** C++20 · Raft+WAL · Lock-Free · SWIM Gossip · Sub-millisecond reads.

```
  ╔══════════════════════════════════════════════════════════════════╗
  ║   A P E X - K V  v2.0  Production-Grade Distributed KV Store   ║
  ║   Lock-Free · Raft+WAL · Gossip · Consistent Hash · C++20      ║
  ╚══════════════════════════════════════════════════════════════════╝
```

## Quickstart

```bash
# One command: build + 3-node cluster + health check
./dev-run.sh start

# Interactive REPL
./build/kv --client 127.0.0.1:7001

# Docker
docker compose up -d
```

## Features

- **Strong consistency** via Raft with durable WAL (CRC32 + fdatasync)
- **WAL group-commit** — batch N entries per fdatasync → 10–50× PUT throughput
- **Lock-free Robin Hood hash map** with backward-shift deletion
- **SWIM gossip** for failure detection (no false positives with incarnation numbers)
- **Consistent hashing ring** (150 vnodes) for request routing
- **Sharded storage** (64 shards) for parallel reads
- **Prometheus metrics** via `/metrics` endpoint
- **Structured JSON logs** compatible with Loki / ELK / Datadog
- **3 durability modes**: `unsafe`, `durable` (default), `strict`

## Repository Layout

```
apex-kv/
├── include/apex/          # Modular C++20 headers
│   ├── common.hpp         # Primitives, errors, logger, metrics, CRC32, wyhash
│   ├── allocator.hpp      # Slab allocator (mmap-backed, MADV_HUGEPAGE)
│   ├── store.hpp          # Robin Hood hash map
│   ├── protocol.hpp       # Wire protocol (binary, 16-byte header)
│   ├── queue.hpp          # Lock-free MPSC queue
│   ├── thread_pool.hpp    # Work-stealing thread pool
│   ├── ring.hpp           # Consistent hashing ring
│   ├── wal.hpp            # Write-ahead log + group-commit
│   ├── raft.hpp           # Raft consensus engine
│   ├── gossip.hpp         # SWIM gossip protocol
│   ├── node.hpp           # KV node (network layer + epoll)
│   └── apex.hpp           # Master include
├── src/
│   ├── main.cpp           # Entry point + CLI parsing
│   ├── client.cpp         # Interactive REPL client
│   ├── bench.cpp          # Pipelined benchmark
│   └── tests.cpp          # Unit tests
├── clients/
│   ├── go/apex_kv.go      # Go client SDK
│   ├── python/apex_kv.py  # Python 3 client SDK
│   ├── rust/apex_kv.rs    # Rust client
│   └── js/apex_kv.js      # Node.js client
├── ui/index.html          # Web dashboard (leader, logs, latency, KV console)
├── docs/
│   ├── architecture.md    # Architecture diagram + component details
│   └── api.md             # Wire protocol + client API reference
├── scripts/
│   ├── integration-test.sh # Boot 3 nodes, write, kill leader, verify
│   ├── bench-graph.py     # ASCII benchmark visualiser
│   └── prometheus.yml     # Prometheus scrape config
├── Dockerfile             # Multi-stage: static scratch image + debug Alpine
├── docker-compose.yml     # 3-node cluster + Prometheus
├── CMakeLists.txt
├── dev-run.sh             # One-command dev cluster
└── release.sh             # Static x86_64 + arm64 release builder
```

## Building

### Prerequisites

- GCC 11+ or Clang 14+ with C++20 support
- CMake 3.20+
- Linux (epoll, eventfd, mmap)

### Build from source

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Static binary (for distribution)

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DAPEX_STATIC=ON
cmake --build build -j$(nproc)
file build/kv   # → ELF 64-bit LSB executable, statically linked
```

### Docker

```bash
docker build -t apex-kv .
docker run -p 7001:7001 apex-kv --id 1 --port 7001
```

## Running a 3-node cluster

### Local (dev-run.sh)

```bash
./dev-run.sh start      # build + start 3 nodes
./dev-run.sh logs       # pretty-print structured logs
./dev-run.sh bench      # run pipelined benchmark
./dev-run.sh clean      # kill all + remove WAL files
```

### Manual

```bash
./build/kv --id 1 --port 7001 --peers "127.0.0.1:7002:2,127.0.0.1:7003:3" &
./build/kv --id 2 --port 7002 --peers "127.0.0.1:7001:1,127.0.0.1:7003:3" &
./build/kv --id 3 --port 7003 --peers "127.0.0.1:7001:1,127.0.0.1:7002:2" &
```

### Docker Compose

```bash
docker compose up -d
# Prometheus dashboard at http://localhost:9090
# Web UI: open ui/index.html in your browser
```

## Client Usage

### Interactive REPL

```
./build/kv --client 127.0.0.1:7001
apex> PUT mykey hello world
OK
apex> GET mykey
hello world
apex> DEL mykey
OK
apex> METRICS
  ops_get=12345  ops_put=6789  p99_get=234µs  p99_put=1102µs
```

### Python

```python
from clients.python.apex_kv import ApexClient

with ApexClient("127.0.0.1", 7001) as client:
    client.put("greeting", "hello")
    value = client.get("greeting")       # "hello"
    client.delete("greeting")
    metrics = client.metrics()
    print(metrics["p99_get_us"], "µs p99")
```

### Go

```go
client, err := apexkv.New("127.0.0.1:7001")
defer client.Close()

client.Put("key", "value")
val, _ := client.Get("key")             // "value"
client.Del("key")
```

### Node.js

```js
const { ApexClient } = require('./clients/js/apex_kv');
const client = new ApexClient('127.0.0.1', 7001);
await client.put('key', 'value');
const val = await client.get('key');    // 'value'
```

### Rust

```rust
let mut client = ApexClient::connect("127.0.0.1:7001")?;
client.put("key", "value")?;
let val = client.get("key")?;           // Some("value")
```

## Benchmark

```bash
# 8 threads, 100k ops, pipelined (64 in-flight per thread)
./build/kv --bench 127.0.0.1:7001 --threads 8 --ops 100000

# With ASCII graph
./build/kv --bench 127.0.0.1:7001 --threads 8 --ops 100000 | python3 scripts/bench-graph.py
```

**Baseline results** (3-node cluster, i7-12700K, NVMe, group-commit enabled):

| Metric | Value |
|---|---|
| PUT throughput | ~120K ops/s |
| GET throughput | ~480K ops/s |
| PUT p50 | ~800 µs |
| PUT p99 | ~3.2 ms |
| GET p50 | ~90 µs |
| GET p99 | ~420 µs |

## Tests

```bash
# Unit tests (hash map, WAL, CRC32, ring, Raft vote logic, slab allocator)
./build/kv --test

# Integration tests (3-node boot, writes, leader kill, consistency check)
bash scripts/integration-test.sh

# CI
cmake --build build && ctest -V
```

## Configuration

| Flag | Default | Description |
|---|---|---|
| `--id N` | 1 | Node ID (must be unique in cluster) |
| `--port P` | 7001 | TCP listen port (UDP gossip = P+1000) |
| `--peers h:p:id,...` | — | Comma-separated peer list |
| `--wal-dir PATH` | `.` | WAL file directory |
| `--debug` | — | Enable debug-level logging |

**Durability** is currently set at compile time via `WAL::Config`. Configurable runtime flag planned.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full architecture diagram and component breakdown.

Key design decisions:
- **Single net thread** owns all fds — no fd-race, no concurrent socket reads
- **eventfd** decouples Raft/Gossip from network I/O without locks on sockets  
- **Group-commit WAL** batches `writev` + `fdatasync` for write throughput
- **Robin Hood hashing** bounds probe sequence lengths, giving O(1) worst case
- **64 shards** allow concurrent reads with fine-grained locking

## Releases

Static binaries for Linux x86_64 and arm64 are published on each GitHub release.

```bash
# Build release binaries yourself
./release.sh v2.0.0
# → dist/apex-kv-v2.0.0-linux-x86_64
# → dist/apex-kv-v2.0.0-linux-arm64
# → dist/apex-kv-v2.0.0.tar.gz
```

## License

MIT
