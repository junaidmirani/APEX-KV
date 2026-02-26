# APEX-KV API Reference

## Wire Protocol

All communication uses a binary protocol with a fixed 16-byte header.

### Message Header

```
 0               1               2               3
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
├───────────────────────────────────────────────────────────────────┤
│                     magic = 0xA9ECBD10                           │
├───────────────────────────────────────────────────────────────────┤
│     cmd (1B)   │  flags (1B)   │      sender_id (2B, BE)         │
├───────────────────────────────────────────────────────────────────┤
│                   payload_len (4B, big-endian)                    │
├───────────────────────────────────────────────────────────────────┤
│                     seq (4B, big-endian)                          │
└───────────────────────────────────────────────────────────────────┘
```

- `magic`: Always `0xA9ECBD10`. Reject if mismatch.
- `cmd`: Command byte (see table below).
- `sender_id`: Node ID of the sender (used by Raft to identify peers).
- `payload_len`: Bytes following this header.
- `seq`: Client-assigned request sequence number; echoed in responses.

### Encoded Strings

Strings in payloads are length-prefixed:

```
[len: uint32 big-endian][bytes: len bytes of UTF-8]
```

### Command Table

| Cmd | Byte | Direction | Payload |
|-----|------|-----------|---------|
| `GET` | 0x01 | Client→Server | `str(key)` |
| `PUT` | 0x02 | Client→Server | `str(key) str(val)` |
| `DEL` | 0x03 | Client→Server | `str(key)` |
| `PING` | 0x04 | Client→Server | _(empty)_ |
| `METRICS` | 0x05 | Client→Server | _(empty)_ |
| `OK` | 0x10 | Server→Client | _(empty)_ |
| `VALUE` | 0x11 | Server→Client | `str(value)` |
| `NOT_FOUND` | 0x12 | Server→Client | _(empty)_ |
| `ERROR` | 0x13 | Server→Client | `str(message)` |
| `REDIRECT` | 0x14 | Server→Client | `str("host:port")` |
| `PONG` | 0x15 | Server→Client | _(empty)_ |
| `METRICS_RESP` | 0x16 | Server→Client | `u64 u64 u64 u64` |

### METRICS_RESP Payload

```
[ops_get: uint64 BE][ops_put: uint64 BE][p99_get_us: uint64 BE][p99_put_us: uint64 BE]
```

---

## CLI Reference

### Node mode

```
./kv --id <N> --port <P> [--peers h:p:id,...] [--wal-dir PATH] [--debug]
```

| Flag | Description |
|------|-------------|
| `--id N` | Unique node ID (integer, 1-based) |
| `--port P` | TCP listen port. Gossip UDP = P+1000 |
| `--peers h1:p1:id1,...` | Comma-separated peer specs |
| `--wal-dir PATH` | Directory for WAL files (one per node) |
| `--debug` | Enable DEBUG-level logging |

### Client mode

```
./kv --client <host:port>
```

Starts an interactive REPL. Commands:

| Command | Description |
|---------|-------------|
| `GET <key>` | Retrieve value |
| `PUT <key> <value>` | Write key-value pair |
| `DEL <key>` | Delete key |
| `PING` | Check node liveness |
| `METRICS` | Print server metrics |
| `QUIT` / `EXIT` | Exit |

### Benchmark mode

```
./kv --bench <host:port> [--threads N] [--ops N]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--threads N` | 4 | Number of concurrent client threads |
| `--ops N` | 50000 | Operations per thread |

Uses a pipeline depth of 64 in-flight requests per thread.

### Test mode

```
./kv --test
```

Runs the built-in test suite and exits with code 0 (all pass) or 1 (any fail).

Tests: wyhash, CRC32, SlabAllocator, RobinHoodMap, WAL, ConsistentRing, Raft vote logic.

---

## Prometheus Metrics

When the `/metrics` endpoint is added (or polled via the METRICS command), the following Prometheus metrics are available:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `apex_ops_total` | counter | `node`, `op` | Total ops by type (get/put/del) |
| `apex_raft_commits_total` | counter | `node` | Committed Raft log entries |
| `apex_raft_elections_total` | counter | `node` | Leader election events |
| `apex_latency_us` | gauge | `node`, `op`, `quantile` | Latency percentiles in µs |
| `apex_gossip_members` | gauge | `node`, `state` | Members by state (alive/suspect/dead) |

---

## Python SDK

```python
from apex_kv import ApexClient, ApexError, ApexNotFound

client = ApexClient("127.0.0.1", 7001, timeout=5.0, auto_redirect=True)

# Basic ops
value = client.get("key")            # str | None
client.put("key", "value")           # None (raises on error)
found = client.delete("key")         # bool
client.ping()                        # str: "PONG"
val = client.get_or_default("k", "") # str

# Metrics
m = client.metrics()
# {"ops_get": int, "ops_put": int, "p99_get_us": int, "p99_put_us": int}

# Context manager
with ApexClient("127.0.0.1", 7001) as c:
    c.put("k", "v")

client.close()
```

## Go SDK

```go
import apexkv "github.com/your-org/apex-kv/clients/go"

client, err := apexkv.New("127.0.0.1:7001")
// err: *net.OpError on connect failure

val, err := client.Get("key")
// val: string, err: apexkv.ErrNotFound if missing

err = client.Put("key", "value")
err = client.Del("key")

ok, err := client.Ping()   // bool

m, err := client.Metrics()
// m.OpsGet, m.OpsPut, m.P99GetUs, m.P99PutUs

client.Close()
```

## Node.js SDK

```js
const { ApexClient } = require('./apex_kv');

const client = new ApexClient('127.0.0.1', 7001);
await client.connect();

const val = await client.get('key');       // string | null
await client.put('key', 'value');          // void
const deleted = await client.del('key');   // boolean
const alive = await client.ping();         // boolean

const m = await client.metrics();
// { opsGet: BigInt, opsPut: BigInt, p99GetUs: BigInt, p99PutUs: BigInt }

// Polling watch
const watcher = client.watch(['key1', 'key2'], ({ key, value, prev }) => {
    console.log(`${key} changed: ${prev} → ${value}`);
}, { intervalMs: 500 });
watcher.stop();

client.close();
```

## Rust SDK

```rust
use apex_kv::ApexClient;

let mut client = ApexClient::connect("127.0.0.1:7001")?;

let val: Option<String> = client.get("key")?;
client.put("key", "value")?;
let deleted: bool = client.del("key")?;
let alive: bool = client.ping()?;

let val = client.get_or_default("key", "fallback")?;

let m = client.metrics()?;
println!("p99 GET = {} µs", m.p99_get_us);
```

---

## Error Handling

All clients return typed errors:

| Error | Meaning | Recovery |
|-------|---------|----------|
| Connection refused | Node is down | Try another node |
| `REDIRECT` | This node is not the leader | SDK auto-follows; or connect to provided address |
| `NOT_FOUND` | Key does not exist | Check key spelling |
| `ERROR` | Server-side error (e.g. WAL failure) | Retry with backoff |
| Timeout | Network congestion | Retry with backoff |

All SDKs follow leader redirects automatically by default (`auto_redirect=True`).
