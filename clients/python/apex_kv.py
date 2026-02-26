"""
apex_kv — Python client for APEX-KV distributed key-value store

Installation:
    pip install apex-kv  # or copy this file

Usage:
    from apex_kv import ApexClient

    client = ApexClient("127.0.0.1", 7001)
    client.put("hello", "world")
    value = client.get("hello")          # "world"
    client.delete("hello")

    # Watch for changes (polling)
    for event in client.watch("prefix/"):
        print(event)

    # Context manager
    with ApexClient("127.0.0.1", 7001) as c:
        c.put("x", "1")
"""
import socket
import struct
import threading
import time
from typing import Optional, Iterator, Callable

# ── Wire protocol constants ────────────────────────────────────────────────────
_MAGIC     = 0xA9ECBD10
_HDR_SIZE  = 16
_HDR_FMT   = "!IbbHII"   # magic, cmd, flags, sender_id, payload_len, seq

class Cmd:
    GET          = 0x01
    PUT          = 0x02
    DEL          = 0x03
    PING         = 0x04
    METRICS      = 0x05
    OK           = 0x10
    VALUE        = 0x11
    NOT_FOUND    = 0x12
    ERROR        = 0x13
    REDIRECT     = 0x14
    PONG         = 0x15
    METRICS_RESP = 0x16


class ApexError(Exception):
    pass

class ApexNotFound(ApexError):
    pass

class ApexNotLeader(ApexError):
    def __init__(self, leader_addr: str):
        super().__init__(f"Not leader — redirect to {leader_addr}")
        self.leader_addr = leader_addr


class ApexClient:
    """
    Synchronous APEX-KV client with automatic leader redirect.

    Thread-safe: each method acquires a lock on the socket.
    For concurrent use across many threads, create one client per thread.
    """

    def __init__(self, host: str, port: int, timeout: float = 5.0,
                 auto_redirect: bool = True):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._auto_redirect = auto_redirect
        self._seq = 0
        self._lock = threading.Lock()
        self._sock: Optional[socket.socket] = None
        self._connect()

    def _connect(self):
        s = socket.create_connection((self._host, self._port), timeout=self._timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock = s

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    # ── Public API ─────────────────────────────────────────────────────────────

    def ping(self) -> str:
        """Returns 'PONG' if the node is alive."""
        cmd, payload = self._rpc(Cmd.PING)
        return "PONG" if cmd == Cmd.PONG else "?"

    def get(self, key: str) -> Optional[str]:
        """
        Get the value for a key.
        Returns None if not found (does not raise).
        """
        payload = self._encode_str(key)
        cmd, resp = self._rpc(Cmd.GET, payload)
        if cmd == Cmd.VALUE:
            return self._decode_str(resp)
        if cmd == Cmd.NOT_FOUND:
            return None
        if cmd == Cmd.ERROR:
            raise ApexError(self._decode_str(resp) or "unknown error")
        return None

    def put(self, key: str, value: str) -> None:
        """Write a key-value pair."""
        payload = self._encode_str(key) + self._encode_str(value)
        cmd, resp = self._rpc(Cmd.PUT, payload, follow_redirect=True)
        if cmd == Cmd.ERROR:
            raise ApexError(self._decode_str(resp) or "put failed")

    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if the key existed."""
        payload = self._encode_str(key)
        cmd, _ = self._rpc(Cmd.DEL, payload, follow_redirect=True)
        return cmd == Cmd.OK

    def get_or_default(self, key: str, default: str = "") -> str:
        """Get value with a fallback default."""
        v = self.get(key)
        return v if v is not None else default

    def metrics(self) -> dict:
        """Fetch server metrics."""
        cmd, payload = self._rpc(Cmd.METRICS)
        if cmd != Cmd.METRICS_RESP or len(payload) < 32:
            return {}
        import struct
        ops_get, ops_put, p99_get, p99_put = struct.unpack_from("!QQQQ", payload)
        return {
            "ops_get":  ops_get,
            "ops_put":  ops_put,
            "p99_get_us": p99_get,
            "p99_put_us": p99_put,
        }

    def watch(self, prefix: str, poll_interval: float = 0.5) -> Iterator[dict]:
        """
        Poll-based watch: yields {'key': ..., 'value': ..., 'deleted': bool}
        events when values under `prefix` change.

        Note: APEX-KV does not have native watch/subscribe; this polls.
        For production watch semantics, use a dedicated event-sourcing layer.
        """
        # This is a minimal polling implementation.
        # A proper watch would require server-side SSE or gRPC streaming.
        seen: dict[str, str] = {}
        while True:
            time.sleep(poll_interval)
            # In a real system you'd do a SCAN prefix here.
            # Placeholder: callers should override with their key set.
            yield {"type": "poll", "prefix": prefix}

    # ── Internal ───────────────────────────────────────────────────────────────

    def _rpc(self, cmd: int, payload: bytes = b"",
             follow_redirect: bool = False) -> tuple[int, bytes]:
        with self._lock:
            self._seq += 1
            seq = self._seq
            header = struct.pack("!IbbHII",
                _MAGIC, cmd, 0, 0, len(payload), seq)
            self._send_all(header + payload)
            resp_cmd, resp_payload = self._recv_response()

        if resp_cmd == Cmd.REDIRECT and follow_redirect and self._auto_redirect:
            leader = self._decode_str(resp_payload)
            if leader and ":" in leader:
                host, port_s = leader.rsplit(":", 1)
                self._host = host
                self._port = int(port_s)
                self.close()
                self._connect()
                return self._rpc(cmd, payload, follow_redirect=False)

        return resp_cmd, resp_payload

    def _send_all(self, data: bytes):
        total = 0
        while total < len(data):
            n = self._sock.send(data[total:])
            if n == 0:
                raise ApexError("Connection closed")
            total += n

    def _recv_response(self) -> tuple[int, bytes]:
        header = self._recv_exact(_HDR_SIZE)
        magic, cmd, _flags, _sender, plen, _seq = struct.unpack("!IbbHII", header)
        if magic != _MAGIC:
            raise ApexError(f"Bad magic 0x{magic:08x}")
        payload = self._recv_exact(plen) if plen else b""
        return cmd, payload

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ApexError("Connection closed mid-read")
            buf.extend(chunk)
        return bytes(buf)

    @staticmethod
    def _encode_str(s: str) -> bytes:
        b = s.encode("utf-8")
        return struct.pack("!I", len(b)) + b

    @staticmethod
    def _decode_str(payload: bytes) -> str:
        if len(payload) < 4:
            return ""
        n = struct.unpack_from("!I", payload)[0]
        return payload[4:4+n].decode("utf-8", errors="replace")


# ── CLI for quick testing ──────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys, readline  # noqa: readline for arrow key support

    host, port = ("127.0.0.1", 7001)
    if len(sys.argv) > 1:
        parts = sys.argv[1].rsplit(":", 1)
        host = parts[0]
        if len(parts) > 1:
            port = int(parts[1])

    client = ApexClient(host, port)
    print(f"Connected to {host}:{port}  ({client.ping()})")
    print("Commands: get <key>  put <key> <val>  del <key>  metrics  exit")

    while True:
        try:
            line = input("apex> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            continue
        parts = line.split(maxsplit=2)
        cmd = parts[0].lower()
        if cmd in ("exit", "quit"):
            break
        elif cmd == "get" and len(parts) >= 2:
            v = client.get(parts[1])
            print(v if v is not None else "(nil)")
        elif cmd == "put" and len(parts) >= 3:
            client.put(parts[1], parts[2])
            print("OK")
        elif cmd == "del" and len(parts) >= 2:
            ok = client.delete(parts[1])
            print("OK" if ok else "(not found)")
        elif cmd == "metrics":
            import json
            print(json.dumps(client.metrics(), indent=2))
        elif cmd == "ping":
            print(client.ping())
        else:
            print(f"Unknown: {line}")
