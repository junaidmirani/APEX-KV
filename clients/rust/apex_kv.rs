// apex-kv Rust client
//
// Cargo.toml:
//   [dependencies]
//   apex-kv = { path = "." }
//   tokio = { version = "1", features = ["full"] }
//
// Example:
//   let mut client = ApexClient::connect("127.0.0.1:7001").await?;
//   client.put("hello", "world").await?;
//   let val = client.get("hello").await?;

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

const MAGIC: u32 = 0xA9ECBD10;
const HDR_SIZE: usize = 16;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum Cmd {
    Get         = 0x01,
    Put         = 0x02,
    Del         = 0x03,
    Ping        = 0x04,
    Metrics     = 0x05,
    Ok          = 0x10,
    Value       = 0x11,
    NotFound    = 0x12,
    Error       = 0x13,
    Redirect    = 0x14,
    Pong        = 0x15,
    MetricsResp = 0x16,
    Unknown     = 0xFF,
}

impl From<u8> for Cmd {
    fn from(b: u8) -> Self {
        match b {
            0x01 => Cmd::Get,  0x02 => Cmd::Put, 0x03 => Cmd::Del,
            0x04 => Cmd::Ping, 0x05 => Cmd::Metrics,
            0x10 => Cmd::Ok,   0x11 => Cmd::Value, 0x12 => Cmd::NotFound,
            0x13 => Cmd::Error,0x14 => Cmd::Redirect, 0x15 => Cmd::Pong,
            0x16 => Cmd::MetricsResp,
            _ => Cmd::Unknown,
        }
    }
}

#[derive(Debug)]
pub enum ApexError {
    Io(io::Error),
    NotFound,
    NotLeader { leader: String },
    Server(String),
    Protocol(String),
}

impl std::fmt::Display for ApexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApexError::Io(e)               => write!(f, "io: {e}"),
            ApexError::NotFound            => write!(f, "key not found"),
            ApexError::NotLeader { leader} => write!(f, "not leader, redirect to {leader}"),
            ApexError::Server(s)           => write!(f, "server: {s}"),
            ApexError::Protocol(s)         => write!(f, "protocol: {s}"),
        }
    }
}

impl From<io::Error> for ApexError {
    fn from(e: io::Error) -> Self { ApexError::Io(e) }
}

pub type Result<T> = std::result::Result<T, ApexError>;

pub struct Metrics {
    pub ops_get:    u64,
    pub ops_put:    u64,
    pub p99_get_us: u64,
    pub p99_put_us: u64,
}

/// Synchronous APEX-KV client.
///
/// All methods are blocking. For async, wrap in `tokio::task::spawn_blocking`.
pub struct ApexClient {
    stream: TcpStream,
    seq: u32,
    addr: String,
    auto_redirect: bool,
}

impl ApexClient {
    /// Connect to an APEX-KV node.
    pub fn connect(addr: impl ToSocketAddrs + std::fmt::Display + Clone) -> Result<Self> {
        let stream = TcpStream::connect(addr.clone())?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            seq: 0,
            addr: addr.to_string(),
            auto_redirect: true,
        })
    }

    /// Ping the node.
    pub fn ping(&mut self) -> Result<bool> {
        let (cmd, _) = self.rpc(Cmd::Ping, &[], false)?;
        Ok(cmd == Cmd::Pong)
    }

    /// Get a value. Returns `None` if not found.
    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let payload = encode_str(key);
        let (cmd, resp) = self.rpc(Cmd::Get, &payload, false)?;
        match cmd {
            Cmd::Value    => Ok(Some(decode_str(&resp)?)),
            Cmd::NotFound => Ok(None),
            Cmd::Error    => Err(ApexError::Server(decode_str(&resp).unwrap_or_default())),
            _             => Err(ApexError::Protocol(format!("unexpected cmd {:?}", cmd))),
        }
    }

    /// Get a value with a default fallback.
    pub fn get_or_default(&mut self, key: &str, default: &str) -> Result<String> {
        Ok(self.get(key)?.unwrap_or_else(|| default.to_owned()))
    }

    /// Write a key-value pair. Follows leader redirects automatically.
    pub fn put(&mut self, key: &str, value: &str) -> Result<()> {
        let mut payload = encode_str(key);
        payload.extend(encode_str(value));
        let (cmd, resp) = self.rpc(Cmd::Put, &payload, true)?;
        if cmd == Cmd::Ok { return Ok(()); }
        Err(ApexError::Server(decode_str(&resp).unwrap_or_default()))
    }

    /// Delete a key. Returns true if key existed.
    pub fn del(&mut self, key: &str) -> Result<bool> {
        let payload = encode_str(key);
        let (cmd, _) = self.rpc(Cmd::Del, &payload, true)?;
        Ok(cmd == Cmd::Ok)
    }

    /// Fetch server metrics.
    pub fn metrics(&mut self) -> Result<Metrics> {
        let (cmd, resp) = self.rpc(Cmd::Metrics, &[], false)?;
        if cmd != Cmd::MetricsResp || resp.len() < 32 {
            return Err(ApexError::Protocol("bad metrics response".into()));
        }
        Ok(Metrics {
            ops_get:    u64::from_be_bytes(resp[0..8].try_into().unwrap()),
            ops_put:    u64::from_be_bytes(resp[8..16].try_into().unwrap()),
            p99_get_us: u64::from_be_bytes(resp[16..24].try_into().unwrap()),
            p99_put_us: u64::from_be_bytes(resp[24..32].try_into().unwrap()),
        })
    }

    // ── Internal ───────────────────────────────────────────────────────────────

    fn rpc(&mut self, cmd: Cmd, payload: &[u8], follow_redirect: bool) -> Result<(Cmd, Vec<u8>)> {
        self.seq = self.seq.wrapping_add(1);
        let hdr = build_header(cmd, payload.len() as u32, self.seq);
        self.stream.write_all(&hdr)?;
        self.stream.write_all(payload)?;

        let (rcmd, resp) = read_response(&mut self.stream)?;

        if rcmd == Cmd::Redirect && follow_redirect && self.auto_redirect {
            if let Ok(leader) = decode_str(&resp) {
                if !leader.is_empty() && leader != "unknown" {
                    self.addr = leader;
                    self.stream = TcpStream::connect(&self.addr)?;
                    self.stream.set_nodelay(true)?;
                    return self.rpc(cmd, payload, false);
                }
            }
        }

        Ok((rcmd, resp))
    }
}

fn build_header(cmd: Cmd, payload_len: u32, seq: u32) -> [u8; HDR_SIZE] {
    let mut hdr = [0u8; HDR_SIZE];
    hdr[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    hdr[4] = cmd as u8;
    hdr[8..12].copy_from_slice(&payload_len.to_be_bytes());
    hdr[12..16].copy_from_slice(&seq.to_be_bytes());
    hdr
}

fn read_response(stream: &mut TcpStream) -> Result<(Cmd, Vec<u8>)> {
    let mut hdr = [0u8; HDR_SIZE];
    stream.read_exact(&mut hdr)?;
    let magic = u32::from_be_bytes(hdr[0..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(ApexError::Protocol(format!("bad magic 0x{magic:08x}")));
    }
    let cmd  = Cmd::from(hdr[4]);
    let plen = u32::from_be_bytes(hdr[8..12].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; plen];
    if plen > 0 {
        stream.read_exact(&mut payload)?;
    }
    Ok((cmd, payload))
}

fn encode_str(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = (b.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(b);
    out
}

fn decode_str(payload: &[u8]) -> Result<String> {
    if payload.len() < 4 {
        return Ok(String::new());
    }
    let n = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
    if payload.len() < 4 + n {
        return Err(ApexError::Protocol("string truncated".into()));
    }
    String::from_utf8(payload[4..4+n].to_vec())
        .map_err(|_| ApexError::Protocol("invalid utf8".into()))
}

// ── Example main ──────────────────────────────────────────────────────────────
fn main() -> Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:7001".into());
    let mut client = ApexClient::connect(&addr)?;
    println!("Connected to {addr}  ping={}", client.ping()?);

    client.put("rust-key", "rust-value")?;
    match client.get("rust-key")? {
        Some(v) => println!("get rust-key = {v}"),
        None    => println!("not found"),
    }
    client.del("rust-key")?;

    let m = client.metrics()?;
    println!("metrics: ops_get={} ops_put={} p99_get={}µs",
             m.ops_get, m.ops_put, m.p99_get_us);
    Ok(())
}
