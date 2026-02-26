/**
 * apex-kv — Node.js client for APEX-KV distributed key-value store
 *
 * Works in Node.js 18+ (uses built-in `net` module).
 *
 * @example
 * const { ApexClient } = require('./apex_kv');
 * const client = new ApexClient('127.0.0.1', 7001);
 * await client.put('hello', 'world');
 * const val = await client.get('hello');   // 'world'
 * await client.del('hello');
 * client.close();
 */
'use strict';

const net = require('net');

// Wire protocol
const MAGIC       = 0xA9ECBD10;
const HDR_SIZE    = 16;
const Cmd = {
  GET:          0x01, PUT:    0x02, DEL:  0x03, PING: 0x04, METRICS: 0x05,
  OK:           0x10, VALUE:  0x11, NOT_FOUND: 0x12, ERROR: 0x13,
  REDIRECT:     0x14, PONG:   0x15, METRICS_RESP: 0x16,
};

class ApexError      extends Error {}
class ApexNotFound   extends ApexError { constructor() { super('Key not found'); } }
class ApexNotLeader  extends ApexError { constructor(addr) { super(`Not leader, redirect to ${addr}`); this.leaderAddr = addr; } }

class ApexClient {
  /**
   * @param {string} host
   * @param {number} port
   * @param {{ autoRedirect?: boolean, connectTimeout?: number }} [opts]
   */
  constructor(host, port, opts = {}) {
    this._host          = host;
    this._port          = port;
    this._autoRedirect  = opts.autoRedirect  ?? true;
    this._connectTimeout = opts.connectTimeout ?? 5000;
    this._seq           = 0;
    this._socket        = null;
    this._recvBuf       = Buffer.alloc(0);
    this._pendingReqs   = [];
    this._connected     = false;
    this._connecting    = null;
  }

  /** Connect (called automatically on first use). */
  async connect() {
    if (this._connected) return;
    if (this._connecting) return this._connecting;
    this._connecting = new Promise((resolve, reject) => {
      const s = net.createConnection({ host: this._host, port: this._port });
      s.setNoDelay(true);
      const timer = setTimeout(() => { s.destroy(); reject(new ApexError('Connect timeout')); }, this._connectTimeout);
      s.once('connect', () => { clearTimeout(timer); this._socket = s; this._connected = true; resolve(); });
      s.once('error', err => { clearTimeout(timer); reject(new ApexError(`Connect: ${err.message}`)); });
      s.on('data', chunk => this._onData(chunk));
      s.on('close', () => { this._connected = false; this._socket = null; this._rejectAll(new ApexError('Connection closed')); });
    });
    return this._connecting;
  }

  /** Close the connection. */
  close() {
    if (this._socket) { this._socket.destroy(); this._socket = null; this._connected = false; }
  }

  /** Ping the node. Returns true if alive. */
  async ping() {
    await this.connect();
    const [cmd] = await this._rpc(Cmd.PING);
    return cmd === Cmd.PONG;
  }

  /**
   * Get a value by key.
   * @returns {Promise<string|null>} value or null if not found
   */
  async get(key) {
    await this.connect();
    const payload = this._encStr(key);
    const [cmd, resp] = await this._rpc(Cmd.GET, payload);
    if (cmd === Cmd.VALUE)     return this._decStr(resp);
    if (cmd === Cmd.NOT_FOUND) return null;
    if (cmd === Cmd.ERROR)     throw new ApexError(this._decStr(resp));
    return null;
  }

  /**
   * Put a key-value pair.
   * Automatically follows leader redirects.
   */
  async put(key, value) {
    await this.connect();
    const payload = Buffer.concat([this._encStr(key), this._encStr(value)]);
    const [cmd, resp] = await this._rpc(Cmd.PUT, payload, /* followRedirect */ true);
    if (cmd !== Cmd.OK) throw new ApexError(`PUT failed (${cmd}): ${this._decStr(resp)}`);
  }

  /**
   * Delete a key.
   * @returns {Promise<boolean>} true if key existed
   */
  async del(key) {
    await this.connect();
    const payload = this._encStr(key);
    const [cmd] = await this._rpc(Cmd.DEL, payload, true);
    return cmd === Cmd.OK;
  }

  /** Get value or return a default. */
  async getOrDefault(key, def = '') {
    const v = await this.get(key);
    return v !== null ? v : def;
  }

  /** Fetch server metrics. */
  async metrics() {
    await this.connect();
    const [cmd, resp] = await this._rpc(Cmd.METRICS);
    if (cmd !== Cmd.METRICS_RESP || resp.length < 32) return {};
    return {
      opsGet:     resp.readBigUInt64BE(0),
      opsPut:     resp.readBigUInt64BE(8),
      p99GetUs:   resp.readBigUInt64BE(16),
      p99PutUs:   resp.readBigUInt64BE(24),
    };
  }

  /**
   * Simple polling watch — calls onChange when any of `keys` change.
   * @param {string[]} keys
   * @param {function} onChange
   * @param {{ intervalMs?: number }} [opts]
   * @returns {{ stop: function }}
   */
  watch(keys, onChange, opts = {}) {
    const interval = opts.intervalMs ?? 500;
    const snapshot = {};
    let running = true;
    const poll = async () => {
      for (const key of keys) {
        try {
          const val = await this.get(key);
          const prev = snapshot[key];
          if (val !== prev) {
            snapshot[key] = val;
            onChange({ key, value: val, deleted: val === null, prev });
          }
        } catch { /* ignore transient errors */ }
      }
      if (running) setTimeout(poll, interval);
    };
    poll();
    return { stop() { running = false; } };
  }

  // ── Internal ────────────────────────────────────────────────────────────────

  async _rpc(cmd, payload = Buffer.alloc(0), followRedirect = false) {
    const seq = ++this._seq;
    const hdr = Buffer.alloc(HDR_SIZE);
    hdr.writeUInt32BE(MAGIC, 0);
    hdr.writeUInt8(cmd, 4);
    hdr.writeUInt8(0, 5);
    hdr.writeUInt16BE(0, 6);
    hdr.writeUInt32BE(payload.length, 8);
    hdr.writeUInt32BE(seq, 12);

    const prom = new Promise((res, rej) => this._pendingReqs.push({ res, rej }));
    this._socket.write(Buffer.concat([hdr, payload]));
    const [rcmd, resp] = await prom;

    if (rcmd === Cmd.REDIRECT && followRedirect && this._autoRedirect) {
      const leaderAddr = this._decStr(resp);
      if (leaderAddr && leaderAddr !== 'unknown') {
        const parts = leaderAddr.split(':');
        this._host = parts[0];
        this._port = parseInt(parts[1], 10);
        this.close();
        this._connected = false;
        this._connecting = null;
        await this.connect();
        return this._rpc(cmd, payload, false);
      }
    }

    return [rcmd, resp];
  }

  _onData(chunk) {
    this._recvBuf = Buffer.concat([this._recvBuf, chunk]);
    while (this._recvBuf.length >= HDR_SIZE) {
      const magic = this._recvBuf.readUInt32BE(0);
      if (magic !== MAGIC) { this._recvBuf = Buffer.alloc(0); return; }
      const plen = this._recvBuf.readUInt32BE(8);
      if (this._recvBuf.length < HDR_SIZE + plen) break;
      const cmd     = this._recvBuf.readUInt8(4);
      const payload = this._recvBuf.slice(HDR_SIZE, HDR_SIZE + plen);
      this._recvBuf = this._recvBuf.slice(HDR_SIZE + plen);
      const pending = this._pendingReqs.shift();
      if (pending) pending.res([cmd, payload]);
    }
  }

  _rejectAll(err) {
    for (const p of this._pendingReqs) p.rej(err);
    this._pendingReqs = [];
  }

  _encStr(s) {
    const b = Buffer.from(s, 'utf8');
    const out = Buffer.alloc(4 + b.length);
    out.writeUInt32BE(b.length, 0);
    b.copy(out, 4);
    return out;
  }

  _decStr(buf) {
    if (!buf || buf.length < 4) return '';
    const n = buf.readUInt32BE(0);
    return buf.slice(4, 4 + n).toString('utf8');
  }
}

module.exports = { ApexClient, ApexError, ApexNotFound, ApexNotLeader, Cmd };

// ── Quick CLI ─────────────────────────────────────────────────────────────────
if (require.main === module) {
  (async () => {
    const [host = '127.0.0.1', portS = '7001'] = (process.argv[2] || '127.0.0.1:7001').split(':');
    const client = new ApexClient(host, parseInt(portS, 10));
    await client.connect();
    console.log('Connected.  Try: put key val  /  get key  /  del key  /  exit');
    const readline = require('readline').createInterface({ input: process.stdin, output: process.stdout, prompt: 'apex> ' });
    readline.prompt();
    readline.on('line', async line => {
      const [cmd, k, ...rest] = line.trim().split(' ');
      try {
        if (!cmd || cmd === 'exit') { client.close(); process.exit(0); }
        if (cmd === 'get')  console.log(await client.get(k) ?? '(nil)');
        if (cmd === 'put')  { await client.put(k, rest.join(' ')); console.log('OK'); }
        if (cmd === 'del')  console.log(await client.del(k) ? 'OK' : '(not found)');
        if (cmd === 'ping') console.log(await client.ping() ? 'PONG' : 'FAILED');
        if (cmd === 'metrics') console.log(JSON.stringify(await client.metrics(), (_, v) => typeof v === 'bigint' ? Number(v) : v, 2));
      } catch (e) { console.error('Error:', e.message); }
      readline.prompt();
    });
  })();
}
