// Package apexkv provides a Go client for the APEX-KV distributed key-value store.
//
// Usage:
//
//	client, err := apexkv.New("127.0.0.1:7001")
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
//	if err := client.Put("hello", "world"); err != nil { log.Fatal(err) }
//	val, err := client.Get("hello")   // "world", nil
//	err = client.Del("hello")
package apexkv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// Wire protocol constants (must match apex/protocol.hpp)
const (
	wireMagic   uint32 = 0xA9ECBD10
	hdrSize            = 16
)

type cmd uint8

const (
	cmdGET          cmd = 0x01
	cmdPUT          cmd = 0x02
	cmdDEL          cmd = 0x03
	cmdPING         cmd = 0x04
	cmdMETRICS      cmd = 0x05
	cmdOK           cmd = 0x10
	cmdVALUE        cmd = 0x11
	cmdNOTFOUND     cmd = 0x12
	cmdERROR        cmd = 0x13
	cmdREDIRECT     cmd = 0x14
	cmdPONG         cmd = 0x15
	cmdMETRICSRESP  cmd = 0x16
)

// Sentinel errors
var (
	ErrNotFound = errors.New("key not found")
	ErrNotLeader = errors.New("not leader")
)

// NotLeaderError carries the redirect address.
type NotLeaderError struct{ Addr string }
func (e *NotLeaderError) Error() string { return "not leader, redirect to " + e.Addr }

// Metrics holds server-side statistics returned by the Metrics() call.
type Metrics struct {
	OpsGet     uint64
	OpsPut     uint64
	P99GetUs   uint64
	P99PutUs   uint64
}

// Client is a thread-safe APEX-KV client.
type Client struct {
	mu   sync.Mutex
	conn net.Conn
	seq  atomic.Uint32
	addr string
}

// New creates a connected client. addr is "host:port".
func New(addr string) (*Client, error) {
	c := &Client{addr: addr}
	if err := c.reconnect(); err != nil {
		return nil, fmt.Errorf("apexkv: connect %s: %w", addr, err)
	}
	return c, nil
}

func (c *Client) reconnect() error {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	c.conn = conn
	return nil
}

// Close shuts down the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Ping returns true if the node is alive.
func (c *Client) Ping() (bool, error) {
	rc, _, err := c.rpc(cmdPING, nil, true)
	return rc == cmdPONG, err
}

// Get returns the value for key, or ErrNotFound if not present.
func (c *Client) Get(key string) (string, error) {
	payload := encodeStr(key)
	rc, resp, err := c.rpc(cmdGET, payload, false)
	if err != nil {
		return "", err
	}
	switch rc {
	case cmdVALUE:
		return decodeStr(resp), nil
	case cmdNOTFOUND:
		return "", ErrNotFound
	case cmdERROR:
		return "", fmt.Errorf("apexkv: server error: %s", decodeStr(resp))
	default:
		return "", fmt.Errorf("apexkv: unexpected response 0x%02x", rc)
	}
}

// Put writes key=value. Follows leader redirects automatically.
func (c *Client) Put(key, value string) error {
	payload := append(encodeStr(key), encodeStr(value)...)
	rc, resp, err := c.rpc(cmdPUT, payload, true)
	if err != nil {
		return err
	}
	if rc == cmdOK {
		return nil
	}
	return fmt.Errorf("apexkv: put failed (%02x): %s", rc, decodeStr(resp))
}

// Del deletes a key. Returns nil whether or not the key existed.
func (c *Client) Del(key string) error {
	payload := encodeStr(key)
	_, _, err := c.rpc(cmdDEL, payload, true)
	return err
}

// GetOrDefault returns the value or a default string if not found.
func (c *Client) GetOrDefault(key, def string) (string, error) {
	v, err := c.Get(key)
	if errors.Is(err, ErrNotFound) {
		return def, nil
	}
	return v, err
}

// Metrics fetches server-side statistics.
func (c *Client) Metrics() (Metrics, error) {
	rc, payload, err := c.rpc(cmdMETRICS, nil, false)
	if err != nil {
		return Metrics{}, err
	}
	if rc != cmdMETRICSRESP || len(payload) < 32 {
		return Metrics{}, fmt.Errorf("apexkv: bad metrics response")
	}
	return Metrics{
		OpsGet:   binary.BigEndian.Uint64(payload[0:]),
		OpsPut:   binary.BigEndian.Uint64(payload[8:]),
		P99GetUs: binary.BigEndian.Uint64(payload[16:]),
		P99PutUs: binary.BigEndian.Uint64(payload[24:]),
	}, nil
}

// rpc sends a command and returns (responseCmd, payload, error).
// If followRedirect is true and the server responds REDIRECT, the client
// reconnects to the new leader and retries (once).
func (c *Client) rpc(op cmd, payload []byte, followRedirect bool) (cmd, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	seq := c.seq.Add(1)
	hdr := buildHeader(op, uint32(len(payload)), seq)
	msg := append(hdr, payload...)

	if err := writeAll(c.conn, msg); err != nil {
		return 0, nil, fmt.Errorf("apexkv: send: %w", err)
	}

	rc, resp, err := readResponse(c.conn)
	if err != nil {
		return 0, nil, fmt.Errorf("apexkv: recv: %w", err)
	}

	if rc == cmdREDIRECT && followRedirect {
		leader := decodeStr(resp)
		if leader != "" && leader != "unknown" {
			c.addr = leader
			if err2 := c.reconnect(); err2 == nil {
				// Retry once on the new leader
				c.mu.Unlock()
				rc2, resp2, err3 := c.rpc(op, payload, false)
				c.mu.Lock()
				return rc2, resp2, err3
			}
		}
	}

	return rc, resp, nil
}

// ── Wire helpers ──────────────────────────────────────────────────────────────

func buildHeader(op cmd, payloadLen, seq uint32) []byte {
	hdr := make([]byte, hdrSize)
	binary.BigEndian.PutUint32(hdr[0:], wireMagic)
	hdr[4] = byte(op)
	hdr[5] = 0
	binary.BigEndian.PutUint16(hdr[6:], 0)
	binary.BigEndian.PutUint32(hdr[8:], payloadLen)
	binary.BigEndian.PutUint32(hdr[12:], seq)
	return hdr
}

func readResponse(conn net.Conn) (cmd, []byte, error) {
	hdr := make([]byte, hdrSize)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return 0, nil, err
	}
	magic := binary.BigEndian.Uint32(hdr[0:])
	if magic != wireMagic {
		return 0, nil, fmt.Errorf("bad magic 0x%08x", magic)
	}
	op      := cmd(hdr[4])
	plen    := binary.BigEndian.Uint32(hdr[8:])
	payload := make([]byte, plen)
	if plen > 0 {
		if _, err := io.ReadFull(conn, payload); err != nil {
			return 0, nil, err
		}
	}
	return op, payload, nil
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func encodeStr(s string) []byte {
	b := []byte(s)
	out := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(out, uint32(len(b)))
	copy(out[4:], b)
	return out
}

func decodeStr(payload []byte) string {
	if len(payload) < 4 {
		return ""
	}
	n := binary.BigEndian.Uint32(payload)
	if int(n) > len(payload)-4 {
		return ""
	}
	return string(payload[4 : 4+n])
}
