#pragma once
#include "common.hpp"

enum class Cmd : uint8_t {
    // Client ops
    GET          = 0x01,
    PUT          = 0x02,
    DEL          = 0x03,
    PING         = 0x04,
    METRICS      = 0x05,
    // Responses
    OK           = 0x10,
    VALUE        = 0x11,
    NOT_FOUND    = 0x12,
    ERROR        = 0x13,
    REDIRECT     = 0x14,
    PONG         = 0x15,
    METRICS_RESP = 0x16,
    // Raft RPCs
    VOTE_REQ     = 0x20,
    VOTE_RESP    = 0x21,
    APPEND_REQ   = 0x22,
    APPEND_RESP  = 0x23,
    // Gossip
    GOSSIP_PING  = 0x30,
    GOSSIP_ACK   = 0x31,
};

struct __attribute__((packed)) MsgHeader {
    uint32_t magic       = 0xA9ECBD10u;
    uint8_t  cmd         = 0;
    uint8_t  flags       = 0;
    uint16_t sender_id   = 0;   // Node id of sender (network byte order)
    uint32_t payload_len = 0;   // Payload bytes after this header (network byte order)
    uint32_t seq         = 0;   // Request sequence number (network byte order)
};
static_assert(sizeof(MsgHeader) == 16);

static constexpr uint32_t WIRE_MAGIC = 0xA9ECBD10u;

// ── Serialization helpers ────────────────────────────────────────────────────
struct ByteWriter {
    std::vector<uint8_t> buf;

    void reserve(std::size_t n) { buf.reserve(buf.size() + n); }
    void u8 (uint8_t  v) { buf.push_back(v); }
    void u16(uint16_t v) { v = htons(v);   buf.insert(buf.end(),(uint8_t*)&v,(uint8_t*)&v+2); }
    void u32(uint32_t v) { v = htonl(v);   buf.insert(buf.end(),(uint8_t*)&v,(uint8_t*)&v+4); }
    void u64(uint64_t v) { v = htobe64(v); buf.insert(buf.end(),(uint8_t*)&v,(uint8_t*)&v+8); }
    void raw(const void* p, std::size_t n) {
        buf.insert(buf.end(), static_cast<const uint8_t*>(p),
                              static_cast<const uint8_t*>(p) + n);
    }
    void str(std::string_view s) { u32(s.size()); raw(s.data(), s.size()); }

    // Write a 16-byte header.  payload_len is patched by finalize().
    void header(Cmd c, uint32_t sender_node_id = 0, uint32_t seq = 0) {
        MsgHeader h{};
        h.magic     = htonl(WIRE_MAGIC);
        h.cmd       = static_cast<uint8_t>(c);
        h.sender_id = htons(static_cast<uint16_t>(sender_node_id));
        h.seq       = htonl(seq);
        raw(&h, sizeof h);
    }

    // Patch payload_len after the body has been written.
    void finalize() {
        if (buf.size() < sizeof(MsgHeader)) return;
        uint32_t plen = htonl(static_cast<uint32_t>(buf.size() - sizeof(MsgHeader)));
        std::memcpy(buf.data() + 8, &plen, 4);
    }
};

struct ByteReader {
    const uint8_t* p;
    const uint8_t* end;
    ByteReader(const uint8_t* d, std::size_t n) : p(d), end(d + n) {}
    bool      ok()  const { return p <= end; }
    bool      eof() const { return p >= end; }
    uint8_t   u8()  { return (p < end) ? *p++ : 0; }
    uint16_t  u16() { uint16_t v=0; if(p+2<=end){std::memcpy(&v,p,2);p+=2;} return ntohs(v); }
    uint32_t  u32() { uint32_t v=0; if(p+4<=end){std::memcpy(&v,p,4);p+=4;} return ntohl(v); }
    uint64_t  u64() { uint64_t v=0; if(p+8<=end){std::memcpy(&v,p,8);p+=8;} return be64toh(v); }
    std::string str() {
        uint32_t n = u32();
        if (p + n > end) { p = end; return {}; }
        std::string s(reinterpret_cast<const char*>(p), n);
        p += n;
        return s;
    }
};
