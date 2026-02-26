// ─────────────────────────────────────────────────────────────────────────────
// apex/common.hpp  —  §1 Primitives  §2 Errors  §3 Logger  §4 Metrics
//                     §5 CRC32       §6 wyhash
// ─────────────────────────────────────────────────────────────────────────────
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <functional>
#include <immintrin.h>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

// §1 Compiler primitives
#define APEX_LIKELY(x)   __builtin_expect(!!(x), 1)
#define APEX_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define APEX_INLINE      __attribute__((always_inline)) inline
#define APEX_NOINLINE    __attribute__((noinline))
#define APEX_COLD        __attribute__((cold))
#define APEX_PURE        __attribute__((pure))
#define APEX_HOT         __attribute__((hot))

static constexpr std::size_t CACHELINE = 64;
#define APEX_CL_ALIGNED  alignas(CACHELINE)
#define APEX_CL_PAD(id, used) \
    char _pad_##id[((used) % CACHELINE == 0) ? CACHELINE : (CACHELINE - (used) % CACHELINE)]

APEX_INLINE void cpu_pause()              noexcept { __builtin_ia32_pause(); }
APEX_INLINE void prefetch_r(const void* p) noexcept { __builtin_prefetch(p, 0, 3); }
APEX_INLINE void prefetch_w(const void* p) noexcept { __builtin_prefetch(p, 1, 3); }
using Clock = std::chrono::steady_clock;

// §2 Errors
enum class Errc : uint8_t {
    Ok=0, NotFound=1, NotLeader=2, IoError=3, Corrupt=4, Timeout=5, Full=6, BadArg=7,
};
inline const char* errc_str(Errc e) {
    switch (e) {
        case Errc::Ok:        return "Ok";
        case Errc::NotFound:  return "NotFound";
        case Errc::NotLeader: return "NotLeader";
        case Errc::IoError:   return "IoError";
        case Errc::Corrupt:   return "Corrupt";
        case Errc::Timeout:   return "Timeout";
        case Errc::Full:      return "Full";
        case Errc::BadArg:    return "BadArg";
    }
    return "?";
}

template<typename T>
struct Result {
    bool ok_;
    union { T value_; Errc err_; };
    Result(T v) : ok_(true), value_(std::move(v)) {}
    static Result ok(T v)    { return Result(std::move(v)); }
    static Result err(Errc e){ Result r; r.ok_=false; r.err_=e; return r; }
    bool     is_ok()  const noexcept { return ok_; }
    bool     is_err() const noexcept { return !ok_; }
    Errc     error()  const noexcept { return ok_ ? Errc::Ok : err_; }
    T&       value()        noexcept { return value_; }
    const T& value()  const noexcept { return value_; }
    T        value_or(T def) const { return ok_ ? value_ : def; }
    ~Result() { if (ok_) value_.~T(); }
    Result(const Result& o) : ok_(o.ok_) {
        if (ok_) new(&value_) T(o.value_); else err_ = o.err_;
    }
    Result(Result&& o) noexcept : ok_(o.ok_) {
        if (ok_) new(&value_) T(std::move(o.value_)); else err_ = o.err_;
    }
private:
    Result() {}
};

template<>
struct Result<void> {
    Errc err_; bool ok_;
    Result() : err_(Errc::Ok), ok_(true) {}
    Result(Errc e) : err_(e), ok_(e==Errc::Ok) {}
    static Result ok()        { return Result{}; }
    static Result err(Errc e) { return Result{e}; }
    bool is_ok()  const noexcept { return ok_; }
    bool is_err() const noexcept { return !ok_; }
    Errc error()  const noexcept { return err_; }
};

// §3 Logger — structured JSON output (parseable by Loki/ELK)
enum class LogLevel : int { Debug=0, Info=1, Warn=2, Error=3 };

class Logger {
    std::mutex       mtx_;
    std::atomic<int> min_level_{static_cast<int>(LogLevel::Info)};
    uint32_t         node_id_{0};
    Clock::time_point start_{Clock::now()};
public:
    void set_node_id(uint32_t id) noexcept { node_id_=id; }
    void set_level(LogLevel l)    noexcept { min_level_.store(static_cast<int>(l),std::memory_order_relaxed); }
    bool should_log(LogLevel l) const noexcept {
        return static_cast<int>(l) >= min_level_.load(std::memory_order_relaxed);
    }
    void write(LogLevel lvl, const char* msg) {
        if (!should_log(lvl)) return;
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now()-start_).count();
        const char* ls = (lvl==LogLevel::Debug?"debug":lvl==LogLevel::Info?"info":lvl==LogLevel::Warn?"warn":"error");
        std::lock_guard lk(mtx_);
        std::fprintf(stderr, "{\"ts_us\":%lld,\"node\":%u,\"level\":\"%s\",\"msg\":\"%s\"}\n",
                     (long long)us, node_id_, ls, msg);
    }
};
inline Logger g_log;

#define LOG(lvl, ...)  do { if(g_log.should_log(lvl)){char _b[1024];std::snprintf(_b,sizeof _b,__VA_ARGS__);g_log.write(lvl,_b);} } while(0)
#define LOG_DEBUG(...) LOG(LogLevel::Debug, __VA_ARGS__)
#define LOG_INFO(...)  LOG(LogLevel::Info,  __VA_ARGS__)
#define LOG_WARN(...)  LOG(LogLevel::Warn,  __VA_ARGS__)
#define LOG_ERROR(...) LOG(LogLevel::Error, __VA_ARGS__)

// §4 Metrics — lock-free atomics + Prometheus exposition
struct Histogram {
    APEX_CL_ALIGNED std::atomic<uint64_t> count{0};
    APEX_CL_ALIGNED std::atomic<uint64_t> sum_us{0};
    APEX_CL_ALIGNED std::atomic<uint64_t> buckets[64]{};
    void record(uint64_t us) noexcept {
        count.fetch_add(1, std::memory_order_relaxed);
        sum_us.fetch_add(us, std::memory_order_relaxed);
        int b = (us==0)?0:(63-__builtin_clzll(us));
        if (b>63) b=63;
        buckets[b].fetch_add(1, std::memory_order_relaxed);
    }
    uint64_t percentile(double p) const noexcept {
        uint64_t total = count.load(std::memory_order_relaxed);
        if (!total) return 0;
        uint64_t target=static_cast<uint64_t>(p*static_cast<double>(total)), running=0;
        for (int i=0;i<64;i++){
            running+=buckets[i].load(std::memory_order_relaxed);
            if (running>=target) return (i==0)?0:(uint64_t(1)<<i);
        }
        return uint64_t(1)<<63;
    }
    uint64_t avg_us() const noexcept {
        uint64_t n=count.load(std::memory_order_relaxed);
        return n?sum_us.load(std::memory_order_relaxed)/n:0;
    }
};

struct Metrics {
    std::atomic<uint64_t> ops_get{0}, ops_put{0}, ops_del{0};
    std::atomic<uint64_t> err_not_found{0}, err_not_leader{0}, err_io{0};
    std::atomic<uint64_t> raft_elections{0}, raft_commits{0};
    std::atomic<uint64_t> gossip_alive{0}, gossip_suspect{0}, gossip_dead{0};
    std::atomic<uint64_t> connections_accepted{0}, connections_closed{0};
    Histogram             lat_get_us, lat_put_us;

    void print(uint32_t nid) const {
        std::printf("\n── APEX-KV Metrics (node %u) ───────────────────────────────\n"
            "  ops         get=%-8llu  put=%-8llu  del=%-8llu\n"
            "  errors      not_found=%-6llu  not_leader=%-6llu  io=%-6llu\n"
            "  raft        elections=%-6llu  commits=%-8llu\n"
            "  gossip      alive=%-6llu  suspect=%-6llu  dead=%-6llu\n"
            "  lat GET     avg=%-5llu  p50=%-5llu  p95=%-5llu  p99=%-5llu µs\n"
            "  lat PUT     avg=%-5llu  p50=%-5llu  p95=%-5llu  p99=%-5llu µs\n"
            "────────────────────────────────────────────────────────────\n",
            nid,
            (unsigned long long)ops_get.load(),(unsigned long long)ops_put.load(),(unsigned long long)ops_del.load(),
            (unsigned long long)err_not_found.load(),(unsigned long long)err_not_leader.load(),(unsigned long long)err_io.load(),
            (unsigned long long)raft_elections.load(),(unsigned long long)raft_commits.load(),
            (unsigned long long)gossip_alive.load(),(unsigned long long)gossip_suspect.load(),(unsigned long long)gossip_dead.load(),
            (unsigned long long)lat_get_us.avg_us(),(unsigned long long)lat_get_us.percentile(0.50),
            (unsigned long long)lat_get_us.percentile(0.95),(unsigned long long)lat_get_us.percentile(0.99),
            (unsigned long long)lat_put_us.avg_us(),(unsigned long long)lat_put_us.percentile(0.50),
            (unsigned long long)lat_put_us.percentile(0.95),(unsigned long long)lat_put_us.percentile(0.99));
    }

    // Prometheus text format for /metrics HTTP endpoint
    std::string prometheus(uint32_t nid) const {
        char buf[4096];
        std::snprintf(buf, sizeof buf,
            "# HELP apex_ops_total Total KV operations\n"
            "# TYPE apex_ops_total counter\n"
            "apex_ops_total{node=\"%u\",op=\"get\"} %llu\n"
            "apex_ops_total{node=\"%u\",op=\"put\"} %llu\n"
            "apex_ops_total{node=\"%u\",op=\"del\"} %llu\n"
            "# HELP apex_raft_commits_total Committed Raft log entries\n"
            "# TYPE apex_raft_commits_total counter\n"
            "apex_raft_commits_total{node=\"%u\"} %llu\n"
            "# HELP apex_raft_elections_total Leader election events\n"
            "# TYPE apex_raft_elections_total counter\n"
            "apex_raft_elections_total{node=\"%u\"} %llu\n"
            "# HELP apex_latency_us Operation latency percentiles\n"
            "# TYPE apex_latency_us gauge\n"
            "apex_latency_us{node=\"%u\",op=\"get\",quantile=\"0.5\"} %llu\n"
            "apex_latency_us{node=\"%u\",op=\"get\",quantile=\"0.99\"} %llu\n"
            "apex_latency_us{node=\"%u\",op=\"put\",quantile=\"0.5\"} %llu\n"
            "apex_latency_us{node=\"%u\",op=\"put\",quantile=\"0.99\"} %llu\n"
            "# HELP apex_gossip_members SWIM gossip membership count\n"
            "# TYPE apex_gossip_members gauge\n"
            "apex_gossip_members{node=\"%u\",state=\"alive\"} %llu\n"
            "apex_gossip_members{node=\"%u\",state=\"suspect\"} %llu\n"
            "apex_gossip_members{node=\"%u\",state=\"dead\"} %llu\n",
            nid,(unsigned long long)ops_get.load(),
            nid,(unsigned long long)ops_put.load(),
            nid,(unsigned long long)ops_del.load(),
            nid,(unsigned long long)raft_commits.load(),
            nid,(unsigned long long)raft_elections.load(),
            nid,(unsigned long long)lat_get_us.percentile(0.50),
            nid,(unsigned long long)lat_get_us.percentile(0.99),
            nid,(unsigned long long)lat_put_us.percentile(0.50),
            nid,(unsigned long long)lat_put_us.percentile(0.99),
            nid,(unsigned long long)gossip_alive.load(),
            nid,(unsigned long long)gossip_suspect.load(),
            nid,(unsigned long long)gossip_dead.load());
        return buf;
    }
};
inline Metrics g_metrics;

// §5 CRC32 (IEEE 802.3)
namespace crc32_impl {
    static uint32_t table[256];
    static bool initialized = false;
    static void init() noexcept {
        for (uint32_t i=0;i<256;i++){uint32_t c=i;for(int j=0;j<8;j++)c=(c>>1)^(c&1u?0xEDB88320u:0u);table[i]=c;}
        initialized=true;
    }
}
APEX_INLINE uint32_t crc32(const uint8_t* data, std::size_t len, uint32_t crc=0xFFFFFFFFu) noexcept {
    if (APEX_UNLIKELY(!crc32_impl::initialized)) crc32_impl::init();
    for (std::size_t i=0;i<len;i++) crc=crc32_impl::table[(crc^data[i])&0xFF]^(crc>>8);
    return crc^0xFFFFFFFFu;
}

// §6 wyhash — ~3 GB/s non-cryptographic hash
namespace wyhash {
    static constexpr uint64_t S0=0xa0761d6478bd642full,S1=0xe7037ed1a0b428dbull,
                               S2=0x8ebc6af09c88c6e3ull,S3=0x589965cc75374cc3ull;
    APEX_INLINE uint64_t mix(uint64_t a,uint64_t b) noexcept {
        __uint128_t r=(__uint128_t)a*b; return (uint64_t)(r>>64)^(uint64_t)r;
    }
    APEX_HOT uint64_t hash(const void* key,std::size_t len,uint64_t seed=0) noexcept {
        const auto* p=static_cast<const uint8_t*>(key); seed^=S0;
        uint64_t a=0,b=0;
        if (APEX_LIKELY(len<=16)){
            if (len>=4){uint32_t lo,hi,m,n;memcpy(&lo,p,4);memcpy(&hi,p+len-4,4);a=((uint64_t)lo<<32)|hi;memcpy(&m,p+(len>>3),4);memcpy(&n,p+len-4-(len>>3),4);b=((uint64_t)m<<32)|n;}
            else if(len>0){a=((uint64_t)p[0]<<16)|((uint64_t)p[len>>1]<<8)|p[len-1];}
        } else {
            std::size_t i=len;
            if (APEX_UNLIKELY(i>48)){uint64_t s1=seed,s2=seed;do{uint64_t v0,v1,v2,v3,v4,v5;memcpy(&v0,p,8);memcpy(&v1,p+8,8);memcpy(&v2,p+16,8);memcpy(&v3,p+24,8);memcpy(&v4,p+32,8);memcpy(&v5,p+40,8);seed=mix(v0^S1,v1^seed);s1=mix(v2^S2,v3^s1);s2=mix(v4^S3,v5^s2);p+=48;i-=48;}while(APEX_LIKELY(i>48));seed^=s1^s2;}
            while(APEX_UNLIKELY(i>16)){uint64_t v0,v1;memcpy(&v0,p,8);memcpy(&v1,p+8,8);seed=mix(v0^S1,v1^seed);p+=16;i-=16;}
            memcpy(&a,p+i-16,8);memcpy(&b,p+i-8,8);
        }
        return mix(S1^(uint64_t)len,mix(a^S1,b^seed));
    }
    APEX_INLINE uint64_t hash_str(std::string_view sv,uint64_t seed=0) noexcept { return hash(sv.data(),sv.size(),seed); }
}
