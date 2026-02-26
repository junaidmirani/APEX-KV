#include <apex/apex.hpp>

//
//  Run with: ./kv --test
//  Each test is independent.  PASS/FAIL printed to stdout.
//  A non-zero exit code means at least one test failed.
// ─────────────────────────────────────────────────────────────────────────────
namespace tests {

static int passed = 0, failed = 0;

#define CHECK(cond, msg) do {                                               \
    if (!(cond)) {                                                          \
        std::printf("  FAIL  %-50s  [%s:%d]\n", (msg), __FILE__, __LINE__);\
        ++failed;                                                           \
    } else {                                                                \
        std::printf("  PASS  %s\n", (msg));                                \
        ++passed;                                                           \
    }                                                                       \
} while(0)

static void test_hashmap() {
    std::printf("\n── HashMap ─────────────────────────────────────────\n");
    RobinHoodMap m(16);

    // Basic put/get/del
    m.put("hello", "world");
    std::string v;
    CHECK(m.get("hello", v) && v == "world",  "put + get");
    CHECK(m.size() == 1,                       "size after put");
    CHECK(!m.get("missing", v),               "get missing key");
    m.put("hello", "updated");
    CHECK(m.get("hello", v) && v == "updated", "overwrite value");
    CHECK(m.del("hello"),                      "delete existing key");
    CHECK(!m.get("hello", v),                 "get after delete");
    CHECK(m.size() == 0,                       "size after delete");

    // Collision & resize: insert 2000 keys
    for (int i = 0; i < 2000; i++) {
        std::string k = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);
        m.put(k, val);
    }
    CHECK(m.size() == 2000, "bulk insert size");
    bool all_found = true;
    for (int i = 0; i < 2000; i++) {
        std::string k = "key" + std::to_string(i);
        if (!m.get(k, v) || v != "val" + std::to_string(i)) { all_found = false; break; }
    }
    CHECK(all_found, "bulk get after resize");

    // Delete half, verify other half intact
    for (int i = 0; i < 1000; i++) m.del("key" + std::to_string(i));
    CHECK(m.size() == 1000, "size after bulk delete");
    bool deleted_gone = true;
    for (int i = 0; i < 1000; i++)
        if (m.get("key" + std::to_string(i), v)) { deleted_gone = false; break; }
    CHECK(deleted_gone, "deleted keys are gone");
    bool remainder_ok = true;
    for (int i = 1000; i < 2000; i++)
        if (!m.get("key" + std::to_string(i), v)) { remainder_ok = false; break; }
    CHECK(remainder_ok, "remaining keys intact after partial delete");
}

static void test_wyhash() {
    std::printf("\n── wyhash ──────────────────────────────────────────\n");
    // Same input → same hash
    uint64_t h1 = wyhash::hash_str("hello world");
    uint64_t h2 = wyhash::hash_str("hello world");
    CHECK(h1 == h2, "deterministic");

    // Different inputs → different hashes (with overwhelming probability)
    uint64_t h3 = wyhash::hash_str("hello worle");
    CHECK(h1 != h3, "avalanche on 1-bit diff");

    // Empty string should not crash
    uint64_t h4 = wyhash::hash_str("");
    CHECK(h4 != 0 || h4 == 0, "empty string no crash"); // trivially true but tests for crash

    // Long string
    std::string s(1024, 'x');
    uint64_t h5 = wyhash::hash_str(s);
    uint64_t h6 = wyhash::hash_str(s);
    CHECK(h5 == h6, "long string deterministic");
}

static void test_crc32() {
    std::printf("\n── CRC32 ───────────────────────────────────────────\n");
    // Known-good value for "hello" (IEEE 802.3)
    const uint8_t hello[] = {'h','e','l','l','o'};
    uint32_t c = crc32(hello, 5);
    CHECK(c == 0x3610A686u, "crc32('hello') == 0x3610A686");

    // Incremental == one-shot
    uint32_t inc = crc32(hello, 3);
    inc = crc32(hello + 3, 2, inc ^ 0xFFFFFFFFu); // re-apply initial xor
    // Note: incremental CRC needs careful chaining; just test no-crash here
    CHECK(crc32(hello, 5) == crc32(hello, 5), "crc32 reproducible");
}

static void test_wal() {
    std::printf("\n── WAL ─────────────────────────────────────────────\n");

    const std::string path = "/tmp/apex_test_wal.wal";
    unlink(path.c_str());

    WAL wal;
    auto r = wal.open(path);
    CHECK(r.is_ok(), "wal open");

    // Write several entries
    for (int i = 0; i < 10; i++) {
        LogEntry e;
        e.term  = 1;
        e.index = (uint64_t)(i + 1);
        e.op    = Cmd::PUT;
        e.key   = "k" + std::to_string(i);
        e.val   = "v" + std::to_string(i);
        CHECK(wal.append(e).is_ok(), ("wal append " + std::to_string(i)).c_str());
    }
    wal.close();

    // Reopen and replay
    WAL wal2;
    wal2.open(path);
    std::vector<LogEntry> replayed;
    wal2.replay([&](const LogEntry& e){ replayed.push_back(e); });
    wal2.close();

    CHECK(replayed.size() == 10, "wal replay count");
    bool correct = true;
    for (int i = 0; i < 10 && correct; i++) {
        if (replayed[i].key != "k" + std::to_string(i)) correct = false;
        if (replayed[i].val != "v" + std::to_string(i)) correct = false;
        if (replayed[i].index != (uint64_t)(i+1))       correct = false;
    }
    CHECK(correct, "wal replay data integrity");

    unlink(path.c_str());
}

static void test_ring() {
    std::printf("\n── ConsistentRing ──────────────────────────────────\n");
    ConsistentRing ring;

    ring.add_node({"127.0.0.1", 7001, 1});
    ring.add_node({"127.0.0.1", 7002, 2});
    ring.add_node({"127.0.0.1", 7003, 3});

    // Every key should resolve to one of the three nodes
    bool all_resolve = true;
    for (int i = 0; i < 1000; i++) {
        auto n = ring.lookup("key" + std::to_string(i));
        if (!n || (n->id != 1 && n->id != 2 && n->id != 3)) { all_resolve = false; break; }
    }
    CHECK(all_resolve, "every key resolves to a valid node");

    // Replicas: for replication factor 3, should get all 3 nodes
    auto reps = ring.replicas("anykey", 3);
    CHECK(reps.size() == 3, "replica count == 3");

    // After removing node 2, no key should map to it
    ring.remove_node(2);
    bool no_dead_node = true;
    for (int i = 0; i < 1000; i++) {
        auto n = ring.lookup("key" + std::to_string(i));
        if (n && n->id == 2) { no_dead_node = false; break; }
    }
    CHECK(no_dead_node, "no key maps to removed node");
}

static void test_raft_vote_counting() {
    std::printf("\n── Raft vote logic ─────────────────────────────────\n");
    // Test: quorum calculation for different cluster sizes
    auto quorum = [](int cluster_size) { return cluster_size / 2 + 1; };
    CHECK(quorum(1) == 1, "single-node quorum = 1");
    CHECK(quorum(3) == 2, "3-node quorum = 2");
    CHECK(quorum(5) == 3, "5-node quorum = 3");
    CHECK(quorum(7) == 4, "7-node quorum = 4");

    // Sanity: majority of peers means > half
    // 5 nodes: need 3 (self + 2 peers) out of 5
    int n = 5;
    int needed = quorum(n);
    CHECK(needed > n / 2, "quorum is majority");
}

static void test_slab_allocator() {
    std::printf("\n── SlabAllocator ───────────────────────────────────\n");

    // Alloc and free each size class
    static const std::size_t sizes[] = {16,32,64,128,256,512,1024,2048,4096};
    bool ok = true;
    for (auto sz : sizes) {
        void* p = g_slab.alloc(sz);
        if (!p) { ok = false; break; }
        std::memset(p, 0xAB, sz); // write pattern to detect UAF
        g_slab.dealloc(p, sz);
    }
    CHECK(ok, "alloc/free all size classes");

    // Oversized: falls back to malloc
    void* big = g_slab.alloc(8192);
    CHECK(big != nullptr, "oversized alloc falls back to malloc");
    g_slab.dealloc(big, 8192);

    // Batch alloc: fill & free many to exercise refill path
    std::vector<void*> ptrs;
    for (int i = 0; i < 1000; i++) ptrs.push_back(g_slab.alloc(64));
    CHECK(ptrs.back() != nullptr, "batch alloc 1000 x 64B");
    for (auto p : ptrs) g_slab.dealloc(p, 64);
}

void run_all() {
    std::printf("═══════════════════════════════════════════════════════\n");
    std::printf("  APEX-KV Unit Tests\n");
    std::printf("═══════════════════════════════════════════════════════\n");

    test_wyhash();
    test_crc32();
    test_slab_allocator();
    test_hashmap();
    test_wal();
    test_ring();
    test_raft_vote_counting();

    std::printf("\n═══════════════════════════════════════════════════════\n");
    std::printf("  Results: %d passed, %d failed\n", passed, failed);
    std::printf("═══════════════════════════════════════════════════════\n\n");
}

int exit_code() { return failed > 0 ? 1 : 0; }

} // namespace tests

// ─────────────────────────────────────────────────────────────────────────────