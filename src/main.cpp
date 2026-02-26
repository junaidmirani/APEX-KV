#include <apex/apex.hpp>
#include "client.cpp"
#include "bench.cpp"
#include "tests.cpp"

// ─────────────────────────────────────────────────────────────────────────────
static void print_banner() {
    std::printf(
        "\n"
        "  ╔══════════════════════════════════════════════════════════════════╗\n"
        "  ║   A P E X - K V  v2.0  Production-Grade Distributed KV Store   ║\n"
        "  ║   Lock-Free · Raft+WAL · Gossip · Consistent Hash · C++20      ║\n"
        "  ╚══════════════════════════════════════════════════════════════════╝\n\n");
}

static void print_usage(const char* prog) {
    std::printf(
        "  Node:      %s --id <N> --port <P> [--peers host:port:id,...] [--wal-dir .]\n"
        "  Client:    %s --client <host:port>\n"
        "  Benchmark: %s --bench <host:port> [--threads N] [--ops N]\n"
        "  Tests:     %s --test\n\n"
        "  3-node example:\n"
        "    %s --id 1 --port 7001 --peers 127.0.0.1:7002:2,127.0.0.1:7003:3 &\n"
        "    %s --id 2 --port 7002 --peers 127.0.0.1:7001:1,127.0.0.1:7003:3 &\n"
        "    %s --id 3 --port 7003 --peers 127.0.0.1:7001:1,127.0.0.1:7002:2 &\n"
        "    %s --client 127.0.0.1:7001\n",
        prog, prog, prog, prog, prog, prog, prog, prog);
}

static std::tuple<std::string, uint16_t, uint32_t> parse_peer(const std::string& s) {
    // format: host:port:id
    auto p1 = s.find(':');
    auto p2 = s.rfind(':');
    if (p1 == std::string::npos || p1 == p2)
        return {s, 7001, 0};
    std::string host = s.substr(0, p1);
    uint16_t    port = (uint16_t)std::stoi(s.substr(p1 + 1, p2 - p1 - 1));
    uint32_t    id   = (uint32_t)std::stoul(s.substr(p2 + 1));
    return {host, port, id};
}

int main(int argc, char** argv) {
    print_banner();
    if (argc < 2) { print_usage(argv[0]); return 0; }

    // Initialise CRC32 table early (it's lazy otherwise)
    crc32_impl::init();

    std::string mode;
    uint32_t    node_id       = 1;
    uint16_t    port          = 7001;
    std::string host          = "0.0.0.0";
    std::string wal_dir       = ".";
    std::string client_addr, bench_addr;
    int         bench_threads = 4, bench_ops = 50000;
    std::vector<std::string> peer_strs;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if      (a == "--id"       && i+1<argc) node_id       = std::stoul(argv[++i]);
        else if (a == "--port"     && i+1<argc) port          = (uint16_t)std::stoul(argv[++i]);
        else if (a == "--host"     && i+1<argc) host          = argv[++i];
        else if (a == "--wal-dir"  && i+1<argc) wal_dir       = argv[++i];
        else if (a == "--client"   && i+1<argc) { mode = "client"; client_addr = argv[++i]; }
        else if (a == "--bench"    && i+1<argc) { mode = "bench";  bench_addr  = argv[++i]; }
        else if (a == "--threads"  && i+1<argc) bench_threads = std::stoi(argv[++i]);
        else if (a == "--ops"      && i+1<argc) bench_ops     = std::stoi(argv[++i]);
        else if (a == "--test")                 mode = "test";
        else if (a == "--debug")                g_log.set_level(LogLevel::Debug);
        else if (a == "--peers"    && i+1<argc) {
            std::istringstream ss(argv[++i]);
            std::string p;
            while (std::getline(ss, p, ',')) peer_strs.push_back(p);
        }
        else if (a == "--help" || a == "-h") { print_usage(argv[0]); return 0; }
    }

    if (mode == "test") {
        tests::run_all();
        return tests::exit_code();
    }

    if (mode == "client") {
        auto p1 = client_addr.rfind(':');
        std::string h = client_addr.substr(0, p1);
        uint16_t    p = (uint16_t)std::stoi(client_addr.substr(p1 + 1));
        try { KVClient(h, p).repl(); }
        catch (std::exception& e) { std::fprintf(stderr, "Client: %s\n", e.what()); return 1; }
        return 0;
    }

    if (mode == "bench") {
        auto p1 = bench_addr.rfind(':');
        std::string h = bench_addr.substr(0, p1);
        uint16_t    p = (uint16_t)std::stoi(bench_addr.substr(p1 + 1));
        run_benchmark(h, p, bench_threads, bench_ops);
        return 0;
    }

    // ── Node mode ─────────────────────────────────────────────────────────────
    std::printf("  Starting node id=%u  port=%u  wal-dir=%s\n\n",
                node_id, port, wal_dir.c_str());

    try {
        KVNode node(node_id, "127.0.0.1", port, wal_dir);

        for (auto& ps : peer_strs) {
            auto [ph, pp, pid] = parse_peer(ps);
            if (pid == 0) pid = (uint32_t)(pp % 10000);  // fallback id derivation
            if (pp == port && pid == node_id) continue;  // skip self
            node.add_peer(ph, pp, pid);
        }

        node.start();
        std::printf("  Node %u running.  Press Ctrl-C to stop.\n\n", node_id);

        // Status loop — print metrics every 10 seconds
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            std::printf("  [node %u] %s\n",
                        node_id, node.is_leader() ? "★  LEADER" : "   follower");
            g_metrics.print(node_id);
        }
    } catch (std::exception& e) {
        std::fprintf(stderr, "Fatal: %s\n", e.what());
        return 1;
    }

    return 0;
}