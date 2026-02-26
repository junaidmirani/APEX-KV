#include <apex/apex.hpp>

//
//  Pipelined: each thread sends PIPELINE_DEPTH requests before reading back.
//  This saturates the server's receive buffer and surfaces real throughput.
//  In v1, one request was sent then awaited synchronously — throughput was
//  dominated by round-trip latency, not server capacity.
// ─────────────────────────────────────────────────────────────────────────────
static void run_benchmark(const std::string& host, uint16_t port,
                           int nthreads, int ops_per_thread) {
    static constexpr int PIPELINE_DEPTH = 64; // requests in flight per thread

    std::cout <<
        "\n╔══════════════════════════════════════════════════════════╗\n"
        "║  APEX-KV Benchmark — pipelined                          ║\n"
        "╠══════════════════════════════════════════════════════════╣\n"
        "║  Threads: " << nthreads << "   Ops/thread: " << ops_per_thread <<
        "   Pipeline: " << PIPELINE_DEPTH << "\n"
        "╚══════════════════════════════════════════════════════════╝\n\n";

    std::atomic<uint64_t>               total_ops{0};
    std::vector<std::vector<int64_t>>   latencies(nthreads);
    std::vector<std::thread>            threads;
    auto start = Clock::now();

    auto worker = [&](int tid) {
        KVClient         client(host, port);
        std::mt19937_64  rng(tid * 0xdeadbeefull ^ 0xcafebabeull);
        auto&            lats = latencies[tid];
        lats.reserve(ops_per_thread + PIPELINE_DEPTH);

        int sent = 0, received = 0;

        auto send_one = [&]() {
            std::string key = "k:" + std::to_string(rng() % 100000);
            std::string val = "v:" + std::to_string(rng());
            ByteWriter  w;
            w.header(Cmd::PUT, 0, (uint32_t)(sent + 1));
            w.str(key); w.str(val);
            w.finalize();
            client.send_all(w.buf);
            ++sent;
        };

        auto recv_one = [&]() {
            auto t0 = Clock::now();
            auto [cmd, _] = client.recv_response();
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                          Clock::now() - t0).count();
            lats.push_back(us);
            ++received;
            ++total_ops;
        };

        // Prime the pipeline
        while (sent < std::min(PIPELINE_DEPTH, ops_per_thread)) send_one();

        // Steady state: send one, receive one
        while (received < ops_per_thread) {
            if (sent < ops_per_thread) send_one();
            recv_one();
        }
        // Drain remaining in-flight
        while (received < sent) recv_one();
    };

    threads.reserve(nthreads);
    for (int i = 0; i < nthreads; i++) threads.emplace_back(worker, i);
    for (auto& t : threads) t.join();

    double elapsed = std::chrono::duration<double>(Clock::now() - start).count();
    double throughput = total_ops.load() / elapsed;

    std::vector<int64_t> all;
    all.reserve(nthreads * ops_per_thread);
    for (auto& v : latencies) all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());

    auto pct = [&](double p) -> int64_t {
        std::size_t idx = (std::size_t)(p * (double)all.size() / 100.0);
        return idx < all.size() ? all[idx] : 0;
    };

    std::printf(
        "  Throughput:   %llu ops/sec\n"
        "  Latency p50:  %lld µs\n"
        "  Latency p95:  %lld µs\n"
        "  Latency p99:  %lld µs\n"
        "  Latency p99.9:%lld µs\n\n",
        (unsigned long long)throughput,
        (long long)pct(50),
        (long long)pct(95),
        (long long)pct(99),
        (long long)pct(99.9));
}

// ─────────────────────────────────────────────────────────────────────────────