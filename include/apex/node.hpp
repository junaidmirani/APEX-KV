#pragma once
#include "raft.hpp"
#include "gossip.hpp"
#include "store.hpp"
#include "thread_pool.hpp"
#include "queue.hpp"

// §17  NETWORK LAYER + KV NODE
//
//  All I/O ownership: the net_thread is the ONLY thread that calls send()/recv()
//  on any file descriptor.  This eliminates the fd-race from v1.
//
//  Raft and Gossip want to send messages.  They post to peer_send_queue_ and
//  signal the net_thread via an eventfd.  The net_thread drains the queue and
//  calls the real send — single-owner, no mutex on the fd.
//
//  Peer connection lifecycle:
//    1. KVNode establishes outbound TCP to every peer at startup.
//    2. These fds are registered with epoll and tagged with the peer's node_id.
//    3. Incoming client connections come via accept() on listen_fd_.
//    4. No inbound peer connections are expected (outbound-only for Raft/Gossip).
//
//  Connection table (conns_) is ONLY accessed from net_thread_ — no lock needed.
// ─────────────────────────────────────────────────────────────────────────────

static constexpr int NUM_SHARDS = 64;

struct APEX_CL_ALIGNED Shard {
    std::mutex    mtx;
    RobinHoodMap  map;
    Shard() : map(1 << 14) {}
};

struct Conn {
    int              fd        = -1;
    int              peer_id   = -1;   // ≥0 if this is a peer connection
    std::vector<uint8_t> rbuf;
    std::vector<uint8_t> wbuf;
    std::size_t      wpos      = 0;

    bool is_peer()    const { return peer_id >= 0; }
    bool has_msg()    const {
        if (rbuf.size() < sizeof(MsgHeader)) return false;
        MsgHeader h;
        std::memcpy(&h, rbuf.data(), sizeof h);
        if (ntohl(h.magic) != WIRE_MAGIC) return false;
        return rbuf.size() >= sizeof(h) + ntohl(h.payload_len);
    }
    void queue_write(const std::vector<uint8_t>& data) {
        wbuf.insert(wbuf.end(), data.begin(), data.end());
    }
};

struct PeerSend {
    uint32_t             peer_id;
    std::vector<uint8_t> data;
};

class KVNode {
public:
    KVNode(uint32_t id, const std::string& host, uint16_t port,
           const std::string& wal_dir = ".")
        : id_(id), host_(host), port_(port)
        , pool_(std::thread::hardware_concurrency())
        , ring_()
        , gossip_(id, host, port)
        , raft_(id,
            // send_fn: post to queue + poke eventfd — net_thread does the actual send
            [this](uint32_t peer_id, std::vector<uint8_t> data) {
                peer_send_queue_.enqueue({peer_id, std::move(data)});
                uint64_t v = 1;
                { ssize_t _r = write(event_fd_, &v, 8); (void)_r; }
            },
            // apply_fn: apply committed Raft entries to the sharded map
            [this](const LogEntry& e) { this->apply(e); })
    {
        g_log.set_node_id(id);

        listen_fd_ = tcp_listen(port);
        if (listen_fd_ < 0)
            throw std::runtime_error("Cannot bind port " + std::to_string(port));

        event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (event_fd_ < 0)
            throw std::runtime_error("eventfd failed");

        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0)
            throw std::runtime_error("epoll_create1 failed");

        // Register listen socket
        add_to_epoll(listen_fd_, EPOLLIN | EPOLLET);
        // Register eventfd for raft/gossip send wakeup
        add_to_epoll(event_fd_, EPOLLIN);

        // Open WAL
        std::string wal_path = wal_dir + "/raft_" + std::to_string(id) + ".wal";
        auto wr = raft_.open_wal(wal_path);
        if (wr.is_err())
            LOG_WARN("WAL open failed (%s): continuing without durability", errc_str(wr.error()));

        ring_.add_node({host, port, id});
        LOG_INFO("node %u started on %s:%u", id, host.c_str(), port);
    }

    ~KVNode() {
        stop();
        for (auto& [fd, c] : conns_) close(fd);
        if (listen_fd_ >= 0) close(listen_fd_);
        if (event_fd_  >= 0) close(event_fd_);
        if (epoll_fd_  >= 0) close(epoll_fd_);
    }

    void add_peer(const std::string& peer_host, uint16_t peer_port, uint32_t peer_id) {
        NodeAddr addr{peer_host, peer_port, peer_id};
        ring_.add_node(addr);
        raft_.add_peer(addr);
        gossip_.add_peer(addr);
        peer_addrs_[peer_id] = addr;
        LOG_INFO("registered peer %u at %s:%u", peer_id, peer_host.c_str(), peer_port);
    }

    void start() {
        running_.store(true, std::memory_order_release);
        raft_.start();
        gossip_.start();
        net_thread_ = std::thread([this]{ net_loop(); });
    }

    void stop() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) return;
        raft_.stop();
        gossip_.stop();
        // Wake the net thread so it exits cleanly
        uint64_t v = 1;
        { ssize_t _r = write(event_fd_, &v, 8); (void)_r; }
        if (net_thread_.joinable()) net_thread_.join();
    }

    bool is_leader() const { return raft_.is_leader(); }
    uint32_t id()    const { return id_; }

    // Public KV API (thread-safe)
    Result<std::string> get(const std::string& key) {
        auto t0 = Clock::now();
        int  s  = shard_for(key);
        std::string out;
        bool found;
        {
            std::lock_guard lk(shards_[s].mtx);
            found = shards_[s].map.get(key, out);
        }
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count();
        g_metrics.lat_get_us.record((uint64_t)us);
        g_metrics.ops_get.fetch_add(1, std::memory_order_relaxed);
        if (!found) {
            g_metrics.err_not_found.fetch_add(1, std::memory_order_relaxed);
            return Result<std::string>::err(Errc::NotFound);
        }
        return Result<std::string>::ok(std::move(out));
    }

    Result<void> put(const std::string& key, const std::string& val) {
        auto t0 = Clock::now();
        // Strong-consistency path: route through Raft
        auto r = raft_.propose(Cmd::PUT, key, val);
        if (r.is_err() && r.error() == Errc::NotLeader) {
            // AP fallback: write directly (for demos / eventual consistency mode)
            int s = shard_for(key);
            std::lock_guard lk(shards_[s].mtx);
            shards_[s].map.put(key, val);
        } else if (r.is_err()) {
            g_metrics.err_io.fetch_add(1, std::memory_order_relaxed);
            return r;
        }
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count();
        g_metrics.lat_put_us.record((uint64_t)us);
        g_metrics.ops_put.fetch_add(1, std::memory_order_relaxed);
        return Result<void>::ok();
    }

    Result<void> del(const std::string& key) {
        g_metrics.ops_del.fetch_add(1, std::memory_order_relaxed);
        auto r = raft_.propose(Cmd::DEL, key, "");
        if (r.is_err() && r.error() == Errc::NotLeader) {
            int s = shard_for(key);
            std::lock_guard lk(shards_[s].mtx);
            shards_[s].map.del(key);
            return Result<void>::ok();
        }
        return r;
    }

private:
    uint32_t           id_;
    std::string        host_;
    uint16_t           port_;
    ThreadPool         pool_;
    ConsistentRing     ring_;
    GossipProtocol     gossip_;
    RaftEngine         raft_;
    Shard              shards_[NUM_SHARDS];

    int                listen_fd_ = -1;
    int                event_fd_  = -1;
    int                epoll_fd_  = -1;

    // conns_ is ONLY accessed from net_thread_ — no lock needed.
    std::unordered_map<int, Conn>      conns_;
    std::unordered_map<uint32_t, int>  peer_fd_;   // peer_id → fd
    std::unordered_map<uint32_t, NodeAddr> peer_addrs_;

    MpscQueue<PeerSend>  peer_send_queue_;

    std::atomic<bool>    running_{false};
    std::thread          net_thread_;

    // ── Helpers ──────────────────────────────────────────────────────────────

    void apply(const LogEntry& e) {
        int s = shard_for(e.key);
        std::lock_guard lk(shards_[s].mtx);
        switch (e.op) {
            case Cmd::PUT: shards_[s].map.put(e.key, e.val); break;
            case Cmd::DEL: shards_[s].map.del(e.key);        break;
            default: break;
        }
    }

    APEX_PURE int shard_for(const std::string& key) const noexcept {
        return (int)(wyhash::hash_str(key) & (NUM_SHARDS - 1));
    }

    void add_to_epoll(int fd, uint32_t events) {
        epoll_event ev{};
        ev.events  = events;
        ev.data.fd = fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0)
            LOG_ERROR("epoll_ctl ADD fd=%d: %s", fd, strerror(errno));
    }

    void mod_epoll(int fd, uint32_t events) {
        epoll_event ev{};
        ev.events  = events;
        ev.data.fd = fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
    }

    // ── Network event loop ────────────────────────────────────────────────────
    // Single-threaded owner of all fds.  Everything is edge-triggered.
    void net_loop() {
        // Connect to all known peers
        for (auto& [pid, addr] : peer_addrs_) connect_to_peer(pid, addr);

        static constexpr int MAX_EV = 256;
        epoll_event events[MAX_EV];

        while (running_.load(std::memory_order_relaxed)) {
            int n = epoll_wait(epoll_fd_, events, MAX_EV, 20);
            for (int i = 0; i < n; i++) {
                int      fd = events[i].data.fd;
                uint32_t ev = events[i].events;

                if (fd == listen_fd_) {
                    accept_loop();
                } else if (fd == event_fd_) {
                    // Raft/Gossip posted messages for us to send.
                    uint64_t val;
                    { ssize_t _r = read(event_fd_, &val, 8); (void)_r; }
                    drain_peer_send_queue();
                } else if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    close_conn(fd);
                } else {
                    if (ev & EPOLLOUT) flush_conn(fd);
                    if (ev & EPOLLIN)  read_conn(fd);
                }
            }
        }
    }

    void drain_peer_send_queue() {
        while (auto msg = peer_send_queue_.dequeue()) {
            send_to_peer(msg->peer_id, msg->data);
        }
    }

    void connect_to_peer(uint32_t peer_id, const NodeAddr& addr) {
        int fd = tcp_connect_nb(addr.host, addr.port);
        if (fd < 0) {
            LOG_WARN("connect to peer %u (%s:%u) failed: %s",
                     peer_id, addr.host.c_str(), addr.port, strerror(errno));
            return;
        }
        conns_[fd].fd      = fd;
        conns_[fd].peer_id = (int)peer_id;
        peer_fd_[peer_id]  = fd;
        // EPOLLOUT to detect connect() completion, then switch to EPOLLIN
        add_to_epoll(fd, EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP);
        LOG_DEBUG("connecting to peer %u fd=%d", peer_id, fd);
    }

    void send_to_peer(uint32_t peer_id, const std::vector<uint8_t>& data) {
        auto it = peer_fd_.find(peer_id);
        if (it == peer_fd_.end()) {
            // Try to connect on demand
            auto pit = peer_addrs_.find(peer_id);
            if (pit == peer_addrs_.end()) {
                LOG_WARN("send_to_peer: unknown peer %u", peer_id);
                return;
            }
            connect_to_peer(peer_id, pit->second);
            it = peer_fd_.find(peer_id);
            if (it == peer_fd_.end()) return;
        }
        int fd = it->second;
        if (!conns_.count(fd)) return;
        conns_[fd].queue_write(data);
        flush_conn(fd);
    }

    void accept_loop() {
        for (;;) {
            sockaddr_in client{};
            socklen_t   len = sizeof client;
            int cfd = accept4(listen_fd_, (sockaddr*)&client, &len,
                              SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (cfd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                LOG_ERROR("accept: %s", strerror(errno));
                continue;
            }
            int one = 1;
            setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
            conns_[cfd].fd      = cfd;
            conns_[cfd].peer_id = -1;
            add_to_epoll(cfd, EPOLLIN | EPOLLET | EPOLLRDHUP);
            g_metrics.connections_accepted.fetch_add(1, std::memory_order_relaxed);
            LOG_DEBUG("accepted client fd=%d from %s:%u",
                      cfd, inet_ntoa(client.sin_addr), ntohs(client.sin_port));
        }
    }

    void read_conn(int fd) {
        auto it = conns_.find(fd);
        if (it == conns_.end()) return;
        Conn& conn = it->second;

        // Read until EAGAIN (edge-triggered).
        for (;;) {
            uint8_t tmp[65536];
            ssize_t n = recv(fd, tmp, sizeof tmp, MSG_DONTWAIT);
            if (n > 0) {
                conn.rbuf.insert(conn.rbuf.end(), tmp, tmp + n);
            } else if (n == 0) {
                close_conn(fd); return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                LOG_DEBUG("recv fd=%d: %s", fd, strerror(errno));
                close_conn(fd); return;
            }
        }

        // Dispatch every complete message in the read buffer.
        while (conn.has_msg()) {
            process_msg(conn);
        }
    }

    void process_msg(Conn& conn) {
        MsgHeader hdr;
        std::memcpy(&hdr, conn.rbuf.data(), sizeof hdr);

        uint32_t    plen      = ntohl(hdr.payload_len);
        uint32_t    seq       = ntohl(hdr.seq);
        uint32_t    sender_id = ntohs(hdr.sender_id);
        Cmd         cmd       = static_cast<Cmd>(hdr.cmd);
        const uint8_t* payload = conn.rbuf.data() + sizeof(hdr);

        std::vector<uint8_t> resp;

        switch (cmd) {
            case Cmd::PING: {
                ByteWriter w;
                w.header(Cmd::PONG, id_, seq);
                w.finalize();
                resp = std::move(w.buf);
                break;
            }
            case Cmd::GET: {
                ByteReader r(payload, plen);
                std::string key = r.str();
                auto result = get(key);
                ByteWriter w;
                if (result.is_ok()) {
                    w.header(Cmd::VALUE, id_, seq);
                    w.str(result.value());
                } else {
                    w.header(Cmd::NOT_FOUND, id_, seq);
                }
                w.finalize();
                resp = std::move(w.buf);
                break;
            }
            case Cmd::PUT: {
                ByteReader r(payload, plen);
                std::string key = r.str();
                std::string val = r.str();
                auto result = put(key, val);
                ByteWriter w;
                if (result.is_ok()) {
                    w.header(Cmd::OK, id_, seq);
                } else if (result.error() == Errc::NotLeader) {
                    // Tell the client where the leader is
                    w.header(Cmd::REDIRECT, id_, seq);
                    uint32_t lid = raft_.leader_id();
                    auto pit = peer_addrs_.find(lid);
                    if (pit != peer_addrs_.end())
                        w.str(pit->second.host + ":" + std::to_string(pit->second.port));
                    else
                        w.str("unknown");
                } else {
                    w.header(Cmd::ERROR, id_, seq);
                    w.str(errc_str(result.error()));
                }
                w.finalize();
                resp = std::move(w.buf);
                break;
            }
            case Cmd::DEL: {
                ByteReader r(payload, plen);
                std::string key = r.str();
                auto result = del(key);
                ByteWriter w;
                w.header(result.is_ok() ? Cmd::OK : Cmd::NOT_FOUND, id_, seq);
                w.finalize();
                resp = std::move(w.buf);
                break;
            }
            case Cmd::METRICS: {
                ByteWriter w;
                w.header(Cmd::METRICS_RESP, id_, seq);
                // Serialize key metrics
                w.u64(g_metrics.ops_get.load());
                w.u64(g_metrics.ops_put.load());
                w.u64(g_metrics.lat_get_us.percentile(0.99));
                w.u64(g_metrics.lat_put_us.percentile(0.99));
                w.finalize();
                resp = std::move(w.buf);
                break;
            }
            case Cmd::VOTE_REQ:
            case Cmd::VOTE_RESP:
            case Cmd::APPEND_REQ:
            case Cmd::APPEND_RESP: {
                // Route Raft RPC to the Raft engine.
                // sender_id from the header identifies which peer this came from.
                resp = raft_.handle_rpc(sender_id, cmd, payload, plen);
                break;
            }
            default: {
                LOG_WARN("unknown cmd 0x%02x from fd=%d", (unsigned)cmd, conn.fd);
                ByteWriter w;
                w.header(Cmd::ERROR, id_, seq);
                w.str("unknown command");
                w.finalize();
                resp = std::move(w.buf);
            }
        }

        // Consume the processed bytes from the read buffer.
        std::size_t consumed = sizeof(hdr) + plen;
        conn.rbuf.erase(conn.rbuf.begin(), conn.rbuf.begin() + consumed);

        if (!resp.empty()) {
            conn.queue_write(resp);
            flush_conn(conn.fd);
        }
    }

    void flush_conn(int fd) {
        auto it = conns_.find(fd);
        if (it == conns_.end()) return;
        Conn& conn = it->second;

        while (conn.wpos < conn.wbuf.size()) {
            ssize_t n = send(fd,
                             conn.wbuf.data() + conn.wpos,
                             conn.wbuf.size() - conn.wpos,
                             MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n > 0) {
                conn.wpos += (std::size_t)n;
            } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                // Arm EPOLLOUT so we resume when the socket drains.
                mod_epoll(fd, EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP);
                return;
            } else {
                close_conn(fd); return;
            }
        }
        if (conn.wpos == conn.wbuf.size()) {
            conn.wbuf.clear();
            conn.wpos = 0;
            // Disarm EPOLLOUT to avoid busy-looping when there's nothing to write.
            mod_epoll(fd, EPOLLIN | EPOLLET | EPOLLRDHUP);
        }
    }

    void close_conn(int fd) {
        auto it = conns_.find(fd);
        if (it == conns_.end()) return;
        int peer_id = it->second.peer_id;
        if (peer_id >= 0) {
            peer_fd_.erase((uint32_t)peer_id);
            LOG_WARN("peer %d connection closed (fd=%d)", peer_id, fd);
        } else {
            LOG_DEBUG("client connection closed fd=%d", fd);
        }
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        conns_.erase(it);
        g_metrics.connections_closed.fetch_add(1, std::memory_order_relaxed);
    }

    // ── Socket helpers ────────────────────────────────────────────────────────
    static int tcp_listen(uint16_t port) {
        int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) return -1;
        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(port);
        if (bind(fd, (sockaddr*)&addr, sizeof addr) < 0 || listen(fd, 4096) < 0) {
            close(fd); return -1;
        }
        return fd;
    }

    static int tcp_connect_nb(const std::string& host, uint16_t port) {
        int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) return -1;
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
        addr.sin_port = htons(port);
        int r = connect(fd, (sockaddr*)&addr, sizeof addr);
        if (r < 0 && errno != EINPROGRESS) { close(fd); return -1; }
        return fd;
    }
};

// ─────────────────────────────────────────────────────────────────────────────