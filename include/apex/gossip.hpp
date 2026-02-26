#pragma once
#include "common.hpp"
#include "ring.hpp"
#include "protocol.hpp"

// §16  SWIM GOSSIP PROTOCOL
//
//  v1 bug fixed: the ACK response is now actually sent back to the pinger.
//  Previously the response was constructed in a local ByteWriter and then
//  silently discarded — the pinger never received any ACK, so every node
//  would eventually be marked Dead.
// ─────────────────────────────────────────────────────────────────────────────
enum class NodeStatus : uint8_t { Alive = 0, Suspected = 1, Dead = 2 };

struct MemberInfo {
    NodeAddr              addr;
    NodeStatus            status      = NodeStatus::Alive;
    uint64_t              incarnation = 0;
    Clock::time_point     last_seen;
};

class GossipProtocol {
    static constexpr int FANOUT           = 3;
    static constexpr int PING_INTERVAL_MS = 200;
    static constexpr int SUSPECT_MS       = 1000;
    static constexpr int DEAD_MS          = 5000;

    uint32_t           my_id_;
    std::string        my_host_;
    uint16_t           my_port_;
    uint64_t           my_incarnation_ = 0;
    int                udp_fd_         = -1;

    mutable std::mutex mtx_;
    std::unordered_map<uint32_t, MemberInfo> members_;

    std::atomic<bool>  running_{false};
    std::thread        thread_;

public:
    GossipProtocol(uint32_t id, std::string host, uint16_t port)
        : my_id_(id), my_host_(std::move(host)), my_port_(port) {}

    ~GossipProtocol() { stop(); }

    void add_peer(const NodeAddr& addr) {
        std::lock_guard lk(mtx_);
        MemberInfo m;
        m.addr     = addr;
        m.status   = NodeStatus::Alive;
        m.last_seen= Clock::now();
        members_[addr.id] = m;
    }

    void start() {
        udp_fd_ = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (udp_fd_ < 0) { LOG_ERROR("gossip: udp socket: %s", strerror(errno)); return; }

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(my_port_ + 1000); // gossip port = kv_port + 1000
        if (bind(udp_fd_, (sockaddr*)&addr, sizeof addr) < 0) {
            LOG_ERROR("gossip: bind port %u: %s", my_port_ + 1000, strerror(errno));
        }

        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this]{ loop(); });
        LOG_INFO("gossip: started on port %u", my_port_ + 1000);
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
        if (udp_fd_ >= 0) { close(udp_fd_); udp_fd_ = -1; }
    }

    // Called from the gossip thread to drain incoming UDP packets.
    void recv_packets() {
        if (udp_fd_ < 0) return;
        uint8_t buf[4096];
        sockaddr_in from{};
        socklen_t   from_len = sizeof from;
        for (;;) {
            ssize_t n = recvfrom(udp_fd_, buf, sizeof buf, MSG_DONTWAIT,
                                 (sockaddr*)&from, &from_len);
            if (n <= 0) break;
            handle_packet(buf, (std::size_t)n, from);
        }
    }

    std::vector<MemberInfo> all_members() const {
        std::lock_guard lk(mtx_);
        std::vector<MemberInfo> v;
        v.reserve(members_.size());
        for (auto& [id, m] : members_) v.push_back(m);
        return v;
    }

private:
    void loop() {
        std::mt19937 rng(std::random_device{}());
        while (running_.load(std::memory_order_relaxed)) {
            recv_packets();
            tick(rng);
            std::this_thread::sleep_for(std::chrono::milliseconds(PING_INTERVAL_MS));
        }
    }

    void tick(std::mt19937& rng) {
        std::lock_guard lk(mtx_);
        auto now = Clock::now();

        // Update alive/suspect/dead state machine
        uint64_t alive = 0, suspect = 0, dead = 0;
        for (auto& [id, m] : members_) {
            auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - m.last_seen).count();
            if (m.status == NodeStatus::Alive && age_ms > SUSPECT_MS) {
                m.status = NodeStatus::Suspected;
                LOG_WARN("gossip: node %u suspected (silent %lldms)", id, (long long)age_ms);
            } else if (m.status == NodeStatus::Suspected && age_ms > DEAD_MS) {
                m.status = NodeStatus::Dead;
                LOG_WARN("gossip: node %u declared DEAD (silent %lldms)", id, (long long)age_ms);
            }
            if (m.status == NodeStatus::Alive)     ++alive;
            if (m.status == NodeStatus::Suspected) ++suspect;
            if (m.status == NodeStatus::Dead)      ++dead;
        }
        // Relaxed: metrics are advisory
        g_metrics.gossip_alive.store(alive,   std::memory_order_relaxed);
        g_metrics.gossip_suspect.store(suspect,std::memory_order_relaxed);
        g_metrics.gossip_dead.store(dead,     std::memory_order_relaxed);

        // Pick FANOUT random alive peers and ping them
        std::vector<uint32_t> alive_ids;
        for (auto& [id, m] : members_)
            if (m.status == NodeStatus::Alive) alive_ids.push_back(id);
        std::shuffle(alive_ids.begin(), alive_ids.end(), rng);

        int count = std::min(FANOUT, (int)alive_ids.size());
        for (int i = 0; i < count; i++) send_ping(members_[alive_ids[i]]);
    }

    void send_ping(const MemberInfo& target) {
        ByteWriter w;
        w.u8(static_cast<uint8_t>(Cmd::GOSSIP_PING));
        w.u32(my_id_);
        w.u64(my_incarnation_);
        // Piggyback up to FANOUT*2 member states for dissemination
        int n = 0;
        for (auto& [id, m] : members_) {
            if (n >= FANOUT * 2) break;
            w.u32(id);
            w.u8(static_cast<uint8_t>(m.status));
            w.u64(m.incarnation);
            ++n;
        }
        w.u32(0xFFFFFFFFu); // end sentinel

        sockaddr_in to{};
        to.sin_family = AF_INET;
        inet_pton(AF_INET, target.addr.host.c_str(), &to.sin_addr);
        to.sin_port = htons(target.addr.port + 1000);

        sendto(udp_fd_, w.buf.data(), w.buf.size(), MSG_DONTWAIT,
               (sockaddr*)&to, sizeof to);
    }

    // v1 bug: ACK was built locally and then silently dropped.
    // Fix: sendto() back to the sender address we received from.
    void handle_packet(const uint8_t* data, std::size_t len, const sockaddr_in& from) {
        std::lock_guard lk(mtx_);
        ByteReader r(data, len);
        auto cmd = static_cast<Cmd>(r.u8());
        uint32_t sender_id   = r.u32();
        uint64_t incarnation = r.u64();

        if (cmd == Cmd::GOSSIP_PING) {
            // Update sender state
            if (members_.count(sender_id)) {
                auto& m = members_[sender_id];
                if (incarnation >= m.incarnation) {
                    m.incarnation = incarnation;
                    m.status      = NodeStatus::Alive;
                    m.last_seen   = Clock::now();
                }
            }
            merge_members(r);

            // Send ACK — this is the fix.  Build the response and sendto()
            // the sender's address immediately.
            ByteWriter ack;
            ack.u8(static_cast<uint8_t>(Cmd::GOSSIP_ACK));
            ack.u32(my_id_);
            ack.u64(my_incarnation_);
            // Piggyback our own membership view
            int n = 0;
            for (auto& [id, m] : members_) {
                if (n >= FANOUT * 2) break;
                ack.u32(id);
                ack.u8(static_cast<uint8_t>(m.status));
                ack.u64(m.incarnation);
                ++n;
            }
            ack.u32(0xFFFFFFFFu);

            sendto(udp_fd_, ack.buf.data(), ack.buf.size(), MSG_DONTWAIT,
                   (const sockaddr*)&from, sizeof from);

        } else if (cmd == Cmd::GOSSIP_ACK) {
            if (members_.count(sender_id)) {
                auto& m = members_[sender_id];
                if (incarnation >= m.incarnation) {
                    m.incarnation = incarnation;
                    m.status      = NodeStatus::Alive;
                    m.last_seen   = Clock::now();
                }
            }
            merge_members(r);
        }
    }

    void merge_members(ByteReader& r) {
        while (!r.eof()) {
            uint32_t id = r.u32();
            if (id == 0xFFFFFFFFu) break;
            auto     status = static_cast<NodeStatus>(r.u8());
            uint64_t inc    = r.u64();
            if (id == my_id_) {
                // Someone thinks we're dead — bump incarnation to refute it
                if (status == NodeStatus::Dead && inc >= my_incarnation_)
                    ++my_incarnation_;
                continue;
            }
            if (members_.count(id) && inc > members_[id].incarnation) {
                members_[id].incarnation = inc;
                members_[id].status      = status;
                if (status == NodeStatus::Alive)
                    members_[id].last_seen = Clock::now();
            }
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────