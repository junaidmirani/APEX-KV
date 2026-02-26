#pragma once
#include "wal.hpp"
#include "ring.hpp"

// §15  RAFT CONSENSUS ENGINE
//
//  Full Raft: leader election, log replication, majority commit, apply.
//
//  v1 bug fixed: The send function is now a callback provided by KVNode.
//  This means:
//    (a) All network writes go through one owner (the net thread).
//    (b) RaftEngine identifies responding peers by the node_id embedded in
//        the MsgHeader.sender_id field — no fd-to-peer guessing.
//    (c) WAL is written before we ACK AppendEntries.
// ─────────────────────────────────────────────────────────────────────────────
enum class RaftRole { Follower, Candidate, Leader };

struct RaftPeer {
    NodeAddr addr;
    uint64_t next_index   = 1;
    uint64_t match_index  = 0;
    bool     vote_granted = false;
};

class RaftEngine {
public:
    // send_fn(peer_id, serialized_message) — provided by KVNode
    using SendFn  = std::function<void(uint32_t, std::vector<uint8_t>)>;
    // apply_fn(entry) — called when an entry is committed
    using ApplyFn = std::function<void(const LogEntry&)>;

    RaftEngine(uint32_t my_id, SendFn send_fn, ApplyFn apply_fn)
        : my_id_(my_id)
        , send_fn_(std::move(send_fn))
        , apply_fn_(std::move(apply_fn))
    {}

    Result<void> open_wal(const std::string& path) {
        auto r = wal_.open(path);
        if (r.is_err()) return r;
        // Replay WAL to restore log state
        return wal_.replay([this](const LogEntry& e) {
            log_.push_back(e);
            if (e.index > commit_index_) commit_index_ = e.index;
        });
    }

    void add_peer(NodeAddr addr) {
        std::lock_guard lk(mtx_);
        peers_.push_back({addr, (uint64_t)log_.size() + 1, 0, false});
    }

    void start() {
        running_.store(true, std::memory_order_release);
        raft_thread_ = std::thread([this]{ loop(); });
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        if (raft_thread_.joinable()) raft_thread_.join();
    }

    // Client submits a write.  Returns NotLeader if this node is not the leader.
    Result<void> propose(Cmd op, std::string key, std::string val) {
        std::lock_guard lk(mtx_);
        if (role_ != RaftRole::Leader) {
            g_metrics.err_not_leader.fetch_add(1, std::memory_order_relaxed);
            return Result<void>::err(Errc::NotLeader);
        }
        LogEntry e;
        e.term  = current_term_;
        e.index = log_.size() + 1;
        e.op    = op;
        e.key   = std::move(key);
        e.val   = std::move(val);
        auto wr = wal_.append(e);
        if (wr.is_err()) return wr;
        log_.push_back(e);
        for (auto& p : peers_) send_append_entries(p);
        return Result<void>::ok();
    }

    // Called by the net thread when a Raft RPC arrives from a peer.
    // Returns the serialized response (empty if no response needed).
    std::vector<uint8_t> handle_rpc(uint32_t from_node_id, Cmd cmd,
                                     const uint8_t* payload, std::size_t plen) {
        std::lock_guard lk(mtx_);
        ByteReader r(payload, plen);
        switch (cmd) {
            case Cmd::VOTE_REQ:    return handle_vote_req(r);
            case Cmd::VOTE_RESP:   handle_vote_resp(from_node_id, r); return {};
            case Cmd::APPEND_REQ:  return handle_append_req(r);
            case Cmd::APPEND_RESP: handle_append_resp(from_node_id, r); return {};
            default:               return {};
        }
    }

    bool     is_leader()     const { std::lock_guard lk(mtx_); return role_ == RaftRole::Leader; }
    uint32_t leader_id()     const { std::lock_guard lk(mtx_); return leader_id_; }
    uint64_t current_term()  const { std::lock_guard lk(mtx_); return current_term_; }

private:
    uint32_t                my_id_;
    SendFn                  send_fn_;
    ApplyFn                 apply_fn_;
    WAL                     wal_;
    mutable std::mutex      mtx_;

    RaftRole                role_           = RaftRole::Follower;
    uint64_t                current_term_   = 0;
    uint32_t                voted_for_      = 0;
    uint32_t                leader_id_      = 0;
    std::vector<LogEntry>   log_;
    uint64_t                commit_index_   = 0;
    uint64_t                last_applied_   = 0;
    std::vector<RaftPeer>   peers_;
    int                     votes_           = 0;

    std::atomic<bool>       running_{false};
    std::thread             raft_thread_;

    static constexpr int HEARTBEAT_MS    = 50;
    static constexpr int ELECTION_MIN_MS = 150;
    static constexpr int ELECTION_MAX_MS = 300;

    Clock::time_point election_deadline_ = Clock::now();

    void reset_election_timer() {
        thread_local std::mt19937 rng(std::random_device{}() ^ (uint64_t)my_id_);
        int ms = ELECTION_MIN_MS + (int)(rng() % (ELECTION_MAX_MS - ELECTION_MIN_MS));
        election_deadline_ = Clock::now() + std::chrono::milliseconds(ms);
    }

    void loop() {
        reset_election_timer();
        while (running_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            std::lock_guard lk(mtx_);
            apply_committed();
            if (role_ == RaftRole::Leader) {
                send_heartbeats();
            } else if (Clock::now() >= election_deadline_) {
                start_election();
            }
        }
    }

    void start_election() {
        role_          = RaftRole::Candidate;
        ++current_term_;
        voted_for_     = my_id_;
        votes_         = 1;
        reset_election_timer();
        g_metrics.raft_elections.fetch_add(1, std::memory_order_relaxed);

        uint64_t last_idx  = log_.empty() ? 0 : log_.back().index;
        uint64_t last_term = log_.empty() ? 0 : log_.back().term;

        LOG_INFO("raft: node %u starting election term %llu",
                 my_id_, (unsigned long long)current_term_);

        ByteWriter w;
        w.header(Cmd::VOTE_REQ, my_id_);
        w.u64(current_term_);
        w.u32(my_id_);
        w.u64(last_idx);
        w.u64(last_term);
        w.finalize();
        for (auto& p : peers_) send_fn_(p.addr.id, w.buf);
    }

    std::vector<uint8_t> handle_vote_req(ByteReader& r) {
        uint64_t term          = r.u64();
        uint32_t candidate_id  = r.u32();
        uint64_t last_log_idx  = r.u64();
        uint64_t last_log_term = r.u64();

        if (term > current_term_) {
            current_term_ = term;
            role_         = RaftRole::Follower;
            voted_for_    = 0;
            reset_election_timer();
        }

        uint64_t my_last_idx  = log_.empty() ? 0 : log_.back().index;
        uint64_t my_last_term = log_.empty() ? 0 : log_.back().term;
        bool log_ok = (last_log_term > my_last_term)
                   || (last_log_term == my_last_term && last_log_idx >= my_last_idx);
        bool grant  = (term == current_term_) && log_ok
                   && (voted_for_ == 0 || voted_for_ == candidate_id);
        if (grant) {
            voted_for_ = candidate_id;
            reset_election_timer();
        }

        ByteWriter w;
        w.header(Cmd::VOTE_RESP, my_id_);
        w.u64(current_term_);
        w.u8(grant ? 1 : 0);
        w.finalize();
        return w.buf;
    }

    void handle_vote_resp(uint32_t from_id, ByteReader& r) {
        uint64_t term    = r.u64();
        bool     granted = r.u8() != 0;

        if (term > current_term_) {
            current_term_ = term;
            role_ = RaftRole::Follower;
            return;
        }
        if (role_ != RaftRole::Candidate || term != current_term_) return;
        if (granted) {
            ++votes_;
            int quorum = (int)(peers_.size() + 1) / 2 + 1;
            if (votes_ >= quorum) become_leader();
        }
        (void)from_id;
    }

    void become_leader() {
        role_      = RaftRole::Leader;
        leader_id_ = my_id_;
        for (auto& p : peers_) {
            p.next_index  = log_.size() + 1;
            p.match_index = 0;
        }
        LOG_INFO("raft: node %u became LEADER term %llu",
                 my_id_, (unsigned long long)current_term_);
        send_heartbeats();
    }

    void send_heartbeats() {
        for (auto& p : peers_) send_append_entries(p);
    }

    void send_append_entries(RaftPeer& peer) {
        uint64_t prev_idx  = peer.next_index - 1;
        uint64_t prev_term = (prev_idx > 0 && prev_idx <= log_.size())
                             ? log_[prev_idx - 1].term : 0;

        ByteWriter w;
        w.header(Cmd::APPEND_REQ, my_id_);
        w.u64(current_term_);
        w.u32(my_id_);
        w.u64(prev_idx);
        w.u64(prev_term);
        w.u64(commit_index_);

        // Count entries to send
        uint32_t n_entries = 0;
        for (uint64_t i = peer.next_index; i <= (uint64_t)log_.size(); i++) ++n_entries;
        w.u32(n_entries);
        for (uint64_t i = peer.next_index; i <= (uint64_t)log_.size(); i++) {
            const auto& e = log_[i - 1];
            w.u64(e.term); w.u64(e.index);
            w.u8(static_cast<uint8_t>(e.op));
            w.str(e.key);  w.str(e.val);
        }
        w.finalize();
        send_fn_(peer.addr.id, w.buf);
    }

    std::vector<uint8_t> handle_append_req(ByteReader& r) {
        uint64_t term       = r.u64();
        uint32_t leader     = r.u32();
        uint64_t prev_idx   = r.u64();
        uint64_t prev_term  = r.u64();
        uint64_t commit     = r.u64();
        uint32_t n_entries  = r.u32();

        bool success = false;

        if (term >= current_term_) {
            if (term > current_term_ || role_ != RaftRole::Follower) {
                current_term_ = term;
                role_         = RaftRole::Follower;
                voted_for_    = 0;
            }
            leader_id_ = leader;
            reset_election_timer();

            bool log_matches = (prev_idx == 0)
                || (prev_idx <= log_.size() && log_[prev_idx - 1].term == prev_term);

            if (log_matches) {
                success = true;
                for (uint32_t i = 0; i < n_entries; i++) {
                    LogEntry e;
                    e.term  = r.u64(); e.index = r.u64();
                    e.op    = static_cast<Cmd>(r.u8());
                    e.key   = r.str(); e.val = r.str();

                    if (e.index <= log_.size()) {
                        if (log_[e.index - 1].term == e.term) continue;
                        // Conflict: truncate and rewrite
                        log_.resize(e.index - 1);
                    }
                    // Persist to WAL before appending to in-memory log
                    if (wal_.append(e).is_err()) { success = false; break; }
                    log_.push_back(e);
                }
                if (commit > commit_index_)
                    commit_index_ = std::min(commit, (uint64_t)log_.size());
                apply_committed();
            }
        }

        ByteWriter w;
        w.header(Cmd::APPEND_RESP, my_id_);
        w.u64(current_term_);
        w.u8(success ? 1 : 0);
        w.u64(log_.size()); // hint: our last stored index
        w.finalize();
        return w.buf;
    }

    // CRITICAL: from_node_id lets us credit the correct peer.
    // v1 iterated ALL peers and credited all of them — completely wrong.
    void handle_append_resp(uint32_t from_node_id, ByteReader& r) {
        uint64_t term      = r.u64();
        bool     success   = r.u8() != 0;
        uint64_t match_idx = r.u64();

        if (term > current_term_) {
            current_term_ = term;
            role_ = RaftRole::Follower;
            return;
        }
        if (role_ != RaftRole::Leader) return;

        // Find the specific peer that responded.
        RaftPeer* peer = nullptr;
        for (auto& p : peers_) {
            if (p.addr.id == from_node_id) { peer = &p; break; }
        }
        if (!peer) {
            LOG_WARN("raft: response from unknown node %u", from_node_id);
            return;
        }

        if (success) {
            peer->match_index = std::max(peer->match_index, match_idx);
            peer->next_index  = peer->match_index + 1;
            advance_commit();
        } else {
            // Follower rejected: back off next_index by one and retry.
            // A production system uses the conflict term hint for faster catch-up.
            if (peer->next_index > 1) --peer->next_index;
            send_append_entries(*peer);
        }
    }

    void advance_commit() {
        // Find the highest N such that:
        //   log[N].term == current_term  AND
        //   at least (quorum) peers have match_index >= N
        for (uint64_t n = log_.size(); n > commit_index_; n--) {
            if (log_[n - 1].term != current_term_) continue;
            int count = 1; // self always has this entry
            for (auto& p : peers_) if (p.match_index >= n) ++count;
            int quorum = (int)(peers_.size() + 1) / 2 + 1;
            if (count >= quorum) {
                commit_index_ = n;
                g_metrics.raft_commits.fetch_add(n - last_applied_, std::memory_order_relaxed);
                apply_committed();
                break;
            }
        }
    }

    void apply_committed() {
        while (last_applied_ < commit_index_) {
            ++last_applied_;
            apply_fn_(log_[last_applied_ - 1]);
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────