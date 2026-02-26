#pragma once
#include "common.hpp"

// ─────────────────────────────────────────────────────────────────────────────
struct NodeAddr {
    std::string host;
    uint16_t    port   = 0;
    uint32_t    id     = 0;
};

class ConsistentRing {
    static constexpr int VNODES = 150;
    mutable std::mutex mtx_;
    std::vector<std::pair<uint64_t, NodeAddr>> ring_;

public:
    void add_node(const NodeAddr& n) {
        std::lock_guard lk(mtx_);
        for (int i = 0; i < VNODES; i++) {
            std::string vk = n.host + ":" + std::to_string(n.port) + "#" + std::to_string(i);
            ring_.emplace_back(wyhash::hash_str(vk), n);
        }
        std::sort(ring_.begin(), ring_.end(),
                  [](const auto& a, const auto& b){ return a.first < b.first; });
        ring_.erase(std::unique(ring_.begin(), ring_.end(),
                    [](const auto& a, const auto& b){ return a.first == b.first; }),
                    ring_.end());
    }

    void remove_node(uint32_t id) {
        std::lock_guard lk(mtx_);
        ring_.erase(std::remove_if(ring_.begin(), ring_.end(),
                    [id](const auto& e){ return e.second.id == id; }), ring_.end());
    }

    std::optional<NodeAddr> lookup(std::string_view key) const {
        std::lock_guard lk(mtx_);
        if (ring_.empty()) return std::nullopt;
        uint64_t h = wyhash::hash_str(key);
        auto it = std::lower_bound(ring_.begin(), ring_.end(), h,
                  [](const auto& e, uint64_t v){ return e.first < v; });
        if (it == ring_.end()) it = ring_.begin();
        return it->second;
    }

    std::vector<NodeAddr> replicas(std::string_view key, int n) const {
        std::lock_guard lk(mtx_);
        if (ring_.empty()) return {};
        uint64_t h = wyhash::hash_str(key);
        auto it = std::lower_bound(ring_.begin(), ring_.end(), h,
                  [](const auto& e, uint64_t v){ return e.first < v; });
        std::vector<NodeAddr> result;
        for (std::size_t i = 0; i < ring_.size() && (int)result.size() < n; i++) {
            const auto& e = ring_[(it - ring_.begin() + i) % ring_.size()];
            bool dup = false;
            for (auto& r : result) if (r.id == e.second.id) { dup = true; break; }
            if (!dup) result.push_back(e.second);
        }
        return result;
    }
};

// ─────────────────────────────────────────────────────────────────────────────