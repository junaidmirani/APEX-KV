#pragma once
#include "common.hpp"

template<typename T>
class MpscQueue {
    struct APEX_CL_ALIGNED Node {
        std::atomic<Node*> next{nullptr};
        alignas(CACHELINE) T value;
    };

    APEX_CL_ALIGNED std::atomic<Node*> head_;
    APEX_CL_ALIGNED std::atomic<Node*> tail_;

public:
    MpscQueue() {
        auto* stub = new Node{};
        head_.store(stub, std::memory_order_relaxed);
        tail_.store(stub, std::memory_order_relaxed);
    }

    ~MpscQueue() {
        while (auto* n = head_.load(std::memory_order_relaxed)) {
            auto* next = n->next.load(std::memory_order_relaxed);
            delete n;
            head_.store(next, std::memory_order_relaxed);
        }
    }

    // Wait-free: any thread can enqueue without blocking.
    void enqueue(T val) {
        auto* node    = new Node{};
        node->value   = std::move(val);
        // Release: our writes to node->value must be visible before the
        // consumer loads this node via the head pointer.
        Node* prev    = tail_.exchange(node, std::memory_order_acq_rel);
        prev->next.store(node, std::memory_order_release);
    }

    // Lock-free: safe to call from a single consumer thread only.
    std::optional<T> dequeue() noexcept {
        Node* head = head_.load(std::memory_order_relaxed);
        // Acquire: we must see the full value written by enqueue (release above).
        Node* next = head->next.load(std::memory_order_acquire);
        if (!next) return std::nullopt;
        head_.store(next, std::memory_order_relaxed);
        T val = std::move(next->value);
        delete head;
        return val;
    }
};
