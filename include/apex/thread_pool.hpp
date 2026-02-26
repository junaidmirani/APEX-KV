#pragma once
#include "common.hpp"

class ThreadPool {
public:
    using Task = std::function<void()>;

    explicit ThreadPool(int n = 0) {
        int count = n > 0 ? n : (int)std::thread::hardware_concurrency();
        queues_.resize(count);
        for (int i = 0; i < count; i++) queues_[i] = std::make_unique<WorkQueue>();
        for (int i = 0; i < count; i++) {
            workers_.emplace_back([this, i]{ loop(i); });
            cpu_set_t cs; CPU_ZERO(&cs); CPU_SET(i % count, &cs);
            pthread_setaffinity_np(workers_.back().native_handle(), sizeof cs, &cs);
        }
        n_ = count;
    }

    ~ThreadPool() {
        stop_.store(true, std::memory_order_release);
        for (auto& q : queues_) { std::lock_guard lk(q->mtx); q->cv.notify_all(); }
        for (auto& t : workers_) if (t.joinable()) t.join();
    }

    void submit(Task t) {
        int idx = next_.fetch_add(1, std::memory_order_relaxed) % n_;
        queues_[idx]->push(std::move(t));
    }

    int size() const noexcept { return n_; }

private:
    struct WorkQueue {
        std::deque<Task>        tasks;
        std::mutex              mtx;
        std::condition_variable cv;

        void push(Task t) {
            { std::lock_guard lk(mtx); tasks.push_back(std::move(t)); }
            cv.notify_one();
        }
        bool try_steal(Task& out) {
            std::unique_lock lk(mtx, std::try_to_lock);
            if (!lk || tasks.empty()) return false;
            out = std::move(tasks.front()); tasks.pop_front(); return true;
        }
        bool try_pop(Task& out) {
            std::unique_lock lk(mtx, std::try_to_lock);
            if (!lk || tasks.empty()) return false;
            out = std::move(tasks.back()); tasks.pop_back(); return true;
        }
        bool wait_pop(Task& out, const std::atomic<bool>& stop) {
            std::unique_lock lk(mtx);
            cv.wait(lk, [&]{ return !tasks.empty() || stop.load(std::memory_order_relaxed); });
            if (tasks.empty()) return false;
            out = std::move(tasks.back()); tasks.pop_back(); return true;
        }
    };

    void loop(int id) {
        thread_local std::mt19937 rng(std::random_device{}() ^ uint64_t(id));
        Task t;
        while (!stop_.load(std::memory_order_relaxed)) {
            if (queues_[id]->try_pop(t))  { t(); continue; }
            bool stole = false;
            for (int a = 0; a < n_; a++) {
                int v = rng() % n_;
                if (v != id && queues_[v]->try_steal(t)) { t(); stole = true; break; }
            }
            if (!stole) {
                if (queues_[id]->wait_pop(t, stop_) && t) t();
            }
        }
    }

    int                                     n_{0};
    std::atomic<int>                        next_{0};
    std::atomic<bool>                       stop_{false};
    std::vector<std::unique_ptr<WorkQueue>> queues_;
    std::vector<std::thread>                workers_;
};
