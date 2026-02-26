#pragma once
#include "common.hpp"

class SlabAllocator {
public:
    static constexpr int  NUM_CLASSES  = 9;
    static constexpr int  SLAB_SIZE    = 64 * 4096;  // 256 KiB per slab

    struct FreeNode { FreeNode* next; };

    struct APEX_CL_ALIGNED Class {
        std::atomic<FreeNode*> head{nullptr};
        uint32_t               obj_size{0};
        // Pad to a full cache line so adjacent class heads don't share a line.
        APEX_CL_PAD(0, sizeof(std::atomic<FreeNode*>) + sizeof(uint32_t));
    };

    SlabAllocator() noexcept {
        static const uint32_t sizes[NUM_CLASSES] = {16,32,64,128,256,512,1024,2048,4096};
        for (int i = 0; i < NUM_CLASSES; i++) classes_[i].obj_size = sizes[i];
    }

    APEX_HOT APEX_INLINE void* alloc(std::size_t n) noexcept {
        int cls = size_class(n);
        if (APEX_UNLIKELY(cls < 0)) return std::malloc(n);
        return pop_free(cls);
    }

    APEX_HOT APEX_INLINE void dealloc(void* p, std::size_t n) noexcept {
        if (APEX_UNLIKELY(!p)) return;
        int cls = size_class(n);
        if (APEX_UNLIKELY(cls < 0)) { std::free(p); return; }
        push_free(cls, p);
    }

    template<typename T, typename... Args>
    T* construct(Args&&... args) {
        void* p = alloc(sizeof(T));
        if (APEX_UNLIKELY(!p)) return nullptr;
        return ::new(p) T(std::forward<Args>(args)...);
    }

    template<typename T>
    void destroy(T* p) noexcept {
        if (!p) return;
        p->~T();
        dealloc(p, sizeof(T));
    }

private:
    Class classes_[NUM_CLASSES];

    APEX_PURE APEX_INLINE int size_class(std::size_t n) noexcept {
        if (APEX_UNLIKELY(n == 0 || n > 4096)) return -1;
        // Round up to next power-of-two, compute log2, subtract log2(16)=4.
        n = (n <= 16) ? 16 : (std::size_t(1) << (64 - __builtin_clzll(n - 1)));
        return __builtin_ctzll(n) - 4;
    }

    APEX_NOINLINE APEX_COLD void* refill(int cls) noexcept {
        uint32_t obj_sz = classes_[cls].obj_size;
        void* slab = mmap(nullptr, SLAB_SIZE, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (APEX_UNLIKELY(slab == MAP_FAILED)) {
            LOG_ERROR("slab mmap failed: %s", strerror(errno));
            return nullptr;
        }
        madvise(slab, SLAB_SIZE, MADV_HUGEPAGE);

        // Object 0 is returned directly.  Objects 1..N-1 are linked onto
        // the free list in a single batch push (one CAS, not N).
        char*    base       = static_cast<char*>(slab);
        std::size_t count   = SLAB_SIZE / obj_sz;
        FreeNode* batch_head = nullptr;
        FreeNode* batch_tail = nullptr;
        for (std::size_t i = count - 1; i >= 1; i--) {
            auto* node = reinterpret_cast<FreeNode*>(base + i * obj_sz);
            node->next  = batch_head;
            batch_head  = node;
            if (!batch_tail) batch_tail = node;
        }
        if (batch_head) {
            // Release: the freshly initialized nodes must be visible to other
            // threads that subsequently acquire-load the head pointer.
            FreeNode* expected = classes_[cls].head.load(std::memory_order_relaxed);
            do { batch_tail->next = expected; }
            while (!classes_[cls].head.compare_exchange_weak(
                       expected, batch_head,
                       std::memory_order_release,
                       std::memory_order_relaxed));
        }
        return base;
    }

    APEX_HOT APEX_INLINE void* pop_free(int cls) noexcept {
        // Acquire: we must see the initialization writes done by the thread
        // that pushed this node (via release in push_free / refill).
        FreeNode* node = classes_[cls].head.load(std::memory_order_acquire);
        while (true) {
            if (APEX_UNLIKELY(!node)) return refill(cls);
            prefetch_r(node->next);
            if (classes_[cls].head.compare_exchange_weak(
                    node, node->next,
                    std::memory_order_acquire,   // success: acquire next node
                    std::memory_order_relaxed))  // failure: just re-read
                return node;
        }
    }

    APEX_HOT APEX_INLINE void push_free(int cls, void* p) noexcept {
        auto* node = static_cast<FreeNode*>(p);
        // Release: our writes to the node content must be visible before the
        // next thread pops this node and reads its content.
        FreeNode* head = classes_[cls].head.load(std::memory_order_relaxed);
        do { node->next = head; }
        while (!classes_[cls].head.compare_exchange_weak(
                   head, node,
                   std::memory_order_release,
                   std::memory_order_relaxed));
    }
};

static SlabAllocator g_slab;
