#pragma once
#include "allocator.hpp"

class RobinHoodMap {
public:
    static constexpr uint64_t EMPTY    = 0;
    static constexpr uint32_t MAX_PSL  = 128;
    static constexpr double   MAX_LOAD = 0.75;

    struct alignas(32) Slot {
        uint64_t hash  = EMPTY;
        uint16_t psl   = 0;
        uint16_t klen  = 0;
        uint32_t vlen  = 0;
        char*    key   = nullptr;
        char*    val   = nullptr;
    };
    static_assert(sizeof(Slot) == 32, "Slot layout changed — re-verify cache line packing");

    explicit RobinHoodMap(std::size_t initial_cap = 1 << 16)
        : cap_(next_pow2(initial_cap))
        , slots_(alloc_slots(cap_))
        , mask_(cap_ - 1)
        , size_(0)
        , grow_at_(static_cast<std::size_t>(cap_ * MAX_LOAD))
    {}

    ~RobinHoodMap() {
        for (std::size_t i = 0; i < cap_; i++)
            if (slots_[i].hash != EMPTY) free_kv(slots_[i]);
        ::free(slots_);
    }

    RobinHoodMap(const RobinHoodMap&)            = delete;
    RobinHoodMap& operator=(const RobinHoodMap&) = delete;
    RobinHoodMap(RobinHoodMap&&)                 = delete;

    APEX_HOT bool get(std::string_view key, std::string& out) const noexcept {
        const uint64_t h = hash_key(key);
        std::size_t    i = h & mask_;
        uint16_t     psl = 0;
        for (;;) {
            prefetch_r(&slots_[(i + 4) & mask_]);
            const Slot& s = slots_[i];
            // Invariant: if our PSL exceeds the slot's PSL, the key can't
            // exist further in the probe sequence (Robin Hood property).
            if (APEX_UNLIKELY(s.hash == EMPTY || psl > s.psl)) return false;
            if (s.hash == h && s.klen == (uint16_t)key.size()
                && std::memcmp(s.key, key.data(), key.size()) == 0) {
                out.assign(s.val, s.vlen);
                return true;
            }
            i = (i + 1) & mask_;
            ++psl;
        }
    }

    APEX_HOT void put(std::string_view key, std::string_view val) {
        if (APEX_UNLIKELY(size_ >= grow_at_)) rehash(cap_ * 2);

        const uint64_t h = hash_key(key);

        // Fast path: update value of existing key.
        {
            std::size_t i = h & mask_;
            uint16_t  psl = 0;
            for (;;) {
                Slot& s = slots_[i];
                if (s.hash == EMPTY || psl > s.psl) break;
                if (s.hash == h && s.klen == (uint16_t)key.size()
                    && std::memcmp(s.key, key.data(), key.size()) == 0) {
                    char* nv = dup_str(val.data(), val.size());
                    g_slab.dealloc(s.val, s.vlen + 1);
                    s.val  = nv;
                    s.vlen = static_cast<uint32_t>(val.size());
                    return;
                }
                i = (i + 1) & mask_;
                ++psl;
            }
        }

        // Insert new entry using Robin Hood displacement.
        Slot ins;
        ins.hash = h;
        ins.psl  = 0;
        ins.klen = static_cast<uint16_t>(key.size());
        ins.vlen = static_cast<uint32_t>(val.size());
        ins.key  = dup_str(key.data(), key.size());
        ins.val  = dup_str(val.data(), val.size());

        std::size_t i = h & mask_;
        for (;;) {
            Slot& s = slots_[i];
            if (s.hash == EMPTY) { s = ins; ++size_; return; }
            if (ins.psl > s.psl) std::swap(ins, s); // steal from the "rich"
            i = (i + 1) & mask_;
            ++ins.psl;
            if (APEX_UNLIKELY(ins.psl > MAX_PSL)) {
                // Pathological clustering: force rehash, then retry.
                // This should never happen at load < 0.75 with a good hash.
                Slot displaced = ins;
                rehash(cap_ * 2);
                put(std::string_view(displaced.key, displaced.klen),
                    std::string_view(displaced.val, displaced.vlen));
                free_kv(displaced);
                return;
            }
        }
    }

    APEX_HOT bool del(std::string_view key) noexcept {
        const uint64_t h = hash_key(key);
        std::size_t    i = h & mask_;
        uint16_t     psl = 0;
        for (;;) {
            Slot& s = slots_[i];
            if (s.hash == EMPTY || psl > s.psl) return false;
            if (s.hash == h && s.klen == (uint16_t)key.size()
                && std::memcmp(s.key, key.data(), key.size()) == 0) {
                free_kv(s);
                // Backward shift: move subsequent entries one slot back as
                // long as they have PSL > 0.  This restores the Robin Hood
                // invariant without leaving tombstones.
                for (;;) {
                    std::size_t j    = (i + 1) & mask_;
                    Slot&       next = slots_[j];
                    if (next.hash == EMPTY || next.psl == 0) {
                        slots_[i] = Slot{};
                        break;
                    }
                    slots_[i]      = next;
                    slots_[i].psl -= 1;
                    i              = j;
                }
                --size_;
                return true;
            }
            i = (i + 1) & mask_;
            ++psl;
        }
    }

    std::size_t size()     const noexcept { return size_; }
    std::size_t capacity() const noexcept { return cap_;  }

    template<typename Fn>
    void for_each(Fn&& fn) const {
        for (std::size_t i = 0; i < cap_; i++) {
            const Slot& s = slots_[i];
            if (s.hash != EMPTY)
                fn(std::string_view(s.key, s.klen),
                   std::string_view(s.val, s.vlen));
        }
    }

private:
    std::size_t  cap_;
    Slot*        slots_;
    std::size_t  mask_;
    std::size_t  size_;
    std::size_t  grow_at_;

    static Slot* alloc_slots(std::size_t cap) {
        void* p = aligned_alloc(CACHELINE, cap * sizeof(Slot));
        if (!p) throw std::bad_alloc{};
        std::memset(p, 0, cap * sizeof(Slot));
        return static_cast<Slot*>(p);
    }

    APEX_PURE APEX_INLINE static uint64_t hash_key(std::string_view k) noexcept {
        uint64_t h = wyhash::hash_str(k);
        // 0 is the EMPTY sentinel; remap it to 1.
        return h == EMPTY ? 1 : h;
    }

    APEX_INLINE static char* dup_str(const char* s, std::size_t n) noexcept {
        char* p = static_cast<char*>(g_slab.alloc(n + 1));
        if (p) { std::memcpy(p, s, n); p[n] = '\0'; }
        return p;
    }

    static void free_kv(Slot& s) noexcept {
        if (s.key) { g_slab.dealloc(s.key, s.klen + 1); s.key = nullptr; }
        if (s.val) { g_slab.dealloc(s.val, s.vlen + 1); s.val = nullptr; }
    }

    APEX_NOINLINE void rehash(std::size_t new_cap) {
        new_cap = next_pow2(new_cap);
        Slot*       old   = slots_;
        std::size_t oldcap = cap_;
        slots_   = alloc_slots(new_cap);
        cap_     = new_cap;
        mask_    = new_cap - 1;
        grow_at_ = static_cast<std::size_t>(new_cap * MAX_LOAD);
        size_    = 0;
        for (std::size_t i = 0; i < oldcap; i++) {
            Slot& s = old[i];
            if (s.hash != EMPTY) {
                put(std::string_view(s.key, s.klen),
                    std::string_view(s.val, s.vlen));
                free_kv(s);
            }
        }
        ::free(old);
        LOG_DEBUG("hashmap rehash cap=%zu", new_cap);
    }

    APEX_PURE static std::size_t next_pow2(std::size_t n) noexcept {
        if (n <= 1) return 1;
        --n;
        n |= n >> 1; n |= n >> 2; n |= n >> 4;
        n |= n >> 8; n |= n >> 16; n |= n >> 32;
        return ++n;
    }
};
