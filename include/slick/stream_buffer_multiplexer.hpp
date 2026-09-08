/********************************************************************************
 * Copyright (c) 2026 Slick Quant LLC
 * All rights reserved
 *
 * This file is part of the slick-stream-buffer-multiplexer. Redistribution and
 * use in source and binary forms, with or without modification, are permitted
 * exclusively under the terms of the MIT license which is available at
 * https://github.com/SlickQuant/slick-stream-buffer-multiplexer/blob/main/LICENSE
 *
 ********************************************************************************/

#pragma once

#include <slick/stream_buffer.hpp>
#include <slick/queue.hpp>

#include <atomic>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION was replaced by the Traits parameter of
// the class (see slick::queue_traits). It gated a std::atomic member, so sizeof() differed
// across a -DNDEBUG boundary in a header-only class: two translation units that disagreed shared
// one definition and silently violated the ODR. A template argument is part of the mangled name,
// so the same disagreement now surfaces as an ordinary link error instead. Warn rather than error
// so upgrading does not break a build that still passes -D; the macro itself has no effect.
#define SLICK_STREAM_BUFFER_MULTIPLEXER_STR_(x) #x
#define SLICK_STREAM_BUFFER_MULTIPLEXER_STR(x) SLICK_STREAM_BUFFER_MULTIPLEXER_STR_(x)
#if defined(_MSC_VER)
// The __FILE__(__LINE__): prefix makes the warning clickable in the VS problem list.
#define SLICK_STREAM_BUFFER_MULTIPLEXER_DEPRECATED_MACRO(msg) \
    __pragma(message(__FILE__ "(" SLICK_STREAM_BUFFER_MULTIPLEXER_STR(__LINE__) "): warning: " msg))
#else
#define SLICK_STREAM_BUFFER_MULTIPLEXER_DEPRECATED_MACRO(msg) \
    _Pragma(SLICK_STREAM_BUFFER_MULTIPLEXER_STR(GCC warning msg))
#endif

#ifdef SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION
SLICK_STREAM_BUFFER_MULTIPLEXER_DEPRECATED_MACRO(
    "SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION is ignored; it was replaced by "
    "slick::queue_traits::enable_loss_detection. See README.")
#endif

namespace slick {

/**
 * @brief Shared-queue element: identifies one published message by producer + sequence.
 *
 * At namespace scope rather than nested in the multiplexer so that it stays one type across every
 * Traits configuration - two differently-configured multiplexers must agree on the element type of
 * a shared record queue they both map.
 */
struct multiplexer_record {
    uint64_t sequence;
    uint32_t producer_id;
    uint32_t pad0 = 0;
};
static_assert(sizeof(multiplexer_record) == 16, "multiplexer_record must be 16 bytes");

/// A dereferenced message as returned by read(). Evaluates to false if no message was available.
struct multiplex_record {
    const uint8_t* data = nullptr;
    uint32_t length = 0;
    uint32_t producer_id = 0;
    explicit operator bool() const noexcept { return data != nullptr; }
};

// The multiplexer is configured by slick::queue_traits - the traits of the shared record queue it
// is built on - rather than by a traits type of its own. Every knob it has beyond that queue is a
// passthrough, and its one real tunable asks the same question queue_traits::enable_loss_detection
// already asks, so a separate flag would only let the two disagree. That matters because
// loss_count() SUMS the two counters: a half-configured pair would return a partial total that
// reads like a complete one. One flag, one number.
//
//     slick::basic_stream_buffer_multiplexer<slick::debug_queue_traits> mux(1024);  // counting
//
// slick::default_queue_traits - what the plain stream_buffer_multiplexer name uses - follows
// NDEBUG, so name a traits struct explicitly to pin the configuration across build types.

namespace detail {

// Deliberately not std::hardware_destructive_interference_size: its value tracks -mtune and -mcpu,
// so two translation units built with different flags would disagree about alignof and the member
// offsets of a header-only class - the very ODR trap this release removes elsewhere. Named
// distinctly rather than reusing slick-queue's slick::detail::cacheline_size, because two
// definitions of one name in the shared slick::detail namespace would clash.
#if defined(__APPLE__) && defined(__aarch64__)
inline constexpr std::size_t multiplexer_cacheline_size = 128;  // Apple silicon reports 128
#else
inline constexpr std::size_t multiplexer_cacheline_size = 64;
#endif

// dereference() builds a fresh cursor pointing straight at one record's own sequence and then
// checks the exact sequence it got back, so neither read_traits knob earns its cost here:
//
// - detect_reset rewinds a cursor to 0 on seq >= next_seq_, for a consumer whose cursor outlived a
//   reset(). This cursor is one statement old and outlives nothing. The rewind can only make
//   read() return record 0, which the sequence check then rejects anyway - so it buys an acquire
//   load of next_seq_ per dereference and no accuracy.
// - count_loss adds (seq - cursor) when a slot holds a newer record. For a sequential scan that is
//   the number of records skipped; for a jump-read it is "how far the producer has run past this
//   one record", which every later lapped dereference would add all over again. The multiplexer
//   counts one loss per lapped record itself, which is the number that means something.
struct dereference_read_traits : slick::read_traits {
    static constexpr bool detect_reset = false;
    static constexpr bool count_loss = false;
};

}  // namespace detail

/**
 * @brief A lock-free multi-producer multi-consumer byte stream multiplexer.
 *
 * Each producer owns its own slick::stream_buffer (an independently-sized data
 * ring with its own control ring), giving per-producer lap/loss detection and
 * literal slick::stream_buffer& compatibility (e.g. for slick::dynamic_buffer)
 * via producer_buffer::stream_buffer(), or shared ownership (e.g. for slick-net's
 * websocket_session) via producer_buffer::stream_buffer_ptr() and
 * get_producer_buffer(). consume() additionally publishes a small
 * {sequence, producer_id} record into one shared slick::queue<multiplexer_record>,
 * which acts as the lock-free MPMC fan-in / global ordering point.
 *
 * Consumers call read(cursor), which dequeues the next shared-queue record and
 * dereferences it back into the matching producer's stream_buffer, returning a
 * zero-copy (data, length, producer_id) view.
 *
 * Producers and the shared queue each independently choose local memory or
 * shared memory (IPC): see add_producer() overloads and the constructors below.
 * A cross-process consumer only needs to register (via add_producer) the
 * producer ids whose shared memory it has access to; records referencing other
 * producer ids are silently skipped (not counted as loss - see loss_count()).
 *
 * @tparam Traits Compile-time feature configuration, see slick::queue_traits.
 *
 * Caveats:
 * - add_producer() is single-threaded setup: call it before any producer or
 *   consumer threads start.
 * - Each producer_buffer's producer-side methods (prepare/commit/consume/...)
 *   must be called from a single thread, same as slick::stream_buffer.
 * - loss_count() sums two counters, both switched by Traits::enable_loss_detection:
 *   the shared queue's own wrap loss and this instance's multiplexer-level loss.
 *   A third, each producer_buffer's inner-ring loss, is separate and the
 *   multiplexer's own reads never touch it - it only moves for reads the caller
 *   makes through producer_buffer::stream_buffer().read<counting_traits>().
 *   Entries whose producer_id is unregistered on this instance are never counted
 *   as loss.
 * - A single message (one consume() call) is limited to < 4 GiB; consume() throws
 *   std::length_error rather than truncating one that is not.
 */
template<queue_traits_type Traits = default_queue_traits>
class basic_stream_buffer_multiplexer {
public:
    using traits = Traits;
    /// Shared-queue element: identifies one published message by producer + sequence.
    using record = slick::multiplexer_record;
    /// A dereferenced message as returned by read().
    using multiplex_record = slick::multiplex_record;
    /// The shared MPMC fan-in queue type - the same Traits, passed straight through.
    using shared_queue_type = slick::queue<record, Traits>;

    /// Per-producer handle: wraps one slick::stream_buffer and publishes into the shared record queue.
    class producer_buffer {
    public:
        using published_record = slick::stream_buffer::published_record;

        producer_buffer(producer_buffer&&) noexcept = default;
        producer_buffer& operator=(producer_buffer&&) noexcept = default;
        producer_buffer(const producer_buffer&) = delete;
        producer_buffer& operator=(const producer_buffer&) = delete;

        // ------------------------------------------------------------------
        // Producer side (single thread only) - forwards to the inner stream_buffer
        // ------------------------------------------------------------------

        std::pair<uint8_t*, std::size_t> prepare(std::size_t n) { return buffer_->prepare(n); }
        void commit(std::size_t n) noexcept { buffer_->commit(n); }

        /**
         * @brief Publish the first n committed bytes as one message, and fan it into
         *        the shared record queue so multiplexer consumers can find it.
         *
         * @throws std::length_error if the message does not fit a record's 32-bit length field
         *         (a single message must be < 4 GiB), propagated from
         *         slick::stream_buffer::consume(). It is thrown before any state moves, so
         *         nothing is published to either ring and the bytes stay readable - publish them
         *         as several smaller records instead.
         */
        published_record consume(std::size_t n) {
            published_record rec = buffer_->consume(n);
            if (rec) {
                const uint64_t idx = shared_queue_->reserve();
                record& r = *(*shared_queue_)[idx];
                r.sequence = rec.sequence;
                r.producer_id = producer_id_;
                r.pad0 = 0;
                shared_queue_->publish(idx);
            }
            return rec;
        }

        void discard() noexcept { buffer_->discard(); }
        const uint8_t* data() const noexcept { return buffer_->data(); }
        std::size_t size() const noexcept { return buffer_->size(); }

        // ------------------------------------------------------------------
        // Shared accessors
        // ------------------------------------------------------------------

        uint64_t capacity() const noexcept { return buffer_->capacity(); }
        uint32_t control_size() const noexcept { return buffer_->control_size(); }

        /// This producer's own inner-ring loss. The multiplexer dereferences records through
        /// traits that do not count (see detail::dereference_read_traits), so this only moves for
        /// reads the caller makes directly through stream_buffer().read<Traits>() with a Traits
        /// whose count_loss is true. Multiplexer-level loss is the multiplexer's loss_count().
        uint64_t loss_count() const noexcept { return buffer_->loss_count(); }
        uint64_t initial_reading_index() const noexcept { return buffer_->initial_reading_index(); }
        bool own_buffer() const noexcept { return buffer_->own_buffer(); }
        bool use_shm() const noexcept { return buffer_->use_shm(); }

        /// Literal slick::stream_buffer& access, e.g. for slick::dynamic_buffer.
        slick::stream_buffer& stream_buffer() noexcept { return *buffer_; }
        const slick::stream_buffer& stream_buffer() const noexcept { return *buffer_; }

        /// Shared-ownership handle to the underlying stream_buffer, e.g. for
        /// slick-net's websocket_session, which takes std::shared_ptr<slick::stream_buffer>.
        /// Note: this keeps the stream_buffer alive but not this producer_buffer itself;
        /// use get_producer_buffer() if consume() must remain callable after the
        /// owning multiplexer is destroyed.
        std::shared_ptr<slick::stream_buffer> stream_buffer_ptr() noexcept { return buffer_; }
        std::shared_ptr<const slick::stream_buffer> stream_buffer_ptr() const noexcept { return buffer_; }

        uint32_t producer_id() const noexcept { return producer_id_; }

    private:
        friend class basic_stream_buffer_multiplexer;

        producer_buffer(uint32_t producer_id, std::shared_ptr<shared_queue_type> shared_queue,
                        std::shared_ptr<slick::stream_buffer> buffer) noexcept
            : producer_id_(producer_id), shared_queue_(std::move(shared_queue)), buffer_(std::move(buffer)) {}

        uint32_t producer_id_;
        std::shared_ptr<shared_queue_type> shared_queue_;  // shared ownership with the multiplexer
        std::shared_ptr<slick::stream_buffer> buffer_;     // shared ownership (e.g. with slick-net)
    };

    // ------------------------------------------------------------------
    // Construction: shared record queue (local memory or shared memory)
    // ------------------------------------------------------------------

    /// Create a local-memory shared record queue.
    explicit basic_stream_buffer_multiplexer(uint32_t shared_queue_size)
        : shared_queue_(std::make_shared<shared_queue_type>(shared_queue_size)) {}

    /// Create a shared-memory record queue (creator).
    basic_stream_buffer_multiplexer(uint32_t shared_queue_size, const char* shm_queue_name)
        : shared_queue_(std::make_shared<shared_queue_type>(shared_queue_size, shm_queue_name)) {}

    /// Open an existing shared-memory record queue (geometry self-described).
    explicit basic_stream_buffer_multiplexer(const char* shm_queue_name)
        : shared_queue_(std::make_shared<shared_queue_type>(shm_queue_name)) {}

    basic_stream_buffer_multiplexer(const basic_stream_buffer_multiplexer&) = delete;
    basic_stream_buffer_multiplexer& operator=(const basic_stream_buffer_multiplexer&) = delete;

    /**
     * @brief Remove a shared-memory segment by name, the explicit stale-segment recovery step.
     *
     * Applies to both kinds of name this class creates - a shared record queue's and a producer
     * stream_buffer's - since both are plain slick::shm segments underneath. Use it when a previous
     * run died mid-initialization and left a segment wedged, which construction over that name
     * reports by throwing:
     *
     * @code
     * slick::stream_buffer_multiplexer::remove("md_records");  // clear what a dead run left
     * slick::stream_buffer_multiplexer mux(1024, "md_records");
     * @endcode
     *
     * @return true if the segment was removed.
     *
     * @warning Only call this when no process is using the segment. On POSIX the name is unlinked
     *          immediately, so the next creator gets a fresh segment while any process still mapped
     *          to the old one keeps reading the orphaned copy. On Windows this is a no-op returning
     *          true - a section there disappears once the last handle closes.
     */
    static bool remove(const char* const shm_name) noexcept {
        return slick::stream_buffer::remove(shm_name);
    }

    // ------------------------------------------------------------------
    // Producer registration (single-threaded setup, before producer/consumer threads start)
    // ------------------------------------------------------------------

    /// Register a local-memory producer.
    std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size) {
        return add_producer_impl(producer_id, std::make_shared<slick::stream_buffer>(capacity, control_size));
    }

    /// Register a shared-memory producer (creator).
    std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size, const char* shm_name) {
        return add_producer_impl(producer_id, std::make_shared<slick::stream_buffer>(capacity, control_size, shm_name));
    }

    /// Open an existing shared-memory producer (geometry self-described).
    std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, const char* shm_name) {
        return add_producer_impl(producer_id, std::make_shared<slick::stream_buffer>(shm_name));
    }

    bool has_producer(uint32_t producer_id) const noexcept {
        return find_producer(producer_id) != nullptr;
    }

    /// Shared-ownership handle to a producer_buffer, so it (and its stream_buffer
    /// and shared record queue references) can outlive this multiplexer.
    /// Returns nullptr if producer_id is unregistered (or out of range) on this instance.
    std::shared_ptr<producer_buffer> get_producer_buffer(uint32_t producer_id) {
        auto it = producers_.find(producer_id);
        return it == producers_.end() ? nullptr : it->second;
    }

    std::shared_ptr<const producer_buffer> get_producer_buffer(uint32_t producer_id) const {
        auto it = producers_.find(producer_id);
        return it == producers_.end() ? nullptr : it->second;
    }

    /// Number of registered producers on this multiplexer instance.
    size_t producer_count() const noexcept { return producers_.size(); }

    // ------------------------------------------------------------------
    // Consumer side
    // ------------------------------------------------------------------

    /**
     * @brief Read the next message in global order.
     * @param cursor Reference to the consumer's reading cursor into the shared record queue.
     * @return The next message (data, length, producer_id), or a falsy multiplex_record if none is available.
     */
    multiplex_record read(uint64_t& cursor) noexcept {
        return read_impl(cursor);
    }

    /**
     * @brief Read the next message using a shared atomic cursor (work-stealing).
     * @param cursor Reference to the shared atomic reading cursor into the shared record queue.
     * @return The next message (data, length, producer_id), or a falsy multiplex_record if none is available.
     */
    multiplex_record read(std::atomic<uint64_t>& cursor) noexcept {
        return read_impl(cursor);
    }

    /// Shared-queue wrap loss plus multiplexer-level loss (shared-queue entries whose
    /// producer_id is registered on this instance but were lapped by that producer's
    /// own ring before being dereferenced). Entries whose producer_id is unregistered
    /// on this instance are silently skipped and never counted as loss.
    ///
    /// Both terms are switched together by Traits::enable_loss_detection and read 0 when it is
    /// off - one flag, so this total is never partial. Records are skipped correctly either way;
    /// only the counters are silent.
    uint64_t loss_count() const noexcept {
        return shared_queue_->loss_count() + loss_count_.load(std::memory_order_relaxed);
    }

    /// Initial reading cursor for a late-joining consumer (see slick::queue::initial_reading_index()).
    uint64_t initial_reading_index() const noexcept {
        return shared_queue_->initial_reading_index();
    }

    /**
     * @brief Find a registered producer by id.
     * @param producer_id Producer id to look up.
     * @return Non-owning pointer to the producer, or nullptr if producer_id is unregistered.
     */
    producer_buffer* find_producer(uint32_t producer_id) noexcept {
        return const_cast<producer_buffer*>(std::as_const(*this).find_producer(producer_id));
    }

    /**
     * @brief Find a registered producer by id.
     * @param producer_id Producer id to look up.
     * @return Non-owning pointer to the producer, or nullptr if producer_id is unregistered.
     */
    const producer_buffer* find_producer(uint32_t producer_id) const noexcept {
        if (producer_id < dense_producers_.size()) {
            return dense_producers_[producer_id];
        }
        auto it = producers_.find(producer_id);
        return it == producers_.end() ? nullptr : it->second.get();
    }

private:
    static constexpr size_t dense_lookup_limit_ = 4096;

    std::shared_ptr<producer_buffer> add_producer_impl(uint32_t producer_id, std::shared_ptr<slick::stream_buffer> buffer) {
        if (has_producer(producer_id)) {
            throw std::invalid_argument("producer_id " + std::to_string(producer_id) + " already registered");
        }
        auto producer = std::shared_ptr<producer_buffer>(
            new producer_buffer(producer_id, shared_queue_, std::move(buffer)));
        producers_.emplace(producer_id, producer);

        if (producer_id < dense_lookup_limit_) {
            const size_t slot = static_cast<size_t>(producer_id);
            if (slot >= dense_producers_.size()) {
                dense_producers_.resize(slot + 1, nullptr);
            }
            dense_producers_[slot] = producer.get();
        }

        return producer;
    }

    /// Dereference a shared-queue record into the matching producer's stream_buffer.
    /// Returns a falsy multiplex_record if the record has been lapped before being
    /// dereferenced. Caller must ensure has_producer(rec.producer_id) is true.
    multiplex_record dereference(producer_buffer& producer, const record& rec) noexcept {
        uint64_t local_cursor = rec.sequence;
        auto [data, length] =
            producer.stream_buffer().template read<detail::dereference_read_traits>(local_cursor);
        // The exact-sequence check is what makes this a dereference rather than a scan: read()
        // happily skips forward to whatever the slot now holds, and only local_cursor says
        // whether that was the record we asked for.
        if (data != nullptr && local_cursor == rec.sequence + 1) {
            return { data, length, rec.producer_id };
        }
        return {};
    }

    template <typename Cursor>
    multiplex_record read_impl(Cursor& cursor) noexcept {
        for (;;) {
            auto [rec, n] = shared_queue_->read(cursor);
            (void)n;
            if (rec == nullptr) {
                return {};
            }

            const record copy = *rec;
            auto* producer = find_producer(copy.producer_id);
            if (producer == nullptr) {
                continue;  // not registered on this instance: intentionally ignored, not loss
            }
            if (auto result = dereference(*producer, copy)) {
                return result;
            }
            if constexpr (Traits::enable_loss_detection) {
                loss_count_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    // Read-only after setup and touched by every read(), so they come first, ahead of the counter
    // below: a consumer's fetch_add on a lossy read would otherwise invalidate the line every
    // other consumer is reading the lookup tables from.
    std::unordered_map<uint32_t, std::shared_ptr<producer_buffer>> producers_;
    std::vector<producer_buffer*> dense_producers_;
    std::shared_ptr<shared_queue_type> shared_queue_;

    // Unconditional, and so is its alignment. Making it depend on Traits would buy one cacheline
    // on an object a process holds one or two of - never in an array, never in shared memory - in
    // exchange for a [[no_unique_address]] storage helper and a compiler-specific attribute. The
    // ODR hazard the old macro carried is already gone: the configuration is a template argument,
    // so two configurations are different types whether or not their sizes differ. What the trait
    // still gates is the fetch_add in read_impl(), which is where the cost actually was.
    //
    // The alignment is what keeps it off the line holding the lookup tables above: a lossy read on
    // one consumer would otherwise invalidate a line every other consumer reads on every message.
    alignas(detail::multiplexer_cacheline_size) std::atomic<uint64_t> loss_count_{ 0 };
};

/// The default-configured multiplexer: loss counters on in debug builds, off in release, via
/// slick::default_queue_traits. Name a traits struct explicitly
/// (basic_stream_buffer_multiplexer<slick::debug_queue_traits>) to pin it regardless of NDEBUG.
using stream_buffer_multiplexer = basic_stream_buffer_multiplexer<>;

}  // namespace slick
