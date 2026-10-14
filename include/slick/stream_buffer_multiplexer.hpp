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

/// Which primitive a registered producer publishes through. At namespace scope for the same
/// reason as multiplexer_record: it is one type across every Traits configuration.
enum class producer_kind : uint8_t {
    stream_buffer,  ///< a slick::stream_buffer, driven through producer_buffer
    queue,          ///< a slick::queue<uint8_t>, driven through producer_queue
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
 * Each producer owns its own byte ring, of one of two kinds sharing one producer_id space:
 *
 * - a slick::stream_buffer (add_producer(), driven through producer_buffer): an
 *   independently-sized data ring with its own control ring, giving per-producer
 *   lap/loss detection and literal slick::stream_buffer& compatibility (e.g. for
 *   slick::dynamic_buffer) via producer_buffer::stream_buffer(), or shared ownership
 *   (e.g. for slick-net's websocket_session) via producer_buffer::stream_buffer_ptr()
 *   and get_producer_buffer();
 * - a slick::queue<uint8_t> (add_queue_producer(), driven through producer_queue):
 *   reserve / write / publish, which several threads may do concurrently.
 *
 * Publishing through either handle - producer_buffer::consume() or
 * producer_queue::publish() - additionally publishes a small {sequence, producer_id}
 * record into one shared slick::queue<multiplexer_record>, which acts as the lock-free
 * MPMC fan-in / global ordering point.
 *
 * Consumers call read(cursor), which dequeues the next shared-queue record and
 * dereferences it back into the matching producer's ring, returning a zero-copy
 * (data, length, producer_id) view. The view is the same for both kinds: length is a
 * byte count.
 *
 * Producers and the shared queue each independently choose local memory or
 * shared memory (IPC): see the add_producer() / add_queue_producer() overloads and the
 * constructors below. A cross-process consumer only needs to register the
 * producer ids whose shared memory it has access to; records referencing other
 * producer ids are silently skipped, and skipping one is not itself counted as
 * loss - though see loss_count() for what the shared-queue term cannot filter.
 *
 * @tparam Traits Compile-time feature configuration, see slick::queue_traits.
 *
 * Caveats:
 * - add_producer() and add_queue_producer() are single-threaded setup: call them
 *   before any producer or consumer threads start.
 * - Each producer_buffer's producer-side methods (prepare/commit/consume/...)
 *   must be called from a single thread, same as slick::stream_buffer. A
 *   producer_queue has no such limit.
 * - loss_count() sums two counters, both switched by Traits::enable_loss_detection:
 *   the shared queue's own wrap loss and this instance's multiplexer-level loss.
 *   Only the second is filtered by registration; see loss_count(). A third, each
 *   producer's inner-ring loss, is separate: see producer_buffer::loss_count() and
 *   producer_queue::loss_count(), which do not behave alike.
 * - A single stream_buffer message (one consume() call) is limited to < 4 GiB;
 *   consume() throws std::length_error rather than truncating one that is not. A
 *   queue message is limited to the queue's size by slick::queue::reserve().
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
    /// A queue producer's byte ring - the same Traits once more, so that under the default
    /// configuration it is literally slick::queue<uint8_t> and an existing one can be adopted.
    using queue_type = slick::queue<uint8_t, Traits>;

    /**
     * @brief What every registered producer has in common, whichever ring it publishes through.
     *
     * producer_buffer and producer_queue derive from it so that one dense lookup table can point
     * at either kind - producer ids are unique across both, so a table per kind would be mostly
     * holes - and so the fan-in into the shared record queue is written once. It has no virtual
     * functions: read() tells the two apart by kind(), which sits beside the pointer to the inner
     * ring it has to load anyway.
     */
    class producer {
    public:
        // Neither copyable nor movable, and so neither is anything derived from it. The
        // multiplexer hands out shared_ptr handles and keeps a raw pointer to the same object in
        // its dense lookup table, so a caller who did
        // `auto stolen = std::move(*mux.get_producer_buffer(0));` would empty the shared_ptr
        // members of an object both of those still point at - and the next publish or read()
        // would dereference a null pointer. Nothing needs to move one: it is constructed in place
        // and only ever held by shared_ptr.
        producer(producer&&) = delete;
        producer& operator=(producer&&) = delete;
        producer(const producer&) = delete;
        producer& operator=(const producer&) = delete;

        uint32_t producer_id() const noexcept { return producer_id_; }
        producer_kind kind() const noexcept { return kind_; }

    protected:
        producer(uint32_t producer_id, producer_kind kind,
                 std::shared_ptr<shared_queue_type> shared_queue) noexcept
            : producer_id_(producer_id), kind_(kind), shared_queue_(std::move(shared_queue)) {}

        // Non-virtual on purpose: a handle is only ever destroyed as its own type, by the deleter
        // the shared_ptr captured when it was created.
        ~producer() = default;

        /// Fan one published message into the shared record queue so multiplexer consumers can
        /// find it. noexcept: reserve() only throws for a count of 0 or one above the queue's
        /// size, and this reserves exactly one record.
        void fan_in(uint64_t sequence) noexcept {
            const uint64_t idx = shared_queue_->reserve();
            record& r = *(*shared_queue_)[idx];
            r.sequence = sequence;
            r.producer_id = producer_id_;
            r.pad0 = 0;
            shared_queue_->publish(idx);
        }

    private:
        uint32_t producer_id_;
        producer_kind kind_;
        std::shared_ptr<shared_queue_type> shared_queue_;  // shared ownership with the multiplexer
    };

    /// Per-producer handle: wraps one slick::stream_buffer and publishes into the shared record queue.
    class producer_buffer final : public producer {
    public:
        using published_record = slick::stream_buffer::published_record;

        /// The producer_kind every producer_buffer reports.
        static constexpr producer_kind handle_kind = producer_kind::stream_buffer;

        // Neither copyable nor movable - see producer, which is what enforces it. Spelled out
        // again so the diagnostic names this class.
        producer_buffer(producer_buffer&&) = delete;
        producer_buffer& operator=(producer_buffer&&) = delete;
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
                this->fan_in(rec.sequence);
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

    private:
        friend class basic_stream_buffer_multiplexer;

        producer_buffer(uint32_t producer_id, std::shared_ptr<shared_queue_type> shared_queue,
                        std::shared_ptr<slick::stream_buffer> buffer) noexcept
            : producer(producer_id, handle_kind, std::move(shared_queue)), buffer_(std::move(buffer)) {}

        std::shared_ptr<slick::stream_buffer> buffer_;     // shared ownership (e.g. with slick-net)
    };

    /**
     * @brief Per-producer handle: wraps one slick::queue<uint8_t> and publishes into the shared
     *        record queue.
     *
     * The producer side is slick::queue's own - reserve(), write through operator[], publish() -
     * and one message is the n bytes of one reserve()/publish() pair. Unlike a producer_buffer,
     * several threads may drive one producer_queue at once: slick::queue's reserve/publish and the
     * fan-in are both lock-free MPMC. Records of concurrent publishers reach the shared queue in
     * the order their publish() calls got there, which need not be reservation order.
     *
     * Only what goes through publish() here is multiplexed. Bytes published on the inner queue
     * directly - through queue(), or by whoever else holds an adopted queue - are never fanned
     * in, so multiplexer consumers do not see them.
     */
    class producer_queue final : public producer {
    public:
        /// The producer_kind every producer_queue reports.
        static constexpr producer_kind handle_kind = producer_kind::queue;

        // Neither copyable nor movable - see producer, which is what enforces it. Spelled out
        // again so the diagnostic names this class.
        producer_queue(producer_queue&&) = delete;
        producer_queue& operator=(producer_queue&&) = delete;
        producer_queue(const producer_queue&) = delete;
        producer_queue& operator=(const producer_queue&) = delete;

        // ------------------------------------------------------------------
        // Producer side (any number of threads) - forwards to the inner queue
        // ------------------------------------------------------------------

        /**
         * @brief Reserve n contiguous bytes for one message.
         * @return The index to write at through operator[] and to hand to publish().
         * @throws std::invalid_argument if n is 0, std::runtime_error if n exceeds size() -
         *         propagated from slick::queue::reserve().
         *
         * The reservation takes n rounded up to a multiple of items_per_slot(). It never blocks:
         * a consumer that has fallen a whole ring behind loses the bytes it overwrites.
         */
        uint64_t reserve(uint32_t n = 1) { return queue_->reserve(n); }

        uint8_t* operator[](uint64_t index) noexcept { return (*queue_)[index]; }
        const uint8_t* operator[](uint64_t index) const noexcept { return (*queue_)[index]; }

        /**
         * @brief Publish the n bytes written at index as one message, and fan it into the shared
         *        record queue so multiplexer consumers can find it.
         * @param index The index reserve() returned.
         * @param n The message length in bytes - the n given to the matching reserve().
         */
        void publish(uint64_t index, uint32_t n = 1) noexcept {
            queue_->publish(index, n);
            this->fan_in(index);
        }

        // ------------------------------------------------------------------
        // Shared accessors
        // ------------------------------------------------------------------

        /// Capacity of the byte ring.
        uint32_t size() const noexcept { return queue_->size(); }
        /// The minimum number of bytes one reserve() consumes; see slick::queue.
        uint32_t items_per_slot() const noexcept { return queue_->items_per_slot(); }

        /// The inner queue's own loss counter - a diagnostic of that queue, not a count of
        /// multiplexer records. slick::queue has no per-call read traits, so unlike
        /// producer_buffer::loss_count() the multiplexer cannot keep its dereferences out of
        /// it: with Traits::enable_loss_detection on, each one that finds its record lapped adds
        /// slick::queue's "how far the slot has moved on" distance, and every later lapped
        /// dereference adds its own all over again. Multiplexer-level loss - one per lapped
        /// record - is the multiplexer's loss_count(). Reads 0 when the trait is off.
        uint64_t loss_count() const noexcept { return queue_->loss_count(); }
        uint64_t initial_reading_index() const noexcept { return queue_->initial_reading_index(); }
        bool own_buffer() const noexcept { return queue_->own_buffer(); }
        bool use_shm() const noexcept { return queue_->use_shm(); }

        /// Literal slick::queue<uint8_t, Traits>& access. Publishing through it directly bypasses
        /// the multiplexer; see the class comment.
        queue_type& queue() noexcept { return *queue_; }
        const queue_type& queue() const noexcept { return *queue_; }

        /// Shared-ownership handle to the underlying queue. Note: this keeps the queue alive but
        /// not this producer_queue itself; use get_producer_queue() if publish() must remain
        /// callable after the owning multiplexer is destroyed.
        std::shared_ptr<queue_type> queue_ptr() noexcept { return queue_; }
        std::shared_ptr<const queue_type> queue_ptr() const noexcept { return queue_; }

    private:
        friend class basic_stream_buffer_multiplexer;

        producer_queue(uint32_t producer_id, std::shared_ptr<shared_queue_type> shared_queue,
                       std::shared_ptr<queue_type> queue) noexcept
            : producer(producer_id, handle_kind, std::move(shared_queue))
            , item_mask_(uint64_t{ queue->items_per_slot() } - 1)
            , queue_(std::move(queue)) {}

        // items_per_slot() - 1, cached for dereference(): read() advances a cursor by the whole
        // reservation, which is the message length rounded up to this granularity.
        uint64_t item_mask_;
        std::shared_ptr<queue_type> queue_;
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
     * @brief Remove a shared-memory segment by name: the explicit cleanup step, and the
     *        stale-segment recovery step.
     *
     * Applies to every kind of name this class creates - a shared record queue's, a producer
     * stream_buffer's and a producer queue's - since all are plain slick::shm segments
     * underneath. Nothing unlinks a
     * segment on destruction, so on POSIX a name outlives every process that used it and the next
     * creator attaches to it instead of starting fresh. Call this from whichever process
     * coordinates the lifetime: at startup for a clean start, or at shutdown once no peer will
     * attach again. It also clears a segment a previous run left wedged by dying
     * mid-initialization, which construction over that name reports by throwing:
     *
     * @code
     * slick::stream_buffer_multiplexer::remove("md_records");  // clear what the last run left
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
    //
    // producer_id is one namespace across both kinds: an id held by a stream_buffer producer
    // cannot be registered again as a queue producer, or the other way round.
    // ------------------------------------------------------------------

    /// Register a local-memory producer.
    std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size) {
        return add_producer_impl(producers_, producer_id, std::make_shared<slick::stream_buffer>(capacity, control_size));
    }

    /// Register a shared-memory producer (creator).
    std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size, const char* shm_name) {
        return add_producer_impl(producers_, producer_id, std::make_shared<slick::stream_buffer>(capacity, control_size, shm_name));
    }

    /// Open an existing shared-memory producer (geometry self-described).
    std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, const char* shm_name) {
        return add_producer_impl(producers_, producer_id, std::make_shared<slick::stream_buffer>(shm_name));
    }

    /// Register a local-memory queue producer with one control slot per byte. Fine for a small
    /// ring; for a large one pass items_per_slot, or the 16-byte slots dwarf the data.
    std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, uint32_t size) {
        return add_producer_impl(producer_queues_, producer_id, std::make_shared<queue_type>(size));
    }

    /// Register a local-memory queue producer whose control slots each cover items_per_slot
    /// bytes - the minimum one reserve() consumes; see slick::queue.
    std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, uint32_t size, uint32_t items_per_slot) {
        return add_producer_impl(producer_queues_, producer_id, std::make_shared<queue_type>(size, items_per_slot));
    }

    /// Register a shared-memory queue producer (creator).
    std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, uint32_t size, const char* shm_name) {
        return add_producer_impl(producer_queues_, producer_id, std::make_shared<queue_type>(size, shm_name));
    }

    /// Register a shared-memory queue producer (creator) with items_per_slot bytes per control slot.
    std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, uint32_t size, uint32_t items_per_slot, const char* shm_name) {
        return add_producer_impl(producer_queues_, producer_id, std::make_shared<queue_type>(size, items_per_slot, shm_name));
    }

    /// Open an existing shared-memory queue producer (geometry self-described).
    std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, const char* shm_name) {
        return add_producer_impl(producer_queues_, producer_id, std::make_shared<queue_type>(shm_name));
    }

    /**
     * @brief Register a queue the caller already has as a queue producer.
     * @throws std::invalid_argument if queue is null.
     *
     * Only messages published through the returned producer_queue are multiplexed. Whatever else
     * writes to the queue directly stays invisible to multiplexer consumers, while still using up
     * the ring - so it can lap messages that were multiplexed.
     */
    std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, std::shared_ptr<queue_type> queue) {
        if (queue == nullptr) {
            throw std::invalid_argument("queue must not be null");
        }
        return add_producer_impl(producer_queues_, producer_id, std::move(queue));
    }

    /// True if producer_id is registered on this instance, as either kind.
    bool has_producer(uint32_t producer_id) const noexcept {
        return find_any(producer_id) != nullptr;
    }

    /// Shared-ownership handle to a producer_buffer, so it (and its stream_buffer
    /// and shared record queue references) can outlive this multiplexer.
    /// Returns nullptr if producer_id is unregistered (or out of range) on this instance, or
    /// is registered as a queue producer.
    std::shared_ptr<producer_buffer> get_producer_buffer(uint32_t producer_id) {
        return find_shared(producers_, producer_id);
    }

    std::shared_ptr<const producer_buffer> get_producer_buffer(uint32_t producer_id) const {
        return find_shared(producers_, producer_id);
    }

    /// Shared-ownership handle to a producer_queue, so it (and its queue and shared record queue
    /// references) can outlive this multiplexer.
    /// Returns nullptr if producer_id is unregistered (or out of range) on this instance, or
    /// is registered as a stream_buffer producer.
    std::shared_ptr<producer_queue> get_producer_queue(uint32_t producer_id) {
        return find_shared(producer_queues_, producer_id);
    }

    std::shared_ptr<const producer_queue> get_producer_queue(uint32_t producer_id) const {
        return find_shared(producer_queues_, producer_id);
    }

    /// The container get_producer_buffers() hands back. Named so callers that must spell the
    /// type do not have to track the representation, which returning the table by reference
    /// otherwise pins in place.
    using producer_map = std::unordered_map<uint32_t, std::shared_ptr<producer_buffer>>;

    /// The container get_producer_queues() hands back; the queue-producer counterpart of
    /// producer_map.
    using producer_queue_map = std::unordered_map<uint32_t, std::shared_ptr<producer_queue>>;

    /**
     * @brief Every registered stream_buffer producer, keyed by producer_id. Queue producers are
     *        in get_producer_queues().
     *
     * Zero-copy - this is the multiplexer's own registration table, not a snapshot. Nothing is
     * allocated, so it is cheap enough to call in a monitoring loop. The reference stays valid
     * until the next add_producer(), which is single-threaded setup, so in practice for the
     * lifetime of the multiplexer; the shared_ptr elements can be copied out to outlive it.
     *
     * Two consequences of handing back the table rather than a copy:
     * - Order is the hash map's - unspecified, and not stable across runs or implementations.
     *   Sort by producer_id at the call site if a report needs a fixed sequence.
     * - const does not propagate: a const multiplexer still yields shared_ptr<producer_buffer>,
     *   because these are the very shared_ptrs the table holds. Use the const overloads of
     *   get_producer_buffer() or find_producer() where a const-qualified producer matters.
     *
     * @code
     * uint64_t total = 0;
     * for (const auto& [id, producer] : mux.get_producer_buffers()) {
     *     total += producer->loss_count();
     * }
     * @endcode
     */
    const producer_map& get_producer_buffers() const noexcept { return producers_; }

    /**
     * @brief Every registered queue producer, keyed by producer_id.
     *
     * The counterpart of get_producer_buffers(), with the same contract: zero-copy, valid until
     * the next add_queue_producer(), unspecified order, and const does not propagate to the
     * elements. The two tables never share a key, since producer_id is unique across both kinds.
     */
    const producer_queue_map& get_producer_queues() const noexcept { return producer_queues_; }

    /// Number of registered producers on this multiplexer instance, of both kinds.
    size_t producer_count() const noexcept { return producers_.size() + producer_queues_.size(); }

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
    /// producer_id is registered on this instance but were lapped by that producer's own ring
    /// before being dereferenced). A lapped entry counts once, whichever kind of producer it
    /// names.
    ///
    /// Only the second term is filtered by registration. An entry this instance actually reads
    /// and finds to name an unregistered producer is skipped without counting - but the first
    /// term counts entries the shared queue dropped by wrapping, and a lapped slot has already
    /// been overwritten, so nothing is left to say which producer it named. An instance that
    /// deliberately registers a subset of the producers therefore sees wrap loss for producers it
    /// does not care about, making this an upper bound on what it actually missed. The loss is
    /// not attributable after the fact by any means; size the shared queue so it does not wrap
    /// and the term goes to 0, leaving the total exactly this instance's own loss.
    ///
    /// Both terms are switched together by Traits::enable_loss_detection and read 0 when it is
    /// off. Records are skipped correctly either way; only the counters are silent.
    uint64_t loss_count() const noexcept {
        return shared_queue_->loss_count() + loss_count_.load(std::memory_order_relaxed);
    }

    /// Initial reading cursor for a late-joining consumer (see slick::queue::initial_reading_index()).
    uint64_t initial_reading_index() const noexcept {
        return shared_queue_->initial_reading_index();
    }

    /**
     * @brief Find a registered stream_buffer producer by id.
     * @param producer_id Producer id to look up.
     * @return Non-owning pointer to the producer, or nullptr if producer_id is unregistered or
     *         is registered as a queue producer.
     */
    producer_buffer* find_producer(uint32_t producer_id) noexcept {
        return const_cast<producer_buffer*>(std::as_const(*this).find_producer(producer_id));
    }

    /**
     * @brief Find a registered stream_buffer producer by id.
     * @param producer_id Producer id to look up.
     * @return Non-owning pointer to the producer, or nullptr if producer_id is unregistered or
     *         is registered as a queue producer.
     */
    const producer_buffer* find_producer(uint32_t producer_id) const noexcept {
        return as_handle<producer_buffer>(find_any(producer_id));
    }

    /**
     * @brief Find a registered queue producer by id.
     * @param producer_id Producer id to look up.
     * @return Non-owning pointer to the producer, or nullptr if producer_id is unregistered or
     *         is registered as a stream_buffer producer.
     */
    producer_queue* find_producer_queue(uint32_t producer_id) noexcept {
        return const_cast<producer_queue*>(std::as_const(*this).find_producer_queue(producer_id));
    }

    /**
     * @brief Find a registered queue producer by id.
     * @param producer_id Producer id to look up.
     * @return Non-owning pointer to the producer, or nullptr if producer_id is unregistered or
     *         is registered as a stream_buffer producer.
     */
    const producer_queue* find_producer_queue(uint32_t producer_id) const noexcept {
        return as_handle<producer_queue>(find_any(producer_id));
    }

private:
    static constexpr size_t dense_lookup_limit_ = 4096;

    /// The handle behind a producer of either kind, or nullptr if p is null or is the other kind.
    template <typename Handle>
    static const Handle* as_handle(const producer* p) noexcept {
        return p != nullptr && p->kind() == Handle::handle_kind ? static_cast<const Handle*>(p) : nullptr;
    }

    /// The shared handle registered under producer_id in one of the typed tables, or nullptr.
    template <typename Map>
    static typename Map::mapped_type find_shared(const Map& map, uint32_t producer_id) {
        auto it = map.find(producer_id);
        return it == map.end() ? nullptr : it->second;
    }

    /// Find a registered producer of either kind. Every id below dense_lookup_limit_ lives in the
    /// dense table, so only ids at or above it - too sparse to index a table by - reach the
    /// typed tables, which never share a key.
    const producer* find_any(uint32_t producer_id) const noexcept {
        if (producer_id < dense_producers_.size()) {
            return dense_producers_[producer_id];
        }
        if (producer_id < dense_lookup_limit_) {
            return nullptr;
        }
        if (auto it = producers_.find(producer_id); it != producers_.end()) {
            return it->second.get();
        }
        auto it = producer_queues_.find(producer_id);
        return it == producer_queues_.end() ? nullptr : it->second.get();
    }

    producer* find_any(uint32_t producer_id) noexcept {
        return const_cast<producer*>(std::as_const(*this).find_any(producer_id));
    }

    /// Wrap a producer's ring in its handle and register it: in its kind's typed table, which
    /// owns it, and - for an id below dense_lookup_limit_ - in the dense table read() uses.
    template <typename Map, typename Ring>
    typename Map::mapped_type add_producer_impl(Map& map, uint32_t producer_id, std::shared_ptr<Ring> ring) {
        using handle_type = typename Map::mapped_type::element_type;
        if (has_producer(producer_id)) {
            throw std::invalid_argument("producer_id " + std::to_string(producer_id) + " already registered");
        }

        // Order matters for exception safety. find_any() trusts the dense table alone for an id
        // below dense_lookup_limit_, so a handle that reached its typed table but not the dense
        // one would be owned yet invisible: a retry would pass the duplicate check above, fail to
        // insert over the stranded entry, and leave the dense table pointing at a handle only the
        // caller owns - a dangling pointer for read() once the caller lets go. So the dense table
        // grows first, which only adds empty slots and leaves nothing visible if it throws. The
        // map insert can still throw, but it is all-or-nothing and the slot is still empty then.
        // The store that publishes the handle to read() comes last, and cannot fail.
        const bool dense = producer_id < dense_lookup_limit_;
        const size_t slot = static_cast<size_t>(producer_id);
        if (dense && slot >= dense_producers_.size()) {
            dense_producers_.resize(slot + 1, nullptr);
        }
        auto handle = std::shared_ptr<handle_type>(
            new handle_type(producer_id, shared_queue_, std::move(ring)));
        map.emplace(producer_id, handle);
        if (dense) {
            dense_producers_[slot] = handle.get();
        }

        return handle;
    }

    /// Dereference a shared-queue record into the matching producer's stream_buffer.
    /// Returns a falsy multiplex_record if the record has been lapped before being
    /// dereferenced.
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

    /// Dereference a shared-queue record into the matching producer's queue. Returns a falsy
    /// multiplex_record if the record has been lapped before being dereferenced - its control
    /// slot or its bytes.
    multiplex_record dereference(producer_queue& producer, const record& rec) noexcept {
        uint64_t local_cursor = rec.sequence;
        auto [data, length] = producer.queue_->read(local_cursor);
        // Same exact-hit check as above, with slick::queue's arithmetic: read() leaves the cursor
        // one whole reservation past the index it actually read - the message length rounded up
        // to items_per_slot. A lapped slot, or a wrap marker sending the cursor elsewhere, both
        // hand back a message that started at some other index, and the cursor shows it.
        const uint64_t footprint = (uint64_t{ length } + producer.item_mask_) & ~producer.item_mask_;
        if (data != nullptr && local_cursor == rec.sequence + footprint) {
            // An intact control slot does not mean intact bytes. Messages differ in length, so a
            // later reservation can cover this message's bytes without landing on its slot: in an
            // 8-byte ring, "ijkl" at index 8 overwrites positions 0-3 and with them "CD" at 2,
            // whose slot still reads 2. Reservations take indices in order, so the first byte is
            // gone exactly once the reservation counter passes rec.sequence + size().
            //
            // Two limits, both on the safe side. It is best-effort, as slick::stream_buffer's
            // own check is: a producer can still reserve after the load. And it is
            // conservative: a wrapping reservation skips the rest of a lap, so the counter can
            // pass that mark through indices nothing wrote, and a message lying in the skipped
            // tail is reported lost while its bytes are still there. Telling the two apart would
            // need slick::queue's wrap markers, which its public interface does not expose.
            //
            // end < rec.sequence: the queue was reset() under this record.
            const uint64_t end = producer.queue_->initial_reading_index();
            if (end >= rec.sequence && end - rec.sequence <= producer.queue_->size()) {
                return { data, length, rec.producer_id };
            }
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
            producer* source = find_any(copy.producer_id);
            if (source == nullptr) {
                continue;  // not registered on this instance: intentionally ignored, not loss
            }
            // The handles are final and tagged, so this is a branch and two inlined bodies
            // rather than a virtual call.
            const multiplex_record result = source->kind() == producer_kind::stream_buffer
                ? dereference(static_cast<producer_buffer&>(*source), copy)
                : dereference(static_cast<producer_queue&>(*source), copy);
            if (result) {
                return result;
            }
            if constexpr (Traits::enable_loss_detection) {
                loss_count_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    // Read-only after setup and touched by every read(), so they come first, ahead of the counter
    // below: a consumer's fetch_add on a lossy read would otherwise invalidate the line every
    // other consumer is reading the lookup tables from. The dense table and the shared queue lead
    // because they are the two read() loads for any id below dense_lookup_limit_; the typed
    // tables are ownership, enumeration and the fallback for ids above it.
    std::vector<producer*> dense_producers_;  // either kind, indexed by producer_id
    std::shared_ptr<shared_queue_type> shared_queue_;
    producer_map producers_;
    producer_queue_map producer_queues_;

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
