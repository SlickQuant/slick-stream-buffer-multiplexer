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

#include <slick/stream_buffer.h>
#include <slick/queue.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION
#if !defined(NDEBUG)
#define SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION 1
#else
#define SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION 0
#endif
#endif

namespace slick {

/**
 * @brief A lock-free multi-producer multi-consumer byte stream multiplexer.
 *
 * Each producer owns its own slick::stream_buffer (an independently-sized data
 * ring with its own control ring), giving per-producer lap/loss detection and
 * literal slick::stream_buffer& compatibility (e.g. for slick::dynamic_buffer)
 * via producer_buffer::stream_buffer(), or shared ownership (e.g. for slick-net's
 * websocket_session) via producer_buffer::stream_buffer_ptr() and
 * get_producer_buffer(). consume() additionally publishes a small
 * {sequence, producer_id} record into one shared slick::queue<record>, which
 * acts as the lock-free MPMC fan-in / global ordering point.
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
 * Caveats:
 * - add_producer() is single-threaded setup: call it before any producer or
 *   consumer threads start.
 * - Each producer_buffer's producer-side methods (prepare/commit/consume/...)
 *   must be called from a single thread, same as slick::stream_buffer.
 * - Three independent loss counters exist: shared_queue_->loss_count() (shared
 *   queue wrap loss), this->loss_count() (which adds multiplexer-level loss:
 *   shared-queue entries whose producer_id is registered on this instance but
 *   were lapped by that producer's own ring before being dereferenced), and
 *   each producer_buffer's own loss_count() (inner ring loss). Entries whose
 *   producer_id is unregistered on this instance are never counted as loss.
 *   The multiplexer-level counter and its contribution to this->loss_count()
 *   compile out when SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION is 0
 *   (default: 1 in debug builds, 0 in release builds), matching
 *   slick::stream_buffer and slick::queue's own loss-detection macros.
 * - A single message (one consume() call) is limited to < 4 GiB.
 */
class stream_buffer_multiplexer {
public:
    /// Shared-queue element: identifies one published message by producer + sequence.
    struct record {
        uint64_t sequence;
        uint32_t producer_id;
        uint32_t pad0 = 0;
    };
    static_assert(sizeof(record) == 16, "record must be 16 bytes");

    /// A dereferenced message as returned by read(). Evaluates to false if no message was available.
    struct multiplex_record {
        const uint8_t* data = nullptr;
        uint32_t length = 0;
        uint32_t producer_id = 0;
        explicit operator bool() const noexcept { return data != nullptr; }
    };

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
         */
        published_record consume(std::size_t n) noexcept {
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
        /// owning stream_buffer_multiplexer is destroyed.
        std::shared_ptr<slick::stream_buffer> stream_buffer_ptr() noexcept { return buffer_; }
        std::shared_ptr<const slick::stream_buffer> stream_buffer_ptr() const noexcept { return buffer_; }

        uint32_t producer_id() const noexcept { return producer_id_; }

    private:
        friend class stream_buffer_multiplexer;

        producer_buffer(uint32_t producer_id, std::shared_ptr<slick::queue<record>> shared_queue,
                        std::shared_ptr<slick::stream_buffer> buffer) noexcept
            : producer_id_(producer_id), shared_queue_(std::move(shared_queue)), buffer_(std::move(buffer)) {}

        uint32_t producer_id_;
        std::shared_ptr<slick::queue<record>> shared_queue_;  // shared ownership with stream_buffer_multiplexer
        std::shared_ptr<slick::stream_buffer> buffer_;        // shared ownership (e.g. with slick-net)
    };

    // ------------------------------------------------------------------
    // Construction: shared record queue (local memory or shared memory)
    // ------------------------------------------------------------------

    /// Create a local-memory shared record queue.
    explicit stream_buffer_multiplexer(uint32_t shared_queue_size)
        : shared_queue_(std::make_shared<slick::queue<record>>(shared_queue_size)) {}

    /// Create a shared-memory record queue (creator).
    stream_buffer_multiplexer(uint32_t shared_queue_size, const char* shm_queue_name)
        : shared_queue_(std::make_shared<slick::queue<record>>(shared_queue_size, shm_queue_name)) {}

    /// Open an existing shared-memory record queue (geometry self-described).
    explicit stream_buffer_multiplexer(const char* shm_queue_name)
        : shared_queue_(std::make_shared<slick::queue<record>>(shm_queue_name)) {}

    stream_buffer_multiplexer(const stream_buffer_multiplexer&) = delete;
    stream_buffer_multiplexer& operator=(const stream_buffer_multiplexer&) = delete;

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
        return producer_id < producers_.size() && producers_[producer_id] != nullptr;
    }

    /// Shared-ownership handle to a producer_buffer, so it (and its stream_buffer
    /// and shared record queue references) can outlive this stream_buffer_multiplexer.
    /// Returns nullptr if producer_id is unregistered (or out of range) on this instance.
    std::shared_ptr<producer_buffer> get_producer_buffer(uint32_t producer_id) {
        if (!has_producer(producer_id)) {
            return nullptr;
        }
        return producers_[producer_id];
    }

    std::shared_ptr<const producer_buffer> get_producer_buffer(uint32_t producer_id) const {
        if (!has_producer(producer_id)) {
            return nullptr;
        }
        return producers_[producer_id];
    }

    /// Number of producer_id slots allocated so far (may include unregistered/nullptr gaps).
    size_t producer_slot_count() const noexcept { return producers_.size(); }

    // ------------------------------------------------------------------
    // Consumer side
    // ------------------------------------------------------------------

    /**
     * @brief Read the next message in global order.
     * @param cursor Reference to the consumer's reading cursor into the shared record queue.
     * @return The next message (data, length, producer_id), or a falsy multiplex_record if none is available.
     */
    multiplex_record read(uint64_t& cursor) noexcept {
        for (;;) {
            auto [rec, n] = shared_queue_->read(cursor);
            (void)n;
            if (rec == nullptr) {
                return {};
            }
            const record copy = *rec;
            if (!has_producer(copy.producer_id)) {
                continue;  // not registered on this instance: intentionally ignored, not loss
            }
            if (auto result = dereference(copy)) {
                return result;
            }
#if SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION
            loss_count_.fetch_add(1, std::memory_order_relaxed);
#endif
        }
    }

    /**
     * @brief Read the next message using a shared atomic cursor (work-stealing).
     * @param cursor Reference to the shared atomic reading cursor into the shared record queue.
     * @return The next message (data, length, producer_id), or a falsy multiplex_record if none is available.
     */
    multiplex_record read(std::atomic<uint64_t>& cursor) noexcept {
        for (;;) {
            auto [rec, n] = shared_queue_->read(cursor);
            (void)n;
            if (rec == nullptr) {
                return {};
            }
            const record copy = *rec;
            if (!has_producer(copy.producer_id)) {
                continue;  // not registered on this instance: intentionally ignored, not loss
            }
            if (auto result = dereference(copy)) {
                return result;
            }
#if SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION
            loss_count_.fetch_add(1, std::memory_order_relaxed);
#endif
        }
    }

    /// Shared-queue wrap loss plus multiplexer-level loss (shared-queue entries whose
    /// producer_id is registered on this instance but were lapped by that producer's
    /// own ring before being dereferenced). Entries whose producer_id is unregistered
    /// on this instance are silently skipped and never counted as loss.
    /// The multiplexer-level term is always 0 if SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION is 0.
    uint64_t loss_count() const noexcept {
#if SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION
        return shared_queue_->loss_count() + loss_count_.load(std::memory_order_relaxed);
#else
        return shared_queue_->loss_count();
#endif
    }

    /// Initial reading cursor for a late-joining consumer (see slick::queue::initial_reading_index()).
    uint64_t initial_reading_index() const noexcept {
        return shared_queue_->initial_reading_index();
    }

private:
    std::shared_ptr<producer_buffer> add_producer_impl(uint32_t producer_id, std::shared_ptr<slick::stream_buffer> buffer) {
        if (has_producer(producer_id)) {
            throw std::invalid_argument("producer_id " + std::to_string(producer_id) + " already registered");
        }
        if (producer_id >= producers_.size()) {
            producers_.resize(producer_id + 1);
        }
        producers_[producer_id].reset(new producer_buffer(producer_id, shared_queue_, std::move(buffer)));
        return producers_[producer_id];
    }

    /// Dereference a shared-queue record into the matching producer's stream_buffer.
    /// Returns a falsy multiplex_record if the record has been lapped before being
    /// dereferenced. Caller must ensure has_producer(rec.producer_id) is true.
    multiplex_record dereference(const record& rec) noexcept {
        uint64_t local_cursor = rec.sequence;
        auto [data, length] = producers_[rec.producer_id]->stream_buffer().read(local_cursor);
        if (data != nullptr && local_cursor == rec.sequence + 1) {
            return { data, length, rec.producer_id };
        }
        return {};
    }

    std::vector<std::shared_ptr<producer_buffer>> producers_;
    std::shared_ptr<slick::queue<record>> shared_queue_;
#if SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION
    std::atomic<uint64_t> loss_count_{0};
#endif
};

}  // namespace slick
