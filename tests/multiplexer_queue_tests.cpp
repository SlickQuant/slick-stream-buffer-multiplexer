/********************************************************************************
 * Copyright (c) 2026 Slick Quant LLC
 * All rights reserved
 *
 * This file is part of the SlickStreamBufferMultiplexer. Redistribution and use
 * in source and binary forms, with or without modification, are permitted
 * exclusively under the terms of the MIT license which is available at
 * https://github.com/SlickQuant/slick-stream-buffer-multiplexer/blob/main/LICENSE
 *
 ********************************************************************************/

#include <gtest/gtest.h>
#include <slick/stream_buffer_multiplexer.hpp>

#include "multiplexer_test_support.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <numeric>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

using slick::producer_kind;
using slick::stream_buffer_multiplexer;
using mux_test::counting_multiplexer;
using mux_test::publish_bytes;
using mux_test::publish_message;

namespace {

// While non-zero on this thread, the replaced operator new below refuses any request of at least
// this many bytes. It is how FailedRegistrationLeavesNothingBehind makes one specific allocation
// inside add_queue_producer() fail; everywhere else it is 0 and the replacement is plain malloc.
thread_local std::size_t fail_allocations_from = 0;

/// Arms the failure threshold for one scope.
class failing_allocations {
public:
    explicit failing_allocations(std::size_t from) noexcept { fail_allocations_from = from; }
    ~failing_allocations() { fail_allocations_from = 0; }
    failing_allocations(const failing_allocations&) = delete;
    failing_allocations& operator=(const failing_allocations&) = delete;
};

/// What every replaced allocation function below does; nullptr means refused or out of memory.
void* test_allocate(std::size_t size) noexcept {
    if (fail_allocations_from != 0 && size >= fail_allocations_from) {
        return nullptr;
    }
    return std::malloc(size != 0 ? size : 1);
}

void* test_allocate_or_throw(std::size_t size) {
    if (void* p = test_allocate(size)) {
        return p;
    }
    throw std::bad_alloc();
}

}  // namespace

// Replaced for the whole test binary: there is no narrower way to make a std::vector inside the
// multiplexer fail to grow. Every non-aligned form is replaced, not just the scalar pair the
// others forward to by default - a sanitizer runtime supplies its own versions of whichever ones
// a program leaves alone, and then memory from its nothrow new (std::stable_sort's temporary
// buffer, say) would come back through this free(). The aligned forms are left alone entirely,
// so they still pair only with each other.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"  // free() is the matching release here
#endif
void* operator new(std::size_t size) { return test_allocate_or_throw(size); }
void* operator new[](std::size_t size) { return test_allocate_or_throw(size); }
void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return test_allocate(size); }
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept { return test_allocate(size); }

void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

namespace {

void take_queue(slick::queue<uint8_t>&) {}

/// Drain the multiplexer from `cursor`, returning the first byte of every message it delivers.
template <typename Multiplexer>
std::vector<uint8_t> drain_first_bytes(Multiplexer& mux, uint64_t& cursor) {
    std::vector<uint8_t> values;
    while (auto rec = mux.read(cursor)) {
        values.push_back(rec.data[0]);
    }
    return values;
}

template <typename Map>
std::vector<uint32_t> sorted_keys(const Map& map) {
    std::vector<uint32_t> ids;
    for (const auto& [id, producer] : map) {
        (void)producer;
        ids.push_back(id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

}  // namespace

// --------------------------------------------------------------------------------------------
// A slick::queue<uint8_t> as a producer
// --------------------------------------------------------------------------------------------

TEST(MultiplexerQueueTests, SingleQueueProducerRoundtrip) {
    stream_buffer_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 1024);
    EXPECT_EQ(q0->producer_id(), 0u);
    EXPECT_EQ(q0->kind(), producer_kind::queue);

    publish_message(*q0, "hello", 5);

    uint64_t cursor = 0;
    auto rec = mux.read(cursor);
    ASSERT_TRUE(rec);
    EXPECT_EQ(rec.producer_id, 0u);
    EXPECT_EQ(rec.length, 5u);
    EXPECT_EQ(std::memcmp(rec.data, "hello", 5), 0);
    EXPECT_EQ(cursor, 1u);

    EXPECT_FALSE(mux.read(cursor));
    EXPECT_EQ(mux.loss_count(), 0u);
}

// The point of the feature: both kinds fan into one shared record queue, so a consumer gets one
// stream in publish order however the producers are mixed.
TEST(MultiplexerQueueTests, MixedProducersKeepPublishOrder) {
    counting_multiplexer mux(64);
    auto b0 = mux.add_producer(0, 1024, 16);
    auto q1 = mux.add_queue_producer(1, 1024);
    auto b2 = mux.add_producer(2, 1024, 16);

    EXPECT_EQ(b0->kind(), producer_kind::stream_buffer);
    EXPECT_EQ(q1->kind(), producer_kind::queue);

    publish_message(*b0, "aaa", 3);
    publish_message(*q1, "bbbb", 4);
    publish_message(*b2, "cc", 2);
    publish_message(*q1, "ddddd", 5);
    publish_message(*b0, "e", 1);

    struct expected {
        uint32_t producer_id;
        std::string bytes;
    };
    const std::vector<expected> order{ {0, "aaa"}, {1, "bbbb"}, {2, "cc"}, {1, "ddddd"}, {0, "e"} };

    uint64_t cursor = 0;
    for (const auto& want : order) {
        auto rec = mux.read(cursor);
        ASSERT_TRUE(rec);
        EXPECT_EQ(rec.producer_id, want.producer_id);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(rec.data), rec.length), want.bytes);
    }
    EXPECT_FALSE(mux.read(cursor));
    EXPECT_EQ(mux.loss_count(), 0u);
}

// The work-stealing overload goes through the same dereference.
TEST(MultiplexerQueueTests, SharedAtomicCursorReadsQueueProducers) {
    stream_buffer_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 64);
    publish_bytes(*q0, 3);

    std::atomic<uint64_t> shared_cursor{0};
    for (uint8_t want = 0; want < 3; ++want) {
        auto rec = mux.read(shared_cursor);
        ASSERT_TRUE(rec);
        EXPECT_EQ(rec.length, 1u);
        EXPECT_EQ(rec.data[0], want);
    }
    EXPECT_FALSE(mux.read(shared_cursor));
}

// A byte queue is normally created with items_per_slot > 1, and then slick::queue advances a
// cursor by the reservation rather than by the message length. The dereference has to round the
// same way to recognize its own record: lengths below, at, and above the granularity all have to
// come back, and none may be mistaken for a lapped one.
TEST(MultiplexerQueueTests, ItemsPerSlotMessagesDereferenceExactly) {
    counting_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 1024, 64);
    EXPECT_EQ(q0->size(), 1024u);
    EXPECT_EQ(q0->items_per_slot(), 64u);

    std::vector<uint8_t> pattern(200);
    std::iota(pattern.begin(), pattern.end(), static_cast<uint8_t>(1));

    const std::vector<uint32_t> lengths{ 3, 200, 64, 65, 1 };  // 64 + 256 + 64 + 128 + 64 = 576 bytes
    for (uint32_t length : lengths) {
        publish_message(*q0, pattern.data(), length);
    }

    uint64_t cursor = 0;
    for (uint32_t length : lengths) {
        auto rec = mux.read(cursor);
        ASSERT_TRUE(rec) << "length " << length;
        EXPECT_EQ(rec.length, length);
        EXPECT_EQ(std::memcmp(rec.data, pattern.data(), length), 0) << "length " << length;
    }
    EXPECT_FALSE(mux.read(cursor));
    EXPECT_EQ(mux.loss_count(), 0u);
}

// A reservation that does not fit before the end of the ring restarts at the beginning rather
// than splitting, leaving a wrap marker in the slot it skipped from. The record names the index
// the message really starts at, so it is found directly and comes back in one piece.
TEST(MultiplexerQueueTests, ReservationThatWrapsTheRingStaysContiguous) {
    counting_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 256, 64);

    std::vector<uint8_t> first(100, 0xAA);
    std::vector<uint8_t> second(200);
    std::iota(second.begin(), second.end(), static_cast<uint8_t>(0));

    uint64_t cursor = 0;
    publish_message(*q0, first.data(), first.size());   // bytes 0..127 of the ring
    auto r0 = mux.read(cursor);
    ASSERT_TRUE(r0);
    EXPECT_EQ(r0.length, 100u);
    EXPECT_EQ(std::memcmp(r0.data, first.data(), first.size()), 0);

    // 256 bytes of reservation do not fit in the 128 left, so this one starts over at index 256.
    const uint64_t index = q0->reserve(static_cast<uint32_t>(second.size()));
    EXPECT_EQ(index, 256u);
    std::memcpy((*q0)[index], second.data(), second.size());
    q0->publish(index, static_cast<uint32_t>(second.size()));

    auto r1 = mux.read(cursor);
    ASSERT_TRUE(r1);
    EXPECT_EQ(r1.length, 200u);
    EXPECT_EQ(r1.data, (*q0)[index]);
    EXPECT_EQ(std::memcmp(r1.data, second.data(), second.size()), 0);

    EXPECT_FALSE(mux.read(cursor));
    EXPECT_EQ(mux.loss_count(), 0u);
}

// --------------------------------------------------------------------------------------------
// Loss
// --------------------------------------------------------------------------------------------

// The queue counterpart of ProducerRingLapCausesMultiplexerLoss: a record whose bytes the
// producer's own ring has since overwritten is skipped and counted once.
TEST(MultiplexerQueueTests, QueueLapCausesMultiplexerLoss) {
    counting_multiplexer mux(64);          // generous shared queue: no shared-queue loss
    auto q0 = mux.add_queue_producer(0, 4);  // tiny ring: laps after 4 one-byte messages

    publish_bytes(*q0, 10);

    uint64_t cursor = 0;
    // only the last 4 published messages (6..9) are still present in the producer's ring
    EXPECT_EQ(drain_first_bytes(mux, cursor), (std::vector<uint8_t>{6, 7, 8, 9}));
    EXPECT_EQ(mux.loss_count(), 6u);
}

// Regression: messages of different lengths lap the ring at different rates than their control
// slots. In an 8-byte ring "ijkl" at index 8 covers positions 0-3, so it overwrites "CD" at 2 as
// well as "AB" at 0 - but it only lands on slot 0, and "CD"'s slot still says index 2. The
// dereference used to trust that slot and returned "kl" as "CD", counting a loss of 1. A
// message's bytes are gone once the reservation counter is more than one ring past it, and both
// overwritten messages must be skipped and counted.
TEST(MultiplexerQueueTests, OverwrittenBytesAreNotReturned) {
    counting_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 8);

    for (const char* message : {"AB", "CD", "EFGH", "ijkl"}) {
        publish_message(*q0, message, std::strlen(message));
    }

    uint64_t cursor = 0;
    std::vector<std::string> delivered;
    while (auto rec = mux.read(cursor)) {
        delivered.emplace_back(reinterpret_cast<const char*>(rec.data), rec.length);
    }
    EXPECT_EQ(delivered, (std::vector<std::string>{"EFGH", "ijkl"}));
    EXPECT_EQ(mux.loss_count(), 2u);
}

// The documented price of that check: it is conservative. A reservation that does not fit before
// the end of the ring skips the rest of the lap, so the reservation counter passes indices
// nothing wrote. Here "nopq" does not fit at position 5 and restarts at index 16, leaving 13-15
// unwritten - and "GH" at positions 6-7 lies in that tail, untouched. The counter says it is more
// than a ring old all the same, so it is skipped and counted. This pins that behaviour: a false
// loss, never a corrupt message.
TEST(MultiplexerQueueTests, MessageInASkippedTailIsReportedLost) {
    counting_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 8);

    for (const char* message : {"AB", "CDEF", "GH", "ijklm", "nopq"}) {
        publish_message(*q0, message, std::strlen(message));
    }

    uint64_t cursor = 0;
    std::vector<std::string> delivered;
    while (auto rec = mux.read(cursor)) {
        delivered.emplace_back(reinterpret_cast<const char*>(rec.data), rec.length);
    }
    // "AB" and "ijklm" lost their slots to "nopq", "CDEF" its bytes to "ijklm", and "GH" nothing.
    EXPECT_EQ(delivered, (std::vector<std::string>{"nopq"}));
    EXPECT_EQ(mux.loss_count(), 4u);

    // "GH" really is still there: the queue itself hands it back from its own index.
    uint64_t queue_cursor = 6;
    auto [ptr, len] = q0->queue().read(queue_cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(ptr), len), "GH");
}

// Records are skipped correctly with the counters off; only the numbers are silent.
TEST(MultiplexerQueueTests, QueueLapIsSkippedWithCountersOff) {
    mux_test::silent_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 4);

    publish_bytes(*q0, 10);

    uint64_t cursor = 0;
    EXPECT_EQ(drain_first_bytes(mux, cursor), (std::vector<uint8_t>{6, 7, 8, 9}));
    EXPECT_EQ(mux.loss_count(), 0u);
    EXPECT_EQ(q0->loss_count(), 0u);
}

// The documented difference from producer_buffer::loss_count(). slick::queue has no per-call read
// traits, so the multiplexer cannot keep its dereferences out of the inner queue's own counter:
// each lapped one adds how far that slot has moved on, not 1. Over the same ten messages as above
// the six lapped dereferences land on slots holding 8, 9, 6, 7, 8, 9 while asking for 0..5, which
// is 8 + 8 + 4 + 4 + 4 + 4 = 32. That is a property of the queue, not a count of anything the
// multiplexer lost - which is why loss_count() on the multiplexer is the number to read.
TEST(MultiplexerQueueTests, InnerQueueLossCountIsNotARecordCount) {
    counting_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 4);

    publish_bytes(*q0, 10);
    EXPECT_EQ(q0->loss_count(), 0u);  // nothing has read the queue yet

    uint64_t cursor = 0;
    drain_first_bytes(mux, cursor);

    EXPECT_EQ(mux.loss_count(), 6u);
    EXPECT_EQ(q0->loss_count(), 32u);
}

// --------------------------------------------------------------------------------------------
// Registration
// --------------------------------------------------------------------------------------------

// producer_id is one namespace across both kinds, below the dense lookup limit and above it.
TEST(MultiplexerQueueTests, DuplicateProducerIdAcrossKindsThrows) {
    stream_buffer_multiplexer mux(64);

    mux.add_producer(0, 1024, 16);
    EXPECT_THROW(mux.add_queue_producer(0, 64), std::invalid_argument);

    mux.add_queue_producer(1, 64);
    EXPECT_THROW(mux.add_producer(1, 1024, 16), std::invalid_argument);
    EXPECT_THROW(mux.add_queue_producer(1, 128), std::invalid_argument);

    mux.add_producer(9000, 1024, 16);
    EXPECT_THROW(mux.add_queue_producer(9000, 64), std::invalid_argument);

    mux.add_queue_producer(9001, 64);
    EXPECT_THROW(mux.add_producer(9001, 1024, 16), std::invalid_argument);

    EXPECT_EQ(mux.producer_count(), 4u);
    EXPECT_EQ(mux.get_producer_buffers().size(), 2u);
    EXPECT_EQ(mux.get_producer_queues().size(), 2u);
}

TEST(MultiplexerQueueTests, InvalidQueueArgumentsThrow) {
    using queue_type = stream_buffer_multiplexer::queue_type;
    stream_buffer_multiplexer mux(64);

    EXPECT_THROW(mux.add_queue_producer(0, 1000), std::invalid_argument);                    // size not pow2
    EXPECT_THROW(mux.add_queue_producer(0, static_cast<uint32_t>(0)), std::invalid_argument);  // size 0
    EXPECT_THROW(mux.add_queue_producer(0, 1024, 48), std::invalid_argument);                // items_per_slot not pow2
    EXPECT_THROW(mux.add_queue_producer(0, 64, 128), std::invalid_argument);                 // items_per_slot > size
    EXPECT_THROW(mux.add_queue_producer(0, std::shared_ptr<queue_type>{}), std::invalid_argument);

    // A failed registration leaves nothing behind.
    EXPECT_FALSE(mux.has_producer(0));
    EXPECT_EQ(mux.producer_count(), 0u);

    // reserve() reports a bad length the way slick::queue does.
    auto q0 = mux.add_queue_producer(0, 64);
    EXPECT_THROW(q0->reserve(0), std::invalid_argument);
    EXPECT_THROW(q0->reserve(65), std::runtime_error);
}

// Regression: registration used to insert the handle into its typed table and only then grow the
// dense lookup table. If that growth threw std::bad_alloc the table kept a producer the lookup
// could not see - an id below the dense limit is looked up in the dense table alone - so a retry
// passed the duplicate check, could not insert over the stranded entry, and left the dense table
// pointing at a handle only the caller owned. Dropping it left read() a dangling pointer.
//
// Adopting a ready-made queue keeps every other allocation of the call small (the handle, a map
// node), so with the threshold armed the only request refused is the dense table's: 1001 pointers
// for id 1000.
TEST(MultiplexerQueueTests, FailedRegistrationLeavesNothingBehind) {
    using queue_type = stream_buffer_multiplexer::queue_type;
    constexpr uint32_t kId = 1000;

    stream_buffer_multiplexer mux(64);
    auto queue = std::make_shared<queue_type>(64);

    {
        failing_allocations fail(4096);
        EXPECT_THROW(mux.add_queue_producer(kId, queue), std::bad_alloc);
    }

    // Nothing is half-registered: the lookup and the tables agree that the id is free.
    EXPECT_FALSE(mux.has_producer(kId));
    EXPECT_EQ(mux.producer_count(), 0u);
    EXPECT_TRUE(mux.get_producer_queues().empty());
    EXPECT_EQ(mux.get_producer_queue(kId), nullptr);
    EXPECT_EQ(queue.use_count(), 1);  // and no stranded handle is holding the queue

    // So a retry registers normally, and the handle read() will use is the one the table owns.
    auto q0 = mux.add_queue_producer(kId, queue);
    ASSERT_EQ(mux.get_producer_queue(kId), q0);
    ASSERT_EQ(mux.find_producer_queue(kId), q0.get());
    EXPECT_EQ(mux.producer_count(), 1u);

    // The caller letting go of its handle must not take the producer away from read().
    publish_message(*q0, "kept", 4);
    q0.reset();

    uint64_t cursor = 0;
    auto rec = mux.read(cursor);
    ASSERT_TRUE(rec);
    EXPECT_EQ(rec.producer_id, kId);
    EXPECT_EQ(rec.length, 4u);
    EXPECT_EQ(std::memcmp(rec.data, "kept", 4), 0);
}

// The cross-kind face of the same bug: with the stranded entry hidden from the duplicate check,
// the id could be registered again as the other kind, leaving one id in both typed tables. Both
// kinds register through the same code, so a failed registration has to leave the id free for
// either.
TEST(MultiplexerQueueTests, IdOfAFailedRegistrationIsFreeForEitherKind) {
    stream_buffer_multiplexer mux(64);
    auto queue = std::make_shared<stream_buffer_multiplexer::queue_type>(64);

    {
        failing_allocations fail(4096);
        EXPECT_THROW(mux.add_queue_producer(600, queue), std::bad_alloc);
    }
    EXPECT_FALSE(mux.has_producer(600));

    // The id the failed queue registration asked for is free for either kind.
    auto b0 = mux.add_producer(600, 1024, 16);
    EXPECT_EQ(mux.find_producer(600), b0.get());
    EXPECT_EQ(mux.find_producer_queue(600), nullptr);
    EXPECT_EQ(mux.producer_count(), 1u);
    EXPECT_TRUE(mux.get_producer_queues().empty());
}

// Ids interleave the two kinds and straddle dense_lookup_limit_ deliberately: every id has to
// resolve to the right kind through both the dense table and the fallback, each typed table has
// to carry exactly its own kind, and each typed accessor has to refuse an id of the other kind.
TEST(MultiplexerQueueTests, AccessorsTellTheTwoKindsApart) {
    stream_buffer_multiplexer mux(64);
    mux.add_producer(0, 1024, 16);
    mux.add_queue_producer(1, 64);
    mux.add_producer(2, 1024, 16);
    mux.add_queue_producer(3, 64);
    mux.add_producer(9000, 1024, 16);
    mux.add_queue_producer(9001, 64);

    EXPECT_EQ(mux.producer_count(), 6u);

    const auto& buffers = mux.get_producer_buffers();
    const auto& queues = mux.get_producer_queues();
    EXPECT_EQ(sorted_keys(buffers), (std::vector<uint32_t>{0, 2, 9000}));
    EXPECT_EQ(sorted_keys(queues), (std::vector<uint32_t>{1, 3, 9001}));

    for (const auto& [id, producer] : buffers) {
        ASSERT_NE(producer, nullptr);
        EXPECT_TRUE(mux.has_producer(id));
        EXPECT_EQ(producer->producer_id(), id);
        EXPECT_EQ(producer->kind(), producer_kind::stream_buffer);
        EXPECT_EQ(mux.get_producer_buffer(id), producer);
        EXPECT_EQ(mux.find_producer(id), producer.get());
        EXPECT_EQ(mux.get_producer_queue(id), nullptr);
        EXPECT_EQ(mux.find_producer_queue(id), nullptr);
    }
    for (const auto& [id, producer] : queues) {
        ASSERT_NE(producer, nullptr);
        EXPECT_TRUE(mux.has_producer(id));
        EXPECT_EQ(producer->producer_id(), id);
        EXPECT_EQ(producer->kind(), producer_kind::queue);
        EXPECT_EQ(mux.get_producer_queue(id), producer);
        EXPECT_EQ(mux.find_producer_queue(id), producer.get());
        EXPECT_EQ(mux.get_producer_buffer(id), nullptr);
        EXPECT_EQ(mux.find_producer(id), nullptr);
    }

    // Unregistered ids: one in a hole of the dense table, one past its end, one above the limit.
    for (uint32_t id : {4u, 100u, 9002u}) {
        EXPECT_FALSE(mux.has_producer(id)) << "id " << id;
        EXPECT_EQ(mux.get_producer_buffer(id), nullptr);
        EXPECT_EQ(mux.find_producer(id), nullptr);
        EXPECT_EQ(mux.get_producer_queue(id), nullptr);
        EXPECT_EQ(mux.find_producer_queue(id), nullptr);
    }

    // The const overloads hand back const-qualified producers.
    const auto& const_mux = mux;
    static_assert(std::is_same_v<decltype(const_mux.get_producer_queue(1)),
                                 std::shared_ptr<const stream_buffer_multiplexer::producer_queue>>);
    static_assert(std::is_same_v<decltype(const_mux.find_producer_queue(1)),
                                 const stream_buffer_multiplexer::producer_queue*>);
    EXPECT_EQ(const_mux.find_producer_queue(1), mux.find_producer_queue(1));
    EXPECT_EQ(const_mux.find_producer_queue(9001), mux.find_producer_queue(9001));
    EXPECT_EQ(const_mux.find_producer(1), nullptr);
}

// Messages published above the dense lookup limit go through the fallback lookup, for both kinds.
TEST(MultiplexerQueueTests, SparseIdsOfBothKindsAreReadable) {
    stream_buffer_multiplexer mux(64);
    auto b = mux.add_producer(9000, 1024, 16);
    auto q = mux.add_queue_producer(9001, 64);

    publish_message(*q, "queue", 5);
    publish_message(*b, "buffer", 6);

    uint64_t cursor = 0;
    auto r0 = mux.read(cursor);
    ASSERT_TRUE(r0);
    EXPECT_EQ(r0.producer_id, 9001u);
    EXPECT_EQ(r0.length, 5u);
    EXPECT_EQ(std::memcmp(r0.data, "queue", 5), 0);

    auto r1 = mux.read(cursor);
    ASSERT_TRUE(r1);
    EXPECT_EQ(r1.producer_id, 9000u);
    EXPECT_EQ(r1.length, 6u);
    EXPECT_EQ(std::memcmp(r1.data, "buffer", 6), 0);

    EXPECT_FALSE(mux.read(cursor));
}

// The same contract as get_producer_buffers(): the table itself by reference, not a snapshot.
TEST(MultiplexerQueueTests, GetProducerQueuesIsZeroCopy) {
    stream_buffer_multiplexer mux(64);
    mux.add_producer(0, 1024, 16);
    EXPECT_TRUE(mux.get_producer_queues().empty());  // a stream_buffer producer is not in it

    mux.add_queue_producer(1, 64);
    EXPECT_EQ(&mux.get_producer_queues(), &mux.get_producer_queues());
    EXPECT_EQ(mux.get_producer_queues().size(), 1u);
    mux.add_queue_producer(2, 64);
    EXPECT_EQ(mux.get_producer_queues().size(), 2u);
    EXPECT_EQ(mux.get_producer_buffers().size(), 1u);

    const auto& const_mux = mux;
    const auto& all = const_mux.get_producer_queues();
    static_assert(std::is_same_v<decltype(all), const stream_buffer_multiplexer::producer_queue_map&>,
                  "get_producer_queues() must hand back the table by reference");
    EXPECT_EQ(all.at(2)->size(), 64u);
}

// --------------------------------------------------------------------------------------------
// The producer_queue handle
// --------------------------------------------------------------------------------------------

TEST(MultiplexerQueueTests, ProducerQueueForwardedApi) {
    stream_buffer_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 256, 16);

    EXPECT_EQ(q0->size(), 256u);
    EXPECT_EQ(q0->items_per_slot(), 16u);
    EXPECT_EQ(q0->loss_count(), 0u);
    EXPECT_EQ(q0->initial_reading_index(), 0u);
    EXPECT_TRUE(q0->own_buffer());
    EXPECT_FALSE(q0->use_shm());

    const uint64_t index = q0->reserve(5);
    EXPECT_EQ(index, 0u);
    std::memcpy((*q0)[index], "hello", 5);
    q0->publish(index, 5);

    const auto& const_q0 = *q0;
    EXPECT_EQ(std::memcmp(const_q0[index], "hello", 5), 0);
    EXPECT_EQ(q0->initial_reading_index(), 16u);  // one whole reservation further on
}

// The default configuration's queue type is literally slick::queue<uint8_t>, so the handle's
// queue can be passed to code written against that type - and a queue such code already owns can
// be adopted. The Traits follow the multiplexer's, like the shared record queue's.
TEST(MultiplexerQueueTests, QueueAccessorTypeCompat) {
    static_assert(std::is_same_v<stream_buffer_multiplexer::queue_type, slick::queue<uint8_t>>);
    static_assert(std::is_same_v<counting_multiplexer::queue_type,
                                 slick::queue<uint8_t, slick::debug_queue_traits>>);
    static_assert(std::is_same_v<mux_test::silent_multiplexer::queue_type,
                                 slick::queue<uint8_t, slick::queue_traits>>);

    stream_buffer_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, 64);

    static_assert(std::is_same_v<decltype(q0->queue()), slick::queue<uint8_t>&>,
                  "producer_queue::queue() must return slick::queue<uint8_t>&");
    static_assert(std::is_same_v<decltype(q0->queue_ptr()), std::shared_ptr<slick::queue<uint8_t>>>,
                  "producer_queue::queue_ptr() must return std::shared_ptr<slick::queue<uint8_t>>");
    take_queue(q0->queue());

    auto sp = q0->queue_ptr();
    ASSERT_NE(sp, nullptr);
    EXPECT_EQ(sp.get(), &q0->queue());
    EXPECT_GT(sp.use_count(), 1);  // shared with producer_queue's own copy

    publish_message(*q0, "x", 1);

    uint64_t cursor = 0;
    auto [ptr, len] = q0->queue().read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 1u);
    EXPECT_EQ(ptr[0], 'x');
}

// Same reason as ProducerBufferIsNeitherCopyableNorMovable: the handle is reachable by shared_ptr
// and by raw pointer from the dense lookup table at once.
TEST(MultiplexerQueueTests, ProducerQueueIsNeitherCopyableNorMovable) {
    using pq = stream_buffer_multiplexer::producer_queue;
    static_assert(!std::is_move_constructible_v<pq>, "producer_queue must not be movable");
    static_assert(!std::is_move_assignable_v<pq>, "producer_queue must not be move-assignable");
    static_assert(!std::is_copy_constructible_v<pq>, "producer_queue must not be copyable");
    static_assert(!std::is_copy_assignable_v<pq>, "producer_queue must not be copy-assignable");
}

// An existing queue can be registered as it is. Only what is published through the handle is
// multiplexed: a message put on the queue directly is there for anyone reading the queue, but no
// record was fanned in for it, so multiplexer consumers never see it.
TEST(MultiplexerQueueTests, AdoptedQueueMultiplexesOnlyWhatGoesThroughTheHandle) {
    auto queue = std::make_shared<slick::queue<uint8_t>>(256, 16);

    stream_buffer_multiplexer mux(64);
    auto q0 = mux.add_queue_producer(0, queue);
    EXPECT_EQ(q0->queue_ptr(), queue);
    EXPECT_EQ(q0->size(), 256u);
    EXPECT_EQ(q0->items_per_slot(), 16u);

    publish_message(*q0, "via-mux", 7);

    const uint64_t direct = queue->reserve(6);  // straight onto the queue, bypassing the handle
    std::memcpy((*queue)[direct], "direct", 6);
    queue->publish(direct, 6);

    publish_message(*q0, "again", 5);

    uint64_t cursor = 0;
    auto r0 = mux.read(cursor);
    ASSERT_TRUE(r0);
    EXPECT_EQ(r0.length, 7u);
    EXPECT_EQ(std::memcmp(r0.data, "via-mux", 7), 0);

    auto r1 = mux.read(cursor);
    ASSERT_TRUE(r1);
    EXPECT_EQ(r1.length, 5u);
    EXPECT_EQ(std::memcmp(r1.data, "again", 5), 0);

    EXPECT_FALSE(mux.read(cursor));
    EXPECT_EQ(mux.loss_count(), 0u);

    // All three messages are in the queue itself.
    uint64_t queue_cursor = 0;
    std::vector<uint32_t> lengths;
    for (;;) {
        auto [ptr, len] = queue->read(queue_cursor);
        if (ptr == nullptr) break;
        lengths.push_back(len);
    }
    EXPECT_EQ(lengths, (std::vector<uint32_t>{7, 6, 5}));
}

// The handle holds its queue and the shared record queue by shared_ptr, so one copied out of the
// multiplexer keeps publishing after the multiplexer is gone.
TEST(MultiplexerQueueTests, ProducerQueueOutlivesTheMultiplexer) {
    std::shared_ptr<stream_buffer_multiplexer::producer_queue> kept;
    {
        stream_buffer_multiplexer mux(64);
        mux.add_queue_producer(0, 64);
        kept = mux.get_producer_queue(0);
    }

    ASSERT_NE(kept, nullptr);
    publish_message(*kept, "late", 4);  // publish() still works with the multiplexer gone

    uint64_t cursor = 0;
    auto [ptr, len] = kept->queue().read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 4u);
    EXPECT_EQ(std::memcmp(ptr, "late", 4), 0);
}
