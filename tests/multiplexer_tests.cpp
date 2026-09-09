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

#include <atomic>
#include <cstring>
#include <algorithm>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <vector>

using slick::stream_buffer_multiplexer;
using mux_test::counting_multiplexer;
using mux_test::counting_read_traits;
using mux_test::publish_message;

namespace {

void take_stream_buffer(slick::stream_buffer&) {}

}  // namespace

TEST(MultiplexerTests, EmptyMultiplexerReadReturnsFalsy) {
    stream_buffer_multiplexer mux(64);

    EXPECT_EQ(mux.producer_count(), 0u);
    EXPECT_EQ(mux.initial_reading_index(), 0u);
    EXPECT_EQ(mux.loss_count(), 0u);

    uint64_t cursor = 0;
    auto rec = mux.read(cursor);
    EXPECT_FALSE(rec);
    EXPECT_EQ(rec.data, nullptr);
    EXPECT_EQ(rec.length, 0u);
    EXPECT_EQ(cursor, 0u);

    std::atomic<uint64_t> shared_cursor{0};
    auto rec2 = mux.read(shared_cursor);
    EXPECT_FALSE(rec2);
}

TEST(MultiplexerTests, SingleProducerRoundtrip) {
    stream_buffer_multiplexer mux(64);
    auto p0 = mux.add_producer(0, 1024, 16);
    EXPECT_EQ(p0->producer_id(), 0u);

    publish_message(*p0, "hello", 5);

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

TEST(MultiplexerTests, MultipleProducersNonContiguousIds) {
    stream_buffer_multiplexer mux(64);
    auto p0 = mux.add_producer(0, 1024, 16);
    auto p2 = mux.add_producer(2, 2048, 32);

    EXPECT_EQ(mux.producer_count(), 2u);
    EXPECT_TRUE(mux.has_producer(0));
    EXPECT_FALSE(mux.has_producer(1));
    EXPECT_TRUE(mux.has_producer(2));

    publish_message(*p0, "aaa", 3);
    publish_message(*p2, "bbbb", 4);
    publish_message(*p0, "ccccc", 5);

    uint64_t cursor = 0;

    auto r0 = mux.read(cursor);
    ASSERT_TRUE(r0);
    EXPECT_EQ(r0.producer_id, 0u);
    EXPECT_EQ(r0.length, 3u);
    EXPECT_EQ(std::memcmp(r0.data, "aaa", 3), 0);

    auto r1 = mux.read(cursor);
    ASSERT_TRUE(r1);
    EXPECT_EQ(r1.producer_id, 2u);
    EXPECT_EQ(r1.length, 4u);
    EXPECT_EQ(std::memcmp(r1.data, "bbbb", 4), 0);

    auto r2 = mux.read(cursor);
    ASSERT_TRUE(r2);
    EXPECT_EQ(r2.producer_id, 0u);
    EXPECT_EQ(r2.length, 5u);
    EXPECT_EQ(std::memcmp(r2.data, "ccccc", 5), 0);

    EXPECT_FALSE(mux.read(cursor));
    EXPECT_EQ(mux.loss_count(), 0u);
}

TEST(MultiplexerTests, ConsumeZeroPublishesNothing) {
    stream_buffer_multiplexer mux(64);
    auto p0 = mux.add_producer(0, 1024, 16);

    auto [ptr, sz] = p0->prepare(3);
    ASSERT_NE(ptr, nullptr);
    std::memcpy(ptr, "abc", 3);
    p0->commit(3);

    auto rec = p0->consume(0);
    EXPECT_FALSE(static_cast<bool>(rec));

    uint64_t cursor = 0;
    EXPECT_FALSE(mux.read(cursor));
}

TEST(MultiplexerTests, UnregisteredProducerQueries) {
    stream_buffer_multiplexer mux(64);
    mux.add_producer(0, 1024, 16);

    EXPECT_FALSE(mux.has_producer(5));
    EXPECT_EQ(mux.get_producer_buffer(5), nullptr);

    const auto& const_mux = mux;
    EXPECT_EQ(const_mux.get_producer_buffer(5), nullptr);
}

TEST(MultiplexerTests, DuplicateProducerIdThrows) {
    stream_buffer_multiplexer mux(64);
    mux.add_producer(0, 1024, 16);
    EXPECT_THROW(mux.add_producer(0, 2048, 32), std::invalid_argument);
}

TEST(MultiplexerTests, InvalidSizesThrow) {
    EXPECT_THROW(stream_buffer_multiplexer(static_cast<uint32_t>(0)), std::invalid_argument);    // shared queue size not pow2
    EXPECT_THROW(stream_buffer_multiplexer(100), std::invalid_argument);  // shared queue size not pow2

    stream_buffer_multiplexer mux(64);
    EXPECT_THROW(mux.add_producer(0, 1000, 16), std::invalid_argument);  // capacity not pow2
    EXPECT_THROW(mux.add_producer(0, 1024, 15), std::invalid_argument);  // control_size not pow2
}

TEST(MultiplexerTests, ProducerBufferForwardedApi) {
    stream_buffer_multiplexer mux(64);
    auto p0 = mux.add_producer(0, 1024, 16);

    EXPECT_EQ(p0->capacity(), 1024u);
    EXPECT_EQ(p0->control_size(), 16u);
    EXPECT_EQ(p0->loss_count(), 0u);
    EXPECT_EQ(p0->initial_reading_index(), 0u);
    EXPECT_TRUE(p0->own_buffer());
    EXPECT_FALSE(p0->use_shm());

    auto [ptr, sz] = p0->prepare(5);
    ASSERT_NE(ptr, nullptr);
    std::memcpy(ptr, "hello", 5);
    p0->commit(5);
    EXPECT_EQ(p0->size(), 5u);
    EXPECT_EQ(std::memcmp(p0->data(), "hello", 5), 0);

    p0->discard();
    EXPECT_EQ(p0->size(), 0u);
}

TEST(MultiplexerTests, StreamBufferAccessorTypeCompat) {
    stream_buffer_multiplexer mux(64);
    auto p0 = mux.add_producer(0, 1024, 16);

    static_assert(std::is_same_v<decltype(p0->stream_buffer()), slick::stream_buffer&>,
                  "producer_buffer::stream_buffer() must return slick::stream_buffer&");
    take_stream_buffer(p0->stream_buffer());

    publish_message(*p0, "x", 1);

    uint64_t cursor = 0;
    auto [ptr, len] = p0->stream_buffer().read(cursor);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, 1u);
    EXPECT_EQ(ptr[0], 'x');
}

// A registered producer_buffer is reachable two ways at once - by shared_ptr from
// get_producer_buffer() and by raw pointer from the multiplexer's dense lookup table - so moving
// one out from under the multiplexer would empty the shared_ptr members of an object both still
// point at, and the next consume() or read() would dereference a null buffer_. Nothing needs to
// move one, so the operation does not exist.
TEST(MultiplexerTests, ProducerBufferIsNeitherCopyableNorMovable) {
    using pb = stream_buffer_multiplexer::producer_buffer;
    static_assert(!std::is_move_constructible_v<pb>, "producer_buffer must not be movable");
    static_assert(!std::is_move_assignable_v<pb>, "producer_buffer must not be move-assignable");
    static_assert(!std::is_copy_constructible_v<pb>, "producer_buffer must not be copyable");
    static_assert(!std::is_copy_assignable_v<pb>, "producer_buffer must not be copy-assignable");
}

TEST(MultiplexerTests, StreamBufferPtrAccessorTypeCompat) {
    stream_buffer_multiplexer mux(64);
    auto p0 = mux.add_producer(0, 1024, 16);

    static_assert(std::is_same_v<decltype(p0->stream_buffer_ptr()), std::shared_ptr<slick::stream_buffer>>,
                  "producer_buffer::stream_buffer_ptr() must return std::shared_ptr<slick::stream_buffer>");

    std::shared_ptr<slick::stream_buffer> sp = p0->stream_buffer_ptr();
    ASSERT_NE(sp, nullptr);
    EXPECT_EQ(sp.get(), &p0->stream_buffer());
    EXPECT_GT(sp.use_count(), 1);  // shared with producer_buffer's own copy
}

// --------------------------------------------------------------------------------------------
// v2.0.0: the configuration moved from macros to a Traits template parameter
// --------------------------------------------------------------------------------------------

// The old SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION gated a std::atomic member of a
// header-only class, so sizeof() disagreed across a -DNDEBUG boundary and two translation units
// silently shared one definition. As a template argument the configuration is part of the type,
// so the same disagreement cannot compile through: these are different types.
TEST(MultiplexerTraitsTests, ConfigurationIsPartOfTheType) {
    static_assert(!std::is_same_v<counting_multiplexer, mux_test::silent_multiplexer>,
                  "differently-configured multiplexers must be different types");
    static_assert(std::is_same_v<stream_buffer_multiplexer,
                                 slick::basic_stream_buffer_multiplexer<slick::default_queue_traits>>,
                  "the plain name must alias the default configuration");

    // The counter is an unconditional member, so the layout does not vary with the configuration
    // at all - what varies is read()'s body. Either way sizeof no longer changes behind one name
    // across a -DNDEBUG boundary, which is what the old macro got wrong.
    EXPECT_EQ(sizeof(mux_test::silent_multiplexer), sizeof(counting_multiplexer));
}

// The record type is shared, not per-configuration: two differently-configured multiplexers have
// to agree on the element type of a shared record queue they both map.
TEST(MultiplexerTraitsTests, RecordTypeIsIndependentOfTraits) {
    static_assert(std::is_same_v<counting_multiplexer::record, mux_test::silent_multiplexer::record>,
                  "the shared-queue element type must not depend on Traits");
    static_assert(std::is_same_v<counting_multiplexer::record, slick::multiplexer_record>, "");
    static_assert(std::is_same_v<stream_buffer_multiplexer::multiplex_record, slick::multiplex_record>, "");
    EXPECT_EQ(sizeof(slick::multiplexer_record), 16u);
}

// Both loss terms are opt-in now. Records are skipped correctly either way; only the counter is
// silent - so the same run reports 6 with counting on and 0 with it off.
TEST(MultiplexerTraitsTests, LossCountersAreOptIn) {
    counting_multiplexer counting(64);
    auto cp = counting.add_producer(0, 1024, 4);   // tiny control ring: laps after 4 records
    mux_test::publish_bytes(*cp, 10);

    mux_test::silent_multiplexer silent(64);
    auto sp = silent.add_producer(0, 1024, 4);
    mux_test::publish_bytes(*sp, 10);

    auto drain = [](auto& mux) {
        std::vector<uint8_t> values;
        uint64_t cursor = 0;
        for (;;) {
            auto rec = mux.read(cursor);
            if (!rec) break;
            values.push_back(rec.data[0]);
        }
        return values;
    };

    const std::vector<uint8_t> expected{6, 7, 8, 9};
    EXPECT_EQ(drain(counting), expected);
    EXPECT_EQ(drain(silent), expected);  // identical delivery...

    EXPECT_EQ(counting.loss_count(), 6u);  // ...but only one of them says so
    EXPECT_EQ(silent.loss_count(), 0u);
}

// The multiplexer dereferences by jumping a fresh cursor to one exact sequence, where read()'s own
// count_loss would add the whole "how far has the producer run past this record" gap on every
// lapped dereference and double-count wildly. So it counts one loss per lapped record itself and
// leaves the producer's counter alone: the two numbers measure different things.
TEST(MultiplexerTraitsTests, DereferenceDoesNotTouchProducerLossCount) {
    counting_multiplexer mux(64);
    auto p0 = mux.add_producer(0, 1024, 4);
    mux_test::publish_bytes(*p0, 10);

    uint64_t cursor = 0;
    while (mux.read(cursor)) {
    }

    EXPECT_EQ(mux.loss_count(), 6u);     // six records lapped before they could be dereferenced
    EXPECT_EQ(p0->loss_count(), 0u);     // the inner counter is the caller's to drive, not ours
}

// ...and it is still driveable, but it answers a different question, which is why the multiplexer
// keeps its own. Over the identical ring - 10 records through a 4-slot control ring - a sequential
// scan lands on slot 0, finds seq 8 in it, and jumps the cursor straight there: it skips 8 and
// recovers only {8, 9}. The multiplexer instead asks each shared-queue record for its own exact
// sequence, so it recovers all four records still resident, {6, 7, 8, 9}, and loses 6. Both counts
// are right for what they measure - which is exactly why adding read()'s number to a jump-read
// would be meaningless.
TEST(MultiplexerTraitsTests, ProducerLossCountMovesForACountingScan) {
    counting_multiplexer mux(64);
    auto p0 = mux.add_producer(0, 1024, 4);
    mux_test::publish_bytes(*p0, 10);

    std::vector<uint8_t> scanned;
    uint64_t cursor = 0;
    for (;;) {
        auto [data, length] = p0->stream_buffer().read<counting_read_traits>(cursor);
        if (data == nullptr) break;
        scanned.push_back(data[0]);
    }
    EXPECT_EQ(scanned, (std::vector<uint8_t>{8, 9}));
    EXPECT_EQ(p0->loss_count(), 8u);

    std::vector<uint8_t> dereferenced;
    uint64_t mux_cursor = 0;
    for (;;) {
        auto rec = mux.read(mux_cursor);
        if (!rec) break;
        dereferenced.push_back(rec.data[0]);
    }
    EXPECT_EQ(dereferenced, (std::vector<uint8_t>{6, 7, 8, 9}));
    EXPECT_EQ(mux.loss_count(), 6u);

    // The default read traits do not count, which is a semantic difference and not just a speed
    // one: the same scan on a fresh producer skips the same records and reports nothing.
    auto p1 = mux.add_producer(1, 1024, 4);
    mux_test::publish_bytes(*p1, 10);
    uint64_t plain_cursor = 0;
    while (p1->stream_buffer().read(plain_cursor).first != nullptr) {
    }
    EXPECT_EQ(p1->loss_count(), 0u);
}

// --------------------------------------------------------------------------------------------
// v2.0.0: consume() reports an oversized record instead of truncating it
// --------------------------------------------------------------------------------------------

// A record's length field is 32 bits. Release used to cast the length down and publish a wrong
// one; now consume() throws before any state moves - which for the multiplexer must also mean
// nothing was fanned into the shared record queue.
TEST(MultiplexerTests, OversizedConsumeThrowsAndPublishesNothing) {
    constexpr bool kSizeTHoldsFourGiB = sizeof(std::size_t) > 4;
    if (!kSizeTHoldsFourGiB) {
        GTEST_SKIP() << "a 4 GiB ring cannot be addressed on a 32-bit build";
    }
    constexpr uint64_t kCapacity = 1ull << 32;  // 4 GiB ring - pages stay untouched below
    constexpr uint64_t kMaxRecord = 0xFFFFFFFFull;

    stream_buffer_multiplexer mux(64);
    std::shared_ptr<stream_buffer_multiplexer::producer_buffer> p0;
    try {
        p0 = mux.add_producer(0, kCapacity, 16);
    } catch (const std::bad_alloc&) {
        GTEST_SKIP() << "not enough memory for a 4 GiB ring";
    }

    auto [ptr, sz] = p0->prepare(kCapacity);
    ASSERT_NE(ptr, nullptr);
    p0->commit(kCapacity);
    ASSERT_EQ(p0->size(), kCapacity);

    EXPECT_THROW(p0->consume(kCapacity), std::length_error);

    // Nothing published in either ring, and the bytes are still there to publish in pieces that fit.
    EXPECT_EQ(p0->size(), kCapacity);
    uint64_t cursor = 0;
    EXPECT_FALSE(mux.read(cursor));

    const auto rec = p0->consume(kMaxRecord);  // the largest record that fits, exactly
    ASSERT_TRUE(static_cast<bool>(rec));
    EXPECT_EQ(rec.length, kMaxRecord);

    auto delivered = mux.read(cursor);
    ASSERT_TRUE(delivered);
    EXPECT_EQ(delivered.length, kMaxRecord);
    EXPECT_EQ(delivered.producer_id, 0u);
}

// --------------------------------------------------------------------------------------------
// Enumerating every registered producer
// --------------------------------------------------------------------------------------------

// Ids straddle dense_lookup_limit_ deliberately: 9000 lives only in the map, the rest are also in
// the dense fast-path table, and the table returned here must carry every one of them exactly
// once. Order is the hash map's and explicitly not part of the contract, so this sorts first.
TEST(MultiplexerTests, GetProducerBuffersExposesEveryProducer) {
    stream_buffer_multiplexer mux(64);
    mux.add_producer(9000, 1024, 16);
    mux.add_producer(2, 1024, 16);
    mux.add_producer(0, 1024, 16);
    mux.add_producer(7, 1024, 16);

    const auto& all = mux.get_producer_buffers();
    ASSERT_EQ(all.size(), mux.producer_count());
    ASSERT_EQ(all.size(), 4u);

    std::vector<uint32_t> ids;
    for (const auto& [id, producer] : all) {
        ASSERT_NE(producer, nullptr);
        EXPECT_EQ(id, producer->producer_id());   // key and payload agree
        ids.push_back(id);
    }
    std::sort(ids.begin(), ids.end());
    EXPECT_EQ(ids, (std::vector<uint32_t>{0, 2, 7, 9000}));

    // The same objects the singular accessors hand out, not copies.
    for (const auto& [id, producer] : all) {
        EXPECT_EQ(producer.get(), mux.get_producer_buffer(id).get());
        EXPECT_EQ(producer.get(), mux.find_producer(id));
    }
}

// The point of returning the table by reference: no snapshot is built, so repeated calls hand back
// the same object rather than an equal one.
TEST(MultiplexerTests, GetProducerBuffersIsZeroCopy) {
    stream_buffer_multiplexer mux(64);
    mux.add_producer(0, 1024, 16);

    EXPECT_EQ(&mux.get_producer_buffers(), &mux.get_producer_buffers());

    // It tracks registrations rather than being a frozen copy.
    EXPECT_EQ(mux.get_producer_buffers().size(), 1u);
    mux.add_producer(1, 1024, 16);
    EXPECT_EQ(mux.get_producer_buffers().size(), 2u);
}

TEST(MultiplexerTests, GetProducerBuffersOnEmptyMultiplexer) {
    stream_buffer_multiplexer mux(64);
    EXPECT_TRUE(mux.get_producer_buffers().empty());
}

// The reference dies with the multiplexer, but the shared_ptr elements do not: copy them out and
// the producers - and the shared record queue they hold - stay alive and usable.
TEST(MultiplexerTests, ProducersCopiedOutOfTheTableOutliveTheMultiplexer) {
    std::vector<std::shared_ptr<stream_buffer_multiplexer::producer_buffer>> kept;
    {
        stream_buffer_multiplexer mux(64);
        mux.add_producer(0, 1024, 16);
        mux.add_producer(1, 1024, 16);
        for (const auto& [id, producer] : mux.get_producer_buffers()) {
            (void)id;
            kept.push_back(producer);
        }
    }

    ASSERT_EQ(kept.size(), 2u);
    for (const auto& producer : kept) {
        publish_message(*producer, "late", 4);   // consume() still works with the multiplexer gone
    }
}

// Accessible from a const multiplexer - the usual shape for reporting code. Note const does not
// propagate to the producers, which is the documented trade for handing back the table itself.
TEST(MultiplexerTests, GetProducerBuffersIsAvailableOnAConstMultiplexer) {
    stream_buffer_multiplexer mux(64);
    mux.add_producer(3, 1024, 16);

    const auto& const_mux = mux;
    const auto& all = const_mux.get_producer_buffers();
    static_assert(std::is_same_v<decltype(all), const stream_buffer_multiplexer::producer_map&>,
                  "get_producer_buffers() must hand back the table by reference");
    ASSERT_EQ(all.size(), 1u);
    EXPECT_EQ(all.at(3)->capacity(), 1024u);
}
