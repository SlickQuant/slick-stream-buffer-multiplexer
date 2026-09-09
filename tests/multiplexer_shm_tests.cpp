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

#include <cstring>
#include <stdexcept>

using slick::stream_buffer_multiplexer;
using mux_test::publish_message;

TEST(MultiplexerShmTests, ShmRoundtrip) {
    stream_buffer_multiplexer creator(64, "mux_roundtrip_records");
    auto c0 = creator.add_producer(0, 1024, 16, "mux_roundtrip_p0");
    auto c1 = creator.add_producer(1, 2048, 32, "mux_roundtrip_p1");

    stream_buffer_multiplexer opener("mux_roundtrip_records");
    auto o0 = opener.add_producer(0, "mux_roundtrip_p0");
    auto o1 = opener.add_producer(1, "mux_roundtrip_p1");

    EXPECT_EQ(o0->capacity(), 1024u);
    EXPECT_EQ(o0->control_size(), 16u);
    EXPECT_TRUE(o0->use_shm());
    EXPECT_FALSE(o0->own_buffer());

    EXPECT_EQ(o1->capacity(), 2048u);
    EXPECT_EQ(o1->control_size(), 32u);

    publish_message(*c0, "hello", 5);
    publish_message(*c1, "world!", 6);

    uint64_t cursor = 0;
    auto r0 = opener.read(cursor);
    ASSERT_TRUE(r0);
    EXPECT_EQ(r0.producer_id, 0u);
    EXPECT_EQ(r0.length, 5u);
    EXPECT_EQ(std::memcmp(r0.data, "hello", 5), 0);

    auto r1 = opener.read(cursor);
    ASSERT_TRUE(r1);
    EXPECT_EQ(r1.producer_id, 1u);
    EXPECT_EQ(r1.length, 6u);
    EXPECT_EQ(std::memcmp(r1.data, "world!", 6), 0);

    EXPECT_FALSE(opener.read(cursor));
    EXPECT_EQ(opener.loss_count(), 0u);
}

TEST(MultiplexerShmTests, ShmPartialProducerVisibility) {
    stream_buffer_multiplexer creator(64, "mux_partial_records");
    auto c0 = creator.add_producer(0, 1024, 16, "mux_partial_p0");
    auto c1 = creator.add_producer(1, 2048, 32);  // local-only - not visible to the opener

    stream_buffer_multiplexer opener("mux_partial_records");
    opener.add_producer(0, "mux_partial_p0");
    EXPECT_TRUE(opener.has_producer(0));
    EXPECT_FALSE(opener.has_producer(1));

    publish_message(*c0, "aaa", 3);
    publish_message(*c1, "bbbb", 4);
    publish_message(*c0, "ccccc", 5);

    uint64_t cursor = 0;

    auto r0 = opener.read(cursor);
    ASSERT_TRUE(r0);
    EXPECT_EQ(r0.producer_id, 0u);
    EXPECT_EQ(r0.length, 3u);
    EXPECT_EQ(std::memcmp(r0.data, "aaa", 3), 0);

    // producer 1's record is unregistered on the opener side -> silently skipped, not loss
    auto r1 = opener.read(cursor);
    ASSERT_TRUE(r1);
    EXPECT_EQ(r1.producer_id, 0u);
    EXPECT_EQ(r1.length, 5u);
    EXPECT_EQ(std::memcmp(r1.data, "ccccc", 5), 0);

    EXPECT_FALSE(opener.read(cursor));
    EXPECT_EQ(opener.loss_count(), 0u);
}

TEST(MultiplexerShmTests, ShmDuplicateProducerIdThrows) {
    stream_buffer_multiplexer mux(64, "mux_dup_records");
    mux.add_producer(0, 1024, 16, "mux_dup_p0");

    EXPECT_THROW(mux.add_producer(0, 2048, 32), std::invalid_argument);
    EXPECT_THROW(mux.add_producer(0, "mux_dup_p0"), std::invalid_argument);
}

TEST(MultiplexerShmTests, ShmGeometryMismatchThrows) {
    stream_buffer_multiplexer creator1(64, "mux_geo_mismatch_records");
    creator1.add_producer(0, 1024, 16, "mux_geo_mismatch_p0");

    // a second "creator" with the same shared-queue name but different size
    EXPECT_THROW(stream_buffer_multiplexer(128, "mux_geo_mismatch_records"), std::runtime_error);

    // a second "creator" producer with the same shm name but different geometry
    stream_buffer_multiplexer creator2(64, "mux_geo_mismatch_records2");
    EXPECT_THROW(creator2.add_producer(0, 2048, 16, "mux_geo_mismatch_p0"), std::runtime_error);
}

TEST(MultiplexerShmTests, ShmOpenerWithoutCreatorThrows) {
    EXPECT_THROW(stream_buffer_multiplexer("mux_nonexistent_records"), std::runtime_error);

    stream_buffer_multiplexer mux(64, "mux_opener_no_producer_records");
    EXPECT_THROW(mux.add_producer(0, "mux_nonexistent_p0"), std::runtime_error);
}

// --------------------------------------------------------------------------------------------
// v2.0.0: explicit stale-segment recovery
// --------------------------------------------------------------------------------------------

// remove() is the documented recovery step for a name a dead run left behind. The multiplexer
// creates two kinds of segment - the shared record queue and each producer stream_buffer - and one
// remove() covers both, since both are plain slick::shm segments underneath.
//
// The return value is deliberately not asserted here, because it is not portable for a segment
// this test shut down cleanly: on POSIX slick::queue and slick::stream_buffer already unlink an
// owned segment in their destructors, so by this point the names are gone and remove() reports
// false; on Windows it is a no-op that always reports true. What is portable - and what recovery
// code actually depends on - is the outcome: after remove(), a creator over the same name gets a
// segment it genuinely owns, carrying none of the previous run's records.
TEST(MultiplexerShmTests, RemoveClearsBothKindsOfSegment) {
    {
        stream_buffer_multiplexer first(64, "mux_remove_records");
        auto p0 = first.add_producer(0, 1024, 16, "mux_remove_p0");
        publish_message(*p0, "stale", 5);
    }

    stream_buffer_multiplexer::remove("mux_remove_records");
    stream_buffer_multiplexer::remove("mux_remove_p0");

    stream_buffer_multiplexer second(64, "mux_remove_records");
    auto p0 = second.add_producer(0, 1024, 16, "mux_remove_p0");
    EXPECT_TRUE(p0->use_shm());
    EXPECT_TRUE(p0->own_buffer());  // a fresh segment, not an attach to the old one

    uint64_t cursor = second.initial_reading_index();
    publish_message(*p0, "fresh", 5);

    auto rec = second.read(cursor);
    ASSERT_TRUE(rec);
    EXPECT_EQ(rec.length, 5u);
    EXPECT_EQ(std::memcmp(rec.data, "fresh", 5), 0);
    EXPECT_FALSE(second.read(cursor));
}

// A segment that no owner ever unlinked is the case remove() exists for, and there the answer is
// the same everywhere: true on POSIX because the name was really there, true on Windows because
// remove() is a no-op. slick::shm::shared_memory is used directly because its destructor closes
// the handle without unlinking - which is exactly the state a process killed mid-run leaves.
TEST(MultiplexerShmTests, RemoveReportsTrueForALeftoverSegment) {
    {
        slick::shm::shared_memory leftover("mux_leftover_records", 4096, slick::shm::open_or_create,
                                           slick::shm::access_mode::read_write);
        ASSERT_NE(leftover.data(), nullptr);
    }

    EXPECT_TRUE(stream_buffer_multiplexer::remove("mux_leftover_records"));
}

// A name nothing ever created is not an error to remove - recovery code should be able to call it
// unconditionally before creating.
TEST(MultiplexerShmTests, RemoveIsSafeOnAnUnknownName) {
    stream_buffer_multiplexer::remove("mux_never_created_records");  // must not throw

    stream_buffer_multiplexer mux(64, "mux_never_created_records");
    auto p0 = mux.add_producer(0, 1024, 16);
    uint64_t cursor = 0;
    publish_message(*p0, "ok", 2);
    ASSERT_TRUE(mux.read(cursor));
}

// --------------------------------------------------------------------------------------------
// The documented limit of partial-visibility loss accounting
// --------------------------------------------------------------------------------------------

// Skipping an unregistered producer's entry is not counted as loss - but that filter can only
// apply to entries this instance actually reads. Shared-queue wrap loss is counted before any
// producer_id is known: the slot has been overwritten, so nothing can say whose record it held.
// A consumer that registers a subset of producers therefore gets an upper bound, not its own
// loss. This pins that so the number is never mistaken for attribution.
TEST(MultiplexerShmTests, WrapLossCountsUnregisteredProducersToo) {
    mux_test::counting_multiplexer creator(4, "mux_attrib_records");  // four slots: wraps quickly
    auto c17 = creator.add_producer(17, 1024, 16, "mux_attrib_p17");

    // A consumer that deliberately never registers producer 17.
    mux_test::counting_multiplexer opener("mux_attrib_records");
    EXPECT_FALSE(opener.has_producer(17));

    for (int i = 0; i < 10; ++i) {
        const auto value = static_cast<uint8_t>(i);
        publish_message(*c17, &value, 1);
    }

    uint64_t cursor = 0;
    EXPECT_FALSE(opener.read(cursor));  // every entry it can see names a producer it skips

    // Not 0. A reader starting at cursor 0 finds slot 0 already rewritten by record 8, jumps
    // there, and counts the 8 it passed - before any producer_id is in hand, because those slots
    // are exactly the ones no longer there to name one. Only the 2 entries it can still reach
    // (records 8 and 9) get the registration filter, and those are skipped without counting.
    // 8 counted + 2 skipped = the 10 published.
    EXPECT_EQ(opener.loss_count(), 8u);
}
