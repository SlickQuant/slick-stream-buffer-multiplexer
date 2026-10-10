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
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

using slick::stream_buffer_multiplexer;
using mux_test::publish_message;

/// Nothing unlinks a segment on destruction, so on POSIX every name a test uses would outlive the
/// test and hand the next run a segment still holding this run's records. shm() unlinks a name the
/// first time a test uses it, clearing whatever a crashed run left behind, and TearDown() unlinks
/// it again once the test's multiplexers are gone. slick::shm is called directly so the fixture
/// does not lean on the remove() these tests exercise.
class MultiplexerShmTests : public ::testing::Test {
protected:
    const char* shm(const char* name) {
        for (const auto& used : names_) {
            if (used == name) {
                return name;
            }
        }
        slick::shm::shared_memory::remove(name);
        names_.emplace_back(name);
        return name;
    }

    void TearDown() override {
        for (const auto& name : names_) {
            slick::shm::shared_memory::remove(name.c_str());
        }
    }

private:
    std::vector<std::string> names_;
};

TEST_F(MultiplexerShmTests, ShmRoundtrip) {
    stream_buffer_multiplexer creator(64, shm("mux_roundtrip_records"));
    auto c0 = creator.add_producer(0, 1024, 16, shm("mux_roundtrip_p0"));
    auto c1 = creator.add_producer(1, 2048, 32, shm("mux_roundtrip_p1"));

    stream_buffer_multiplexer opener(shm("mux_roundtrip_records"));
    auto o0 = opener.add_producer(0, shm("mux_roundtrip_p0"));
    auto o1 = opener.add_producer(1, shm("mux_roundtrip_p1"));

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

TEST_F(MultiplexerShmTests, ShmPartialProducerVisibility) {
    stream_buffer_multiplexer creator(64, shm("mux_partial_records"));
    auto c0 = creator.add_producer(0, 1024, 16, shm("mux_partial_p0"));
    auto c1 = creator.add_producer(1, 2048, 32);  // local-only - not visible to the opener

    stream_buffer_multiplexer opener(shm("mux_partial_records"));
    opener.add_producer(0, shm("mux_partial_p0"));
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

TEST_F(MultiplexerShmTests, ShmDuplicateProducerIdThrows) {
    stream_buffer_multiplexer mux(64, shm("mux_dup_records"));
    mux.add_producer(0, 1024, 16, shm("mux_dup_p0"));

    EXPECT_THROW(mux.add_producer(0, 2048, 32), std::invalid_argument);
    EXPECT_THROW(mux.add_producer(0, shm("mux_dup_p0")), std::invalid_argument);
}

TEST_F(MultiplexerShmTests, ShmGeometryMismatchThrows) {
    stream_buffer_multiplexer creator1(64, shm("mux_geo_mismatch_records"));
    creator1.add_producer(0, 1024, 16, shm("mux_geo_mismatch_p0"));

    // a second "creator" with the same shared-queue name but different size
    EXPECT_THROW(stream_buffer_multiplexer(128, shm("mux_geo_mismatch_records")), std::runtime_error);

    // a second "creator" producer with the same shm name but different geometry
    stream_buffer_multiplexer creator2(64, shm("mux_geo_mismatch_records2"));
    EXPECT_THROW(creator2.add_producer(0, 2048, 16, shm("mux_geo_mismatch_p0")), std::runtime_error);
}

TEST_F(MultiplexerShmTests, ShmOpenerWithoutCreatorThrows) {
    EXPECT_THROW(stream_buffer_multiplexer(shm("mux_nonexistent_records")), std::runtime_error);

    stream_buffer_multiplexer mux(64, shm("mux_opener_no_producer_records"));
    EXPECT_THROW(mux.add_producer(0, shm("mux_nonexistent_p0")), std::runtime_error);
}

// --------------------------------------------------------------------------------------------
// v2.0.0: explicit stale-segment recovery
// --------------------------------------------------------------------------------------------

// remove() is the documented cleanup and recovery step for a name a previous run left behind. The
// multiplexer creates two kinds of segment - the shared record queue and each producer
// stream_buffer - and one remove() covers both, since both are plain slick::shm segments underneath.
//
// The return value is portable now that nothing unlinks on destruction: on POSIX both names are
// still there after the first multiplexer is gone, so remove() really unlinks them, and on Windows
// it is a no-op that always reports true. What recovery code depends on is the outcome: after
// remove(), a creator over the same name gets a segment it genuinely owns, carrying none of the
// previous run's records.
TEST_F(MultiplexerShmTests, RemoveClearsBothKindsOfSegment) {
    {
        stream_buffer_multiplexer first(64, shm("mux_remove_records"));
        auto p0 = first.add_producer(0, 1024, 16, shm("mux_remove_p0"));
        publish_message(*p0, "stale", 5);
    }

    EXPECT_TRUE(stream_buffer_multiplexer::remove(shm("mux_remove_records")));
    EXPECT_TRUE(stream_buffer_multiplexer::remove(shm("mux_remove_p0")));

    stream_buffer_multiplexer second(64, shm("mux_remove_records"));
    auto p0 = second.add_producer(0, 1024, 16, shm("mux_remove_p0"));
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

// remove() does not need the segment to be a valid queue or stream_buffer - which is what makes it
// the recovery step for one a creator died while initializing. slick::shm::shared_memory is used
// directly to leave exactly that: a name that exists and holds nothing either class would accept.
TEST_F(MultiplexerShmTests, RemoveReportsTrueForALeftoverSegment) {
    {
        slick::shm::shared_memory leftover(shm("mux_leftover_records"), 4096, slick::shm::open_or_create,
                                           slick::shm::access_mode::read_write);
        ASSERT_NE(leftover.data(), nullptr);
    }

    EXPECT_TRUE(stream_buffer_multiplexer::remove(shm("mux_leftover_records")));
}

// A name nothing ever created is not an error to remove - recovery code should be able to call it
// unconditionally before creating.
TEST_F(MultiplexerShmTests, RemoveIsSafeOnAnUnknownName) {
    stream_buffer_multiplexer::remove(shm("mux_never_created_records"));  // must not throw

    stream_buffer_multiplexer mux(64, shm("mux_never_created_records"));
    auto p0 = mux.add_producer(0, 1024, 16);
    uint64_t cursor = 0;
    publish_message(*p0, "ok", 2);
    ASSERT_TRUE(mux.read(cursor));
}

// --------------------------------------------------------------------------------------------
// v2.1.0: segments outlive their creator
// --------------------------------------------------------------------------------------------

// A creator that exits while a consumer is still attached must not take the names with it. Before
// slick-stream-buffer 2.1.0 and slick-queue 2.3.0 it unlinked them on POSIX, so the consumer kept
// reading an orphaned mapping while the restarted creator silently made fresh segments under the
// same names - and nothing published after the restart ever reached that consumer.
TEST_F(MultiplexerShmTests, RestartedCreatorRejoinsAttachedConsumer) {
    auto first = std::make_unique<stream_buffer_multiplexer>(64, shm("mux_restart_records"));
    auto p0 = first->add_producer(0, 1024, 16, shm("mux_restart_p0"));

    stream_buffer_multiplexer consumer(shm("mux_restart_records"));
    consumer.add_producer(0, shm("mux_restart_p0"));
    uint64_t cursor = consumer.initial_reading_index();

    publish_message(*p0, "before", 6);

    // The creator exits: the producer handle shares ownership of both segments, so it goes too.
    p0.reset();
    first.reset();

    stream_buffer_multiplexer second(64, shm("mux_restart_records"));
    auto restarted = second.add_producer(0, 1024, 16, shm("mux_restart_p0"));
    EXPECT_FALSE(restarted->own_buffer());  // attached to the surviving segment, not a fresh one

    publish_message(*restarted, "after!!", 7);

    auto r0 = consumer.read(cursor);
    ASSERT_TRUE(r0);
    EXPECT_EQ(r0.length, 6u);
    EXPECT_EQ(std::memcmp(r0.data, "before", 6), 0);

    auto r1 = consumer.read(cursor);
    ASSERT_TRUE(r1);
    EXPECT_EQ(r1.producer_id, 0u);
    EXPECT_EQ(r1.length, 7u);
    EXPECT_EQ(std::memcmp(r1.data, "after!!", 7), 0);

    EXPECT_FALSE(consumer.read(cursor));
}

#if !defined(_WIN32)
// With no process left attached the names still survive - there is no last-handle rule on POSIX to
// free them - so the next creator attaches to the old segments and finds the old records. This is
// why a run that wants a clean start calls remove() first. Windows frees a section with its last
// handle, so there the same sequence starts fresh and there is nothing to assert.
TEST_F(MultiplexerShmTests, SegmentsOutliveEveryMultiplexer) {
    {
        stream_buffer_multiplexer first(64, shm("mux_outlive_records"));
        auto p0 = first.add_producer(0, 1024, 16, shm("mux_outlive_p0"));
        EXPECT_TRUE(p0->own_buffer());
        publish_message(*p0, "kept", 4);
    }

    stream_buffer_multiplexer second(64, shm("mux_outlive_records"));
    auto p0 = second.add_producer(0, 1024, 16, shm("mux_outlive_p0"));
    EXPECT_FALSE(p0->own_buffer());

    uint64_t cursor = 0;
    auto rec = second.read(cursor);
    ASSERT_TRUE(rec);
    EXPECT_EQ(rec.length, 4u);
    EXPECT_EQ(std::memcmp(rec.data, "kept", 4), 0);
    EXPECT_FALSE(second.read(cursor));
}
#endif

// --------------------------------------------------------------------------------------------
// The documented limit of partial-visibility loss accounting
// --------------------------------------------------------------------------------------------

// Skipping an unregistered producer's entry is not counted as loss - but that filter can only
// apply to entries this instance actually reads. Shared-queue wrap loss is counted before any
// producer_id is known: the slot has been overwritten, so nothing can say whose record it held.
// A consumer that registers a subset of producers therefore gets an upper bound, not its own
// loss. This pins that so the number is never mistaken for attribution.
TEST_F(MultiplexerShmTests, WrapLossCountsUnregisteredProducersToo) {
    mux_test::counting_multiplexer creator(4, shm("mux_attrib_records"));  // four slots: wraps quickly
    auto c17 = creator.add_producer(17, 1024, 16, shm("mux_attrib_p17"));

    // A consumer that deliberately never registers producer 17.
    mux_test::counting_multiplexer opener(shm("mux_attrib_records"));
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
