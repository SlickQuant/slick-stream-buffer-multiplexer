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

#include <cstring>
#include <stdexcept>

using slick::stream_buffer_multiplexer;

namespace {

void publish_message(stream_buffer_multiplexer::producer_buffer& pb, const void* src, std::size_t n) {
    auto [ptr, sz] = pb.prepare(n);
    ASSERT_NE(ptr, nullptr);
    ASSERT_GE(sz, n);
    std::memcpy(ptr, src, n);
    pb.commit(n);
    pb.consume(n);
}

}  // namespace

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
