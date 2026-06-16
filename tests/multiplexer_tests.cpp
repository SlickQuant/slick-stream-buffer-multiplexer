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
#include <slick/stream_buffer_multiplexer.h>

#include <atomic>
#include <cstring>
#include <type_traits>

using slick::stream_buffer_multiplexer;

namespace {

// prepare + write + commit + consume in one step
void publish_message(stream_buffer_multiplexer::producer_buffer& pb, const void* src, std::size_t n) {
    auto [ptr, sz] = pb.prepare(n);
    ASSERT_NE(ptr, nullptr);
    ASSERT_GE(sz, n);
    std::memcpy(ptr, src, n);
    pb.commit(n);
    pb.consume(n);
}

void take_stream_buffer(slick::stream_buffer&) {}

}  // namespace

TEST(MultiplexerTests, EmptyMultiplexerReadReturnsFalsy) {
    stream_buffer_multiplexer mux(64);

    EXPECT_EQ(mux.producer_slot_count(), 0u);
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

    EXPECT_EQ(mux.producer_slot_count(), 3u);
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
