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

#pragma once

#include <gtest/gtest.h>
#include <slick/stream_buffer_multiplexer.hpp>

#include <cstddef>
#include <cstring>

namespace mux_test {

// The multiplexer takes slick::queue_traits, so these need no traits structs of their own - the
// stock counting and silent configurations are exactly what the tests want. Naming them matters:
// slick::default_queue_traits switches on the build type, so a test that asserts an exact loss
// count has to pin its configuration to get the same number out of Debug and Release.

/// Both loss counters on, regardless of NDEBUG.
using counting_multiplexer = slick::basic_stream_buffer_multiplexer<slick::debug_queue_traits>;

/// Both loss counters off, regardless of NDEBUG - the counterpart used to assert that a silent
/// counter really does read 0 while records are still skipped correctly.
using silent_multiplexer = slick::basic_stream_buffer_multiplexer<slick::queue_traits>;

/// Read the producer's own ring with counting on. slick::read_traits does not count by default, so
/// producer_buffer::loss_count() only moves for a read made through traits like these.
struct counting_read_traits : slick::read_traits {
    static constexpr bool count_loss = true;
};

/// Publish one message through either kind of producer handle in one step: reserve + write +
/// publish for a producer_queue, prepare + write + commit + consume for a producer_buffer. One
/// name for both so a test reads the same whichever ring is underneath.
template <typename Producer>
void publish_message(Producer& producer, const void* src, std::size_t n) {
    if constexpr (requires { producer.reserve(1u); }) {
        const auto size = static_cast<uint32_t>(n);
        const uint64_t index = producer.reserve(size);
        std::memcpy(producer[index], src, n);
        producer.publish(index, size);
    } else {
        auto [ptr, sz] = producer.prepare(n);
        ASSERT_NE(ptr, nullptr);
        ASSERT_GE(sz, n);
        std::memcpy(ptr, src, n);
        producer.commit(n);
        producer.consume(n);
    }
}

/// Publish `count` single-byte messages numbered 0, 1, 2, ... - enough to lap a small ring.
template <typename Producer>
void publish_bytes(Producer& producer, int count) {
    for (int i = 0; i < count; ++i) {
        const auto value = static_cast<uint8_t>(i);
        publish_message(producer, &value, 1);
    }
}

}  // namespace mux_test
