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

#include <array>
#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

using mux_test::counting_multiplexer;

namespace {

struct payload {
    uint32_t producer_id;
    uint64_t seq;
};

}  // namespace

TEST(MultiplexerMpmcTests, MultiThreadedBroadcast) {
    constexpr uint32_t kProducers = 4;
    constexpr int kMessagesPerProducer = 200;
    constexpr int kConsumers = 3;
    constexpr int kTotal = kProducers * kMessagesPerProducer;

    // all add_producer calls happen here, single-threaded, before any thread starts
    counting_multiplexer mux(1024);  // counting, so the loss_count() == 0 below has teeth in Release too
    for (uint32_t pid = 0; pid < kProducers; ++pid) {
        mux.add_producer(pid, 1 << 16, 256);
    }

    std::vector<std::thread> producers;
    for (uint32_t pid = 0; pid < kProducers; ++pid) {
        producers.emplace_back([&mux, pid] {
            auto pb = mux.get_producer_buffer(pid);
            for (uint64_t seq = 0; seq < kMessagesPerProducer; ++seq) {
                payload data{pid, seq};
                auto [ptr, sz] = pb->prepare(sizeof(data));
                ASSERT_NE(ptr, nullptr);
                ASSERT_GE(sz, sizeof(data));
                std::memcpy(ptr, &data, sizeof(data));
                pb->commit(sizeof(data));
                pb->consume(sizeof(data));
            }
        });
    }

    std::vector<std::thread> consumers;
    // Not std::vector<bool>: it packs its elements into shared words, so two consumer threads
    // clearing different flags would be a read-modify-write on the same word - a data race, and
    // the one container where writing distinct elements concurrently is not safe. std::vector<int>
    // below is fine, since its elements are distinct memory locations and the joins order them.
    std::array<std::atomic<bool>, kConsumers> ok;
    for (auto& flag : ok) {
        flag.store(true, std::memory_order_relaxed);
    }
    std::vector<int> received(kConsumers, 0);
    for (int c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&, c] {
            uint64_t cursor = 0;
            std::vector<uint64_t> expected(kProducers, 0);
            int count = 0;
            while (count < kTotal) {
                auto rec = mux.read(cursor);
                if (!rec) {
                    std::this_thread::yield();
                    continue;
                }
                if (rec.length != sizeof(payload) || rec.producer_id >= kProducers) {
                    ok[c].store(false, std::memory_order_relaxed);
                    break;
                }
                payload data;
                std::memcpy(&data, rec.data, sizeof(data));
                if (data.producer_id != rec.producer_id || data.seq != expected[rec.producer_id]) {
                    ok[c].store(false, std::memory_order_relaxed);
                    break;
                }
                ++expected[rec.producer_id];
                ++count;
            }
            received[c] = count;
        });
    }

    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    for (int c = 0; c < kConsumers; ++c) {
        EXPECT_TRUE(ok[c].load(std::memory_order_relaxed))
            << "consumer " << c << " observed unexpected data";
        EXPECT_EQ(received[c], kTotal);
    }
    EXPECT_EQ(mux.loss_count(), 0u);
    for (uint32_t pid = 0; pid < kProducers; ++pid) {
        EXPECT_EQ(mux.get_producer_buffer(pid)->loss_count(), 0u);
    }
}

TEST(MultiplexerMpmcTests, WorkStealingSharedCursor) {
    constexpr uint32_t kProducers = 3;
    constexpr int kMessagesPerProducer = 150;
    constexpr int kConsumers = 4;
    constexpr int kTotal = kProducers * kMessagesPerProducer;

    counting_multiplexer mux(1024);  // counting, so the loss_count() == 0 below has teeth in Release too
    for (uint32_t pid = 0; pid < kProducers; ++pid) {
        mux.add_producer(pid, 1 << 16, 256);
    }

    std::vector<std::thread> producers;
    for (uint32_t pid = 0; pid < kProducers; ++pid) {
        producers.emplace_back([&mux, pid] {
            auto pb = mux.get_producer_buffer(pid);
            for (uint64_t seq = 0; seq < kMessagesPerProducer; ++seq) {
                payload data{pid, seq};
                auto [ptr, sz] = pb->prepare(sizeof(data));
                ASSERT_NE(ptr, nullptr);
                ASSERT_GE(sz, sizeof(data));
                std::memcpy(ptr, &data, sizeof(data));
                pb->commit(sizeof(data));
                pb->consume(sizeof(data));
            }
        });
    }
    for (auto& t : producers) t.join();

    std::atomic<uint64_t> shared_cursor{0};
    std::atomic<int> total_count{0};
    std::vector<std::vector<uint64_t>> seen(kConsumers);

    std::vector<std::thread> consumers;
    for (int c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&, c] {
            for (;;) {
                if (total_count.load(std::memory_order_relaxed) >= kTotal) break;
                auto rec = mux.read(shared_cursor);
                if (!rec) {
                    if (total_count.load(std::memory_order_relaxed) >= kTotal) break;
                    std::this_thread::yield();
                    continue;
                }
                ASSERT_EQ(rec.length, sizeof(payload));
                payload data;
                std::memcpy(&data, rec.data, sizeof(data));
                seen[c].push_back(data.producer_id * static_cast<uint64_t>(kMessagesPerProducer) + data.seq);
                total_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& t : consumers) t.join();

    std::vector<int> union_count(kTotal, 0);
    int total = 0;
    for (auto& v : seen) {
        for (auto idx : v) {
            ++union_count[idx];
            ++total;
        }
    }
    EXPECT_EQ(total, kTotal);
    for (int i = 0; i < kTotal; ++i) {
        EXPECT_EQ(union_count[i], 1) << "global index " << i;
    }
    EXPECT_EQ(mux.loss_count(), 0u);
}

TEST(MultiplexerMpmcTests, SharedQueueWrapCausesLoss) {
    // counting_multiplexer, not the default: the shared-queue wrap counter is opt-in through
    // Traits::shared_queue_traits, and the default configuration follows NDEBUG.
    counting_multiplexer mux(4);                      // tiny shared queue: holds only 4 records
    auto p0 = mux.add_producer(0, 1 << 16, 256);  // generous producer ring: no inner lap

    for (int i = 0; i < 10; ++i) {
        auto [ptr, sz] = p0->prepare(1);
        ASSERT_NE(ptr, nullptr);
        *ptr = static_cast<uint8_t>(i);
        p0->commit(1);
        p0->consume(1);
    }

    uint64_t cursor = 0;
    auto rec = mux.read(cursor);
    ASSERT_TRUE(rec);
    EXPECT_EQ(rec.producer_id, 0u);
    EXPECT_EQ(rec.length, 1u);
    EXPECT_EQ(rec.data[0], 8);  // shared queue of size 4: slot 0 was last written by record 8
    EXPECT_EQ(mux.loss_count(), 8u);
}

TEST(MultiplexerMpmcTests, ProducerRingLapCausesMultiplexerLoss) {
    counting_multiplexer mux(64);        // generous shared queue: no shared-queue loss
    auto p0 = mux.add_producer(0, 1024, 4);   // tiny control ring: laps after 4 records

    for (int i = 0; i < 10; ++i) {
        auto [ptr, sz] = p0->prepare(1);
        ASSERT_NE(ptr, nullptr);
        *ptr = static_cast<uint8_t>(i);
        p0->commit(1);
        p0->consume(1);
    }

    uint64_t cursor = 0;
    std::vector<uint8_t> values;
    for (;;) {
        auto rec = mux.read(cursor);
        if (!rec) break;
        ASSERT_EQ(rec.length, 1u);
        values.push_back(rec.data[0]);
    }

    // only the last 4 published records (6..9) are still present in the producer's control ring
    EXPECT_EQ(values, (std::vector<uint8_t>{6, 7, 8, 9}));
    EXPECT_EQ(mux.loss_count(), 6u);
}
