# slick-stream-buffer-multiplexer

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Header-only](https://img.shields.io/badge/header--only-yes-brightgreen.svg)](#installation)
[![Lock-free](https://img.shields.io/badge/concurrency-lock--free-orange.svg)](#architecture)

slick-stream-buffer-multiplexer is a header-only C++ library that fans multiple
independent byte streams into one ordered, lock-free, multi-producer
multi-consumer (MPMC) stream. It is the MPMC counterpart to
[slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer) (SPMC),
built by composing it with [slick-queue](https://github.com/SlickQuant/slick-queue)
(lock-free MPMC ring).

Each producer owns its own `slick::stream_buffer` - an independently-sized byte
ring with its own control ring - so producers can have completely different
message sizes and rates. A single shared `slick::queue<record>` merges every
producer's published messages into one globally-ordered stream that consumers
read as `(data pointer, length, producer_id)`.

## How it works

```
 producer 0 ──prepare/commit──▶ [ stream_buffer 0 ] ──consume(n)──┐
 producer 1 ──prepare/commit──▶ [ stream_buffer 1 ] ──consume(n)──┼─▶ {sequence, producer_id}
 producer N ──prepare/commit──▶ [ stream_buffer N ] ──consume(n)──┘        │
                                                                            ▼
                                                          [ shared queue<record> ]
                                                                            │
                                              consumer A (own cursor) ◀────┤  read(cursor) dereferences
                                              consumer B (own cursor) ◀────┤  {sequence, producer_id} back
                                              process C (shared memory) ◀──┘  into stream_buffer[producer_id]
```

Each `producer_buffer::consume(n)` does two things:

1. Publishes into its own `slick::stream_buffer`, exactly like
   `slick::stream_buffer::consume(n)` - returning the same
   `published_record{sequence, data, length}`.
2. Fans a tiny `{sequence, producer_id}` record into the shared
   `slick::queue<record>`.

`multiplexer.read(cursor)` dequeues the next `{sequence, producer_id}` record
and dereferences it back into `producers[producer_id]->stream_buffer().read(sequence)`,
returning a zero-copy view directly into that producer's ring.

## Features

- **Lock-free** MPMC fan-in of N independently-sized byte streams
- Each producer keeps **zero-copy** `(data, length, producer_id)` semantics
- Per-producer `.stream_buffer()` accessor returns a literal `slick::stream_buffer&`,
  so each producer is a drop-in [slick::dynamic_buffer](https://github.com/SlickQuant/slick-dynamic-buffer) target
- **Mix local-memory and shared-memory producers** under one multiplexer
- **Shared memory support** for inter-process communication
- **Cross-platform** - Windows, Linux, macOS
- Modern **C++20**

## Requirements

- C++20 compatible compiler
- [slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer) and
  [slick-queue](https://github.com/SlickQuant/slick-queue) (fetched automatically when not installed)

## Installation

Header-only. Add the `include` directory to your include path:

```cpp
#include <slick/stream_buffer_multiplexer.h>
```

### Using CMake FetchContent

```cmake
include(FetchContent)

set(BUILD_SLICK_STREAM_BUFFER_MULTIPLEXER_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    slick-stream-buffer-multiplexer
    GIT_REPOSITORY https://github.com/SlickQuant/slick-stream-buffer-multiplexer.git
    GIT_TAG v1.0.0 # See https://github.com/SlickQuant/slick-stream-buffer-multiplexer/releases for latest version
)
FetchContent_MakeAvailable(slick-stream-buffer-multiplexer)

target_link_libraries(your_target PRIVATE slick::stream_buffer_multiplexer)
```

## Usage

### Registering producers and publishing

```cpp
#include <slick/stream_buffer_multiplexer.h>

// shared record queue: 1024 slots (must be power of 2), local memory
slick::stream_buffer_multiplexer mux(1024);

// each producer gets its own independently-sized stream_buffer, registered
// by an explicit producer_id (single-threaded setup, before any threads start)
auto market_data = mux.add_producer(0, 1ull << 26, 1u << 16);  // 64 MB data ring, 64K records
auto order_events = mux.add_producer(1, 1ull << 16, 1u << 10); // 64 KB data ring, 1K records

// producer side: same prepare/commit/consume interface as slick::stream_buffer
auto [ptr, size] = market_data->prepare(64 * 1024);
std::size_t n = receive_bytes(ptr, size);
market_data->commit(n);
market_data->consume(n);   // publishes locally AND fans {sequence, producer_id} into the shared queue
```

### Consuming the merged stream

```cpp
uint64_t cursor = mux.initial_reading_index();  // or 0 to replay history
for (;;) {
    if (auto rec = mux.read(cursor)) {
        handle_message(rec.data, rec.length, rec.producer_id);  // zero-copy view
    }
}
```

A shared `std::atomic<uint64_t>` cursor gives work-stealing semantics across
consumer threads - each message is delivered to exactly one consumer:

```cpp
std::atomic<uint64_t> shared_cursor{0};
if (auto rec = mux.read(shared_cursor)) {
    handle_message(rec.data, rec.length, rec.producer_id);
}
```

### `slick::dynamic_buffer` interop

`producer_buffer::stream_buffer()` returns a literal `slick::stream_buffer&`,
so each producer can be wrapped directly:

```cpp
#include <slick/dynamic_buffer.h>

slick::dynamic_buffer dyn_buf(market_data->stream_buffer());
boost::asio::async_read(socket, dyn_buf, ...);
```

## Shared memory usage

The shared record queue and each producer choose local memory or shared memory
**independently**:

```cpp
// shared record queue: shm creator
slick::stream_buffer_multiplexer server(1024, "md_records");

// producer 0: shared memory (visible to other processes)
server.add_producer(0, 1ull << 26, 1u << 16, "md_p0");

// producer 1: local memory only (in-process consumers only)
server.add_producer(1, 1ull << 16, 1u << 10);
```

A consumer in another process opens the shared queue and registers only the
producer ids whose shared memory it has access to:

```cpp
slick::stream_buffer_multiplexer client("md_records");
client.add_producer(0, "md_p0");   // producer 1 is intentionally not registered here

uint64_t cursor = client.initial_reading_index();
auto rec = client.read(cursor);    // producer 0 records dereference normally;
                                    // producer 1 records are silently skipped and
                                    // do NOT count toward client.loss_count()
```

The `producer_id` -> shared-memory-name mapping is a caller convention (e.g. a
shared config), not enforced by the library. Records whose `producer_id` is out
of range or unregistered on a given multiplexer instance are silently skipped
(not counted as loss) - this is how a consumer naturally ignores producers it
doesn't (or can't) open.

## API Overview

### `stream_buffer_multiplexer`

```cpp
// shared record queue
explicit stream_buffer_multiplexer(uint32_t shared_queue_size);                       // local memory
stream_buffer_multiplexer(uint32_t shared_queue_size, const char* shm_queue_name);     // shm creator
explicit stream_buffer_multiplexer(const char* shm_queue_name);                        // shm opener

// producer registration (single-threaded setup, before producer/consumer threads start)
std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size);                       // local memory
std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size, const char* shm_name); // shm creator
std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, const char* shm_name);                                           // shm opener

bool has_producer(uint32_t producer_id) const noexcept;
std::shared_ptr<producer_buffer> get_producer_buffer(uint32_t producer_id); // shared ownership; nullptr if unregistered
size_t producer_slot_count() const noexcept;

multiplex_record read(uint64_t& cursor) noexcept;
multiplex_record read(std::atomic<uint64_t>& cursor) noexcept;  // work-stealing

uint64_t loss_count() const noexcept;
uint64_t initial_reading_index() const noexcept;
```

### `multiplex_record`

```cpp
struct multiplex_record {
    const uint8_t* data;
    uint32_t length;
    uint32_t producer_id;
    explicit operator bool() const noexcept;  // false if no message was available
};
```

### `producer_buffer`

Forwards the familiar `slick::stream_buffer` producer interface, plus
`consume()` which additionally fans a record into the shared queue:

```cpp
std::pair<uint8_t*, size_t> prepare(size_t n);
void commit(size_t n) noexcept;
published_record consume(size_t n) noexcept;  // same as slick::stream_buffer::consume
void discard() noexcept;
const uint8_t* data() const noexcept;
size_t size() const noexcept;

uint64_t capacity() const noexcept;
uint32_t control_size() const noexcept;
uint64_t loss_count() const noexcept;          // this producer's own ring loss
uint64_t initial_reading_index() const noexcept;
bool own_buffer() const noexcept;
bool use_shm() const noexcept;

slick::stream_buffer& stream_buffer() noexcept;  // for slick::dynamic_buffer etc.
std::shared_ptr<slick::stream_buffer> stream_buffer_ptr() noexcept;  // for slick-net's websocket_session etc.
uint32_t producer_id() const noexcept;
```

## Important Constraints

**Three independent loss counters.** `shared_queue_->loss_count()` (the shared
record queue wrapped before a consumer read its entry), `multiplexer.loss_count()`
(adds multiplexer-level loss: a shared-queue entry whose `producer_id` IS
registered on this instance but whose entry was lapped by that producer's own
ring before dereferencing), and each `producer_buffer::loss_count()` (that
producer's own ring lapped a slow consumer). `multiplexer.loss_count()` already
includes `shared_queue_->loss_count()`. Shared-queue entries whose `producer_id`
is *unregistered* on this instance are silently skipped and never counted as
loss - see [Shared memory usage](#shared-memory-usage).

**Configurable loss detection.** Like `slick::stream_buffer` and `slick::queue`,
the multiplexer-level loss counter compiles out when
`SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION` is `0` (default: `1` in
debug builds via `!defined(NDEBUG)`, `0` in release builds). When disabled,
`multiplexer.loss_count()` returns only `shared_queue_->loss_count()`. Define
the macro to `1`/`0` before including the header to override the default.

**Pointer invalidation.** Same as `slick::stream_buffer`: `prepare()` may
relocate the readable region, invalidating previous `data()`/`prepare()`
pointers. Pointers returned by `read()` stay valid until that producer's ring
laps them.

**Single producer thread per `producer_buffer`.** All producer-side methods
(`prepare`/`commit`/`consume`/`discard`) for a given `producer_buffer` must be
called from one thread, same as `slick::stream_buffer`. Different
`producer_buffer`s may be driven by different threads concurrently.

**`add_producer` is single-threaded setup only.** Register all producers before
starting any producer or consumer threads.

**Message size** is limited to < 4 GiB per record.

**Power-of-2 geometry.** `shared_queue_size`, each producer's `capacity`, and
`control_size` must all be powers of 2 (enforced by `slick::queue` and
`slick::stream_buffer`).

**Shared ownership and lifetime.** `producer_buffer` holds its `stream_buffer`
and the shared record queue via `shared_ptr`, so `stream_buffer_ptr()` (the
`stream_buffer` alone, e.g. for a `slick-net` `websocket_session`) and
`get_producer_buffer()` (the whole `producer_buffer`, including `consume()`)
can both safely outlive the `stream_buffer_multiplexer` that created them - the
shared record queue stays alive as long as any `producer_buffer` referencing it
does. Note that `stream_buffer_ptr()` alone does not keep the owning
`producer_buffer` (or its `consume()`) alive; use `get_producer_buffer()` for
that.

## Architecture

Built entirely from existing primitives - no new shared-memory layout. Each
producer is a complete `slick::stream_buffer` (own data ring + control ring,
independently sized). The shared `slick::queue<record>` (`record` = `{uint64_t
sequence; uint32_t producer_id; uint32_t pad0;}`, 16 bytes) is the lock-free MPMC
fan-in/merge point. `read(cursor)` copies `{sequence, producer_id}` out of the
shared queue. If `producer_id` is unregistered on this instance, the entry is
silently skipped (not counted as loss). Otherwise it dereferences
`producers[producer_id]->stream_buffer().read(sequence)`; an exact match
(`data != nullptr && local_cursor == sequence + 1`) returns the zero-copy view,
otherwise the entry is counted as multiplexer-level loss. Either way, the next
shared-queue entry is tried.

## Building and Testing

```bash
cmake -S . -B build
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
```

## License

SlickStreamBufferMultiplexer is released under the [MIT License](LICENSE).

**Made with ⚡ by [SlickQuant](https://github.com/SlickQuant)**
