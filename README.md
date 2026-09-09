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
- Each `producer_buffer` satisfies the `slick::dynamic_buffer` buffer concept and is a drop-in
  [`slick::dynamic_buffer<slick::stream_buffer_multiplexer::producer_buffer>`](https://github.com/SlickQuant/slick-dynamic-buffer) target
- **Mix local-memory and shared-memory producers** under one multiplexer
- **Shared memory support** for inter-process communication
- **Opt-in diagnostics** via a `Traits` template parameter - no ODR-hazardous feature macros
- **Cross-platform** - Windows, Linux, macOS
- Modern **C++20**

## Requirements

- C++20 compatible compiler
- CMake 3.21 or newer
- [slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer) >= 2.0.0 and
  [slick-queue](https://github.com/SlickQuant/slick-queue) >= 2.0.0 (fetched automatically when
  not installed). Both are required: v2.0.0 of this library is built on their traits-based
  configuration.

## Installation

Header-only. Add the `include` directory to your include path:

```cpp
#include <slick/stream_buffer_multiplexer.hpp>
```

### Using CMake FetchContent

```cmake
include(FetchContent)

set(BUILD_SLICK_STREAM_BUFFER_MULTIPLEXER_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    slick-stream-buffer-multiplexer
    GIT_REPOSITORY https://github.com/SlickQuant/slick-stream-buffer-multiplexer.git
    GIT_TAG v2.0.0 # See https://github.com/SlickQuant/slick-stream-buffer-multiplexer/releases for latest version
)
FetchContent_MakeAvailable(slick-stream-buffer-multiplexer)

target_link_libraries(your_target PRIVATE slick::stream_buffer_multiplexer)
```

## Usage

### Registering producers and publishing

```cpp
#include <slick/stream_buffer_multiplexer.hpp>

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
                                    // producer 1 records are silently skipped, and
                                    // skipping one is not itself counted as loss
```

The `producer_id` -> shared-memory-name mapping is a caller convention (e.g. a
shared config), not enforced by the library. Records whose `producer_id` is out
of range or unregistered on a given multiplexer instance are silently skipped -
this is how a consumer naturally ignores producers it doesn't (or can't) open.
Skipping one is not counted as loss, but note this filters only the
multiplexer-level term of `loss_count()`; see
[Important Constraints](#important-constraints) for what shared-queue wrap loss
cannot attribute.

For best performance, assign `producer_id` values contiguously starting at `0`
whenever practical. The multiplexer keeps low ids on a dense lookup fast path;
sparse or high `producer_id` values are still supported, but may fall back to a
slower hash lookup.

### Recovering a stale segment

A creator that dies mid-initialization leaves its segment wedged, and constructing over that
name reports it by throwing. Recovery is deliberately manual - nothing portable distinguishes
a dead creator from a slow one - so clear the name first:

```cpp
slick::stream_buffer_multiplexer::remove("md_records");  // shared record queue
slick::stream_buffer_multiplexer::remove("md_p0");       // a producer's stream_buffer

slick::stream_buffer_multiplexer server(1024, "md_records");
server.add_producer(0, 1ull << 26, 1u << 16, "md_p0");
```

One call covers both kinds of name, since both are `slick::shm` segments underneath. Only call
it when no process is using the segment: on POSIX the name is unlinked immediately, so the next
creator gets a fresh segment while any process still mapped to the old one keeps reading the
orphaned copy. On Windows it is a no-op returning `true` - a section there disappears once the
last handle closes.

## Compile-time configuration

`slick::stream_buffer_multiplexer` is an alias for `slick::basic_stream_buffer_multiplexer<>`.
Its optional features are a `Traits` template parameter, not macros - and the parameter is
[`slick::queue_traits`](https://github.com/SlickQuant/slick-queue), the traits of the shared
record queue it is built on:

```cpp
// stock configurations - no traits struct of your own needed
slick::basic_stream_buffer_multiplexer<slick::debug_queue_traits> counting(1024);  // counters on
slick::basic_stream_buffer_multiplexer<slick::queue_traits>       silent(1024);    // counters off

// or derive, e.g. to drop read_last() - the multiplexer never calls it, and it costs a CAS
// per publish. Note it is part of the queue's shared-memory feature nibble, so every process
// mapping one segment must agree.
struct lean : slick::queue_traits {
    static constexpr bool enable_read_last = false;
};
slick::basic_stream_buffer_multiplexer<lean> mux(1024);
```

The multiplexer has no traits type of its own on purpose. Everything else it exposes is a
passthrough to that queue, and its one real tunable asks the same question
`queue_traits::enable_loss_detection` already asks - so a separate flag would only let the two
disagree. That matters because `multiplexer.loss_count()` **sums** the two counters: a
half-configured pair would return a partial total that reads like a complete one. One flag,
one number.

`slick::default_queue_traits` - what the plain `stream_buffer_multiplexer` name uses - is
`debug_queue_traits` in debug builds and `queue_traits` in release. Name a traits struct
explicitly whenever you need the same behaviour (and the same loss numbers) out of both build
types.

> **Upgrading from 1.x.** `SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION` is ignored and
> warns. It gated a `std::atomic` member of a header-only class, so `sizeof` differed across a
> `-DNDEBUG` boundary: two translation units that disagreed silently violated the ODR. Because a
> template argument is part of the mangled name, the same disagreement is now an ordinary link
> error. `slick::stream_buffer`'s and `slick::queue`'s equivalent macros changed the same way -
> see their READMEs for `slick::read_traits` and `slick::queue_traits`.

## API Overview

### `stream_buffer_multiplexer`

Configured by a `Traits` parameter - see
[Compile-time configuration](#compile-time-configuration).

```cpp
// shared record queue
explicit stream_buffer_multiplexer(uint32_t shared_queue_size);                       // local memory
stream_buffer_multiplexer(uint32_t shared_queue_size, const char* shm_queue_name);     // shm creator
explicit stream_buffer_multiplexer(const char* shm_queue_name);                        // shm opener

static bool remove(const char* shm_name) noexcept;  // stale-segment recovery, see Shared memory usage

// producer registration (single-threaded setup, before producer/consumer threads start)
std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size);                       // local memory
std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size, const char* shm_name); // shm creator
std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, const char* shm_name);                                           // shm opener

bool has_producer(uint32_t producer_id) const noexcept;
std::shared_ptr<producer_buffer> get_producer_buffer(uint32_t producer_id); // shared ownership; nullptr if unregistered
using producer_map = std::unordered_map<uint32_t, std::shared_ptr<producer_buffer>>;
const producer_map& get_producer_buffers() const noexcept;                 // all of them, zero-copy
producer_buffer* find_producer(uint32_t producer_id) noexcept;             // non-owning; nullptr if unregistered
size_t producer_count() const noexcept; // number of registered producers

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
published_record consume(size_t n);           // same as slick::stream_buffer::consume; throws std::length_error at >= 4 GiB
void discard() noexcept;
const uint8_t* data() const noexcept;
size_t size() const noexcept;

uint64_t capacity() const noexcept;
uint32_t control_size() const noexcept;
uint64_t loss_count() const noexcept;          // this producer's own ring loss; see Important Constraints
uint64_t initial_reading_index() const noexcept;
bool own_buffer() const noexcept;
bool use_shm() const noexcept;

slick::stream_buffer& stream_buffer() noexcept;  // for slick::dynamic_buffer etc.
std::shared_ptr<slick::stream_buffer> stream_buffer_ptr() noexcept;  // for slick-net's websocket_session etc.
uint32_t producer_id() const noexcept;
```

### Enumerating producers

`get_producer_buffers()` hands back the multiplexer's registration table by
reference, keyed by `producer_id`. Nothing is copied or allocated, so it is cheap
enough to call in a monitoring loop:

```cpp
uint64_t total = 0;
for (const auto& [id, producer] : mux.get_producer_buffers()) {
    total += producer->loss_count();   // per-producer inner-ring loss
}
```

The reference stays valid until the next `add_producer()` - which is
single-threaded setup, so in practice for the lifetime of the multiplexer. The
`shared_ptr` elements carry the same ownership as `get_producer_buffer()`, so
copying one out keeps that producer (and the shared record queue) alive
independently of the multiplexer.

Two things follow from returning the table itself rather than a snapshot:

- **Order is unspecified** - it is the hash map's, and not stable across runs or
  standard-library implementations. Sort by `producer_id` at the call site if a
  report needs a fixed sequence.
- **`const` does not propagate.** A `const` multiplexer still yields
  `shared_ptr<producer_buffer>`, because these are the very handles the table
  holds. Use `get_producer_buffer(id)` or `find_producer(id)` on a `const`
  multiplexer where a `const`-qualified producer matters.

Use `stream_buffer_multiplexer::producer_map` if you need to name the type, rather
than spelling the container out.

## Important Constraints

**Three independent loss counters.** `multiplexer.loss_count()` sums two of them:
the shared record queue wrapping before a consumer read its entry, and
multiplexer-level loss - a shared-queue entry whose `producer_id` IS registered on
this instance but which was lapped by that producer's own ring before it could be
dereferenced. Both are switched by `Traits::enable_loss_detection` (see
[Compile-time configuration](#compile-time-configuration)) and read `0` when it is
off; records are skipped correctly either way, only the counters are silent.

**Only the multiplexer-level term is filtered by registration.** An entry this
instance reads and finds to name an unregistered producer is skipped without
counting. Shared-queue *wrap* loss cannot be filtered that way: a lapped slot has
already been overwritten, so nothing is left to say which producer it named. A
consumer that deliberately registers a subset of producers - see
[Shared memory usage](#shared-memory-usage) - therefore sees wrap loss for
producers it does not care about, and `loss_count()` is an upper bound on what it
actually missed. Concretely: ignore producer 17, give the shared queue four slots,
let 17 publish ten records, and `loss_count()` reports 8. No after-the-fact
attribution is possible; size the shared queue so it does not wrap and the term
goes to `0`, leaving the total exactly this instance's own loss.

The third, `producer_buffer::loss_count()`, is that producer's own ring loss, and
the multiplexer never drives it. `read(cursor)` dereferences by jumping a fresh
cursor to one exact sequence, where `slick::stream_buffer::read()`'s counter would
add the whole "how far has the producer run past this record" gap on *every*
lapped dereference and double-count wildly. It only moves for reads you make
directly:

```cpp
struct counting : slick::read_traits { static constexpr bool count_loss = true; };

uint64_t cursor = 0;
while (p0->stream_buffer().read<counting>(cursor).first) { }
p0->loss_count();   // what that sequential scan skipped
```

Note the two answer different questions. Over 10 records through a 4-slot control
ring, a sequential scan lands on slot 0, finds record 8 in it and jumps there,
recovering `{8, 9}` and reporting 8 skipped; the multiplexer asks each shared-queue
record for its own sequence, recovers all four still resident (`{6, 7, 8, 9}`), and
reports 6 lost. Both are right for what they measure - which is why they are not
added together.

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

**`producer_id` layout affects lookup cost.** For the fastest `read()` path,
prefer contiguous `producer_id` values starting at `0`. Sparse or high ids are
valid, but can miss the dense lookup fast path and use a hash lookup instead.

**Message size** is limited to < 4 GiB per record. `consume()` throws
`std::length_error` rather than truncating a larger one, and throws before any state
moves - nothing is published to the producer's ring or the shared record queue, and the
bytes stay readable, so they can go out as several smaller records.

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
silently skipped (skipping is not itself counted as loss). Otherwise it dereferences
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
