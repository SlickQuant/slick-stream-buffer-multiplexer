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

Each producer owns its own byte ring, so producers can have completely different
message sizes and rates. A producer is one of two kinds, freely mixed under one
multiplexer and one `producer_id` space:

- a `slick::stream_buffer` - an independently-sized byte ring with its own
  control ring, written with `prepare`/`commit`/`consume` from one thread;
- a `slick::queue<uint8_t>` - a byte queue written with `reserve`/`publish`,
  which several threads may do at once.

A single shared `slick::queue<record>` merges every producer's published
messages into one globally-ordered stream that consumers read as
`(data pointer, length, producer_id)`, whichever kind published them.

## How it works

```
 producer 0 ──prepare/commit──▶ [ stream_buffer 0 ] ──consume(n)────┐
 producer 1 ──prepare/commit──▶ [ stream_buffer 1 ] ──consume(n)────┼─▶ {sequence, producer_id}
 producer 2 ──reserve/write───▶ [ queue<uint8_t> 2 ] ──publish(i,n)─┘        │
                                                                              ▼
                                                            [ shared queue<record> ]
                                                                              │
                                                consumer A (own cursor) ◀────┤  read(cursor) dereferences
                                                consumer B (own cursor) ◀────┤  {sequence, producer_id} back
                                                process C (shared memory) ◀──┘  into the ring of producer_id
```

Publishing through a producer handle does two things. For a stream_buffer
producer, `producer_buffer::consume(n)`:

1. Publishes into its own `slick::stream_buffer`, exactly like
   `slick::stream_buffer::consume(n)` - returning the same
   `published_record{sequence, data, length}`.
2. Fans a tiny `{sequence, producer_id}` record into the shared
   `slick::queue<record>`.

For a queue producer, `producer_queue::publish(index, n)` does the same pair:
`slick::queue::publish(index, n)` on its own queue, then a `{index, producer_id}`
record into the shared queue.

`multiplexer.read(cursor)` dequeues the next `{sequence, producer_id}` record
and dereferences it back into that producer's own ring -
`stream_buffer().read(sequence)` or `queue().read(index)` - returning a
zero-copy view directly into it.

## Features

- **Lock-free** MPMC fan-in of N independently-sized byte streams
- **Two kinds of producer** in one ordered stream: `slick::stream_buffer` and
  `slick::queue<uint8_t>`
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
- [slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer) >= 2.1.0 and
  [slick-queue](https://github.com/SlickQuant/slick-queue) >= 2.3.0 (fetched automatically when
  not installed). Both are required: this library is built on their traits-based configuration,
  and from v2.1.0 on their shared-memory lifetime - see [Segment lifetime](#segment-lifetime).

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
    GIT_TAG v2.2.0 # See https://github.com/SlickQuant/slick-stream-buffer-multiplexer/releases for latest version
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

### Queue producers

A producer can be a `slick::queue<uint8_t>` instead. Register it with
`add_queue_producer()` and publish through the `producer_queue` handle it returns, which
has `slick::queue`'s own producer interface:

```cpp
// 1 MB byte queue, minimum 64 bytes per reservation (both must be powers of 2)
auto fills = mux.add_queue_producer(2, 1u << 20, 64);

auto index = fills->reserve(n);             // n contiguous bytes; never blocks
std::memcpy((*fills)[index], message, n);
fills->publish(index, n);                   // publishes locally AND fans {index, producer_id} into the shared queue
```

One message is the `n` bytes of one `reserve()`/`publish()` pair, and consumers get it
exactly as they get a stream_buffer message - `rec.length` is `n`. What is different is
the producer side:

- **Several threads may publish through one `producer_queue`.** `slick::queue` is MPMC,
  and so is the fan-in. Each thread's own messages stay in order; messages of different
  threads are ordered by when their `publish()` reached the shared queue.
- **Give a large queue an `items_per_slot`.** It is the minimum number of bytes one
  `reserve()` consumes, and `slick::queue` keeps one 16-byte control slot per unit of it.
  The two-argument `add_queue_producer(id, size)` uses 1 - a slot per byte, 16 MB of
  control for 1 MB of data - which is only reasonable for a small ring. Choose it to
  match a typical message, not the largest; see
  [slick-queue](https://github.com/SlickQuant/slick-queue#byte-buffers-and-control-array-memory).
- **A message is limited to the queue's size.** `reserve()` throws `std::runtime_error`
  for a larger `n` and `std::invalid_argument` for 0, as `slick::queue::reserve()` does.

`producer_id` is one namespace across both kinds: an id registered with `add_producer()`
cannot be registered again with `add_queue_producer()`, or the other way round.

An existing queue can be adopted instead of created:

```cpp
auto queue = std::make_shared<slick::queue<uint8_t>>(1u << 20, 64);
auto fills = mux.add_queue_producer(2, queue);
```

Only what is published through the handle is multiplexed. A message put on the queue
directly - through `fills->queue()`, or by anything else holding `queue` - is there for
whoever reads that queue, but no record is fanned in for it, so `mux.read()` never
returns it. It still uses up the ring, so it can lap messages that were multiplexed.

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

// producer 2: a queue producer in shared memory
server.add_queue_producer(2, 1u << 20, 64, "md_q2");
```

A consumer in another process opens the shared queue and registers only the
producer ids whose shared memory it has access to:

```cpp
slick::stream_buffer_multiplexer client("md_records");
client.add_producer(0, "md_p0");         // producer 1 is intentionally not registered here
client.add_queue_producer(2, "md_q2");   // size and items_per_slot come from the segment

uint64_t cursor = client.initial_reading_index();
auto rec = client.read(cursor);    // producer 0 and 2 records dereference normally;
                                    // producer 1 records are silently skipped, and
                                    // skipping one is not itself counted as loss
```

Each side has to register an id as the kind it is: the record carries only
`{sequence, producer_id}`, so the kind of ring behind an id is part of the same caller
convention as its name. A consumer built against 2.1 or earlier has no queue producers;
it shares the record queue with a 2.2 producer process all the same, and skips queue
producers' records as unregistered, without counting them as loss.

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

### Segment lifetime

Destroying a multiplexer or a producer handle closes its mappings but never unlinks a
shared-memory segment - not even one it created. On POSIX a name therefore stays in `/dev/shm`
until it is removed explicitly, and a creator that finds it attaches to the existing segment
instead of starting fresh: the shared record queue still holds its records, a producer's
`own_buffer()` is `false` - for either kind of producer - and a creator passing a different
geometry throws. That is what lets a restarted producer rejoin consumers that are still
attached.

Whichever process coordinates the lifetime removes the names - at startup to guarantee a clean
start, or at shutdown once no peer will attach again:

```cpp
slick::stream_buffer_multiplexer::remove("md_records");  // shared record queue
slick::stream_buffer_multiplexer::remove("md_p0");       // a producer's stream_buffer
slick::stream_buffer_multiplexer::remove("md_q2");       // a producer's queue

slick::stream_buffer_multiplexer server(1024, "md_records");
server.add_producer(0, 1ull << 26, 1u << 16, "md_p0");
server.add_queue_producer(2, 1u << 20, 64, "md_q2");
```

One call covers every kind of name, since all are `slick::shm` segments underneath. Only call
it when no process is using the segment: on POSIX the name is unlinked immediately, so the next
creator gets a fresh segment while any process still mapped to the old one keeps reading the
orphaned copy. On Windows it is a no-op returning `true` - a section there disappears once the
last handle closes, so nothing outlives the processes using it.

> **Upgrading from 2.0.** The creator used to unlink its segments in its destructor on POSIX.
> A producer that exited first therefore orphaned every consumer still attached, and its restart
> silently created a new segment under the same name that those consumers never saw. Code that
> relied on that cleanup now leaves segments behind and attaches to them on its next run - add
> the `remove()` calls above. The shared-memory formats are unchanged, so 2.0 and 2.1 processes
> can map the same segments.

### Recovering a stale segment

A creator that dies mid-initialization leaves its segment wedged, and constructing over that
name reports it by throwing. Recovery is the same `remove()` call, and deliberately manual -
nothing portable distinguishes a dead creator from a slow one - so clear the name before
creating again.

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

Queue producers take the same `Traits`: a multiplexer's `queue_type` is
`slick::queue<uint8_t, Traits>`. For the plain `stream_buffer_multiplexer` that is literally
`slick::queue<uint8_t>`, which is what lets one be adopted, or handed on through
`producer_queue::queue()`. A queue with different traits is a different type and cannot be
registered.

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

static bool remove(const char* shm_name) noexcept;  // unlink a segment name, see Segment lifetime

// producer registration (single-threaded setup, before producer/consumer threads start);
// producer_id is unique across both kinds

// stream_buffer producers
std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size);                       // local memory
std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, uint64_t capacity, uint32_t control_size, const char* shm_name); // shm creator
std::shared_ptr<producer_buffer> add_producer(uint32_t producer_id, const char* shm_name);                                           // shm opener

// queue producers; queue_type is slick::queue<uint8_t, Traits>
std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, uint32_t size);                                              // local memory
std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, uint32_t size, uint32_t items_per_slot);                     // local memory
std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, uint32_t size, const char* shm_name);                        // shm creator
std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, uint32_t size, uint32_t items_per_slot, const char* shm_name); // shm creator
std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, const char* shm_name);                                       // shm opener
std::shared_ptr<producer_queue> add_queue_producer(uint32_t producer_id, std::shared_ptr<queue_type> queue);                          // adopt an existing queue

bool has_producer(uint32_t producer_id) const noexcept;  // either kind
size_t producer_count() const noexcept;                  // number of registered producers, both kinds

// stream_buffer producers by id; nullptr if unregistered or a queue producer
std::shared_ptr<producer_buffer> get_producer_buffer(uint32_t producer_id); // shared ownership
producer_buffer* find_producer(uint32_t producer_id) noexcept;             // non-owning
using producer_map = std::unordered_map<uint32_t, std::shared_ptr<producer_buffer>>;
const producer_map& get_producer_buffers() const noexcept;                 // all of them, zero-copy

// queue producers by id; nullptr if unregistered or a stream_buffer producer
std::shared_ptr<producer_queue> get_producer_queue(uint32_t producer_id);  // shared ownership
producer_queue* find_producer_queue(uint32_t producer_id) noexcept;        // non-owning
using producer_queue_map = std::unordered_map<uint32_t, std::shared_ptr<producer_queue>>;
const producer_queue_map& get_producer_queues() const noexcept;            // all of them, zero-copy

multiplex_record read(uint64_t& cursor) noexcept;
multiplex_record read(std::atomic<uint64_t>& cursor) noexcept;  // work-stealing

uint64_t loss_count() const noexcept;
uint64_t initial_reading_index() const noexcept;
```

### `multiplex_record`

```cpp
struct multiplex_record {
    const uint8_t* data;
    uint32_t length;        // in bytes, whichever kind of producer published it
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
slick::producer_kind kind() const noexcept;    // producer_kind::stream_buffer
```

### `producer_queue`

Forwards the `slick::queue` producer interface, with `publish()` additionally
fanning a record into the shared queue. Any number of threads may use one:

```cpp
uint64_t reserve(uint32_t n = 1);              // n contiguous bytes, rounded up to items_per_slot; throws like slick::queue::reserve
uint8_t* operator[](uint64_t index) noexcept;  // where to write the reserved bytes
void publish(uint64_t index, uint32_t n = 1) noexcept;  // same as slick::queue::publish, plus the fan-in

uint32_t size() const noexcept;                // capacity in bytes
uint32_t items_per_slot() const noexcept;
uint64_t loss_count() const noexcept;          // the inner queue's own counter; see Important Constraints
uint64_t initial_reading_index() const noexcept;
bool own_buffer() const noexcept;
bool use_shm() const noexcept;

queue_type& queue() noexcept;                  // slick::queue<uint8_t, Traits>&
std::shared_ptr<queue_type> queue_ptr() noexcept;
uint32_t producer_id() const noexcept;
slick::producer_kind kind() const noexcept;    // producer_kind::queue
```

### Enumerating producers

`get_producer_buffers()` hands back the multiplexer's registration table of
stream_buffer producers by reference, keyed by `producer_id`, and
`get_producer_queues()` does the same for queue producers. Nothing is copied or
allocated, so both are cheap enough to call in a monitoring loop:

```cpp
uint64_t total = 0;
for (const auto& [id, producer] : mux.get_producer_buffers()) {
    total += producer->loss_count();   // per-producer inner-ring loss
}
for (const auto& [id, producer] : mux.get_producer_queues()) {
    report(id, producer->size(), producer->use_shm());
}
```

The two tables never share a key, and together they hold `producer_count()`
entries. Each reference stays valid until the next registration of its kind -
which is single-threaded setup, so in practice for the lifetime of the
multiplexer. The `shared_ptr` elements carry the same ownership as
`get_producer_buffer()` / `get_producer_queue()`, so copying one out keeps that
producer (and the shared record queue) alive independently of the multiplexer.

Two things follow from returning the table itself rather than a snapshot:

- **Order is unspecified** - it is the hash map's, and not stable across runs or
  standard-library implementations. Sort by `producer_id` at the call site if a
  report needs a fixed sequence.
- **`const` does not propagate.** A `const` multiplexer still yields
  `shared_ptr<producer_buffer>`, because these are the very handles the table
  holds. Use `get_producer_buffer(id)` or `find_producer(id)` on a `const`
  multiplexer where a `const`-qualified producer matters - or
  `get_producer_queue(id)` / `find_producer_queue(id)` for a queue producer.

Use `stream_buffer_multiplexer::producer_map` and
`stream_buffer_multiplexer::producer_queue_map` if you need to name the types,
rather than spelling the containers out.

## Important Constraints

**Three independent loss counters.** `multiplexer.loss_count()` sums two of them:
the shared record queue wrapping before a consumer read its entry, and
multiplexer-level loss - a shared-queue entry whose `producer_id` IS registered on
this instance but which was lapped by that producer's own ring before it could be
dereferenced (for a queue producer, also one whose bytes may already have been
overwritten - see below). Both are switched by `Traits::enable_loss_detection` (see
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

**`producer_queue::loss_count()` is not kept clean the same way.** `slick::queue`
configures its counter per type, not per call, so the multiplexer cannot switch it
off for its own dereferences. With `Traits::enable_loss_detection` on, every
dereference that finds its record lapped adds `slick::queue`'s "how far has this
slot moved on" distance to the inner queue's counter - and each later lapped
dereference adds its own again. Ten one-byte messages through a 4-byte queue lose
6 records, which `multiplexer.loss_count()` reports as 6 while that queue's own
`loss_count()` reads 32. Treat it as a diagnostic of the queue, and read loss off
the multiplexer. With the trait off (the default in release builds) it stays 0.

**A queue message is lost once its bytes may be overwritten, not only its slot.**
Messages differ in length, so a later reservation can cover an older message's
bytes without landing on its control slot: in an 8-byte queue, `"ijkl"` at index 8
overwrites positions 0-3 - `"AB"` at 0 and `"CD"` at 2 - while `"CD"`'s slot still
names it. `read()` therefore also skips (and counts) any queue message the
reservation counter has moved more than one ring past. Two limits, both erring
toward loss rather than corruption:

- It is **best-effort**, like `slick::stream_buffer`'s own check: a producer can
  still reserve and overwrite the bytes after `read()` has returned them.
- It is **conservative**. A reservation that does not fit before the end of the
  ring skips the rest of the lap, so the counter also passes indices nothing wrote.
  A message lying in that skipped tail is reported lost although its bytes are
  intact. Telling the two apart would need `slick::queue`'s wrap markers, which its
  public interface does not expose.

**Pointer invalidation.** Same as `slick::stream_buffer`: `prepare()` may
relocate the readable region, invalidating previous `data()`/`prepare()`
pointers. A queue producer never relocates anything: a `reserve()`d region stays
where it is. Pointers returned by `read()` stay valid until that producer's ring
laps them, for either kind.

**Single producer thread per `producer_buffer`.** All producer-side methods
(`prepare`/`commit`/`consume`/`discard`) for a given `producer_buffer` must be
called from one thread, same as `slick::stream_buffer`. Different
`producer_buffer`s may be driven by different threads concurrently. A
`producer_queue` has no such limit - any number of threads may `reserve()` and
`publish()` through the same one.

**Registration is single-threaded setup only.** Register all producers - with
`add_producer` and `add_queue_producer` - before starting any producer or
consumer threads.

**One `producer_id` space.** An id names one producer of one kind. Registering an
id that is already taken throws `std::invalid_argument`, whichever kind holds it.
Processes sharing a record queue must also agree on the kind behind each id: the
record does not say, and an id registered as the wrong kind dereferences into the
wrong ring.

**`producer_id` layout affects lookup cost.** For the fastest `read()` path,
prefer contiguous `producer_id` values starting at `0`. Sparse or high ids are
valid, but can miss the dense lookup fast path and use a hash lookup instead.

**Message size** is limited to < 4 GiB per record. `consume()` throws
`std::length_error` rather than truncating a larger one, and throws before any state
moves - nothing is published to the producer's ring or the shared record queue, and the
bytes stay readable, so they can go out as several smaller records. A queue
producer's message is limited to its queue's `size`; `reserve()` throws for a
larger one, before anything is published.

**Power-of-2 geometry.** `shared_queue_size`, each stream_buffer producer's
`capacity` and `control_size`, and each queue producer's `size` and
`items_per_slot` must all be powers of 2 (enforced by `slick::queue` and
`slick::stream_buffer`).

**Shared ownership and lifetime.** `producer_buffer` holds its `stream_buffer`
and the shared record queue via `shared_ptr`, so `stream_buffer_ptr()` (the
`stream_buffer` alone, e.g. for a `slick-net` `websocket_session`) and
`get_producer_buffer()` (the whole `producer_buffer`, including `consume()`)
can both safely outlive the `stream_buffer_multiplexer` that created them - the
shared record queue stays alive as long as any `producer_buffer` referencing it
does. Note that `stream_buffer_ptr()` alone does not keep the owning
`producer_buffer` (or its `consume()`) alive; use `get_producer_buffer()` for
that. `producer_queue` follows the same rules with `queue_ptr()` and
`get_producer_queue()`.

## Architecture

Built entirely from existing primitives - no new shared-memory layout. Each
producer is a complete `slick::stream_buffer` (own data ring + control ring,
independently sized) or a complete `slick::queue<uint8_t>`. The shared
`slick::queue<record>` (`record` = `{uint64_t sequence; uint32_t producer_id;
uint32_t pad0;}`, 16 bytes) is the lock-free MPMC fan-in/merge point; `sequence`
is a stream_buffer's record sequence or a queue's reservation index, and the
record is the same for both. `read(cursor)` copies `{sequence, producer_id}` out
of the shared queue. If `producer_id` is unregistered on this instance, the entry
is silently skipped (skipping is not itself counted as loss). Otherwise it
dereferences into that producer's ring with a fresh cursor set to `sequence` and
requires an exact hit: for a stream_buffer, `stream_buffer().read(sequence)` with
`data != nullptr && local_cursor == sequence + 1`; for a queue,
`queue().read(sequence)` with the cursor left exactly one reservation - the
message length rounded up to `items_per_slot` - past `sequence`, and the queue's
reservation counter no more than one ring past `sequence`, so its bytes cannot yet
have been overwritten. A hit returns the zero-copy view, anything else is counted
as multiplexer-level loss. Either way, the next shared-queue entry is tried.

Both kinds of producer handle derive from one small non-virtual base, so the
lookup `read()` does is the same for either: one dense table indexed by
`producer_id` for ids below 4096 (a hash lookup above that), then a branch on the
producer's kind into an inlined dereference.

## Building and Testing

```bash
cmake -S . -B build
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
```

## License

SlickStreamBufferMultiplexer is released under the [MIT License](LICENSE).

**Made with ⚡ by [SlickQuant](https://github.com/SlickQuant)**
