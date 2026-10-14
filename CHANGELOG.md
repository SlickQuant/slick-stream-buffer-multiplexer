# Changelog

All notable changes to this project will be documented in this file.

## [2.2.0] - 2026-10-14

Adds a second kind of producer: a `slick::queue<uint8_t>` byte queue, merged into the same
globally ordered stream as the `slick::stream_buffer` producers. Nothing existing changes
signature, and the record and shared-memory formats are unchanged.

### Added

- Queue producers. `add_queue_producer()` registers a `slick::queue<uint8_t, Traits>` - the
  multiplexer's new `queue_type`, literally `slick::queue<uint8_t>` for the default alias - in
  local memory (with or without `items_per_slot`), as a shared-memory creator (with or without
  `items_per_slot`) or opener, or by adopting an existing `std::shared_ptr<queue_type>`. Only
  messages published through the returned handle are multiplexed; anything written to an adopted
  queue directly is invisible to `read()`.
- `producer_queue`, the queue producer's handle: `reserve()` / `operator[]` / `publish()` as in
  `slick::queue`, with `publish()` also fanning a `{index, producer_id}` record into the shared
  queue. Unlike a `producer_buffer`, any number of threads may publish through one. It also
  exposes `size()`, `items_per_slot()`, `loss_count()`, `initial_reading_index()`,
  `own_buffer()`, `use_shm()`, `queue()` and `queue_ptr()`.
- `get_producer_queue()`, `find_producer_queue()` and `get_producer_queues()` (with the
  `producer_queue_map` alias), the queue-producer counterparts of `get_producer_buffer()`,
  `find_producer()` and `get_producer_buffers()`, with the same ownership and zero-copy contracts.
- `slick::producer_kind`, and a common non-virtual `producer` base for both handles carrying
  `producer_id()` and `kind()`. `producer_buffer` derives from it; its interface is unchanged and
  it is still neither copyable nor movable.

### Changed

- `producer_id` is one namespace across both kinds: registering an id already held by either
  kind throws `std::invalid_argument`. `has_producer()` and `producer_count()` count both kinds,
  while `get_producer_buffer()`, `find_producer()` and `get_producer_buffers()` keep returning
  stream_buffer producers only - `nullptr` for a queue producer's id.
- Both kinds share one dense lookup table, so `read()` does one lookup and a branch on the kind;
  a stream_buffer record pays that one predictable branch over 2.1. An unregistered id below the
  dense limit (4096) now misses without falling through to a hash lookup, which makes skipping
  unregistered producers cheaper for a consumer that registers only some of them.
- `remove()` covers a queue producer's segment as well; it is a plain `slick::shm` segment too.
- A consumer built against 2.1 or earlier can share a record queue with a 2.2 process. It has no
  queue producers, so it skips their records as unregistered, without counting them as loss.

### Queue producer loss

- A queue message whose bytes may have been overwritten is skipped and counted, not only one
  whose control slot was lapped. Messages differ in length, so a later reservation can cover an
  older message's bytes without landing on its slot; once the queue's reservation counter is more
  than one ring past a message, the message is treated as lost. The check is best-effort, as
  `slick::stream_buffer`'s own is, and conservative: a message lying in the tail a wrapping
  reservation skipped is reported lost although its bytes are intact.
- `producer_queue::loss_count()` is the inner queue's own counter and is not a record count.
  `slick::queue` configures loss detection per type, not per call, so with
  `Traits::enable_loss_detection` on, each lapped dereference adds slick-queue's skip distance to
  it. `multiplexer.loss_count()` remains the number to read.

### Fixed

- A registration that throws - `std::bad_alloc` while growing the dense lookup table - no longer
  leaves the producer half-registered. In 2.1 the handle was inserted into the registration table
  before the dense table grew, so a failure there still registered the producer even though the
  call threw, and the retry was rejected as a duplicate. The dense table now grows first, so a
  failed registration leaves nothing behind and the id can be registered again.

## [2.1.0] - 2026-10-09

Tracks the slick-stream-buffer v2.1.0 and slick-queue v2.3.0 releases. No code in this library
changed; the behaviour change below arrives through the raised dependency floors.

### Changed

- Requires slick-stream-buffer >= 2.1.0 and slick-queue >= 2.3.0 (both were 2.0.0), in the
  build's `find_package()` / `FetchContent` pins and in the installed config's
  `find_dependency()` calls.
- **Behaviour change (POSIX):** a shared-memory segment is no longer unlinked on destruction -
  neither the shared record queue's nor a producer `stream_buffer`'s, and not even by the
  instance that created it. Previously a creator that exited first unlinked the name while
  consumers were still attached: they kept reading the orphaned mapping while a restarted
  producer silently created a fresh segment under the same name, so the two never met again. A
  restarted creator now reattaches to the existing segments and rejoins those consumers.
- Migration: code that relied on destruction to clean up now leaves its segments in `/dev/shm`,
  and its next run attaches to them instead of starting fresh - the shared record queue still
  holds the old records, a producer's `own_buffer()` is `false`, and a creator passing a
  different geometry throws. Call `stream_buffer_multiplexer::remove(name)` for every name at
  startup to start clean, or at shutdown once no peer will attach again. It was the
  stale-segment recovery step in 2.0.0 and is now the ordinary cleanup step too.
- Not breaking: Windows is unaffected, since a section there already died with its last handle,
  and neither shared-memory format changed, so 2.0 and 2.1 processes can map the same segments -
  though a 2.0 creator still unlinks its own on exit.
- The shared record queue keeps slick-queue's default `items_per_slot = 1` (new in slick-queue
  2.1.0). Every publish reserves exactly one 16-byte record, so there is nothing for a wider
  control slot to cover; raising it would only spend several records of ring per message.

## [2.0.0] - 2026-09-08

Tracks the slick-stream-buffer and slick-queue v2.0.0 releases and applies the same fix here:
compile-time configuration moves from macros to a Traits template parameter.

### Breaking

- Compile-time configuration is now a `Traits` template parameter, and that parameter is
  `slick::queue_traits` - the traits of the shared record queue the multiplexer is built on,
  not a traits type of its own. `slick::stream_buffer_multiplexer` is an alias for
  `basic_stream_buffer_multiplexer<>`, so code naming `slick::stream_buffer_multiplexer`,
  `::producer_buffer`, `::record` or `::multiplex_record` is unaffected.
  `slick::default_queue_traits` follows `NDEBUG`, so name a traits struct explicitly to pin the
  configuration across build types.
- `SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION` is ignored; it warns and does nothing.
  It gated a `std::atomic` member of a header-only class, so `sizeof` differed across a
  `-DNDEBUG` boundary and two translation units that disagreed silently violated the ODR. A
  template argument is part of the mangled name, so the same disagreement is now an ordinary
  link error instead.
- `Traits::enable_loss_detection` switches both terms of `multiplexer.loss_count()` together -
  the shared queue's wrap loss and multiplexer-level loss - and both read 0 when it is off.
  They are one flag rather than two because `loss_count()` sums them: a half-configured pair
  would return a partial total that reads like a complete one. Records are skipped correctly
  either way; only the counters are silent.
- `producer_buffer` is no longer movable; its move constructor and move assignment were public
  and defaulted in 1.x and are now deleted. A registered producer is reachable both by the
  `shared_ptr` `add_producer()` returns and by a raw pointer in the multiplexer's dense lookup
  table, so moving one out emptied the `shared_ptr` members of an object both still pointed at,
  and the next `consume()` or `read()` dereferenced a null buffer. Nothing needs to move one: it
  is constructed in place and only ever held by `shared_ptr`.
- `producer_buffer::consume()` is no longer `noexcept`: it propagates the `std::length_error`
  that `slick::stream_buffer::consume()` now throws for a record of 4 GiB or more. It is thrown
  before any state moves, so nothing is published to either the producer's ring or the shared
  record queue and the bytes stay readable.
- `producer_buffer::loss_count()` now reads 0 unless the caller reads that producer's ring
  through `stream_buffer().read<Traits>()` with a `Traits` whose `count_loss` is true. The
  multiplexer's own dereference deliberately does not count: it jumps a fresh cursor to one
  exact sequence, and `read()`'s counter measures the gap a *sequential* scan skipped, which
  every later lapped dereference would add all over again.
- `cmake_minimum_required` raised to 3.21 (`PROJECT_IS_TOP_LEVEL` needs it, so the stated 3.10
  could never configure), and the C++20 requirement now travels with the target via
  `target_compile_features(... INTERFACE cxx_std_20)` instead of `CMAKE_CXX_STANDARD`, which
  applied only to this build and left installed consumers compiling the headers under whatever
  standard they happened to use.
- The installed `slick-stream-buffer-multiplexerConfig.cmake` requires its dependencies with
  `find_dependency(... CONFIG)` instead of falling back to `FetchContent`. A config file runs
  inside a consumer's `find_package()`, where git-cloning a dependency hides a missing install
  behind a network fetch and can pull a different version than the one this package was
  installed against - and the pins it carried were still v1.

### Changed

- Requires slick-stream-buffer >= 2.0.0 and slick-queue >= 2.0.0, and includes
  `<slick/queue.hpp>` rather than the deprecated `<slick/queue.h>` shim.
- `record` and `multiplex_record` are also spelled `slick::multiplexer_record` and
  `slick::multiplex_record` at namespace scope, so the shared-queue element type stays one type
  across every `Traits` configuration - two differently-configured multiplexers mapping one
  segment must agree on it.
- `loss_count()` is documented more precisely: only its multiplexer-level term is filtered by
  producer registration. Shared-queue wrap loss is counted before any `producer_id` is known -
  a lapped slot has already been overwritten - so an instance registering a subset of producers
  sees wrap loss for producers it ignores, and the total is an upper bound on what it missed.
  The behaviour is unchanged from 1.x; the previous wording claimed the filter applied to both.
- `dereference()` reads through traits with `detect_reset` off. The trait resynchronizes a
  cursor that outlived a `reset()`; this cursor is built from the record's own sequence one
  statement earlier and outlives nothing, and the exact-sequence check already rejects what the
  rewind would return - so it cost an acquire load of `next_seq_` per dereference and bought no
  accuracy.

### Added

- `stream_buffer_multiplexer::remove(shm_name)`, the explicit stale-segment recovery step for a
  name a dead run left behind. One call covers both kinds of segment this class creates - the
  shared record queue and a producer's `stream_buffer` - since both are `slick::shm` segments
  underneath.
- `get_producer_buffers()`, returning every registered producer keyed by `producer_id` as a
  `const producer_map&` - the registration table itself, so enumerating allocates nothing. The
  order is the hash map's and `const` does not propagate to the handles; the new public
  `producer_map` alias lets callers name the type without spelling the container.
- `find_producer()` overloads, public and documented.

## [1.0.1] - 2026-06-17

- Renamed canonical header from slick/stream_buffer_multiplexer.h to slick/stream_buffer_multiplexer.hpp. The old .h path is kept as a backward-compatibility shim that re-exports the new header and emits a compiler warning directing users to update their includes.

## [1.0.0] - 2026-06-15

### Added

- Initial release of `slick::stream_buffer_multiplexer`, a lock-free MPMC byte
  stream multiplexer built by composing `slick::stream_buffer` (per-producer
  SPMC byte ring) with `slick::queue<record>` (shared MPMC fan-in queue).
- Per-producer `producer_buffer` with the familiar
  `prepare`/`commit`/`consume`/`discard`/`data`/`size` interface, plus a
  `.stream_buffer()` accessor returning a literal `slick::stream_buffer&` for
  drop-in `slick::dynamic_buffer` interop, and a `.stream_buffer_ptr()`
  accessor returning `std::shared_ptr<slick::stream_buffer>` for shared
  ownership with `slick-net`'s `websocket_session`.
- `get_producer_buffer(producer_id)` returning `std::shared_ptr<producer_buffer>`
  (or `nullptr` if unregistered), for shared ownership of a producer (including
  `consume()`) that can outlive the `stream_buffer_multiplexer`.
- `add_producer` registration API (local memory, shared-memory creator, and
  shared-memory opener overloads) with explicit `producer_id`, allowing any mix
  of local-memory and shared-memory producers under one shared queue. Returns
  `std::shared_ptr<producer_buffer>`, so the registered producer (including
  `consume()`) can outlive the `stream_buffer_multiplexer`.
- `read(cursor)` and `read(std::atomic<uint64_t>& cursor)` (work-stealing)
  returning a zero-copy `multiplex_record{data, length, producer_id}`.
- Three independent loss counters: shared-queue wrap loss, multiplexer-level
  loss (shared-queue entries whose `producer_id` is registered on this instance
  but were lapped by that producer's own ring before being dereferenced), and
  each producer's own ring loss. The multiplexer-level counter is configurable
  via `SLICK_STREAM_BUFFER_MULTIPLEXER_ENABLE_LOSS_DETECTION` (default: enabled
  in debug builds, disabled in release), matching `slick::stream_buffer` and
  `slick::queue`.
- Shared memory (IPC) support with `producer_id` <-> shm-name mapping by caller
  convention; unregistered producer ids on a consumer instance are silently
  skipped rather than treated as an error or counted as loss.

### Changed

- Replaced the confusing `producer_slot_count()` API with
  `producer_count()`, which now returns the actual number of registered
  producers.
- Optimized producer lookup with a dense fast path for low `producer_id`
  values while still supporting sparse registrations through a fallback map.
- Clarified in the README that contiguous `producer_id` values, ideally
  starting at `0`, provide the best lookup performance.
