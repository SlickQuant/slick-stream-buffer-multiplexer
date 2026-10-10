# Changelog

All notable changes to this project will be documented in this file.

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
