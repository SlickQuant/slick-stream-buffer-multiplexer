# Changelog

All notable changes to this project will be documented in this file.

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
