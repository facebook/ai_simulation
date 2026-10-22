/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimBarrier.h"

#include <stdexcept>
#include <utility>

#include <fmt/core.h>
#include <folly/logging/xlog.h>

namespace packsim::coro {

namespace {
// Validate-and-return helper used as the FIRST member initializer
// (numParticipants_) so the throw fires before later members
// (arrived_, waiters_) are constructed. Without this, the queue's
// capacity argument would need a fallback for invalid N, then construct
// + immediately destruct on the throwing path — wasted work and
// unnecessary indirection.
uint32_t validateNumParticipants(const uint32_t n) {
  if (n == 0) {
    throw std::invalid_argument(
        fmt::format("SimBarrier: numParticipants must be > 0, got {}", n));
  }
  return n;
}
} // namespace

SimBarrier::SimBarrier(const uint32_t numParticipants)
    : numParticipants_(validateNumParticipants(numParticipants)),
      // Capacity hint = numParticipants_. NOTE: folly::DMPSCQueue's
      // constructor adds threshold slack — effective producer-side
      // capacity is `N + (N + 9) / 10`, not strictly N. The
      // XCHECK_LE(count, numParticipants_) tripwire in await_suspend
      // catches over-arrival in the common case; bulletproof detection
      // is deferred.
      waiters_(numParticipants_) {}

SimBarrier::~SimBarrier() noexcept {
  // The barrier should only destruct when no participant is suspended on
  // it (refcount → 0). At that point arrived_ must be 0 and waiters_
  // must be empty. Otherwise a participant dropped its shared_ptr ref
  // without arriving — programmer error.
  // memory_order_relaxed: shared_ptr's release on the last refcount drop
  // already establishes happens-before with all participant threads, so
  // an additional acquire here would be redundant.
  XCHECK_EQ(arrived_.load(std::memory_order_relaxed), 0u)
      << "SimBarrier destructed with non-zero arrival count. A participant "
         "released its shared_ptr without arriving (or finished arriving). "
         "numParticipants="
      << numParticipants_;
  XCHECK(waiters_.empty())
      << "SimBarrier destructed with non-empty waiter queue. Programmer "
         "error: see above. numParticipants="
      << numParticipants_;
}

void SimBarrier::Awaitable::await_suspend(
    const std::coroutine_handle<> h) const noexcept {
  // INVARIANT: enqueue BEFORE fetch_add. The dispatcher's mutex-free
  // drain (after its fetch_add returns N) sees all N entries because
  // each enqueue is sequenced-before the corresponding fetch_add on
  // its thread, and the queue's MPSC ordering preserves visibility.
  barrier->waiters_.enqueue(
      WaiterEntry{
          .ctx = participantCtx.unwrap(), .resumeCb = [h] { h.resume(); }});

  const uint32_t count =
      barrier->arrived_.fetch_add(1, std::memory_order_acq_rel) + 1;

  // Best-effort tripwire (one comparison) for over-arrival within an
  // iteration. NOT authoritative — only fires in the narrow race where
  // the over-arrival's fetch_add lands AFTER the dispatcher has drained
  // ≥1 slot but BEFORE it resets arrived_ to 0. In wider windows the
  // over-arrival busy-spins in waiters_.enqueue (DMPSCQueue with
  // MayBlock=false spins on asm_volatile_pause until a slot frees, no
  // assertion), eventually enqueues, then its fetch_add returns ≤ N
  // (post-reset) — silently passing this check while leaking a stale
  // resume callback into the next round. Bulletproof detection of
  // over-arrival is deferred; this check catches the easy cases
  // without hot-path cost.
  XCHECK_LE(count, barrier->numParticipants_)
      << "SimBarrier received more arrivals than numParticipants_="
      << barrier->numParticipants_
      << "; check for duplicate arrivals or wrong participant count.";

  if (count != barrier->numParticipants_) {
    return;
  }

  // Dispatcher path. Drain N entries and scheduleRemote a resume for
  // each. Reset arrived_ at the end — frame-based PDES guarantees K+1
  // arrivals cannot become visible to other workers until the next
  // frame boundary, by which time the reset has completed. The
  // release on the store(0) below pairs with the next iteration's
  // fetch_add(acq_rel) so the reset is visible before any K+1 arrival
  // can publish its WaiterEntry.
  // Single `entry` reused across iterations: folly::DMPSCQueue's
  // dequeue(T&) ASSIGNS into the slot (`item = dequeueImpl()`), so the
  // moved-from resumeCb from iteration K is overwritten by K+1's
  // dequeue — no stale-state hazard from reuse.
  WaiterEntry entry;
  for (uint32_t i = 0; i < barrier->numParticipants_; ++i) {
    barrier->waiters_.dequeue(entry);
    // Extract locals so the read of `entry.ctx` and the move of
    // `entry.resumeCb` are sequenced on separate lines — clearer and
    // immune to a future edit accidentally dropping the std::move.
    SimContext* const ctx = entry.ctx;
    auto resumeCb = std::move(entry.resumeCb);
    ctx->scheduleRemote(std::move(resumeCb));
  }
  barrier->arrived_.store(0, std::memory_order_release);
}

} // namespace packsim::coro
