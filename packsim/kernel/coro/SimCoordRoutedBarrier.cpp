/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/coro/SimCoordRoutedBarrier.h"

#include <cstdint>
#include <stdexcept>
#include <utility>

#include <fmt/core.h>
#include <folly/logging/xlog.h>

namespace packsim::coro {

namespace {
// Validate-and-return helper used as the SECOND member-init (driving
// `numParticipants_`); the throw fires before any later member —
// `arrivals_` and `inFlight_` — is initialized, and the constructor
// body's `arrivals_.reserve()` never runs on the throwing path.
// (`coordCtx_` is the first member and is trivially constructed before
// the throw site; no resource ownership issue.) Matches sibling
// SimBarrier.cpp.
uint32_t validateNumParticipants(const uint32_t n) {
  if (n == 0) {
    throw std::invalid_argument(
        fmt::format(
            "SimCoordRoutedBarrier: numParticipants must be > 0, got {}", n));
  }
  return n;
}
} // namespace

SimCoordRoutedBarrier::SimCoordRoutedBarrier(
    folly::not_null<SimContext*> coordCtx,
    const uint32_t numParticipants)
    : coordCtx_(coordCtx),
      numParticipants_(validateNumParticipants(numParticipants)) {
  arrivals_.reserve(numParticipants_);
}

SimCoordRoutedBarrier::~SimCoordRoutedBarrier() noexcept {
  // Acquire pairs with the release in postArrival's fetch_sub: ensures
  // the destructor sees the final state of every postArrival that has
  // already run. In normal usage scheduler.run() returning provides the
  // synchronization, but the explicit ordering is belt-and-suspenders
  // for cross-thread destruction patterns.
  const auto pending = inFlight_.load(std::memory_order_acquire);

  // Two-phase check, in this order, because reading arrivals_ from this
  // thread is only safe once coord is proven quiescent. If pending > 0
  // a postArrival callback is queued or mid-execution on coord, so
  // emplace_back/clear could race with our read — abort on inFlight_
  // alone without touching the vector.
  XCHECK_EQ(pending, 0u)
      << "SimCoordRoutedBarrier destroyed with " << pending
      << " in-flight postArrival callbacks (numParticipants_="
      << numParticipants_
      << "); the queued callbacks hold raw `this` and would UAF on the "
         "next coord tick.";

  // pending == 0 + acquire load above synchronizes with every postArrival's
  // release fetch_sub, so all coord-side mutations of arrivals_ are
  // visible and no further coord callback is in flight. Safe to read.
  XCHECK(arrivals_.empty())
      << "SimCoordRoutedBarrier destroyed with " << arrivals_.size()
      << " pending arrivals already aggregated on coord (numParticipants_="
      << numParticipants_ << "); participant coroutines will leak.";
}

void SimCoordRoutedBarrier::Awaitable::await_suspend(
    const std::coroutine_handle<> h) const noexcept {
  // Increment BEFORE scheduleRemote so a racing destructor sees
  // inFlight_ > 0 and trips XCHECK (false-positive abort beats silent
  // UAF). Decremented at the end of postArrival. acq_rel chosen
  // (rather than just release) for two reasons: (a) the acquire half
  // ensures the fetch_add is not reordered AFTER the subsequent
  // scheduleRemote queue write — preventing a destructor on a third
  // thread from seeing inFlight_ at zero while the queued event still
  // lives in the inbox; (b) the release half pairs with the dtor's
  // acquire load. The cross-thread happens-before between this
  // fetch_add and postArrival's fetch_sub is provided by PDES's
  // cross-context inbox in scheduleRemote (NOT by inFlight_'s atomic
  // ordering itself).
  barrier->inFlight_.fetch_add(1, std::memory_order_acq_rel);
  // scheduleRemote with a member-fn pointer binds `barrier.unwrap()`
  // as the `this` argument, then forwards (participantCtx, resumeCb)
  // into postArrival when the cross-context hop completes.
  barrier->coordCtx_->scheduleRemote(
      &SimCoordRoutedBarrier::postArrival,
      barrier.unwrap(),
      participantCtx,
      std::function<void()>{[h] { h.resume(); }});
}

void SimCoordRoutedBarrier::postArrival(
    const folly::not_null<SimContext*> participantCtx,
    std::function<void()> resumeCb) noexcept {
  arrivals_.emplace_back(participantCtx, std::move(resumeCb));
  // Best-effort tripwire (one comparison) for over-arrival within an
  // iteration. If this XCHECK ever fires, the dispatch path below is also
  // skipped (size > N makes the equality false), permanently jamming the
  // barrier — but the abort happens first in any practical case.
  XCHECK_LE(arrivals_.size(), numParticipants_)
      << "SimCoordRoutedBarrier received more arrivals than numParticipants_="
      << numParticipants_
      << "; check for duplicate arrivals or wrong participant count.";
  if (arrivals_.size() == numParticipants_) {
    // Move out resumeCb per-element; arrivals_.clear() below destroys
    // the moved-from std::function objects (valid-but-unspecified state
    // is OK for destruction).
    for (auto& arrival : arrivals_) {
      arrival.participantCtx->scheduleRemote(std::move(arrival.resumeCb));
    }
    arrivals_.clear();
  }
  // Decrement AFTER dispatch so a destructor running between the
  // dispatch and this fetch_sub still sees inFlight_ > 0 and aborts
  // (the dispatched resume callbacks don't reference `this`, but the
  // belt-and-suspenders ordering keeps the contract simple). acq_rel
  // matches the await_suspend fetch_add and pairs with the dtor's
  // acquire load.
  inFlight_.fetch_sub(1, std::memory_order_acq_rel);
}

} // namespace packsim::coro
