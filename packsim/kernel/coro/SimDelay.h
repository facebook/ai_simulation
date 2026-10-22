/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>
#include <type_traits>

#include <folly/CPortability.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

namespace packsim::coro {

namespace detail {
// Zero simulation-time delay. Named so simYield's body reads as
// `simDelay(ctx, detail::kZeroDelay)` rather than the magic literal
// `Time(0)`. SimDelay.h-internal — not part of the public coro API.
// Time(uint64_t) is not constexpr, so this is `inline const`, not
// `inline constexpr`. Making Time constexpr would let it be.
inline const Time kZeroDelay{0};
} // namespace detail

/**
 * Awaitable that suspends the coroutine and resumes it after a delay on the
 * given SimContext. The resumption is scheduled through the PDES (Parallel
 * Discrete Event Simulation) engine, so context affinity (same thread) is
 * preserved.
 *
 * For zero delay, this still goes through the event queue (like
 * SimContext::scheduleNow in packsim/kernel/sim-context.h) to prevent stack
 * overflow from deeply chained coroutine resumptions. Tradeoff: resumption
 * is deferred by one scheduler trip even when delay is zero. For
 * synchronous-complete awaitables prefer co_return-early-from-await_ready
 * patterns.
 *
 * Lifetime contract: the SimContext AND the enclosing coroutine frame
 * MUST outlive the scheduled resume callback. SimDelay does not record
 * the scheduled event handle, so an externally-triggered destroy of the
 * coroutine handle (via std::coroutine_handle<>::destroy() — not the
 * SimTask path) would leave a queued resume callback pointing at a
 * destroyed frame = UB. `SimScopedTask::reset()`, and the cascade that
 * destroys an awaited `SimResultTask` along with its parent, do exactly
 * that, so a frame that may be destroyed mid-delay must use
 * `simScopedDelay` instead.
 *
 * NOTE: Single SimContext only — the resume callback fires on the same
 * SimContext that scheduled it. Cross-context delay = co_await
 * simScheduleOn(other) followed by co_await simDelay(other, ...).
 */
struct SimDelay {
  // SimContext that schedules and dispatches this delay's resume callback.
  // MUST outlive both the suspended coroutine and the scheduled resume.
  const folly::not_null<SimContext*> ctx;

  // How long to suspend before resuming. Zero is allowed (equivalent to
  // simYield — defers to the event queue without advancing simulation time).
  const Time delay;

  // Explicit constructor — required because the special-member
  // declarations below make the struct no longer an aggregate; without
  // this constructor the factory's `SimDelay{ctx, delay}` syntax would
  // fail to compile.
  SimDelay(folly::not_null<SimContext*> ctxIn, Time delayIn) noexcept
      : ctx(ctxIn), delay(delayIn) {}

  // Lifetime and assignability contract, explicit on the type rather
  // than implied by the const-member language rule. Awaitables are
  // use-once expression-scoped values: pass them around by copy/move
  // (typically NRVO-elided), but never reassign.
  SimDelay(const SimDelay&) = default;
  SimDelay(SimDelay&&) noexcept = default;
  SimDelay& operator=(const SimDelay&) = delete;
  SimDelay& operator=(SimDelay&&) = delete;
  ~SimDelay() = default;

  /** @return Always false: simDelay always suspends to schedule a resume. */
  constexpr bool await_ready() const noexcept {
    return false;
  }

  /**
   * Suspend the coroutine and schedule its resumption after `delay` on
   * `ctx` via the PDES engine. Terminates on scheduler OOM (see
   * SimTask.h, "Error model").
   *
   * @param h        Handle to the suspended coroutine frame.
   */
  void await_suspend(std::coroutine_handle<> h) const noexcept {
    ctx->schedule(delay, [h] { h.resume(); });
  }

  /** No-op. simDelay does not produce a value. */
  constexpr void await_resume() const noexcept {}
};

/**
 * Factory for a SimDelay awaitable.
 *
 * @param ctx      Simulation context that schedules the resume.
 * @param delay    How long to suspend before resuming.
 *
 * @return         A SimDelay awaitable; must be `co_await`-ed.
 */
[[nodiscard(
    "simDelay's return value must be co_await'd; discarding it creates a "
    "coroutine primitive that is never scheduled")]]
FOLLY_ALWAYS_INLINE SimDelay
simDelay(folly::not_null<SimContext*> ctx, Time delay) noexcept {
  return SimDelay{ctx, delay};
}

// Lock the assignment-deletion contract at compile time so a future
// member-mutation refactor that re-enables assignment fails loudly.
static_assert(!std::is_copy_assignable_v<SimDelay>);
static_assert(!std::is_move_assignable_v<SimDelay>);

/**
 * Yield to the PDES (Parallel Discrete Event Simulation) scheduler without
 * advancing simulation time.
 *
 * Equivalent to SimContext::scheduleNow (see packsim/kernel/sim-context.h) —
 * defers to the priority queue to prevent stack overflow from long chains
 * of synchronous work at the same simulation time.
 *
 * @param ctx      Simulation context that schedules the resume.
 *
 * @return         A SimDelay awaitable with zero delay; must be `co_await`-ed.
 */
[[nodiscard(
    "simYield's return value must be co_await'd; discarding it creates a "
    "coroutine primitive that is never scheduled")]]
FOLLY_ALWAYS_INLINE SimDelay
simYield(folly::not_null<SimContext*> ctx) noexcept {
  return simDelay(ctx, detail::kZeroDelay);
}

} // namespace packsim::coro
