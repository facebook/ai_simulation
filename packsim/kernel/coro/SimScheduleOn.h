/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>

#include <folly/CPortability.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/sim-context.h"

namespace packsim::coro {

/**
 * Cross-context coroutine migration awaitable.
 *
 * Suspends the coroutine on the current SimContext and resumes it on the
 * target SimContext. The resume is scheduled via `scheduleRemote()`, which
 * adds the engine's lookahead delay to avoid causality paradoxes — the
 * coroutine resumes at `now() + lookahead` on the target context, not at
 * the same logical time it suspended.
 *
 * The only thread-safe primitive for moving a coroutine across
 * SimContexts. Per-context primitives (SimSignal, SimFuture, SimQueue) are
 * single-context only — to coordinate across contexts, use SimScheduleOn
 * to migrate first, then call the primitive on the target context.
 *
 * CROSS-CONTEXT: `targetCtx` MUST outlive both the suspended coroutine and
 * the scheduled resume callback enqueued on `targetCtx`. There is no
 * `cancel()` escape hatch.
 *
 * Example:
 *
 *     SimTask coordinatorTask(SimContext* ctxA, SimContext* ctxB) {
 *       // Work on context A.
 *       co_await simDelay(ctxA, Time::fromNs(100));
 *       computeOnA();
 *
 *       // Migrate to context B (resumes after lookahead delay).
 *       co_await simScheduleOn(ctxB);
 *       computeOnB();
 *     }
 *
 * Lookahead delay: configurable via
 * `packsim/kernel/scheduler.h`'s `Scheduler::setLookaheadTimeFs` (read back
 * via `getLookaheadTime()`). A Scheduler starts at 1ns (`kDefaultLookaheadFs`
 * in kernel/scheduler.cpp), and `PackSimSimulation` sets it to the topology's
 * smallest link delay. The resume runs on the target context's PDES (Parallel
 * Discrete Event Simulation) worker thread.
 *
 * Footgun: simScheduleOn(currentContext) still incurs a lookahead delay (the
 * scheduler doesn't optimize same-context self-migration). Avoid calling
 * simScheduleOn on the context the coroutine is already running on — use a
 * plain simYield(ctx) if you only want to defer to the next dispatch.
 */
struct SimScheduleOn {
  // Target context to migrate to. MUST outlive both the suspended
  // coroutine and the scheduled resume callback.
  const folly::not_null<SimContext*> targetCtx;

  /** @return Always false: SimScheduleOn always suspends to schedule a
   *          remote resume on the target context. */
  constexpr bool await_ready() const noexcept {
    return false;
  }

  /**
   * Suspend the coroutine and schedule its resumption on `targetCtx` via
   * `scheduleRemote` (adds engine lookahead delay to avoid causality
   * paradoxes). Scheduler OOM (bad_alloc from `scheduleRemote`) escapes this
   * `noexcept` boundary and terminates the process (see SimTask.h, "Error
   * model"). The lookahead is always positive: `PdesEngine` rejects 0 when
   * it is constructed or set (see `PdesEngine.SetLookaheadTimeRejectsZero`).
   *
   * @param h        Handle to the suspended coroutine frame.
   */
  void await_suspend(std::coroutine_handle<> h) const noexcept {
    targetCtx->scheduleRemote([h] { h.resume(); });
  }

  /** No-op. SimScheduleOn doesn't produce a value. */
  constexpr void await_resume() const noexcept {}
};

/**
 * Factory for a SimScheduleOn awaitable.
 *
 * @param targetCtx    The simulation context to migrate the coroutine onto.
 *                     MUST outlive the resulting awaitable's resume.
 *
 * @return             A SimScheduleOn awaitable; must be `co_await`-ed.
 *
 * @throw std::invalid_argument
 *                     If a raw nullptr is passed for `targetCtx`. The
 *                     throw fires in the caller's parameter-binding frame
 *                     (the `noexcept` on this function only covers the
 *                     body, which cannot fail).
 */
[[nodiscard(
    "simScheduleOn's return value must be co_await'd; discarding it creates a "
    "coroutine primitive that is never scheduled")]]
FOLLY_ALWAYS_INLINE SimScheduleOn
simScheduleOn(folly::not_null<SimContext*> targetCtx) noexcept {
  return SimScheduleOn{targetCtx};
}

} // namespace packsim::coro
