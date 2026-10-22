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

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

namespace packsim::coro {

/**
 * Frame-local cancellable delay.
 *
 * A one-shot delay (like `simDelay`) whose scheduled resume is cancelled by the
 * awaitable's OWN destructor — so destroying the suspended coroutine frame
 * (e.g. `SimScopedTask::reset()` at teardown) tears the delay down cleanly with
 * no dangling PDES (Parallel Discrete Event Simulation) event. Contrast:
 *   - `simDelay` — does NOT record its event handle, so destroying a frame
 *     parked on it leaves the scheduled resume to fire into the freed frame
 *     (UB).
 *   - `SimCancellableDelay` — a LONG-LIVED member with an explicit `cancel()`;
 *     its frame-local `Awaitable` does not self-cancel, so it relies on the
 *     owner calling `cancel()`. Wrong tool for a per-frame transient delay.
 *
 * Use `SimScopedDelay` for a one-shot delay INSIDE a coroutine that may be
 * destroyed mid-delay at teardown (e.g. `LinkLayerScheduler::txDriver`'s
 * serialization wait, so the `txDriver` can be `reset()` while parked there).
 * It is the delay companion to `SimScopedTask`'s scoped/RAII teardown model —
 * see `SimScopedTask`'s "When to use this" note for when the scoped/RAII
 * model applies vs the cooperative `shutdownRequested_` idiom.
 *
 * The awaitable IS the primitive (same shape as `SimDelay`): construct it via
 * the `simScopedDelay(ctx, delay)` factory and `co_await` it inline. It is
 * non-copyable AND non-movable — it owns a `Scheduler::EventHandle` that its
 * destructor cancels, and because `Event` is a trivially-copied handle (see
 * pdes/Event.h) any copy/move would let two objects cancel the same event.
 * `co_await simScopedDelay(...)` needs neither: the prvalue is materialized
 * directly into the coroutine frame (guaranteed copy elision).
 *
 * GOTCHA — `destroy()` does NOT run code between consecutive `co_await`s: the
 * statements after `co_await simScopedDelay(...)` do NOT run when the frame is
 * destroyed mid-delay. Wrap cancel-sensitive cleanup in a function-scope RAII
 * guard. Same caveat as every cancellable coro primitive.
 *
 * Single SimContext only — the resume fires on the context that scheduled it.
 */
class SimScopedDelay {
 public:
  SimScopedDelay(folly::not_null<SimContext*> ctx, Time delay) noexcept
      : ctx_(ctx), delay_(delay) {}

  SimScopedDelay(const SimScopedDelay&) = delete;
  SimScopedDelay& operator=(const SimScopedDelay&) = delete;
  SimScopedDelay(SimScopedDelay&&) = delete;
  SimScopedDelay& operator=(SimScopedDelay&&) = delete;

  /**
   * Cancel the scheduled resume if the frame is destroyed before the delay
   * fired (the teardown case). No-op if the delay already fired or was never
   * armed — `Event::cancel()` is guarded by a version check (see pdes/Event.h),
   * so cancelling a fired or stale event handle is safe.
   */
  ~SimScopedDelay() noexcept {
    scheduledResume_.cancel();
  }

  /** @return Always false: the delay always suspends to schedule a resume. */
  constexpr bool await_ready() const noexcept {
    return false;
  }

  /**
   * Suspend and schedule the resume after `delay_`, recording the event handle
   * so the destructor can cancel it. Terminates on scheduler OOM (SimTask.h,
   * "Error model").
   *
   * @param h  Handle to the suspended coroutine frame.
   */
  void await_suspend(std::coroutine_handle<> h) noexcept {
    scheduledResume_ = ctx_->schedule(delay_, [h] { h.resume(); });
  }

  /** No-op. simScopedDelay does not produce a value. */
  constexpr void await_resume() const noexcept {}

 private:
  const folly::not_null<SimContext*> ctx_;
  const Time delay_;
  // PDES event handle of the scheduled resume. Cancelled by ~SimScopedDelay if
  // the frame is destroyed before the resume fires.
  Scheduler::EventHandle scheduledResume_{};
};

/**
 * Factory for a `SimScopedDelay` awaitable.
 *
 * @param ctx    Simulation context that schedules the resume.
 * @param delay  How long to suspend before resuming.
 *
 * @return       A `SimScopedDelay` awaitable; must be `co_await`-ed inline.
 */
[[nodiscard(
    "simScopedDelay's return value must be co_await'd; discarding it creates a "
    "coroutine primitive that is never scheduled")]]
FOLLY_ALWAYS_INLINE SimScopedDelay
simScopedDelay(folly::not_null<SimContext*> ctx, Time delay) noexcept {
  return SimScopedDelay{ctx, delay};
}

} // namespace packsim::coro
