/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>
#include <type_traits>

#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

namespace packsim::coro {

/**
 * Long-lived primitive whose `awaitDelay(ctx, delay)` method returns a
 * cancellable awaitable. The primitive holds the PDES (Parallel Discrete
 * Event Simulation) event handle of the scheduled resume callback AND the
 * suspended coroutine handle, so cancel() can synchronously remove the
 * resume from the PDES queue AND destroy the suspended frame.
 *
 * Use this for periodic-timer patterns where callback code holds a
 * `Scheduler::EventHandle` and calls `cancel()` on it (e.g., the alpha-update
 * and rate-increase timers in `DcqcnCongestionControl`, which are written as
 * callbacks). Converted to a coroutine, such a timer keeps the callback's
 * cancel semantics (the resume callback never runs), AND cancel() also frees
 * the coroutine frame.
 *
 * Example:
 *
 *     SimCancellableDelay alphaTimer_;  // member of the owning class
 *
 *     SimTask RateController::alphaUpdateLoop() {
 *       while (true) {
 *         co_await alphaTimer_.awaitDelay(simCtx_, alphaPeriod_);
 *         alpha_ = updateAlpha(alpha_);
 *       }
 *     }
 *
 *     void RateController::cancelTimers() {
 *       alphaTimer_.cancel();  // synchronous: matches Event.cancel()
 *     }
 *
 * Cancellation-and-restart pattern: call `alphaTimer_.cancel()` to tear down
 * the in-flight delay, then start a fresh `alphaUpdateLoop()` coroutine to
 * re-arm with a new period.
 *
 * cancel() semantics match the existing PDES `Event.cancel()` semantic
 * exactly (see packsim/kernel/pdes/Event.h `Scheduler::EventHandle::cancel`) —
 * the scheduled callback never runs, and the PDES queue is in the same
 * state as if the delay had never been scheduled.
 *
 * Non-copyable AND non-movable: owns a `coroutine_handle` whose frame
 * address is fixed, so it must be stored as a long-lived member of the
 * owning object — never passed around by value.
 *
 * Single-in-flight contract: at most one delay may be in flight at any
 * time. Awaiting a second `awaitDelay` while another is in flight
 * aborts via XCHECK in `Awaitable::await_suspend`.
 *
 * Not destroy-safe on a bare await: destroying a frame parked on
 * `co_await awaitDelay(...)` by other means (e.g. `SimScopedTask::reset()`)
 * leaves the resume scheduled and the freed handle recorded: the resume
 * would run the freed frame, and a later cancel() would destroy it twice.
 * Use `SimScopedDelay` in such a frame. As a `simWaitFirst` operand,
 * `awaitDelay()` is safe: the race's teardown cancels it.
 *
 * Class invariant: `hasInFlight()` is true iff `suspendedHandle_` is
 * non-null AND `scheduledResume_` refers to the live PDES event for that
 * handle. The two are written and cleared as a pair (in
 * `Awaitable::await_suspend`, in `cancel()`, and inside the resume
 * lambda).
 *
 * Safe to call cancel() multiple times. Safe to call when no delay is in
 * flight (no-op).
 */
class SimCancellableDelay {
 public:
  SimCancellableDelay() = default;

  // Owns a coroutine_handle whose frame has a fixed memory location, so any
  // copy or move would alias or strand it.
  SimCancellableDelay(const SimCancellableDelay&) = delete;
  SimCancellableDelay& operator=(const SimCancellableDelay&) = delete;
  SimCancellableDelay(SimCancellableDelay&&) = delete;
  SimCancellableDelay& operator=(SimCancellableDelay&&) = delete;

  /**
   * Destructor. Always-on XCHECK (not XDCHECK): one predictable branch per
   * destruction catches frame leaks in opt builds where XDCHECK would
   * compile out.
   */
  ~SimCancellableDelay() noexcept {
    XCHECK(!hasInFlight())
        << "SimCancellableDelay destroyed with an in-flight delay; "
           "coroutine frame and scheduled event would be orphaned";
  }

  /**
   * Cancel the in-flight delay synchronously. Removes the scheduled resume
   * from PDES via Scheduler::EventHandle::cancel() (see
   * packsim/kernel/pdes/Event.h), then destroys the suspended coroutine frame.
   * Safe on a default-constructed instance and safe to call multiple times.
   *
   * GOTCHA — cancel() does NOT run code between consecutive co_awaits:
   * `coroutine_handle::destroy()` unwinds the stack frame at the suspension
   * point and runs destructors of locals scoped to that suspension. It does
   * NOT execute statements that come after the awaited expression.
   * Concretely:
   *
   *     co_await timer.awaitDelay(ctx, delay);
   *     cleanup_logic_here();    // NEVER RUNS on cancel
   *     co_await otherAwaitable;
   *
   * If the cancellation point matters for correctness — e.g., a partially-
   * acquired resource that needs explicit release — wrap the lifetime in
   * an RAII guard at function scope, since destructors of locals at the
   * suspension point WILL run during destroy() unwind. Mirrors the
   * GOTCHA paragraph in coro/WaiterState.h for the SimSignal/SimFuture/
   * SimQueue family — the same destroy()-skips-between-awaits caveat
   * applies to every coro primitive whose cancel() destroys a frame.
   *
   * `noexcept` relies on `Scheduler::EventHandle::cancel()` being `noexcept`
   * (a single bool write; see the static_asserts in the private section
   * below).
   */
  void cancel() noexcept {
    scheduledResume_.cancel();
    scheduledResume_ = {};
    if (suspendedHandle_) {
      // Copy-then-clear-then-destroy: clearing suspendedHandle_ before
      // h.destroy() guarantees hasInFlight() == false during frame
      // destruction, so a coroutine body that re-enters cancel() (or
      // observes the primitive through any path) sees clean state.
      const auto h = suspendedHandle_;
      suspendedHandle_ = {};
      h.destroy();
    }
  }

  // True if a delay is currently in flight (suspended coroutine awaits a
  // scheduled resume).
  [[nodiscard]] bool hasInFlight() const noexcept {
    return static_cast<bool>(suspendedHandle_);
  }

  /**
   * Single-use awaitable returned by `awaitDelay()`. Users typically don't
   * name this type — they consume it inline via
   * `co_await timer.awaitDelay(ctx, delay)`. Same shape as
   * `SimSignal::Awaitable`, `SimFuture::Awaitable`, `SimQueue::Awaitable`,
   * `SimBarrier::Awaitable`.
   */
  struct Awaitable {
    const folly::not_null<SimCancellableDelay*> self;
    const folly::not_null<SimContext*> ctx;
    const Time delay;

    /** @return Always false: the delay always suspends to schedule a resume. */
    constexpr bool await_ready() const noexcept {
      return false;
    }

    /**
     * Suspend the coroutine and schedule a resume on `ctx` after `delay`,
     * recording the event handle and frame handle into `*self` for later
     * synchronous cancellation. Terminates on scheduler OOM (see
     * SimTask.h, "Error model").
     *
     * Always-on XCHECK (not XDCHECK): one predictable branch per suspend
     * catches double-await misuse in opt builds where XDCHECK would compile
     * out.
     *
     * @param h        Handle to the suspended coroutine frame.
     */
    void await_suspend(std::coroutine_handle<> h) const noexcept {
      XCHECK(!self->hasInFlight())
          << "SimCancellableDelay::awaitDelay await_suspend with an in-flight "
             "delay; would leak previous coroutine frame and orphan scheduled "
             "event";
      // Post-schedule writes are race-free against the lambda because PDES
      // is single-thread-per-context: the lambda can only fire from the same
      // worker thread that's currently executing this await_suspend, and
      // that thread won't return to the dispatch loop until after the writes
      // complete. Two scenarios would break this — both are out-of-contract:
      //   1. Synchronous dispatch (PDES never does this today; see
      //      PdesEngine.cpp's dispatch loop — events fire only there).
      //   2. Cross-context misuse — `ctx` is not the awaiter's context. Also
      //      breaks `h.resume()` independently. Same-context is the contract.
      self->scheduledResume_ = ctx->schedule(delay, [h, hdl = self] {
        hdl->suspendedHandle_ = {};
        hdl->scheduledResume_ = {};
        h.resume();
      });
      self->suspendedHandle_ = h;
    }

    /** No-op. The awaitable does not produce a value. */
    constexpr void await_resume() const noexcept {}

    /**
     * Tear down this awaitable's hold on the delay. Used by the
     * `simWaitFirst` loser-cancel contract to drop a suspended waiter
     * when another operand wins the race. Forwards to
     * `SimCancellableDelay::cancel`.
     */
    void cancel() noexcept {
      self->cancel();
    }
  };

  /**
   * Returns the awaitable. `co_await` it to suspend and schedule a resume
   * on `ctx` after `delay`, recording state into this instance for later
   * synchronous `cancel()`.
   *
   * @param ctx      Simulation context that schedules the resume. MUST
   *                 outlive both the suspended coroutine and the
   *                 scheduled resume. MUST be the same context that
   *                 will later drive the suspended coroutine; cross-
   *                 context use is unsupported (use simScheduleOn for
   *                 cross-context migration before awaiting here).
   * @param delay    How long to suspend before resuming. Zero is allowed
   *                 (equivalent to simYield — defers to the event queue
   *                 without advancing simulation time).
   *
   * @return         An Awaitable that must be `co_await`-ed.
   */
  [[nodiscard(
      "SimCancellableDelay::awaitDelay()'s return value must be co_await'd; "
      "discarding it creates a coroutine primitive that is never scheduled")]]
  Awaitable awaitDelay(folly::not_null<SimContext*> ctx, Time delay) noexcept {
    return Awaitable{this, ctx, delay};
  }

 private:
  // Compile-time validation of the contracts SimCancellableDelay::cancel()
  // — and the resume lambda in Awaitable::await_suspend that hands work to
  // it — rely on for their noexcept guarantee. Lock these here so a future
  // Scheduler refactor that breaks any of them fails to compile rather
  // than silently lose the noexcept.
  static_assert(
      std::is_nothrow_default_constructible_v<Scheduler::EventHandle>);
  static_assert(std::is_nothrow_assignable_v<
                Scheduler::EventHandle&,
                Scheduler::EventHandle>);
  static_assert(
      noexcept(std::declval<const Scheduler::EventHandle&>().cancel()));

  // cancel() also calls std::coroutine_handle<>::destroy() on the
  // suspended frame. The libstdc++ this is built against does NOT decorate
  // destroy() with the noexcept specifier (so a static_assert on
  // `noexcept(... .destroy())` would fail), but destroy() is
  // implemented as a single __builtin_coro_destroy call that the
  // compiler emits as a non-throwing branch. If destroy() ever does
  // throw, cancel()'s noexcept will fire std::terminate — the practical
  // guarantee callers rely on. Re-evaluate adding a static_assert here
  // once the toolchain catches up.

  // PDES event handle of the scheduled resume callback. Default-constructed =
  // no in-flight delay. cancel() removes the event from the scheduler's
  // queue synchronously.
  Scheduler::EventHandle scheduledResume_;

  // Suspended coroutine handle; valid iff a delay is in flight. Cleared
  // by the resume callback (natural fire) or by cancel().
  std::coroutine_handle<> suspendedHandle_;
};

} // namespace packsim::coro
