/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>
#include <cstdint>

#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/coro/ResumeMode.h"
#include "packsim/kernel/coro/WaiterState.h"
#include "packsim/kernel/sim-context.h"

namespace packsim::coro {

/**
 * A simulation-aware signal for converting push-style callbacks (e.g.,
 * transport message completions) into coroutine-friendly awaitables.
 *
 * Design:
 *   - Multiple signals can accumulate while the coroutine is suspended
 *     or running (counted via pendingSignals_).
 *   - When the coroutine `co_await`s the signal:
 *     - If signals are pending, it resumes immediately (await_ready=true)
 *     - Otherwise, it suspends until signal() is called.
 *   - signal() either resumes the waiter directly (default) or schedules
 *     the resume via scheduleNow.
 *   - The deferred-resume bookkeeping (event-handle tracking, race-safe cancel,
 *     single-resume-per-pending-window guard) lives in WaiterState and is
 *     shared across SimSignal/SimFuture/SimQueue.
 *   - All operations run on a single PDES (Parallel Discrete Event
 *     Simulation) context thread — no locks.
 *
 * Example (handleAck, complete, progress are placeholders for the
 * calling class's own methods):
 *
 *     SimSignal transportDone(simCtx);
 *
 *     // Wire transport callbacks:
 *     setOnRequestAckedCallback([&](Message* msg) {
 *       handleAck(msg);
 *       transportDone.signal();
 *     });
 *
 *     // In coroutine:
 *     while (!complete()) {
 *       co_await transportDone.wait();
 *       progress();
 *     }
 *
 * CONTEXT-LOCAL: All operations MUST happen on the SimContext passed at
 * construction. The primitive MUST outlive any in-flight `co_await` against
 * it AND any resume callbacks already enqueued in the PDES queue.
 *
 * To signal from another context, enqueue a remote event:
 *
 *     targetCtx->scheduleRemote([&sig] { sig.signal(); });
 *
 * or `co_await simScheduleOn(targetCtx)` before calling signal().
 */
class SimSignal {
 public:
  /**
   * @param ctx     Simulation context (for deferred resume scheduling).
   * @param mode    ResumeMode::DIRECT (default) — signal() resumes the
   *                coroutine inline within the caller. ResumeMode::DEFERRED
   *                — resumption is posted via scheduleNow. Use Deferred
   *                for deep-chain consumers where direct resume could grow
   *                the stack unboundedly.
   *
   *                Note: SimQueue defaults to ResumeMode::DEFERRED instead
   *                — see SimQueue.h's STACK-DEPTH SAFETY section for the
   *                split rationale.
   */
  explicit SimSignal(
      folly::not_null<SimContext*> ctx,
      const ResumeMode mode = ResumeMode::DIRECT)
      : waiter_(ctx, mode) {}

  SimSignal(const SimSignal&) = delete;
  SimSignal& operator=(const SimSignal&) = delete;
  SimSignal(SimSignal&&) = delete;
  SimSignal& operator=(SimSignal&&) = delete;

  /**
   * Destructor. Always-on XCHECK catches the "SimSignal destroyed while
   * a coroutine is still suspended on it" lifetime bug — the suspended
   * coroutine frame would leak otherwise.
   */
  ~SimSignal() noexcept {
    XCHECK(!waiter_.has()) << "SimSignal destroyed with a suspended waiter; "
                              "coroutine frame leaked";
  }

  struct Awaitable {
    SimSignal& sig;

    // ----- Special members (Rule of Five) -----
    // The destructor below detaches this waiter, so the awaitable is no longer
    // an aggregate. It is moved ONLY before `await_suspend` installs it
    // (simWaitFirst moves operands into its tuple at construction; a bare
    // `co_await` elides the move), so a moved-from source has no installed
    // waiter and its destructor is a no-op. Copy is deleted.
    explicit Awaitable(SimSignal& s) noexcept : sig(s) {}
    Awaitable(Awaitable&&) noexcept = default;
    Awaitable& operator=(Awaitable&&) = delete;
    Awaitable(const Awaitable&) = delete;
    Awaitable& operator=(const Awaitable&) = delete;

    /**
     * Destroy-safe teardown. If the owning coroutine frame is destroyed while
     * suspended on this awaitable (e.g. `SimScopedTask::reset()`), detach the
     * waiter — cancel any pending deferred resume and clear the installed
     * handle — WITHOUT destroying the frame (it is already unwinding), so
     * `~SimSignal` sees no waiter and no deferred resume fires into the freed
     * frame. No-op when no waiter is installed (the loser-cancel and
     * normal-resume paths clear it first).
     *
     * `detach()` clears the SimSignal's single SHARED waiter slot
     * unconditionally. That is safe because the single-waiter invariant
     * (`await_suspend`'s XCHECK) plus the move-before-install rule above
     * guarantee the slot holds either nothing or *this* awaitable's handle —
     * never some other live waiter's. If a future change ever held a
     * moved-from awaitable across an install, or added a second await entry
     * point, this dtor would need to guard the clear on owning the slot.
     */
    ~Awaitable() noexcept {
      sig.waiter_.detach();
    }

    /**
     * True if signals are pending — the coroutine resumes immediately
     * without suspending. Otherwise the coroutine suspends until signal().
     */
    constexpr bool await_ready() const noexcept {
      return sig.pendingSignals_ > 0;
    }

    void await_suspend(std::coroutine_handle<> h) const noexcept {
      XCHECK(!sig.waiter_.has())
          << "SimSignal already has a suspended waiter; concurrent waiters "
             "are not supported";
      sig.waiter_.install(h);
    }

    /** Consume one signal on resume. */
    void await_resume() noexcept {
      XDCHECK_GT(sig.pendingSignals_, 0u);
      sig.pendingSignals_--;
    }

    /**
     * Tear down this awaitable's hold on the signal. Used by the
     * `simWaitFirst` loser-cancel contract to drop a suspended waiter
     * when another operand wins the race. Forwards to `SimSignal::cancel`.
     */
    void cancel() noexcept {
      sig.cancel();
    }
  };

  /**
   * Wait for the next signal. Returns immediately if signals are pending.
   *
   * @return  An Awaitable that the caller must `co_await`.
   */
  [[nodiscard(
      "SimSignal::wait()'s return value must be co_await'd; discarding it "
      "creates an awaitable that never suspends or consumes a signal")]]
  Awaitable wait() noexcept {
    return Awaitable{*this};
  }

  /**
   * Signal the coroutine that an event has occurred. If a coroutine is
   * waiting, it is resumed (inline or via scheduleNow per ResumeMode).
   * Otherwise the signal is accumulated. Terminates on scheduler OOM
   * (see SimTask.h, "Error model").
   */
  void signal() noexcept {
    pendingSignals_++;
    waiter_.notify();
  }

  /** Number of pending (unconsumed) signals. */
  [[nodiscard]] uint32_t pendingSignals() const noexcept {
    return pendingSignals_;
  }

  /**
   * True if a coroutine is suspended waiting for this signal (incl. across
   * the deferred-resume window in ResumeMode::DEFERRED mode).
   */
  [[nodiscard]] bool hasWaiter() const noexcept {
    return waiter_.has();
  }

  /**
   * Tear down the suspended waiter (if any) without resuming it. Removes
   * any pending deferred resume from the PDES queue, then destroys the
   * suspended coroutine frame. Safe to call multiple times and safe when
   * no waiter is suspended (no-op).
   *
   * Does NOT clear `pendingSignals_`. If the SimSignal is reused after
   * cancel(), a fresh waiter will consume the previously accumulated
   * signals via await_ready before suspending.
   */
  void cancel() noexcept {
    waiter_.cancel();
  }

 private:
  detail::WaiterState waiter_;
  uint32_t pendingSignals_ = 0;
};

} // namespace packsim::coro
