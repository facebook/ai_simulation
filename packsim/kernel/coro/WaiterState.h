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

#include "packsim/kernel/coro/ResumeMode.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"

namespace packsim::coro::detail {

// Lock down the Scheduler::EventHandle contract that WaiterState's noexcept
// paths rely on (cancel(), isActive() in notify() and the destructor's
// XCHECK, and the scheduledResume_ self-clear in the fire lambda). If a future
// Scheduler refactor breaks any of these, we fail to compile here rather than
// silently lose the noexcept guarantee.
static_assert(std::is_nothrow_default_constructible_v<Scheduler::EventHandle>);
static_assert(std::is_nothrow_assignable_v<
              Scheduler::EventHandle&,
              Scheduler::EventHandle>);
static_assert(noexcept(std::declval<const Scheduler::EventHandle&>().cancel()));
static_assert(
    noexcept(std::declval<const Scheduler::EventHandle&>().isActive()));

/**
 * Composable building block for coroutine primitives — owns a single-waiter
 * slot plus the race-safe deferred-resume + cancel-during-pending machinery.
 * Used today by SimSignal, SimFuture, and SimQueue.
 *
 * Lives in `packsim::coro::detail::`: application code must not depend on it
 * directly. NOT intended for application use — write a coroutine
 * primitive (sibling to SimSignal/SimFuture/SimQueue) and compose this as
 * a member if you need single-waiter + race-safe deferred-resume +
 * cancel-during-pending semantics.
 *
 * Each owning primitive holds a single suspended coroutine (its waiter),
 * and on the producer event (signal/resolve/push) either resumes it inline
 * (ResumeMode::DIRECT) or posts the resume through scheduleNow
 * (ResumeMode::DEFERRED) — bounding stack depth on long producer chains.
 *
 * The deferred path is the one with subtle invariants:
 *   - We track the scheduled event handle so cancel() can synchronously remove
 *     the in-flight resume from the PDES queue, closing the race where
 *     the queued lambda still captures a coroutine handle that cancel()
 *     would otherwise destroy.
 *   - Multiple notify() calls before the lambda fires schedule ONE resume
 *     (guarded by scheduledResume_.isActive()), so only the first
 *     deferred resume is enqueued.
 *
 * CONTEXT-LOCAL: install/notify/cancel must run on the SimContext passed
 * at construction. Holding a coroutine handle whose frame lives at a
 * fixed memory address means this type is non-copyable and non-movable.
 *
 * ASSERTION POLICY: lifetime tripwires (destructor invariants, double-await
 * misuse, primitive-destroyed-with-suspended-waiter) use always-on XCHECK
 * so opt builds catch silent frame leaks and dispatch-time UAFs. Internal
 * consistency checks (await_resume sees resolved state, await_resume sees
 * non-empty queue) use XDCHECK because the failure mode is a logic bug in
 * the awaitable itself and only manifests when the awaitable is co_await'd
 * out-of-protocol — both of which surface in dev/test runs. SimSignal,
 * SimFuture, and SimQueue follow this same split.
 *
 * GOTCHA — cancel() does NOT run code between consecutive co_awaits:
 * `cancel()` invokes `coroutine_handle::destroy()`, which unwinds the
 * stack frame at the suspension point and runs destructors of locals
 * scoped to that suspension. It does NOT execute statements that come
 * after the awaited expression. Concretely:
 *
 *     co_await primitiveA;       // suspended here, then cancel()-ed
 *     cleanup_logic_here();      // NEVER RUNS on cancel
 *     co_await primitiveB;
 *
 * If the cancellation point matters for correctness — e.g., a partially-
 * acquired resource that needs explicit release — wrap the lifetime in
 * an RAII guard at function scope, since destructors of locals at the
 * suspension point WILL run during destroy() unwind.
 */
class WaiterState {
 public:
  /**
   * @param ctx     Simulation context that schedules the deferred
   *                resume. MUST outlive any in-flight notify().
   * @param mode    Direct: notify() resumes the waiter inline.
   *                Deferred: notify() posts the resume via scheduleNow
   *                (preferred for deep producer chains to bound stack
   *                depth).
   */
  explicit WaiterState(
      folly::not_null<SimContext*> ctx,
      const ResumeMode mode) noexcept
      : ctx_(ctx), mode_(mode) {}

  WaiterState(const WaiterState&) = delete;
  WaiterState& operator=(const WaiterState&) = delete;
  WaiterState(WaiterState&&) = delete;
  WaiterState& operator=(WaiterState&&) = delete;

  /**
   * Destructor. Two defensive XCHECKs guard the lifetime contract.
   *
   * Class invariants the destructor leans on:
   *   - I1: `waiter_` is non-null iff a coroutine is suspended on this
   *         WaiterState (set by install(), cleared by direct notify(), the
   *         deferred-resume lambda body, or cancel()).
   *   - I2: `scheduledResume_.isActive()` is true iff a deferred-resume
   *         lambda is queued in PDES and still active (set by the deferred
   *         path of notify(), cleared by the lambda body or cancel()).
   *   - Coupling: today, I2 ⟹ I1. The deferred lambda is the only thing
   *     that clears `waiter_` in the deferred path, and it does so before
   *     calling `h.resume()`. So a queued lambda implies the consumer is
   *     still suspended.
   *
   * Why we don't fear the captured `this` in the deferred lambda:
   * the lambda captures `this` raw, so destroying WaiterState while the
   * lambda is still queued would, in principle, dangle `this` and UAF on
   * `this->waiter_` / `this->scheduledResume_` when PDES fires it. We
   * never reach that UAF because:
   *   - "Lambda still queued and dispatchable" ⇔ I2 holds.
   *   - I2 ⟹ I1 (by Coupling), so the I1 XCHECK below would already abort.
   *   - The I2 XCHECK below makes the "lambda still queued" check direct
   *     instead of relying on the I2 ⟹ I1 invariant — defense-in-depth
   *     against a future refactor that decouples them (e.g., a code path
   *     that clears `waiter_` without also taking down the queued event).
   *
   * Owning primitives (SimSignal/SimFuture/SimQueue) already check has()
   * with primitive-specific messages in their destructors, so the I1
   * XCHECK here is redundant *for them*. Both checks remain as the
   * authoritative leak-and-UAF tripwire for any future direct user of
   * WaiterState.
   */
  ~WaiterState() noexcept {
    XCHECK(!waiter_) << "WaiterState destroyed with a suspended waiter; "
                        "coroutine frame leaked";
    XCHECK(!scheduledResume_.isActive())
        << "WaiterState destroyed with an active deferred resume queued; "
           "the lambda's captured `this` would UAF on dispatch";
  }

  /**
   * Install a coroutine handle as the waiter. Caller (await_suspend) is
   * responsible for the single-waiter XCHECK so the diagnostic can name
   * the owning primitive.
   *
   * @param h    Handle to the suspended coroutine frame.
   */
  void install(std::coroutine_handle<> h) noexcept {
    waiter_ = h;
  }

  /** @return True if a waiter is currently installed. */
  [[nodiscard]] bool has() const noexcept {
    return static_cast<bool>(waiter_);
  }

  /**
   * Resume the waiter inline (ResumeMode::DIRECT) or post a deferred resume
   * via scheduleNow (ResumeMode::DEFERRED). No-op if no waiter is
   * suspended.
   *
   * `noexcept` per the framework exception policy (see SimTask.h, "Error
   * model"). Deferred path: `ctx_->scheduleNow` allocation OOM = terminate at
   * the failure site rather than unwinding into a partially-committed
   * state. DIRECT path: `h.resume()` runs the body inline on this stack
   * — the noexcept claim depends on the resumed coroutine type's
   * `unhandled_exception()` not rethrowing (SimTask satisfies this; see
   * "Error model" in SimTask.h for the contract any future coroutine type
   * composed with WaiterState must adopt).
   */
  void notify() noexcept {
    if (!waiter_) {
      return;
    }
    if (mode_ == ResumeMode::DIRECT) {
      // DIRECT mode: h.resume() runs the suspended coroutine body inline
      // on this stack. notify()'s noexcept claim depends on the coroutine
      // type's unhandled_exception() terminating rather than rethrowing —
      // SimTask satisfies this; see SimTask.h, "Error model" for the contract
      // that any new coroutine type composed here must adopt.
      const auto h = waiter_;
      waiter_ = {};
      h.resume();
    } else if (!scheduledResume_.isActive()) {
      // Deferred path: leave `waiter_` set (asymmetric with the Direct
      // path above) so cancel() during the queued-but-not-yet-fired
      // window can find the suspended frame and destroy it. The lambda
      // body below clears `waiter_` itself before resuming — see the
      // class destructor docstring (invariants I1/I2) for the full
      // lifetime argument.
      //
      // Lambda captures `this` raw; the WaiterState destructor's
      // XCHECK(!scheduledResume_.isActive()) (defense-in-depth I2) is
      // the explicit guard against dispatch-time UAF if the owner is
      // destroyed while this lambda is queued. See the destructor
      // comment for why the I1 XCHECK already covers it today.
      //
      // Self-clear in the lambda so cancel() races (after dispatch but
      // before the resumed coroutine completes) see empty state, and a
      // nested co_await on the same primitive in the resumed coroutine
      // can install a fresh waiter without tripping the install XCHECK.
      const auto h = waiter_;
      scheduledResume_ = ctx_->scheduleNow([this, h] {
        waiter_ = {};
        scheduledResume_ = {};
        h.resume();
      });
      XDCHECK(waiter_)
          << "deferred notify must leave waiter_ set so cancel() during the "
             "queued-but-not-yet-fired window can find and destroy the frame";
    }
    // else: a deferred resume is already pending; subsequent notify()s are
    // absorbed and the consumer picks up state via the existing resume.
  }

  /**
   * Tear down the suspended waiter (if any) without resuming it. Removes
   * any pending deferred resume from the PDES queue, then destroys the
   * suspended coroutine frame. Safe to call multiple times and safe when
   * no waiter is suspended (no-op).
   *
   * See class-level GOTCHA: `destroy()` does NOT run statements between
   * consecutive co_awaits — only destructors of locals scoped to the
   * suspension point.
   *
   * `noexcept` relies on `Scheduler::EventHandle::cancel()` being `noexcept`
   * (a single bool write; see the static_asserts above the class).
   */
  void cancel() noexcept {
    scheduledResume_.cancel();
    scheduledResume_ = {};
    if (waiter_) {
      const auto h = waiter_;
      waiter_ = {};
      h.destroy();
    }
  }

  /**
   * Detach the suspended waiter WITHOUT destroying its frame. For the case
   * where the frame is ALREADY being destroyed — the awaitable's own
   * destructor running during `coroutine_handle::destroy()` (e.g.
   * `SimScopedTask::reset()`). Cancels any pending deferred resume and clears
   * `waiter_`, so the WaiterState's destructor XCHECKs pass and no deferred
   * resume fires into the freed frame. Idempotent.
   *
   * Distinct from `cancel()`: `cancel()` destroys the frame (the SimWaitFirst
   * loser-cancel contract destroys a *child* frame); `detach()` must NOT, as
   * calling `destroy()` on the frame currently unwinding is undefined.
   */
  void detach() noexcept {
    scheduledResume_.cancel();
    scheduledResume_ = {};
    waiter_ = {};
  }

 private:
  const folly::not_null<SimContext*> ctx_;
  const ResumeMode mode_;

  // Set in install(); cleared by cancel() or by the resume path.
  std::coroutine_handle<> waiter_;

  // PDES event for the in-flight deferred resume. Set by notify() in the
  // deferred path, cleared by the fire lambda or by cancel().
  Scheduler::EventHandle scheduledResume_;
};

} // namespace packsim::coro::detail
