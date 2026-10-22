/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>
#include <optional>
#include <type_traits>
#include <utility>

#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/coro/ResumeMode.h"
#include "packsim/kernel/coro/WaiterState.h"
#include "packsim/kernel/sim-context.h"

namespace packsim::coro {

/**
 * A one-shot promise/future for coroutine request-response patterns.
 *
 * The producer calls `resolve(value)` when the result is ready.
 * The consumer calls `co_await future` to get the result.
 *
 * Example — awaitable message send (single-shot):
 *
 *     SimFuture<void> sendFuture(simCtx);
 *     transport->send(msg, [&]{ sendFuture.resolve(); });
 *     co_await sendFuture;  // suspends until transport ACKs
 *
 * Example — request/response loop (natural reuse, no explicit reset):
 *
 *     SimFuture<Reply> rpc(simCtx);
 *     for (auto& req : pending) {
 *       transport->send(req, [&](Reply r){ rpc.resolve(std::move(r)); });
 *       handleReply(co_await rpc);  // await_resume clears the slot
 *     }
 *
 * Teardown: if the producer never resolves and the future owner needs to
 * destroy the primitive, call `cancel()` first — it synchronously removes
 * any pending deferred resume from the PDES queue and destroys the
 * suspended coroutine frame. The awaitable does not detach itself if that
 * frame is destroyed any other way (e.g. `SimScopedTask::reset()`), so the
 * future would keep the freed handle; see "Teardown contract" in
 * SimScopedTask.h.
 *
 * CONTEXT-LOCAL: All operations MUST happen on the SimContext passed at
 * construction. The primitive MUST outlive any in-flight `co_await` against
 * it AND any resume callbacks already enqueued in the PDES (Parallel
 * Discrete Event Simulation) queue.
 *
 * Both specializations are single-slot channels: one `resolve()` per
 * consume cycle. `await_resume` auto-clears the slot, so a subsequent
 * `co_await` suspends until the next `resolve()`. Calling `resolve()`
 * twice without an intervening consume (or `reset()`) trips an XCHECK.
 *
 * Deferred-resume bookkeeping (event-handle tracking, race-safe cancel,
 * single-resume-per-pending-window guard) lives in WaiterState and is
 * shared with SimSignal/SimQueue.
 */
template <typename T = void>
class SimFuture;

/** Specialization for void (no value, just a completion signal). */
template <>
class SimFuture<void> {
 public:
  /**
   * @param ctx     Simulation context (for deferred resume scheduling).
   * @param mode    ResumeMode::DIRECT (default) — resolve() resumes the
   *                coroutine inline. ResumeMode::DEFERRED — posts the
   *                resume via scheduleNow.
   *
   *                Note: SimQueue defaults to ResumeMode::DEFERRED instead
   *                — see SimQueue.h's STACK-DEPTH SAFETY section for the
   *                split rationale.
   */
  explicit SimFuture(
      folly::not_null<SimContext*> ctx,
      const ResumeMode mode = ResumeMode::DIRECT)
      : waiter_(ctx, mode) {}

  SimFuture(const SimFuture&) = delete;
  SimFuture& operator=(const SimFuture&) = delete;
  SimFuture(SimFuture&&) = delete;
  SimFuture& operator=(SimFuture&&) = delete;

  /**
   * Destructor. Always-on XCHECK catches the "SimFuture destroyed while
   * a coroutine is still suspended on it" lifetime bug — the suspended
   * coroutine frame would leak otherwise.
   */
  ~SimFuture() noexcept {
    XCHECK(!waiter_.has())
        << "SimFuture<void> destroyed with a suspended waiter; "
           "coroutine frame leaked";
  }

  struct Awaitable {
    SimFuture& f;

    /** True if already resolved — coroutine resumes immediately. */
    constexpr bool await_ready() const noexcept {
      return f.resolved_;
    }

    void await_suspend(std::coroutine_handle<> h) const noexcept {
      XCHECK(!f.waiter_.has())
          << "SimFuture<void> already has a suspended waiter; concurrent "
             "waiters are not supported";
      f.waiter_.install(h);
    }

    /**
     * Clear `resolved_` on consume so a subsequent `co_await` suspends
     * until the next `resolve()`. Mirrors the auto-clear semantics in
     * SimFuture<T>::Awaitable::await_resume.
     */
    void await_resume() noexcept {
      XDCHECK(f.resolved_);
      f.resolved_ = false;
    }
  };

  [[nodiscard(
      "SimFuture::operator co_await()'s return value must be co_await'd; "
      "discarding it creates an awaitable that never suspends or retrieves "
      "the value")]]
  Awaitable operator co_await() noexcept {
    return Awaitable{*this};
  }

  /**
   * Signal completion. Resumes the waiting coroutine if one exists.
   * One-shot per consume cycle: a second `resolve()` before the prior
   * one is consumed (or `reset()`) trips an XCHECK. Terminates on
   * scheduler OOM (see SimTask.h, "Error model").
   */
  void resolve() noexcept {
    XCHECK(!resolved_)
        << "SimFuture<void>::resolve() called twice without intervening "
           "consume or reset() — without this XCHECK, the second call "
           "would silently be discarded";
    resolved_ = true;
    waiter_.notify();
  }

  [[nodiscard]] bool isResolved() const noexcept {
    return resolved_;
  }

  /**
   * Check if a coroutine is suspended awaiting resolution.
   *
   * @return True if a coroutine is suspended awaiting resolution (incl.
   *         across the deferred-resume window in ResumeMode::DEFERRED mode).
   */
  [[nodiscard]] bool hasWaiter() const noexcept {
    return waiter_.has();
  }

  /**
   * Reset for reuse. Must not be called while a waiter is suspended; call
   * cancel() first if needed.
   */
  void reset() noexcept {
    XCHECK(!waiter_.has())
        << "SimFuture<void>::reset() called with suspended waiter; "
           "coroutine frame would be orphaned";
    resolved_ = false;
  }

  /**
   * Tear down the suspended waiter (if any) without resuming it. Removes
   * any pending deferred resume from the PDES queue, then destroys the
   * suspended coroutine frame. Safe to call multiple times and safe when
   * no waiter is suspended (no-op).
   *
   * Does NOT clear `resolved_`. The two paths that revert resolved_ are
   * (a) consume via `co_await` (await_resume auto-clears), and
   * (b) explicit `reset()`. Call reset() after cancel() if you need a
   * clean slate without going through a consume.
   */
  void cancel() noexcept {
    waiter_.cancel();
  }

 private:
  detail::WaiterState waiter_;
  bool resolved_ = false;
};

/**
 * Value-carrying specialization. T is moved out on resume, leaving the
 * future in the unresolved state — a subsequent `co_await` on the same
 * future will suspend until the next `resolve()`.
 *
 * Requirements on T:
 *   - std::is_nothrow_move_constructible_v<T> — enforced via static_assert.
 *     Move-throwing types would let await_resume strand a moved-from value
 *     in the slot when its reset() step is skipped.
 */
template <typename T>
class SimFuture {
  // If T's move throws, await_resume would leave value_ populated with a
  // moved-from object and skip the value_.reset() — corrupting the slot.
  // Require nothrow-move so the consume path can't observe that state.
  static_assert(
      std::is_nothrow_move_constructible_v<T>,
      "SimFuture<T> requires T to be nothrow-move-constructible so "
      "await_resume's move-then-reset sequence can't strand a moved-from "
      "value in the slot.");

 public:
  /**
   * @param ctx     Simulation context (for deferred resume scheduling).
   * @param mode    ResumeMode::DIRECT (default) — resolve() resumes the
   *                coroutine inline. ResumeMode::DEFERRED — posts the
   *                resume via scheduleNow.
   *
   *                Note: SimQueue defaults to ResumeMode::DEFERRED instead
   *                — see SimQueue.h's STACK-DEPTH SAFETY section for the
   *                split rationale.
   */
  explicit SimFuture(
      folly::not_null<SimContext*> ctx,
      const ResumeMode mode = ResumeMode::DIRECT)
      : waiter_(ctx, mode) {}

  SimFuture(const SimFuture&) = delete;
  SimFuture& operator=(const SimFuture&) = delete;
  SimFuture(SimFuture&&) = delete;
  SimFuture& operator=(SimFuture&&) = delete;

  /**
   * Destructor. Always-on XCHECK catches the "SimFuture<T> destroyed
   * while a coroutine is still suspended on it" lifetime bug — the
   * suspended coroutine frame would leak otherwise.
   */
  ~SimFuture() noexcept {
    XCHECK(!waiter_.has()) << "SimFuture<T> destroyed with a suspended waiter; "
                              "coroutine frame leaked";
  }

  struct Awaitable {
    SimFuture& f;

    /** True if value already resolved — coroutine resumes immediately. */
    constexpr bool await_ready() const noexcept {
      return f.value_.has_value();
    }

    void await_suspend(std::coroutine_handle<> h) const noexcept {
      XCHECK(!f.waiter_.has())
          << "SimFuture<T> already has a suspended waiter; concurrent "
             "waiters are not supported";
      f.waiter_.install(h);
    }

    /**
     * Move the resolved value out and clear it, returning the future to the
     * unresolved state. A subsequent `co_await` will suspend until the next
     * `resolve()` (no need to call `reset()` between consume cycles).
     *
     * Unconditionally `noexcept`: the class-level static_assert already
     * requires `is_nothrow_move_constructible_v<T>`.
     *
     * @return The resolved value (moved from the slot).
     */
    T await_resume() noexcept {
      XDCHECK(f.value_.has_value());
      T item = std::move(*f.value_);
      f.value_.reset();
      return item;
    }
  };

  [[nodiscard(
      "SimFuture::operator co_await()'s return value must be co_await'd; "
      "discarding it creates an awaitable that never suspends or retrieves "
      "the value")]]
  Awaitable operator co_await() noexcept {
    return Awaitable{*this};
  }

  /**
   * Signal completion with a value. Resumes the waiting coroutine if one
   * exists. One-shot: a second `resolve()` before the prior value is
   * consumed (or before `reset()`) trips an XCHECK. Terminates on
   * scheduler OOM (see SimTask.h, "Error model").
   *
   * @param value    The value to deliver to the waiting consumer; moved
   *                 into the slot.
   */
  void resolve(T value) noexcept {
    XCHECK(!value_.has_value())
        << "SimFuture<T>::resolve() called twice without intervening consume "
           "or reset() — without this XCHECK, the second value would "
           "silently overwrite the first";
    value_ = std::move(value);
    waiter_.notify();
  }

  [[nodiscard]] bool isResolved() const noexcept {
    return value_.has_value();
  }

  /**
   * Check if a coroutine is suspended awaiting resolution.
   *
   * @return True if a coroutine is suspended awaiting resolution (incl.
   *         across the deferred-resume window in ResumeMode::DEFERRED mode).
   */
  [[nodiscard]] bool hasWaiter() const noexcept {
    return waiter_.has();
  }

  /**
   * Reset for reuse. Must not be called while a waiter is suspended; call
   * cancel() first if needed.
   */
  void reset() noexcept {
    XCHECK(!waiter_.has())
        << "SimFuture<T>::reset() called with suspended waiter; "
           "coroutine frame would be orphaned";
    value_.reset();
  }

  /**
   * Tear down the suspended waiter (if any) without resuming it. Semantics
   * match SimFuture<void>::cancel(); does NOT clear the slot. The two
   * paths that revert value_ are (a) consume via `co_await` (await_resume
   * auto-clears), and (b) explicit `reset()`. Call reset() after cancel()
   * if you need a clean slate without going through a consume.
   */
  void cancel() noexcept {
    waiter_.cancel();
  }

 private:
  detail::WaiterState waiter_;
  std::optional<T> value_;
};

} // namespace packsim::coro
