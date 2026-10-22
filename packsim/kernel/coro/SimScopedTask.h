/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>
#include <exception>
#include <utility>

namespace packsim::coro {

/**
 * Scoped-lifetime, externally-destroyable top-level coroutine.
 *
 * Like `SimTask`, `SimScopedTask` is a top-level (never `co_await`ed)
 * eager-start coroutine driven by the PDES (Parallel Discrete Event Simulation)
 * engine. Unlike `SimTask` — which is fire-and-forget, hides its handle, and
 * auto-destroys its frame on completion — `SimScopedTask` hands its
 * `coroutine_handle` to the owner, so the owner controls frame lifetime
 * (RAII: the task lives as long as the object that holds it) and can tear the
 * coroutine down deterministically at any suspension point.
 *
 * Use this for a long-running driver coroutine that an owning object must be
 * able to cancel at teardown (e.g. `LinkLayerScheduler`'s `txDriver`): the
 * owner holds the task as a member and `shutdown()` calls `reset()`, which
 * destroys the suspended frame. The frame unwind runs the destructors of the
 * awaitables the body is suspended on; the destroy-safe ones (see "Teardown
 * contract") cancel their installed waiters and scheduled PDES events.
 *
 * Naming note: "Scoped" here means the task's lifetime is scoped to its owner
 * (RAII), not a container of tasks — contrast `folly::coro::AsyncScope`, which
 * is a multi-task scope. The single owned move-only coroutine analog is
 * `folly::coro::Task` (lazy/awaited) ~ packsim's `SimResultTask`; folly has no
 * "held, externally-destroyed background task" because it cancels cooperatively
 * via `folly::CancellationToken` rather than by destroying frames. PackSim
 * destroys frames, which is safe only while the frame is parked on an awaitable
 * that cleans up after itself (see "Teardown contract").
 *
 * Example:
 *
 *     class LinkLayerScheduler {
 *       void start() { txDriver_ = txDriver(); }     // eager-start, store
 *       void shutdown() { txDriver_.reset(); }       // cancel mid-flight
 *       SimScopedTask txDriver();                     // the coroutine
 *       coro::SimScopedTask txDriver_;                // owned member
 *     };
 *
 * ### When to use this (scoped/RAII teardown) vs cooperative shutdown
 * Prefer this when (a) abandoning the coroutine at its current suspension point
 * is correct teardown — `reset()`'s `destroy()` unwinds the frame and does NOT
 * run the code between awaits — AND (b) the body only parks on destroy-safe
 * awaitables (listed under "Teardown contract"). If shutdown must RUN graceful
 * logic (flush / notify / persist), or the body awaits a non-destroy-safe
 * operation, use the cooperative idiom instead (a latched `shutdownRequested_`
 * operand of every race + `co_return` on `isLatched()`).
 *
 * ### Lifecycle
 *   - `initial_suspend = suspend_never` — eager start; the body runs to its
 *     first `co_await` (or to completion) on invocation, matching `SimTask` /
 *     `SimResultTask`.
 *   - `final_suspend = suspend_always` — the frame is NOT auto-destroyed on
 *     completion; it stays suspended (`done() == true`) so the owner remains
 *     the single owner of frame lifetime. `reset()` (or `~SimScopedTask`)
 *     destroys it. This is what makes the handle safe to hold and destroy
 *     without racing an auto-destroy.
 *
 * ### Teardown contract
 * `reset()` calls `std::coroutine_handle<>::destroy()`. This is well-defined
 * ONLY when:
 *   1. The coroutine is SUSPENDED (not currently executing on the stack).
 *      Never call `reset()` from inside the coroutine's own body.
 *   2. The awaitable it is suspended on cleans up after itself when the frame
 *      is destroyed. Destroy-safe:
 *        - `SimSignal::wait()`, `SimEvent::wait()` and `simScopedDelay()`.
 *        - `simWaitFirst()` / `collectAny()`, when every operand's
 *          `simWaitFirstTeardown` fully cancels it (all built-in operands do).
 *        - `SimStopwatch::expiration()` / `startOneShot()`, but the awaitable
 *          clears only the suspended handle and leaves any resume scheduled:
 *          after destroying the frame, `stop()` or `cancel()` the stopwatch
 *          before the stopwatch itself is destroyed. Do not `cancel()` it
 *          first: while the frame is parked on it, `cancel()` destroys that
 *          frame, so the later `reset()` would destroy it a second time.
 *        - `co_await` of a `SimResultTask` whose body is itself parked on one
 *          of these (the destroy cascades into its frame).
 *      NOT destroy-safe — a queued resume or an installed waiter keeps the
 *      freed frame's handle — so never park a resettable `SimScopedTask`, or a
 *      `SimResultTask` it awaits, directly on:
 *        - `simDelay()` / `simYield()` or `simScheduleOn()`.
 *        - `co_await` of a `SimFuture`, `SimQueue::pop()`, or a bare
 *          `SimCancellableDelay::awaitDelay()` (safe as a `simWaitFirst`
 *          operand).
 *        - `SimBarrier::arrive()` or `SimCoordRoutedBarrier::arrive()`.
 *
 * GOTCHA — `destroy()` does NOT run code between consecutive `co_await`s: it
 * unwinds the frame at the current suspension point and runs destructors of
 * locals scoped to that point, but NOT statements after the awaited
 * expression. Wrap any cancel-sensitive cleanup in a function-scope RAII guard.
 *
 * ### Error model
 * Fatal-only — an uncaught exception in the body calls `std::terminate()`
 * (matches `SimTask`). Recoverable errors must be handled in-coroutine.
 *
 * Move-only; default-constructs to an empty (no-frame) state so an owner can
 * hold it as a member and assign the started task in (e.g.) `start()`.
 *
 * Single SimContext only: the coroutine resumes on whichever context's PDES
 * worker dispatches its scheduled events. Unlike a `SimTask`, do not move a
 * `SimScopedTask` to another context: the owner calls `reset()` on its own
 * context, so the frame must stay parked there, and `simScheduleOn` is not
 * destroy-safe.
 */
class [[nodiscard(
    "SimScopedTask owns the coroutine frame; discarding it immediately destroys "
    "the just-started coroutine. Store it (e.g. in a member) to keep it alive")]]
SimScopedTask {
 public:
  struct promise_type;

 private:
  using Handle = std::coroutine_handle<promise_type>;

 public:
  struct promise_type {
    SimScopedTask get_return_object() noexcept {
      return SimScopedTask{Handle::from_promise(*this)};
    }

    /** @return suspend_never — eager start on invocation. */
    constexpr std::suspend_never initial_suspend() const noexcept {
      return {};
    }

    /**
     * @return suspend_always — keep the frame alive after completion so the
     * owner controls destruction (no auto-destroy, unlike `SimTask`).
     */
    constexpr std::suspend_always final_suspend() const noexcept {
      return {};
    }

    constexpr void return_void() const noexcept {}

    /** Fatal-only error policy — see "Error model" in the class docstring. */
    [[noreturn]] void unhandled_exception() const noexcept {
      std::terminate();
    }
  };

  /** Construct an empty task that owns no frame. */
  SimScopedTask() noexcept = default;

  SimScopedTask(const SimScopedTask&) = delete;
  SimScopedTask& operator=(const SimScopedTask&) = delete;

  SimScopedTask(SimScopedTask&& other) noexcept
      : handle_(std::exchange(other.handle_, {})) {}

  SimScopedTask& operator=(SimScopedTask&& other) noexcept {
    if (this != &other) {
      reset();
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }

  ~SimScopedTask() {
    reset();
  }

  /**
   * Destroy the owned frame (if any) and become empty. Idempotent. MUST be
   * called only while the coroutine is suspended (see "Teardown contract").
   */
  void reset() noexcept {
    if (handle_) {
      // Clear the handle BEFORE destroy() so any reentrant observation of
      // *this (e.g. a local destructor in the unwound frame that touches the
      // owner) sees an empty, quiescent task.
      const auto h = handle_;
      handle_ = {};
      h.destroy();
    }
  }

  /** True iff this owns a frame (started and not yet reset / moved-from). */
  [[nodiscard]] bool valid() const noexcept {
    return static_cast<bool>(handle_);
  }

  /** True iff the owned coroutine has run to completion (at final_suspend). */
  [[nodiscard]] bool done() const noexcept {
    return handle_ && handle_.done();
  }

 private:
  explicit SimScopedTask(Handle h) noexcept : handle_(h) {}
  friend struct promise_type;

  Handle handle_{};
};

} // namespace packsim::coro
