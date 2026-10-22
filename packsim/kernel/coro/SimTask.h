/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>
#include <exception>
#include <memory>
#include <type_traits>
#include <utility>

#include <folly/logging/xlog.h>

namespace packsim::coro {

/**
 * Fire-and-forget coroutine return type for simulation tasks.
 *
 * A SimTask coroutine starts eagerly (runs until first co_await) and
 * auto-destroys its frame on completion. The coroutine is resumed by the
 * PDES (Parallel Discrete Event Simulation) engine via scheduled events,
 * preserving context affinity.
 *
 * SimTask itself is an empty struct — the coroutine frame is owned by the
 * suspended handle, not by this object.
 *
 * Example:
 *
 *     SimTask transmitLoop(SimContext* ctx) {
 *       while (auto pkt = dequeue()) {
 *         co_await simDelay(ctx, computeTxTime(pkt));
 *         onTxComplete(pkt);
 *       }
 *     }
 *
 * NOTE: A SimTask runs on one SimContext at a time — the coroutine frame
 * resumes on whichever context's PDES worker thread dispatches its scheduled
 * events. Move it to another context only by awaiting `simScheduleOn`; the
 * per-context primitives it awaits must belong to its current context.
 *
 * Lambda-coroutine lifetime: when defining a SimTask coroutine as a
 * lambda that captures by reference, the captured objects MUST outlive
 * the coroutine frame. The coroutine resumes asynchronously from PDES;
 * if the lambda object goes out of scope before the final resume, the
 * captured references dangle = UB. Either store the captured objects in
 * an enclosing scope that outlives the scheduler.run() driving the task,
 * or capture by value / move ownership into the coroutine.
 *
 * Error model. This block is the framework-wide exception policy; the other
 * coro primitives cite it by name.
 *  - Framework-internal failures (allocation while scheduling an event, or
 *    growing a waiter container) are noexcept and call std::terminate() at
 *    the failing site rather than unwinding out of a partially-suspended
 *    coroutine. Operational paths do allocate (event-queue entries, waiter
 *    containers, the child frames `simWaitFirst` spawns), but an allocation
 *    failure never propagates as an exception.
 *  - User-callback failures are fatal-only: an uncaught exception in a
 *    coroutine body reaches promise_type::unhandled_exception(), which calls
 *    std::terminate(). Any coroutine type composed with WaiterState must
 *    adopt this. Recoverable errors must be handled in-coroutine via
 *    try/catch + co_return.
 */
struct SimTask {
  /**
   * C++20 coroutine promise type. The methods below implement the
   * coroutine-protocol hooks the compiler synthesizes calls to.
   */
  struct promise_type {
    /** @return The SimTask handed back to the caller of the coroutine. */
    constexpr SimTask get_return_object() const noexcept {
      return {};
    }

    /** @return suspend_never — coroutine starts eagerly on invocation. */
    constexpr std::suspend_never initial_suspend() const noexcept {
      return {};
    }

    /** @return suspend_never — coroutine frame auto-destroys on completion. */
    constexpr std::suspend_never final_suspend() const noexcept {
      return {};
    }

    /** No-op: SimTask coroutines have no return channel. */
    constexpr void return_void() const noexcept {}

    /**
     * Called by the C++20 ABI when an exception escapes the coroutine
     * body. SimTask is fatal-only: the body MUST be throw-free, so any
     * escaping exception terminates the process loudly rather than
     * silently swallowing it (no return channel for errors) or leaking
     * the coroutine frame (the ABI bypasses final_suspend's auto-destroy
     * when unhandled_exception throws).
     */
    [[noreturn]] void unhandled_exception() const noexcept {
      std::terminate();
    }
  };
};

// Empty-struct invariant: any non-empty member would imply a stored
// coroutine_handle (or similar) that breaks the eager-start / auto-destroy
// lifetime contract. is_empty_v also catches a future [[no_unique_address]]
// member that would pass sizeof == 1.
static_assert(std::is_empty_v<SimTask>);
static_assert(std::is_trivially_copyable_v<SimTask>);
static_assert(std::is_trivially_destructible_v<SimTask>);

// ============================================================================
// SimResultTask<T> — value-returning coroutine, single-await.
// ============================================================================

/**
 * Value-returning coroutine return type. Inspired by `folly::coro::Task<T>`
 * but specialized for PackSim's PDES single-context model.
 *
 *
 * ### Differences vs `SimTask`
 *   - Frame survives past completion (final_suspend = suspend_always-style)
 *     so the awaiter can extract the returned value.
 *   - SimResultTask<T> owns the coroutine_handle and destroys it in its
 *     destructor; therefore SimResultTask<T> is move-only (not empty).
 *   - Body still starts eagerly (initial_suspend = suspend_never), matching
 *     SimTask. If the body completes synchronously (no internal co_await),
 *     the returned task has the value already stored and `co_await
 *     std::move(task)` reads it without suspending the caller.
 *
 * ### Why eager start?
 * SimResultTask is intended for short-lived per-state helper coroutines
 * driven by an outer dispatcher (see `link-layer/llr/llr.cpp`'s
 * `txStateMachine` for the canonical use). The dispatcher does `co_await
 * runFoo()`, so the body needs to begin executing as soon as the helper is
 * invoked — the async work it kicks off (sending a frame, arming a timer)
 * should hit the PDES queue immediately, not defer to the moment of `co_await`.
 * Lazy start (folly::coro::Task's choice) would require a separate "kick" step
 * which doesn't fit the simulation runtime.
 *
 * ### Single-await contract
 * A SimResultTask<T> can be `co_await`'d at most once. The value is moved
 * out in `await_resume`. After consumption, awaiting again is undefined
 * behavior. The type enforces this at compile time via `operator co_await()
 * &&` (rvalue-only) + move-only ownership.
 *
 *     SimResultTask<int> producer() {
 *       co_await simDelay(ctx, Time::fromNs(100));
 *       co_return 42;
 *     }
 *
 *     SimTask consumer() {
 *       int v = co_await producer();   // moves out the int
 *       use(v);
 *     }
 *
 * ### Symmetric-transfer resume
 * When the producer body finishes, its `final_suspend` returns an awaiter
 * whose `await_suspend` returns the parent's `coroutine_handle`, transferring
 * control directly to the parent without going through the scheduler. This
 * matches `folly::coro::Task<T>` behavior and is HALO-friendly.
 *
 * ### Single-thread / single-SimContext only
 * Unlike a `SimTask` (which may migrate with `simScheduleOn`), producer and
 * consumer must run on the same SimContext, and that context is
 * single-threaded by PDES design. The promise's `continuation_` slot is a
 * plain `std::coroutine_handle`, NOT atomic — cross-thread setup of
 * `continuation_` (await_suspend on thread B while the body completes
 * on thread A) would race and could deadlock the parent. This is safe
 * in PackSim because every SimResultTask invocation runs end-to-end on
 * its owning SimContext's worker thread; do NOT use SimResultTask in
 * code paths that can hop threads mid-await.
 *
 * ### Eager-start drop hazard
 * Because the body starts running on invocation, dropping a SimResultTask
 * whose body is suspended on an internal await is dangerous: `~SimResultTask`
 * calls `handle.destroy()` which destroys the frame, but the awaitable the
 * body is suspended on may hold references into that frame (e.g., a
 * registered waiter slot). Destruction is well-defined only while the body
 * is parked on a destroy-safe awaitable (see "Teardown contract" in
 * SimScopedTask.h), and the type itself can't enforce this.
 * Defenses:
 *   - `[[nodiscard]]` on the class catches the most common slip
 *     (`producer();` instead of `co_await producer();`).
 *   - The destructor `XDCHECK`s that the frame is `done()` OR was actually
 *     `co_await`'d (its `continuation_` is set) — surfacing in debug builds a
 *     drop of a still-running task that nobody is awaiting, while permitting
 *     the legitimate cascade case where an awaiting parent frame is torn down
 *     (its destruction destroys this still-running child via the awaited
 *     temporary).
 *   - In production code, a SimResultTask should always be `co_await`'d.
 *
 * ### Coroutine-parameter dangling
 * Same hazard as any C++20 coroutine: by-reference parameters are stored
 * in the frame, but the frame outlives the call site. If a SimResultTask
 * helper is invoked with a temporary or stack-local reference and the body
 * suspends, the reference dangles when the body resumes. Prefer pass-by-
 * value or `not_null<T*>` for inputs that need to survive a suspension.
 *
 * ### Error model
 * Fatal-only — uncaught exception in the coroutine body calls
 * `std::terminate()` (matches SimTask). Recoverable errors must be in-
 * coroutine try/catch + `co_return folly::Expected<T, E>{...}` or similar
 * (`std::expected` needs C++23). `await_resume` is `noexcept` and moves out
 * the value, so T's move constructor must also be nothrow (enforced via
 * static_assert below) — otherwise a moving exception would terminate inside
 * `await_resume`.
 */
template <typename T>
class [[nodiscard(
    "SimResultTask must be co_await'd; dropping it discards the body's "
    "return value (the body has already executed any side effects)")]]
SimResultTask {
  static_assert(!std::is_reference_v<T>, "T must not be a reference type");
  static_assert(
      std::is_nothrow_move_constructible_v<T>,
      "T must be nothrow move-constructible: Awaiter::await_resume is "
      "noexcept and moves the value out, so a throwing move would call "
      "std::terminate inside await_resume");
  static_assert(
      std::is_nothrow_destructible_v<T>,
      "T must be nothrow destructible: ~T() runs inside Awaiter::await_resume "
      "(noexcept), so a throwing destructor would call std::terminate.");

 public:
  struct promise_type;

 private:
  using Handle = std::coroutine_handle<promise_type>;

 public:
  struct promise_type {
    // Storage for the return value; constructed by `return_value`, destroyed
    // by `~promise_type` if still held, or by `Awaiter::await_resume` when
    // the value is moved out. Intentionally uninitialized at promise
    // construction — engagement is tracked by hasValue_; never read value_
    // without checking hasValue_ first.
    union {
      T value_;
    };
    bool hasValue_ = false;

    // Continuation of the awaiting coroutine. Set by
    // `Awaiter::await_suspend`; resumed by `FinalAwaiter::await_suspend`
    // via symmetric transfer. (folly::coro::Task and cppcoro call this the
    // "continuation"; same concept.)
    std::coroutine_handle<> continuation_{};

    promise_type() noexcept {}

    ~promise_type() noexcept {
      if (hasValue_) {
        value_.~T();
      }
    }

    promise_type(const promise_type&) = delete;
    promise_type& operator=(const promise_type&) = delete;
    promise_type(promise_type&&) = delete;
    promise_type& operator=(promise_type&&) = delete;

    SimResultTask get_return_object() noexcept {
      return SimResultTask{Handle::from_promise(*this)};
    }

    /** @return suspend_never — coroutine starts eagerly on invocation. */
    constexpr std::suspend_never initial_suspend() const noexcept {
      return {};
    }

    /**
     * Final-suspend awaiter: holds the frame alive (so the caller can read
     * `value_`) AND symmetrically transfers control to the continuation if
     * one is attached.
     */
    struct FinalAwaiter {
      bool await_ready() const noexcept {
        return false;
      }

      std::coroutine_handle<> await_suspend(Handle h) noexcept {
        auto c = h.promise().continuation_;
        return c ? c : std::noop_coroutine();
      }

      void await_resume() const noexcept {}
    };
    constexpr FinalAwaiter final_suspend() const noexcept {
      return {};
    }

    template <typename U = T>
      requires std::is_constructible_v<T, U&&>
    void return_value(U&& v) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
      std::construct_at(std::addressof(value_), std::forward<U>(v));
      hasValue_ = true;
    }

    // Fatal-only error policy — see "Error model" in the class docstring.
    [[noreturn]] void unhandled_exception() const noexcept {
      std::terminate();
    }
  };

 private:
  // Move-only handle ownership. Constructed only by
  // promise_type::get_return_object.
  explicit SimResultTask(Handle h) noexcept : handle_(h) {}
  friend struct promise_type;

 public:
  SimResultTask(const SimResultTask&) = delete;
  SimResultTask& operator=(const SimResultTask&) = delete;

  SimResultTask(SimResultTask&& other) noexcept
      : handle_(std::exchange(other.handle_, nullptr)) {}

  SimResultTask& operator=(SimResultTask&& other) noexcept {
    if (this != &other) {
      if (handle_) {
        // Same eager-start drop hazard as in the destructor: assigning
        // over a still-running task destroys its frame mid-await.
        XDCHECK(handle_.done())
            << "SimResultTask move-assigned over a still-running task — "
               "the body's awaitables would be destroyed mid-suspend";
        handle_.destroy();
      }
      handle_ = std::exchange(other.handle_, nullptr);
    }
    return *this;
  }

  ~SimResultTask() noexcept {
    if (handle_) {
      // A still-running body at destruction is well-defined ONLY as part of a
      // parent's cascade teardown: the task was co_await'd (its continuation is
      // set) and the awaiting parent frame is being destroyed (e.g. via
      // SimScopedTask::reset()), which destroys this frame too. The cascade is
      // safe only if this body is parked on a destroy-safe awaitable (see
      // "Teardown contract" in SimScopedTask.h).
      // A still-running body with NO continuation means the task was dropped
      // without ever being co_await'd — that IS the eager-start bug this guards
      // (the dropped body may leave awaitables dangling). Surface it in debug
      // builds at the dropping site rather than at a later use-after-free.
      XDCHECK(handle_.done() || handle_.promise().continuation_)
          << "SimResultTask dropped while running and never co_await'd; the "
             "body's awaitable may hold dangling references into the destroyed "
             "frame. Always co_await the SimResultTask.";
      handle_.destroy();
    }
  }

  /**
   * `co_await std::move(task)` — single-use awaiter. SimResultTask<T> is
   * move-only to enforce single-await; awaiting a moved-from task is
   * undefined behavior.
   */
  auto operator co_await() && noexcept {
    return Awaiter{handle_};
  }

 private:
  // Awaiter — single-use. `co_await std::move(task)` triggers this via
  // `operator co_await() &&` above. If the task body has already
  // completed synchronously (eager start + no internal co_await),
  // `await_ready` is true and the parent reads the stored value without
  // suspending. Otherwise the parent suspends, the body's `final_suspend`
  // symmetric-transfers to the parent on completion, and `await_resume`
  // moves out the value.
  struct Awaiter {
    Handle h;

    bool await_ready() const noexcept {
      return h.done();
    }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<> continuation) noexcept {
      h.promise().continuation_ = continuation;
      // Eager-start invariant: by the time we reach here, the body has
      // already executed up to its first internal co_await and is
      // suspended there (await_ready() returned false, so the body is
      // not done). FinalAwaiter will resume `continuation` via
      // symmetric transfer when the body completes its final_suspend.
      // Suspend the continuation here by returning noop.
      return std::noop_coroutine();
    }

    T await_resume() noexcept {
      // Structural invariant: by the C++20 coroutine ABI, await_resume
      // runs only after final_suspend, which runs only after
      // return_value (sets hasValue_=true) or unhandled_exception
      // (terminates). No additional check needed here.
      auto& p = h.promise();
      T result = std::move(p.value_);
      p.value_.~T();
      p.hasValue_ = false;
      return result;
    }
  };

  Handle handle_{};
};

// ============================================================================
// SimResultTask<void> — value-less awaitable variant.
// ============================================================================

/**
 * Value-less specialization of SimResultTask. Same eager-start, single-await,
 * symmetric-transfer semantics as SimResultTask<T>, but the body produces no
 * value — a parent that needs to wait for completion (without a return value)
 * can `co_await` the task directly.
 *
 * ### Why a separate type
 *
 * The C++ coroutine spec requires the promise type to define exactly one of
 * `return_value` or `return_void` — never both. Plus the storage shape
 * differs: there is no `union { void value_; }`. A partial specialization is
 * the cleanest way to express both differences without conditionally-enabled
 * members and `if constexpr` branches in every awaiter method.
 *
 * ### When to use this vs SimTask
 *
 *   - **SimTask** — fire-and-forget. NOT awaitable. Use for top-level loops
 *     and free-running tasks where no caller waits on completion.
 *   - **SimResultTask<void>** — awaitable, single-use. Use for helper
 *     coroutines that the parent needs to `co_await` for completion but
 *     where there is no meaningful return value.
 *
 * Example — replacing a SimSignal-coupled "wait for completion" pattern:
 *
 *     // Before — SimTask + SimSignal completion wiring
 *     SimTask doWork(SimSignal* done) {
 *       co_await runStep1();
 *       co_await runStep2();
 *       done->signal();
 *     }
 *     SimSignal sig{ctx};
 *     doWork(&sig);
 *     co_await sig.wait();
 *
 *     // After — SimResultTask<void>
 *     SimResultTask<void> doWork() {
 *       co_await runStep1();
 *       co_await runStep2();
 *     }
 *     co_await doWork();
 *
 * All other contracts from SimResultTask<T> apply unchanged: eager start,
 * symmetric-transfer resume on completion, single-await, fatal-only error
 * model, single-thread / single-SimContext only, and the eager-start drop
 * hazard (debug-only XDCHECK on a still-running frame).
 */
template <>
class [[nodiscard(
    "SimResultTask<void> must be co_await'd; dropping it abandons the body's "
    "completion (the body has already executed any side effects) — to "
    "spawn fire-and-forget, use SimTask instead")]] SimResultTask<void> {
 public:
  struct promise_type;

 private:
  using Handle = std::coroutine_handle<promise_type>;

 public:
  struct promise_type {
    // Continuation of the awaiting coroutine. Set by
    // `Awaiter::await_suspend`; resumed by `FinalAwaiter::await_suspend`
    // via symmetric transfer. Matches the `SimResultTask<T>` promise.
    std::coroutine_handle<> continuation_{};

    // The coroutine machinery never copies or moves the promise, but we
    // delete the operations to match `SimResultTask<T>::promise_type`'s
    // defensive contract and prevent accidental misuse.
    promise_type() noexcept = default;
    promise_type(const promise_type&) = delete;
    promise_type& operator=(const promise_type&) = delete;
    promise_type(promise_type&&) = delete;
    promise_type& operator=(promise_type&&) = delete;
    ~promise_type() = default;

    SimResultTask get_return_object() noexcept {
      return SimResultTask{Handle::from_promise(*this)};
    }

    /** @return suspend_never — coroutine starts eagerly on invocation. */
    constexpr std::suspend_never initial_suspend() const noexcept {
      return {};
    }

    /**
     * Final-suspend awaiter: holds the frame alive (so the awaiter knows
     * the body finished) AND symmetrically transfers control to the
     * continuation if one is attached. Same shape as
     * `SimResultTask<T>::FinalAwaiter`, just without the value-storage
     * interaction.
     */
    struct FinalAwaiter {
      bool await_ready() const noexcept {
        return false;
      }

      std::coroutine_handle<> await_suspend(Handle h) noexcept {
        auto c = h.promise().continuation_;
        return c ? c : std::noop_coroutine();
      }

      void await_resume() const noexcept {}
    };
    constexpr FinalAwaiter final_suspend() const noexcept {
      return {};
    }

    constexpr void return_void() const noexcept {}

    // Fatal-only error policy — see "Error model" in SimResultTask<T>'s
    // class docstring; the void specialization adopts the same contract.
    [[noreturn]] void unhandled_exception() const noexcept {
      std::terminate();
    }
  };

 private:
  // Move-only handle ownership. Constructed only by
  // promise_type::get_return_object.
  explicit SimResultTask(Handle h) noexcept : handle_(h) {}
  friend struct promise_type;

 public:
  SimResultTask(const SimResultTask&) = delete;
  SimResultTask& operator=(const SimResultTask&) = delete;

  SimResultTask(SimResultTask&& other) noexcept
      : handle_(std::exchange(other.handle_, nullptr)) {}

  SimResultTask& operator=(SimResultTask&& other) noexcept {
    if (this != &other) {
      if (handle_) {
        // Same eager-start drop hazard as in the destructor: assigning
        // over a still-running task destroys its frame mid-await. Match
        // SimResultTask<T>'s move-assignment guard.
        XDCHECK(handle_.done())
            << "SimResultTask<void> move-assigned over a still-running "
               "task — the body's awaitables would be destroyed mid-suspend";
        handle_.destroy();
      }
      handle_ = std::exchange(other.handle_, nullptr);
    }
    return *this;
  }

  ~SimResultTask() noexcept {
    if (handle_) {
      // A still-running body at destruction is well-defined ONLY as part of a
      // parent's cascade teardown: the task was co_await'd (its continuation is
      // set) and the awaiting parent frame is being destroyed (e.g. via
      // SimScopedTask::reset()), which destroys this frame too. The cascade is
      // safe only if this body is parked on a destroy-safe awaitable (see
      // "Teardown contract" in SimScopedTask.h).
      // A still-running body with NO continuation means the task was dropped
      // without ever being co_await'd — the eager-start bug this guards.
      XDCHECK(handle_.done() || handle_.promise().continuation_)
          << "SimResultTask<void> dropped while running and never co_await'd; "
             "the body's awaitable may hold dangling references into the "
             "destroyed frame. Always co_await the SimResultTask.";
      handle_.destroy();
    }
  }

  /**
   * `co_await std::move(task)` — single-use awaiter. SimResultTask<void>
   * is move-only to enforce single-await; awaiting a moved-from task is
   * undefined behavior.
   */
  auto operator co_await() && noexcept {
    return Awaiter{handle_};
  }

 private:
  // Awaiter — single-use. `co_await std::move(task)` triggers this via
  // `operator co_await() &&` above. If the task body has already
  // completed synchronously (eager start + no internal co_await),
  // `await_ready` is true and the parent resumes without suspending.
  // Otherwise the parent suspends, the body's `final_suspend`
  // symmetric-transfers to the parent on completion, and
  // `await_resume` is a no-op.
  struct Awaiter {
    Handle h;

    bool await_ready() const noexcept {
      return h.done();
    }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<> continuation) noexcept {
      h.promise().continuation_ = continuation;
      return std::noop_coroutine();
    }

    void await_resume() const noexcept {}
  };

  Handle handle_{};
};

} // namespace packsim::coro
