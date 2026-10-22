/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>

#include <folly/CPortability.h>
#include <folly/Function.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/coro/SimCancellableDelay.h"
#include "packsim/kernel/coro/SimEvent.h"
#include "packsim/kernel/coro/SimSignal.h"
#include "packsim/kernel/coro/SimStopwatch.h"
#include "packsim/kernel/coro/SimTask.h"

namespace packsim::coro {

namespace detail {

// Built-in cancellation adapters for the SimCoro primitives. Each
// adapter forwards to the operand's own `cancel()` method — the
// adapter layer exists so user-defined operand types (which may not
// have a `cancel()` method) can opt in by defining
// `simWaitFirstCancel(MyAwaitable&)` in their own namespace; ADL
// finds it during template instantiation.
//
// Defined in `packsim::coro::detail`; the unqualified call inside
// `armOne`'s canceller lambda uses `using detail::simWaitFirstCancel;`
// so these names are visible alongside any user-defined adapters.
//
// (The using-declaration in armOne's lambda body is the only header-
// scope `using` in this file; it's necessary to enable ADL on user-
// defined operand types — qualified `detail::simWaitFirstCancel(*opPtr)`
// would suppress ADL and only ever find the built-ins below.)

FOLLY_ALWAYS_INLINE void simWaitFirstCancel(
    SimCancellableDelay::Awaitable& aw) noexcept {
  aw.cancel();
}

FOLLY_ALWAYS_INLINE void simWaitFirstCancel(SimSignal::Awaitable& aw) noexcept {
  aw.cancel();
}

FOLLY_ALWAYS_INLINE void simWaitFirstCancel(
    SimStopwatch::Awaitable& aw) noexcept {
  aw.cancel();
}

FOLLY_ALWAYS_INLINE void simWaitFirstCancel(SimEvent::Awaitable& aw) noexcept {
  aw.cancel();
}

// Teardown-cancel adapters — used by `~SimWaitFirstAwaitable` when the race is
// destroyed UNRESOLVED (the owning coroutine frame is being torn down, e.g. via
// `SimScopedTask::reset()`). Distinct from `simWaitFirstCancel` (the resolve-
// path loser-cancel): teardown must FULLY cancel every operand so no installed
// waiter and no scheduled PDES event survives the frame's destruction.
//
// For `SimCancellableDelay` / `SimSignal` / `SimEvent` the loser-cancel IS
// already a full teardown (it cancels any scheduled resume and destroys the
// waiter frame), so the generic fallback simply forwards to
// `simWaitFirstCancel`. `SimStopwatch` is the sole exception: its loser-cancel
// (`cancelWaiterOnly`) deliberately PRESERVES the scheduled resume so a losing
// timer keeps counting for the next race iteration — at teardown that would
// leave `scheduledResume_` armed and trip `~SimStopwatch`'s XCHECK, so the
// override does a full `cancel()` instead.
template <typename Aw>
void simWaitFirstTeardown(Aw& aw) noexcept {
  simWaitFirstCancel(aw);
}

FOLLY_ALWAYS_INLINE void simWaitFirstTeardown(
    SimStopwatch::Awaitable& aw) noexcept {
  // Full cancel: clears the scheduled resume AND destroys the waiter frame,
  // unlike `cancelWaiterOnly()` (the loser-cancel) which keeps the timer live.
  aw.self->cancel();
}

// State machine for one `simWaitFirst` race. Replaces three flag
// booleans (resolved, armingInProgress, parentResumed) with explicit
// named states so transitions are auditable at a glance and impossible
// combinations (e.g., parentResumed=true while armingInProgress=true)
// are unrepresentable.
//
// Legal transitions:
//
//   INITIAL ──await_suspend()──→ ARMING
//   ARMING ──armAll completes, no resolve──→ ARMED
//   ARMING ──resolve() during arming──→ ARMING_RESOLVED
//   ARMING_RESOLVED ──await_suspend post-loop resume──→ PARENT_RESUMED
//   ARMED ──resolve() after arming──→ PARENT_RESUMED
//
// Once in PARENT_RESUMED, *this may have been destructed during the
// resume — no further access is allowed.
enum class WaitFirstRaceState : uint8_t {
  // Sentinel: SimWaitFirstAwaitable was constructed but await_suspend
  // has not been called yet. Never observed by the implementation
  // (await_suspend transitions to ARMING as its first action) — kept
  // explicit so destructor traces / debugger watches can distinguish
  // "constructed but never awaited" from any post-await state.
  INITIAL,
  // Inside await_suspend's armAll loop; no winner has been recorded.
  ARMING,
  // A child coroutine resolved during armAll. Parent resume is deferred
  // to await_suspend's post-loop check (resuming inside armAll would let
  // the parent destruct *this mid-loop).
  ARMING_RESOLVED,
  // armAll is complete; no winner has been recorded yet. Waiting for a
  // child coroutine to call resolve().
  ARMED,
  // parentHandle.resume() has been called. *this may have been
  // destructed during the resume; no further access allowed.
  PARENT_RESUMED,
};

// Shared race state. Lives in the SimWaitFirstAwaitable, which lives in
// the parent's coroutine frame (held alive until co_await resolves).
template <std::size_t N>
struct WaitFirstState {
  WaitFirstRaceState state{WaitFirstRaceState::INITIAL};
  std::coroutine_handle<> parentHandle{};
  std::size_t resolvedIdx{0};

  // Cancellers for each operand. Set during await_suspend before any
  // child coroutine could complete. Called for losers by resolve().
  //
  // Stored as folly::Function with a `noexcept` signature so each
  // captures a reference to its operand's primitive without leaking
  // the operand type into WaitFirstState — and so the type system
  // enforces that captured callables don't throw (std::function has
  // no noexcept signature support pre-C++26 and would silently
  // std::terminate via the surrounding noexcept).
  //
  // folly::Function's small-buffer optimization stores the captured
  // single pointer (`opPtr = &op`) inline, so the cancellers add no heap
  // allocation of their own; the race still allocates up to one child
  // coroutine frame per operand.
  std::array<folly::Function<void() noexcept>, N> cancellers{};

  void resolve(std::size_t idx) noexcept {
    // Re-entrant guard: any post-resolve state means a winner is
    // already recorded. A loser's PDES event raced through before its
    // canceller ran — drop quietly; the winning resume already woke
    // (or will wake) the parent.
    if (state == WaitFirstRaceState::ARMING_RESOLVED ||
        state == WaitFirstRaceState::PARENT_RESUMED) {
      return;
    }
    // state must be ARMING or ARMED here.
    resolvedIdx = idx;

    // Cancel losers BEFORE resuming the parent. Two reasons:
    //   1. If we resumed parent first, parent could destruct the
    //      SimWaitFirstAwaitable (and this state) before we got to cancel
    //      losers — leaving cancellers[] dangling.
    //   2. Synchronous cancel guarantees losers' PDES events are removed
    //      and loser child frames are destroyed before the parent runs;
    //      no chance of a stale resume firing into a destroyed state.
    for (std::size_t i = 0; i < N; ++i) {
      if (i != idx && cancellers[i]) {
        cancellers[i]();
      }
    }

    if (state == WaitFirstRaceState::ARMING) {
      // We're in the middle of await_suspend's arming loop. Resuming the
      // parent here would let it destruct *this before the loop finishes.
      // Defer to await_suspend, which detects ARMING_RESOLVED post-loop
      // and resumes then.
      state = WaitFirstRaceState::ARMING_RESOLVED;
      return;
    }

    // state == ARMED: armAll has completed; resume now.
    state = WaitFirstRaceState::PARENT_RESUMED;
    // resume() may recursively run the parent's body all the way to the
    // next suspension or completion. The parent may destruct this state
    // during that run. MUST be the last statement in resolve() — no
    // post-resume access to *this allowed.
    parentHandle.resume();
  }
};

// Child coroutine: awaits one operand, then resolves the race.
//
// Body is intentionally minimal: any code added here that runs after
// resolve() executes after the parent has potentially destructed the
// state (and this child's frame). Keep resolve() the last statement
// before co_return.
//
// Operand is taken by reference because it lives in the parent's
// awaitable tuple (stable address until parent resumes). Reference avoids
// move-out costs on every operand.
template <std::size_t Idx, std::size_t N, typename Op>
SimTask waitFirstChild(
    folly::not_null<WaitFirstState<N>*> state,
    folly::not_null<Op*> op) {
  // A named reference, not `co_await *op`: GCC 11.3, 12.1 and 12.2 (GCC PR
  // c++/105406) copy an awaitable reached through `*op`, and this one is
  // move-only.
  Op& operand = *op;
  co_await operand;
  state->resolve(Idx);
}

} // namespace detail

/**
 * Implementation of the `simWaitFirst` race primitive. Constructed via
 * the `simWaitFirst()` factory function below — users should not
 * instantiate this class directly. See `simWaitFirst` for the
 * user-facing semantics, examples, operand requirements, cancellation
 * contract, tie-breaking rules, and lifetime guarantees.
 *
 * Implementation outline: the awaitable holds the operands by value
 * (moved into a tuple); on `await_suspend`, it spawns one child
 * `SimTask` per operand (each `co_await`s its operand and calls a
 * shared resolver). The first child to resolve cancels the others via
 * the ADL `simWaitFirstCancel` hook and resumes the parent. State is
 * coordinated via the `WaitFirstRaceState` enum (see above).
 */
template <typename... Ops>
class SimWaitFirstAwaitable {
 public:
  // Upper bound on operand count. Picked at the size where the
  // per-instance cancellers array (`folly::Function<void() noexcept>`)
  // starts to dwarf the parent coroutine frame; beyond this, the
  // operand set is probably modeling something better expressed as a
  // SimQueue or a dedicated dispatcher.
  static constexpr std::size_t kMaxOperands = 16;

  static_assert(
      sizeof...(Ops) >= 2,
      "simWaitFirst requires at least 2 operands");
  static_assert(
      sizeof...(Ops) <= kMaxOperands,
      "simWaitFirst with >kMaxOperands operands suggests redesign — "
      "consider SimQueue or a dedicated dispatcher for many-operand "
      "fan-in patterns");
  static_assert(
      (std::is_move_constructible_v<Ops> && ...),
      "simWaitFirst operands must be move-constructible (the awaitable "
      "holds them in a std::tuple). Awaitables with deleted move "
      "constructors (e.g., SimBarrier::Awaitable, "
      "SimCoordRoutedBarrier::Awaitable) cannot currently be raced. "
      "Workaround: wrap them in a SimTask that broadcasts a SimSignal, "
      "then race the signal.");
  static_assert(
      (std::is_nothrow_move_constructible_v<Ops> && ...),
      "simWaitFirst operands must be noexcept-move-constructible (the "
      "awaitable's constructor is noexcept; a throwing move would call "
      "std::terminate).");

  /**
   * Construct the awaitable from the operand pack. Operands are moved
   * into the internal tuple at construction; their addresses inside the
   * tuple are stable for the awaitable's lifetime (which spans the
   * `co_await` expression in the parent coroutine's frame).
   *
   * @param ops  The operand awaitables to race. 2..kMaxOperands.
   */
  SimWaitFirstAwaitable(Ops... ops) noexcept : ops_(std::move(ops)...) {}

  // Non-copyable, non-movable: stores operands and child-coroutine state
  // at fixed addresses (referenced by spawned child coroutines).
  SimWaitFirstAwaitable(const SimWaitFirstAwaitable&) = delete;
  SimWaitFirstAwaitable& operator=(const SimWaitFirstAwaitable&) = delete;
  SimWaitFirstAwaitable(SimWaitFirstAwaitable&&) = delete;
  SimWaitFirstAwaitable& operator=(SimWaitFirstAwaitable&&) = delete;

  /**
   * Destroy-time teardown. If the race is destroyed UNRESOLVED — i.e. the
   * owning coroutine frame is being torn down while still suspended in the
   * `co_await` (e.g. `SimScopedTask::reset()`) — every still-armed operand is
   * fully cancelled so its child coroutine frame is destroyed and no installed
   * waiter / scheduled PDES event survives into the operands' own destructors.
   *
   * A NORMALLY-resolved race is in `PARENT_RESUMED` by the time this runs (the
   * winner self-completed and `resolve()` already loser-cancelled the rest), so
   * the guard skips teardown — a single enum compare on the hot path.
   * Teardown runs whenever the race is destroyed unresolved (armed, or never
   * awaited), and leaves nothing behind only if every operand's
   * `simWaitFirstTeardown` fully cancels it, as the built-in adapters do.
   */
  ~SimWaitFirstAwaitable() {
    if (state_.state != detail::WaitFirstRaceState::PARENT_RESUMED) {
      teardownAll(std::index_sequence_for<Ops...>{});
    }
  }

  /**
   * Always returns false: simWaitFirst always suspends and routes through
   * child coroutines, even when an operand has `await_ready() == true`.
   * Keeps the resolution path uniform and avoids forking the side-effect
   * chain (some primitives consume state in `await_resume`; spawning a
   * child exercises the same path the operand would on a normal co_await).
   *
   * @return Always `false`.
   */
  constexpr bool await_ready() const noexcept {
    return false;
  }

  /**
   * Suspend the parent coroutine and arm all operand child coroutines.
   * If any child resolves synchronously during arming (e.g., its operand
   * had `await_ready() == true`), parent resume is deferred until armAll
   * completes — preventing `*this` from being destructed mid-loop.
   *
   * @param h  Handle to the suspended parent coroutine frame.
   */
  void await_suspend(std::coroutine_handle<> h) noexcept {
    state_.parentHandle = h;
    state_.state = detail::WaitFirstRaceState::ARMING;
    armAll(std::index_sequence_for<Ops...>{});
    // armAll exits with state in {ARMING (no child resolved) or
    // ARMING_RESOLVED (a child resolved synchronously during arming)}.
    if (state_.state == detail::WaitFirstRaceState::ARMING_RESOLVED) {
      // Deferred resume from resolve() — we're outside the arming loop
      // now and can safely resume the parent.
      state_.state = detail::WaitFirstRaceState::PARENT_RESUMED;
      // resume() may run the parent's body all the way through to the
      // next suspension or completion, including destruction of *this.
      // MUST be the last statement in await_suspend — no post-resume
      // access to *this allowed (mirrors resolve()'s contract).
      state_.parentHandle.resume();
    } else {
      // No child resolved during arming; transition to ARMED and wait
      // for a child to call resolve().
      state_.state = detail::WaitFirstRaceState::ARMED;
    }
  }

  /**
   * @return The size_t index (in [0, sizeof...(Ops))) of the operand
   *         that won the race.
   */
  std::size_t await_resume() const noexcept {
    return state_.resolvedIdx;
  }

 private:
  template <std::size_t... Is>
  void armAll(std::index_sequence<Is...>) noexcept {
    (armOne<Is>(), ...);
  }

  // Fully cancel every operand during destroy-time teardown (see the
  // destructor). Each `simWaitFirstTeardown` destroys the operand's child
  // coroutine frame (if armed), unlinks its waiter, and cancels any scheduled
  // resume; it is idempotent on a not-yet-armed or already-cancelled operand,
  // so applying it across all operands is safe in every unresolved state.
  // Destroying a child frame unwinds it at its `co_await operand` suspension
  // and does NOT run `state_->resolve()`, so there is no reentrancy back into
  // the race state while we iterate.
  template <std::size_t... Is>
  void teardownAll(std::index_sequence<Is...>) noexcept {
    using detail::simWaitFirstTeardown;
    (simWaitFirstTeardown(std::get<Is>(ops_)), ...);
  }

  template <std::size_t I>
  void armOne() noexcept {
    // If an earlier armOne<J<I> already triggered a synchronous resolve
    // (operand's await_ready was true), don't arm any more operands —
    // state is now ARMING_RESOLVED, parent resume is deferred to
    // await_suspend's post-loop check. Any late-arming child here would
    // be a guaranteed loser, would never get cancelled (resolve()'s
    // loser-cancel pass already ran with cancellers[I] still default-
    // constructed), and would suspend on its operand permanently —
    // leaking the child coroutine frame for the life of the simulation.
    if (state_.state == detail::WaitFirstRaceState::ARMING_RESOLVED) {
      return;
    }
    auto& op = std::get<I>(ops_);
    // Capture by reference: op lives in this->ops_, this lives in the
    // parent frame, parent stays suspended until resolve() resumes it.
    // The using-declaration here is the canonical ADL hook — see the
    // detail-namespace block at the top of this file for the full
    // rationale.
    state_.cancellers[I] = [opPtr = &op]() noexcept {
      using detail::simWaitFirstCancel;
      simWaitFirstCancel(*opPtr);
    };
    // Spawn child coroutine. SimTask is fire-and-forget; the child
    // immediately runs to its `co_await operand` and suspends. When op
    // resolves (or the canceller destroys the frame), the child either
    // calls resolve() or vanishes. Either way, the child's frame is
    // managed by its co_await suspension.
    detail::waitFirstChild<I>(folly::not_null{&state_}, folly::not_null{&op});
  }

  std::tuple<Ops...> ops_;
  detail::WaitFirstState<sizeof...(Ops)> state_;
};

/**
 * Race a fixed pack of cancellable awaitables. Returns the size_t
 * index of the first one to resolve; cancels the rest.
 *
 * Modeled after `folly::coro::collectAny` / `tokio::select!`.
 * Specialized for PackSim's PDES (Parallel Discrete Event Simulation)
 * single-context model: all operands suspend the same parent coroutine
 * on the same SimContext, so no cross-thread coordination is needed.
 *
 * Example — race a timer against a signal:
 *
 *     SimCancellableDelay timer;
 *     SimSignal sig{simCtx_};
 *
 *     SimTask retryLoop() {
 *       while (true) {
 *         const size_t idx = co_await simWaitFirst(
 *             timer.awaitDelay(simCtx_, Time::fromNs(retryNs_)),
 *             sig.wait());
 *         if (idx == 0) {
 *           // timer fired first → trigger retry
 *         } else {
 *           // signal fired first → cancel scheduled retry, restart
 *         }
 *       }
 *     }
 *
 * Variadic — accepts 2..`SimWaitFirstAwaitable::kMaxOperands` operands.
 * Returns size_t in [0, N) identifying the winner. Operands are
 * evaluated in argument order.
 *
 * ### Cancellation
 *
 * On the winner's resolution, each loser's primitive is cancelled via
 * `simWaitFirstCancel(operand)` (ADL free function). The cancel call:
 *   1. Removes any in-flight scheduled resume from the PDES queue.
 *   2. Destroys the suspended child coroutine frame for that operand.
 *
 * Supported operand types (built-in adapters):
 *   - `SimCancellableDelay::Awaitable` (returned by `awaitDelay()`)
 *   - `SimSignal::Awaitable` (returned by `wait()`)
 *   - `SimStopwatch::Awaitable` (returned by `expiration()` /
 *     `startOneShot()`)
 *   - `SimEvent::Awaitable` (returned by `wait()`)
 *
 * To extend `simWaitFirst` to a new awaitable type, define a free
 * function in the same namespace as the awaitable:
 *
 *     void simWaitFirstCancel(MyPrimitive::Awaitable& aw) noexcept {
 *       aw.cancel();  // or whatever the primitive's tear-down is
 *     }
 *
 * Argument-dependent lookup (ADL) finds it during template instantiation.
 *
 * There is a SECOND extension hook: `simWaitFirstTeardown(operand)`, called
 * by `~SimWaitFirstAwaitable` when the race is destroyed UNRESOLVED (the
 * owning coroutine frame is being torn down, e.g. via
 * `SimScopedTask::reset()`). Teardown must FULLY cancel the operand so no
 * installed waiter and no scheduled PDES event survives the frame's
 * destruction. The generic `simWaitFirstTeardown` just forwards to
 * `simWaitFirstCancel` — correct ONLY if your loser-cancel is already a full
 * teardown. If your loser-cancel deliberately PRESERVES a scheduled event
 * (as `SimStopwatch`'s `cancelWaiterOnly()` does, to keep a losing timer
 * counting across race iterations), you MUST also define an override:
 *
 *     void simWaitFirstTeardown(MyPrimitive::Awaitable& aw) noexcept {
 *       aw.fullCancel();  // clear the preserved event too
 *     }
 *
 * Otherwise the preserved event outlives the frame and trips the primitive's
 * destructor XCHECK at teardown. See the `simWaitFirstCancel` /
 * `simWaitFirstTeardown` adapter definitions near the top of this header for
 * the built-in examples (`SimStopwatch` is the lone override).
 *
 * ### Operand requirements
 *
 * Each operand must be (a) move-constructible (the awaitable holds them
 * in a `std::tuple`) and (b) noexcept-move-constructible (the awaitable's
 * constructor is `noexcept`, so a throwing move would `std::terminate`).
 * Both are checked at compile time via `static_assert`.
 *
 * NOTE: Awaitables with deleted move constructors (e.g.,
 * `SimBarrier::Awaitable`, `SimCoordRoutedBarrier::Awaitable`) cannot
 * currently be raced. Workaround: wrap them in a `SimTask` that
 * eventually broadcasts a `SimSignal`, then race the signal.
 *
 * ### Tie-breaking
 *
 * If multiple operands are immediately ready (`await_ready() == true`)
 * at the time `co_await` is evaluated, the operand with the lowest index
 * in argument order wins. Deterministic.
 *
 * If multiple operands resolve at the same sim-tick via PDES events,
 * PDES dispatch order tie-breaks. In single-threaded PDES this is
 * deterministic; in multi-threaded PDES it follows the engine's
 * tie-break rules.
 *
 * ### Single-context contract
 *
 * Every operand's primitive must be scoped to the same SimContext as
 * the parent coroutine that issues the `co_await`. Cross-context races
 * are unsupported (use `simScheduleOn` to migrate first).
 *
 * ### Lifetime
 *
 * The awaitable lives in the parent coroutine's frame until the
 * `co_await` expression resolves, at which point all child frames have
 * been destroyed (winner naturally on `co_return`, losers via cancel)
 * and the operands' primitives have no residual in-flight state.
 *
 * `noexcept` everywhere on operational paths; allocation failures
 * during child-coroutine spawn terminate at the failing site (per
 * SimTask.h, "Error model").
 *
 * @param ops  The operand awaitables to race. Taken by value and moved
 *             into the awaitable's tuple.
 * @return     A `SimWaitFirstAwaitable` ready to be `co_await`ed; the
 *             `co_await` expression yields the size_t index of the
 *             winning operand.
 */
template <typename... Ops>
// NOTE: keep this [[nodiscard]] message in sync with collectAny's
// below — both are required because the attribute does not propagate
// from a sibling.
[[nodiscard(
    "simWaitFirst's return value must be co_await'd; discarding it constructs "
    "an awaitable that never enters await_suspend, so no operand is ever armed "
    "and no race resolves")]]
SimWaitFirstAwaitable<std::decay_t<Ops>...> simWaitFirst(
    Ops&&... ops) noexcept {
  return SimWaitFirstAwaitable<std::decay_t<Ops>...>(std::forward<Ops>(ops)...);
}

/**
 * `collectAny` — folly::coro-aligned alias for `simWaitFirst`.
 *
 * Mirrors `folly::coro::collectAny` naming so packsim::coro coroutine
 * code reads similarly to folly::coro coroutine code. Identical
 * semantics to `simWaitFirst` (race a fixed pack of cancellable
 * awaitables; return `size_t` index of the first to resolve; cancel
 * the rest). Use whichever name reads more clearly at the call site.
 *
 * @param ops  The operand awaitables to race. Body constructs the
 *             same `SimWaitFirstAwaitable` template instantiation as
 *             `simWaitFirst`; a `static_assert` in the test suite
 *             pins the type equivalence.
 *
 * @return     A `SimWaitFirstAwaitable` ready to be `co_await`ed;
 *             the `co_await` expression yields the size_t index of
 *             the winning operand. See `simWaitFirst` for full
 *             semantics, operand requirements, and the
 *             `simWaitFirstCancel` ADL contract.
 */
template <typename... Ops>
// NOTE: keep this [[nodiscard]] message in sync with simWaitFirst's
// above — both are required because the attribute does not propagate
// from a delegated callee.
[[nodiscard(
    "collectAny's return value must be co_await'd; discarding it constructs "
    "an awaitable that never enters await_suspend, so no operand is ever armed "
    "and no race resolves")]]
SimWaitFirstAwaitable<std::decay_t<Ops>...> collectAny(Ops&&... ops) noexcept {
  // Direct construction — a true alias of `simWaitFirst`, with the same
  // body. Keeping the two bodies identical ensures a future change touches
  // both forms uniformly.
  return SimWaitFirstAwaitable<std::decay_t<Ops>...>(std::forward<Ops>(ops)...);
}

} // namespace packsim::coro
