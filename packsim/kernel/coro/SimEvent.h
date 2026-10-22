/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include <folly/container/small_vector.h>
#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"

namespace packsim::coro {

/**
 * Mode of a SimEvent.
 *
 * Pulse — wakes only those waiters currently suspended on `wait()` at the
 *         moment broadcast() is called. Waiters that arrive *after* the
 *         broadcast see nothing — they suspend and stay suspended until the
 *         next broadcast. Use for events where missing the pulse is fine
 *         because the receiver re-checks observable state on its next
 *         iteration anyway (e.g., "ctlosAvailable" — the TX driver re-polls
 *         its queues on every wake).
 *
 * Latched — sticky bit. broadcast() sets the bit AND wakes current waiters.
 *           A subsequent wait() while the bit is set returns immediately
 *           (await_ready=true). The bit stays set until reset() is called.
 *           Use for monotonic state changes where missing the broadcast
 *           would be a correctness bug (e.g., "shutdownRequested": a
 *           coroutine spawned after shutdown must immediately observe it
 *           and exit, not wait for another broadcast that will never come).
 */
enum class SimEventMode : uint8_t {
  PULSE,
  LATCHED,
};

/**
 * Multi-waiter broadcast event. Sister to SimSignal — but where SimSignal
 * is single-waiter with a pending-signal counter (queues missed signals for
 * the eventual consumer), SimEvent is multi-waiter without queueing (no
 * per-waiter pending count; latecomers don't see past pulses).
 *
 * The Pulse vs Latched mode distinction is essential — see SimEventMode for
 * when each applies.
 *
 * Use SimEvent when N≥2 coroutines need to react to the same event. Use
 * SimSignal when exactly one coroutine reacts (and you want missed signals
 * queued).
 *
 * Example — a LATCHED stop event ends every worker coroutine, including one
 * that starts waiting after the broadcast:
 *
 *     SimEvent stopRequested(simCtx_, SimEventMode::LATCHED);
 *
 *     // In each worker coroutine (`work` is that worker's own SimSignal):
 *     co_await simWaitFirst(work.wait(), stopRequested.wait());
 *     if (stopRequested.isLatched()) co_return;
 *
 *     // From the owner's stop path:
 *     stopRequested.broadcast();   // wakes all parked coroutines
 *
 * ### Multi-waiter list
 *
 * Each `wait()` returns an Awaitable that, when suspended, installs itself
 * as a node in the SimEvent's intrusive doubly-linked list of waiters. The
 * Awaitable's address must be stable from `await_suspend` through either
 * `broadcast()`-driven resume or `cancelWaiter()`. SimWaitFirst guarantees
 * this stability: operands live in the SimWaitFirst awaitable's tuple,
 * which lives in the parent coroutine's frame (suspended throughout the
 * race).
 *
 * ### Per-waiter cancellation
 *
 * `simWaitFirstCancel(SimEvent::Awaitable&)` — called by SimWaitFirst when
 * this waiter loses the race — removes ONLY the named waiter from the list,
 * cancels its scheduled resume (if any), and destroys its coroutine frame.
 * Sibling waiters are unaffected. This contrasts with SimSignal::cancel()
 * which destroys *the* sole waiter; SimEvent's per-waiter cancel is a
 * fundamentally different contract.
 *
 * ### Resume mode
 *
 * `broadcast()` is DEFERRED: each waiter's resume is posted via
 * `scheduleNow`, never `h.resume()` inline. Reasons:
 *   - Bounds stack depth on broadcasts that touch many waiters.
 *   - Avoids re-entrancy hazards: if a hot-path method fires a broadcast
 *     that wakes a coroutine which then calls back into a hot-path method,
 *     DEFERRED mode breaks the chain across PDES dispatch ticks.
 *
 * `broadcast()` and `wait()` have no DIRECT-style entry, so they can't be
 * misconfigured at the use site — consumer call sites cannot accidentally
 * introduce a re-entrancy chain through them.
 *
 * `broadcastInline()` is the ONLY DIRECT-style path on SimEvent: it resumes
 * waiters synchronously, for callers that need the woken coroutines to run
 * before they return. See the method's docstring for the contract callers
 * must satisfy and the misuse hazards (stack growth, re-entrancy).
 *
 * ### Lifetime contract
 *
 * - The SimContext passed at construction MUST outlive the SimEvent AND any
 *   in-flight scheduled resume callback.
 * - SimEvent's destructor XCHECKs that no waiters are installed. It cannot see
 *   resumes `broadcast()` has already queued, since broadcast() detaches the
 *   list first. The owning class MUST drain waiters before destruction: wake
 *   them and let each run past its wait (a woken waiter's Awaitable reads the
 *   event when it is destroyed), or destroy their frames (a destroyed waiter's
 *   Awaitable unlinks itself).
 *
 * ### Thread / context locality
 *
 * All operations (wait, broadcast, reset, cancelWaiter) MUST happen on the
 * SimContext passed at construction. To broadcast from another context,
 * marshal via `ctx->scheduleRemote([&e] { e.broadcast(); })` or use
 * `co_await simScheduleOn(targetCtx)` first.
 */
class SimEvent {
 public:
  /**
   * @param ctx   Simulation context. MUST outlive the SimEvent and any
   *              in-flight scheduled resume callback.
   * @param mode  Pulse (default) or Latched. See SimEventMode.
   */
  explicit SimEvent(
      folly::not_null<SimContext*> ctx,
      SimEventMode mode = SimEventMode::PULSE) noexcept
      : ctx_(ctx), mode_(mode) {}

  // Owns intrusive list of Awaitables (each holding a coroutine_handle and
  // a Scheduler::EventHandle capturing `this` raw). Any copy/move would dangle
  // the entire waiter list.
  SimEvent(const SimEvent&) = delete;
  SimEvent& operator=(const SimEvent&) = delete;
  SimEvent(SimEvent&&) = delete;
  SimEvent& operator=(SimEvent&&) = delete;

  /**
   * Destructor. Always-on XCHECK catches the "SimEvent destroyed while
   * waiters are still installed" lifetime bug: each installed waiter would
   * leak its coroutine frame, and any queued resume callback (scheduled by
   * a prior broadcast() that hasn't fired yet) holds the waiter's
   * Awaitable* raw and would use-after-free on it when PDES dispatched it.
   *
   * Note: broadcast() detaches the waiter list before scheduling each
   * resume, so `head_ == nullptr` is the single invariant that captures
   * both "no installed waiters" and "no in-flight resumes can dangle a
   * waiter that's still in the list". A queued resume holds its Awaitable*
   * directly, and that Awaitable reads this SimEvent when it is destroyed
   * after its resume, so this check cannot catch an event destroyed while
   * woken waiters have yet to run past their wait.
   *
   * Owning classes drain waiters first: wake them so they co_return, or
   * destroy their frames (e.g. `SimScopedTask::reset()`).
   */
  ~SimEvent() noexcept {
    XCHECK(head_ == nullptr)
        << "SimEvent destroyed with installed waiters; the suspended "
           "coroutine frames would leak and any queued resume lambda "
           "holds its Awaitable* raw and would use-after-free on it when "
           "PDES dispatched it";
  }

  /**
   * Wake all currently-suspended waiters. In Latched mode, also set the
   * sticky bit (a subsequent wait() will resolve immediately via
   * await_ready until reset() is called).
   *
   * Each waiter's resume is posted via `ctx_->scheduleNow` (DEFERRED), so
   * broadcast() returns promptly and the resumes happen on subsequent PDES
   * ticks. This bounds stack depth and breaks re-entrancy chains.
   *
   * The current waiter list is detached at the start of broadcast(); if a
   * resumed waiter (later, when its scheduled callback fires) calls wait()
   * again on this SimEvent, it installs into a fresh empty list and waits
   * for the *next* broadcast.
   *
   * `noexcept` — terminates on scheduler OOM (per SimTask.h, "Error model").
   */
  void broadcast() noexcept {
    if (mode_ == SimEventMode::LATCHED) {
      isLatched_ = true;
    }

    // Detach the current waiter list. Any waiter that subsequently calls
    // wait() (e.g., from inside its own resume) installs into a fresh list
    // and is not re-resumed by this broadcast.
    Awaitable* current = head_;
    head_ = nullptr;

    // Schedule a deferred resume for each detached waiter. We capture the
    // Awaitable* by value; its address is stable for the duration of the
    // suspended coroutine (it lives in the parent's frame). The captured
    // `aw->handle` will be cleared and resumed by the lambda body.
    while (current != nullptr) {
      Awaitable* const next = current->next;
      // Clear list pointers so a subsequent cancelWaiter() (e.g., from a
      // SimWaitFirst loser-cancel that arrives in the queued-but-not-fired
      // window) sees the waiter unlinked.
      current->prev = nullptr;
      current->next = nullptr;
      Awaitable* const aw = current;
      aw->scheduledResume = ctx_->scheduleNow([aw] {
        // Self-clear-then-resume so a cancelWaiter() race during the
        // queued-but-not-yet-fired window is harmless: the lambda body has
        // already cleared `scheduledResume` and `handle` before resume.
        aw->scheduledResume = {};
        const auto h = aw->handle;
        aw->handle = {};
        // h.resume() may run the resumed coroutine all the way to its
        // next suspension or completion. The resumed body may destruct
        // *aw and the SimEvent itself (e.g., when the consumer owns this
        // SimEvent and tears it down inline). Per-waiter state has been
        // cleared (scheduledResume, handle) BEFORE resume so any reentry
        // observes a quiescent waiter. MUST be the last statement in
        // this lambda — no post-resume access to *aw or this SimEvent
        // allowed.
        h.resume();
      });
      current = next;
    }
  }

  /**
   * Synchronous variant of `broadcast()`: resumes each waiter INLINE on
   * the calling stack (via `h.resume()`) rather than scheduling a deferred
   * resume. By the time this returns, every waiter has been resumed and
   * has run synchronously through to its next suspension or `co_return`.
   *
   * Use it when the woken coroutines must run before the caller continues —
   * e.g. a producer waking a parked consumer without a PDES round trip
   * (`LinkLayerScheduler`'s data and CtlOS wakes), or an owner about to
   * destruct — and only when:
   *   1. Re-entrancy is bounded: typically the woken path never re-enters the
   *      calling producer, so wakes cannot recurse without limit.
   *   2. No woken coroutine tears down another waiter of this event — by
   *      destroying its frame (e.g. `SimScopedTask::reset()`), or by winning
   *      a `simWaitFirst` race that has a second operand on this event. That
   *      waiter's handle has already been taken, and it is resumed anyway.
   *   3. No woken coroutine destroys this event while later waiters are still
   *      to be resumed: each waiter's Awaitable reads the event when it is
   *      destroyed after its resume.
   *
   * Hazards if misused:
   *   - Stack growth: each waiter's resume runs nested on this stack. With
   *     thousands of waiters or deep coroutine chains, can stack-overflow.
   *   - Re-entrancy: if a waiter's body fires another `broadcastInline` on
   *     the same event, behavior is well-defined (the new broadcast runs
   *     nested within this one's resume loop and is a no-op because
   *     `head_` was already cleared before resuming any waiter — see the
   *     `LatchedBroadcastInlineReEntrantIsSafe` test) but the recursion
   *     is on the same stack.
   *
   * In Latched mode, also sets the sticky bit (same as `broadcast()`).
   *
   * `noexcept` — terminates if any resumed coroutine throws. A coroutine
   * resumed by the framework has no caller to propagate to, so a throw
   * here is a framework-internal failure rather than a recoverable error.
   */
  void broadcastInline() noexcept {
    if (mode_ == SimEventMode::LATCHED) {
      isLatched_ = true;
    }
    // Snapshot ALL handles before any resume runs. The naive walk-and-
    // resume pattern (read `next`, resume `current`, advance to `next`)
    // is unsafe: a resumed coroutine may destroy an `Awaitable` still
    // linked here (e.g. by destroying the frame that holds it), leaving
    // the walk pointer dangling.
    //
    // Snapshot first → unlink everything → resume from the snapshot.
    // After this loop the SimEvent's own list is empty and no Awaitable
    // is referenced by us. The snapshot cannot protect a waiter that is
    // torn down before its turn; see item 2 of the contract above.
    //
    // Inline capacity for small waiter sets; larger ones allocate on the
    // heap. Correctness is unaffected.
    constexpr std::size_t kInlineCap = 8;
    folly::small_vector<std::coroutine_handle<>, kInlineCap> handles;
    for (Awaitable* aw = head_; aw != nullptr;) {
      Awaitable* const next = aw->next;
      aw->prev = nullptr;
      aw->next = nullptr;
      // Invariant: an Awaitable in `head_` has no `scheduledResume`
      // pending. `broadcast()` detaches `head_` before assigning
      // `scheduledResume` on the detached chain, so a head_-linked
      // Awaitable can never carry a queued deferred resume. If a future
      // refactor breaks this — e.g., a new broadcast variant that
      // schedules without detaching — the queued lambda would fire on a
      // freed Awaitable after this synchronous resume returns and
      // unlinked it.
      XDCHECK(!aw->scheduledResume.isValid())
          << "broadcastInline: head_-linked Awaitable has a pending "
             "scheduledResume; broadcast() must detach head_ before "
             "scheduling deferred resumes (see invariant above)";
      if (aw->handle) {
        handles.emplace_back(aw->handle);
        aw->handle = {};
      }
      aw = next;
    }
    head_ = nullptr;
    // h.resume() may run the resumed body to completion, which may
    // destruct *this (the SimEvent) entirely once no later waiter remains
    // (contract item 3). The loop intentionally only touches the
    // local `handles` vector after each resume; no member access of
    // *this past this point. MUST stay this way: any post-resume
    // member access would be UAF in the "owner-destructs-during-
    // resume" case.
    for (const auto& h : handles) {
      h.resume();
    }
  }

  /**
   * Latched mode only: clear the sticky bit. Subsequent wait() calls (when
   * no broadcast is pending) suspend rather than immediately resuming.
   *
   * Asserts mode_ == LATCHED via XDCHECK in dev/test builds — `reset()` is a
   * mutator with explicit intent, so calling it on a Pulse-mode event is a
   * coherent programming error: the caller has stated they want to clear a
   * bit that does not exist.
   *
   * Does NOT wake any installed waiters or affect any in-flight scheduled
   * resumes.
   */
  void reset() noexcept {
    XDCHECK(mode_ == SimEventMode::LATCHED)
        << "SimEvent::reset() called on a Pulse-mode event; no-op (no bit "
           "to clear). Likely a programming error.";
    isLatched_ = false;
  }

  /**
   * @return The sticky bit. In Pulse mode this is always false (broadcast()
   * never sets it; the bit is logically absent). In Latched mode this is
   * true once broadcast() has fired and false once reset() clears it.
   *
   * Use this from inside a coroutine to re-check the latched state on every
   * wake (the broadcast that set the bit may have happened while the
   * coroutine was busy elsewhere; the wait() that wakes it observes the
   * post-bit state).
   *
   * Asymmetry vs reset(): isLatched() is a pure query, deliberately
   * permissive in Pulse mode. Generic code paths that work against either
   * mode (e.g., a shared "is anything latched yet?" check in a SimWaitFirst
   * branch) should be able to call it without first dispatching on mode.
   * "Latched? No." is a coherent answer in Pulse mode — the bit is
   * logically absent. reset() does not have a coherent "no-op" reading
   * (the caller asked to clear a non-existent bit), so it XDCHECKs.
   */
  [[nodiscard]] bool isLatched() const noexcept {
    return isLatched_;
  }

  /**
   * Single-use awaitable returned by `wait()`. Holds list pointers and
   * waiter state inline (intrusive node). Stable address contract: the
   * Awaitable MUST live at a fixed address from `await_suspend` through
   * either `broadcast()`-driven resume or `cancelWaiter()`. SimWaitFirst
   * provides this guarantee for its operand tuple.
   *
   * Returned by value from `wait()`; the compiler stores the value in the
   * caller's coroutine frame (stable for the duration of the suspended
   * co_await expression).
   */
  struct Awaitable {
    // Brace-initialized via `Awaitable{this}` from wait()'s call site.
    // `folly::not_null` enforces the non-null invariant at the type level
    // — there is no `Awaitable{}` default-construction path that could
    // leave `event` null and invite UB on a subsequent await_*().
    const folly::not_null<SimEvent*> event;

    // Intrusive list pointers. nullptr when not in a list (initial state,
    // or after broadcast() / cancelWaiter() unlink).
    Awaitable* prev{nullptr};
    Awaitable* next{nullptr};

    // Suspended coroutine handle. Set by await_suspend; cleared by the
    // resume lambda OR by cancelWaiter().
    std::coroutine_handle<> handle{};

    // PDES event handle of the queued resume (DEFERRED mode). Set by
    // broadcast(); cleared by the resume lambda OR by cancelWaiter().
    Scheduler::EventHandle scheduledResume{};

    // ----- Special members (Rule of Five) -----
    // The user-declared destructor below detaches this waiter, so the
    // awaitable is no longer an aggregate and needs an explicit constructor.
    // It is moved ONLY before `await_suspend` installs it (simWaitFirst moves
    // operands into its tuple at construction; a bare `co_await` elides the
    // move), so a moved-from source is always still un-installed (prev/next/
    // scheduledResume null) and its own destructor is a no-op. Copy is deleted
    // — an installed intrusive list node must never be duplicated.
    explicit Awaitable(folly::not_null<SimEvent*> e) noexcept : event(e) {}
    Awaitable(Awaitable&&) noexcept = default;
    Awaitable& operator=(Awaitable&&) = delete;
    Awaitable(const Awaitable&) = delete;
    Awaitable& operator=(const Awaitable&) = delete;

    /**
     * Destroy-safe teardown. If the owning coroutine frame is destroyed while
     * still suspended on this awaitable (e.g. `SimScopedTask::reset()`), unlink
     * from the event's waiter list and cancel any pending deferred resume — so
     * `~SimEvent` sees an empty list and no scheduled resume fires into the
     * freed frame. Does NOT destroy the frame (it is already unwinding).
     *
     * No-op on every other path: the broadcast resume lambda and
     * `cancelWaiter()` both clear prev/next/scheduledResume before this runs,
     * and the double-unlink is idempotent (neighbor fixups are identical).
     */
    ~Awaitable() noexcept {
      scheduledResume.cancel();
      scheduledResume = {};
      if (prev != nullptr) {
        prev->next = next;
      } else if (event->head_ == this) {
        event->head_ = next;
      }
      if (next != nullptr) {
        next->prev = prev;
      }
    }

    /**
     * True iff Latched mode and the sticky bit is set — the consumer
     * resumes immediately without suspending or installing into the list.
     */
    [[nodiscard]] bool await_ready() const noexcept {
      return event->isLatched_;
    }

    /**
     * Suspend; install `this` as the head of the waiter list. (LIFO is fine
     * — broadcast iterates the list and wakes everyone.)
     */
    void await_suspend(std::coroutine_handle<> h) noexcept {
      handle = h;
      // Push to head of list (O(1) install).
      next = event->head_;
      if (event->head_ != nullptr) {
        event->head_->prev = this;
      }
      event->head_ = this;
    }

    /** No-op. The awaitable does not produce a value. */
    constexpr void await_resume() const noexcept {}

    /**
     * Tear down this waiter's hold on the event. Used by the
     * `simWaitFirst` loser-cancel contract to drop a single suspended
     * waiter when another operand wins the race. Forwards to
     * `SimEvent::cancelWaiter(this)`. Sibling waiters on the same event
     * are unaffected.
     */
    void cancel() noexcept {
      event->cancelWaiter(this);
    }
  };

  /**
   * @return An Awaitable that the caller MUST `co_await`. In Pulse mode the
   *         consumer suspends until the next broadcast(). In Latched mode
   *         the consumer resumes immediately if the sticky bit is set;
   *         otherwise suspends.
   */
  [[nodiscard(
      "SimEvent::wait()'s return value must be co_await'd; discarding it "
      "constructs an awaitable that is never installed in the waiter list "
      "and produces no observable effect")]]
  Awaitable wait() noexcept {
    return Awaitable{this};
  }

  /**
   * Per-waiter cancellation: remove `aw` from the waiter list, cancel its
   * scheduled resume (if any), and destroy its coroutine frame. Used by
   * SimWaitFirst's loser-cancel contract via the ADL adapter
   * `simWaitFirstCancel(SimEvent::Awaitable&)`.
   *
   * Sibling waiters are unaffected.
   *
   * Idempotent and safe to call when the waiter is in any state:
   *   - Suspended in list (broadcast hasn't fired): unlink, destroy frame.
   *   - Queued resume pending (broadcast fired, lambda not yet executed):
   *     cancel the event, destroy frame.
   *   - Already resumed (lambda fired): no-op (handle/scheduledResume both
   *     cleared by the lambda body).
   *
   * GOTCHA — cancel() does NOT run code between consecutive co_awaits:
   * `coroutine_handle::destroy()` unwinds the stack frame at the suspension
   * point and runs destructors of locals scoped to that suspension. It does
   * NOT execute statements that come after the awaited expression. See the
   * matching paragraph in coro/WaiterState.h for full discussion.
   */
  void cancelWaiter(folly::not_null<Awaitable*> aw) noexcept {
    // Cancel scheduled resume if pending. Safe if not in flight (no-op).
    aw->scheduledResume.cancel();
    aw->scheduledResume = {};

    // Unlink from list if still linked. broadcast() unlinks each detached
    // waiter (zeroes prev/next) before scheduling its resume; so a waiter
    // has non-null prev/next iff broadcast() hasn't fired for it yet AND
    // no prior cancel ran. The unlink below is a no-op in the
    // already-broadcast or already-cancelled cases.
    if (aw->prev != nullptr) {
      aw->prev->next = aw->next;
    } else if (head_ == aw) {
      head_ = aw->next;
    }
    if (aw->next != nullptr) {
      aw->next->prev = aw->prev;
    }
    aw->prev = nullptr;
    aw->next = nullptr;

    // Destroy coroutine frame if handle still set. The resume lambda would
    // have cleared handle before calling h.resume(); if cancel beats the
    // lambda, we destroy here.
    if (aw->handle) {
      const auto h = aw->handle;
      aw->handle = {};
      // h.destroy() unwinds the suspended frame, which may run destructors
      // of locals scoped to that suspension (which themselves may touch
      // *aw or this SimEvent). All per-waiter state has been cleared
      // (prev/next/handle/scheduledResume) BEFORE destroy so any reentry
      // observes a fully unlinked waiter. MUST be the last statement in
      // cancelWaiter() — no post-destroy access to *aw allowed.
      h.destroy();
    }
  }

 private:
  // Compile-time validation that the noexcept paths' assumptions hold.
  static_assert(
      std::is_nothrow_default_constructible_v<Scheduler::EventHandle>);
  static_assert(std::is_nothrow_assignable_v<
                Scheduler::EventHandle&,
                Scheduler::EventHandle>);
  static_assert(
      noexcept(std::declval<const Scheduler::EventHandle&>().cancel()));

  const folly::not_null<SimContext*> ctx_;
  const SimEventMode mode_;

  // Intrusive head of the waiter list. nullptr = no waiters installed.
  Awaitable* head_{nullptr};

  // Latched-mode sticky bit. Always false in Pulse mode.
  bool isLatched_{false};
};

// SimWaitFirst's cancellation adapter for SimEvent::Awaitable lives
// alongside the adapters for SimSignal::Awaitable, SimCancellableDelay::
// Awaitable, and SimStopwatch::Awaitable in SimWaitFirst.h's `detail`
// namespace.

} // namespace packsim::coro
