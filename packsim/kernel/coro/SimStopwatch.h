/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>
#include <cstdint>
#include <type_traits>

#include <folly/Utility.h>
#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

namespace packsim::coro {

/**
 * Long-lived primitive whose `expiration()` awaitable wakes when an internal
 * stopwatch reaches `maxDuration`. The stopwatch can be started, reset, or
 * stopped externally — composing cleanly with the spec-style "timer with
 * external reset events" pattern that SimDelay / SimCancellableDelay don't
 * support.
 *
 * Use this for the LLR replay_timer / data_age_timer / pcs_lost_status_timer
 * (UEC §5.1, Tbl 5-10). Each of those timers has multi-rule reset semantics:
 *
 *   replay_timer (Tbl 5-10) — 6 rules:
 *     1) Held at zero whenever outstanding_seq == 0.
 *     2) Activated when a new LLR frame TX completes.
 *     3) Activated when a replay frame TX completes.
 *     4) Reset to zero on LLR_ACK that frees buffer space.
 *     5) Reset to zero on LLR_NACK reception.
 *     6) On reaching max → expired; held at zero until replay commences (or
 *        an LLR_ACK arrives — re-arm via rule 4).
 *
 * SimDelay's `co_await simDelay(N)` is one-shot: any external "reset" requires
 * cancelling the suspended frame and re-spawning the awaiter. SimStopwatch
 * keeps the awaiter installed across reset() / stop() / start() transitions
 * and re-targets the underlying scheduled resume.
 *
 * State transitions (current_state --event--> next_state):
 *
 *   STOPPED  --start()------------>  RUNNING (begin counting from zero)
 *   STOPPED  --reset()------------>  RUNNING (begin counting from zero)
 *   STOPPED  --pause()------------>  XCHECK  (no elapsed time to preserve)
 *   STOPPED  --resume()----------->  XCHECK  (no in-flight pause to resume)
 *   STOPPED  --stop()------------->  STOPPED (no-op)
 *   STOPPED  --cancel()----------->  STOPPED (destroys the installed
 *                                            waiter, if any)
 *   STOPPED  --cancelWaiterOnly()->  STOPPED (destroys the installed
 *                                            waiter, if any)
 *   RUNNING  --start()------------>  RUNNING (no-op — already counting)
 *   RUNNING  --reset()------------>  RUNNING (re-zero, re-target scheduled
 *                                            resume)
 *   RUNNING  --pause()------------>  PAUSED  (save elapsed, cancel scheduled
 *                                            resume; waiter stays installed)
 *   RUNNING  --resume()----------->  RUNNING (no-op — already running)
 *   RUNNING  --stop()------------->  STOPPED (cancel scheduled resume)
 *   RUNNING  --cancel()----------->  STOPPED (destroy waiter; cancel
 *                                            scheduled)
 *   RUNNING  --cancelWaiterOnly()->  RUNNING (destroy waiter; scheduled
 *                                            resume preserved)
 *   RUNNING  --(elapsed)---------->  EXPIRED (auto — fires waiter if any)
 *   PAUSED   --start()------------>  RUNNING (re-arm from zero — discards
 *                                            saved elapsed; use resume() to
 *                                            preserve it)
 *   PAUSED   --reset()------------>  RUNNING (re-arm from zero; preserves
 *                                            any in-flight waiter)
 *   PAUSED   --pause()------------>  PAUSED  (no-op — already paused)
 *   PAUSED   --resume()----------->  RUNNING (re-arm with remaining =
 *                                            targetDuration - accumElapsed)
 *   PAUSED   --stop()------------->  STOPPED (back to idle; discards saved
 *                                            elapsed)
 *   PAUSED   --cancel()----------->  STOPPED (destroy waiter; back to idle)
 *   PAUSED   --cancelWaiterOnly()->  PAUSED  (destroy waiter; state and
 *                                            saved elapsed preserved)
 *   EXPIRED  --start()------------>  XCHECK  (use reset() to re-arm
 *                                            explicitly)
 *   EXPIRED  --reset()------------>  RUNNING (re-arm from zero)
 *   EXPIRED  --pause()------------>  XCHECK  (already past expiration —
 *                                            nothing left to preserve)
 *   EXPIRED  --resume()----------->  XCHECK  (already past expiration —
 *                                            nothing to resume)
 *   EXPIRED  --stop()------------->  STOPPED (back to idle)
 *   EXPIRED  --cancel()----------->  STOPPED (no-op for waiter; back to
 *                                            idle)
 *   EXPIRED  --cancelWaiterOnly()->  EXPIRED (no-op for waiter; state
 *                                            preserved)
 *
 * Awaiter behavior:
 *   - `expiration()` returns an Awaitable. await_ready() is true iff currently
 *     EXPIRED — the consumer immediately resumes.
 *   - Otherwise the consumer suspends. It is woken when:
 *       * the scheduled resume fires (state transitions to EXPIRED), OR
 *       * cancelWaiterOnly() is invoked via the Awaitable::cancel adapter
 *         (SimWaitFirst loser-cancel contract; the frame is destroyed and
 *         the consumer never resumes; stopwatch state and scheduled resume
 *         are preserved so other call sites can keep using the stopwatch),
 *         OR
 *       * cancel() is invoked (full teardown for owner shutdown; the frame
 *         is destroyed, scheduled resume is cancelled, and state is reset
 *         to STOPPED).
 *       * the owning coroutine frame is destroyed while suspended here (e.g.
 *         `SimScopedTask::reset()`): the `~Awaitable` destructor clears the
 *         suspended handle so the still-scheduled resume cannot wake the freed
 *         frame — but DELIBERATELY leaves the scheduled resume and state intact
 *         (a per-type destructor cannot distinguish this from a loser-cancel,
 *         which must keep counting). The owner must therefore still `cancel()`
 *         / `stop()` the stopwatch — its destructor XCHECKs that no scheduled
 *         resume is in flight (`!scheduledResume_.isActive()`).
 *   - stop() does NOT wake the consumer — it just removes the scheduled resume
 *     from the PDES queue. The consumer stays suspended; future start() or
 *     reset() will re-schedule a resume that eventually wakes them.
 *
 * Example — implementing replay_timer rule 4 (LLR_ACK frees buffer → reset):
 *
 *     SimStopwatch replayTimer_{simCtx_, Time::fromNs(replayTimerMaxNs_)};
 *
 *     SimTask replayTimerLoop() {
 *       while (enabled_) {
 *         co_await simWaitFirst(
 *             replayTimer_.expiration(),
 *             bufferAdvanced_.wait(),  // rule 4
 *             nackReceived_.wait(),    // rule 5
 *             enterFlush_.wait());
 *         // dispatch on winner; e.g., on bufferAdvanced: replayTimer_.reset();
 *       }
 *     }
 *
 * Example — one-shot delay with custom duration (the rare case where the
 * loop body needs a single delay whose length is not the configured max,
 * e.g., `LinkLayerScheduler`'s CtlOS spacing gate, whose wait is recomputed
 * on every arm):
 *
 *     const std::size_t idx = co_await simWaitFirst(
 *         stopwatch.startOneShot(nextWait()), dataReady.wait());
 *     // A losing stopwatch keeps its resume scheduled; clear it before the
 *     // next startOneShot().
 *     stopwatch.stop();
 *
 * `startOneShot` is provided here for ergonomic parity with the configured-
 * max API; it has the same suspend-resume-cancel contract as `expiration()`,
 * but it arms the stopwatch itself, so no resume may already be scheduled.
 *
 * CONTEXT-LOCAL: All operations MUST happen on the SimContext passed at
 * construction. The primitive MUST outlive any in-flight `co_await` against
 * it AND any resume callbacks already enqueued in the PDES queue.
 *
 * To call start() / reset() / stop() / cancel() from another context,
 * enqueue a remote event:
 *
 *     targetCtx->scheduleRemote([&sw] { sw.reset(); });
 *
 * or `co_await simScheduleOn(targetCtx)` before calling the mutator. The
 * consumer side (the coroutine that `co_await`s `expiration()`) must
 * already be running on the stopwatch's context — migrate via
 * `simScheduleOn` before awaiting if it isn't.
 *
 * Single-waiter contract: at most one waiter may be suspended on
 * `expiration()` (or the awaitable returned by `startOneShot`) at any
 * time. The contract is enforced by always-on XCHECKs in
 * `Awaitable::await_suspend` (catches a second `co_await`) and in
 * `startOneShot()` (catches a re-arm with a stale waiter, or while a resume
 * is still scheduled).
 *
 * Non-copyable AND non-movable: owns a `coroutine_handle` whose frame
 * address is fixed, so it must be stored as a long-lived member of the
 * owning object — never passed around by value.
 */
class SimStopwatch {
 public:
  /**
   * @param ctx          Simulation context. MUST outlive any in-flight
   *                     scheduled resume AND any suspended waiter on this
   *                     stopwatch.
   * @param maxDuration  The configured timer-max value (e.g., from
   *                     LlrConfig::replayTimerMax). Used by start() / reset()
   *                     to compute the next expiration time. Per UEC §5.1
   *                     Tbl 5-9, these are static configuration values that
   *                     do not change at runtime.
   */
  explicit SimStopwatch(
      folly::not_null<SimContext*> ctx,
      Time maxDuration) noexcept
      : ctx_(ctx), maxDuration_(maxDuration) {}

  // Owns a coroutine_handle whose frame has a fixed memory location, and
  // a Scheduler::EventHandle that captures `this` raw — any copy/move would
  // alias or strand both.
  SimStopwatch(const SimStopwatch&) = delete;
  SimStopwatch& operator=(const SimStopwatch&) = delete;
  SimStopwatch(SimStopwatch&&) = delete;
  SimStopwatch& operator=(SimStopwatch&&) = delete;

  /**
   * Destructor. Two always-on XCHECKs catch lifetime bugs in opt builds where
   * XDCHECK would compile out: a suspended waiter would leak its frame, and a
   * queued resume lambda would use-after-free on the captured `this` when
   * PDES dispatched it.
   *
   * Note: a default-constructed `Scheduler::EventHandle` (the post-fire and
   * post-cancel state) returns `isActive() == false`, so the second
   * XCHECK only fires when a resume is genuinely still queued.
   */
  ~SimStopwatch() noexcept {
    XCHECK(!suspendedHandle_)
        << "SimStopwatch destroyed with a suspended waiter; "
           "coroutine frame leaked";
    XCHECK(!scheduledResume_.isActive())
        << "SimStopwatch destroyed with a queued resume; the scheduled "
           "lambda holds `this` and will use-after-free when PDES "
           "dispatches it";
  }

  /**
   * Begin counting from zero with the configured maxDuration. XCHECKs that
   * state != EXPIRED (use reset() to re-arm an EXPIRED stopwatch). If RUNNING,
   * no-op (already counting). If STOPPED or PAUSED, arm and transition to
   * RUNNING — note that calling `start()` from PAUSED **discards** the saved
   * elapsed time and restarts the full duration; use `resume()` to continue
   * the remaining `maxDuration - elapsed`.
   */
  void start() noexcept {
    XCHECK(state_ != State::EXPIRED)
        << "SimStopwatch::start() from EXPIRED; use reset() to re-arm";
    if (state_ == State::RUNNING) {
      return;
    }
    armRun(maxDuration_);
  }

  /**
   * Re-zero the stopwatch and continue running. Equivalent to stop()+start()
   * but preserves an in-flight suspended waiter — the waiter stays installed
   * and is woken at the new deadline. Allowed from any state (STOPPED,
   * RUNNING, PAUSED, EXPIRED) — always transitions to RUNNING with elapsed=0.
   */
  void reset() noexcept {
    cancelScheduledResume();
    armRun(maxDuration_);
  }

  /**
   * Pause counting while preserving elapsed time. Cancels the in-flight
   * scheduled resume. On the next `resume()` the stopwatch re-arms with
   * `targetDuration - elapsed` remaining — so a 4ns timer paused at 1ns
   * and resumed will expire 3ns later.
   *
   * Differs from `stop()`: `stop()` resets the counter so a subsequent
   * `start()` runs the full duration again; `pause()` saves elapsed so
   * `resume()` completes the original budget.
   *
   * Allowed from RUNNING. No-op from PAUSED. XCHECKs from STOPPED /
   * EXPIRED — these states have no elapsed time to preserve and pausing
   * them is a programming error.
   *
   * A suspended waiter on `expiration()` stays installed across the
   * pause; `resume()`'s re-arm fires it at the new deadline.
   */
  void pause() noexcept {
    XCHECK(state_ == State::RUNNING || state_ == State::PAUSED)
        << "SimStopwatch::pause() requires RUNNING or PAUSED, got state="
        << +folly::to_underlying(state_);
    if (state_ == State::PAUSED) {
      return;
    }
    accumulatedElapsed_ += (ctx_->now() - runStartTime_);
    cancelScheduledResume();
    state_ = State::PAUSED;
  }

  /**
   * Resume counting from where `pause()` left off. Re-arms the scheduled
   * resume with `targetDuration - elapsed` remaining, saturated at zero
   * (so a stopwatch paused after its budget was already consumed
   * expires on the very next PDES tick).
   *
   * Allowed from PAUSED. No-op from RUNNING. XCHECKs from STOPPED /
   * EXPIRED.
   */
  void resume() noexcept {
    XCHECK(state_ == State::PAUSED || state_ == State::RUNNING)
        << "SimStopwatch::resume() requires PAUSED or RUNNING, got state="
        << +folly::to_underlying(state_);
    if (state_ == State::RUNNING) {
      return;
    }
    const Time remaining = (accumulatedElapsed_ < targetDuration_)
        ? (targetDuration_ - accumulatedElapsed_)
        : Time::fromNs(0);
    runStartTime_ = ctx_->now();
    armScheduledResume(remaining);
    state_ = State::RUNNING;
  }

  /**
   * Stop counting; on the next start() the elapsed counter restarts from
   * zero. The scheduled resume (if any) is removed from the PDES queue. A
   * suspended waiter on `expiration()` is NOT woken — it stays installed;
   * future start() or reset() will re-arm a resume that eventually wakes
   * it.
   *
   * Idempotent. Safe from any state — but note that `stop()` does not
   * tear down a suspended `expiration()` waiter; the destructor's first
   * XCHECK will fire if the owner destructs the SimStopwatch with a
   * waiter still installed. Either call `cancel()` before destruction
   * to tear down the waiter, or guarantee the consumer coroutine
   * completes (e.g., via the surrounding shutdown order).
   */
  void stop() noexcept {
    cancelScheduledResume();
    state_ = State::STOPPED;
  }

  /**
   * Full shutdown teardown: tear down the suspended waiter (destroying its
   * frame), remove any scheduled resume from PDES, and reset state to
   * STOPPED. Distinct from stop() — cancel() destroys the waiter; stop()
   * leaves it suspended.
   *
   * Use this when the stopwatch is being torn down (e.g., owner shutdown),
   * NOT for the SimWaitFirst loser-cancel contract — the Awaitable::cancel
   * adapter routes through `cancelWaiterOnly()` instead so a losing operand
   * does not force-stop a stopwatch that other call sites are still using.
   *
   * Idempotent. Safe to call when no waiter is suspended (no-op for the
   * waiter; still cancels any scheduled resume).
   *
   * GOTCHA — cancel() does NOT run code between consecutive co_awaits:
   * `coroutine_handle::destroy()` unwinds the stack frame at the suspension
   * point and runs destructors of locals scoped to that suspension. It does
   * NOT execute statements that come after the awaited expression. See the
   * matching paragraph in coro/SimCancellableDelay.h for full discussion;
   * the same caveat applies here.
   */
  void cancel() noexcept {
    cancelScheduledResume();
    state_ = State::STOPPED;
    // MUST be last — `h.destroy()` may unwind into *this. See the
    // reentrancy caveat in `cancelWaiterOnly()` body.
    cancelWaiterOnly();
  }

  /**
   * SimWaitFirst loser-cancel contract: drop the suspended waiter (if any)
   * by destroying its frame. Deliberately does NOT cancel the scheduled
   * resume and does NOT modify `state_` — this lets a single
   * `simWaitFirst` race over multiple stopwatches (e.g., `txRunAdvance`
   * awaiting replay_timer + data_age_timer + pcs_lost_status_timer)
   * cancel the losing operands' waiters without resetting their underlying
   * timer state. Losers keep counting toward their own expirations.
   *
   * Idempotent. Safe to call when no waiter is suspended (no-op).
   *
   * WARNING: Because the scheduled resume is preserved, this method alone
   * is NOT sufficient teardown before destruction. If a `RUNNING`
   * stopwatch is destroyed after only `cancelWaiterOnly()`, the
   * destructor's `!scheduledResume_.isActive()` XCHECK will fire. For
   * owner shutdown call `cancel()` (full teardown) or `stop()`.
   *
   * GOTCHA — same as `cancel()`: `coroutine_handle::destroy()` unwinds
   * the suspended frame at the current suspension point. It does NOT
   * execute statements that come after the awaited expression. See the
   * matching paragraph in `coro/SimCancellableDelay.h` for full
   * discussion.
   */
  void cancelWaiterOnly() noexcept {
    if (suspendedHandle_) {
      const auto h = suspendedHandle_;
      // Clear `suspendedHandle_` BEFORE `h.destroy()` so any reentrant
      // call observes a quiescent stopwatch — the frame being destroyed
      // may run destructors of locals that touch *this.
      suspendedHandle_ = {};
      // MUST be the last statement — no post-destroy access to *this
      // allowed (the destroyed frame may have owned *this or its members).
      h.destroy();
    }
  }

  /**
   * True iff state == RUNNING (strict — PAUSED returns false). A paused
   * stopwatch is still alive and will resume counting on `resume()`, but
   * it is not actively running. Callers asking "should this fire?" should
   * use `running()`; callers asking "is this stopwatch in use?" should
   * use `running() || paused()`.
   */
  [[nodiscard]] bool running() const noexcept {
    return state_ == State::RUNNING;
  }

  /** True iff state == PAUSED. */
  [[nodiscard]] bool paused() const noexcept {
    return state_ == State::PAUSED;
  }

  /** True iff state == EXPIRED. */
  [[nodiscard]] bool expired() const noexcept {
    return state_ == State::EXPIRED;
  }

  /**
   * Single-use awaitable returned by `expiration()` and `startOneShot()`.
   * Users typically don't name this type — they consume it inline via
   * `co_await stopwatch_.expiration()`. Same shape as the other coro
   * primitives' Awaitable types.
   */
  struct Awaitable {
    const folly::not_null<SimStopwatch*> self;

    // ----- Special members (Rule of Five) -----
    // The destructor below clears the stopwatch's suspended-waiter handle, so
    // the awaitable is no longer an aggregate. It is moved ONLY before
    // `await_suspend` installs the handle (simWaitFirst moves operands into
    // its tuple at construction; a bare `co_await` elides the move), so a
    // moved-from source clears nothing. Copy is deleted.
    explicit Awaitable(folly::not_null<SimStopwatch*> sw) noexcept : self(sw) {}
    Awaitable(Awaitable&&) noexcept = default;
    Awaitable& operator=(Awaitable&&) = delete;
    Awaitable(const Awaitable&) = delete;
    Awaitable& operator=(const Awaitable&) = delete;

    /**
     * Destroy-safe teardown. If the owning coroutine frame is destroyed while
     * suspended on this awaitable (e.g. `SimScopedTask::reset()`), clear the
     * stopwatch's suspended-waiter handle so the scheduled resume — which may
     * still be in flight — does not resume the freed frame (the resume lambda
     * no-ops when `suspendedHandle_` is empty). Does NOT destroy the frame (it
     * is already unwinding).
     *
     * This clears the stopwatch's single SHARED `suspendedHandle_`
     * unconditionally. Safe because the single-waiter invariant
     * (`await_suspend`'s XCHECK) plus the move-before-install rule above
     * guarantee the slot holds either nothing or *this* awaitable's handle —
     * never some other live waiter's. If a future change ever held a moved-from
     * awaitable across an install, or added a second await entry point, this
     * dtor would need to guard the clear on owning the slot.
     *
     * Deliberately does NOT cancel the scheduled resume: a stopwatch raced via
     * `simWaitFirst` keeps a losing timer counting (`cancelWaiterOnly`), and a
     * per-type destructor can't distinguish a bare-await teardown from a race
     * loser. So a bare-awaited stopwatch is destroy-safe ONLY if the owner also
     * cancels the stopwatch at teardown — which clears the scheduled resume.
     * The stopwatch's own destructor already requires this; see
     * `Llr::shutdown()`, which `cancel()`s every member timer.
     */
    ~Awaitable() noexcept {
      self->suspendedHandle_ = {};
    }

    /**
     * True if currently EXPIRED — the consumer resumes immediately without
     * suspending.
     */
    [[nodiscard]] bool await_ready() const noexcept {
      return self->state_ == State::EXPIRED;
    }

    /**
     * Suspend; install `h` as the waiter. When the scheduled resume fires
     * (or when start()/reset() is called from STOPPED with this waiter
     * already installed), the waiter is woken.
     *
     * XCHECK enforces the single-waiter contract (see class doc).
     */
    void await_suspend(std::coroutine_handle<> h) const noexcept {
      XCHECK(!self->suspendedHandle_)
          << "SimStopwatch::expiration await_suspend with an existing "
             "suspended waiter; would leak the previous coroutine frame";
      self->suspendedHandle_ = h;
    }

    /** No-op. The awaitable does not produce a value. */
    constexpr void await_resume() const noexcept {}

    /**
     * Tear down this awaitable's hold on the stopwatch. Used by the
     * `simWaitFirst` loser-cancel contract to drop a suspended waiter
     * when another operand wins the race. Forwards to
     * `SimStopwatch::cancelWaiterOnly` — leaves the stopwatch's RUNNING
     * state and scheduled resume intact so a losing operand in a multi-
     * stopwatch race does not silently stop timers that other rule call
     * sites are still relying on.
     *
     * Callers that want full teardown (e.g., owner shutdown) should call
     * `SimStopwatch::cancel()` directly, not through the awaitable.
     *
     * `const` because the Awaitable's own state doesn't change — only
     * `*self` is mutated, and `self` is a (const) pointer to a non-const
     * SimStopwatch.
     */
    void cancel() const noexcept {
      self->cancelWaiterOnly();
    }
  };

  /**
   * Returns an awaitable whose suspension wakes when the stopwatch reaches
   * `maxDuration`. If currently EXPIRED, the awaitable is immediately ready
   * (await_ready=true).
   *
   * If the stopwatch is STOPPED when expiration() is awaited, the consumer
   * suspends and stays suspended until either:
   *   - start() / reset() is called, then the scheduled resume fires, OR
   *   - cancel() is called (frame destroyed).
   *
   * Per the single-waiter contract, only one consumer may await at a time.
   */
  [[nodiscard(
      "SimStopwatch::expiration()'s return value must be co_await'd; "
      "discarding it creates an awaitable that never suspends")]]
  Awaitable expiration() noexcept {
    return Awaitable{this};
  }

  /**
   * One-shot helper: schedule a fresh resume in `duration` and return the
   * awaitable. Requires that no resume is scheduled: on a RUNNING stopwatch
   * (armed by `start()`, `reset()` or `resume()`, or left running by a lost
   * `simWaitFirst` race) call `stop()` or `cancel()` first, or the XCHECK
   * below aborts. Shares the underlying `suspendedHandle_` slot with
   * `expiration()` — only one waiter (across either entry point) may be in
   * flight at a time, also enforced by an XCHECK below.
   *
   * Use case: `LinkLayerScheduler`'s CtlOS spacing gate, whose wait (until
   * the next CtlOS may leave the wire) is recomputed on every arm, so no
   * fixed `maxDuration` fits.
   *
   * Asymmetry vs. `start()`: unlike `start()` (which XCHECKs from EXPIRED
   * and requires `reset()` to re-arm), `startOneShot()` re-arms from
   * EXPIRED as well as from STOPPED or PAUSED (discarding any saved
   * elapsed time). This matches the iterative consumer-loop pattern where
   * each iteration computes a fresh delay: after the previous awaitable
   * resolves, state is EXPIRED and the waiter slot has been cleared by the
   * resume lambda in `armScheduledResume`, so the next `startOneShot(...)`
   * call re-arms directly.
   */
  [[nodiscard(
      "SimStopwatch::startOneShot()'s return value must be co_await'd; "
      "discarding it creates an awaitable that never suspends")]]
  Awaitable startOneShot(Time duration) noexcept {
    // Re-targeting the deadline of an already-suspended waiter is silent
    // surprise; require the caller to drain the previous await before
    // arming a new one. Mirrors SimCancellableDelay::awaitDelay's
    // single-in-flight contract.
    XCHECK(!suspendedHandle_)
        << "SimStopwatch::startOneShot() called with a waiter already "
           "suspended on expiration(); would silently re-target its "
           "deadline. Drain the previous await first.";
    XCHECK(!scheduledResume_.isActive())
        << "SimStopwatch::startOneShot() called with a resume already "
           "scheduled; the previous awaitable was discarded without being "
           "awaited. Drain the previous await first, or call stop() / "
           "cancel() to clear the scheduled resume.";
    // No cancelScheduledResume() here — the XCHECK above guarantees no
    // resume is in-flight, so a cancel() call would be a no-op. Trusting
    // the always-on XCHECK keeps the body's intent visible.
    armRun(duration);
    return Awaitable{this};
  }

 private:
  enum class State : uint8_t { STOPPED, RUNNING, PAUSED, EXPIRED };

  /**
   * Re-zero elapsed, set the target duration, capture the run start time,
   * arm the scheduled resume, and transition to RUNNING. Shared prologue
   * for `start()` / `reset()` / `startOneShot()` so the four-line
   * invariant ("target/accumulated/runStart/arm/state") lives in one
   * place. Caller is responsible for any state-machine precondition
   * (e.g., `start()` XCHECKs not-EXPIRED) and for cancelling any prior
   * scheduled resume.
   */
  void armRun(Time duration) noexcept {
    targetDuration_ = duration;
    accumulatedElapsed_ = Time::fromNs(0);
    runStartTime_ = ctx_->now();
    armScheduledResume(duration);
    state_ = State::RUNNING;
  }

  /**
   * Schedule a resume in `delay`. Captures `this` raw; the lambda fires only
   * if not cancelled first via cancelScheduledResume(). On fire, transitions
   * to EXPIRED and resumes the waiter (if any).
   */
  void armScheduledResume(Time delay) noexcept {
    scheduledResume_ = ctx_->schedule(delay, [this] {
      scheduledResume_ = {};
      state_ = State::EXPIRED;
      if (suspendedHandle_) {
        const auto h = suspendedHandle_;
        suspendedHandle_ = {};
        // h.resume() may recursively run the resumed coroutine all the
        // way to its next suspension or completion. The resumed body
        // may destruct *this (e.g., when the SimStopwatch is owned by
        // the resumed coroutine's frame). State has been cleared
        // (scheduledResume_ = {}, suspendedHandle_ = {}) BEFORE resume
        // so that any reentrant call observes a quiescent stopwatch.
        // MUST be the last statement in this lambda — no post-resume
        // access to *this allowed.
        h.resume();
      }
    });
  }

  /**
   * Remove any in-flight scheduled resume from the PDES queue. After this,
   * the lambda will not fire. Safe to call multiple times and safe when no
   * resume is scheduled.
   */
  void cancelScheduledResume() noexcept {
    scheduledResume_.cancel();
    scheduledResume_ = {};
  }

  // Compile-time validation that the noexcept paths' assumptions hold. If a
  // future Scheduler refactor breaks any of these, we fail to compile here
  // rather than silently lose noexcept guarantees.
  static_assert(
      std::is_nothrow_default_constructible_v<Scheduler::EventHandle>);
  static_assert(std::is_nothrow_assignable_v<
                Scheduler::EventHandle&,
                Scheduler::EventHandle>);
  static_assert(
      noexcept(std::declval<const Scheduler::EventHandle&>().cancel()));
  static_assert(
      noexcept(std::declval<const Scheduler::EventHandle&>().isActive()));

  const folly::not_null<SimContext*> ctx_;
  const Time maxDuration_;

  State state_{State::STOPPED};

  // PDES event handle of the in-flight scheduled resume. Default-constructed =
  // no resume scheduled. Cleared by the resume lambda on fire, or by
  // cancelScheduledResume() before re-arming.
  Scheduler::EventHandle scheduledResume_{};

  // Suspended waiter; valid iff a coroutine awaits expiration() (or the
  // awaitable from startOneShot). Cleared by the resume lambda on fire,
  // or by cancel() (destroy).
  std::coroutine_handle<> suspendedHandle_{};

  // Configured budget for the current run. Set by armRun(); read by
  // resume() to compute remaining = target - elapsed.
  Time targetDuration_{};

  // Sum of elapsed across all RUNNING segments since the last
  // start/reset. Bumped on each pause() by (now - runStartTime_);
  // zeroed on start/reset (via armRun).
  Time accumulatedElapsed_{};

  // Sim-time at which the most recent RUNNING segment began. Set by
  // armRun/resume; read by pause() to compute the elapsed delta.
  Time runStartTime_{};
};

// SimWaitFirst's cancellation and teardown adapters for
// SimStopwatch::Awaitable live alongside the adapters for
// SimSignal::Awaitable, SimCancellableDelay::Awaitable and
// SimEvent::Awaitable in SimWaitFirst.h's `detail` namespace.

} // namespace packsim::coro
