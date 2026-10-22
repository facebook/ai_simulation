/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <vector>

#include <folly/memory/not_null.h>

#include "packsim/kernel/sim-context.h"

namespace packsim::coro {

/**
 * N-1-N collective barrier with a dedicated coordinator context.
 *
 * CROSS-CONTEXT: thread-safe by design (aggregation single-threaded on
 * coord; cross-thread arrivals via lock-free MPSC inbox). coordCtx_
 * MUST outlive every SimCoordRoutedBarrier and every in-flight callback.
 *
 * DESTRUCTION CONTRACT: SimCoordRoutedBarrier MUST be destroyed only
 * AFTER `Scheduler::run()` returns and no participant thread is mid-
 * `await_suspend`. Violating this contract is undefined behavior — the
 * destructor's `inFlight_ == 0` check has a narrow window where a
 * participant has entered `await_suspend` but not yet retired its
 * `inFlight_.fetch_add`, so the dtor can observe zero, free `this`,
 * and the participant's subsequent `fetch_add` becomes UAF. See
 * "Lifetime" below for the full rationale.
 *
 * Coordinates N participant coroutines (potentially on N different
 * SimContexts) so they all resume together once every participant has
 * arrived. Aggregation state lives on a single dedicated SimContext (the
 * "coord context"); arrivals from any thread route through coord's
 * lock-free MPSC inbox via scheduleRemote, are aggregated single-threaded,
 * and on the Nth arrival coord dispatches release callbacks back to each
 * participant via a second scheduleRemote hop.
 *
 * The coord context is the only piece of cross-instance shared state. It
 * can be reused across multiple SimCoordRoutedBarrier instances and other
 * future fan-in/fan-out primitives — every coordination primitive that routes
 * through it gets single-threaded state mutation for free, since PDES
 * pins each context to one worker thread.
 *
 * Routing & timing
 * ----------------
 *   1. Participant calls `co_await barrier.arrive(participantCtx)`.
 *      Internally:
 *      coordCtx_->scheduleRemote(&SimCoordRoutedBarrier::postArrival, this,
 *                                 participantCtx, cb)
 *      → +1 × lookahead (cross-context hop into coord).
 *   2. coord runs postArrival single-threaded; appends to arrivals_.
 *   3. On the Nth arrival, coord loops arrivals_ and does
 *      ctx->scheduleRemote(cb) for each participant
 *      → +1 × lookahead (cross-context hop back).
 *   Total per-barrier latency: 2 × lookahead.
 *
 *   arrivals_ is cleared after dispatch, so the same
 *   SimCoordRoutedBarrier can be reused across iteration loops without
 *   reconstruction.
 *
 * Lifetime
 * --------
 *   coordCtx_ MUST outlive every SimCoordRoutedBarrier that uses it AND every
 *   in-flight callback. There is no cancel(); a participant that fails
 *   to arrive leaves arrivals_ permanently short and the other
 *   participants' coroutines never resume. The destructor XCHECKs both
 *   `inFlight_ == 0` AND `arrivals_.empty()`, in that order:
 *     - inFlight_ catches the narrower window where K participants have
 *       suspended (await_suspend queued postArrival on coord) but coord
 *       hasn't run those callbacks yet — arrivals_ is still empty, but
 *       the queued lambdas hold raw `this` and would UAF on next tick.
 *     - arrivals_ catches "K of N postArrivals already aggregated."
 *   The order matters: arrivals_ is mutated only on coord's thread, so
 *   reading it from the destructor's thread is only race-free once
 *   inFlight_ is observed at zero (which acquire-synchronizes with every
 *   postArrival's release fetch_sub and thus with every emplace_back/
 *   clear that ran on coord). Both checks together cover every state
 *   where in-flight callbacks reference the barrier, so we abort loudly
 *   instead of silently corrupting memory.
 *
 *   DESTRUCTION CONTRACT: SimCoordRoutedBarrier MUST be destroyed only after
 *   the caller-owned `Scheduler::run()` has returned AND no further
 *   worker is queued to touch this barrier. Destruction during active
 *   scheduling is undefined behavior. The dtor catches every contract
 *   violation that the barrier's atomics make observable (queued-but-
 *   unfired postArrival via `inFlight_`; partial aggregation via
 *   `arrivals_`). The only window left open is participant-thread-
 *   not-yet-fetched_add: if a participant has entered `await_suspend`
 *   but has not yet executed the `inFlight_.fetch_add` instruction
 *   AND the destructor runs on a thread with no happens-before to it,
 *   the destructor could observe `inFlight_ == 0`, pass both XCHECKs,
 *   free `this`, and the participant's subsequent fetch_add would
 *   UAF. This is only reachable when the destructor runs while
 *   `Scheduler::run()` is still active — exactly the documented
 *   contract violation; `Scheduler::run()` returning provides the
 *   happens-before that closes the window for compliant callers.
 *
 * Thread-safety
 * -------------
 *   arrivals_ is mutated only from postArrival running on coordCtx_'s
 *   owning thread (PDES guarantees per-context single-thread execution),
 *   so no atomics or mutex are needed. Cross-thread arrival traffic is
 *   funneled through coordCtx_'s lock-free MPSC inbox by scheduleRemote.
 *
 * Compared to CollectiveBarrier (endpoint/runtime/collective-barrier.h)
 * ---------------------------------------------------------------------
 *   Same N-1-N routing pattern and 2 × lookahead latency. Differences:
 *     - Coord is a dedicated SimContext, NOT a participant — no
 *       "rank 0 is special" implicit contract.
 *     - One SimCoordRoutedBarrier instance, not N participant-local instances.
 *     - No workload config / rank ID dependency — arrivals are counted
 *       anonymously. Misbehaving participants double-arriving silently
 *       corrupt the count; layer rank-ID checks in a wrapper if needed.
 *     - No dependency on packsim modules other than the simulation context
 *       (`kernel/sim-context.h`).
 *
 * Example — single barrier, N participants on N contexts:
 *
 *     SimContext coordCtx(&scheduler, "coord");
 *     SimCoordRoutedBarrier barrier(&coordCtx, kNumParticipants);
 *
 *     coro::SimTask participant(
 *         SimContext* myCtx, SimCoordRoutedBarrier* barrier) {
 *       co_await runLocalWork(myCtx);
 *       // Resumes on myCtx after all N participants have arrived.
 *       co_await barrier->arrive(myCtx);
 *       doNextPhase();
 *     }
 *
 *     for (auto* ctx : participantCtxs) {
 *       participant(ctx, &barrier);
 *     }
 *     scheduler.run();
 *     // After run() returns, all coord callbacks have drained. Safe to
 *     // destroy `barrier` and then `coordCtx` (in this order — coordCtx
 *     // MUST outlive every barrier that uses it).
 *
 * No simBarrier() factory function (unlike sibling simDelay/simScopedDelay/
 * simScheduleOn): SimCoordRoutedBarrier is a long-lived, non-copyable,
 * non-movable stateful object — construct it directly. Sibling factories
 * return single-use awaitables, which is a different pattern.
 */
class SimCoordRoutedBarrier {
 public:
  /**
   * @param coordCtx         Coordinator SimContext. Single-threaded host
   *                         for arrivals_ aggregation. MUST outlive this
   *                         barrier and every in-flight callback.
   * @param numParticipants  Number of arrivals required per release.
   *                         MUST be > 0.
   *
   * @throw std::invalid_argument if numParticipants == 0.
   */
  SimCoordRoutedBarrier(
      folly::not_null<SimContext*> coordCtx,
      const uint32_t numParticipants);

  // Non-copyable, non-movable: arrive() captures `this` in scheduleRemote
  // callbacks, so moving would orphan in-flight aggregation.
  SimCoordRoutedBarrier(const SimCoordRoutedBarrier&) = delete;
  SimCoordRoutedBarrier& operator=(const SimCoordRoutedBarrier&) = delete;
  SimCoordRoutedBarrier(SimCoordRoutedBarrier&&) = delete;
  SimCoordRoutedBarrier& operator=(SimCoordRoutedBarrier&&) = delete;

  /**
   * Loud failure on mid-aggregation destruction. Two-part check:
   *   - inFlight_ == 0:    catches "K participants suspended, postArrival
   *                        callbacks still queued on coord but not yet
   *                        fired."
   *   - arrivals_.empty(): catches "K of N already aggregated on coord."
   *
   * Both required: any queued-but-unfired postArrival holds raw `this`
   * and would UAF on the next coord tick. Always-on XCHECK (not XDCHECK)
   * so opt builds catch it too.
   */
  ~SimCoordRoutedBarrier() noexcept;

  /**
   * Awaitable returned by arrive(). co_await suspends the current
   * coroutine; resume happens on `participantCtx` after all
   * numParticipants have arrived.
   */
  struct Awaitable {
    // Non-owning back-reference to the barrier; lifetime is managed by
    // the caller (see Lifetime). The Awaitable's lifetime is bounded by
    // the surrounding `co_await`, which is bounded by the participant's
    // SimTask, so this raw pointer is always valid during use.
    const folly::not_null<SimCoordRoutedBarrier*> barrier;

    // Context the suspended coroutine resumes on after all N have arrived.
    const folly::not_null<SimContext*> participantCtx;

    Awaitable(
        folly::not_null<SimCoordRoutedBarrier*> barrierIn,
        folly::not_null<SimContext*> participantCtxIn) noexcept
        : barrier(barrierIn), participantCtx(participantCtxIn) {}

    // One-shot value type: constructed by arrive() via guaranteed copy-
    // elision (C++17 prvalue → object init), consumed in place by
    // co_await. Mirrors SimCoordRoutedBarrier's Rule-of-Five — no copy,
    // no move, no assign at this layer either. Misuse (storing in a
    // container, wrapping in a generic adapter, etc.) fails loudly at
    // compile time.
    Awaitable(const Awaitable&) = delete;
    Awaitable& operator=(const Awaitable&) = delete;
    Awaitable(Awaitable&&) = delete;
    Awaitable& operator=(Awaitable&&) = delete;
    ~Awaitable() = default;

    /** @return Always false: arrive always suspends. */
    constexpr bool await_ready() const noexcept {
      return false;
    }

    /**
     * Suspend and post arrival to coord. Terminates on scheduler OOM
     * (see SimTask.h, "Error model"); `noexcept` means `std::terminate` runs at
     * the failing site before any destructor would observe a partial
     * state (`inFlight_` incremented but no postArrival queued).
     *
     * @param h  Handle to the suspended coroutine frame.
     */
    void await_suspend(const std::coroutine_handle<> h) const noexcept;

    /** No-op. SimCoordRoutedBarrier doesn't produce a value. */
    constexpr void await_resume() const noexcept {}
  };

  /**
   * Arrive at the barrier and obtain an awaitable. co_await it to
   * suspend until all numParticipants have arrived.
   *
   * @param participantCtx  Context to resume the coroutine on. MUST
   *                        outlive the eventual resume.
   *
   * @return  An Awaitable that must be `co_await`-ed; suspends the
   *          calling coroutine until all numParticipants have arrived
   *          (resumes 2 × lookahead later via the coord round-trip).
   */
  // Non-const intentionally: arrive() itself only constructs an Awaitable,
  // but the subsequent co_await starts a mutation chain (postArrival
  // mutates arrivals_). Marking arrive() const would advertise a purity
  // that doesn't exist.
  [[nodiscard(
      "SimCoordRoutedBarrier::arrive() returns an awaitable; discarding it creates a "
      "barrier participant that never arrives")]]
  Awaitable arrive(const folly::not_null<SimContext*> participantCtx) noexcept {
    return Awaitable{this, participantCtx};
  }

 private:
  /**
   * Aggregate one arrival on coordCtx_'s thread. On the Nth call,
   * dispatches release callbacks to each participant and clears the
   * arrivals buffer.
   *
   * Runs on coordCtx_'s owning thread via scheduleRemote (per-context
   * single-thread guarantee), so arrivals_ mutation is race-free.
   *
   * noexcept: a partial dispatch (some resume callbacks queued, others
   * not) would leave the barrier jammed and some participants unresumed.
   * Rather than try to rearm-and-rethrow halfway through (with N-K
   * coroutines leaking either way), we treat any failure inside as
   * non-recoverable: bad_alloc from arrivals_.emplace_back or from
   * scheduleRemote propagates as std::terminate, producing an immediate
   * crash with a stack trace pointing at the actual allocation site.
   * Consistent with how the rest of PackSim treats simulator-level
   * allocation failure.
   *
   * @param participantCtx  The participant's context to resume on.
   * @param resumeCb        The resume callback (target of the second
   *                        scheduleRemote hop on release).
   */
  void postArrival(
      folly::not_null<SimContext*> participantCtx,
      std::function<void()> resumeCb) noexcept;

  // One pending arrival waiting on coord for the Nth-arrival trigger.
  // Stored as a struct (not std::pair) to preserve the folly::not_null
  // contract on `participantCtx` end-to-end and to give the fields
  // self-documenting names.
  struct PendingArrival {
    // Context the participant should resume on.
    folly::not_null<SimContext*> participantCtx;

    // Resume callback (target of the second scheduleRemote hop on release).
    std::function<void()> resumeCb;
  };

  // Coordinator SimContext. arrivals_ is mutated only on this context's
  // owning PDES worker thread.
  const folly::not_null<SimContext*> coordCtx_;

  // Number of arrivals required per release. Validated > 0 in the ctor.
  const uint32_t numParticipants_;

  // Aggregation buffer. Mutated only from postArrival on coordCtx_'s
  // owning thread.
  std::vector<PendingArrival> arrivals_;

  // Count of postArrival callbacks queued on coord but not yet executed.
  // Incremented in await_suspend (any participant thread) before
  // scheduleRemote queues the event; decremented at the end of
  // postArrival on coord's thread. Atomic because the destructor and
  // await_suspend run on different threads — `inFlight_` exists ONLY
  // for the destructor's quiescence check, NOT to synchronize between
  // await_suspend and postArrival (that happens-before is provided by
  // PDES's cross-context inbox in scheduleRemote).
  std::atomic<uint32_t> inFlight_{0};
};

} // namespace packsim::coro
