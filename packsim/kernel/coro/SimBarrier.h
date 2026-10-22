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
#include <memory>
#include <utility>

#include <folly/concurrency/DynamicBoundedQueue.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/sim-context.h"

namespace packsim::coro {

/**
 * Lock-free N-way collective barrier. Participants synchronize via an
 * atomic counter and a bounded lock-free MPSC queue; no dedicated
 * coordinator context is required.
 *
 * CROSS-CONTEXT: thread-safe by design (lock-free MPSC `waiters_` queue
 * + acq_rel atomic `arrived_` counter; no per-instance shared context).
 * Participants on different SimContexts may call `arrive()` concurrently
 * from different PDES worker threads.
 *
 * Lifetime is managed by `std::shared_ptr` (the class derives
 * `std::enable_shared_from_this`). `arrive()` returns an Awaitable that
 * holds a strong reference to the barrier — the barrier stays alive
 * through every participant's suspension period. When all participants
 * have resumed and dropped their references, the barrier destructs
 * automatically. Construct via `std::make_shared<SimBarrier>(N)`;
 * calling `arrive()` on a stack-allocated SimBarrier throws
 * `std::bad_weak_ptr` (free guard against the most common misuse).
 *
 * Routing & timing
 * ----------------
 *   1. Participant calls `co_await barrier->arrive(myCtx)`. await_suspend
 *      enqueues (participantCtx, resume_cb) into the lock-free MPSC queue,
 *      then atomically increments the arrival counter.
 *   2. The participant whose fetch_add returns N is the dispatcher. For
 *      each (participantCtx, resume_cb) pair in the queue, the dispatcher
 *      calls participantCtx->scheduleRemote(resume_cb) — one cross-context
 *      hop per participant, +1 × lookahead. All resumes fire at sim-time
 *      T+lookahead.
 *
 *   Total per-barrier latency: 1 × lookahead (vs the 2 × lookahead of
 *   a coord-routed N-1-N design).
 *
 *   The barrier is reusable across iterations: the dispatcher resets
 *   `arrived_` to 0 after dispatch. The reset cannot race with K+1
 *   arrivals because PackSim's PDES is frame-based — cross-context
 *   resume events are buffered in per-worker eventBacklog queues and
 *   only flushed into priority queues at the next frame boundary.
 *
 * Thread-safety
 * -------------
 *   Multi-threaded PDES is safe by construction. Multiple participants
 *   can call arrive() concurrently from different worker threads. Race
 *   correctness rests on a single memory-ordering invariant:
 *
 *       waiters_.enqueue(...) is sequenced-before
 *       arrived_.fetch_add(...) on each participant's thread.
 *
 *   The dispatcher's fetch_add returning N synchronizes (acq_rel) with
 *   all prior fetch_adds, so all N enqueues are visible when the
 *   dispatcher drains the queue.
 *
 *   Dispatcher identity (which participant becomes "last") is
 *   non-deterministic in multi-threaded PDES. The dispatch order at
 *   sim-time T+lookahead follows from PDES's own tie-breaking on the
 *   resume events.
 *
 * No cancel by design
 * -------------------
 *   A participant that fails to arrive jams the barrier; the other
 *   participants suspend forever, leaking their coroutine frames. Same
 *   constraint as `packsim::CollectiveBarrier`. If an iteration needs
 *   to abort safely, layer cancellation on top.
 *
 * Compared to CollectiveBarrier (endpoint/runtime/collective-barrier.h)
 * ---------------------------------------------------------------------
 *   SimBarrier is a flat N-way self-coordinating barrier; CollectiveBarrier
 *   remains the N-1-N coord-routed design with rank-aware leader logic
 *   (rank-ID validation, per-rank callback dispatch). Different patterns:
 *     - Use SimBarrier for flat synchronization across N peers.
 *     - Use CollectiveBarrier for rank-aware coordination tied to a
 *       workload schedule.
 *
 * Example — single barrier, N participants on N contexts:
 *
 *     auto barrier = std::make_shared<SimBarrier>(kNumParticipants);
 *
 *     coro::SimTask participant(SimContext* myCtx,
 *                                std::shared_ptr<SimBarrier> barrier) {
 *       co_await runLocalWork(myCtx);
 *       co_await barrier->arrive(myCtx);  // resumes after all N arrive
 *       doNextPhase();
 *     }
 *
 *     for (auto* ctx : participantCtxs) {
 *       participant(ctx, barrier);
 *     }
 *     scheduler.run();
 *     // After run() returns, participants have completed and dropped their
 *     // shared_ptr references. Local `barrier` drops here; refcount → 0;
 *     // barrier destructs.
 */
class SimBarrier : public std::enable_shared_from_this<SimBarrier> {
 public:
  /**
   * Construct a barrier expecting `numParticipants` arrivals per release.
   * MUST be constructed via `std::make_shared<SimBarrier>(N)`; calling
   * `arrive()` on a SimBarrier not owned by a shared_ptr throws
   * `std::bad_weak_ptr` (the `shared_from_this` guard).
   *
   * @param numParticipants  Number of arrivals required per release.
   *                         MUST be > 0.
   *
   * @throw std::invalid_argument if numParticipants == 0.
   */
  explicit SimBarrier(const uint32_t numParticipants);

  // Non-copyable, non-movable: copying would split aggregation state and
  // confuse the shared_ptr/enable_shared_from_this invariant.
  SimBarrier(const SimBarrier&) = delete;
  SimBarrier& operator=(const SimBarrier&) = delete;
  SimBarrier(SimBarrier&&) = delete;
  SimBarrier& operator=(SimBarrier&&) = delete;

  /**
   * Destructor invariant: the barrier should only destruct after all
   * participants have arrived and resumed (refcount → 0). At that point
   * `arrived_` must be 0 and `waiters_` must be empty. A non-empty
   * waiters queue or non-zero counter at destruction means a participant
   * suspended on the barrier had its shared_ptr dropped without arriving
   * — programmer error. XCHECK fires loudly so misuse aborts visibly.
   */
  ~SimBarrier() noexcept;

  /**
   * Awaitable returned by arrive(). Holds a strong reference to the
   * barrier so the barrier stays alive through await_suspend AND through
   * the entire suspension period (Awaitable lives in the coroutine frame
   * until the resume fires). co_await suspends the participant; resume
   * happens on `participantCtx` after all numParticipants have arrived.
   */
  struct Awaitable {
    // Strong ref → keeps the barrier alive across the suspension.
    std::shared_ptr<SimBarrier> barrier;
    folly::not_null<SimContext*> participantCtx;

    Awaitable(
        std::shared_ptr<SimBarrier> barrierIn,
        folly::not_null<SimContext*> participantCtxIn) noexcept
        : barrier(std::move(barrierIn)), participantCtx(participantCtxIn) {}

    // One-shot value type: constructed by arrive() via guaranteed
    // copy-elision (C++17 prvalue → object init), consumed in place by
    // co_await. Mirrors SimBarrier's Rule-of-Five — no copy, no move,
    // no assign at this layer either. Misuse (storing in a container,
    // wrapping in a generic adapter, etc.) fails loudly at compile
    // time instead of silently degrading shared_ptr move to copy.
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
     * Suspend and post the arrival.
     *
     * INVARIANT: enqueue BEFORE fetch_add. The dispatcher (whose
     * fetch_add returns N) will drain the queue and must see all N
     * entries; sequencing the enqueue before the fetch_add on each
     * participant's thread ensures this via the queue's MPSC ordering
     * combined with the atomic's acq_rel synchronization.
     *
     * noexcept: per the framework exception policy (SimTask.h, "Error model"),
     * internal failures terminate at the failing site rather than
     * unwinding into a partially-suspended coroutine. The two real
     * allocation sites here are (1) `WaiterEntry`'s `std::function`
     * capture (SBO usually covers a single coroutine_handle, but is
     * implementation-defined) and (2) scheduleRemote's internal
     * allocation. DMPSCQueue is pre-allocated at construction and
     * does not allocate per-enqueue.
     *
     * @param h  Handle to the suspended coroutine frame.
     */
    void await_suspend(const std::coroutine_handle<> h) const noexcept;

    /** No-op. SimBarrier doesn't produce a value. */
    constexpr void await_resume() const noexcept {}
  };

  /**
   * Arrive at the barrier. co_await the returned Awaitable to suspend
   * until all numParticipants have arrived; resume happens on
   * `participantCtx` 1 × lookahead after the dispatcher fires.
   *
   * @param participantCtx  Context to resume the coroutine on. MUST
   *                        outlive the eventual resume.
   *
   * @return  An Awaitable that must be `co_await`-ed; suspends the
   *          calling coroutine until all numParticipants have arrived.
   *
   * @throw std::bad_weak_ptr if `shared_from_this()` fails. Two
   *                          causes: (1) the barrier was not
   *                          constructed via `std::make_shared` (the
   *                          common stack-allocation guard), or (2)
   *                          all `std::shared_ptr` references were
   *                          released and `arrive()` was invoked from
   *                          a callback still holding a raw pointer.
   */
  // Not noexcept (vs sibling SimCoordRoutedBarrier::arrive() which is
  // noexcept): shared_from_this() throws std::bad_weak_ptr when the
  // barrier wasn't built via std::make_shared — this is the documented
  // stack-allocation guard, not a hidden failure mode.
  [[nodiscard(
      "SimBarrier::arrive() returns an awaitable; discarding it creates a "
      "barrier participant that never arrives")]]
  Awaitable arrive(const folly::not_null<SimContext*> participantCtx) {
    return Awaitable{shared_from_this(), participantCtx};
  }

 private:
  // Internal queue entry. ctx is a raw SimContext* (not
  // folly::not_null) so this struct stays default-constructible —
  // required by DMPSCQueue::dequeue(T&), which writes into a
  // pre-constructed slot reused across dispatcher iterations. The
  // not_null guarantee from arrive()'s API boundary is unwrapped at
  // exactly one site (the enqueue in await_suspend); by the time the
  // dispatcher reads ctx, the pointer has already been validated
  // non-null at the call site.
  struct WaiterEntry {
    // Context the participant should resume on. Validated non-null at
    // the API boundary (see comment block above) before the unwrap from
    // folly::not_null<SimContext*> at the enqueue site.
    SimContext* ctx{};

    // Resume callback ([h]{ h.resume(); }) the dispatcher hands to
    // ctx->scheduleRemote on the Nth arrival.
    std::function<void()> resumeCb;
  };

  const uint32_t numParticipants_;

  // Atomic arrival counter. Reset to 0 by the dispatcher after each
  // release. Reset is race-free because PackSim's frame-based PDES
  // buffers cross-context resume events until the next frame boundary,
  // so K+1 arrivals cannot race with K's reset.
  std::atomic<uint32_t> arrived_{0};

  // Bounded lock-free MPSC queue. Capacity = numParticipants_; multiple
  // producers (participants) enqueue concurrently, single consumer
  // (dispatcher) drains. MayBlock=false selects the busy-spin overflow
  // path (asm_volatile_pause) over futex-wait — fine here because
  // correct usage never exceeds capacity (at most N concurrent enqueues
  // per iteration; dispatcher drains before the next iteration races).
  folly::DMPSCQueue<WaiterEntry, /*MayBlock=*/false> waiters_;
};

} // namespace packsim::coro
