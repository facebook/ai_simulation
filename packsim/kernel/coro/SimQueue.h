/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>
#include <cstddef>
#include <deque>
#include <type_traits>
#include <utility>

#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>

#include "packsim/kernel/coro/ResumeMode.h"
#include "packsim/kernel/coro/WaiterState.h"
#include "packsim/kernel/sim-context.h"

namespace packsim::coro {

/**
 * An awaitable FIFO queue for simulation coroutines.
 *
 * Designed for the TX-pump pattern where a coroutine loops draining a queue:
 *
 *     SimTask txPump(SimContext* ctx, SimQueue<Packet*>& queue) {
 *       while (true) {
 *         auto* pkt = co_await queue.pop();
 *         co_await simDelay(ctx, computeTxTime(pkt));
 *         onTxComplete(pkt);
 *       }
 *     }
 *
 * Producers enqueue items:
 *
 *     queue.push(pkt);  // resumes the consumer if suspended
 *
 * CONTEXT-LOCAL: All push/pop calls MUST happen on the same SimContext
 * passed at construction. The queue MUST outlive any in-flight `co_await`
 * against it AND any resume callbacks already enqueued in the PDES
 * (Parallel Discrete Event Simulation) queue.
 *
 * STACK-DEPTH SAFETY: default `ResumeMode::DEFERRED` posts each consumer
 * resume through scheduleNow — one PDES dispatch per push instead of one
 * stack frame, bounding stack depth on long producer chains. Same-tick
 * semantics are preserved. `ResumeMode::DIRECT` is WITHOUT
 * FRAMEWORK-ENFORCED DEPTH LIMIT — the caller is responsible for bounding
 * the chain; a producer that pushes N items under DIRECT mode stacks N
 * coroutine frames before the first pop returns. Use DIRECT only when N
 * is bounded by a compile-time or statically-known runtime constraint.
 *
 * Teardown: if a consumer is suspended on pop() and the queue owner needs
 * to destroy the primitive, call cancel() first. Items already queued are
 * preserved — cancel() only affects the suspended consumer. The awaitable
 * does not detach itself if the consumer's frame is destroyed any other way
 * (e.g. `SimScopedTask::reset()`), so the queue would keep the freed handle;
 * see "Teardown contract" in SimScopedTask.h.
 *
 * Requirements on T:
 *   - std::is_nothrow_move_constructible_v<T> — enforced via static_assert.
 *     Move-throwing types would let await_resume strand a moved-from item
 *     at the front of the queue when its pop_front step is skipped.
 *
 * Deferred-resume bookkeeping (event-handle tracking, race-safe cancel,
 * single-resume-per-pending-window guard) lives in WaiterState and is
 * shared with SimSignal/SimFuture.
 */
template <typename T>
class SimQueue {
  // If T's move throws, await_resume would leave the front element
  // moved-from and skip the pop_front — next consumer reads a corrupt
  // item. Require nothrow-move so the consume path can't observe that
  // state.
  static_assert(
      std::is_nothrow_move_constructible_v<T>,
      "SimQueue<T> requires T to be nothrow-move-constructible so "
      "await_resume's move-then-pop sequence can't strand a moved-from "
      "item at the front of the queue.");

 public:
  /**
   * @param ctx  Simulation context.
   * @param mode See STACK-DEPTH SAFETY above. Defaults to
   *             ResumeMode::DEFERRED.
   */
  explicit SimQueue(
      folly::not_null<SimContext*> ctx,
      const ResumeMode mode = ResumeMode::DEFERRED)
      : waiter_(ctx, mode) {}

  SimQueue(const SimQueue&) = delete;
  SimQueue& operator=(const SimQueue&) = delete;
  SimQueue(SimQueue&&) = delete;
  SimQueue& operator=(SimQueue&&) = delete;

  /**
   * Destructor. Always-on XCHECK catches the "SimQueue destroyed while a
   * consumer is still suspended on pop()" lifetime bug — the suspended
   * coroutine frame would leak otherwise.
   */
  ~SimQueue() noexcept {
    XCHECK(!waiter_.has()) << "SimQueue destroyed with a suspended consumer; "
                              "coroutine frame leaked";
  }

  /**
   * Awaitable handle returned by pop().
   */
  struct Awaitable {
    SimQueue& q;

    /** True if items are available — consumer resumes immediately. */
    constexpr bool await_ready() const noexcept {
      return !q.items_.empty();
    }

    void await_suspend(std::coroutine_handle<> h) const noexcept {
      XCHECK(!q.waiter_.has())
          << "SimQueue already has a suspended consumer; concurrent "
             "consumers are not supported";
      q.waiter_.install(h);
    }

    /**
     * Pop the front item and return it to the resumed coroutine.
     *
     * Unconditionally `noexcept`: the class-level static_assert already
     * requires `is_nothrow_move_constructible_v<T>`.
     */
    T await_resume() noexcept {
      XDCHECK(!q.items_.empty());
      T item = std::move(q.items_.front());
      q.items_.pop_front();
      return item;
    }
  };

  /** Await the next item. Returns immediately if items are available. */
  [[nodiscard(
      "SimQueue::pop()'s return value must be co_await'd; discarding it "
      "creates an awaitable that never suspends or pops an item")]]
  Awaitable pop() noexcept {
    return Awaitable{*this};
  }

  /**
   * Enqueue an item. Resumes the suspended consumer if one exists.
   * Terminates on `emplace_back` allocation OOM or scheduler OOM in
   * `WaiterState::notify` (see SimTask.h, "Error model").
   *
   * @param item The item to enqueue; moved into the queue.
   */
  void push(T item) noexcept {
    items_.emplace_back(std::move(item));
    waiter_.notify();
  }

  [[nodiscard]] bool empty() const noexcept {
    return items_.empty();
  }

  [[nodiscard]] std::size_t size() const noexcept {
    return items_.size();
  }

  /**
   * True if a coroutine is suspended awaiting an item (incl. across the
   * deferred-resume window in ResumeMode::DEFERRED mode).
   */
  [[nodiscard]] bool hasWaiter() const noexcept {
    return waiter_.has();
  }

  /**
   * Tear down the suspended consumer (if any) without delivering an item.
   * Removes any pending deferred resume from the PDES queue, then destroys
   * the suspended coroutine frame. Safe to call multiple times and safe
   * when no consumer is suspended (no-op).
   *
   * Does NOT clear queued items — a later consumer can still pop them.
   */
  void cancel() noexcept {
    waiter_.cancel();
  }

 private:
  // WaiterState managing the suspended consumer coroutine.
  detail::WaiterState waiter_;
  // FIFO queue of pending items.
  std::deque<T> items_;
};

} // namespace packsim::coro
