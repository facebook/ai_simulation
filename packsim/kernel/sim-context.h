/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <utility>

#include <folly/CppAttributes.h>

#include "packsim/kernel/pdes/SimulationContext.h"
#include "packsim/kernel/random.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/time.h"
#include "packsim/kernel/trace.h"

namespace packsim {

struct SimContext : public ::packsim::pdes::SimulationContext {
  using Parent = ::packsim::pdes::SimulationContext;
  explicit SimContext(
      Scheduler* FOLLY_NONNULL scheduler,
      const std::string& name,
      uint64_t seed,
      Tracer::ConditionalMap* conditionalMap)
      : Parent(name),
        random(seed),
        tracer(this, conditionalMap),
        scheduler_(scheduler) {
    scheduler_->registerContext(this);
  }

  explicit SimContext(Scheduler* FOLLY_NONNULL scheduler, uint64_t seed)
      : Parent(), random(seed), tracer(this, nullptr), scheduler_(scheduler) {
    scheduler_->registerContext(this);
  }

  explicit SimContext(
      Scheduler* FOLLY_NONNULL scheduler,
      const std::string& name)
      : SimContext(scheduler, name, 0, nullptr) {}

  Time now() const {
    return Time(nowFs());
  }

  /**
   * Schedule `std::invoke(func, std::move(args)...)` on this context, `delay`
   * after `Scheduler::timeNow()` (see `Scheduler::scheduleAfter`). From
   * another context while the engine runs, `delay` must be at least the
   * lookahead time; `scheduleRemote` does that.
   *
   * @param delay  Offset from `Scheduler::timeNow()`.
   * @param func   Callable, function pointer or member-function pointer.
   * @param args   Arguments stored in the event and passed to `func` as
   *               rvalues, so `func` takes each by value or by `const&`, never
   *               by non-const reference (see `Scheduler::scheduleAfter`).
   *
   * @return  Handle that can cancel the event before it runs.
   *
   * @throw std::runtime_error if the event time is before this context's
   *        current time, or if called while the engine runs from a thread
   *        outside the PDES pool.
   */
  template <typename TFunc, typename... TArgs>
  Scheduler::EventHandle
  schedule(Time const& delay, TFunc&& func, TArgs&&... args) {
    return scheduler_->scheduleAfter(
        this, delay, std::forward<TFunc>(func), std::forward<TArgs>(args)...);
  }

  /**
   * Schedule `std::invoke(func, std::move(args)...)` on this context at
   * `Scheduler::timeNow()`. Call it from this context or while the engine is
   * not running; from another context use `scheduleRemote`.
   *
   * @param func  Callable, function pointer or member-function pointer.
   * @param args  Arguments stored in the event and passed to `func` as
   *              rvalues, so `func` takes each by value or by `const&`, never
   *              by non-const reference (see `Scheduler::scheduleAfter`).
   *
   * @return  Handle that can cancel the event before it runs.
   *
   * @throw std::runtime_error if that time is before this context's current
   *        time, or if called while the engine runs from a thread outside
   *        the PDES pool.
   */
  template <typename TFunc, typename... TArgs>
  Scheduler::EventHandle scheduleNow(TFunc&& func, TArgs&&... args) {
    return scheduler_->scheduleAfter(
        this, Time(0), std::forward<TFunc>(func), std::forward<TArgs>(args)...);
  }

  /**
   * Schedule `std::invoke(func, std::move(args)...)` on this context one
   * lookahead after `Scheduler::timeNow()`, the smallest delay that is safe
   * when the caller runs on another context.
   *
   * This coordinates events across all ranks of a collective without causality
   * paradoxes: one rank (rank 0, for example) acts as leader and the others
   * send their events to it; once the leader has them all, it triggers the
   * next iteration by sending events back. No mutexes or other global
   * synchronization are needed, because the global scheduler orders the
   * workload-level events across ranks.
   *
   * @param func  Callable, function pointer or member-function pointer.
   * @param args  Arguments stored in the event and passed to `func` as
   *              rvalues, so `func` takes each by value or by `const&`, never
   *              by non-const reference (see `Scheduler::scheduleAfter`).
   *
   * @return  Handle that can cancel the event before it runs.
   *
   * @throw std::runtime_error if called while the engine runs from a thread
   *        outside the PDES pool.
   */
  template <typename TFunc, typename... TArgs>
  Scheduler::EventHandle scheduleRemote(TFunc&& func, TArgs&&... args) {
    return scheduler_->scheduleAfter(
        this,
        getLookaheadTime(),
        std::forward<TFunc>(func),
        std::forward<TArgs>(args)...);
  }

  Time getTimeUntilEvent(const Scheduler::EventHandle& event) {
    return scheduler_->getTimeUntilEvent(event);
  }

  Time getLookaheadTime() const {
    return scheduler_->getLookaheadTime();
  }

  // The shared scheduler driving this context (one per simulation).
  Scheduler* getScheduler() const {
    return scheduler_;
  }

  Random random;
  std::atomic_uint_fast64_t dataRcvd{0};
  std::atomic_uint_fast64_t dataRcvdExpected{0};
  Tracer tracer;

 private:
  Scheduler* const scheduler_;
};

} // namespace packsim
