/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/pdes/PdesEngine.h"

#include <algorithm>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/ExceptionString.h>
#include <folly/Likely.h>
#include <folly/ScopeGuard.h>
#include <folly/logging/xlog.h>
#include <folly/system/HardwareConcurrency.h>
#include <gflags/gflags.h>

#include "packsim/kernel/stack-trace.h"

DEFINE_double(
    pdes_rebalance_seconds,
    2,
    "How often to rebalance contexts across threads");
DEFINE_double(
    pdes_rebalance_ratio,
    1.05,
    "Rebalance if max/mean ratio greater than this");
DEFINE_bool(
    pdes_integrate_balance,
    false,
    "If set, integrate per-context load across whole sim.");
DEFINE_bool(
    pdes_print_cpu_load,
    false,
    "Print CPU load statistics at end of simulation");

namespace packsim::pdes {

namespace {
// The engine and pool worker the calling thread belongs to. A pool thread sets
// it once, when it starts; on every other thread it stays empty.
struct CurrentWorker {
  const PdesEngine* engine{nullptr};
  detail::WorkerThread* worker{nullptr};
};
thread_local CurrentWorker tlsCurrentWorker;

// Source of PdesEngine::engineId_; 0 is reserved for "no engine".
std::atomic_uint64_t nextEngineId{1};
} // namespace

PdesEngine::PdesEngine(const uint64_t lookaheadTimeFs, const size_t numThreads)
    : engineId_(nextEngineId.fetch_add(1, std::memory_order_relaxed)),
      lookaheadTimeFs_(lookaheadTimeFs),
      numThreads_(numThreads > 0 ? numThreads : folly::available_concurrency()),
      startSyncPointPtr_(std::make_unique<std::barrier<>>(numThreads_ + 1)),
      endSyncPointPtr_(std::make_unique<std::barrier<>>(numThreads_ + 1)) {
  if (lookaheadTimeFs == 0) {
    throw std::invalid_argument("Lookahead time must be a positive value.");
  }
  lastRebalanceTime_ = getTime();
  createThreadPool();
}

PdesEngine::~PdesEngine() {
  // Signal worker threads to close.
  nextFrameTimeFs_ = kTerminationFs;
  // Release all worker threads to close themselves.
  // NOTE: Wrapping function call in void() to silence nodiscard warning.
  void(startSyncPointPtr_->arrive());
}

void PdesEngine::printExitStats() const {
  if (!FLAGS_pdes_print_cpu_load) {
    return;
  }
  int64_t avgTotalContextTimeNs = 0;
  std::vector<int64_t> totalContextTimeNsVector;
  totalContextTimeNsVector.reserve(threadPool_.size());
  for (auto const& workerPtr : threadPool_) {
    auto ctxs = workerPtr->contexts.rlock();
    int64_t totalContextTimeNs = 0;
    int64_t minContextTimeNs =
        (ctxs->empty() ? 0 : std::numeric_limits<int64_t>::max());
    int64_t maxContextTimeNs = 0;
    for (auto const ctx : *ctxs) {
      minContextTimeNs = std::min(minContextTimeNs, ctx->cpuTimeSpentNs);
      maxContextTimeNs = std::max(maxContextTimeNs, ctx->cpuTimeSpentNs);
      totalContextTimeNs += ctx->cpuTimeSpentNs;
    }

    // NOTE: Using sync logging to make sure it flushes before exit.
    std::cout
        << fmt::format(
               "CPU Time for thread {} contexts: min = {} ns, max = {} ns, sum = {} ns",
               workerPtr->threadId,
               minContextTimeNs,
               maxContextTimeNs,
               totalContextTimeNs)
        << std::endl;

    avgTotalContextTimeNs += totalContextTimeNs / std::ssize(threadPool_);
    totalContextTimeNsVector.emplace_back(totalContextTimeNs);
  }
  const int64_t minTotalContextTimeNs = *std::min_element(
      totalContextTimeNsVector.begin(), totalContextTimeNsVector.end());
  const int64_t maxTotalContextTimeNs = *std::max_element(
      totalContextTimeNsVector.begin(), totalContextTimeNsVector.end());

  // NOTE: Using sync logging to make sure it flushes before exit.
  if (avgTotalContextTimeNs > 0) {
    std::cout
        << fmt::format(
               "Total CPU Time across threads: avg = {} ns, min/avg = {}, max/avg = {}",
               avgTotalContextTimeNs,
               minTotalContextTimeNs * 1.0 / avgTotalContextTimeNs,
               maxTotalContextTimeNs * 1.0 / avgTotalContextTimeNs)
        << std::endl;
  }
}

void PdesEngine::run(uint64_t const durationFs) {
  // Checked before the engine is marked running, so a rejected run leaves it
  // able to run again.
  if (std::numeric_limits<uint64_t>::max() - durationFs <= nextFrameTimeFs_) {
    throw std::runtime_error(
        "Time overflow, cannot run simulation any further.");
  }
  {
    std::unique_lock startStopLock(startStopMutex_);
    if (isRunning_) {
      return;
    }
    isRunning_ = true;
    isStopping_ = false;
  }
  const uint64_t finishTimeFs = nextFrameTimeFs_ + durationFs;
  // Using scope exit to handle exception situations as well. Exiting the run
  // function resets the running/stopping state and clears any pending pause
  // request so a request never carries over into the next run().
  SCOPE_EXIT {
    std::unique_lock startStopLock(startStopMutex_);
    nextFrameTimeFs_ = lastEventTimeFs_;
    for (const auto& thread : threadPool_) {
      const auto ctxs = thread->contexts.rlock();
      for (auto* ctx : *ctxs) {
        ctx->currentTimeFs.store(lastEventTimeFs_, std::memory_order_relaxed);
      }
    }

    pauseRequested_.store(false);
    isRunning_ = false;
    isStopping_ = false;
  };

  if (durationFs > 0) {
    // Runs all events across all threads until we reach the requested time.
    // This advances time regardless of whether there are events to run.
    bool wasPaused = false;
    while ((nextFrameTimeFs_ < finishTimeFs) && (getEventCount() > 0)) {
      runFrame(std::min(lookaheadTimeFs_, finishTimeFs - nextFrameTimeFs_));
      const PostFrameAction action = handlePostFrame();
      if (action == PostFrameAction::STOPPED) {
        break;
      }
      if (action == PostFrameAction::PAUSED) {
        wasPaused = true;
        break;
      }
    }
    {
      std::unique_lock startStopLock(startStopMutex_);
      if (!isStopping_ && !wasPaused) {
        lastEventTimeFs_ = std::max(lastEventTimeFs_, finishTimeFs);
      }
    }
  } else {
    // Run util there are no events left in the system. If there are no events
    // to begin with, this returns right away without advancing time.
    while (getEventCount() > 0) {
      runFrame(lookaheadTimeFs_);
      if (handlePostFrame() != PostFrameAction::CONTINUE) {
        break;
      }
    }
  }
}

void PdesEngine::requestPause() {
  pauseRequested_.store(true);
}

void PdesEngine::clearPauseRequest() {
  pauseRequested_.store(false);
}

void PdesEngine::addBreakpointFs(const uint64_t timeFs) {
  breakpointsFs_.emplace(timeFs);
}

void PdesEngine::removeBreakpointFs(const uint64_t timeFs) {
  breakpointsFs_.erase(timeFs);
}

void PdesEngine::clearAllBreakpoints() {
  breakpointsFs_.clear();
}

std::set<uint64_t> PdesEngine::getBreakpointsFs() const {
  return breakpointsFs_;
}

bool PdesEngine::hasBreakpointFs(const uint64_t timeFs) const {
  return breakpointsFs_.contains(timeFs);
}

bool PdesEngine::isNotRunning() const {
  std::shared_lock startStopLock(startStopMutex_);
  return !isRunning_;
}

uint64_t PdesEngine::getLastEventTimeFs() const {
  std::shared_lock startStopLock(startStopMutex_);
  return lastEventTimeFs_;
}

FOLLY_ALWAYS_INLINE bool PdesEngine::consumePauseTrigger() {
  // Atomically consume an explicit pause request: a failed compare_exchange
  // performs no store, so a requestPause() that races in after the compare
  // reads false is not clobbered (it is honored on the next frame, or cleared
  // by run()'s SCOPE_EXIT if the run ends first). Breakpoints are checked only
  // when no explicit pause was pending: an explicit pause deliberately leaves
  // breakpointsFs_ untouched, so a breakpoint at or before the current time
  // survives the paused return and triggers a pause again on the next run()'s
  // first frame (intended: that breakpoint is still set until removed). The
  // caller holds startStopMutex_, so
  // reading lastEventTimeFs_ is safe. breakpointsFs_ itself needs no lock: it
  // is only mutated by the control-plane methods while the engine is stopped,
  // and this read/erase runs on the same run-owning thread between frames, so
  // all access is single-threaded.
  bool expected = true;
  bool shouldPause = pauseRequested_.compare_exchange_strong(expected, false);
  if (!shouldPause) {
    // A single frame can advance lastEventTimeFs_ past several breakpoints at
    // once (large lookahead or closely-packed breakpoints). Consume every
    // breakpoint at or before the current time in one trigger so the caller
    // pauses once here, instead of pausing on later frames at times well past
    // the user's breakpoint values. breakpointsFs_ is sorted, so erase the
    // whole prefix up to the first breakpoint still in the future.
    const auto firstFuture = breakpointsFs_.upper_bound(lastEventTimeFs_);
    if (firstFuture != breakpointsFs_.begin()) {
      breakpointsFs_.erase(breakpointsFs_.begin(), firstFuture);
      shouldPause = true;
    }
  }
  return shouldPause;
}

FOLLY_ALWAYS_INLINE PdesEngine::PostFrameAction PdesEngine::handlePostFrame() {
  std::unique_lock startStopLock(startStopMutex_);
  if (isStopping_) {
    return PostFrameAction::STOPPED;
  }
  return consumePauseTrigger() ? PostFrameAction::PAUSED
                               : PostFrameAction::CONTINUE;
}

bool PdesEngine::stop() {
  std::unique_lock startStopLock(startStopMutex_);
  if (isRunning_) {
    isStopping_ = true;
    return true;
  }
  return false;
}

uint64_t PdesEngine::nowFs(SimulationContext* context) const {
  return context->currentTimeFs;
}

uint64_t PdesEngine::nowFs() const {
  if (UNLIKELY(!isRunning_)) {
    return lastEventTimeFs_;
  }
  return getCurrentThread()->currentContext->currentTimeFs;
}

uint64_t PdesEngine::getLookaheadTimeFs() const {
  return lookaheadTimeFs_;
}

void PdesEngine::setLookaheadTimeFs(const uint64_t lookaheadTimeFs) {
  if (lookaheadTimeFs == 0) {
    throw std::invalid_argument("Lookahead time must be a positive value.");
  }
  std::unique_lock startStopLock(startStopMutex_);
  if (isRunning_) {
    throw std::runtime_error(
        "Cannot change lookahead time if simulation is running.");
  }
  lookaheadTimeFs_ = lookaheadTimeFs;
}

uint64_t PdesEngine::getEventCount() const {
  uint64_t events = 0;
  for (const auto& thread : threadPool_) {
    events += thread->eventBacklog.size();
    auto ctxs = thread->contexts.rlock();
    for (const auto& ctx : *ctxs) {
      events += ctx->eventQ.size();
    }
  }
  return events;
}

uint64_t PdesEngine::getEventsExecuted() const {
  uint64_t events = 0;
  for (const auto& thread : threadPool_) {
    events += thread->eventsExecuted;
  }
  return events;
}

size_t PdesEngine::contextToThreadNum(SimulationContext* context) const {
  // The release store below publishes `threadIdx` with the engine id. Between
  // frames, rebalanceContexts() moves contexts with relaxed stores, which the
  // frame barriers publish.
  if (LIKELY(context->engineId.load(std::memory_order_acquire) == engineId_)) {
    return context->threadIdx.load(std::memory_order_relaxed);
  }
  std::unique_lock constructionLock(threadIdConstructMutex_);
  // Check again in case another thread assigned the context while this one
  // waited for the lock.
  if (context->engineId.load(std::memory_order_relaxed) != engineId_) {
    const size_t threadId = nextContextId_++ % threadPool_.size();
    threadPool_.at(threadId)->contexts.wlock()->emplace_back(context);
    context->threadIdx.store(threadId, std::memory_order_relaxed);
    context->engineId.store(engineId_, std::memory_order_release);
  }
  return context->threadIdx.load(std::memory_order_relaxed);
}

void PdesEngine::registerContext(SimulationContext* context) {
  contextToThreadNum(context);
}

detail::WorkerThread* PdesEngine::getCurrentThread() const {
  if (UNLIKELY(tlsCurrentWorker.engine != this)) {
    throw std::runtime_error(
        "Cannot schedule an event on a thread that is not part of the PDES thread pool. "
        "Provide a context explicitly if scheduling from any thread outside the PDES pool.");
  }
  return tlsCurrentWorker.worker;
}

void PdesEngine::runFrame(uint64_t const durationFs) {
  if (durationFs == 0) {
    throw std::invalid_argument("Cannot run durations that are non-positive.");
  }
  if (durationFs > lookaheadTimeFs_) {
    throw std::invalid_argument(
        "Cannot run longer than lookahead time in one shot.");
  }
  if (std::numeric_limits<uint64_t>::max() - durationFs <= nextFrameTimeFs_) {
    throw std::runtime_error(
        "Time overflow, cannot run simulation any further.");
  }
  nextFrameTimeFs_ += durationFs;
  // Run all worker threads for one round.
  // NOTE: Wrapping function call in void() to silence nodiscard warning.
  void(startSyncPointPtr_->arrive());
  // Wait until all threads finish.
  endSyncPointPtr_->arrive_and_wait();

  // Set the current time on all threads to the last event that was executed,
  // and set that as the next frame time.
  {
    std::unique_lock startStopLock(startStopMutex_);
    uint64_t finishTimeFs = lastEventTimeFs_;
    for (const auto& thread : threadPool_) {
      finishTimeFs = std::max(finishTimeFs, thread->lastEventTimeFs);
    }

    for (const auto& thread : threadPool_) {
      const auto ctxs = thread->contexts.rlock();
      for (auto* ctx : *ctxs) {
        ctx->currentTimeFs.store(finishTimeFs, std::memory_order_relaxed);
      }
    }

    lastEventTimeFs_ = finishTimeFs;
  }

  // Drain every exception the workers queued this frame, so none is left to
  // surface from a later run(), and rethrow the one from the earliest event in
  // simulation order. Workers catch each context's exception separately and
  // carry on with their other contexts, and contexts do not affect each other
  // within a frame, so every context that throws queues the same first
  // exception whatever the thread count or which thread runs it.
  std::vector<detail::ExceptionTrace> traces;
  while (!exceptionBacklog_.empty()) {
    traces.emplace_back(exceptionBacklog_.dequeue());
  }
  if (!traces.empty()) {
    std::stable_sort(
        traces.begin(), traces.end(), [](const auto& a, const auto& b) {
          return a.eventKey < b.eventKey;
        });
    for (size_t i = 1; i < traces.size(); ++i) {
      XLOGF(
          ERR,
          "Last frame's exception {} of {} is not rethrown: {}. Exception "
          "stack trace from running callback: \n{}",
          i + 1,
          traces.size(),
          folly::exceptionStr(traces[i].exceptionPtr),
          traces[i].stackTrace);
    }
    XLOGF(
        ERR,
        "Last frame generated {} exceptions, rethrowing the earliest one: {}. "
        "Exception stack trace from running callback: \n{}",
        traces.size(),
        folly::exceptionStr(traces.front().exceptionPtr),
        traces.front().stackTrace);
    std::rethrow_exception(traces.front().exceptionPtr);
  }

  if (threadPool_.size() > 1) {
    rebalanceContexts();
  }
}

void PdesEngine::queueWorkerException(
    const detail::WorkerThread& worker,
    RawEvent*& event) noexcept {
  constexpr uint64_t kNoEventKey = std::numeric_limits<uint64_t>::max();
  exceptionBacklog_.enqueue(
      detail::ExceptionTrace(
          /* stackTrace= */ getCurrentExceptionStackTrace(),
          /* exceptionPtr= */ std::current_exception(),
          /* eventKey= */
          event ? std::tuple{event->timeFs, event->sortId, worker.threadId}
                : std::tuple{kNoEventKey, kNoEventKey, worker.threadId}));
  // Recycle rather than free the event that threw: a handle to it may outlive
  // this frame, and the pool's reset() is what makes such a handle read as
  // stale instead of reading freed memory.
  if (event) {
    eventPool().push(event);
  }
}

void PdesEngine::createThreadPool() {
  threadPool_.reserve(numThreads_);
  for (size_t i = 0; i < numThreads_; ++i) {
    auto worker = std::make_unique<detail::WorkerThread>();
    worker->threadPtr = std::make_unique<std::jthread>([workerPtr =
                                                            worker.get(),
                                                        this]() {
      tlsCurrentWorker = {.engine = this, .worker = workerPtr};
      // This frame's snapshot of the worker's contexts. Each frame assigns it
      // afresh, which replaces the previous frame's contents and keeps the
      // capacity, so taking the snapshot stops allocating once it fits.
      std::vector<SimulationContext*> contexts;
      for (;;) {
        // Wait for the main thread to signal run.
        startSyncPointPtr_->arrive_and_wait();

        // Terminate the execution if signaled by destructor.
        if (nextFrameTimeFs_ == kTerminationFs) {
          break;
        }

        try {
          // Sync all the scheduled events into the priority queue.
          while (!workerPtr->eventBacklog.empty()) {
            auto backlogEvent = workerPtr->eventBacklog.dequeue();
            backlogEvent->context->eventQ.push(backlogEvent);
          }
          // It is possible new contexts are created within this iteration, so
          // run from a snapshot of the list.
          contexts = *workerPtr->contexts.rlock();
        } catch (...) {
          RawEvent* noEvent{nullptr};
          queueWorkerException(*workerPtr, noEvent);
          contexts.clear();
        }
        // A context's exception ends only that context's frame: the worker
        // still runs its other contexts, so each one that throws is reported.
        for (auto context : contexts) {
          RawEvent* event{nullptr};
          try {
            auto& eventQ = context->eventQ;
            auto const timerStart = getTime();
            if (contextToThreadNum(context) != workerPtr->threadId) {
              // Safety check to ensure rebalancing worked correctly
              throw std::runtime_error(
                  fmt::format(
                      "Context {} is on the wrong thread: {} != {}",
                      context->nameWithPtr(),
                      contextToThreadNum(context),
                      workerPtr->threadId));
            }
            // Execute all events that fall within time range.
            workerPtr->currentContext = context;
            for (;;) {
              // If no events, we end this frame.
              if (eventQ.empty() ||
                  (eventQ.top()->timeFs >= nextFrameTimeFs_)) {
                break;
              }
              // Skip inactive events, execute otherwise.
              event = eventQ.top();
              eventQ.pop();
              if (event->context != context) {
                // Safety check to ensure rebalancing worked correctly
                throw std::runtime_error(
                    fmt::format(
                        "Event context {} does not match PQ context {}",
                        event->context->nameWithPtr(),
                        context->nameWithPtr()));
              }
              // Relaxed, like every access to the flag (see RawEvent::reset()).
              if (event->isActive.load(std::memory_order_relaxed)) {
                if (context->currentTimeFs > event->timeFs) {
                  // If this is happening, it is likely because you are
                  // scheduling events on other contexts that are supposed to
                  // run before lookahead time. Try reducing the lookahead time
                  // or make sure you enforce it correctly.
                  throw std::runtime_error(
                      fmt::format(
                          "Causality Paradox: Cannot execute an event at time {} "
                          "in the past relative to current time {} for context {}.",
                          event->timeFs,
                          context->currentTimeFs.load(),
                          event->context->nameWithPtr()));
                }

                // Only this thread writes the clock during a frame; other
                // threads sample it, and the frame barriers order everything
                // else, so the store needs no fence.
                context->currentTimeFs.store(
                    event->timeFs, std::memory_order_relaxed);
                workerPtr->lastEventTimeFs =
                    std::max(event->timeFs, workerPtr->lastEventTimeFs);
                event->func();
                workerPtr->eventsExecuted++;
              }
              eventPool().push(event);
            }
            auto const timerEnd = getTime();
            auto const timeSpentNs =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    timerEnd - timerStart)
                    .count();
            context->cpuTimeSpentNs += timeSpentNs;
          } catch (...) {
            queueWorkerException(*workerPtr, event);
          }
        }

        // Advance time to beginning of next frame once all events are
        // done in this frame.
        // NOTE: This is intentionally outside the try/catch block above. In
        // case the user captures and discards an earlier exception and calls
        // run again, this will ensure that a causality exception is raised in
        // case other events were skipped.
        {
          // Scoped so that the read lock is released before the barrier.
          const auto ctxs = workerPtr->contexts.rlock();
          for (auto* ctx : *ctxs) {
            ctx->currentTimeFs.store(
                nextFrameTimeFs_, std::memory_order_relaxed);
          }
        }

        // Tell main thread this frame is complete.
        // NOTE: Wrapping function call in void() to silence nodiscard
        // warning since return value is not useful here.
        void(endSyncPointPtr_->arrive());
      }
    });
    worker->threadId = threadPool_.size();
    threadPool_.emplace_back(std::move(worker));
  }
}

void PdesEngine::rebalanceContexts() {
  if (!shouldRebalance()) {
    return;
  }
  lastRebalanceTime_ = getTime();

  // Sort contexts by time spent in decreasing order
  std::priority_queue<std::tuple<int64_t /*time (ns)*/, SimulationContext*>>
      ctxTimes;
  std::vector<SimulationContext*> contexts;
  for (const auto& thread : threadPool_) {
    auto ctxs = thread->contexts.wlock();
    for (const auto& ctx : *ctxs) {
      contexts.emplace_back(ctx);
      ctxTimes.emplace(ctx->cpuTimeSpentNs, ctx);
    }
    ctxs->clear();
    thread->currentContext = nullptr;

    // Drain thread's backlog
    while (!thread->eventBacklog.empty()) {
      auto event = thread->eventBacklog.dequeue();
      event->context->eventQ.push(event);
    }
  }

  // If flag is not set, reset the time spent per context to 0. We've already
  // saved it in the priority queue for sorting, so we don't need it in the
  // SimulationContext anymore
  if (!FLAGS_pdes_integrate_balance) {
    for (auto ctx : contexts) {
      ctx->cpuTimeSpentNs = 0;
    }
  }

  // Keep track of how much time has been assigned to each thread, so far, in
  // decreasing order
  using TT = std::tuple<int64_t, size_t>;
  std::priority_queue<TT, std::vector<TT>, std::greater<TT>> newThreadTimes;
  for (size_t i = 0; i < threadPool_.size(); ++i) {
    // Initialize thread times to 0
    newThreadTimes.emplace(0, i);
  }

  while (!ctxTimes.empty()) {
    // Greedy algorithm: Assign the remaining context with the most time spent
    // to the thread with the least total time spent
    const auto [ctxNs, ctx] = ctxTimes.top();
    ctxTimes.pop();
    auto [threadNs, threadIdx] = newThreadTimes.top();
    newThreadTimes.pop();

    ctx->threadIdx.store(threadIdx, std::memory_order_relaxed);

    // Move event queue into new thread
    auto newThread = threadPool_.at(threadIdx).get();
    newThread->contexts.wlock()->emplace_back(ctx);
    threadNs += ctxNs;
    newThreadTimes.emplace(threadNs, threadIdx);
  }

  const double durationSeconds =
      std::chrono::duration<double>(getTime() - lastRebalanceTime_).count();
  XLOGF(DBG0, "Rebalanced contexts in {:0.03f}ms", durationSeconds * 1e3);
}

bool PdesEngine::shouldRebalance() const {
  auto const now = getTime();
  auto const durationSeconds =
      std::chrono::duration<double>(now - lastRebalanceTime_).count();
  if (durationSeconds < FLAGS_pdes_rebalance_seconds) {
    return false;
  }

  double totalNs = 0;
  double maxNs = 0;
  for (const auto& thread : threadPool_) {
    double threadNs = 0;
    auto ctxs = thread->contexts.rlock();
    for (const auto ctx : *ctxs) {
      threadNs += ctx->cpuTimeSpentNs;
    }
    totalNs += threadNs;
    maxNs = std::max(maxNs, threadNs);
  }
  const double avgNs = totalNs / threadPool_.size();

  auto const maxMeanRatio = avgNs > 0 ? maxNs / avgNs : 1;
  if (maxMeanRatio > FLAGS_pdes_rebalance_ratio) {
    XLOGF(DBG0, "Max/Mean Ratio: {:0.02f} -- Rebalancing", maxMeanRatio);
    return true;
  }
  XLOGF(DBG0, "Max/Mean Ratio: {:0.02f} -- Not rebalancing", maxMeanRatio);
  return false;
}

std::vector<double> PdesEngine::getThreadCpuWorkTimes() const {
  std::vector<double> times(threadPool_.size(), 0.0);
  for (size_t t = 0; t < times.size(); ++t) {
    auto ctxs = threadPool_.at(t)->contexts.rlock();
    for (const auto ctx : *ctxs) {
      times[t] += ctx->cpuTimeSpentNs;
    }
  }
  return times;
}

PdesEngine::time_point PdesEngine::getTime() const {
  return std::chrono::steady_clock::now();
}

} // namespace packsim::pdes
