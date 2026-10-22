/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <folly/Likely.h>
#include <folly/Portability.h>
#include <folly/SharedMutex.h>
#include <folly/SingletonThreadLocal.h>
#include <folly/Synchronized.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GTestProd.h>
#include <gflags/gflags.h>

#include "packsim/kernel/pdes/Event.h"
#include "packsim/kernel/pdes/SimulationContext.h"

DECLARE_double(pdes_rebalance_seconds);
DECLARE_double(pdes_rebalance_ratio);
DECLARE_bool(pdes_integrate_balance);

namespace packsim::pdes {

namespace detail {

template <typename T>
class ObjectAllocator {
 public:
  ObjectAllocator() = default;

  ObjectAllocator(const ObjectAllocator&) = delete;
  ObjectAllocator(ObjectAllocator&&) = delete;
  ObjectAllocator& operator=(const ObjectAllocator&) = delete;
  ObjectAllocator& operator=(ObjectAllocator&&) = delete;

  T* FOLLY_NONNULL pop() {
    if (objects_.empty()) {
      return new T();
    }
    T* ret = objects_.back();
    objects_.pop_back();
    return ret;
  }

  // Resets `ptr` into the pool and clears it. The caller's pointer is cleared
  // before anything can throw, so a failed append (out of memory) leaks the
  // object rather than leaving the caller holding a reset one.
  void push(T*& ptr) {
    XCHECK(ptr != nullptr);
    T* const object = std::exchange(ptr, nullptr);
    object->reset();
    objects_.emplace_back(object);
  }

  ~ObjectAllocator() {
    for (auto& p : objects_) {
      delete p;
    }
  }

 private:
  std::vector<T*> objects_;
};

struct WorkerThread {
  WorkerThread() = default;

  WorkerThread(const WorkerThread&) = delete;
  WorkerThread(WorkerThread&&) = delete;
  WorkerThread& operator=(const WorkerThread&) = delete;
  WorkerThread& operator=(WorkerThread&&) = delete;

  ~WorkerThread() {
    while (!eventBacklog.empty()) {
      auto* event = eventBacklog.dequeue();
      delete event;
    }
  }

  // Thread that will back up this worker.
  std::unique_ptr<std::jthread> threadPtr;

  // All contexts this thread is responsible for
  folly::Synchronized<std::vector<SimulationContext*>> contexts;

  // Context from currently executing event.
  SimulationContext* currentContext{};

  // Index of the current thread in the pool.
  size_t threadId{};

  uint64_t lastEventTimeFs{0};

  uint64_t eventsExecuted{0};

  // Atomic queue to accept new events to this thread. Events are flushed into
  // the priority queue after each event that is executed. The main goal is to
  // avoid having to synchronize the priority queue and instead have the atomic
  // queue take care of synchronization.
  folly::UnboundedQueue<
      RawEvent*,
      /* SingleProducer= */ false,
      /* SingleConsumer= */ true,
      /* MayBlock= */ false>
      eventBacklog;
};

struct ExceptionTrace {
  std::string stackTrace;
  std::exception_ptr exceptionPtr;
  // Simulation-order key of the exception: the time and sort id of the event
  // that threw (the maximum of both when no event was executing), then the
  // index of the worker thread that caught it.
  std::tuple<uint64_t, uint64_t, size_t> eventKey;

  explicit ExceptionTrace(
      std::string stackTrace,
      std::exception_ptr exceptionPtr,
      std::tuple<uint64_t, uint64_t, size_t> eventKey)
      : stackTrace(std::move(stackTrace)),
        exceptionPtr(std::move(exceptionPtr)),
        eventKey(eventKey) {}
};
} // namespace detail

/// PDES engine is a general purpose parallel DES engine for all purposes.
/// This PDES engine can schedule events to run on multiple threads by
/// referencing all events based on the context they are associated with.  The
/// user can decide how they associate events to various in memory objects
/// (i.e., contexts) based on their design. For example, whether events are
/// pulled together per node, per pod, per region or per building for a network
/// simulation is user's call. Scheduling avoids mutexes and relies on atomics:
/// it locks only the first time it sees a context. The run/stop state has its
/// own mutex, taken at frame boundaries and by `stop()` and
/// `getLastEventTimeFs()`.
///
/// NOTE: `schedule(delayFs, ...)` and `scheduleNow` act on the calling thread's
/// current context, so call them only from an event on a PDES worker thread.
/// While `run()` executes, `scheduleAt` and `nowFs()` must also be called from
/// such an event; between runs, call the engine from one thread at a time.
/// `stop()`, `requestPause()` and `getLastEventTimeFs()` may be called from any
/// thread.
///
/// Example usage:
///
///   auto pdes = PdesEngine(/* lookaheadTimeFs= */ 10, /* numThreads= */ 2);
///
///   // Split a network into two cutsets
///   Cutset cutset1; // represents network cutset 1
///   Cutset cutset2; // represents network cutset 2
///
///   pdes.scheduleAt(&cutset1, /* timeFs= */ 0, []() { /* in cutset 1 */ });
///   pdes.scheduleAt(&cutset2, /* timeFs= */ 0, []() { /* in cutset 2 */ });
///
///   pdes.run(); // Run the chain of events starting from above until finish
///
/// The run function above will block until all events in the system are flushed
/// out. If you want to only run for a specific duration, you can supply that as
/// an argument.
///
/// NOTE: Cutset must derive from SimulationContext to be used as a context.
///
/// NOTE: This engine will throw if causality is violated at any point. That
/// could happen if you schedule events without paying attention to cross
/// context delay. Events cannot be scheduled on another context earlier than
/// lookahead time into the future, otherwise, causality may be violated and
/// this will throw.
///
/// NOTE: We assume time to progress in the units of femtoseconds (1e-15 secs).
///
/// NOTE: When scheduling events on any context, make sure the function
/// scheduled on a context only manipulates data/state related to that context.
/// Undefined behavior and hard to debug issues can arise otherwise.
class PdesEngine {
 public:
  /**
   * Create a Parallel DES (PDES) engine to run with cross context latency of at
   * least lookahead time between all contexts.
   *
   * @param  lookaheadTimeFs  Minimum cross context delay; must be positive.
   * @param  numThreads       Number of parallel threads. Use 0 to use all CPUs
   *                          available on this machine.
   */
  explicit PdesEngine(uint64_t lookaheadTimeFs, size_t numThreads = 0);

  PdesEngine(PdesEngine const&) = delete;
  PdesEngine& operator=(PdesEngine const&) = delete;
  PdesEngine(PdesEngine&&) = delete;
  PdesEngine& operator=(PdesEngine&&) = delete;

  virtual ~PdesEngine();

  /**
   * Schedule an event to run at a given time on the given context.
   *
   * @param  context   Target context that will run this event.
   * @param  timeFs    Absolute simulation time, in femtoseconds, for the event
   *                   on target context.
   * @param  func      Function to run for this event.
   * @param  args      Arguments to pass to function, as rvalues: the event
   *                   runs once, so `func` takes each by value or by `const&`,
   *                   never by non-const reference.
   *
   * @return  Handle that can cancel the event before it runs.
   *
   * @throw std::runtime_error if `timeFs` is before `context`'s current time,
   *        or if called while the engine runs from a thread outside the PDES
   *        pool.
   */
  template <typename TFunc, typename... TArgs>
  Event scheduleAt(
      SimulationContext* context,
      uint64_t timeFs,
      TFunc&& func,
      TArgs&&... args) {
    // If scheduleAt is called on the current context, shortcut the lock-free
    // queue and schedule directly on the correct priority queue. This is
    // required because we only check the lock free queue at the beginning of a
    // frame.
    if (isRunning_ && context == getCurrentThread()->currentContext) {
      const uint64_t nowFs = context->currentTimeFs;
      if (timeFs < nowFs) {
        throw std::runtime_error("Cannot schedule an event in the past");
      }
      return schedule(
          timeFs - nowFs,
          std::forward<TFunc>(func),
          std::forward<TArgs>(args)...);
    }
    auto const threadId = contextToThreadNum(context);
    return schedule(
        context,
        threadId,
        timeFs,
        std::forward<TFunc>(func),
        std::forward<TArgs>(args)...);
  }

  /**
   * Schedule an event on the current context, `delayFs` after that context's
   * current time.
   *
   * @param  delayFs   Delay relative to the current context's simulation time.
   * @param  func      Function to run for this event.
   * @param  args      Arguments to pass to function, as rvalues: the event
   *                   runs once, so `func` takes each by value or by `const&`,
   *                   never by non-const reference.
   *
   * @return  Handle that can cancel the event before it runs.
   *
   * @throw std::runtime_error if called from a thread outside the PDES pool,
   *        or if the event time overflows.
   */
  template <typename TFunc, typename... TArgs>
  Event schedule(uint64_t delayFs, TFunc&& func, TArgs&&... args) {
    auto* const currentThread = getCurrentThread();
    auto const context = currentThread->currentContext;
    const uint64_t nowFs = context->currentTimeFs;
    const uint64_t timeFs = nowFs + delayFs;
    // A delay above UINT64_MAX - nowFs overflows the sum.
    if (timeFs < nowFs) {
      throw std::runtime_error("Time overflow, cannot schedule the event.");
    }
    auto eventPtr = eventPool().pop();
    eventPtr->timeFs = timeFs;
    // A C++20 pack init-capture, not std::bind. The one behavioural difference
    // is that bind unwraps a std::reference_wrapper before the call while this
    // passes it through; every callee takes a concrete reference, which the
    // wrapper converts to implicitly. An event runs at most once -- a worker
    // pops it, runs `func` and recycles it -- so the stored arguments are
    // passed on as rvalues: a callee that takes one by value, such as a
    // `PacketPtr` sink, takes it over instead of copying it.
    eventPtr->func = [f = std::forward<TFunc>(func),
                      ... a = std::forward<TArgs>(args)]() mutable {
      std::invoke(f, std::move(a)...);
    };
    eventPtr->context = context;
    eventPtr->setSortId(context->ctxIdx, context->nextEventId++);
    context->eventQ.push(eventPtr);
    return Event(eventPtr);
  }

  /**
   * Schedule an event to run now on the current context. You might use this to
   * prevent stack overflow for chained local events.
   *
   * @param  func      Function to run for this event.
   * @param  args      Arguments to pass to function, as rvalues: the event
   *                   runs once, so `func` takes each by value or by `const&`,
   *                   never by non-const reference.
   *
   * @return  Handle that can cancel the event before it runs.
   *
   * @throw std::runtime_error if called from a thread outside the PDES pool.
   */
  template <typename TFunc, typename... TArgs>
  Event scheduleNow(TFunc&& func, TArgs&&... args) {
    return schedule(
        /*delayFs=*/0, std::forward<TFunc>(func), std::forward<TArgs>(args)...);
  }

  /**
   * Run the engine for the provided duration. If duration is 0 this will run
   * the engine until stop() is called or all events are finished.
   *
   * @param  durationFs  Duration to run in femtoseconds. Default is run until
   *                     all events are done.
   *
   * NOTE: If the duration is 0, this runs until no events left. While if
   * duration is > 0, this runs to reach the requested time regardless of
   * whether there are events or not.
   *
   * NOTE: Calling run when the engine is running has no effect and returns
   * immediately without blocking.
   */
  virtual void run(uint64_t const durationFs = 0);

  /**
   * Stop the engine. Engine will stop after it finishes the current frame. This
   * function is thread-safe and can be called from any context or main thread.
   *
   * NOTE: This puts in a request for engine to stop at the end of current
   * frame. Stopping it NOT immediate.
   *
   * @return True if the engine was running and it was stopped.
   */
  virtual bool stop();

  /**
   * Returns the current time for the context.
   *
   * @param  context  Context for which to check time.
   *
   * @return  Context time in femtoseconds.
   */
  virtual uint64_t nowFs(SimulationContext* context) const;

  /**
   * Returns the current time for the current context.
   *
   * @return  Context time in femtoseconds; while the engine is not running,
   *          the time where the last `run()` stopped (0 before the first run).
   *
   * @throw std::runtime_error if called while the engine runs from a thread
   *        outside the PDES pool.
   */
  virtual uint64_t nowFs() const;

  /**
   * Returns the lookahead time.
   *
   * @return  Lookahead time in femtoseconds.
   */
  virtual uint64_t getLookaheadTimeFs() const;

  /**
   * Set the lookahead time. This is only allowed before the engine is started.
   *
   * @param  lookaheadTimeFs  Minimum cross context delay; must be positive.
   *
   * @throw  std::invalid_argument if `lookaheadTimeFs` is 0.
   * @throw  std::runtime_error if the engine is running.
   */
  virtual void setLookaheadTimeFs(uint64_t lookaheadTimeFs);

  /**
   * Returns the total pending events currently in the system.
   *
   * @return  Number of pending events.
   */
  virtual uint64_t getEventCount() const;

  /**
   * Ensure that a context has been assigned to a thread so that, even if no
   * events are scheduled on that context, the time on the context will be
   * advanced. This is primarily of importance in unit testing, where we may
   * advance the simulation time and then check for the state of an object at
   * that time, outside of a simulation event.
   */
  void registerContext(SimulationContext* context);

  /**
   * For statistics tracking: Returns the total # of events executed by the
   * simulator across all threads up until this point. Not thread safe, in the
   * sense that it's reading non-atomic data from each thread, but calling it
   * while the simulator is running won't result in a crash, just potentially
   * TSAN complaining.
   */
  uint64_t getEventsExecuted() const;

  /**
   * Request the engine to pause at the next frame boundary. The engine will
   * stop after finishing the current frame.
   *
   * NOTE: the request is best-effort, not queued. run() clears it on every
   * exit, so a request that races the tail of an already-exiting run() may be
   * dropped. Do not rely on this to "arm" a pause for a future run(); set the
   * request while the engine is not running, or use a breakpoint.
   */
  void requestPause();

  /**
   * Clear a previously set pause request without resuming the engine.
   */
  void clearPauseRequest();

  /**
   * Add a time-based breakpoint. The engine will pause when simulation time
   * reaches or exceeds this value.
   *
   * NOTE: the breakpoint methods (addBreakpointFs / removeBreakpointFs /
   * clearAllBreakpoints / getBreakpointsFs) are not thread-safe. Unlike
   * requestPause(), they read and mutate breakpointsFs_ (a non-atomic std::set)
   * directly, and the run loop also erases reached breakpoints between frames.
   * Call them only while the engine is not running (see isNotRunning());
   * concurrent access during run() is a data race.
   *
   * @param timeFs  Breakpoint time in femtoseconds.
   */
  void addBreakpointFs(uint64_t timeFs);

  /**
   * Remove a specific breakpoint.
   *
   * @param timeFs  Breakpoint time to remove.
   */
  void removeBreakpointFs(uint64_t timeFs);

  /**
   * Remove all breakpoints.
   */
  void clearAllBreakpoints();

  /**
   * Get all currently set breakpoints.
   *
   * @return  Sorted set of breakpoint times in femtoseconds.
   */
  std::set<uint64_t> getBreakpointsFs() const;

  /**
   * Check whether a breakpoint is set at the given time, without copying the
   * breakpoint set (unlike getBreakpointsFs()). Same not-thread-safe caveat as
   * the other breakpoint methods above.
   *
   * @param timeFs  Breakpoint time in femtoseconds.
   *
   * @return  True if a breakpoint is set at timeFs.
   */
  bool hasBreakpointFs(uint64_t timeFs) const;

  /**
   * Check whether run() is currently executing on the engine. Returns true
   * before run() is first called and after run() returns (whether it completed,
   * was stopped, or returned at a pause/breakpoint); false while run() is on
   * the stack.
   *
   * This deliberately tracks only "is run() executing", not a distinct "paused"
   * state. A controller that pauses the engine queries it only after run()
   * returns, which is exactly when it wants this signal.
   *
   * @return True when run() is not currently executing.
   */
  bool isNotRunning() const;

  /**
   * Returns the time of the last executed event in femtoseconds, read under
   * startStopMutex_ (shared) so it is safe to call from any thread, including
   * while the sim is running (in which case it reflects the last completed
   * frame boundary).
   *
   * @return Last event time in femtoseconds.
   */
  uint64_t getLastEventTimeFs() const;

  /**
   * Print, for each worker thread, the minimum, maximum and sum of its
   * contexts' CPU times, then a cross-thread summary, to stdout when
   * `--pdes_print_cpu_load` is set. It reads every registered context, so call
   * it while they are all alive and the engine is not running. The destructor
   * does not call it, because owners often destroy their contexts before the
   * engine.
   */
  void printExitStats() const;

  static detail::ObjectAllocator<RawEvent>& eventPool() {
    return folly::SingletonThreadLocal<
        detail::ObjectAllocator<RawEvent>>::get();
  }

 protected:
  /**
   * Schedule an event to run at given time on the given context. Make sure
   * events triggered between different contexts are at least lookahead time
   * into the future.
   *
   * @param  context   Context that will run this event.
   * @param  threadId  Thread ID that will run this event.
   * @param  timeFs    Absolute simulation time, in femtoseconds. Must be at
   *                   least `context`'s current time.
   * @param  func      Function to run for this event.
   * @param  args      Arguments to pass to function, as rvalues: the event
   *                   runs once, so `func` takes each by value or by `const&`,
   *                   never by non-const reference.
   *
   * @return  Handle that can cancel the event before it runs.
   *
   * @throw std::runtime_error if `timeFs` is before `context`'s current time.
   */
  template <typename TFunc, typename... TArgs>
  Event schedule(
      SimulationContext* context,
      size_t threadId,
      uint64_t timeFs,
      TFunc&& func,
      TArgs&&... args) {
    // `context` may be running on another thread. A valid cross-context event
    // lands at or after the end of the current frame, and no context's clock
    // passes the frame end before the frame completes, so this rejects only
    // events that would fail the causality check if they ran. It throws, as
    // that check does, so `run()` reports the error to its caller instead of
    // the process aborting.
    if (timeFs < context->currentTimeFs.load()) {
      throw std::runtime_error("Cannot schedule an event in the past");
    }
    auto eventPtr = eventPool().pop();

    if (LIKELY(isRunning_)) {
      SimulationContext* srcCtx = getCurrentThread()->currentContext;
      eventPtr->setSortId(srcCtx->ctxIdx, srcCtx->nextEventId++);
    } else {
      eventPtr->setSortId(kDefaultCtxIdx, defaultEventCtr_++);
    }
    auto thread = threadPool_.at(threadId).get();

    // Create a new event.
    eventPtr->timeFs = timeFs;
    // A C++20 pack init-capture, not std::bind. The one behavioural difference
    // is that bind unwraps a std::reference_wrapper before the call while this
    // passes it through; every callee takes a concrete reference, which the
    // wrapper converts to implicitly. An event runs at most once -- a worker
    // pops it, runs `func` and recycles it -- so the stored arguments are
    // passed on as rvalues: a callee that takes one by value, such as a
    // `PacketPtr` sink, takes it over instead of copying it.
    eventPtr->func = [f = std::forward<TFunc>(func),
                      ... a = std::forward<TArgs>(args)]() mutable {
      std::invoke(f, std::move(a)...);
    };
    eventPtr->context = context;

    // Put the event into the right thread based on context. This approach
    // guarantees that all events associated with the same context are always
    // executed by the same thread.
    //
    thread->eventBacklog.enqueue(eventPtr);

    return Event(eventPtr);
  }

  /**
   * Tell the associated thread for a given context. This round-robins all
   * contexts across various threads with the assumption that all contexts are
   * relatively similar in load. The assignment is cached in the context, so
   * only the first lookup by this engine takes a lock.
   *
   * NOTE: A context belongs to one engine at a time: a lookup by another engine
   * assigns it to that engine's threads. Once another engine has taken a
   * context, the engine it left must not run again, because that engine still
   * lists the context on the thread it had: destroy the old engine first, as
   * `Scheduler::buildNewEngine()` does.
   *
   * @param  context  Context to assign to a thread ID.
   *
   * @return  Thread ID for the context which is between 0 and numThreads_
   */
  size_t contextToThreadNum(SimulationContext* context) const;

  /**
   * Get the current worker thread.
   *
   * @return  Pointer for current worker thread.
   *
   * @throws  If called from any thread outside of PDES's internal pool.
   */
  detail::WorkerThread* getCurrentThread() const;

  /**
   * Run the simulation for one frame.
   *
   * @param  durationFs  Duration (in Femtoseconds) to run for which must
   *                     be <= lookahead time.
   */
  virtual void runFrame(uint64_t const durationFs);

  /**
   * Creates a pool of threads ready to go at a signal from main thread. These
   * threads execute a subset of events each based on the context each event is
   * associated with. All events related to a given context is always executed
   * by the same thread making things thread-safe by construction.
   *
   * NOTE: Please make sure functions scheduled to run in any context manipulate
   * data only in that context. Otherwise, undefined behavior or race conditions
   * might appear.
   */
  virtual void createThreadPool();

  /**
   * Rebalance contexts among threads. Uses a greedy algorithm.
   */
  void rebalanceContexts();

  /**
   * Queue the exception being handled for run() to rethrow, keyed by the event
   * that threw, and recycle that event. Call it only from a catch block on a
   * worker thread. A failure while recording the exception (out of memory) has
   * nowhere to be reported, so it terminates the process.
   *
   * @param worker  The worker thread that caught the exception.
   * @param event   The event that was executing, or nullptr when none was;
   *                cleared once the event is back in the pool.
   */
  void queueWorkerException(
      const detail::WorkerThread& worker,
      RawEvent*& event) noexcept;

  /**
   * Helper function to determine whether we _should_ rebalance among threads
   */
  bool shouldRebalance() const;

  /**
   * Helper function for shouldRebalance() that gathers the time spent per
   * thread since last rebalance
   */
  std::vector<double> getThreadCpuWorkTimes() const;

  using time_point = std::chrono::steady_clock::time_point;
  virtual time_point getTime() const;

 private:
  // Returns true if the engine should pause between frames (an explicit pause
  // request or a reached breakpoint) and consumes that trigger. The caller must
  // hold startStopMutex_ (this reads lastEventTimeFs_); it does not touch the
  // lock itself, so the caller manages it via RAII.
  FOLLY_ALWAYS_INLINE bool consumePauseTrigger();

  // Result of the per-frame pause/stop check shared by both run() loops.
  enum class PostFrameAction { CONTINUE, PAUSED, STOPPED };

  // Shared post-frame bookkeeping: under startStopMutex_, checks for a stop
  // request and then consumePauseTrigger(). Returns what the run loop should do
  // next.
  FOLLY_ALWAYS_INLINE PostFrameAction handlePostFrame();

  // Frame time the destructor sets to release the worker threads. `runFrame`
  // refuses to advance frame time this far, so no real frame reaches it.
  static constexpr uint64_t kTerminationFs = UINT64_MAX;

  // Identifies this engine in `SimulationContext::engineId`; unique for the
  // life of the process.
  const uint64_t engineId_;

  uint64_t lookaheadTimeFs_;
  const size_t numThreads_;
  const std::unique_ptr<std::barrier<>> startSyncPointPtr_;
  const std::unique_ptr<std::barrier<>> endSyncPointPtr_;

  // Serializes assigning a context this engine has not seen to a thread.
  mutable folly::SharedMutex threadIdConstructMutex_;
  mutable size_t nextContextId_ = 0;

  std::vector<std::unique_ptr<detail::WorkerThread>> threadPool_;
  uint64_t nextFrameTimeFs_ = 0;

  // Variables for tracking calls to start/stop methods.
  mutable folly::SharedMutex startStopMutex_;
  bool isRunning_ = false;
  bool isStopping_ = false;
  uint64_t lastEventTimeFs_ = 0;

  // Atomic queue to hold exceptions thrown by worker threads until
  // we sync with main thread.
  // NOTE: This helps avoid crashing the process since uncaptured exceptions
  // in worker threads leads to std::terminate() being called on all threads
  // causing all kinds of undefined behavior such as null deref etc.
  folly::UnboundedQueue<
      detail::ExceptionTrace,
      /* SingleProducer= */ false,
      /* SingleConsumer= */ true,
      /* MayBlock= */ false>
      exceptionBacklog_;

  std::chrono::steady_clock::time_point lastRebalanceTime_;

  static constexpr uint64_t kDefaultCtxIdx = UINT64_MAX;
  uint64_t defaultEventCtr_{0};

  // Pause/breakpoint support, for stepping a run. breakpointsFs_ is not
  // mutex-guarded: it is mutated only by the control-plane methods while the
  // engine is stopped, and read by the run loop on the same thread between
  // frames, so all access is single-threaded. pauseRequested_ is atomic because
  // requestPause() may be called asynchronously while run() executes, from
  // another thread or from a signal handler on the same thread.
  std::atomic<bool> pauseRequested_{false};
  std::set<uint64_t> breakpointsFs_;

  // ========= Friend tests =========
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineRingTransmissionModel);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineNumThreads);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineAlreadyRunning);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineRunFrameInvalidDurationZero);
  FOLLY_GTEST_FRIEND_TEST(
      PdesEngine,
      PdesEngineRunFrameInvalidDurationGreaterThanLookahead);
  FOLLY_GTEST_FRIEND_TEST(
      PdesEngine,
      PdesEngineRunFrameInvalidDurationOverflow);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineRunInvalidDurationOverflow);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineCausalityParadox);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineContextlessValidateContext);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, RebalanceEvents);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEnginePauseAndResume);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineBreakpointPause);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineRemoveBreakpoint);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineClearPauseRequest);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineGetBreakpoints);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineMultiThreadedPause);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEngineMultipleSequentialBreakpoints);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEnginePauseAndBreakpointInteraction);
  FOLLY_GTEST_FRIEND_TEST(PdesEngine, PdesEnginePauseWithBoundedDuration);
};

} // namespace packsim::pdes
