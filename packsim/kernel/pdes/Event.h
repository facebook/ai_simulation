/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <utility>

#include <folly/CPortability.h>
#include <folly/CppAttributes.h>
#include <folly/Function.h>
#include <folly/Likely.h>

namespace packsim::pdes {

struct SimulationContext;

struct RawEvent {
  RawEvent() = default;

  // Action to take when the time comes. Callables of up to 48 bytes that move
  // without throwing, such as a member function, its object and a PacketPtr,
  // are stored inline, so scheduling them does not allocate.
  // 64 bytes, 16-byte aligned: declared first so that its alignment adds no
  // padding and the event packs into 96 bytes.
  folly::Function<void()> func;

  // Event time in femtoseconds (1e-15 seconds).
  // 8 bytes
  uint64_t timeFs = 0;

  // Index of context that scheduled this event, for consistent sorting
  // 8 bytes
  uint64_t sortId = 0;

  // Context used to schedule this event. Context is a pointer to something in
  // memory. Its purpose is to just glue the events together for the sake of
  // event scheduling. All events with the same context run on the same thread.
  // 8 bytes
  SimulationContext* context{};

  // Used to track "versions" in case something else holds a reference to this
  // event past its lifetime
  // 4 bytes
  std::atomic_uint32_t version{0};

  // Whether this event should be executed comes time.
  // This can be used to cancel events before scheduled time eviction.
  // 1 byte
  std::atomic_bool isActive = true;

  // Number of low-order bits of sortId reserved for the per-context event id;
  // the scheduling context index is packed into the remaining high-order bits.
  static constexpr uint64_t kSortIdEventIdBits = 32;

  inline void setSortId(uint64_t ctxIdx, uint64_t eventId) noexcept {
    sortId = (ctxIdx << kSortIdEventIdBits) | eventId;
  }

  // Every access to `version` and `isActive` is relaxed. Only the thread that
  // runs an event's context pops, runs, skips and resets the event, and a
  // handle may read or cancel it only on that thread or between frames. Program
  // order, or the event queues and the frame barriers (which also order a
  // context's move to another thread), order each access against those it must
  // see, and none of them publishes other data, so they only need to be
  // tear-free: the bump needs no read-modify-write and no store a fence. A
  // handle used from another thread within a frame races the event under any
  // memory order.
  void reset() {
    version.store(
        version.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    sortId = 0;
    timeFs = 0;
    func = nullptr;
    context = nullptr;
    isActive.store(true, std::memory_order_relaxed);
  }
};

static_assert(
    sizeof(RawEvent) <= 96,
    "RawEvent outgrew 96 bytes; keep func first so that its 16-byte alignment "
    "adds no padding");

/**
 * Handle to a scheduled event, returned by the engine's schedule calls, to
 * cancel the event before it runs or ask whether it is still pending. It holds
 * the event's pointer and version, so it may outlive the event: once the event
 * has run or been discarded, its slot is recycled under a new version and the
 * handle reads as stale: cancel() does nothing and isActive() returns false.
 *
 * Example usage:
 *
 *      Event timeout = engine.schedule(delayFs, &Conn::onTimeout, this);
 *      ...
 *      timeout.cancel(); // no-op if onTimeout already ran
 *
 * NOTE: Use a handle only on the worker thread that runs the event's context,
 * or between runs. Every handle packsim keeps comes from scheduling on its
 * owner's own context and stays there, so the event is cancelled through it,
 * run and recycled on one worker, in program order. The frame barriers order
 * the rare accesses made between runs, and a context's move to another worker,
 * which rebalancing does only between frames. The accessors rely on this rule
 * and use relaxed atomics (see RawEvent::reset()); a handle used from another
 * worker during a frame races the event under any memory order.
 */
struct Event {
  Event() = default;

  explicit Event(RawEvent* rawEventPtr)
      : rawEvent(rawEventPtr),
        rawEventVersion(rawEventPtr->version.load(std::memory_order_relaxed)) {}

  /**
   * Stop the event's callback from running; the engine discards the event
   * when it reaches the front of its context's queue. No-op on a
   * default-constructed handle, on a stale handle (the event already ran or
   * was discarded and its slot was recycled), and on an already-cancelled
   * event, so callers need not check isActive() first. Called from the
   * event's own callback, it does not stop that run; it only makes isActive()
   * return false.
   */
  void cancel() const noexcept {
    // cancel()'s noexcept guarantee depends on isValid() being noexcept;
    // pin that at compile time so a future logging/dispatch addition to
    // isValid() that introduces a throwing path fails to build here
    // rather than silently terminating from inside a noexcept call.
    static_assert(noexcept(std::declval<const Event&>().isValid()));
    if (isValid()) {
      // Relaxed (see RawEvent::reset()): program order or the frame barriers
      // already order this store before the engine checks the flag.
      rawEvent->isActive.store(false, std::memory_order_relaxed);
    }
  }

  // True from scheduling until the event is cancelled or its callback has
  // returned; still true while the callback itself runs.
  bool isActive() const noexcept {
    if (isValid()) {
      return rawEvent->isActive.load(std::memory_order_relaxed);
    }
    return false;
  }

  uint64_t getEventTimeFs() const noexcept {
    if (isValid()) {
      return rawEvent->timeFs;
    }
    return 0;
  }

  bool isValid() const noexcept {
    return rawEvent != nullptr &&
        rawEvent->version.load(std::memory_order_relaxed) == rawEventVersion;
  }

  RawEvent* rawEvent{nullptr};
  uint32_t rawEventVersion{};
};

struct EventComparator {
  FOLLY_ALWAYS_INLINE bool operator()(
      RawEvent* const& lhs,
      RawEvent* const& rhs) const noexcept {
    if (LIKELY(lhs->timeFs != rhs->timeFs)) {
      return lhs->timeFs > rhs->timeFs;
    }
    return lhs->sortId > rhs->sortId;
  }
};

} // namespace packsim::pdes
