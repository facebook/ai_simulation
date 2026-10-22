/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <folly/memory/not_null.h>

#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/duration.h"
#include "packsim/kernel/pdes/PdesEngine.h"
#include "packsim/kernel/pdes/SimulationContext.h"
#include "packsim/kernel/time.h"

namespace packsim {

class PacketPtr;

// A single packet-drop event emitted by the simulation core (switch and
// endpoint drop sites) to a packet-dropped subscriber, e.g. a debugger's drop
// breakpoint. `pkt` is a borrowed pointer, valid only for the duration of the
// onPacketDropped() call (never null at any drop site).
// NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
struct DropEvent {
  // Name of the device that dropped the packet. A borrowed reference (like
  // `pkt`), valid only during onPacketDropped(); avoids a per-drop string copy
  // on the hot path when no subscriber consumes it.
  const std::string& deviceName;
  // FlexId of that device.
  FlexDeviceId deviceId;
  // Port the packet was dropped on.
  uint32_t port{};
  // Why the packet was dropped.
  packsim::switches::thrift::PacketDropReason reason{};
  // Borrowed pointer to the dropped packet, valid only during
  // onPacketDropped(). No default initializer: folly::not_null deletes its
  // default constructor, so `pkt{}` does not compile. Every DropEvent site sets
  // this explicitly.
  folly::not_null<const PacketPtr*> pkt;
  // Total queue occupancy of the dropping device at drop time, in bytes.
  uint32_t queueDepthBytes{};
  // Simulation time of the drop, in femtoseconds.
  uint64_t simTimeFs{};
};

class Scheduler {
 public:
  Scheduler();

  using EventHandle = packsim::pdes::Event;
  using SimulationContext = ::packsim::pdes::SimulationContext;

  /**
   * Schedule `std::invoke(func, std::move(args)...)` on `ctx` at `timeNow()`
   * plus `delay`. `timeNow()` is the calling context's current time while the
   * engine runs; otherwise it is where the last `run()` stopped (0 before the
   * first run). `func` and `args` are decay-copied into the event, which runs
   * once and passes the stored arguments to `func` as rvalues: a callee takes
   * each by value (taking it over) or by `const&`, never by non-const
   * reference. While the engine runs and `ctx` is not the calling context,
   * `delay` must be at least the lookahead time.
   *
   * @param ctx    Context that runs the event.
   * @param delay  Offset from `timeNow()`.
   * @param func   Callable, function pointer or member-function pointer.
   * @param args   Arguments stored in the event and passed to `func` as
   *               rvalues.
   *
   * @return  Handle that can cancel the event before it runs.
   *
   * @throw std::runtime_error if the event time is before `ctx`'s current
   *        time, or if called while the engine runs from a thread outside
   *        the PDES pool.
   */
  template <typename TFunc, typename... TArgs>
  EventHandle scheduleAfter(
      SimulationContext* ctx,
      const Duration delay,
      TFunc&& func,
      TArgs&&... args) {
    const uint64_t timeFs = delay.getFs() + engine_->nowFs();
    return engine_->scheduleAt(
        ctx, timeFs, std::forward<TFunc>(func), std::forward<TArgs>(args)...);
  }

  /**
   * `scheduleAfter` with the delay held in a `Time`.
   *
   * @param ctx    Context that runs the event.
   * @param delay  Offset from `timeNow()`.
   * @param func   Callable, function pointer or member-function pointer.
   * @param args   Arguments stored in the event and passed to `func`.
   *
   * @return  Handle that can cancel the event before it runs.
   *
   * @throw std::runtime_error as the `Duration` overload does.
   */
  template <typename TFunc, typename... TArgs>
  EventHandle scheduleAfter(
      SimulationContext* ctx,
      Time const delay,
      TFunc&& func,
      TArgs&&... args) {
    return scheduleAfter(
        ctx,
        Duration(delay.getFs()),
        std::forward<TFunc>(func),
        std::forward<TArgs>(args)...);
  }

  uint64_t getTotalEvents();

  Time timeNow();

  Time getTimeUntilEvent(const EventHandle& event);

  void run();

  void run(const Time time);

  bool stop();

  void setLookaheadTimeFs(uint64_t lookaheadTimeFs);

  void buildNewEngine();

  void registerContext(SimulationContext* ctx);

  Time getLookaheadTime() const;

  // Request the engine to pause at the next frame boundary.
  void requestPause();

  // Clear a previously set pause request.
  void clearPauseRequest();

  // Not thread-safe: the four breakpoint methods read/mutate the engine's
  // breakpoint set directly (no lock), and the run loop erases reached
  // breakpoints between frames. Call them only while the engine is not running
  // (see isNotRunning()); concurrent access during run() is a data race.
  //
  // Add a time-based breakpoint (femtoseconds).
  void addBreakpointFs(uint64_t timeFs);

  // Remove a specific breakpoint (femtoseconds).
  void removeBreakpointFs(uint64_t timeFs);

  // Remove all breakpoints.
  void clearAllBreakpoints();

  // Get all currently set breakpoints (femtoseconds).
  std::set<uint64_t> getBreakpointsFs() const;

  // Whether a breakpoint is set at timeFs (femtoseconds), without copying the
  // breakpoint set. Same not-thread-safe caveat as the methods above.
  bool hasBreakpointFs(uint64_t timeFs) const;

  // Whether the engine is currently not running.
  bool isNotRunning() const;

  // Time of the last executed event in femtoseconds. Reads the engine's
  // lastEventTimeFs_ under startStopMutex_ (shared), so it is safe to call from
  // any thread (including a controller thread outside the engine) without
  // racing run().
  uint64_t getLastEventTimeFs() const;

  // Prints each PDES worker thread's CPU time across its contexts to stdout
  // when --pdes_print_cpu_load is set. Reads every registered context, so call
  // it while they are all alive and the engine is not running.
  void printExitStats() const;

  // Packet-dropped event. A subscriber is installed and cleared only while the
  // engine is stopped (see isNotRunning()); the switch and endpoint drop sites
  // raise it during run() through onPacketDropped(). Held here because the
  // Scheduler is the one simulation-wide object every node can reach
  // (SimContext is per node), which keeps the switch and endpoint layers
  // independent of whatever subscribes.
  using OnPacketDroppedCallback = std::function<void(const DropEvent&)>;
  void setOnPacketDroppedCallback(const OnPacketDroppedCallback& cb);
  void onPacketDropped(const DropEvent& event);

 private:
  std::unique_ptr<packsim::pdes::PdesEngine> engine_;
  std::vector<SimulationContext*> contexts_;
  OnPacketDroppedCallback onPacketDropped_;
};

} // namespace packsim
