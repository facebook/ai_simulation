/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <queue>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "packsim/kernel/pdes/Event.h"

namespace packsim::pdes {

struct SimulationContext {
  explicit SimulationContext(const std::string& name)
      : name(name), ctxIdx(idCtr_++) {}

  SimulationContext()
      : SimulationContext(fmt::format("SimContext{}", idCtr_)) {}

  SimulationContext(const SimulationContext&) = delete;
  SimulationContext(SimulationContext&&) = delete;
  SimulationContext& operator=(const SimulationContext&) = delete;
  SimulationContext& operator=(SimulationContext&&) = delete;

  ~SimulationContext() {
    while (!eventQ.empty()) {
      auto* event = eventQ.top();
      eventQ.pop();
      delete event;
    }
  }

  uint64_t nowFs() const noexcept {
    return currentTimeFs;
  }

  uint64_t nowPs() const noexcept {
    return currentTimeFs / 1'000UL;
  }

  uint64_t nowNs() const noexcept {
    return currentTimeFs / 1'000'000UL;
  }

  uint64_t nowUs() const noexcept {
    return currentTimeFs / 1'000'000'000UL;
  }

  uint64_t nowMs() const noexcept {
    return currentTimeFs / 1'000'000'000'000UL;
  }

  std::string nameWithPtr() const;

  // Advanced as this context's events execute; atomic so other threads can
  // sample it while the engine is running.
  std::atomic_uint_fast64_t currentTimeFs{0};

  // Name used to identify this context in logs (see nameWithPtr()).
  std::string name;

  // Per-context priority queue of pending events, ordered by event time.
  std::priority_queue<RawEvent*, std::vector<RawEvent*>, EventComparator>
      eventQ;

  // Monotonic per-context event id, used to build a stable event sort key.
  uint32_t nextEventId{0};

  // Track active execution time of this context.
  int64_t cpuTimeSpentNs{0};

  // Unique index of this context, assigned at construction from idCtr_.
  const uint64_t ctxIdx;

  // Id of the engine that last assigned this context to one of its worker
  // threads; 0 until an engine first sees the context. Engine ids start at 1
  // and are never reused, so a context handed to a new engine is assigned
  // again there. The engine a context left must not run again (see
  // PdesEngine::contextToThreadNum()).
  std::atomic_uint64_t engineId{0};

  // Index of the worker thread of engine `engineId` that runs this context's
  // events.
  std::atomic_size_t threadIdx{0};

 private:
  // Process-wide counter used to assign a unique ctxIdx to each context.
  static uint64_t idCtr_;
};

} // namespace packsim::pdes
