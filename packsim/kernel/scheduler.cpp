/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/scheduler.h"

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

#include <fmt/format.h>
#include <folly/logging/xlog.h>
#include <folly/system/HardwareConcurrency.h>
#include <gflags/gflags.h>

#include "packsim/kernel/duration.h"
#include "packsim/kernel/time.h"
// @oss-disable

// 0 selects the default worker count; see useSmallDefaultThreadCount()
// below.
DEFINE_uint32(
    pdes_threads,
    0,
    "# of threads for PDES Scheduler to use (0 = default)");

using packsim::pdes::PdesEngine;

constexpr uint64_t kDefaultLookaheadFs = 1'000'000; // 1ns

// Small default worker count, used in place of one-per-core. Fanning out to
// one worker per core on a many-core host makes simulations slow and
// memory-hungry — pathologically so under ThreadSanitizer, whose
// instrumentation cost is superlinear in thread count. 4 keeps cross-thread
// coverage while staying fast, matching the value the link-layer test fixtures
// pin. An explicit --pdes_threads always overrides.
constexpr size_t kSmallDefaultThreads = 4;

namespace packsim {

namespace {
// Whether to default to kSmallDefaultThreads rather than one worker per core.
//
// @oss-disable
// Always true: every binary gets the small default. // @oss-enable
//
// One worker per core is the worse default to fall back on: it is pathological
// on many-core hosts — a test that passes in under 3 minutes at 4 threads has
// taken over 10 minutes with one worker per core — while on more modest hosts
// the difference is small. Anyone who wants the cores passes --pdes_threads.
bool useSmallDefaultThreadCount() {
  // @oss-disable
  return true; // @oss-enable
}
} // namespace

Scheduler::Scheduler() {
  buildNewEngine();
}

void Scheduler::buildNewEngine() {
  const size_t pdesThreads = FLAGS_pdes_threads > 0
      ? FLAGS_pdes_threads
      : (useSmallDefaultThreadCount() ? kSmallDefaultThreads
                                      : folly::available_concurrency());
  XLOGF(INFO, "Creating PDES engine with {} threads", pdesThreads);
  engine_ = std::make_unique<PdesEngine>(kDefaultLookaheadFs, pdesThreads);
  for (auto ctx : contexts_) {
    engine_->registerContext(ctx);
  }
}

uint64_t Scheduler::getTotalEvents() {
  return engine_->getEventsExecuted();
}

Time Scheduler::getTimeUntilEvent(const EventHandle& event) {
  const auto timeFs = event.getEventTimeFs();
  if (timeFs == 0) {
    return Time(0);
  }
  return Time(timeFs - engine_->nowFs());
}

Time Scheduler::timeNow() {
  const auto timeFs = engine_->nowFs();
  return Time(timeFs);
}

void Scheduler::run() {
  engine_->run();
}

void Scheduler::run(const Time time) {
  engine_->run(time.getFs());
}

void Scheduler::runUntil(const Time end) {
  // While the engine runs, timeNow() is the calling event's context time;
  // return before reading it, as run() does nothing then.
  if (!isNotRunning()) {
    return;
  }
  const Time now = timeNow();
  if (end < now) {
    throw std::invalid_argument(
        fmt::format(
            "runUntil end time {} fs is before the current time {} fs",
            end.getFs(),
            now.getFs()));
  }
  // PdesEngine::run(0) runs until no events are left, so a zero span must not
  // reach it.
  const Duration span = Duration::between(now, end);
  if (span.isZero()) {
    return;
  }
  engine_->run(span.getFs());
}

bool Scheduler::stop() {
  return engine_->stop();
}

void Scheduler::setLookaheadTimeFs(const uint64_t lookaheadTimeFs) {
  engine_->setLookaheadTimeFs(lookaheadTimeFs);
}

void Scheduler::registerContext(SimulationContext* simCtx) {
  contexts_.emplace_back(simCtx);
  engine_->registerContext(simCtx);
}

Time Scheduler::getLookaheadTime() const {
  return Time(engine_->getLookaheadTimeFs());
}

void Scheduler::requestPause() {
  engine_->requestPause();
}

void Scheduler::clearPauseRequest() {
  engine_->clearPauseRequest();
}

void Scheduler::addBreakpointFs(const uint64_t timeFs) {
  engine_->addBreakpointFs(timeFs);
}

void Scheduler::removeBreakpointFs(const uint64_t timeFs) {
  engine_->removeBreakpointFs(timeFs);
}

void Scheduler::clearAllBreakpoints() {
  engine_->clearAllBreakpoints();
}

std::set<uint64_t> Scheduler::getBreakpointsFs() const {
  return engine_->getBreakpointsFs();
}

bool Scheduler::hasBreakpointFs(const uint64_t timeFs) const {
  return engine_->hasBreakpointFs(timeFs);
}

bool Scheduler::isNotRunning() const {
  return engine_->isNotRunning();
}

uint64_t Scheduler::getLastEventTimeFs() const {
  return engine_->getLastEventTimeFs();
}

void Scheduler::printExitStats() const {
  engine_->printExitStats();
}

void Scheduler::setOnPacketDroppedCallback(const OnPacketDroppedCallback& cb) {
  // The subscriber is read by onPacketDropped() during run(); replacing it is
  // only safe while the engine is stopped. Enforce that documented contract so
  // a concurrent set during a run fails fast instead of racing.
  XCHECK(isNotRunning()) << "setOnPacketDroppedCallback must only be called "
                            "while the engine is stopped";
  onPacketDropped_ = cb;
}

void Scheduler::onPacketDropped(const DropEvent& event) {
  // No subscriber is the normal configuration: nothing in the simulator
  // subscribes (tests do, as a debugger's drop breakpoint would), so each drop
  // is otherwise just counted at its site.
  if (onPacketDropped_) {
    onPacketDropped_(event);
  }
}

} // namespace packsim
