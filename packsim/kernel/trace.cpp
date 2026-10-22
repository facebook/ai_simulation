/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/trace.h"

#include <memory>
#include <stdexcept>
#include <utility>

#include <fmt/format.h>
#include <folly/json.h>
#include <gflags/gflags.h>

#include "packsim/kernel/sim-context.h"

DEFINE_uint32(
    all_conditional_traces_interval_us,
    0,
    "If nonzero, log all conditional traces at this interval in microseconds. "
    "No effect when no conditional trace is registered.");

namespace packsim {

Tracer::Tracer(SimContext* simCtx, ConditionalMap* conditionalMap)
    : simCtx_(simCtx), conditionalMap_(conditionalMap) {
  if (conditionalMap_ == nullptr) {
    return;
  }
  for (const auto& [category, intervalNs] : *conditionalMap_) {
    if (intervalNs == 0) {
      throw std::invalid_argument(
          fmt::format(
              "Simulation.conditionalTraceIntervalsNs['{}'] is 0; a conditional-trace interval must be at least 1 ns",
              category));
    }
  }
}

void Tracer::trace(const std::string& name, const double value) {
  traces_[name].emplace_back(simCtx_->nowNs(), value);
}

void Tracer::logPeriodic(
    const std::string& name,
    const Time interval,
    const std::function<double()>& getVal) {
  trace(name, getVal());

  periodicEvents_[name] = simCtx_->schedule(
      interval, &Tracer::logPeriodic, this, name, interval, getVal);
}

void Tracer::startLogPeriodicSnapToInterval(
    const std::string& name,
    const Time interval,
    const std::function<double()>& getVal) {
  const uint64_t intervalNs = interval.getNs();
  if (intervalNs == 0) {
    throw std::invalid_argument(
        fmt::format(
            "Periodic trace '{}' has an interval under 1 ns; it must be at least 1 ns",
            name));
  }
  const uint64_t currentNsAfterSnap = simCtx_->nowNs() % intervalNs;
  if (currentNsAfterSnap == 0) {
    // log now and reschedule, we are already on an even multiple of interval.
    logPeriodic(name, interval, getVal);
    return;
  }
  const Time delay = Time::fromNs(intervalNs - currentNsAfterSnap);

  periodicEvents_[name] = simCtx_->schedule(
      delay, &Tracer::logPeriodic, this, name, interval, getVal);
}

void Tracer::cancelPeriodic(const std::string& name) {
  periodicEvents_.at(name).cancel();
}

void Tracer::tryCancelPeriodic(const std::string& name) {
  const auto it = periodicEvents_.find(name);
  if (it != periodicEvents_.end()) {
    it->second.cancel();
  }
}

void Tracer::logPeriodicConditional(
    const std::string& category,
    const std::string& name,
    const std::function<double()>& getVal) {
  if (conditionalMap_ == nullptr) {
    throw std::runtime_error(
        fmt::format(
            "Tried to register trace {} in category {} but no conditional map was provided",
            name,
            category));
  }
  const auto it = conditionalMap_->find(category);
  if (it == conditionalMap_->end()) {
    if (FLAGS_all_conditional_traces_interval_us != 0) {
      const auto interval =
          Time::fromUs(FLAGS_all_conditional_traces_interval_us);
      startLogPeriodicSnapToInterval(name, interval, getVal);
    }
    return;
  }
  auto interval = Time::fromNs(it->second);
  if (FLAGS_all_conditional_traces_interval_us != 0) {
    const auto allTracesInterval =
        Time::fromUs(FLAGS_all_conditional_traces_interval_us);
    if (allTracesInterval < interval) {
      interval = allTracesInterval;
    }
  }
  startLogPeriodicSnapToInterval(name, interval, getVal);
}

void Tracer::registerSnapshot(
    const std::string& name,
    const std::function<double()>& getVal) {
  snapshotFunctions_[name] = getVal;
}

folly::F14FastMap<std::string, double> Tracer::getSnapshot() {
  folly::F14FastMap<std::string, double> snap;
  for (const auto& e : snapshotFunctions_) {
    snap[e.first] = e.second();
  }
  return snap;
}

Tracer::TraceEvent* Tracer::startEvent(
    const std::string& name,
    const std::string& category,
    const uint64_t pid,
    const uint64_t tid,
    folly::dynamic args) {
  const auto it = lastEvent_.find({pid, tid});
  if ((it != lastEvent_.end()) && (it->second->endTimeUs == UINT64_MAX)) {
    throw std::runtime_error(
        fmt::format(
            "Previous event on pid={} tid={} started at {}us is not yet complete: name='{}', category='{}'",
            pid,
            tid,
            it->second->startTimeUs,
            name,
            category));
  }
  auto e = std::make_unique<TraceEvent>();
  e->name = name;
  e->category = category;
  e->pid = pid;
  e->tid = tid;
  e->args = std::move(args);
  e->startTimeUs = simCtx_->now().getUs();

  auto p = e.get();
  traceEvents_.emplace_back(std::move(e));
  lastEvent_[{pid, tid}] = p;
  return p;
}

void Tracer::TraceEvent::end(const Time now) {
  if (this->endTimeUs != UINT64_MAX) {
    throw std::runtime_error(
        fmt::format("Event already ended at {}us", this->endTimeUs));
  }
  this->endTimeUs = now.getUs();
}

folly::dynamic Tracer::getTraceEvents() {
  folly::dynamic events = folly::dynamic::array;
  for (const auto& e : traceEvents_) {
    const auto record = [&e](const uint64_t ts, const char* phase) {
      folly::dynamic event = folly::dynamic::object;
      event["name"] = e->name;
      event["cat"] = e->category;
      event["ts"] = ts;
      event["pid"] = e->pid;
      event["tid"] = e->tid;
      event["ph"] = phase;
      return event;
    };
    folly::dynamic beginRecord = record(e->startTimeUs, "B");
    beginRecord["args"] = e->args;
    events.push_back(std::move(beginRecord));
    // endTimeUs is still UINT64_MAX if the event never ended; leave it open.
    if (e->endTimeUs != UINT64_MAX) {
      events.push_back(record(e->endTimeUs, "E"));
    }
  }

  return events;
}

} // namespace packsim
