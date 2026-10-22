/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/packsim-logger.h"

#include <sstream>
#include <string>
#include <utility>

#include <folly/ExceptionString.h>
#include <folly/debugging/exception_tracer/SmartExceptionTracer.h>
#include <folly/json/dynamic.h>
#include <folly/json/json.h>

namespace packsim {

namespace {
template <typename T>
std::string toString(T const& t) {
  std::ostringstream out;
  out << t;
  return out.str();
}
} // namespace

IEventSink * FOLLY_NULLABLE & PacksimLogger::sink() {
  static IEventSink* s = nullptr;
  return s;
}

std::string& PacksimLogger::runId() {
  static std::string id;
  return id;
}

void PacksimLogger::initialize(std::string const& id, IEventSink& eventSink) {
  folly::exception_tracer::installHandlers();
  runId() = id;
  sink() = &eventSink;
}

void PacksimLogger::log(const std::exception& ex) {
  // Read the sink once: two calls would be two nullable reads as far as the
  // nullability analysis is concerned, so the guard would not cover the use.
  auto* const eventSink = sink();
  if (eventSink == nullptr) {
    return;
  }

  LogEvent event;
  event.type = LogDataType::EXCEPTION;
  event.runId = runId();
  event.message = toString(folly::exceptionStr(ex));
  event.stackTrace = toString(folly::exception_tracer::getTrace(ex));
  eventSink->log(event);
}

void PacksimLogger::log(
    const LogDataType logType,
    std::source_location location) {
  auto* const eventSink = sink();
  if (eventSink == nullptr) {
    return;
  }

  LogEvent event;
  event.type = logType;
  event.runId = runId();
  event.location = std::move(location);
  eventSink->log(event);
}

void PacksimLogger::logProgress(
    const double fracCompleted,
    const uint64_t bytesRcvd,
    std::source_location location) {
  auto* const eventSink = sink();
  if (eventSink == nullptr) {
    return;
  }

  folly::dynamic data = folly::dynamic::object;
  data["frac_completed_pct"] = fracCompleted;
  data["bytes_received"] = bytesRcvd;

  LogEvent event;
  event.type = LogDataType::PROGRESS;
  event.runId = runId();
  event.jsonData = folly::toJson(data);
  event.location = std::move(location);
  eventSink->log(event);
}

} // namespace packsim
