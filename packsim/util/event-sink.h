/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <source_location>
#include <string>

namespace packsim {

enum class LogDataType {
  BEGIN,
  END,
  EXCEPTION,
  PROGRESS,
};

/**
 * One structured event from a simulation run.
 *
 * Which fields carry a value depends on `type`; the rest stay empty. Keeping
 * the payload as plain strings is deliberate -- it is what lets this header
 * stay free of any particular logging backend's types.
 */
struct LogEvent {
  // What happened.
  LogDataType type{LogDataType::BEGIN};
  // Identifies the simulation run this event belongs to.
  std::string runId{};
  // EXCEPTION only: the exception's message.
  std::string message{};
  // EXCEPTION only: the captured stack trace.
  std::string stackTrace{};
  // PROGRESS only: a JSON object of progress counters.
  std::string jsonData{};
  // Where in packsim the event was raised.
  std::source_location location{};
};

/**
 * Destination for the structured events packsim emits about a run.
 *
 * The backend sits behind this interface and is injected at
 * PacksimLogger::initialize, so packsim depends on no particular event store.
 */
class IEventSink {
 public:
  virtual ~IEventSink() = default;

  /**
   * Record one event.
   *
   * An implementation is expected to flush an EXCEPTION event before
   * returning, because the process may be on its way down; every other type
   * may be delivered asynchronously.
   *
   * @param event  The event to record.
   */
  virtual void log(const LogEvent& event) = 0;
};

} // namespace packsim
