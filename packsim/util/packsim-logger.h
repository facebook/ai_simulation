/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <exception>
#include <source_location>
#include <string>

#include <folly/CppAttributes.h>

#include "packsim/util/event-sink.h"

namespace packsim {

/**
 * Packsim run logger.
 *
 * A static facade rather than an injected object because it is called from
 * deep inside the simulation loop, where threading a logger through would
 * touch every layer. What it writes to is injected instead: initialize() takes
 * the sink, so this target depends on no particular logging backend.
 *
 * Every log call is a no-op until initialize() has been called, so a run that
 * does not want logging (a test, or --is_test) simply never initializes.
 */
class PacksimLogger {
 public:
  /**
   * Point the logger at a sink and start logging under a run id.
   *
   * @param runId  Identifies this run in the sink's records.
   * @param sink   Destination for events. Must outlive every log call.
   */
  static void initialize(std::string const& runId, IEventSink& sink);

  /**
   * Log an event and the call site it came from.
   *
   * @param dataType  What happened.
   * @param location  Defaulted; callers do not pass this.
   */
  static void log(
      const LogDataType dataType,
      const std::source_location location = std::source_location::current());

  /**
   * Log an exception together with its stack trace.
   *
   * @param ex  The exception to record.
   */
  static void log(const std::exception& ex);

  /**
   * Log simulation progress.
   *
   * @param fracCompleted  Completion as a percentage.
   * @param bytesRcvd      Bytes received so far in the run.
   * @param location       Defaulted; callers do not pass this.
   */
  static void logProgress(
      const double fracCompleted,
      const uint64_t bytesRcvd,
      const std::source_location location = std::source_location::current());

 private:
  // Null until initialize() runs, which is what makes every call inert.
  static IEventSink * FOLLY_NULLABLE & sink();
  static std::string& runId();
};
} // namespace packsim
