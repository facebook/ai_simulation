/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <iostream>

#include "packsim/util/event-sink.h"

namespace packsim {

/**
 * IEventSink that writes one JSON object per line (JSON Lines) to a stream.
 *
 * This is the open-source default. JSON Lines keeps the events greppable as
 * plain text while still parsing cleanly, so a run's log can be piped straight
 * into `jq` without the sink needing a backend.
 *
 * Example usage:
 *
 *      JsonLinesEventSink sink;
 *      PacksimLogger::initialize(runId, sink);
 *
 * NOTE: The stream is captured by reference and must outlive the sink.
 */
class JsonLinesEventSink final : public IEventSink {
 public:
  /**
   * @param out  Stream to write events to. Defaults to stdout.
   */
  explicit JsonLinesEventSink(std::ostream& out = std::cout);

  /**
   * Write one event as a single JSON object followed by a newline.
   *
   * Empty fields are omitted rather than emitted as empty strings, so a line
   * carries only what its event type actually populated. An `EXCEPTION` event
   * flushes before returning, per the `IEventSink` contract.
   *
   * @param event  The event to record.
   */
  void log(const LogEvent& event) override;

 private:
  // Destination stream, owned by the caller.
  std::ostream& out_;
};

} // namespace packsim
