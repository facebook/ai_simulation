/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/json-lines-event-sink.h"

#include <ostream>
#include <string>

#include <folly/json/dynamic.h>
#include <folly/json/json.h>

namespace packsim {

namespace {

char const* toString(const LogDataType type) {
  switch (type) {
    case LogDataType::BEGIN:
      return "begin";
    case LogDataType::END:
      return "end";
    case LogDataType::EXCEPTION:
      return "exception";
    case LogDataType::PROGRESS:
      return "progress";
  }
  // Deliberately no `default:` -- an added enumerator should trip -Wswitch here
  // rather than be silently mislabelled. This fallback is only for a value that
  // is not any enumerator at all, where falling off the end would be undefined
  // behaviour; a logger must not take the process down over a bad label.
  return "unknown";
}

} // namespace

JsonLinesEventSink::JsonLinesEventSink(std::ostream& out) : out_{out} {}

void JsonLinesEventSink::log(const LogEvent& event) {
  folly::dynamic line = folly::dynamic::object("type", toString(event.type))(
      "runId", event.runId);

  if (!event.message.empty()) {
    line["message"] = event.message;
  }
  if (!event.stackTrace.empty()) {
    line["stackTrace"] = event.stackTrace;
  }
  if (!event.jsonData.empty()) {
    // Nest the parsed object rather than the raw text, so a consumer does not
    // have to decode a JSON string out of a JSON field. Text that does not
    // parse is kept verbatim instead of costing the whole event.
    try {
      line["data"] = folly::parseJson(event.jsonData);
    } catch (const std::exception&) {
      line["data"] = event.jsonData;
    }
  }

  line["location"] = folly::dynamic::object("file", event.location.file_name())(
      "line", event.location.line())(
      "function", event.location.function_name());

  out_ << folly::toJson(line) << '\n';

  if (event.type == LogDataType::EXCEPTION) {
    // The process may be on its way down; an unflushed line would be lost.
    out_.flush();
  }
}

} // namespace packsim
