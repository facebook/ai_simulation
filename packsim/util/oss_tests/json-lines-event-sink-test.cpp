/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <ostream>
#include <source_location>
#include <sstream>
#include <string>
#include <vector>

#include <folly/json/dynamic.h>
#include <folly/json/json.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/event-sink.h"
#include "packsim/util/json-lines-event-sink.h"

namespace packsim {
namespace {

using ::testing::HasSubstr;

class JsonLinesEventSinkTest : public ::testing::Test {
 protected:
  // Parse the single line the sink wrote. Asserting on parsed JSON rather than
  // on the raw text keeps the cases independent of key order and spacing.
  folly::dynamic parseSoleLine() {
    const auto text = out_.str();
    EXPECT_EQ(std::count(text.begin(), text.end(), '\n'), 1)
        << "expected exactly one line, got: " << text;
    return folly::parseJson(text);
  }

  std::ostringstream out_;
  JsonLinesEventSink sink_{out_};
};

TEST_F(JsonLinesEventSinkTest, WritesTypeAndRunIdAsOneJsonLine) {
  sink_.log(LogEvent{.type = LogDataType::BEGIN, .runId = "run-1"});

  const auto line = parseSoleLine();

  EXPECT_EQ(line["type"].asString(), "begin");
  EXPECT_EQ(line["runId"].asString(), "run-1");
}

TEST_F(JsonLinesEventSinkTest, RendersEveryLogDataTypeAsItsOwnName) {
  const auto render = [](LogDataType type) {
    std::ostringstream out;
    JsonLinesEventSink sink{out};
    sink.log(LogEvent{.type = type, .runId = "run-1"});
    return folly::parseJson(out.str())["type"].asString();
  };

  const std::vector<std::string> rendered = {
      render(LogDataType::BEGIN),
      render(LogDataType::END),
      render(LogDataType::EXCEPTION),
      render(LogDataType::PROGRESS)};

  const std::vector<std::string> expected = {
      "begin", "end", "exception", "progress"};
  EXPECT_EQ(rendered, expected);
}

TEST_F(JsonLinesEventSinkTest, OmitsFieldsTheEventDidNotPopulate) {
  sink_.log(LogEvent{.type = LogDataType::BEGIN, .runId = "run-1"});

  const auto line = parseSoleLine();

  // A BEGIN event carries no message, stack trace or data, and empty strings
  // for them would be indistinguishable from a genuinely empty value.
  EXPECT_FALSE(line.contains("message"));
  EXPECT_FALSE(line.contains("stackTrace"));
  EXPECT_FALSE(line.contains("data"));
}

TEST_F(JsonLinesEventSinkTest, NestsJsonDataAsAnObjectRatherThanAString) {
  sink_.log(
      LogEvent{
          .type = LogDataType::PROGRESS,
          .runId = "run-1",
          .jsonData = R"({"packets": 42})"});

  const auto line = parseSoleLine();

  ASSERT_TRUE(line["data"].isObject()) << folly::toJson(line);
  EXPECT_EQ(line["data"]["packets"].asInt(), 42);
}

TEST_F(JsonLinesEventSinkTest, KeepsUnparseableJsonDataVerbatim) {
  sink_.log(
      LogEvent{
          .type = LogDataType::PROGRESS,
          .runId = "run-1",
          .jsonData = "not json at all"});

  // Losing the whole event over a malformed payload would be worse than
  // carrying the payload through as text.
  EXPECT_EQ(parseSoleLine()["data"].asString(), "not json at all");
}

TEST_F(JsonLinesEventSinkTest, RecordsTheCallSite) {
  sink_.log(
      LogEvent{
          .type = LogDataType::BEGIN,
          .runId = "run-1",
          .location = std::source_location::current()});

  const auto location = parseSoleLine()["location"];

  EXPECT_THAT(
      location["file"].asString(), HasSubstr("json-lines-event-sink-test"));
  EXPECT_GT(location["line"].asInt(), 0);
}

TEST_F(JsonLinesEventSinkTest, WritesOneLinePerEvent) {
  sink_.log(LogEvent{.type = LogDataType::BEGIN, .runId = "run-1"});
  sink_.log(LogEvent{.type = LogDataType::END, .runId = "run-1"});

  const auto text = out_.str();

  EXPECT_EQ(std::count(text.begin(), text.end(), '\n'), 2) << text;
}

// A `std::ostringstream` cannot report whether it was flushed, so count
// `sync()` calls instead -- that is what `ostream::flush()` invokes on the
// underlying buffer, and a buffered `<<` does not.
class CountingStringBuf : public std::stringbuf {
 public:
  int syncCount() const {
    return syncCount_;
  }

 protected:
  int sync() override {
    ++syncCount_;
    return std::stringbuf::sync();
  }

 private:
  int syncCount_{0};
};

class JsonLinesEventSinkFlushTest : public ::testing::Test {
 protected:
  CountingStringBuf buf_;
  std::ostream out_{&buf_};
  JsonLinesEventSink sink_{out_};
};

TEST_F(JsonLinesEventSinkFlushTest, FlushesOnAnExceptionEvent) {
  sink_.log(LogEvent{.type = LogDataType::EXCEPTION, .runId = "run-1"});

  // The process may be on its way down by the time an exception is logged, so
  // the line has to reach the stream before log() returns.
  EXPECT_GT(buf_.syncCount(), 0);
}

TEST_F(JsonLinesEventSinkFlushTest, DoesNotFlushOnANonExceptionEvent) {
  sink_.log(LogEvent{.type = LogDataType::PROGRESS, .runId = "run-1"});

  // Every other type may be delivered lazily; flushing each one would make a
  // progress-heavy run pay a syscall per event.
  EXPECT_EQ(buf_.syncCount(), 0);
}

} // namespace
} // namespace packsim
