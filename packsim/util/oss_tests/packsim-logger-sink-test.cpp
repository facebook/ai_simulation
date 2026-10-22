/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/event-sink.h"
#include "packsim/util/packsim-logger.h"

namespace packsim {
namespace {

using ::testing::HasSubstr;

// The initialized path lives in its own binary. PacksimLogger keeps the sink
// in a function-local static with no way to unset it, so a test that
// initializes would turn every later test in the same binary into a live log
// call. Splitting the two groups by binary is what keeps
// packsim-logger-test.cpp's uninitialized-guard tests honest under
// --gtest_shuffle, rather than relying on declaration order.
class RecordingSink : public IEventSink {
 public:
  void log(const LogEvent& event) override {
    events.emplace_back(event);
  }

  std::vector<LogEvent> events;
};

// Before this seam existed the only sink was the network-backed one, so none
// of this could be asserted in a unit test that is not allowed to reach the
// network.
class PacksimLoggerSinkTest : public ::testing::Test {
 protected:
  void SetUp() override {
    PacksimLogger::initialize("run-1234", sink_);
  }

  RecordingSink sink_;
};

TEST_F(PacksimLoggerSinkTest, CallSiteLogCarriesTypeAndRunId) {
  PacksimLogger::log(LogDataType::BEGIN);

  ASSERT_EQ(sink_.events.size(), 1);
  EXPECT_EQ(sink_.events[0].type, LogDataType::BEGIN);
  EXPECT_EQ(sink_.events[0].runId, "run-1234");
}

// The call site must be the caller's, not somewhere inside PacksimLogger --
// that is the whole point of the defaulted source_location parameter, and it
// is easy to break by adding a forwarding layer that re-defaults it.
TEST_F(PacksimLoggerSinkTest, CallSiteLogRecordsTheCallersLocation) {
  PacksimLogger::log(LogDataType::END);

  ASSERT_EQ(sink_.events.size(), 1);
  EXPECT_THAT(
      std::string(sink_.events[0].location.file_name()),
      HasSubstr("packsim-logger-sink-test.cpp"));
}

TEST_F(PacksimLoggerSinkTest, ExceptionLogCarriesMessageAndTrace) {
  try {
    throw std::runtime_error("simulated failure");
  } catch (const std::exception& ex) {
    PacksimLogger::log(ex);
  }

  ASSERT_EQ(sink_.events.size(), 1);
  EXPECT_EQ(sink_.events[0].type, LogDataType::EXCEPTION);
  EXPECT_THAT(sink_.events[0].message, HasSubstr("simulated failure"));
  EXPECT_FALSE(sink_.events[0].stackTrace.empty());
}

TEST_F(PacksimLoggerSinkTest, ProgressLogCarriesCountersAsJson) {
  PacksimLogger::logProgress(/*fracCompleted=*/42.5, /*bytesRcvd=*/1024);

  ASSERT_EQ(sink_.events.size(), 1);
  EXPECT_EQ(sink_.events[0].type, LogDataType::PROGRESS);
  EXPECT_THAT(sink_.events[0].jsonData, HasSubstr("\"frac_completed_pct\""));
  EXPECT_THAT(sink_.events[0].jsonData, HasSubstr("42.5"));
  EXPECT_THAT(sink_.events[0].jsonData, HasSubstr("\"bytes_received\""));
  EXPECT_THAT(sink_.events[0].jsonData, HasSubstr("1024"));
}

// Progress events populate the same call-site column as the BEGIN/END ones,
// so they need the caller's location too. Asserting it separately from the
// END case matters: logProgress reaches the sink by a different path, and a
// LogEvent left with a default-constructed location still logs happily -- it
// just records file "" line 0, which no test would notice without this.
TEST_F(PacksimLoggerSinkTest, ProgressLogRecordsTheCallersLocation) {
  PacksimLogger::logProgress(/*fracCompleted=*/1.0, /*bytesRcvd=*/1);

  ASSERT_EQ(sink_.events.size(), 1);
  EXPECT_THAT(
      std::string(sink_.events[0].location.file_name()),
      HasSubstr("packsim-logger-sink-test.cpp"));
  EXPECT_GT(sink_.events[0].location.line(), 0);
}

} // namespace
} // namespace packsim
