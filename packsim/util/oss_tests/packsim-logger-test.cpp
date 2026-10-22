/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <limits>
#include <stdexcept>

#include <gtest/gtest.h>

#include "packsim/util/packsim-logger.h"

namespace packsim {
namespace {

// Every PacksimLogger entry point is gated on a sink that is only set by
// initialize(). Until then the logger must be completely inert -- that guard
// is what keeps unit tests, and any simulator invocation that was not started
// through the instrumented CLI, from reaching the logging backend at all.
//
// These tests deliberately never call initialize(). The sink lives in a
// function-local static with no way to unset it, so initializing here would
// leak into every later test in this binary and stop the calls below from
// exercising the guard. The initialized path is covered in its own binary,
// packsim-logger-sink-test.cpp, against a recording sink.

TEST(PacksimLoggerTest, CallSiteLogIsInertBeforeInitialize) {
  EXPECT_NO_THROW(PacksimLogger::log(LogDataType::BEGIN));
  EXPECT_NO_THROW(PacksimLogger::log(LogDataType::END));
  EXPECT_NO_THROW(PacksimLogger::log(LogDataType::PROGRESS));
  EXPECT_NO_THROW(PacksimLogger::log(LogDataType::EXCEPTION));
}

TEST(PacksimLoggerTest, ExceptionLogIsInertBeforeInitialize) {
  const std::runtime_error ex("simulated failure");

  EXPECT_NO_THROW(PacksimLogger::log(ex));
}

TEST(PacksimLoggerTest, ProgressLogIsInertBeforeInitialize) {
  EXPECT_NO_THROW(PacksimLogger::logProgress(0.0, 0));
  EXPECT_NO_THROW(PacksimLogger::logProgress(0.5, 1024));
  EXPECT_NO_THROW(
      PacksimLogger::logProgress(1.0, std::numeric_limits<uint64_t>::max()));
}

// The guard is a plain early return, so it must hold on every call rather than
// latching after the first one.
TEST(PacksimLoggerTest, GuardHoldsAcrossRepeatedCalls) {
  for (uint64_t i = 0; i < 100; ++i) {
    EXPECT_NO_THROW(PacksimLogger::log(LogDataType::PROGRESS));
    EXPECT_NO_THROW(PacksimLogger::logProgress(0.01 * i, i));
  }
}

} // namespace
} // namespace packsim
