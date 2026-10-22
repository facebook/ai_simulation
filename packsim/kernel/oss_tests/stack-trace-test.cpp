/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#include "packsim/kernel/stack-trace.h"

namespace packsim {

TEST(StackTrace, NoTraceWhenNoExceptionInFlight) {
  EXPECT_EQ(getCurrentExceptionStackTrace(), "");
}

TEST(StackTrace, CapturesTraceInsideCatchBlock) {
  std::string trace;
  try {
    throw std::runtime_error("boom");
  } catch (const std::runtime_error&) {
    trace = getCurrentExceptionStackTrace();
  }

  EXPECT_FALSE(trace.empty());
}

TEST(StackTrace, NamesTheThrownExceptionType) {
  std::string trace;
  try {
    throw std::logic_error("boom");
  } catch (const std::logic_error&) {
    trace = getCurrentExceptionStackTrace();
  }

  EXPECT_NE(trace.find("logic_error"), std::string::npos) << trace;
}

TEST(StackTrace, ClearsOnceTheCatchBlockHasExited) {
  try {
    throw std::runtime_error("boom");
  } catch (const std::runtime_error&) {
    // Swallow it; the exception is no longer in flight below.
  }

  EXPECT_EQ(getCurrentExceptionStackTrace(), "");
}

} // namespace packsim
