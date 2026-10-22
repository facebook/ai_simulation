/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <vector>

#include <gtest/gtest.h>

#include "packsim/util/rr-counter.h"

namespace packsim {

// Each test owns its scheduler and context, so no state crosses between tests.
// A struct rather than a GTest fixture because the fixture would have to be
// named `RRCounter` to keep the suite name, colliding with the class under
// test. `Scheduler`'s constructor builds the PDES engine, so nothing else is
// needed to set one up.
struct CounterEnv {
  // Owns the PDES engine the counter is scheduled against.
  packsim::Scheduler scheduler;
  // Simulation context handed to the counter under test.
  packsim::SimContext simCtx{&scheduler, 0};
};

TEST(RRCounter, Uint16RRCounter) {
  CounterEnv env;
  auto ctr = RRCounter<uint16_t>(&env.simCtx, 5, 10);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(ctr(), 5);
    EXPECT_EQ(ctr(), 6);
    EXPECT_EQ(ctr(), 7);
    EXPECT_EQ(ctr(), 8);
    EXPECT_EQ(ctr(), 9);
    EXPECT_EQ(ctr(), 10);
  }

  EXPECT_EQ(ctr(), 5);
  EXPECT_EQ(ctr(), 6);

  ctr.max = 7;
  EXPECT_EQ(ctr(), 7);
  EXPECT_EQ(ctr(), 5);
}

TEST(RRCounter, Uint16RRCounterWithRefFunc) {
  CounterEnv env;
  uint16_t maxVal = 10;
  auto ctr =
      RRCounter<uint16_t>(&env.simCtx, 5, [&maxVal]() { return maxVal; });
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(ctr(), 5);
    EXPECT_EQ(ctr(), 6);
    EXPECT_EQ(ctr(), 7);
    EXPECT_EQ(ctr(), 8);
    EXPECT_EQ(ctr(), 9);
    EXPECT_EQ(ctr(), 10);
  }

  EXPECT_EQ(ctr(), 5);
  EXPECT_EQ(ctr(), 6);

  maxVal = 7;
  EXPECT_EQ(ctr(), 7);
  EXPECT_EQ(ctr(), 5);
}

TEST(RRCounter, Uint16RRCounterWithVectorSize) {
  CounterEnv env;
  std::vector<int> values(10);
  auto ctr = RRCounter<uint16_t>(
      &env.simCtx, 5, [&values]() { return values.size(); });
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(ctr(), 5);
    EXPECT_EQ(ctr(), 6);
    EXPECT_EQ(ctr(), 7);
    EXPECT_EQ(ctr(), 8);
    EXPECT_EQ(ctr(), 9);
    EXPECT_EQ(ctr(), 10);
  }

  EXPECT_EQ(ctr(), 5);
  EXPECT_EQ(ctr(), 6);

  values.resize(7);
  EXPECT_EQ(ctr(), 7);
  EXPECT_EQ(ctr(), 5);
}

} // namespace packsim
