/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "packsim/kernel/pdes/Event.h"
#include "packsim/kernel/pdes/SimulationContext.h"

namespace packsim::pdes {

TEST(SimulationContext, NowFsAndNowPsTruncate) {
  SimulationContext ctx("time-accessors");
  ctx.currentTimeFs = 1'999;
  EXPECT_EQ(ctx.nowFs(), 1'999);
  EXPECT_EQ(ctx.nowPs(), 1);
}

TEST(SimulationContext, NowNsNowUsNowMsTruncate) {
  SimulationContext ctx("time-accessors");
  ctx.currentTimeFs = 123'456'789'012'345ULL;
  EXPECT_EQ(ctx.nowNs(), 123'456'789);
  EXPECT_EQ(ctx.nowUs(), 123'456);
  EXPECT_EQ(ctx.nowMs(), 123);
}

TEST(SimulationContext, NamedConstructorUsesGivenName) {
  SimulationContext ctx("my-context");
  EXPECT_EQ(ctx.name, "my-context");
}

TEST(SimulationContext, DefaultConstructorAutoNamesWithPrefix) {
  SimulationContext ctx;
  // Auto-generated name is "SimContext<n>".
  EXPECT_EQ(ctx.name.rfind("SimContext", 0), 0u);
}

TEST(SimulationContext, ContextIndicesAreDistinctAndIncreasing) {
  SimulationContext first("first");
  SimulationContext second("second");
  // ctxIdx is assigned from a process-global counter, so assert relative
  // ordering rather than absolute values.
  EXPECT_LT(first.ctxIdx, second.ctxIdx);
}

TEST(SimulationContext, NameWithPtrContainsNameAndPointer) {
  SimulationContext ctx("labelled");
  const std::string rendered = ctx.nameWithPtr();
  EXPECT_NE(rendered.find("labelled"), std::string::npos);
  EXPECT_NE(rendered.find("0x"), std::string::npos);
}

TEST(SimulationContext, DestructorDrainsPendingEvents) {
  SimulationContext ctx("drain");
  ctx.eventQ.push(new RawEvent());
  // The event must actually be enqueued so ~SimulationContext has something to
  // drain; the destructor then frees it (any leak/use-after-free is caught by
  // ASAN).
  EXPECT_EQ(ctx.eventQ.size(), 1u);
}

} // namespace packsim::pdes
