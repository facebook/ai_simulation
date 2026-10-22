/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/link-layer/ctlos-arbiter.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {
namespace {

// A source whose pending state the test drives directly. Each
// frame it hands out is tagged with the source's id, in the CF_Update payload's
// `vc1` field, so a test can read back which source produced it.
class FakeCtlosSource : public ICtlosSource {
 public:
  explicit FakeCtlosSource(uint8_t id) : id_(id) {}

  [[nodiscard]] bool hasPendingCtlos() const override {
    return pending_;
  }

  [[nodiscard]] std::optional<PacketPtr> popCtlos() override {
    if (!pending_) {
      return std::nullopt;
    }
    ++popCount;
    PacketPtr pkt = createPacket<PacketType::CTLOS_CF_UPDATE>(0);
    pkt.as<CfUpdateFrame>()->cfUpdate.vc1 = id_;
    return pkt;
  }

  void setPending(bool pending) {
    pending_ = pending;
  }

  int popCount{0};

 private:
  uint8_t id_;
  bool pending_{false};
};

// The id a frame was tagged with, i.e. which source produced it.
uint8_t sourceOf(const PacketPtr& pkt) {
  return pkt.as<CfUpdateFrame>()->cfUpdate.vc1;
}

class CtlosArbiterTest : public ::testing::Test {
 protected:
  // The port's slot order, fixed for the fixture's lifetime as a real port's
  // is. The cursor indexes these positions, so they must not move between
  // pulls.
  std::optional<PacketPtr> popNextRr() {
    const std::array<ICtlosSource*, 2> sources{&a_, &b_};
    return arbiter_.popNextRr(sources);
  }

  FakeCtlosSource a_{0};
  FakeCtlosSource b_{1};
  CtlosArbiter arbiter_;
};

TEST_F(CtlosArbiterTest, NoSourcePendingYieldsNothing) {
  EXPECT_FALSE(popNextRr().has_value());
}

TEST_F(CtlosArbiterTest, SingleReadySourceIsServed) {
  b_.setPending(true);
  auto pkt = popNextRr();
  ASSERT_TRUE(pkt.has_value());
  EXPECT_EQ(1, sourceOf(*pkt));
  EXPECT_EQ(0, a_.popCount);
}

// UEC 1.0.3 §5.1.9.1.5: arbitration between the two CtlOS producers "should
// employ an algorithm that avoids starvation of either. To that end, round
// robin is recommended."
TEST_F(CtlosArbiterTest, TwoAlwaysReadySourcesAlternate) {
  a_.setPending(true);
  b_.setPending(true);

  std::vector<uint8_t> served;
  for (int i = 0; i < 4; ++i) {
    auto pkt = popNextRr();
    ASSERT_TRUE(pkt.has_value()) << "pull " << i;
    served.emplace_back(sourceOf(*pkt));
  }

  const std::vector<uint8_t> expected{0, 1, 0, 1};
  EXPECT_EQ(expected, served);
}

// The starvation case the round robin exists to prevent: a producer that
// always has work must not lock the other one out.
TEST_F(CtlosArbiterTest, ABusySourceDoesNotStarveAnIntermittentOne) {
  a_.setPending(true); // never goes idle

  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(popNextRr().has_value());
  }
  b_.setPending(true);

  auto pkt = popNextRr();
  ASSERT_TRUE(pkt.has_value());
  EXPECT_EQ(1, sourceOf(*pkt))
      << "the cursor left the busy source, so the newly ready one goes next";
}

TEST_F(CtlosArbiterTest, CursorSkipsASourceWithNothingPending) {
  a_.setPending(true);
  b_.setPending(false);

  // Two pulls in a row must both come from `a_`: the cursor advances past it
  // each time, finds `b_` empty, and wraps.
  for (int i = 0; i < 2; ++i) {
    auto pkt = popNextRr();
    ASSERT_TRUE(pkt.has_value()) << "pull " << i;
    EXPECT_EQ(0, sourceOf(*pkt));
  }
  EXPECT_EQ(0, b_.popCount);
}

// A port with neither producer enabled presents two empty slots; the sweep
// must tolerate them rather than indexing past the end.
TEST_F(CtlosArbiterTest, AllSlotsEmptyYieldsNothing) {
  a_.setPending(true);
  const std::array<ICtlosSource*, 2> none{nullptr, nullptr};
  EXPECT_FALSE(arbiter_.popNextRr(none).has_value());
  EXPECT_EQ(0, a_.popCount);
}

} // namespace
} // namespace packsim
