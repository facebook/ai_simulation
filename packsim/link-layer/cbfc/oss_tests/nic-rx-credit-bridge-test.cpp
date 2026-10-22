/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/nic-rx-credit-bridge.h"

#include <gtest/gtest.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim::cbfc {
namespace {

// VC1 is lossless and maps from DSCP CS3 (0x18); one credit is 64 bytes.
packsim::thrift::CbfcConfig makeConfig() {
  packsim::thrift::CbfcConfig cfg;
  cfg.numVcs() = 2;
  cfg.isLosslessPerVc() = {false, true};
  packsim::thrift::VcMapEntry rule;
  rule.selector() = packsim::thrift::VcMapSelector::DSCP;
  rule.mask() = 0x3F;
  rule.value() = 0x18;
  rule.vcId() = 1;
  cfg.vcMap() = {rule};
  cfg.expectedCreditSizeBytes() = 64;
  return cfg;
}

PacketPtr makeLosslessPkt() {
  auto pkt = createPacket<PacketType::UDP>(0);
  pkt.as<UdpPacket>()->setDscpFromCos(3);
  return pkt;
}

// Every Cbfc built by EthernetPort gets both wake subscribers from
// wireCbfcWakes. A test that builds one directly has to satisfy the same
// contract; a test asserting on wakes installs its own over these.
void wireNoopWakes(Cbfc& cbfc) {
  cbfc.setOnWakeCallback([] {});
  cbfc.setOnCtlosReadyCallback([] {});
}

class NicRxCreditBridgeTest : public ::testing::Test {
 protected:
  // Cumulative since construction and never reset, so each assertion below is
  // a total rather than a delta.
  uint64_t freed() const {
    return cbfc_.stats().creditsFreed;
  }

  NicRxCreditBridgeTest() {
    wireNoopWakes(cbfc_);
  }

  Scheduler scheduler_;
  SimContext ctx_{&scheduler_, 0};
  Cbfc cbfc_{&ctx_, makeConfig(), kCbfcDefaultLinkBps800Gbps};
  NicRxCreditBridge bridge_{&cbfc_};
};

TEST_F(NicRxCreditBridgeTest, StoringTheArmedPacketClaimsIt) {
  auto pkt = makeLosslessPkt();
  bridge_.arm(*pkt);

  bridge_.onPacketStored(*pkt);

  EXPECT_TRUE(bridge_.disarmAndWasClaimed());
}

// Models a packet nothing buffers: an ACK, a duplicate, or a transport with no
// receive buffer.
TEST_F(NicRxCreditBridgeTest, AnArmedPacketNoBufferStoredIsNotClaimed) {
  auto pkt = makeLosslessPkt();
  bridge_.arm(*pkt);

  EXPECT_FALSE(bridge_.disarmAndWasClaimed());
}

TEST_F(NicRxCreditBridgeTest, StoringADifferentPacketDoesNotClaimTheArmedOne) {
  auto armed = makeLosslessPkt();
  auto other = makeLosslessPkt();
  bridge_.arm(*armed);

  bridge_.onPacketStored(*other);

  EXPECT_FALSE(bridge_.disarmAndWasClaimed());
}

TEST_F(NicRxCreditBridgeTest, DisarmClearsTheClaimForTheNextPacket) {
  auto first = makeLosslessPkt();
  bridge_.arm(*first);
  bridge_.onPacketStored(*first);
  ASSERT_TRUE(bridge_.disarmAndWasClaimed());

  auto second = makeLosslessPkt();
  bridge_.arm(*second);

  EXPECT_FALSE(bridge_.disarmAndWasClaimed())
      << "the previous packet's claim leaked into this one";
}

TEST_F(NicRxCreditBridgeTest, DrainReturnsTheCreditToTheModule) {
  auto pkt = makeLosslessPkt();
  ASSERT_EQ(70u, pkt->getSize()); // 2 credits at 64 B each
  cbfc_.onPacketStored(*pkt);
  ASSERT_EQ(0u, freed());

  bridge_.onPacketDrained(*pkt);

  EXPECT_EQ(2u, freed());
}

TEST_F(NicRxCreditBridgeTest, DetachedBridgeIgnoresALaterDrain) {
  auto pkt = makeLosslessPkt();
  cbfc_.onPacketStored(*pkt);
  bridge_.detach();

  bridge_.onPacketDrained(*pkt);

  EXPECT_EQ(0u, freed());
}

TEST_F(NicRxCreditBridgeTest, ReattachedBridgeReportsToTheNewModule) {
  bridge_.detach();
  Cbfc replacement(&ctx_, makeConfig(), kCbfcDefaultLinkBps800Gbps);
  wireNoopWakes(replacement);
  bridge_.reattach(&replacement);

  auto pkt = makeLosslessPkt();
  replacement.onPacketStored(*pkt);
  bridge_.onPacketDrained(*pkt);

  EXPECT_EQ(2u, replacement.stats().creditsFreed);
  EXPECT_EQ(0u, freed()) << "the drain reached the module it was cut from";
}

TEST_F(NicRxCreditBridgeTest, DetachClearsAnInFlightArm) {
  auto pkt = makeLosslessPkt();
  bridge_.arm(*pkt);
  bridge_.onPacketStored(*pkt);

  bridge_.detach();

  EXPECT_FALSE(bridge_.disarmAndWasClaimed());
}

} // namespace
} // namespace packsim::cbfc
