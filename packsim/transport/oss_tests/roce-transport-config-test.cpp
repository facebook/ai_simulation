/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/transport_config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/roce.h"
#include "packsim/transport/oss_tests/roce-transport-sim-fixture.h"
#include "packsim/transport/roce-base.h"
#include "packsim/transport/transport.h"

namespace tc = packsim::thrift;

namespace packsim {
namespace {

using Cc = RoceTransportConfig::CongestionControl;
using Er = RoceTransportConfig::ErrorRecovery;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

tc::RoceV2CongestionControlConfig makeDummyCongestionControl() {
  tc::RoceV2CongestionControlConfig congestionControl;
  congestionControl.dummy() = tc::DummyRoceV2StrategyConfig{};
  return congestionControl;
}

tc::RoceV2CongestionControlConfig makeDcqcnCongestionControl() {
  tc::RoceV2CongestionControlConfig congestionControl;
  congestionControl.dcqcn() = tc::DcqcnCcConfig{};
  return congestionControl;
}

tc::RoceV2LossRecoveryConfig makeDummyLossRecovery() {
  tc::RoceV2LossRecoveryConfig lossRecovery;
  lossRecovery.dummy() = tc::DummyRoceV2StrategyConfig{};
  return lossRecovery;
}

tc::RoceV2LossRecoveryConfig makeGoBackNLossRecovery(
    uint8_t rnrTimeIdx,
    std::optional<bool> selectiveGapRepair = std::nullopt) {
  tc::GoBackNLrConfig goBackN;
  goBackN.rnrTimeIdx() = rnrTimeIdx;
  if (selectiveGapRepair.has_value()) {
    goBackN.selectiveGapRepair() = *selectiveGapRepair;
  }
  tc::RoceV2LossRecoveryConfig lossRecovery;
  lossRecovery.goBackN() = std::move(goBackN);
  return lossRecovery;
}

tc::RoceV2LossRecoveryConfig makeGoBackNLossRecoveryWithDefaultRnrTimeIdx() {
  tc::RoceV2LossRecoveryConfig lossRecovery;
  lossRecovery.goBackN() = tc::GoBackNLrConfig{};
  return lossRecovery;
}

tc::TransportConfig makeRoce(
    tc::RoceV2CongestionControlConfig congestionControl,
    tc::RoceV2LossRecoveryConfig lossRecovery) {
  tc::RoceV2TransportConfig roceConfig;
  roceConfig.congestionControl() = std::move(congestionControl);
  roceConfig.lossRecovery() = std::move(lossRecovery);
  tc::TransportConfig transport;
  transport.roceV2() = std::move(roceConfig);
  return transport;
}

TEST(RoceTransportConfigFromThrift, ComposableNoneNone) {
  const auto cfg = RoceTransportConfig::fromThrift(
      makeRoce(makeDummyCongestionControl(), makeDummyLossRecovery()));
  EXPECT_EQ(cfg.congestionControl, Cc::NONE);
  EXPECT_EQ(cfg.errorRecovery, Er::NONE);
}

TEST(RoceTransportConfigFromThrift, ComposableDcqcnOnly) {
  const auto cfg = RoceTransportConfig::fromThrift(
      makeRoce(makeDcqcnCongestionControl(), makeDummyLossRecovery()));
  EXPECT_EQ(cfg.congestionControl, Cc::DCQCN);
  EXPECT_EQ(cfg.errorRecovery, Er::NONE);
}

TEST(RoceTransportConfigFromThrift, ComposableGoBackNOnly) {
  const auto cfg = RoceTransportConfig::fromThrift(
      makeRoce(makeDummyCongestionControl(), makeGoBackNLossRecovery(7)));
  EXPECT_EQ(cfg.congestionControl, Cc::NONE);
  EXPECT_EQ(cfg.errorRecovery, Er::GO_BACK_N);
  EXPECT_EQ(cfg.rnrTimeIdx, 7);
  EXPECT_FALSE(cfg.selectiveGapRepair);
}

TEST(RoceTransportConfigFromThrift, ComposableGoBackNSelectiveGapRepair) {
  const auto cfg = RoceTransportConfig::fromThrift(makeRoce(
      makeDummyCongestionControl(),
      makeGoBackNLossRecovery(/*rnrTimeIdx=*/7, /*selectiveGapRepair=*/true)));
  EXPECT_TRUE(cfg.selectiveGapRepair);
}

TEST(RoceTransportConfigFromThrift, ComposableGoBackNUsesDefaultRnrIdx) {
  const auto cfg = RoceTransportConfig::fromThrift(makeRoce(
      makeDummyCongestionControl(),
      makeGoBackNLossRecoveryWithDefaultRnrTimeIdx()));
  EXPECT_EQ(cfg.errorRecovery, Er::GO_BACK_N);
  EXPECT_EQ(cfg.rnrTimeIdx, 1);
}

TEST(RoceTransportConfigFromThrift, ComposableDcqcnPlusGoBackN) {
  const auto cfg = RoceTransportConfig::fromThrift(
      makeRoce(makeDcqcnCongestionControl(), makeGoBackNLossRecovery(3)));
  EXPECT_EQ(cfg.congestionControl, Cc::DCQCN);
  EXPECT_EQ(cfg.errorRecovery, Er::GO_BACK_N);
  EXPECT_EQ(cfg.rnrTimeIdx, 3);
}

TEST(RoceTransportConfigFromThrift, UnsetCongestionControlThrows) {
  tc::RoceV2TransportConfig roceConfig;
  roceConfig.lossRecovery() = makeDummyLossRecovery();
  tc::TransportConfig transport;
  transport.roceV2() = std::move(roceConfig);
  EXPECT_THAT(
      [&] { RoceTransportConfig::fromThrift(transport); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("unsupported RoCE congestion control config")));
}

TEST(RoceTransportConfigFromThrift, UnsetLossRecoveryThrows) {
  tc::RoceV2TransportConfig roceConfig;
  roceConfig.congestionControl() = makeDummyCongestionControl();
  tc::TransportConfig transport;
  transport.roceV2() = std::move(roceConfig);
  EXPECT_THAT(
      [&] { RoceTransportConfig::fromThrift(transport); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("unsupported RoCE loss recovery config")));
}

TEST(RoceTransportConfigFromThrift, UnsetTransportThrows) {
  const tc::TransportConfig transport;
  EXPECT_THAT(
      [&] { RoceTransportConfig::fromThrift(transport); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("transport config is unset")));
}

TEST(RoceTransportConfigFromThrift, RemovedLegacyWireArmsAreRejected) {
  for (const char legacyFieldId : {'\x01', '\x02', '\x04'}) {
    SCOPED_TRACE(int{legacyFieldId});
    // Binary protocol: T_STRUCT + i16 field ID, an empty struct, then STOP.
    std::string legacyWire{"\x0c\x00\x00\x00\x00", 5};
    legacyWire[2] = legacyFieldId;

    const auto transport =
        apache::thrift::BinarySerializer::deserialize<tc::TransportConfig>(
            legacyWire);
    EXPECT_EQ(transport.getType(), tc::TransportConfig::Type::__EMPTY__);
    EXPECT_THAT(
        [&] { RoceTransportConfig::fromThrift(transport); },
        ThrowsMessage<std::invalid_argument>(
            HasSubstr("transport config is unset")));
  }
}

TEST_F(RoceTransportSimTest, DcqcnPlusGoBackNMaterializesBothStrategies) {
  const auto roceCfg = RoceTransportConfig::fromThrift(
      makeRoce(makeDcqcnCongestionControl(), makeGoBackNLossRecovery(1)));
  RoceTransportBase transport(&simCtx_, srcId_, &nicCfg_, roceCfg);
  transport.setSendPacketFn([](const PacketPtr&) { return true; });
  transport.setOnPfcStateChangedCallback([](uint32_t, PfcState) {});
  transport.setOnMessageAckedCallback([](Message*) {});
  transport.setOnMessageReceivedCallback([](Message*) {});

  auto* socket =
      dynamic_cast<RoceSocketBase*>(transport.connect(dstId_, 1234, 0, 0));
  ASSERT_NE(socket, nullptr);
  // Redundant after the fatal ASSERT_NE above, but static analysis does not
  // model gtest's early return, so the check has to be explicit.
  if (socket == nullptr) {
    return;
  }
  EXPECT_NE(socket->getCongestionControl(), nullptr);
  EXPECT_NE(socket->getErrorRecovery(), nullptr);
}

TEST_F(RoceTransportSimTest, GoBackNRejectsRnrTimeIdxPastTimerTable) {
  // kGoBackNRnrFieldToTimeUs has 32 entries, so 32 is the first bad index.
  const auto roceCfg = RoceTransportConfig::fromThrift(makeRoce(
      makeDummyCongestionControl(),
      makeGoBackNLossRecovery(/*rnrTimeIdx=*/32)));
  EXPECT_THAT(
      [&] { RoceTransportBase transport(&simCtx_, srcId_, &nicCfg_, roceCfg); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "rnrTimeIdx 32 is out of range: it indexes "
          "kGoBackNRnrFieldToTimeUs, valid range [0, 31]")));
}

TEST_F(RoceTransportSimTest, GoBackNAcceptsLastRnrTimeIdx) {
  const auto roceCfg = RoceTransportConfig::fromThrift(makeRoce(
      makeDummyCongestionControl(),
      makeGoBackNLossRecovery(/*rnrTimeIdx=*/31)));
  EXPECT_NO_THROW(RoceTransportBase(&simCtx_, srcId_, &nicCfg_, roceCfg));
}

TEST_F(RoceTransportSimTest, DcqcnPlusGoBackNSimCompletes) {
  const auto roceCfg = RoceTransportConfig::fromThrift(
      makeRoce(makeDcqcnCongestionControl(), makeGoBackNLossRecovery(1)));
  RoceTransportBase src(&simCtx_, srcId_, &nicCfg_, roceCfg);
  RoceTransportBase dst(&simCtx_, dstId_, &nicCfg_, roceCfg);

  // Loopback: each side hands its transmitted packet to the peer after a small
  // wire delay, so DCQCN rate limiting and Go-Back-N tracking both run.
  src.setSendPacketFn([&](const PacketPtr& pkt) {
    src.onPacketSent(pkt, nullptr);
    simCtx_.schedule(
        Time::fromNs(100),
        &Transport::onPacketRcvd,
        static_cast<Transport*>(&dst),
        pkt,
        nullptr);
    return true;
  });
  dst.setSendPacketFn([&](const PacketPtr& pkt) {
    dst.onPacketSent(pkt, nullptr);
    simCtx_.schedule(
        Time::fromNs(100),
        &Transport::onPacketRcvd,
        static_cast<Transport*>(&src),
        pkt,
        nullptr);
    return true;
  });
  src.setOnPfcStateChangedCallback([](uint32_t, PfcState) {});
  dst.setOnPfcStateChangedCallback([](uint32_t, PfcState) {});
  uint32_t acked = 0;
  uint32_t received = 0;
  src.setOnMessageAckedCallback([&](Message*) { ++acked; });
  dst.setOnMessageReceivedCallback([&](Message*) { ++received; });

  auto* srcSocket = src.connect(dstId_, 1234, 0, 0);
  auto* dstSocket = dst.connect(srcId_, 2345, 0, 0);
  ASSERT_NE(srcSocket, nullptr);
  ASSERT_NE(dstSocket, nullptr);
  // Bind references up front: static analysis does not model gtest's early
  // return, and pointer nullability would not propagate into the scheduled
  // lambda below anyway.
  if (srcSocket == nullptr || dstSocket == nullptr) {
    return;
  }
  Socket& srcSock = *srcSocket;
  Socket& dstSock = *dstSocket;

  Message msg{
      .sizeBytes = 4096 * 65,
      .srcIdx = srcId_,
      .dstIdx = dstId_,
      .connectionMessageId = 0};
  simCtx_.scheduleNow([&]() {
    srcSock.send(&msg);
    dstSock.recv(&msg);
  });
  scheduler_.run();

  EXPECT_EQ(acked, 1u) << "sender should have received an ACK";
  EXPECT_EQ(received, 1u) << "receiver should have gotten the message";
}

TEST_F(RoceTransportSimTest, QpPairUsesStableSymmetricPhysicalPort) {
  RoceTransportBase forwardTransport(&simCtx_, srcId_, &nicCfg_);
  RoceTransportBase reverseTransport(&simCtx_, dstId_, &nicCfg_);
  std::vector<uint16_t> sentPorts;
  for (const uint16_t port : {uint16_t{3}, uint16_t{7}}) {
    forwardTransport.addPort(port, nullptr, [&, port](const PacketPtr&) {
      sentPorts.push_back(port);
      return true;
    });
    reverseTransport.addPort(port, nullptr, [&, port](const PacketPtr&) {
      sentPorts.push_back(port);
      return true;
    });
  }

  auto* forward = dynamic_cast<RoceSocketBase*>(
      forwardTransport.connect(dstId_, 1234, /*localQPN=*/2, /*remoteQPN=*/8));
  auto* reverse = dynamic_cast<RoceSocketBase*>(
      reverseTransport.connect(srcId_, 1234, /*localQPN=*/8, /*remoteQPN=*/2));
  ASSERT_NE(forward, nullptr);
  ASSERT_NE(reverse, nullptr);

  auto forwardPkt = createPacket<PacketType::ROCE>(100);
  auto reversePkt = createPacket<PacketType::ROCE>(100);
  EXPECT_TRUE(forwardTransport.sendPacket(forward, forwardPkt));
  EXPECT_TRUE(reverseTransport.sendPacket(reverse, reversePkt));

  EXPECT_EQ(sentPorts, (std::vector<uint16_t>{7, 7}));
  for (const auto& pkt : {forwardPkt, reversePkt}) {
    const auto* rocePkt = pkt.as<RocePacket>();
    EXPECT_EQ(rocePkt->egressNicPort, 7);
    EXPECT_EQ(rocePkt->dstNicPort, 7);
  }
}

TEST_F(RoceTransportSimTest, DevicePairsUseDifferentPhysicalPorts) {
  RoceTransportBase transport(&simCtx_, srcId_, &nicCfg_);
  std::vector<uint16_t> sentPorts;
  for (const uint16_t port : {uint16_t{3}, uint16_t{7}}) {
    transport.addPort(port, nullptr, [&, port](const PacketPtr&) {
      sentPorts.push_back(port);
      return true;
    });
  }

  const FlexDeviceId otherDstId{{1, 0, 0, 3}};
  auto* first = dynamic_cast<RoceSocketBase*>(
      transport.connect(dstId_, 1234, /*localQPN=*/2, /*remoteQPN=*/8));
  auto* second = dynamic_cast<RoceSocketBase*>(
      transport.connect(otherDstId, 1235, /*localQPN=*/3, /*remoteQPN=*/9));
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);

  EXPECT_TRUE(transport.sendPacket(first, createPacket<PacketType::ROCE>(100)));
  EXPECT_TRUE(
      transport.sendPacket(second, createPacket<PacketType::ROCE>(100)));

  EXPECT_EQ(sentPorts, (std::vector<uint16_t>{7, 3}));
}

TEST_F(RoceTransportSimTest, WithoutPortsSendFallsBackToTransportCallback) {
  RoceTransportBase transport(&simCtx_, srcId_, &nicCfg_);
  int fallbackSends = 0;
  transport.setSendPacketFn([&](const PacketPtr&) {
    fallbackSends++;
    return true;
  });

  auto* socket = dynamic_cast<RoceSocketBase*>(
      transport.connect(dstId_, 1234, /*localQPN=*/2, /*remoteQPN=*/8));
  ASSERT_NE(socket, nullptr);

  auto pkt = createPacket<PacketType::ROCE>(100);
  EXPECT_TRUE(transport.sendPacket(socket, pkt));

  EXPECT_EQ(fallbackSends, 1);
  // No port was selected, so the steering fields stay unset.
  const auto* rocePkt = pkt.as<RocePacket>();
  EXPECT_FALSE(rocePkt->egressNicPort.has_value());
  EXPECT_FALSE(rocePkt->dstNicPort.has_value());
}

} // namespace
} // namespace packsim
