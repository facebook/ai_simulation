/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/transport_config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/transport/oss_tests/roce-transport-sim-fixture.h"
#include "packsim/transport/roce-base.h"
#include "packsim/transport/transport-factory.h"
#include "packsim/transport/transport.h"

namespace tc = packsim::thrift;

namespace packsim {
namespace {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

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

tc::RoceV2CongestionControlConfig makeDcqcnCongestionControl() {
  tc::RoceV2CongestionControlConfig congestionControl;
  congestionControl.dcqcn() = tc::DcqcnCcConfig{};
  return congestionControl;
}

tc::RoceV2CongestionControlConfig makeDummyCongestionControl() {
  tc::RoceV2CongestionControlConfig congestionControl;
  congestionControl.dummy() = tc::DummyRoceV2StrategyConfig{};
  return congestionControl;
}

tc::RoceV2LossRecoveryConfig makeGoBackNLossRecovery() {
  tc::GoBackNLrConfig goBackN;
  goBackN.rnrTimeIdx() = 1;
  tc::RoceV2LossRecoveryConfig lossRecovery;
  lossRecovery.goBackN() = std::move(goBackN);
  return lossRecovery;
}

tc::RoceV2LossRecoveryConfig makeDummyLossRecovery() {
  tc::RoceV2LossRecoveryConfig lossRecovery;
  lossRecovery.dummy() = tc::DummyRoceV2StrategyConfig{};
  return lossRecovery;
}

// Reuses the RoCE-capable NIC config and sim context so the transport the
// factory returns can actually open a socket, which is how the strategies the
// config selected are observed.
class TransportFactoryTest : public RoceTransportSimTest {
 protected:
  TransportParams params() {
    // Static: the aggregate is returned by value, so a temporary built here
    // would not outlive the reference endpointName binds to.
    static const std::string kEndpointName = "endpoint0";
    return TransportParams{
        .simCtx = &simCtx_,
        .flexId = srcId_,
        .nicConfig = nicCfg_,
        .endpointName = kEndpointName};
  }

  // The socket of a freshly built transport; the strategy accessors on it are
  // the only public view of the RoceTransportConfig the factory passed down.
  static RoceSocketBase* connectSocket(
      RoceTransportBase& transport,
      const FlexDeviceId& dst) {
    transport.setSendPacketFn([](const PacketPtr&) { return true; });
    return dynamic_cast<RoceSocketBase*>(
        transport.connect(dst, 1234, /*localQPN=*/0, /*remoteQPN=*/0));
  }
};

TEST_F(TransportFactoryTest, UnsetTransportConfigThrows) {
  // Arrange.
  ASSERT_EQ(
      nicCfg_.transport()->getType(), tc::TransportConfig::Type::__EMPTY__);

  // Act + Assert.
  EXPECT_THAT(
      [&] { makeTransport(params()); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("transport config is unset")));
}

TEST_F(TransportFactoryTest, RoceV2ConfigDispatchesToRoceTransport) {
  // Arrange.
  *nicCfg_.transport() =
      makeRoce(makeDummyCongestionControl(), makeDummyLossRecovery());

  // Act.
  const std::unique_ptr<Transport> transport = makeTransport(params());

  // Assert: the roceV2 arm selects the RoCE transport implementation.
  EXPECT_NE(dynamic_cast<RoceTransportBase*>(transport.get()), nullptr);
}

TEST_F(TransportFactoryTest, DcqcnArmEnablesOnlyCongestionControl) {
  // Arrange.
  *nicCfg_.transport() =
      makeRoce(makeDcqcnCongestionControl(), makeDummyLossRecovery());

  // Act.
  const std::unique_ptr<Transport> transport = makeTransport(params());

  // Assert: the parsed per-arm config reached the transport, so congestion
  // control is on while loss recovery stays off.
  auto* const roce = dynamic_cast<RoceTransportBase*>(transport.get());
  ASSERT_NE(roce, nullptr);
  if (roce == nullptr) {
    return;
  }
  auto* const socket = connectSocket(*roce, dstId_);
  ASSERT_NE(socket, nullptr);
  // Redundant after the fatal ASSERT_NE above, but static analysis does not
  // model gtest's early return, so the check has to be explicit.
  if (socket == nullptr) {
    return;
  }
  EXPECT_NE(socket->getCongestionControl(), nullptr);
  EXPECT_EQ(socket->getErrorRecovery(), nullptr);
}

TEST_F(TransportFactoryTest, GoBackNArmEnablesOnlyErrorRecovery) {
  // Arrange.
  *nicCfg_.transport() =
      makeRoce(makeDummyCongestionControl(), makeGoBackNLossRecovery());

  // Act.
  const std::unique_ptr<Transport> transport = makeTransport(params());

  // Assert: the mirror of the DCQCN case - only the loss-recovery arm is on.
  auto* const roce = dynamic_cast<RoceTransportBase*>(transport.get());
  ASSERT_NE(roce, nullptr);
  if (roce == nullptr) {
    return;
  }
  auto* const socket = connectSocket(*roce, dstId_);
  ASSERT_NE(socket, nullptr);
  // Redundant after the fatal ASSERT_NE above, but static analysis does not
  // model gtest's early return, so the check has to be explicit.
  if (socket == nullptr) {
    return;
  }
  EXPECT_EQ(socket->getCongestionControl(), nullptr);
  EXPECT_NE(socket->getErrorRecovery(), nullptr);
}

} // namespace
} // namespace packsim
