/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/random.h"
#include "packsim/util/builders.h"

namespace packsim {

using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::StrEq;
using ::testing::ThrowsMessage;

using TransportType = packsim::thrift::TransportConfig::Type;
using CongestionControlType =
    packsim::thrift::RoceV2CongestionControlConfig::Type;
using LossRecoveryType = packsim::thrift::RoceV2LossRecoveryConfig::Type;

namespace {

void expectRoceSelection(
    const TransportKind transport,
    const RoceCongestionControl congestionControl,
    const RoceLossRecovery lossRecovery,
    CongestionControlType expectedCongestionControl,
    LossRecoveryType expectedLossRecovery) {
  const auto sim = packsim::buildFourZoneClosConfig({
      .numAccel = 4,
      .numQueues = 1,
      .transportType = transport,
      .congestionControl = congestionControl,
      .lossRecovery = lossRecovery,
  });
  const auto& transportConfig = *sim.nicConfigMap()->at("default").transport();
  ASSERT_EQ(transportConfig.getType(), TransportType::roceV2);
  const auto& roceConfig = transportConfig.roceV2().value();
  EXPECT_EQ(
      roceConfig.congestionControl()->getType(), expectedCongestionControl);
  EXPECT_EQ(roceConfig.lossRecovery()->getType(), expectedLossRecovery);
}

void expectLegacyAliasRejected(const std::string& alias) {
  EXPECT_THAT(
      [alias] { packsim::parseTransportKind(alias); },
      ThrowsMessage<std::invalid_argument>(
          AllOf(HasSubstr("Unrecognized transport type"), HasSubstr(alias))));
}

} // namespace

class BuildFourZoneClosWorkloadSim : public ::testing::Test {
 protected:
  Random random_{42};
};

class GetWorkCfgTest : public ::testing::Test {
 protected:
  // Random source getWorkCfg() draws vectorized all-to-all sizes from.
  Random random_{/*seed=*/42};
};

TEST(BuildFourZoneClosConfig, RejectsUnknownTransport) {
  EXPECT_THAT(
      [] { packsim::parseTransportKind("not-a-transport"); },
      ThrowsMessage<std::invalid_argument>(
          StrEq("Unrecognized transport type: not-a-transport")));
}

TEST(BuildFourZoneClosConfig, UnknownTransportErrorKeepsOriginalCasing) {
  EXPECT_THAT(
      [] { packsim::parseTransportKind("NotARoce"); },
      ThrowsMessage<std::invalid_argument>(
          StrEq("Unrecognized transport type: NotARoce")));
}

TEST(BuildFourZoneClosConfig, SupportsOrthogonalAlgorithms) {
  expectRoceSelection(
      TransportKind::ROCE,
      RoceCongestionControl::DCQCN,
      RoceLossRecovery::GO_BACK_N,
      CongestionControlType::dcqcn,
      LossRecoveryType::goBackN);
}

TEST(BuildFourZoneClosConfig, RemovedOptimisticRoceAliasIsRejected) {
  expectLegacyAliasRejected("optimisticroce");
}

TEST(BuildFourZoneClosConfig, RemovedGoBackNRoceAliasIsRejected) {
  expectLegacyAliasRejected("gobacknroce");
}

TEST(BuildFourZoneClosConfig, RemovedDcqcnRoceAliasIsRejected) {
  expectLegacyAliasRejected("dcqcnroce");
}

TEST_F(BuildFourZoneClosWorkloadSim, RejectsNonDivisibleCopies) {
  EXPECT_THAT(
      [this] {
        packsim::buildFourZoneClosWorkloadSim(
            random_,
            {
                .workloadType = packsim::thrift::WorkloadType::ALL_TO_ALL,
                .numAccel = 128,
                .sizeBytes = 1024,
                .numCopies = 3,
                .unit = packsim::thrift::SchedulingUnit::ENDPOINT,
                .method = packsim::thrift::SchedulingMethod::SEQUENTIAL,
            });
      },
      ThrowsMessage<std::invalid_argument>(
          StrEq("numAccel must be a multiple of numCopies")));
}

TEST_F(BuildFourZoneClosWorkloadSim, RejectsZeroCopies) {
  EXPECT_THAT(
      [this] {
        packsim::buildFourZoneClosWorkloadSim(
            random_,
            {
                .workloadType = packsim::thrift::WorkloadType::ALL_TO_ALL,
                .numAccel = 128,
                .sizeBytes = 1024,
                .numCopies = 0,
                .unit = packsim::thrift::SchedulingUnit::ENDPOINT,
                .method = packsim::thrift::SchedulingMethod::SEQUENTIAL,
            });
      },
      ThrowsMessage<std::invalid_argument>(
          StrEq("numCopies must be greater than 0")));
}

TEST_F(BuildFourZoneClosWorkloadSim, RejectsZeroNumAccel) {
  EXPECT_THAT(
      [this] {
        packsim::buildFourZoneClosWorkloadSim(
            random_,
            {
                .workloadType = packsim::thrift::WorkloadType::ALL_TO_ALL,
                .numAccel = 0,
                .sizeBytes = 1024,
                .numCopies = 1,
            });
      },
      ThrowsMessage<std::invalid_argument>(
          StrEq("numAccel must be greater than 0")));
}

TEST(BuildFourZoneClosConfig, SpineSwitchesGetExactlyTheWiredPorts) {
  // 128 accelerators build 4 racks per zone; each CTSW has one link to each of
  // them and two to each of its plane's 2 ATSWs, and each ATSW two to each of
  // its plane's 4 CTSWs in all 4 zones.
  const auto sim = buildFourZoneClosConfig(/*numAccel=*/128);
  const auto& ctswGroups = *sim.switchConfigMap()->at("CTSW").portGroups();
  const auto& atswGroups = *sim.switchConfigMap()->at("ATSW").portGroups();

  EXPECT_EQ(ctswGroups.at("uplinks").size(), 4u);
  EXPECT_EQ(ctswGroups.at("downlinks").size(), 4u);
  EXPECT_FALSE(atswGroups.contains("uplinks"));
  EXPECT_EQ(atswGroups.at("downlinks").size(), 32u);
}

TEST(BuildFourZoneClosConfig, SpinePortsSetBothSpineTiers) {
  const auto sim = buildFourZoneClosConfig({
      .numAccel = 128,
      .buildFabricFn =
          [](const uint32_t numAccel, const uint8_t numQueues) {
            auto cfg = buildFourZoneClosCliConfig(numAccel, numQueues);
            cfg.spinePorts = SwitchPortCounts{.uplinks = 8, .downlinks = 40};
            return cfg;
          },
  });

  for (const std::string tier : {"CTSW", "ATSW"}) {
    const auto& groups = *sim.switchConfigMap()->at(tier).portGroups();
    EXPECT_EQ(groups.at("uplinks").size(), 8u) << tier;
    EXPECT_EQ(groups.at("downlinks").size(), 40u) << tier;
  }
}

TEST(BuildFourZoneClosConfig, RejectsZeroQueues) {
  // Each NIC queue's buffer is the NIC buffer divided by the queue count.
  EXPECT_THAT(
      [] {
        packsim::buildFourZoneClosConfig({.numAccel = 128, .numQueues = 0});
      },
      ThrowsMessage<std::invalid_argument>(
          StrEq("numQueues must be greater than 0")));
}

TEST_F(BuildFourZoneClosWorkloadSim, RejectsZeroSizeBytes) {
  EXPECT_THAT(
      [this] {
        packsim::buildFourZoneClosWorkloadSim(
            random_,
            {
                .workloadType = packsim::thrift::WorkloadType::ALL_TO_ALL,
                .numAccel = 4,
                .sizeBytes = 0,
                .numCopies = 1,
            });
      },
      ThrowsMessage<std::invalid_argument>(
          StrEq("sizeBytes must be greater than 0")));
}

TEST_F(BuildFourZoneClosWorkloadSim, ForwardsRoceAlgorithms) {
  const auto sim = packsim::buildFourZoneClosWorkloadSim(
      random_,
      {
          .workloadType = packsim::thrift::WorkloadType::ALL_TO_ALL,
          .numAccel = 128,
          .sizeBytes = 1024,
          .numCopies = 1,
          .unit = packsim::thrift::SchedulingUnit::ENDPOINT,
          .method = packsim::thrift::SchedulingMethod::SEQUENTIAL,
          .congestionControl = RoceCongestionControl::DCQCN,
          .lossRecovery = RoceLossRecovery::GO_BACK_N,
      });

  const auto& transportConfig = *sim.nicConfigMap()->at("default").transport();
  ASSERT_EQ(transportConfig.getType(), TransportType::roceV2);
  const auto& roceConfig = transportConfig.roceV2().value();
  EXPECT_EQ(
      roceConfig.congestionControl()->getType(), CongestionControlType::dcqcn);
  EXPECT_EQ(roceConfig.lossRecovery()->getType(), LossRecoveryType::goBackN);
}

TEST_F(GetWorkCfgTest, ThrowsForAWorkloadTypeItDoesNotBuild) {
  EXPECT_THAT(
      [this] {
        getWorkCfg(
            random_,
            packsim::thrift::WorkloadType::BATCH,
            /*numAccel=*/4,
            /*sizeBytes=*/1024);
      },
      ThrowsMessage<std::runtime_error>(HasSubstr("Unsupported Type")));
}

TEST_F(GetWorkCfgTest, ThrowsForAValueOutsideTheEnum) {
  // A WorkloadType is an int underneath, so a caller can pass a value the IDL
  // does not name.
  constexpr int32_t kWorkloadTypeUnknownToThisBinary = 42;
  EXPECT_THAT(
      [this] {
        getWorkCfg(
            random_,
            static_cast<packsim::thrift::WorkloadType>(
                kWorkloadTypeUnknownToThisBinary),
            /*numAccel=*/4,
            /*sizeBytes=*/1024);
      },
      ThrowsMessage<std::runtime_error>(HasSubstr("Unsupported Type")));
}

} // namespace packsim
