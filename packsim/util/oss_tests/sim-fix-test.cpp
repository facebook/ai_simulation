/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstdint>
#include <map>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/random.h"
#include "packsim/util/oss_tests/sim-fix-test-fixture.h"
#include "packsim/util/sim-fix.h"

using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::Key;
using ::testing::ThrowsMessage;
using ::testing::UnorderedElementsAre;

namespace packsim {
namespace {

packsim::thrift::RankPair makeRankPair(uint32_t srcRank, uint32_t dstRank) {
  packsim::thrift::RankPair rankPair;
  rankPair.srcRank() = srcRank;
  rankPair.dstRank() = dstRank;
  return rankPair;
}

packsim::thrift::NicPair makeNicPair(uint32_t srcNicIdx, uint32_t dstNicIdx) {
  packsim::thrift::NicPair nicPair;
  nicPair.srcNicIdx() = srcNicIdx;
  nicPair.dstNicIdx() = dstNicIdx;
  return nicPair;
}

// An all-to-all between ranks 0 and 1 with channels on and a supplied
// nicQpnPairMap: on NIC pair (0, 0), QPNs 1 and 2 each way, as fixupSimCfg
// numbers one channel of one QP and its notify QP.
packsim::thrift::Simulation makeChannelSimCfgWithSuppliedNicQpnPairMap() {
  packsim::thrift::Simulation simCfg;
  auto& wkld = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
  wkld.name() = "a2a";
  wkld.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  wkld.numEndpoints() = 2;
  wkld.rankToId() = {{0, {0}}, {1, {1}}};
  wkld.channelSettings() = packsim::workload::thrift::ChannelSettings{};
  for (const auto& [src, dst] :
       {std::pair<uint32_t, uint32_t>{0, 1},
        std::pair<uint32_t, uint32_t>{1, 0}}) {
    auto& qpnPairs =
        (*wkld.nicQpnPairMap())[makeRankPair(src, dst)][makeNicPair(0, 0)];
    for (const uint32_t qpn : {1U, 2U}) {
      auto& qpnPair = qpnPairs.emplace_back();
      qpnPair.local() = qpn;
      qpnPair.remote() = qpn;
    }
  }
  return simCfg;
}

} // namespace

TEST(SimFix, TestFixWorkloadsRepeatedWorkloadName) {
  packsim::thrift::Simulation simCfg;
  Random random(/* seed= */ 0);

  simCfg.newWorkloadSchedule()->workloads()->emplace_back().name() =
      "workload1";
  simCfg.newWorkloadSchedule()->workloads()->emplace_back().name() =
      "workload2";
  simCfg.newWorkloadSchedule()->workloads()->emplace_back().name() =
      "workload2";

  EXPECT_THAT(
      [&]() { validateAndFixWorkloads(simCfg, random); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Duplicate input workload name: workload2")));
}

TEST(SimFix, TestFixWorkloadsEmptyWorkloadName) {
  packsim::thrift::Simulation simCfg;
  Random random(/* seed= */ 0);

  simCfg.newWorkloadSchedule()->workloads()->emplace_back().name() = {};

  EXPECT_THAT(
      [&]() { validateAndFixWorkloads(simCfg, random); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Workload name cannot be empty.")));
}

TEST(SimFix, TestFixWorkloadsWithAllReduceRails) {
  packsim::thrift::Simulation simCfg;
  Random random(/* seed= */ 0);

  {
    auto& workload = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
    workload.name() = "workload1";
    packsim::workload::thrift::AllReduce arW;
    arW.sizeBytes() = 1000000;
    arW.numRails() = 8;
    workload.config()->allReduce() = std::move(arW);
    workload.numEndpoints() = 8;
    workload.rankToId() = {
        {0, {0, 0, 0, 0, 0, 0}},
        {1, {0, 0, 0, 0, 0, 0}},
        {2, {0, 0, 0, 0, 0, 0}},
        {3, {0, 0, 0, 0, 0, 0}},
        {4, {0, 0, 0, 0, 0, 0}},
        {5, {0, 0, 0, 0, 0, 0}},
        {6, {0, 0, 0, 0, 0, 0}},
        {7, {0, 0, 0, 0, 0, 0}}};
  }
  {
    auto& workload = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
    workload.name() = "workload2";
    packsim::workload::thrift::AllReduce arW;
    arW.sizeBytes() = 1000000;
    arW.numRails() = 4;
    workload.config()->allReduce() = std::move(arW);
    workload.numEndpoints() = 8;
    workload.rankToId() = {
        {0, {0, 0, 0, 0, 0, 0}},
        {1, {0, 0, 0, 0, 0, 0}},
        {2, {0, 0, 0, 0, 0, 0}},
        {3, {0, 0, 0, 0, 0, 0}},
        {4, {0, 0, 0, 0, 0, 0}},
        {5, {0, 0, 0, 0, 0, 0}},
        {6, {0, 0, 0, 0, 0, 0}},
        {7, {0, 0, 0, 0, 0, 0}}};
  }
  {
    auto& workload = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
    workload.name() = "workload3";
    packsim::workload::thrift::AllToAll a2aW;
    a2aW.sizeBytes() = 1000000;
    workload.config()->allToAll() = std::move(a2aW);
    workload.numEndpoints() = 8;
    workload.rankToId() = {
        {0, {0, 0, 0, 0, 0, 0}},
        {1, {0, 0, 0, 0, 0, 0}},
        {2, {0, 0, 0, 0, 0, 0}},
        {3, {0, 0, 0, 0, 0, 0}},
        {4, {0, 0, 0, 0, 0, 0}},
        {5, {0, 0, 0, 0, 0, 0}},
        {6, {0, 0, 0, 0, 0, 0}},
        {7, {0, 0, 0, 0, 0, 0}}};
  }

  validateAndFixWorkloads(simCfg, random);

  auto const& workloadNameInfos =
      *simCfg.newWorkloadSchedule()->workloadNameInfos();

  EXPECT_EQ(workloadNameInfos.size(), 8 + 4 + 1);

  EXPECT_THAT(
      workloadNameInfos,
      UnorderedElementsAre(
          Key("workload1-rail0"),
          Key("workload1-rail1"),
          Key("workload1-rail2"),
          Key("workload1-rail3"),
          Key("workload1-rail4"),
          Key("workload1-rail5"),
          Key("workload1-rail6"),
          Key("workload1-rail7"),
          Key("workload2-rail0"),
          Key("workload2-rail1"),
          Key("workload2-rail2"),
          Key("workload2-rail3"),
          Key("workload3")));

  for (uint32_t rail = 0; rail < 8; ++rail) {
    EXPECT_EQ(
        *workloadNameInfos.at(fmt::format("workload1-rail{}", rail))
             .originalWorkloadName(),
        "workload1");
    EXPECT_EQ(
        *workloadNameInfos.at(fmt::format("workload1-rail{}", rail))
             .internalWorkloadId(),
        rail);
  }

  for (uint32_t rail = 0; rail < 4; ++rail) {
    EXPECT_EQ(
        *workloadNameInfos.at(fmt::format("workload2-rail{}", rail))
             .originalWorkloadName(),
        "workload2");
    EXPECT_EQ(
        *workloadNameInfos.at(fmt::format("workload2-rail{}", rail))
             .internalWorkloadId(),
        rail + 8);
  }

  EXPECT_EQ(
      *workloadNameInfos.at("workload3").originalWorkloadName(), "workload3");
  EXPECT_EQ(*workloadNameInfos.at("workload3").internalWorkloadId(), 12);
}

// Test that rankToNvDomain is correctly re-mapped from global ranks to
// rail-local ranks during rail splitting.
TEST(SimFix, TestFixWorkloadsRailSplitPreservesNvDomain) {
  packsim::thrift::Simulation simCfg;
  Random random(/* seed= */ 0);

  auto& workload = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
  workload.name() = "workload_nv";
  packsim::workload::thrift::AllReduce arW;
  arW.sizeBytes() = 1000000;
  arW.numRails() = 2;
  workload.config()->allReduce() = std::move(arW);
  workload.numEndpoints() = 8;
  workload.rankToId() = {
      {0, {0, 0, 0, 0, 0, 0}},
      {1, {0, 0, 0, 0, 0, 1}},
      {2, {0, 0, 0, 0, 0, 2}},
      {3, {0, 0, 0, 0, 0, 3}},
      {4, {0, 0, 0, 0, 0, 4}},
      {5, {0, 0, 0, 0, 0, 5}},
      {6, {0, 0, 0, 0, 0, 6}},
      {7, {0, 0, 0, 0, 0, 7}}};
  // Ranks 0-3 in NV domain 0, ranks 4-7 in NV domain 1
  workload.rankToNvDomain() = {
      {0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 1}, {6, 1}, {7, 1}};

  validateAndFixWorkloads(simCfg, random);

  auto const& workloads = *simCfg.newWorkloadSchedule()->workloads();
  ASSERT_EQ(workloads.size(), 2);

  // With 2 rails and 8 global ranks:
  // Rail 0 gets global ranks 0, 2, 4, 6 -> rail-local ranks 0, 1, 2, 3
  // Rail 1 gets global ranks 1, 3, 5, 7 -> rail-local ranks 0, 1, 2, 3
  // globalRank = railRank * numRails + rail

  for (auto const& railW : workloads) {
    ASSERT_TRUE(railW.rankToNvDomain().has_value());
    ASSERT_EQ(railW.rankToNvDomain()->size(), 4);
  }

  // Rail 0: global ranks 0,2,4,6 -> NV domains 0,0,1,1
  auto const& rail0NvDomain = *workloads[0].rankToNvDomain();
  EXPECT_EQ(rail0NvDomain.at(0), 0); // global rank 0
  EXPECT_EQ(rail0NvDomain.at(1), 0); // global rank 2
  EXPECT_EQ(rail0NvDomain.at(2), 1); // global rank 4
  EXPECT_EQ(rail0NvDomain.at(3), 1); // global rank 6

  // Rail 1: global ranks 1,3,5,7 -> NV domains 0,0,1,1
  auto const& rail1NvDomain = *workloads[1].rankToNvDomain();
  EXPECT_EQ(rail1NvDomain.at(0), 0); // global rank 1
  EXPECT_EQ(rail1NvDomain.at(1), 0); // global rank 3
  EXPECT_EQ(rail1NvDomain.at(2), 1); // global rank 5
  EXPECT_EQ(rail1NvDomain.at(3), 1); // global rank 7
}

// Test that rail splitting works correctly when no rankToNvDomain is set.
TEST(SimFix, TestFixWorkloadsRailSplitWithoutNvDomain) {
  packsim::thrift::Simulation simCfg;
  Random random(/* seed= */ 0);

  auto& workload = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
  workload.name() = "workload_no_nv";
  packsim::workload::thrift::AllReduce arW;
  arW.sizeBytes() = 1000000;
  arW.numRails() = 2;
  workload.config()->allReduce() = std::move(arW);
  workload.numEndpoints() = 4;
  workload.rankToId() = {
      {0, {0, 0, 0, 0, 0, 0}},
      {1, {0, 0, 0, 0, 0, 1}},
      {2, {0, 0, 0, 0, 0, 2}},
      {3, {0, 0, 0, 0, 0, 3}}};
  // No rankToNvDomain set

  validateAndFixWorkloads(simCfg, random);

  auto const& workloads = *simCfg.newWorkloadSchedule()->workloads();
  ASSERT_EQ(workloads.size(), 2);

  for (auto const& railW : workloads) {
    EXPECT_FALSE(railW.rankToNvDomain().has_value());
  }
}

TEST(SimFix, TestFixWorkloadsRejectsZeroRails) {
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg({});
  simCfg.newWorkloadSchedule()
      ->workloads()
      ->at(0)
      .config()
      ->allGather()
      ->numRails() = 0;
  Random random(/* seed= */ 0);

  EXPECT_THAT(
      [&] { validateAndFixWorkloads(simCfg, random); },
      ThrowsMessage<std::runtime_error>(AllOf(
          HasSubstr("Workload ag"),
          HasSubstr("numRails must be at least 1, got 0"))));
}

// Global ranks 0 and 2 sit on rail 0 as rail ranks 0 and 1, and global ranks 1
// and 3 on rail 1 as 0 and 1; the 0 -> 1 entry spans two rails.
TEST(SimFix, TestFixWorkloadsSplitsASuppliedNicQpnPairMapPerRail) {
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg({});
  auto& wkld = simCfg.newWorkloadSchedule()->workloads()->at(0);
  const auto addEntry = [&wkld](uint32_t src, uint32_t dst, uint32_t qpn) {
    packsim::workload::thrift::QPNPair qpnPair;
    qpnPair.local() = qpn;
    qpnPair.remote() = qpn + 1;
    (*wkld.nicQpnPairMap())[makeRankPair(src, dst)][makeNicPair(0, 0)]
        .emplace_back(std::move(qpnPair));
    (*wkld.qpnPortConfigs())[qpn] = packsim::thrift::QpnPortConfig{};
  };
  addEntry(0, 2, 11);
  addEntry(1, 3, 13);
  addEntry(0, 1, 15);
  Random random(/* seed= */ 0);

  validateAndFixWorkloads(simCfg, random);

  const auto& workloads = *simCfg.newWorkloadSchedule()->workloads();
  ASSERT_EQ(workloads.size(), 2);
  const auto railPair = makeRankPair(0, 1);
  for (const auto& [index, expectedQpn] :
       {std::pair<size_t, uint32_t>{0, 11}, {1, 13}}) {
    const auto& railW = workloads.at(index);
    ASSERT_EQ(railW.nicQpnPairMap()->size(), 1) << *railW.name();
    EXPECT_EQ(
        *railW.nicQpnPairMap()
             ->at(railPair)
             .at(makeNicPair(0, 0))
             .at(0)
             .local(),
        expectedQpn)
        << *railW.name();
    EXPECT_THAT(*railW.qpnPortConfigs(), UnorderedElementsAre(Key(expectedQpn)))
        << *railW.name();
  }
}

TEST(SimFix, TestFixWorkloadsSplitsASuppliedQpnPairMapPerRail) {
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg({});
  auto& wkld = simCfg.newWorkloadSchedule()->workloads()->at(0);
  const auto addEntry = [&wkld](uint32_t src, uint32_t dst, uint32_t qpn) {
    packsim::workload::thrift::QPNPair qpnPair;
    qpnPair.local() = qpn;
    qpnPair.remote() = qpn + 1;
    (*wkld.qpnPairMap())[src][dst].emplace_back(std::move(qpnPair));
    (*wkld.qpnPortConfigs())[qpn] = packsim::thrift::QpnPortConfig{};
  };
  addEntry(0, 2, 11);
  addEntry(1, 3, 13);
  addEntry(0, 1, 15);
  Random random(/* seed= */ 0);

  validateAndFixWorkloads(simCfg, random);

  const auto& workloads = *simCfg.newWorkloadSchedule()->workloads();
  ASSERT_EQ(workloads.size(), 2);
  for (const auto& [index, expectedQpn] :
       {std::pair<size_t, uint32_t>{0, 11}, {1, 13}}) {
    const auto& railW = workloads.at(index);
    ASSERT_EQ(railW.qpnPairMap()->size(), 1) << *railW.name();
    ASSERT_EQ(railW.qpnPairMap()->at(0).size(), 1) << *railW.name();
    EXPECT_EQ(*railW.qpnPairMap()->at(0).at(1).at(0).local(), expectedQpn)
        << *railW.name();
    EXPECT_THAT(*railW.qpnPortConfigs(), UnorderedElementsAre(Key(expectedQpn)))
        << *railW.name();
  }
}

TEST(BuildNicQPNMapFromQpTemplates, MultipleQpTemplates) {
  // Test with multiple QP templates
  packsim::workload::thrift::Workload workload;
  workload.name() = "test_workload";
  workload.rankToId() = {{0, {0}}, {1, {1}}};

  // Add two QP templates between same ranks but different NICs
  workload.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(0, 1, 0, 0, {1, 2}, {3, 4}));
  workload.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(0, 1, 1, 1, {5, 6}, {7, 8}));

  uint32_t nextQpn = 0;

  buildNicQPNMapFromQpTemplates(workload, nextQpn);

  // Two templates create 4 QPNs (srcQpn and dstQpn for each template)
  // With globally unique QPNs, all 4 should be in qpnPortConfigs
  EXPECT_EQ(workload.qpnPortConfigs()->size(), 4);

  // nextQpn should be 4 (2 templates * 2 QPNs each)
  EXPECT_EQ(nextQpn, 4);

  // Verify nicQpnPairMap has entries for both directions for both templates
  // 2 rank pairs (forward 0->1 and reverse 1->0)
  EXPECT_EQ(workload.nicQpnPairMap()->size(), 2);
}

TEST(BuildNicQPNMapFromQpTemplates, ThrowsOnMissingSrcRank) {
  // Test that function throws when source rank is not in rankToId
  packsim::workload::thrift::Workload workload;
  workload.name() = "test_workload";
  workload.rankToId() = {{1, {1}}}; // Only rank 1, no rank 0

  workload.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(
          0, 1, 0, 0, {1}, {2})); // src rank 0 not in rankToId

  uint32_t nextQpn = 0;

  EXPECT_THAT(
      [&]() { buildNicQPNMapFromQpTemplates(workload, nextQpn); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Failed to find source rank 0")));
}

TEST(BuildNicQPNMapFromQpTemplates, ThrowsOnMissingDstRank) {
  // Test that function throws when destination rank is not in rankToId
  packsim::workload::thrift::Workload workload;
  workload.name() = "test_workload";
  workload.rankToId() = {{0, {0}}}; // Only rank 0, no rank 1

  workload.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(
          0, 1, 0, 0, {1}, {2})); // dst rank 1 not in rankToId

  uint32_t nextQpn = 0;

  EXPECT_THAT(
      [&]() { buildNicQPNMapFromQpTemplates(workload, nextQpn); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Failed to find destination rank 1")));
}

TEST(BuildNicQPNMapFromQpTemplates, SharedNextQpnAcrossWorkloads) {
  // Test that QPNs are unique across workloads sharing the same nextQpn counter
  packsim::workload::thrift::Workload workload1;
  workload1.name() = "workload1";
  workload1.rankToId() = {{0, {0}}, {1, {1}}};
  workload1.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(0, 1, 0, 0, {1}, {2}));

  packsim::workload::thrift::Workload workload2;
  workload2.name() = "workload2";
  workload2.rankToId() = {{0, {0}}, {1, {1}}};
  workload2.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(0, 1, 0, 0, {3}, {4}));

  uint32_t nextQpn = 0;

  buildNicQPNMapFromQpTemplates(workload1, nextQpn);
  buildNicQPNMapFromQpTemplates(workload2, nextQpn);

  // Each template creates 2 QPNs (srcQpn and dstQpn)
  // workload1: QPNs 1, 2
  // workload2: QPNs 3, 4
  EXPECT_EQ(nextQpn, 4);

  // Verify workload1 and workload2 have different QPNs
  EXPECT_TRUE(workload1.qpnPortConfigs()->contains(1));
  EXPECT_TRUE(workload1.qpnPortConfigs()->contains(2));
  EXPECT_TRUE(workload2.qpnPortConfigs()->contains(3));
  EXPECT_TRUE(workload2.qpnPortConfigs()->contains(4));
}

TEST(BuildNicQPNMapFromQpTemplates, VerifyReverseMappings) {
  // Test that reverse mappings are correctly created
  packsim::workload::thrift::Workload workload;
  workload.name() = "test_workload";
  workload.rankToId() = {{0, {0}}, {1, {1}}};
  workload.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(0, 1, 0, 1, {1, 2}, {3, 4}));

  uint32_t nextQpn = 0;

  buildNicQPNMapFromQpTemplates(workload, nextQpn);

  // Check forward mapping: rank(0,1) nic(0,1)
  packsim::thrift::RankPair forwardRankPair;
  forwardRankPair.srcRank() = 0;
  forwardRankPair.dstRank() = 1;

  packsim::thrift::NicPair forwardNicPair;
  forwardNicPair.srcNicIdx() = 0;
  forwardNicPair.dstNicIdx() = 1;

  EXPECT_EQ(
      (*workload.nicQpnPairMap())[forwardRankPair][forwardNicPair].size(), 1);

  // Check reverse mapping: rank(1,0) nic(1,0)
  packsim::thrift::RankPair reverseRankPair;
  reverseRankPair.srcRank() = 1;
  reverseRankPair.dstRank() = 0;

  packsim::thrift::NicPair reverseNicPair;
  reverseNicPair.srcNicIdx() = 1;
  reverseNicPair.dstNicIdx() = 0;

  EXPECT_EQ(
      (*workload.nicQpnPairMap())[reverseRankPair][reverseNicPair].size(), 1);

  // Verify the QPN pairs are correctly linked
  const auto& forwardPair =
      (*workload.nicQpnPairMap())[forwardRankPair][forwardNicPair][0];
  const auto& reversePair =
      (*workload.nicQpnPairMap())[reverseRankPair][reverseNicPair][0];

  EXPECT_EQ(*forwardPair.local(), *reversePair.remote());
  EXPECT_EQ(*forwardPair.remote(), *reversePair.local());
}

// ---------------------------------------------------------------------------
// getNumQPs
// ---------------------------------------------------------------------------

TEST(GetNumQPs, DefaultsToTwoWithoutChannelSettings) {
  packsim::workload::thrift::Workload workload;
  EXPECT_EQ(getNumQPs(workload), 2);
}

TEST(GetNumQPs, ComputesFromChannelSettings) {
  packsim::workload::thrift::Workload workload;
  workload.channelSettings().ensure().numChannels() = 4;
  workload.channelSettings()->numQPsPerChannel() = 3;
  // numChannels * numQPsPerChannel + 1 (notify QP)
  EXPECT_EQ(getNumQPs(workload), 4 * 3 + 1);
}

// ---------------------------------------------------------------------------
// buildQPNMap
// ---------------------------------------------------------------------------

TEST(BuildQPNMap, AssignsMirroredQpnPairsForRankPeers) {
  // allToAll with 2 endpoints -> getRankPeers yields the single pair (0, 1).
  // Both ranks map to the same device id, so they share one QPN counter, making
  // the forward/reverse QPNs differ and exercising the mirror assignment.
  std::vector<packsim::workload::thrift::Workload> workloads(1);
  auto& wkld = workloads[0];
  wkld.name() = "qp";
  wkld.workloadId() = 0;
  wkld.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  wkld.numEndpoints() = 2;
  wkld.rankToId() = {{0, {7}}, {1, {7}}};

  buildQPNMap(workloads);

  const auto& qpnMap = *wkld.qpnPairMap();
  ASSERT_EQ(qpnMap.size(), 2);
  const auto& forward = qpnMap.at(0).at(1);
  const auto& reverse = qpnMap.at(1).at(0);
  // Default getNumQPs == 2, so two QP pairs per direction.
  ASSERT_EQ(forward.size(), 2);
  ASSERT_EQ(reverse.size(), 2);
  // The two pairs in one direction use distinct local QPNs (counter advanced).
  EXPECT_NE(*forward[0].local(), *forward[1].local());
  // Within a single pair, local and remote QPNs differ (two counter bumps).
  EXPECT_NE(*forward[0].local(), *forward[0].remote());
  // Forward and reverse are mirror images of each other.
  for (size_t i = 0; i < forward.size(); ++i) {
    EXPECT_EQ(*forward[i].local(), *reverse[i].remote());
    EXPECT_EQ(*forward[i].remote(), *reverse[i].local());
  }
}

TEST(BuildQPNMap, HonorsChannelSettingsQpCount) {
  std::vector<packsim::workload::thrift::Workload> workloads(1);
  auto& wkld = workloads[0];
  wkld.name() = "qp";
  wkld.workloadId() = 0;
  wkld.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  wkld.numEndpoints() = 2;
  wkld.rankToId() = {{0, {0}}, {1, {1}}};
  wkld.channelSettings().ensure().numChannels() = 2;
  wkld.channelSettings()->numQPsPerChannel() = 2;

  buildQPNMap(workloads);

  // getNumQPs == 2 * 2 + 1 == 5 QP pairs per direction.
  EXPECT_EQ(wkld.qpnPairMap()->at(0).at(1).size(), 5);
  EXPECT_EQ(wkld.qpnPairMap()->at(1).at(0).size(), 5);
}

TEST(BuildQPNMap, DoesNotOverwriteExistingMap) {
  std::vector<packsim::workload::thrift::Workload> workloads(1);
  auto& wkld = workloads[0];
  wkld.name() = "qp";
  wkld.workloadId() = 0;
  wkld.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  wkld.numEndpoints() = 2;
  wkld.rankToId() = {{0, {0}}, {1, {1}}};
  packsim::workload::thrift::QPNPair sentinel;
  sentinel.local() = 99;
  sentinel.remote() = 88;
  wkld.qpnPairMap()[5][6].emplace_back(sentinel);

  buildQPNMap(workloads);

  // The pre-existing map is left untouched.
  ASSERT_EQ(wkld.qpnPairMap()->size(), 1);
  EXPECT_EQ(*wkld.qpnPairMap()->at(5).at(6).at(0).local(), 99);
}

TEST(BuildQPNMap, ThrowsOnWorkloadIdMismatch) {
  std::vector<packsim::workload::thrift::Workload> workloads(1);
  auto& wkld = workloads[0];
  wkld.name() = "qp";
  wkld.workloadId() = 5; // does not match its index (0)
  wkld.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  wkld.numEndpoints() = 2;
  wkld.rankToId() = {{0, {0}}, {1, {1}}};

  EXPECT_THAT(
      [&]() { buildQPNMap(workloads); },
      ThrowsMessage<std::runtime_error>(HasSubstr("WorkloadID mismatch")));
}

// ---------------------------------------------------------------------------
// buildNicQPNMapFromLegacy
// ---------------------------------------------------------------------------

TEST(BuildNicQPNMapFromLegacy, ConvertsLegacyMapToNicZero) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "legacy";
  packsim::workload::thrift::QPNPair pair;
  pair.local() = 10;
  pair.remote() = 20;
  workload.qpnPairMap()[0][1].emplace_back(pair);

  buildNicQPNMapFromLegacy(workload);

  packsim::thrift::RankPair rankPair;
  rankPair.srcRank() = 0;
  rankPair.dstRank() = 1;
  packsim::thrift::NicPair nicPair; // legacy entries are attributed to NIC 0
  nicPair.srcNicIdx() = 0;
  nicPair.dstNicIdx() = 0;
  const auto& converted = (*workload.nicQpnPairMap())[rankPair][nicPair];
  ASSERT_EQ(converted.size(), 1);
  EXPECT_EQ(*converted[0].local(), 10);
  EXPECT_EQ(*converted[0].remote(), 20);
}

TEST(BuildNicQPNMapFromLegacy, EmptyLegacyMapProducesEmptyNicMap) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "empty";
  buildNicQPNMapFromLegacy(workload);
  EXPECT_TRUE(workload.nicQpnPairMap()->empty());
}

// ---------------------------------------------------------------------------
// buildNicQPNMap (routing between template, legacy, and skip paths)
// ---------------------------------------------------------------------------

TEST(BuildNicQPNMap, PrefersQpTemplatesAndClearsThem) {
  std::vector<packsim::workload::thrift::Workload> workloads(1);
  auto& wkld = workloads[0];
  wkld.name() = "templated";
  wkld.rankToId() = {{0, {0}}, {1, {1}}};
  wkld.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(0, 1, 0, 0, {1}, {2}));

  uint32_t nextQpn = 0;
  buildNicQPNMap(workloads, nextQpn);

  EXPECT_EQ(nextQpn, 2); // one template -> a src and a dst QPN
  EXPECT_FALSE(wkld.nicQpnPairMap()->empty());
  EXPECT_TRUE(wkld.qpTemplates()->empty()); // cleared after building
}

TEST(BuildNicQPNMap, FallsBackToLegacyWhenNoTemplates) {
  std::vector<packsim::workload::thrift::Workload> workloads(1);
  auto& wkld = workloads[0];
  wkld.name() = "legacy";
  packsim::workload::thrift::QPNPair pair;
  pair.local() = 3;
  pair.remote() = 4;
  wkld.qpnPairMap()[0][1].emplace_back(pair);

  uint32_t nextQpn = 0;
  buildNicQPNMap(workloads, nextQpn);

  EXPECT_FALSE(wkld.nicQpnPairMap()->empty());
  EXPECT_EQ(nextQpn, 0); // legacy path reuses existing QPNs, not the counter
}

TEST(BuildNicQPNMap, SkipsWorkloadWithPopulatedNicMap) {
  std::vector<packsim::workload::thrift::Workload> workloads(1);
  auto& wkld = workloads[0];
  wkld.name() = "already_built";
  wkld.rankToId() = {{0, {0}}, {1, {1}}};
  wkld.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(0, 1, 0, 0, {1}, {2}));
  packsim::thrift::RankPair rankPair;
  rankPair.srcRank() = 0;
  rankPair.dstRank() = 1;
  packsim::thrift::NicPair nicPair;
  nicPair.srcNicIdx() = 0;
  nicPair.dstNicIdx() = 0;
  (*wkld.nicQpnPairMap())[rankPair][nicPair] = {};

  uint32_t nextQpn = 0;
  buildNicQPNMap(workloads, nextQpn);

  EXPECT_FALSE(wkld.qpTemplates()->empty()); // skipped -> templates untouched
  EXPECT_EQ(nextQpn, 0);
}

// ---------------------------------------------------------------------------
// fixOneWorkload: permutation and batch paths
// ---------------------------------------------------------------------------

TEST(FixOneWorkload, PermutationGeneratesDerangementDstRanks) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "perm";
  workload.config()->permutation() = packsim::workload::thrift::Permutation{};
  workload.numEndpoints() = 8;
  Random random(/* seed= */ 0);

  fixOneWorkload(workload, random);

  const auto& dstRanks = *workload.config()->permutation()->dstRanks();
  ASSERT_EQ(dstRanks.size(), 8);
  // The generated dstRanks form a single rank cycle: a permutation of 0..7
  // with no fixed points. Assert those structural properties rather than the
  // specific random shuffle.
  std::vector<uint32_t> sorted(dstRanks.begin(), dstRanks.end());
  std::sort(sorted.begin(), sorted.end());
  std::vector<uint32_t> expectedSorted(dstRanks.size());
  std::iota(expectedSorted.begin(), expectedSorted.end(), 0U);
  EXPECT_EQ(sorted, expectedSorted); // dstRanks is a permutation of 0..n-1
  // The derangement property is inherently per-element (no rank maps to
  // itself).
  for (uint32_t i = 0; i < 8; ++i) {
    EXPECT_NE(dstRanks[i], i) << "rank " << i << " maps to itself";
  }
}

TEST(FixOneWorkload, PermutationPreservesExistingDstRanks) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "perm";
  packsim::workload::thrift::Permutation perm;
  perm.dstRanks() = {1, 0};
  workload.config()->permutation() = std::move(perm);
  workload.numEndpoints() = 2;
  Random random(/* seed= */ 0);

  fixOneWorkload(workload, random);

  const std::vector<uint32_t> expected = {1, 0};
  EXPECT_EQ(*workload.config()->permutation()->dstRanks(), expected);
}

TEST(FixOneWorkload, BatchCopiesRankToIdToChildrenAndRenumbers) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "batch";
  workload.rankToId() = {{0, {0}}, {1, {1}}};
  packsim::workload::thrift::Batch batch;
  {
    auto& child = batch.children()->emplace_back();
    child.name() = "child0";
    child.config()->allToAll() = packsim::workload::thrift::AllToAll{};
    child.numEndpoints() = 2;
    // rankToId left empty -> must inherit the parent's mapping
  }
  {
    auto& child = batch.children()->emplace_back();
    child.name() = "child1";
    child.config()->allToAll() = packsim::workload::thrift::AllToAll{};
    child.numEndpoints() = 2;
  }
  workload.config()->batch() = std::move(batch);
  Random random(/* seed= */ 0);

  fixOneWorkload(workload, random);

  const auto& children = *workload.config()->batch()->children();
  ASSERT_EQ(children.size(), 2);
  const std::map<uint32_t, std::vector<uint32_t>> expectedRankToId = {
      {0, {0}}, {1, {1}}};
  EXPECT_EQ(*children[0].rankToId(), expectedRankToId);
  // Children are renumbered starting from 1.
  EXPECT_EQ(*children[0].workloadId(), 1);
  EXPECT_EQ(*children[1].workloadId(), 2);
}

TEST(FixOneWorkload, BatchRecursivelyFixesChildren) {
  // A child that itself needs fixing (a permutation with empty dstRanks) must
  // be fixed by the recursive fixOneWorkload call inside the batch handler.
  packsim::workload::thrift::Workload workload;
  workload.name() = "batch";
  workload.rankToId() = {{0, {0}}, {1, {1}}, {2, {2}}, {3, {3}}};
  packsim::workload::thrift::Batch batch;
  auto& child = batch.children()->emplace_back();
  child.name() = "child_perm";
  child.config()->permutation() = packsim::workload::thrift::Permutation{};
  child.numEndpoints() = 4;
  workload.config()->batch() = std::move(batch);
  Random random(/* seed= */ 0);

  fixOneWorkload(workload, random);

  const auto& fixedChild = workload.config()->batch()->children()->at(0);
  EXPECT_EQ(fixedChild.config()->permutation()->dstRanks()->size(), 4);
}

// Only permutation and batch workloads are fixed, and only AllGather,
// ReduceScatter and AllReduce are split across rails, so any other workload
// comes back as it went in.
TEST(FixOneWorkload, IncastIsNeitherFixedNorSplit) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "incast";
  workload.config()->incast() = packsim::workload::thrift::Incast{};
  workload.numEndpoints() = 2;
  workload.rankToId() = {{0, {0}}, {1, {1}}};
  const auto original = workload;
  Random random(/*seed=*/0);

  const auto fixed = fixOneWorkload(workload, random);

  const std::vector<packsim::workload::thrift::Workload> expected = {original};
  EXPECT_EQ(fixed, expected);
}

TEST(FixOneWorkload, RankToIdKeysBelowNumEndpointsAreAccepted) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "a2a";
  workload.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  workload.numEndpoints() = 2;
  workload.rankToId() = {{0, {0}}, {1, {1}}};
  Random random(/* seed= */ 0);

  EXPECT_NO_THROW(fixOneWorkload(workload, random));
}

// A rank indexes per-rank arrays sized by numEndpoints, such as the rows of
// AllToAll.vectorizedSizesBytes, so a rank equal to numEndpoints reads past
// their end.
TEST(FixOneWorkload, RankToIdKeyEqualToNumEndpointsIsRejected) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "a2a";
  workload.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  workload.numEndpoints() = 2;
  workload.rankToId() = {{0, {0}}, {1, {1}}, {2, {2}}};
  Random random(/* seed= */ 0);

  EXPECT_THAT(
      [&]() { fixOneWorkload(workload, random); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload a2a: rankToId has rank 2, but numEndpoints is 2")));
}

TEST(FixOneWorkload, RankToIdListKeyEqualToNumEndpointsIsRejected) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "a2a";
  workload.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  workload.numEndpoints() = 2;
  workload.rankToId() = {{0, {0}}, {1, {1}}};
  workload.rankToIdList() = {{0, {{0}}}, {2, {{2}}}};
  Random random(/* seed= */ 0);

  EXPECT_THAT(
      [&]() { fixOneWorkload(workload, random); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload a2a: rankToIdList has rank 2, but numEndpoints is 2")));
}

// A child that inherits its parent's rankToId is checked against its own
// numEndpoints, which builds the child's per-rank arrays.
TEST(FixOneWorkload, BatchChildWithFewerEndpointsThanInheritedRanksIsRejected) {
  packsim::workload::thrift::Workload workload;
  workload.name() = "batch";
  workload.numEndpoints() = 4;
  workload.rankToId() = {{0, {0}}, {1, {1}}, {2, {2}}, {3, {3}}};
  packsim::workload::thrift::Batch batch;
  auto& child = batch.children()->emplace_back();
  child.name() = "child0";
  child.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  child.numEndpoints() = 2;
  workload.config()->batch() = std::move(batch);
  Random random(/* seed= */ 0);

  EXPECT_THAT(
      [&]() { fixOneWorkload(workload, random); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload child0: rankToId has rank 2, but numEndpoints is 2")));
}

// ---------------------------------------------------------------------------
// fixupSimCfg (end-to-end orchestration)
// ---------------------------------------------------------------------------

TEST(FixupSimCfg, PopulatesDerivedFieldsForSimpleWorkload) {
  packsim::thrift::Simulation simCfg;
  Random random(/* seed= */ 0);

  auto& wkld = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
  wkld.name() = "a2a";
  wkld.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  wkld.numEndpoints() = 2;
  wkld.rankToId() = {{0, {0}}, {1, {1}}};

  fixupSimCfg(simCfg, random);

  // fixNicCfg copies the legacy nic into the "default" entry.
  EXPECT_TRUE(simCfg.nicConfigMap()->contains("default"));

  const auto& fixedWorkloads = *simCfg.newWorkloadSchedule()->workloads();
  ASSERT_EQ(fixedWorkloads.size(), 1);
  const auto& fixed = fixedWorkloads[0];
  EXPECT_EQ(*fixed.workloadId(), 0);
  // buildQPNMap then buildNicQPNMap populate both maps.
  EXPECT_FALSE(fixed.qpnPairMap()->empty());
  EXPECT_FALSE(fixed.nicQpnPairMap()->empty());
  // fixRankToIdList derives rankToIdList from rankToId.
  EXPECT_FALSE(fixed.rankToIdList()->empty());
}

TEST(FixupSimCfg, RejectsIncastWithASourceCountNotBelowNumEndpoints) {
  packsim::thrift::Simulation simCfg;
  Random random(/* seed= */ 0);

  auto& wkld = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
  wkld.name() = "incast";
  packsim::workload::thrift::Incast incast;
  incast.numSources() = 2;
  wkld.config()->incast() = std::move(incast);
  wkld.numEndpoints() = 2;
  wkld.rankToId() = {{0, {0}}, {1, {1}}};

  EXPECT_THAT(
      [&] { fixupSimCfg(simCfg, random); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("numSources 2")));
}

// One template gives each direction of the pair a single QP, which data and the
// notify would then share.
TEST(FixupSimCfg, RejectsARankPairWithOneQpOnItsFirstNicPair) {
  packsim::thrift::Simulation simCfg;
  Random random(/* seed= */ 0);

  auto& wkld = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
  wkld.name() = "a2a";
  wkld.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  wkld.numEndpoints() = 2;
  wkld.rankToId() = {{0, {0}}, {1, {1}}};
  wkld.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(0, 1, 0, 0, {0}, {0}));

  EXPECT_THAT(
      [&] { fixupSimCfg(simCfg, random); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload a2a: rank pair (0, 1) has 1 QP(s) on its first NIC pair "
          "(0, 0)")));
}

// Channel QPs come from qpnPairMap and the templates' QPNs are numbered apart
// from it, so a channel QP could be the notify QP.
TEST(FixupSimCfg, RejectsChannelSettingsWithQpTemplates) {
  packsim::thrift::Simulation simCfg;
  Random random(/* seed= */ 0);

  auto& wkld = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
  wkld.name() = "a2a";
  wkld.config()->allToAll() = packsim::workload::thrift::AllToAll{};
  wkld.numEndpoints() = 2;
  wkld.rankToId() = {{0, {0}}, {1, {1}}};
  wkld.channelSettings() = packsim::workload::thrift::ChannelSettings{};
  wkld.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(0, 1, 0, 0, {0}, {0}));
  wkld.qpTemplates()->emplace_back(
      SimFixTestFixture::createQpTemplate(1, 0, 0, 0, {0}, {0}));

  EXPECT_THAT(
      [&] { fixupSimCfg(simCfg, random); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload a2a: qpTemplates or a nicQpnPairMap supplied without its "
          "qpnPairMap cannot be combined with channel settings")));
}

TEST(FixupSimCfg, RejectsChannelSettingsWithANicQpnPairMapButNoQpnPairMap) {
  auto simCfg = makeChannelSimCfgWithSuppliedNicQpnPairMap();
  Random random(/* seed= */ 0);

  EXPECT_THAT(
      [&] { fixupSimCfg(simCfg, random); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("cannot be combined with channel settings")));
}

// A config that already went through fixupSimCfg carries both maps, numbered
// together.
TEST(FixupSimCfg, AcceptsChannelSettingsWithANicQpnPairMapAndItsQpnPairMap) {
  auto simCfg = makeChannelSimCfgWithSuppliedNicQpnPairMap();
  auto& wkld = simCfg.newWorkloadSchedule()->workloads()->at(0);
  for (const auto& [rankPair, qpnsByNicPair] : *wkld.nicQpnPairMap()) {
    (*wkld.qpnPairMap())[*rankPair.srcRank()][*rankPair.dstRank()] =
        qpnsByNicPair.at(makeNicPair(0, 0));
  }
  Random random(/* seed= */ 0);

  EXPECT_NO_THROW(fixupSimCfg(simCfg, random));
}

// ---------------------------------------------------------------------------
// Rail split: per-rail queue-pair templates and metadata
// ---------------------------------------------------------------------------

TEST(SimFixRailQpTemplates, RenumbersTemplateRanksToRailLocalIndices) {
  // Global (0,2) sits entirely on rail 0 and becomes rail-local (0,1);
  // global (1,3) does the same on rail 1.
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg(
      {SimFixTestFixture::createQpTemplate(0, 2, 0, 0, {1}, {2}),
       SimFixTestFixture::createQpTemplate(1, 3, 0, 0, {3}, {4})});
  Random random(/* seed= */ 0);

  validateAndFixWorkloads(simCfg, random);

  const auto& workloads = *simCfg.newWorkloadSchedule()->workloads();
  ASSERT_EQ(workloads.size(), 2);
  for (const auto& railW : workloads) {
    ASSERT_EQ(railW.qpTemplates()->size(), 1) << *railW.name();
    const auto& ranks = *railW.qpTemplates()->at(0).ranks();
    EXPECT_EQ(*ranks.srcRank(), 0) << *railW.name();
    EXPECT_EQ(*ranks.dstRank(), 1) << *railW.name();
  }
}

TEST(SimFixRailQpTemplates, KeepsThePortConfigOfTheTemplateItCarries) {
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg(
      {SimFixTestFixture::createQpTemplate(0, 2, 0, 0, {1, 2}, {3, 4})});
  Random random(/* seed= */ 0);

  validateAndFixWorkloads(simCfg, random);

  const auto& rail0 = simCfg.newWorkloadSchedule()->workloads()->at(0);
  ASSERT_EQ(*rail0.name(), "ag-rail0");
  ASSERT_EQ(rail0.qpTemplates()->size(), 1);
  const std::vector<uint16_t> expectedSrcPorts{1, 2};
  const std::vector<uint16_t> expectedDstPorts{3, 4};
  const auto& ports = *rail0.qpTemplates()->at(0).ports();
  EXPECT_EQ(*ports.srcPorts(), expectedSrcPorts);
  EXPECT_EQ(*ports.dstPorts(), expectedDstPorts);
}

TEST(SimFixRailQpTemplates, DropsTemplatesThatSpanTwoRails) {
  // Global ranks 0 and 1 land on different rails, so the split never creates
  // that connection. Carrying the template would name a rank the rail
  // sub-workload does not have, which buildNicQPNMapFromQpTemplates rejects.
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg(
      {SimFixTestFixture::createQpTemplate(0, 1, 0, 0, {1}, {2}),
       SimFixTestFixture::createQpTemplate(2, 3, 0, 0, {3}, {4})});
  Random random(/* seed= */ 0);

  validateAndFixWorkloads(simCfg, random);

  for (const auto& railW : *simCfg.newWorkloadSchedule()->workloads()) {
    EXPECT_TRUE(railW.qpTemplates()->empty()) << *railW.name();
  }
}

TEST(SimFixRailQpTemplates, CarriesMetadataOntoEveryRail) {
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg({});
  simCfg.newWorkloadSchedule()->workloads()->at(0).metadata() = {
      {"experiment", "demo-allgather"}};
  Random random(/* seed= */ 0);

  validateAndFixWorkloads(simCfg, random);

  const auto& workloads = *simCfg.newWorkloadSchedule()->workloads();
  ASSERT_EQ(workloads.size(), 2);
  for (const auto& railW : workloads) {
    EXPECT_EQ(railW.metadata()->at("experiment"), "demo-allgather")
        << *railW.name();
  }
}

TEST(SimFixRailQpTemplates, CarriesRingNicSelectionOntoEveryRail) {
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg({});
  simCfg.newWorkloadSchedule()
      ->workloads()
      ->at(0)
      .config()
      ->allGather()
      ->nicSelectionMode() =
      packsim::workload::thrift::RingNicSelectionMode::ALL_AVAILABLE_NICS;
  Random random(/* seed= */ 0);

  validateAndFixWorkloads(simCfg, random);

  for (const auto& railW : *simCfg.newWorkloadSchedule()->workloads()) {
    EXPECT_EQ(
        *railW.config()->allGather()->nicSelectionMode(),
        packsim::workload::thrift::RingNicSelectionMode::ALL_AVAILABLE_NICS);
  }
}

TEST(SimFixRailQpTemplates, CarriesPerNicRankToIdListOntoEachRail) {
  // rankToId is accel-level; rankToIdList carries the per-NIC device ids.
  // Regenerating the list from rankToId instead would drop the trailing NIC
  // component, and every destination would then be unreachable.
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg({});
  simCfg.newWorkloadSchedule()->workloads()->at(0).rankToIdList() = {
      {0, {{1, 0, 1, 1, 1, 0}}},
      {1, {{1, 0, 1, 2, 1, 0}}},
      {2, {{1, 0, 2, 1, 1, 0}}},
      {3, {{1, 0, 2, 2, 1, 0}}}};
  Random random(/* seed= */ 0);

  validateAndFixWorkloads(simCfg, random);

  const auto& workloads = *simCfg.newWorkloadSchedule()->workloads();
  ASSERT_EQ(workloads.size(), 2);
  // Rail 0 owns global ranks 0 and 2, rail 1 owns global ranks 1 and 3.
  const std::vector<std::vector<uint32_t>> expectedRail0Rank1{
      {1, 0, 2, 1, 1, 0}};
  const std::vector<std::vector<uint32_t>> expectedRail1Rank1{
      {1, 0, 2, 2, 1, 0}};
  EXPECT_EQ(workloads[0].rankToIdList()->at(1), expectedRail0Rank1);
  EXPECT_EQ(workloads[1].rankToIdList()->at(1), expectedRail1Rank1);
}

TEST(FixupSimCfg, BuildsPortConfigsForEveryRailOfASplitWorkload) {
  // Regression: the rail split used to drop qpTemplates, so qpnPortConfigs
  // stayed empty for every rail. Port-level addressing then failed at the
  // first packet with "QPN port configs are expected when using port level
  // addresses" -- at run time, long after config validation had passed.
  auto simCfg = SimFixTestFixture::makeTwoRailAllGatherSimCfg(
      {SimFixTestFixture::createQpTemplate(0, 2, 0, 0, {1}, {2}),
       SimFixTestFixture::createQpTemplate(2, 0, 0, 0, {2}, {1}),
       SimFixTestFixture::createQpTemplate(1, 3, 0, 0, {3}, {4}),
       SimFixTestFixture::createQpTemplate(3, 1, 0, 0, {4}, {3})});
  Random random(/* seed= */ 0);

  fixupSimCfg(simCfg, random);

  const auto& workloads = *simCfg.newWorkloadSchedule()->workloads();
  ASSERT_EQ(workloads.size(), 2);
  for (const auto& railW : workloads) {
    EXPECT_FALSE(railW.qpnPortConfigs()->empty()) << *railW.name();
    EXPECT_FALSE(railW.nicQpnPairMap()->empty()) << *railW.name();
  }
}

} // namespace packsim
