/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/random.h"
#include "packsim/util/builders.h"

namespace packsim {
namespace {

using ::testing::AllOf;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

using PrefixId = std::vector<uint32_t>;
using NodeType = thrift::NodeType;
using SchedulingMethod = thrift::SchedulingMethod;
using SchedulingUnit = thrift::SchedulingUnit;

// Every accelerator in the fixture topology gets the FlexId {1, index}.
PrefixId flexId(const uint32_t index) {
  return PrefixId{1, index};
}

// Sorts before every accelerator's FlexId.
const PrefixId kSwitchFlexId{0, 0};

thrift::Node
makeNode(const std::string& name, const PrefixId& id, const NodeType type) {
  thrift::Node node;
  node.name() = name;
  node.id() = id;
  node.type() = type;
  return node;
}

// Accelerator map keys are deliberately the reverse of FlexId order, so a test
// that reads back FlexId order proves the builder sorted rather than relying on
// the map's own iteration order. The SWITCH node is given a FlexId that sorts
// BEFORE every accelerator, so any test reading back placement order fails if
// the builder's ACCEL filter is dropped.
thrift::FlexTopology makeTopology(const uint32_t numAccel) {
  thrift::FlexTopology topology;
  for (uint32_t i = 0; i < numAccel; i++) {
    const auto name = fmt::format("accel{:02d}", numAccel - 1 - i);
    topology.nodes()->emplace(name, makeNode(name, flexId(i), NodeType::ACCEL));
  }
  topology.nodes()->emplace(
      "zzz_switch", makeNode("zzz_switch", kSwitchFlexId, NodeType::SWITCH));
  return topology;
}

thrift::WorkloadNewSchedule makeSchedule(
    const SchedulingUnit unit,
    const SchedulingMethod method,
    const std::vector<int32_t>& endpointsPerWorkload) {
  thrift::WorkloadNewSchedule sched;
  sched.unit() = unit;
  sched.method() = method;
  for (const auto numEndpoints : endpointsPerWorkload) {
    workload::thrift::Workload workload;
    workload.numEndpoints() = numEndpoints;
    sched.workloads()->emplace_back(std::move(workload));
  }
  return sched;
}

// Ranks of one workload, in ascending rank order.
std::vector<PrefixId> placementOf(
    const thrift::WorkloadNewSchedule& sched,
    const size_t workloadIdx) {
  std::vector<PrefixId> placement;
  for (const auto& [rank, id] :
       *sched.workloads()->at(workloadIdx).rankToId()) {
    placement.emplace_back(id);
  }
  return placement;
}

// True when the placement reads as consecutive (even, even+1) node pairs, which
// is what rack-granular placement always produces.
bool preservesRackPairs(const std::vector<PrefixId>& placement) {
  for (size_t r = 0; r + 1 < placement.size(); r += 2) {
    const uint32_t first = placement.at(r).at(1);
    if (first % 2 != 0 || placement.at(r + 1) != flexId(first + 1)) {
      return false;
    }
  }
  return true;
}

} // namespace

class AssignNewWorkloadSchedule : public ::testing::Test {
 protected:
  Random random_{42};
  const thrift::FlexTopology topology8_{makeTopology(8)};
};

TEST_F(AssignNewWorkloadSchedule, OverlapRacksIsRejected) {
  // Arrange
  auto sched =
      makeSchedule(SchedulingUnit::RACK, SchedulingMethod::OVERLAP_RACKS, {4});

  // Act + Assert
  EXPECT_THAT(
      [&] { assignNewWorkloadSchedule(topology8_, sched, random_); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("OVERLAP_RACKS no longer supported")));
}

TEST_F(AssignNewWorkloadSchedule, SupernodePlacesRanksInFlexIdOrder) {
  // Arrange
  const auto topology = makeTopology(4);
  auto sched = makeSchedule(
      SchedulingUnit::SUPERNODE, SchedulingMethod::SEQUENTIAL, {4});

  // Act
  assignNewWorkloadSchedule(topology, sched, random_);

  // Assert: the SWITCH node -- whose FlexId sorts first -- is excluded, and
  // placement follows FlexId, not the reversed map-key order.
  EXPECT_EQ(
      placementOf(sched, 0),
      (std::vector<PrefixId>{flexId(0), flexId(1), flexId(2), flexId(3)}));
}

TEST_F(AssignNewWorkloadSchedule, SupernodeConsumesNodesAcrossWorkloads) {
  // Arrange
  const auto topology = makeTopology(4);
  auto sched = makeSchedule(
      SchedulingUnit::SUPERNODE, SchedulingMethod::SEQUENTIAL, {2, 2});

  // Act
  assignNewWorkloadSchedule(topology, sched, random_);

  // Assert
  EXPECT_EQ(
      placementOf(sched, 0), (std::vector<PrefixId>{flexId(0), flexId(1)}));
  EXPECT_EQ(
      placementOf(sched, 1), (std::vector<PrefixId>{flexId(2), flexId(3)}));
}

TEST_F(AssignNewWorkloadSchedule, EndpointUnitConsumesRacksAcrossWorkloads) {
  // Arrange
  auto sched = makeSchedule(
      SchedulingUnit::ENDPOINT, SchedulingMethod::SEQUENTIAL, {4, 4});

  // Act
  assignNewWorkloadSchedule(topology8_, sched, random_);

  // Assert: the rack cursor advances by numEndpoints / 2 per workload, so the
  // second workload starts at rack 2 rather than at rack 4.
  EXPECT_EQ(
      placementOf(sched, 0),
      (std::vector<PrefixId>{flexId(0), flexId(1), flexId(2), flexId(3)}));
  EXPECT_EQ(
      placementOf(sched, 1),
      (std::vector<PrefixId>{flexId(4), flexId(5), flexId(6), flexId(7)}));
}

TEST_F(AssignNewWorkloadSchedule, AssignsContiguousWorkloadIds) {
  // Arrange
  auto sched = makeSchedule(
      SchedulingUnit::SUPERNODE, SchedulingMethod::SEQUENTIAL, {2, 2, 2});

  // Act
  assignNewWorkloadSchedule(topology8_, sched, random_);

  // Assert
  EXPECT_EQ(*sched.workloads()->at(0).workloadId(), 0);
  EXPECT_EQ(*sched.workloads()->at(1).workloadId(), 1);
  EXPECT_EQ(*sched.workloads()->at(2).workloadId(), 2);
}

TEST_F(AssignNewWorkloadSchedule, RejectsCumulativeOversubscription) {
  // Arrange: neither workload alone exceeds the four accelerators, but their
  // sum does.
  const auto topology = makeTopology(4);
  auto sched = makeSchedule(
      SchedulingUnit::SUPERNODE, SchedulingMethod::SEQUENTIAL, {4, 4});

  // Act + Assert: the reported node count is four, not five, because the SWITCH
  // node is filtered out of the candidate list.
  EXPECT_THAT(
      [&] { assignNewWorkloadSchedule(topology, sched, random_); },
      ThrowsMessage<std::runtime_error>(AllOf(
          HasSubstr("requires 8 hosts"),
          HasSubstr("numAccelerators is only 4"))));
}

TEST_F(AssignNewWorkloadSchedule, EndpointUnitPairsTwoNodesPerRack) {
  // Arrange
  auto sched =
      makeSchedule(SchedulingUnit::ENDPOINT, SchedulingMethod::SEQUENTIAL, {4});

  // Act
  assignNewWorkloadSchedule(topology8_, sched, random_);

  // Assert: four ranks consume the first two racks, each rack contributing an
  // adjacent node pair.
  EXPECT_EQ(
      placementOf(sched, 0),
      (std::vector<PrefixId>{flexId(0), flexId(1), flexId(2), flexId(3)}));
}

TEST_F(AssignNewWorkloadSchedule, EndpointUnitRejectsAnOddRankCount) {
  // Arrange
  auto sched =
      makeSchedule(SchedulingUnit::ENDPOINT, SchedulingMethod::SEQUENTIAL, {3});

  // Act and assert: ranks are placed as rack pairs, so rank 2 would be left
  // without a node.
  EXPECT_THAT(
      [&]() { assignNewWorkloadSchedule(topology8_, sched, random_); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "numEndpoints must be even unless scheduling by SUPERNODE; got 3")));
}

TEST_F(AssignNewWorkloadSchedule, RandomEndpointUnitKeepsRackPairsAdjacent) {
  // Arrange
  auto sched =
      makeSchedule(SchedulingUnit::ENDPOINT, SchedulingMethod::RANDOM, {8});

  // Act
  assignNewWorkloadSchedule(topology8_, sched, random_);

  // Assert: only the rack list is shuffled, so every rank pair is still an
  // even/odd adjacent node pair and every node is used exactly once.
  const auto placement = placementOf(sched, 0);
  ASSERT_EQ(placement.size(), 8);
  std::vector<uint32_t> usedNodes;
  for (size_t r = 0; r < placement.size(); r += 2) {
    SCOPED_TRACE(fmt::format("rank pair starting at {}", r));
    const uint32_t first = placement.at(r).at(1);
    EXPECT_EQ(first % 2, 0u);
    EXPECT_EQ(placement.at(r + 1), flexId(first + 1));
    usedNodes.emplace_back(first);
    usedNodes.emplace_back(first + 1);
  }
  std::sort(usedNodes.begin(), usedNodes.end());
  EXPECT_EQ(usedNodes, (std::vector<uint32_t>{0, 1, 2, 3, 4, 5, 6, 7}));
}

TEST_F(AssignNewWorkloadSchedule, RandomSupernodePermutesIndividualNodes) {
  // Arrange
  auto sched =
      makeSchedule(SchedulingUnit::SUPERNODE, SchedulingMethod::RANDOM, {8});

  // Act
  assignNewWorkloadSchedule(topology8_, sched, random_);

  // Assert: every node is placed exactly once, and the placement breaks the
  // rack pairing -- the node list itself is shuffled, not the rack list.
  const auto placement = placementOf(sched, 0);
  auto sorted = placement;
  std::sort(sorted.begin(), sorted.end());
  EXPECT_EQ(
      sorted,
      (std::vector<PrefixId>{
          flexId(0),
          flexId(1),
          flexId(2),
          flexId(3),
          flexId(4),
          flexId(5),
          flexId(6),
          flexId(7)}));
  EXPECT_FALSE(preservesRackPairs(placement));
}

} // namespace packsim
