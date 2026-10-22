/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/device-id.h"
#include "packsim/transport/roce-base.h"

namespace packsim {
namespace {

// The test topology: two racks of 36 accelerators, 18 NIC ports each.
constexpr size_t kNumPorts = 18;
constexpr uint16_t kRacks = 2;
constexpr uint16_t kAccelsPerRack = 36;

// Device IDs as the topology builds them: {1, 0, rack, accel, nic, 0, 0, 0}.
std::vector<FlexDeviceId> allDeviceIds(const uint16_t numNics = 1) {
  std::vector<FlexDeviceId> ids;
  ids.reserve(size_t{kRacks} * kAccelsPerRack * numNics);
  for (uint16_t rack = 1; rack <= kRacks; ++rack) {
    for (uint16_t accel = 1; accel <= kAccelsPerRack; ++accel) {
      for (uint16_t nic = 1; nic <= numNics; ++nic) {
        ids.emplace_back(
            std::vector<uint32_t>{1, 0, rack, accel, nic, 0, 0, 0});
      }
    }
  }
  return ids;
}

// Flows landing on each slot for one rank, counting every peer it talks to.
std::vector<size_t> flowsPerSlotForDevice(
    const std::vector<FlexDeviceId>& ids,
    const size_t self,
    const size_t numPorts) {
  std::vector<size_t> flows(numPorts, 0);
  for (size_t peer = 0; peer < ids.size(); ++peer) {
    if (peer == self) {
      continue;
    }
    ++flows.at(
        RoceTransportBase::egressPortSlot(ids[self], ids[peer], numPorts));
  }
  return flows;
}

} // namespace

// Guards the regression that left half the fabric idle. The slot was derived
// from queue-pair numbers, which are allocated two per pair and scaled by the
// NIC count, so the index inherited that stride and any stride sharing a factor
// with the port count made the remaining ports unreachable. A 2-rack AllToAll
// ran 9 of every 18 links at ~80% and the other 9 at zero.
TEST(RoceEgressPortSlotTest, EveryRankReachesEveryPort) {
  const std::vector<FlexDeviceId> ids = allDeviceIds();

  for (size_t self = 0; self < ids.size(); ++self) {
    const std::vector<size_t> flows =
        flowsPerSlotForDevice(ids, self, kNumPorts);
    for (size_t slot = 0; slot < kNumPorts; ++slot) {
      EXPECT_GT(flows.at(slot), 0u)
          << "rank " << self << " never used port slot " << slot;
    }
  }
}

// A collective finishes when its slowest rank drains, so what matters is the
// busiest port on any one rank, not the aggregate spread. Full coverage alone
// can still leave one port carrying several times the ideal load.
TEST(RoceEgressPortSlotTest, NoRankOverloadsOnePort) {
  const std::vector<FlexDeviceId> ids = allDeviceIds();

  for (size_t self = 0; self < ids.size(); ++self) {
    const std::vector<size_t> flows =
        flowsPerSlotForDevice(ids, self, kNumPorts);
    const auto [minFlows, maxFlows] =
        std::minmax_element(flows.begin(), flows.end());
    EXPECT_LE(*maxFlows, *minFlows + 1)
        << "rank " << self << " distributed flows unevenly";
  }
}

// Both directions of a QP pair must select the same slot. The destination
// address is rewritten from dstNicPort and the wiring is planar, so an
// asymmetric choice would steer the reply to a port the sender never used.
TEST(RoceEgressPortSlotTest, SlotIsSymmetricUnderSwap) {
  const std::vector<FlexDeviceId> ids = allDeviceIds();

  for (size_t a = 0; a < ids.size(); ++a) {
    for (size_t b = 0; b < ids.size(); ++b) {
      ASSERT_EQ(
          RoceTransportBase::egressPortSlot(ids[a], ids[b], kNumPorts),
          RoceTransportBase::egressPortSlot(ids[b], ids[a], kNumPorts))
          << "asymmetric for ranks " << a << " and " << b;
    }
  }
}

// Port counts vary by topology, and coverage must not depend on the count
// sharing a factor with anything.
TEST(RoceEgressPortSlotTest, SupportedLayoutsReachEveryPort) {
  const std::vector<std::pair<uint16_t, size_t>> layouts{
      {1, 1}, {1, 8}, {1, 18}, {2, 16}, {3, 18}, {6, 16}, {6, 18}};

  for (const auto [numNics, numPorts] : layouts) {
    const std::vector<FlexDeviceId> ids = allDeviceIds(numNics);
    std::set<size_t> used;
    for (size_t self = 0; self < ids.size(); ++self) {
      for (size_t peer = 0; peer < ids.size(); ++peer) {
        if (peer == self) {
          continue;
        }
        used.emplace(
            RoceTransportBase::egressPortSlot(ids[self], ids[peer], numPorts));
      }
    }
    EXPECT_EQ(used.size(), numPorts) << "incomplete coverage with " << numNics
                                     << " NICs, " << numPorts << " ports";
  }
}

TEST(RoceEgressPortSlotDeathTest, ZeroPortsFailsFast) {
  const FlexDeviceId srcId{{1, 0, 0, 1}};
  const FlexDeviceId dstId{{1, 0, 0, 2}};

  EXPECT_DEATH(
      RoceTransportBase::egressPortSlot(srcId, dstId, 0),
      "numPorts must be positive");
}

} // namespace packsim
