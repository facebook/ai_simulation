/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/node/flex-node.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/switch/fb-switch.h"

namespace packsim::fbswitch::test {

/*
 * Switch-under-test scaffolding shared by the switch test suites. Kept in a
 * header so no suite forks it.
 */

/*
 * Config and packet builders for the switch-under-test fixtures below.
 *
 * A stateless class rather than free functions because this is a header: a
 * public helper in a header has to be a static method on a class, which the
 * anonymous-namespace originals in fb-switch-test.cpp did not have to be.
 */
class FbSwitchTestHelpers {
 public:
  // Test constants
  static constexpr uint32_t kDefaultBufferSizeMiB = 32;
  static constexpr uint32_t kDefaultCellSizeBytes = 254;
  static constexpr uint32_t kDefaultNumPorts = 2;
  static constexpr uint32_t kDefaultNumPriorityGroups = 8;
  // Buffers per direction in createTwoBufferSwitchConfig.
  static constexpr uint32_t kNumBuffersPerDirection = 2;
  // A shared-cell limit that no test reaches.
  static constexpr uint32_t kUnlimitedCells = INT32_MAX;

  static packsim::thrift::LpmTableSpec createMinimalLpmTable() {
    packsim::thrift::LpmTableSpec lpmTable;
    lpmTable.prefixTable() = {};

    packsim::thrift::LpmTableEntry defaultRoute;
    defaultRoute.links() = {0}; // Default to link 0
    lpmTable.defaultRoute() = std::move(defaultRoute);

    return lpmTable;
  }

  static packsim::thrift::LpmTableSpec createEmptyLpmTable() {
    packsim::thrift::LpmTableSpec lpmTable;
    lpmTable.prefixTable() = {};

    packsim::thrift::LpmTableEntry defaultRoute;
    defaultRoute.links() = {}; // No links - will cause routing failure
    lpmTable.defaultRoute() = std::move(defaultRoute);

    return lpmTable;
  }

  static packsim::thrift::FlexTopology createMinimalTopology() {
    packsim::thrift::FlexTopology topology;
    topology.name() = "minimal";
    return topology;
  }

  static packsim::thrift::SwitchConfig createTestSwitchConfig(
      uint32_t numPorts = kDefaultNumPorts,
      uint32_t bufferSizeMiB = kDefaultBufferSizeMiB) {
    packsim::thrift::SwitchConfig config;

    // Basic switch configuration
    config.useIngress() = true;
    config.useEgress() = false;
    config.pipelineDelayNs() = 100U;
    config.expDelayNs() = 50U;
    config.enableFatalOnDrop() = false;

    // Buffer configuration
    packsim::thrift::SwitchBufferConfig bufferConfig;
    bufferConfig.cellSizeBytes() = kDefaultCellSizeBytes;
    bufferConfig.totalCells() =
        bufferSizeMiB * 1024 * 1024 / kDefaultCellSizeBytes;
    bufferConfig.headroomPoolCells() = 1024U;

    for (uint32_t portIdx = 0; portIdx < numPorts; ++portIdx) {
      packsim::thrift::PortBufferConfig portConfig;
      portConfig.port() = portIdx;
      portConfig.portRsvdCells() = 5U;
      portConfig.portMaxSharedCells() = kUnlimitedCells;
      portConfig.xonHystCells() = 7U;

      for (uint32_t priorityGroup = 0;
           priorityGroup < kDefaultNumPriorityGroups;
           ++priorityGroup) {
        packsim::thrift::PriorityGroupBufferConfig pgConfig;
        pgConfig.priority() = priorityGroup;
        pgConfig.rsvdCells() = 3U;
        pgConfig.canUsePortRsvd() = (priorityGroup == 0);
        pgConfig.dynamicSharing() = true;
        pgConfig.alpha() = 0.5;
        pgConfig.maxSharedCells() = kUnlimitedCells;
        pgConfig.xonHystCells() = 9U;
        pgConfig.maxHdrmCells() = 0U;
        pgConfig.lossless() = false;
        pgConfig.pfc() = false;
        portConfig.pgs()->emplace_back(std::move(pgConfig));
      }

      bufferConfig.ports()[portIdx] = std::move(portConfig);
    }

    config.bufs() = {std::move(bufferConfig)};

    // Port to buffer mapping
    for (uint32_t portIdx = 0; portIdx < numPorts; ++portIdx) {
      config.portToBufMap()->singleMap()[portIdx] = 0;
    }

    // Port groups
    std::vector<uint32_t> portGroup;
    portGroup.reserve(numPorts);
    for (uint32_t portIdx = 0; portIdx < numPorts; ++portIdx) {
      portGroup.emplace_back(portIdx);
    }
    config.portGroups()["test_group"] = std::move(portGroup);

    return config;
  }

  // A switch with two buffers per direction, so bufferIdx has to count rather
  // than being trivially 0: a single-buffer config cannot distinguish "indexed
  // correctly" from "never assigned".
  static packsim::thrift::SwitchConfig createTwoBufferSwitchConfig(
      uint32_t numPorts) {
    packsim::thrift::SwitchConfig config = createTestSwitchConfig(numPorts);
    config.useEgress() = true;

    // bufs()/egressBufs() must be equal length (XCHECK in the FbSwitch ctor).
    // Copied out first rather than growing the vector from an element of
    // itself.
    const packsim::thrift::SwitchBufferConfig ingressBufferConfig =
        config.bufs()->at(0);
    config.bufs()->assign(kNumBuffersPerDirection, ingressBufferConfig);

    packsim::thrift::SwitchBufferConfig egressBufferConfig;
    egressBufferConfig.cellSizeBytes() = kDefaultCellSizeBytes;
    egressBufferConfig.totalCells() = ingressBufferConfig.totalCells().value();
    egressBufferConfig.headroomPoolCells() = 0U;
    for (uint32_t portIdx = 0; portIdx < numPorts; ++portIdx) {
      packsim::thrift::PortBufferConfig portConfig;
      portConfig.port() = portIdx;
      portConfig.isIngrPort() = false;
      portConfig.isEgrPort() = true;
      portConfig.portRsvdCells() = 0U;
      portConfig.portMaxSharedCells() = kUnlimitedCells;
      // Egress ports populate `ucQueues`; `pgs` is ingress-only.
      for (uint32_t q = 0; q < kDefaultNumPriorityGroups; ++q) {
        packsim::thrift::EgressQueueBufferConfig qConfig;
        qConfig.cos() = q;
        qConfig.rsvdCells() = 0U;
        qConfig.dynamicSharing() = true;
        qConfig.alpha() = 0.5;
        qConfig.maxSharedCells() = kUnlimitedCells;
        qConfig.resumeHystCells() = 0U;
        portConfig.ucQueues()->emplace_back(std::move(qConfig));
      }
      egressBufferConfig.ports()[portIdx] = std::move(portConfig);
    }
    config.egressBufs()->assign(kNumBuffersPerDirection, egressBufferConfig);

    // With both directions enabled FbSwitch reads pairMap, not singleMap.
    for (uint32_t inIdx = 0; inIdx < numPorts; ++inIdx) {
      for (uint32_t outIdx = 0; outIdx < numPorts; ++outIdx) {
        config.portToBufMap()->pairMap()[fmt::format("{}.{}", inIdx, outIdx)] =
            0;
      }
    }

    return config;
  }

  static packsim::PacketPtr createTestPacketWithConnection(
      packsim::PacketType packetType,
      packsim::IPConnection const& connection,
      uint16_t portNum = 0) {
    auto packet = packsim::createPacket<packsim::PacketType::UDP>(1000);
    packet->pktType = packetType;

    auto udpPacket = packet.as<packsim::UdpPacket>();
    udpPacket->conn = &connection;
    udpPacket->hopLimit = 64;

    packet->tag.port = portNum;

    return packet;
  }
};

// Mock EthernetPort for testing
class MockEthernetPort : public packsim::EthernetPort {
 public:
  explicit MockEthernetPort(packsim::SimContext* simCtx)
      : packsim::EthernetPort(simCtx, "100Gbps") {}
};

// Test wrapper class to expose protected methods
class TestFbSwitch : public packsim::FbSwitch {
 public:
  using packsim::FbSwitch::checkIngressEgressAdmission;
  using packsim::FbSwitch::egressBufs_;
  using packsim::FbSwitch::FbSwitch;
  using packsim::FbSwitch::getBuff;
  using packsim::FbSwitch::ingressBufs_;
  using packsim::FbSwitch::onPacketDroppedOnReceive;
  using packsim::FbSwitch::packetDropStats_;
  using packsim::FbSwitch::recordPacketDrop;
  using packsim::FbSwitch::routeSwitchPacket;
};

class FbSwitchTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create minimal switch configuration
    switchConfig_ = FbSwitchTestHelpers::createTestSwitchConfig();

    // Create scheduler and simulation context
    scheduler_ = std::make_unique<packsim::Scheduler>();
    simContext_ = std::make_unique<packsim::SimContext>(scheduler_.get(), 0);

    // Setup switch parameters
    switchName_ = "test_switch";
    flexDeviceId_ = packsim::FlexDeviceId({1, 2, 3});
    lpmTable_ = FbSwitchTestHelpers::createMinimalLpmTable();

    // Create switch with minimal configuration
    packsim::FlexNodeParams const params{
        simContext_.get(), switchName_, flexDeviceId_, &lpmTable_, nullptr};

    fbSwitch_ = std::make_unique<packsim::FbSwitch>(params, &switchConfig_);
  }

  std::unique_ptr<packsim::Scheduler> scheduler_;
  std::unique_ptr<packsim::SimContext> simContext_;
  packsim::thrift::SwitchConfig switchConfig_;
  packsim::thrift::LpmTableSpec lpmTable_;
  std::unique_ptr<packsim::FbSwitch> fbSwitch_;
  std::string switchName_;
  packsim::FlexDeviceId flexDeviceId_;
};

} // namespace packsim::fbswitch::test
