/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <folly/Conv.h>
#include <folly/lang/Hint.h>
#include <folly/logging/LogStreamProcessor.h>
#include <folly/logging/xlog.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/endpoint/runtime/workload-node.h"
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/nic_config_types.h"
#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/port-stats.h"
#include "packsim/node/flex-node.h"
#include "packsim/switch/fb-switch.h"
#include "packsim/topology/flex-topology.h"
#include "packsim/topology/oss_tests/clos-coordinates.h"
#include "packsim/transport/transport.h"
#include "packsim/util/builders.h"
#include "packsim/util/config-builders.h"

namespace packsim {

// Number of QoS (class-of-service) queues per port/link.
inline constexpr uint8_t kNumQosQueues = 8;

class DprRoutingQosTest : public ::testing::TestWithParam<std::tuple<
                              uint32_t /* num of accel */,
                              uint32_t /* src id */,
                              uint32_t /* dst id */,
                              uint8_t /* data class of service */,
                              uint8_t /* ack class of service */>> {
 public:
  void SetUp() override {
    numAccel_ = std::get<0>(GetParam());
    srcIdx_ = std::get<1>(GetParam());
    dstIdx_ = std::get<2>(GetParam());
    dataCos_ = std::get<3>(GetParam());
    ackCos_ = std::get<4>(GetParam());

    simCfg_ = packsim::buildFourZoneClosConfig(numAccel_, kNumQosQueues);
    cliCfg_ = packsim::buildFourZoneClosCliConfig(numAccel_, kNumQosQueues);

    simCfg_.nicConfigMap()["default"].ackCos() = ackCos_;
    simCfg_.nicConfigMap()["default"].dataCos() = dataCos_;

    flexTopo_ = std::make_unique<packsim::FlexTopology>(
        &scheduler_, &simCtx_.random, simCfg_);
    // The rail arithmetic below indexes one RTSW uplink per CTSW.
    ASSERT_EQ(cliCfg_.numRtswCtswLinks, 1u);
    ctswOverSubscriptionRatio_ = std::max(
        1u,
        cliCfg_.nominalRacksPerZone /
            (cliCfg_.numCtswAtswLinks *
             (cliCfg_.numAtsws / kFourZoneClosPlanes)));
    ctsws_ = getClusterSwitches();
    atsws_ = getAggregationSwitches();

    src_ = getAccel(srcIdx_);
    dst_ = getAccel(dstIdx_);

    srcId_ = src_->getFlexId();
    dstId_ = dst_->getFlexId();

    msg_ = std::make_unique<packsim::Message>();
    msg_->sizeBytes = pktSize_;
    msg_->connectionMessageId = 0;
    msg_->srcRank = srcIdx_;
    msg_->dstRank = dstIdx_;
    msg_->srcIdx = srcId_;
    msg_->dstIdx = dstId_;
    msg_->localQPN = 0;
    msg_->remoteQPN = 0;

    simCtx_.scheduleNow(&WorkloadNode::makeSendRequest, src_, msg_.get());
    simCtx_.scheduleNow(&WorkloadNode::makeReceiveRequest, dst_, msg_.get());

    scheduler_.run();

    auto srcPair = getAccelAndRtswPort(src_->getName());
    auto dstPair = getAccelAndRtswPort(dst_->getName());

    srcRtswPort_ = srcPair.second;
    dstRtswPort_ = dstPair.second;

    srcRtsw_ = dynamic_cast<FbSwitch*>(srcRtswPort_->getNode());
    dstRtsw_ = dynamic_cast<FbSwitch*>(dstRtswPort_->getNode());
  }

  std::pair<EthernetPort*, EthernetPort*> getAccelAndRtswPort(
      const std::string& accel) {
    for (const auto& pair : *simCfg_.flex_topology()->links()) {
      if (*pair.second.node1() == accel) {
        return flexTopo_->getLinkPorts(pair.first);
      }
    }
    return std::make_pair(nullptr, nullptr);
  }

  void TearDown() override {
    flexTopo_ = nullptr;
  }

  WorkloadNode* getAccel(uint32_t idx) {
    return flexTopo_->getWorkloadNode(idx);
  }

  std::pair<
      std::vector<std::vector<uint64_t>>,
      std::vector<std::vector<
          uint64_t>>> static getTxRxPktCounts(FbSwitch* sw, std::vector<EthernetPort*> ports) {
    auto swSum = sw->getSummary();
    std::vector<std::vector<uint64_t>> txCounts;
    std::vector<std::vector<uint64_t>> rxCounts;
    for (size_t i = 0; i < ports.size(); ++i) {
      auto ethPort = ports.at(i);
      std::vector<uint64_t> txPortQueueStats;
      std::vector<uint64_t> rxPortQueueStats;
      for (uint8_t q = 0; q < kNumQosQueues; ++q) {
        auto queueStat = ethPort->getPortStats(q);
        txPortQueueStats.emplace_back(queueStat.txPkts);
        rxPortQueueStats.emplace_back(queueStat.rxPkts);
      }
      txCounts.emplace_back(std::move(txPortQueueStats));
      rxCounts.emplace_back(std::move(rxPortQueueStats));
    }
    return std::make_pair(txCounts, rxCounts);
  }

  static std::pair<
      std::vector<std::vector<uint64_t>>,
      std::vector<std::vector<uint64_t>>>
  GetUplinkCounts(FbSwitch* sw) {
    std::vector<EthernetPort*> uplinks;
    auto& ethPorts = sw->getAllPorts();
    auto level = sw->getFlexDeviceId().values.at(0);
    for (size_t i = 0; i < ethPorts.size(); ++i) {
      auto ethPort = ethPorts.at(i).get();
      auto partnerId = ethPort->getPeer()->getFlexDeviceId();
      auto partnerLevel = partnerId.values.at(0);
      if (partnerLevel > level) {
        uplinks.emplace_back(ethPort);
      }
    }

    return getTxRxPktCounts(sw, uplinks);
  }

  static std::pair<
      std::vector<std::vector<uint64_t>>,
      std::vector<std::vector<uint64_t>>>
  GetDownlinkCounts(FbSwitch* sw) {
    std::vector<EthernetPort*> downlinks;
    auto& ethPorts = sw->getAllPorts();
    auto level = sw->getFlexDeviceId().values.at(0);
    for (size_t i = 0; i < ethPorts.size(); ++i) {
      auto ethPort = ethPorts.at(i).get();
      auto partnerId = ethPort->getPeer()->getFlexDeviceId();
      auto partnerLevel = partnerId.values.at(0);
      if (partnerLevel < level) {
        downlinks.emplace_back(ethPort);
      }
    }
    return getTxRxPktCounts(sw, downlinks);
  }

  uint32_t GetAtswDprRail(FlexDeviceId dstDevId) {
    const uint32_t numCtsws = cliCfg_.numCtsws;
    const uint32_t numCtswAtswLinks = cliCfg_.numCtswAtswLinks;
    const uint32_t ctswsPerPlane = numCtsws / kFourZoneClosPlanes;
    const uint32_t atswLinksPerZone =
        numCtsws * numCtswAtswLinks / kFourZoneClosPlanes;

    auto dstInfo = packsim::ClosCoordinates::fromFlexId(dstDevId);

    auto accelPerHost = cliCfg_.numAccelPerHost;
    const uint32_t rail =
        (accelPerHost * dstInfo.host + dstInfo.accel) % cliCfg_.numCtsws;
    // This is the CTSW # inside the plane that the source rack selected
    auto ctswId = rail % ctswsPerPlane;
    const uint32_t plane = rail / ctswsPerPlane;

    // This is the link # from the ctsw that was selected
    const uint32_t ctswUplinkIndex =
        (dstInfo.rack / ctswOverSubscriptionRatio_) % numCtswAtswLinks;
    XLOGF(INFO, "CTSW-ATSW links: {}", numCtswAtswLinks);
    XLOGF(
        INFO,
        "DZone: {}, Plane: {}, CTSW: {}/{}",
        dstInfo.zone,
        plane,
        ctswId,
        numCtsws);

    auto zoneOffset = dstInfo.zone * atswLinksPerZone;
    auto ctswOffset = ctswId * numCtswAtswLinks;
    XLOGF(
        INFO,
        "Zone Offset: {}, CTSW Offset: {}, Uplink Idx: {}",
        zoneOffset,
        ctswOffset,
        ctswUplinkIndex);
    return zoneOffset + ctswOffset + ctswUplinkIndex;
  }

  uint32_t GetAtswId(FlexDeviceId devId) {
    auto dstInfo = packsim::ClosCoordinates::fromFlexId(devId);

    auto accelPerHost = cliCfg_.numAccelPerHost;
    auto ctswId =
        (accelPerHost * dstInfo.host + dstInfo.accel) % cliCfg_.numCtsws;

    const uint32_t numCtsws = cliCfg_.numCtsws;
    const uint32_t numAtsws = cliCfg_.numAtsws;
    const uint32_t plane = ctswId / (numCtsws / kFourZoneClosPlanes);

    uint32_t atswId =
        dstInfo.rack / (ctswOverSubscriptionRatio_ * cliCfg_.numCtswAtswLinks);
    atswId += plane * (numAtsws / kFourZoneClosPlanes);
    return atswId;
  }

  std::vector<FbSwitch*> getLevelNodes(uint16_t level) {
    std::vector<FbSwitch*> out;
    for (const auto& sw : flexTopo_->getAllSwitches()) {
      auto flexId = sw->getFlexDeviceId();
      if (flexId.values.at(0) == level) {
        out.emplace_back(sw.get());
      }
    }
    return out;
  }

  std::vector<FbSwitch*> getClusterSwitches() {
    return getLevelNodes(3);
  }

  std::vector<FbSwitch*> getAggregationSwitches() {
    return getLevelNodes(4);
  }

 protected:
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  packsim::thrift::Simulation simCfg_;
  packsim::PackSimSimConfig cliCfg_;
  uint32_t numAccel_{};

  std::unique_ptr<packsim::FlexTopology> flexTopo_;
  std::unique_ptr<packsim::Message> msg_;
  WorkloadNode* src_{};
  WorkloadNode* dst_{};

  EthernetPort* srcRtswPort_{};
  EthernetPort* dstRtswPort_{};

  std::vector<FbSwitch*> ctsws_;

  std::vector<FbSwitch*> atsws_;

  FbSwitch* srcRtsw_{};
  FbSwitch* dstRtsw_{};

  uint32_t srcIdx_{};
  uint32_t dstIdx_{};
  FlexDeviceId srcId_;
  FlexDeviceId dstId_;
  uint8_t dataCos_{};
  uint8_t ackCos_{};
  uint32_t pktSize_{4096};
  uint32_t ctswOverSubscriptionRatio_{0};
};

} // namespace packsim
