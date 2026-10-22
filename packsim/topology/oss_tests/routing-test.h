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
#include <iostream>
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

class DprRoutingTest : public ::testing::TestWithParam<std::tuple<
                           uint32_t /* num of accel */,
                           uint32_t /* src id */,
                           uint32_t /* dst id */>> {
 public:
  void SetUp() override {
    numAccel_ = std::get<0>(GetParam());
    srcIdx_ = std::get<1>(GetParam());
    dstIdx_ = std::get<2>(GetParam());

    const auto buildFabricFn = getBuildFabricFn();
    cliCfg_ = buildFabricFn(numAccel_, 1);
    simCfg_ = packsim::buildFourZoneClosConfig(
        {.numAccel = numAccel_, .buildFabricFn = buildFabricFn});
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

    auto msg = std::make_unique<packsim::Message>();
    msg->sizeBytes = pktSize_;
    msg->connectionMessageId = 0;
    msg->srcRank = srcIdx_;
    msg->dstRank = dstIdx_;
    msg->srcIdx = srcId_;
    msg->dstIdx = dstId_;
    msg->localQPN = 0;
    msg->remoteQPN = 0;

    simCtx_.scheduleNow(&WorkloadNode::makeSendRequest, src_, msg.get());
    simCtx_.scheduleNow(&WorkloadNode::makeReceiveRequest, dst_, msg.get());

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

  std::
      pair<std::vector<uint64_t>, std::vector<uint64_t>> static getTxRxPktCounts(
          FbSwitch* sw,
          std::vector<EthernetPort*> ports) {
    auto swSum = sw->getSummary();
    std::vector<uint64_t> txCounts;
    std::vector<uint64_t> rxCounts;
    for (size_t i = 0; i < ports.size(); ++i) {
      auto ethPort = ports.at(i);
      auto portStats = ethPort->getPortStats(0);
      txCounts.emplace_back(portStats.txPkts);
      rxCounts.emplace_back(portStats.rxPkts);
    }
    return std::make_pair(txCounts, rxCounts);
  }

  static std::pair<std::vector<uint64_t>, std::vector<uint64_t>>
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

  static std::pair<std::vector<uint64_t>, std::vector<uint64_t>>
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

  // Expect one packet on the destination ATSW downlink the DPR rail selects
  // (and on the acknowledgement's rail when it shares the ATSW).
  void expectDstAtswUplinkCounters() {
    auto srcDevId = src_->getFlexId();
    auto dstDevId = dst_->getFlexId();
    auto srcInfo = packsim::ClosCoordinates::fromFlexId(srcDevId);
    auto dstInfo = packsim::ClosCoordinates::fromFlexId(dstDevId);

    if (srcInfo.zone == dstInfo.zone) {
      return;
    }

    auto numZones = numAccel_ / cliCfg_.numAccelPerZone;
    auto numDnlinks = cliCfg_.numCtsws / kFourZoneClosPlanes *
        cliCfg_.numCtswAtswLinks * numZones;

    auto atswId = GetAtswId(dst_->getFlexId());

    auto dprRail = GetAtswDprRail(dst_->getFlexId());
    XLOGF(INFO, "DPR Rail: {}, Num Links: {}", dprRail, numDnlinks);

    std::vector<uint64_t> expDnTxCounts;
    expDnTxCounts.resize(numDnlinks, 0);
    expDnTxCounts.at(dprRail) = 1;

    // for ack
    auto ackAtswId = GetAtswId(src_->getFlexId());
    if (ackAtswId == atswId) {
      dprRail = GetAtswDprRail(src_->getFlexId());
      expDnTxCounts.at(dprRail) = 1;
    }

    ASSERT_EQ(atsws_.size(), cliCfg_.numAtsws);

    auto dnCounts = GetDownlinkCounts(atsws_.at(atswId));
    auto upDnCounts = dnCounts.first;
    for (size_t i = 0; i < upDnCounts.size(); ++i) {
      if (upDnCounts[i] != 0) {
        std::cerr << "Found non zero at " << i << " size " << upDnCounts.size()
                  << std::endl;
      }
    }
    EXPECT_EQ(expDnTxCounts, upDnCounts);
  }

  // Expect the destination CTSW to receive the inter-zone packet on the uplink
  // its DPR rail selects.
  void expectDstCtswUplinkCounters() {
    auto srcDevId = src_->getFlexId();
    auto dstDevId = dst_->getFlexId();

    auto srcInfo = packsim::ClosCoordinates::fromFlexId(srcDevId);
    auto dstInfo = packsim::ClosCoordinates::fromFlexId(dstDevId);

    if (srcInfo.zone == dstInfo.zone) {
      return;
    }

    auto numUplinks =
        cliCfg_.numAtsws / kFourZoneClosPlanes * cliCfg_.numCtswAtswLinks;
    auto accelPerHost = cliCfg_.numAccelPerHost;
    auto ctswId = dstInfo.zone * cliCfg_.numCtsws +
        (dstInfo.host * accelPerHost + dstInfo.accel) % cliCfg_.numCtsws;

    std::vector<uint64_t> expUpRxCounts;
    expUpRxCounts.resize(numUplinks, 0);
    auto dprRail = (dstInfo.rack / ctswOverSubscriptionRatio_) % numUplinks;
    expUpRxCounts.at(dprRail) = 1;
    ASSERT_EQ(ctsws_.size(), cliCfg_.numCtsws * kFourZoneClosZones);

    auto upCounts = GetUplinkCounts(ctsws_.at(ctswId));
    auto upRxCounts = upCounts.second;
    EXPECT_EQ(expUpRxCounts, upRxCounts);
  }

  // Expect the destination RTSW to receive a cross-rack packet on the uplink
  // its rail selects.
  void expectDstRtswUplinkCounters() {
    auto srcDevId = src_->getFlexId();
    auto dstDevId = dst_->getFlexId();
    auto srcInfo = packsim::ClosCoordinates::fromFlexId(srcDevId);
    auto dstInfo = packsim::ClosCoordinates::fromFlexId(dstDevId);

    bool sameRack =
        (srcInfo.zone == dstInfo.zone) && (srcInfo.rack == dstInfo.rack);

    auto numUplinks = cliCfg_.numCtsws;
    auto accelPerHost = cliCfg_.numAccelPerHost;
    auto dprRail = (dstInfo.host * accelPerHost + dstInfo.accel) % numUplinks;

    std::vector<uint64_t> expUpRxCounts;
    expUpRxCounts.resize(numUplinks, 0);
    expUpRxCounts.at(dprRail) = sameRack ? 0 : 1;

    auto upCounts = GetUplinkCounts(dstRtsw_);
    auto upRxCounts = upCounts.second;
    if (srcInfo.zone == dstInfo.zone) {
      XLOGF(INFO, "Src zone: {}, Dst zone: {}", srcInfo.zone, dstInfo.zone);
      EXPECT_EQ(upRxCounts, expUpRxCounts);
    }

    uint64_t totalRxPkts = 0;
    for (const auto& e : upRxCounts) {
      totalRxPkts += e;
    }
    EXPECT_EQ(totalRxPkts, sameRack ? 0U : 1U);
  }

  // Expect the source and destination RTSW ports facing the accelerators to
  // carry the packet.
  void expectPartnerCounters() {
    auto srcIntfStats = srcRtswPort_->getPortStats(0);
    auto dstIntfStats = dstRtswPort_->getPortStats(0);

    EXPECT_EQ(srcIntfStats.rxPkts, 1);
    EXPECT_EQ(dstIntfStats.txPkts, 1);
  }

  // Expect the source-zone CTSW to send the inter-zone packet on the uplink its
  // DPR rail selects.
  void expectSrcCtswUplinkCounters() {
    auto srcDevId = src_->getFlexId();
    auto dstDevId = dst_->getFlexId();
    auto srcInfo = packsim::ClosCoordinates::fromFlexId(srcDevId);
    auto dstInfo = packsim::ClosCoordinates::fromFlexId(dstDevId);

    if (srcInfo.zone == dstInfo.zone) {
      return;
    }

    auto numUplinks =
        cliCfg_.numAtsws / kFourZoneClosPlanes * cliCfg_.numCtswAtswLinks;
    auto accelPerHost = cliCfg_.numAccelPerHost;
    auto ctswId = srcInfo.zone * cliCfg_.numCtsws +
        (dstInfo.host * accelPerHost + dstInfo.accel) % cliCfg_.numCtsws;

    std::vector<uint64_t> expUpTxCounts;
    expUpTxCounts.resize(numUplinks, 0);
    auto dprRail = (dstInfo.rack / ctswOverSubscriptionRatio_) % numUplinks;
    expUpTxCounts.at(dprRail) = 1;
    ASSERT_EQ(ctsws_.size(), cliCfg_.numCtsws * kFourZoneClosZones);

    auto upCounts = GetUplinkCounts(ctsws_.at(ctswId));
    auto upTxCounts = upCounts.first;
    EXPECT_EQ(expUpTxCounts, upTxCounts);
  }

  // Expect the source RTSW to send a cross-rack packet on the uplink its rail
  // selects.
  void expectSrcRtswUplinkCounters() {
    auto numUplinks = cliCfg_.numCtsws;

    auto srcDevId = src_->getFlexId();
    auto dstDevId = dst_->getFlexId();
    auto srcInfo = packsim::ClosCoordinates::fromFlexId(srcDevId);
    auto dstInfo = packsim::ClosCoordinates::fromFlexId(dstDevId);

    auto accelPerHost = cliCfg_.numAccelPerHost;
    auto dprRail = (dstInfo.host * accelPerHost + dstInfo.accel) % numUplinks;

    bool sameRack =
        (srcInfo.zone == dstInfo.zone) && (srcInfo.rack == dstInfo.rack);

    // We expect to have one packet transmitted on the correct uplink and no
    // other packets transmitted.
    std::vector<uint64_t> expUpTxCounts;
    expUpTxCounts.resize(numUplinks, 0);
    expUpTxCounts.at(dprRail) = sameRack ? 0 : 1;

    auto upCounts = GetUplinkCounts(srcRtsw_);
    auto upTxCounts = upCounts.first;
    EXPECT_EQ(upTxCounts, expUpTxCounts);

    uint64_t totalTxPkts = 0;
    for (const auto& e : upTxCounts) {
      totalTxPkts += e;
    }
    EXPECT_EQ(totalTxPkts, sameRack ? 0U : 1U);
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
  // The fabric the test builds. A subclass overrides it to run the same checks
  // on a different four-zone fabric.
  virtual BuildFabricFn getBuildFabricFn() const {
    return &buildFourZoneClosCliConfig;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  packsim::thrift::Simulation simCfg_;
  packsim::PackSimSimConfig cliCfg_;
  uint32_t numAccel_{};

  std::unique_ptr<packsim::FlexTopology> flexTopo_;
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
  uint32_t pktSize_{4096};
  uint32_t ctswOverSubscriptionRatio_{0};
};

} // namespace packsim
