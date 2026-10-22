/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stdexcept>

#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include "packsim/topology/flex-topology.h"
#include "packsim/util/builders.h"

namespace packsim {

class NicStatsTest : public ::testing::TestWithParam<
                         std::tuple<uint32_t, uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    scheduler_.buildNewEngine();

    nHosts_ = std::get<0>(GetParam());
    srcIdx_ = std::get<1>(GetParam());
    dstIdx_ = std::get<2>(GetParam());
    numMsgs_ = std::get<3>(GetParam());

    simCfg_ = packsim::buildFourZoneClosConfig(nHosts_);
    flexTopo_ = std::make_unique<packsim::FlexTopology>(
        &scheduler_, &simCtx_.random, simCfg_);

    src_ = getAccel(srcIdx_);
    dst_ = getAccel(dstIdx_);

    srcId_ = src_->getFlexId();
    dstId_ = dst_->getFlexId();

    std::vector<std::unique_ptr<packsim::Message>> msgs;
    for (uint32_t i = 0; i < numMsgs_; i++) {
      auto msg = std::make_unique<packsim::Message>();
      msg->sizeBytes = kPktSize;
      msg->connectionMessageId = 0;
      msg->srcRank = srcIdx_;
      msg->dstRank = dstIdx_;
      msg->srcIdx = srcId_;
      msg->dstIdx = dstId_;
      msg->localQPN = 0;
      msg->remoteQPN = 0;

      simCtx_.scheduleNow(&WorkloadNode::makeSendRequest, src_, msg.get());
      simCtx_.scheduleNow(&WorkloadNode::makeReceiveRequest, dst_, msg.get());
      msgs.emplace_back(std::move(msg));
    }

    scheduler_.run();
  }

  void TearDown() override {
    flexTopo_ = nullptr;
  }

  WorkloadNode* getAccel(uint32_t idx) {
    return flexTopo_->getWorkloadNode(idx);
  }

 protected:
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  packsim::thrift::Simulation simCfg_;
  uint32_t nHosts_{};

  std::unique_ptr<packsim::FlexTopology> flexTopo_;
  WorkloadNode* src_{};
  WorkloadNode* dst_{};

  uint32_t srcIdx_{};
  uint32_t dstIdx_{};
  FlexDeviceId srcId_;
  FlexDeviceId dstId_;
  static constexpr uint32_t kPktSize{4096};
  uint32_t numMsgs_{1};
};

TEST_P(NicStatsTest, NicStatsCheck) {
  auto findEntry = [](const auto& entries, const std::vector<uint32_t>& id)
      -> const packsim::thrift::IntfPktStatsEntry& {
    for (const auto& e : entries) {
      if (*e.destId() == id) {
        return e;
      }
    }
    throw std::runtime_error(
        fmt::format("Entry not found for id [{}]", fmt::join(id, ", ")));
  };
  auto srcNicStats = src_->getEndpoint(0)->getNicStatsSummaryResults();
  auto& srcNicCosStats = *srcNicStats.nicStats()->perCosStats();
  auto& srcPerDest = *srcNicStats.nicStats()->perDestStats();
  auto dstNicStats = dst_->getEndpoint(0)->getNicStatsSummaryResults();
  auto& dstNicCosStats = *dstNicStats.nicStats()->perCosStats();
  auto& dstPerDest = *dstNicStats.nicStats()->perDestStats();
  EXPECT_EQ(*srcNicCosStats.at(0).sentAckPkts(), 0);
  EXPECT_EQ(*srcNicCosStats.at(0).sentDataPkts(), numMsgs_);
  EXPECT_EQ(*dstNicCosStats.at(0).sentAckPkts(), numMsgs_);
  EXPECT_EQ(*dstNicCosStats.at(0).sentDataPkts(), 0);
  EXPECT_EQ(*dstNicCosStats.at(0).sentDataBytes(), 0);
  EXPECT_EQ(
      *findEntry(dstPerDest, srcId_.asVec()).intfPktStats()->recvdDataPkts(),
      numMsgs_);
  EXPECT_EQ(
      *findEntry(srcPerDest, dstId_.asVec()).intfPktStats()->sentDataPkts(),
      numMsgs_);
  EXPECT_EQ(
      *findEntry(srcPerDest, dstId_.asVec()).intfPktStats()->recvdAckPkts(),
      numMsgs_);
  EXPECT_EQ(
      *findEntry(dstPerDest, srcId_.asVec()).intfPktStats()->sentAckPkts(),
      numMsgs_);
}

INSTANTIATE_TEST_SUITE_P(
    NicStatsCheck,
    NicStatsTest,
    ::testing::Combine(
        ::testing::Values(128U),
        ::testing::Values(0U),
        ::testing::Range(16U, 32U),
        ::testing::Range(1U, 2U)));
} // namespace packsim
