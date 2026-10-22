/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <tuple>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/topology/flex-topology.h"
#include "packsim/util/builders.h"

namespace packsim {
namespace {
constexpr uint32_t kPacketSizeBytes = 4096;
} // namespace

class RdmaQpDcqcnTest : public ::testing::TestWithParam<
                            std::tuple<uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    nHosts_ = std::get<0>(GetParam());
    srcIdx_ = std::get<1>(GetParam());
    dstIdx_ = std::get<2>(GetParam());

    simCfg_ = packsim::buildFourZoneClosConfig(nHosts_);
    flexTopo_ = std::make_unique<packsim::FlexTopology>(
        &scheduler_, &simCtx_.random, simCfg_);

    auto* src = getAccel(srcIdx_);
    auto* dst = getAccel(dstIdx_);
    if (src == nullptr || dst == nullptr) {
      FAIL() << "No workload node at rank " << srcIdx_ << " or " << dstIdx_;
    }
    src_ = src;
    dst_ = dst;
    dst->setOnMessageReceivedCallback(
        [this](packsim::Message* msg) { received_.emplace_back(msg); });
    src->setOnMessageAckedCallback(
        [this](packsim::Message* msg) { acked_.emplace_back(msg); });

    auto srcId = src->getFlexId();
    auto dstId = dst->getFlexId();

    msg_ = std::make_unique<packsim::Message>();
    msg_->sizeBytes = kPacketSizeBytes;
    msg_->connectionMessageId = 0;
    msg_->srcRank = srcIdx_;
    msg_->dstRank = dstIdx_;
    msg_->srcIdx = srcId;
    msg_->dstIdx = dstId;

    simCtx_.scheduleNow(&WorkloadNode::makeSendRequest, src_, msg_.get());
    simCtx_.scheduleNow(&WorkloadNode::makeReceiveRequest, dst_, msg_.get());

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

  // Declared before flexTopo_ so the nodes, which hold the message and write
  // these vectors, are destroyed first.
  std::unique_ptr<packsim::Message> msg_;
  std::vector<packsim::Message*> received_;
  std::vector<packsim::Message*> acked_;

  std::unique_ptr<packsim::FlexTopology> flexTopo_;
  WorkloadNode* src_{};
  WorkloadNode* dst_{};

  uint32_t srcIdx_{};
  uint32_t dstIdx_{};
};

TEST_P(RdmaQpDcqcnTest, BasicTest) {
  EXPECT_THAT(received_, ::testing::ElementsAre(msg_.get()));
  EXPECT_THAT(acked_, ::testing::ElementsAre(msg_.get()));
}

INSTANTIATE_TEST_SUITE_P(
    BasicTest,
    RdmaQpDcqcnTest,
    ::testing::Combine(
        ::testing::Values(128U),
        ::testing::Values(0U),
        ::testing::Range(1U, 2U)));
} // namespace packsim
