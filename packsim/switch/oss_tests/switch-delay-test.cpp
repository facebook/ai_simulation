/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>

#include <gtest/gtest.h>

#include "packsim/topology/flex-topology.h"
#include "packsim/util/builders.h"

namespace packsim {
namespace {

// Workload node / rank index of the source endpoint.
constexpr uint32_t kSrcRank = 0;
constexpr uint32_t kMsgSizeBytes = 1000;

uint64_t getCompletionTime(
    packsim::thrift::Simulation& simCfg,
    const uint32_t dstRank) {
  Scheduler scheduler;
  SimContext simCtx{&scheduler, 0};

  auto flexTopo = std::make_unique<packsim::FlexTopology>(
      &scheduler, &simCtx.random, simCfg);

  auto src = flexTopo->getWorkloadNode(kSrcRank);
  auto dst = flexTopo->getWorkloadNode(dstRank);

  auto srcId = src->getFlexId();
  auto dstId = dst->getFlexId();

  auto msg = std::make_unique<packsim::Message>();
  msg->sizeBytes = kMsgSizeBytes;
  msg->connectionMessageId = 0;
  msg->srcRank = kSrcRank;
  msg->dstRank = dstRank;
  msg->srcIdx = srcId;
  msg->dstIdx = dstId;

  simCtx.scheduleNow(&WorkloadNode::makeSendRequest, src, msg.get());
  simCtx.scheduleNow(&WorkloadNode::makeReceiveRequest, dst, msg.get());

  scheduler.run();
  uint64_t runtimeNs = simCtx.nowNs();
  return runtimeNs;
}

TEST(SwitchDelayTests, SwitchDelay) {
  constexpr uint32_t numAccel = 128;
  constexpr uint32_t fastDelay = 100;
  constexpr uint32_t slowDelay = fastDelay * 100;
  auto fastCfg = packsim::buildFourZoneClosConfig(numAccel);
  auto slowCfg = packsim::buildFourZoneClosConfig(numAccel);
  fastCfg.switchConfigMap()->at("RTSW").pipelineDelayNs() = fastDelay;
  slowCfg.switchConfigMap()->at("RTSW").pipelineDelayNs() = slowDelay;
  // The first accelerator of the rack's second host: same rack switch as the
  // source, so the message and its acknowledgement each cross it exactly once.
  const uint32_t dstRank = buildFourZoneClosCliConfig(numAccel).numAccelPerHost;

  auto fastRuntime = getCompletionTime(fastCfg, dstRank);
  auto slowRuntime = getCompletionTime(slowCfg, dstRank);
  uint64_t const delta = slowRuntime - fastRuntime;
  uint64_t const expDelta = 2 * (slowDelay - fastDelay);

  EXPECT_EQ(delta, expDelta);
}

} // namespace
} // namespace packsim
