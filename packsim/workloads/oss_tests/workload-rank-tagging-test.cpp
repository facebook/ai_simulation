/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

/**
 * Test that WorkloadRank properly sets workload pointer in messages.
 * This tests the actual message creation path from WorkloadRank::send()
 * and WorkloadRank::recv().
 */
class WorkloadRankTaggingTest : public WorkloadRankTestHarness,
                                public ::testing::Test {
 protected:
  void sendMessage(Message* msg) override {
    // Verify that WorkloadRank sets the workload pointer
    EXPECT_NE(msg->workload, nullptr)
        << "WorkloadRank should set workload pointer in messages";
    EXPECT_EQ(msg->workload, &workCfg_)
        << "Message workload should point to workCfg_";
    EXPECT_EQ(msg->workloadId, *workCfg_.workloadId())
        << "Message workloadId should match workload config";

    // Call parent implementation to continue test
    WorkloadRankTestHarness::sendMessage(msg);
  }

  void recvMessage(Message* msg) override {
    // Verify that WorkloadRank also sets workload pointer in recv messages
    EXPECT_NE(msg->workload, nullptr)
        << "WorkloadRank should set workload pointer in recv messages";
    EXPECT_EQ(msg->workload, &workCfg_);
    EXPECT_EQ(msg->workloadId, *workCfg_.workloadId());

    // Call parent implementation
    WorkloadRankTestHarness::recvMessage(msg);
  }
};

TEST_F(WorkloadRankTaggingTest, AllToAllSetsWorkloadInMessages) {
  // Test regular AllToAll workload (without parallel QPs)
  workCfg_.name() = "A2A_TAGGING_TEST";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 4;

  auto a2a = packsim::workload::thrift::AllToAll();
  a2a.sizeBytes() = 1024;
  a2a.numChunks() = 1;
  workCfg_.config()->allToAll() = a2a;

  // Run the workload - sendMessage() and recvMessage() will verify
  // that msg->workload is set correctly
  run();
  validateStats();
}

TEST_F(WorkloadRankTaggingTest, AllToAllWithParallelQPsSetsWorkloadInMessages) {
  // Test AllToAll with parallel QPs (uses Channel path)
  workCfg_.name() = "A2A_PARALLEL_QP_TAGGING_TEST";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 4;

  auto a2a = packsim::workload::thrift::AllToAll();
  a2a.sizeBytes() = 2048;
  a2a.numChunks() = 2;

  // Enable parallel QPs - this uses Channel/ChannelManager path
  auto channelSettings = packsim::workload::thrift::ChannelSettings();
  channelSettings.numChannels() = 2;
  channelSettings.numQPsPerChannel() = 2;
  workCfg_.channelSettings() = channelSettings;

  workCfg_.config()->allToAll() = a2a;

  // Run the workload - this will use Channel::buildMessage()
  // Our overridden sendMessage() will verify msg->workload is set
  run();
  validateStats();
}

// A later DAG iteration rebuilds its ranks, and each constructor posts its
// notify receives before WorkloadNode can call setIteration.
TEST_F(
    WorkloadRankTaggingTest,
    SetIterationTagsNotifyRecvsPostedAtConstruction) {
  workCfg_.name() = "A2A_ITERATION_TAGGING_TEST";
  workCfg_.workloadId() = 0;
  workCfg_.numEndpoints() = 4;
  auto a2a = packsim::workload::thrift::AllToAll();
  a2a.sizeBytes() = 1024;
  a2a.numChunks() = 1;
  workCfg_.config()->allToAll() = a2a;
  // Fills in the rank and QPN maps the ranks need, and runs one iteration.
  run();

  buildRanks();
  for (const auto& rank : ranks_) {
    ASSERT_FALSE(rank->recvNotifyMessages_.empty());
    rank->setIteration(2);
    for (const auto& [peer, msgs] : rank->recvNotifyMessages_) {
      ASSERT_FALSE(msgs.empty());
      for (const auto& msg : msgs) {
        EXPECT_EQ(msg->iteration, 2u)
            << "rank " << rank->rank_ << ", notify recv from rank " << peer;
      }
    }
  }
}

} // namespace packsim
