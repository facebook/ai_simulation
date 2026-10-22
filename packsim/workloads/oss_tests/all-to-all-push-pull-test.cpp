/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <folly/Utility.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/sim-fix.h"
#include "packsim/workloads/all-to-all-push-pull.h"
#include "packsim/workloads/builder.h"
#include "packsim/workloads/rank/workload-rank.h"

using ::testing::ElementsAre;

namespace packsim {

class AllToAllPushPullTest : public ::testing::Test {
 protected:
  void SetUp() override {
    sentMsgs_.clear();
    recvMsgs_.clear();
    rankCompleted_ = false;
    rankCompletedNs_ = 0;

    // Use a large lookahead so the PDES engine can reach the 1ms PULL
    // drain-delay event in a single frame instead of 1M frames at 1ns.
    scheduler_.setLookaheadTimeFs(packsim::Time::fromMs(10).getFs());

    smcb_ = [this](packsim::Message* msg) {
      sentMsgs_.emplace_back(std::make_unique<packsim::Message>(*msg));
    };
    rmcb_ = [this](packsim::Message* msg) {
      recvMsgs_.emplace_back(std::make_unique<packsim::Message>(*msg));
    };
    rccb_ = [this](uint32_t, uint32_t) {
      rankCompleted_ = true;
      rankCompletedNs_ = simCtx_.nowNs();
    };
  }

  packsim::workload::thrift::Workload buildWorkCfg(
      const std::string& name,
      packsim::workload::thrift::WorkloadMode mode,
      uint32_t numEndpoints,
      uint32_t numChunks) {
    packsim::workload::thrift::Workload workCfg;
    workCfg.name() = name;
    workCfg.workloadId() = 0;
    workCfg.numEndpoints() = numEndpoints;

    std::map<uint32_t, std::vector<uint32_t>> rankToId;
    std::map<uint32_t, std::vector<std::vector<uint32_t>>> rankToIdList;
    for (uint32_t i = 0; i < numEndpoints; ++i) {
      rankToId[i] = {i};
      rankToIdList[i] = {{i}};
    }
    workCfg.rankToId() = rankToId;
    workCfg.rankToIdList() = rankToIdList;

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 1024;
    a2a.numChunks() = numChunks;
    a2a.numGpusPerNvDomain() = 1;
    a2a.workloadMode() = mode;
    workCfg.config()->allToAll() = a2a;

    std::vector<packsim::workload::thrift::Workload> workCfgs({workCfg});
    packsim::buildQPNMap(workCfgs);
    uint32_t nextQpn = 0;
    packsim::buildNicQPNMap(workCfgs, nextQpn);
    return std::move(workCfgs.at(0));
  }

  packsim::AllToAllPushPullRank createRank(
      packsim::workload::thrift::Workload& workCfg) {
    packsim::WorkloadRankParams params{
        .simCtx = &simCtx_,
        .workloadId = 0,
        .workCfg = workCfg,
        .rank = 0,
        .smcb = smcb_,
        .rmcb = rmcb_,
        .rccb = rccb_,
        .parent = nullptr};
    return packsim::AllToAllPushPullRank(params);
  }

  std::vector<packsim::Message*> filterMessages(
      std::vector<std::unique_ptr<packsim::Message>>& msgs,
      bool isWrite,
      bool isRead) {
    std::vector<packsim::Message*> result;
    for (auto& msg : msgs) {
      if ((isWrite && msg->isWrite) || (isRead && msg->isRead)) {
        result.emplace_back(msg.get());
      }
    }
    return result;
  }

  packsim::Scheduler scheduler_;
  packsim::SimContext simCtx_{&scheduler_, 0};
  std::vector<std::unique_ptr<packsim::Message>> sentMsgs_;
  std::vector<std::unique_ptr<packsim::Message>> recvMsgs_;
  bool rankCompleted_{false};
  uint64_t rankCompletedNs_{0};
  std::function<void(packsim::Message*)> smcb_;
  std::function<void(packsim::Message*)> rmcb_;
  std::function<void(uint32_t, uint32_t)> rccb_;
};

TEST_F(AllToAllPushPullTest, PushModeWriteOnly) {
  auto workCfg = buildWorkCfg(
      "Push_WriteOnly", packsim::workload::thrift::WorkloadMode::PUSH, 2, 2);
  auto rank = createRank(workCfg);

  rank.run();
  scheduler_.run();

  auto writeMsgs =
      filterMessages(sentMsgs_, /*isWrite=*/true, /*isRead=*/false);
  ASSERT_EQ(writeMsgs.size(), 2) << "Expected 2 WRITE messages (2 chunks)";

  auto readMsgs = filterMessages(recvMsgs_, /*isWrite=*/false, /*isRead=*/true);
  EXPECT_EQ(readMsgs.size(), 0) << "No READ messages in PUSH mode";

  for (auto* writeMsg : writeMsgs) {
    rank.requestAcked(writeMsg);
  }

  EXPECT_TRUE(rankCompleted_) << "Rank should complete after all WRITEs acked";
  EXPECT_TRUE(rank.complete()) << "complete() should return true";
}

// Each rank writes its peer two 3 GiB chunks. The 6 GiB it expects does not
// fit in 32 bits.
TEST_F(AllToAllPushPullTest, PushExpectedBytesCountsMoreThan32BitsPerPeer) {
  constexpr uint64_t kChunkBytes = uint64_t{3} << 30;
  auto workCfg = buildWorkCfg(
      "Push_Large", packsim::workload::thrift::WorkloadMode::PUSH, 2, 2);
  workCfg.config()->allToAll()->sizeBytes() = 2 * 2 * kChunkBytes;
  auto rank = createRank(workCfg);
  EXPECT_EQ(rank.getExpectedBytes(), 2 * kChunkBytes);
}

TEST_F(AllToAllPushPullTest, PullModeReadOnly) {
  auto workCfg = buildWorkCfg(
      "Pull_ReadOnly", packsim::workload::thrift::WorkloadMode::PULL, 2, 2);
  auto rank = createRank(workCfg);

  rank.run();
  scheduler_.run();

  auto readMsgs = filterMessages(recvMsgs_, /*isWrite=*/false, /*isRead=*/true);
  ASSERT_EQ(readMsgs.size(), 2) << "Expected 2 READ messages (2 chunks)";

  auto writeMsgs =
      filterMessages(sentMsgs_, /*isWrite=*/true, /*isRead=*/false);
  EXPECT_EQ(writeMsgs.size(), 0) << "No WRITE messages in PULL mode";

  for (auto* readMsg : readMsgs) {
    rank.requestAcked(readMsg);
  }

  // PULL mode raises rank-complete after a 1ms drain delay.
  // Run the scheduler to process the deferred event.
  scheduler_.run();

  EXPECT_TRUE(rankCompleted_) << "Rank should complete after all READs acked";
  EXPECT_TRUE(rank.complete()) << "complete() should return true";
}

// The channel path posts plain receives, so a PULL workload with
// channelSettings would never issue a READ; building its rank rejects it.
TEST_F(AllToAllPushPullTest, PullWithChannelSettingsThrows) {
  auto workCfg = buildWorkCfg(
      "Pull_Channels", packsim::workload::thrift::WorkloadMode::PULL, 2, 2);
  workCfg.channelSettings() = packsim::workload::thrift::ChannelSettings{};

  EXPECT_THAT(
      [&] { createRank(workCfg); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("cannot be combined with channelSettings")));
}

// A batch's channelSettings carry its children's receives, so a PULL child of
// such a batch is rejected too.
TEST_F(AllToAllPushPullTest, PullChildOfChannelBatchThrows) {
  auto batchCfg = buildWorkCfg(
      "Pull_Child", packsim::workload::thrift::WorkloadMode::PULL, 2, 2);
  auto child = batchCfg;
  packsim::workload::thrift::Batch batch;
  batch.children()->emplace_back(std::move(child));
  batchCfg.name() = "Channel_Batch";
  batchCfg.config()->batch() = std::move(batch);
  batchCfg.channelSettings() = packsim::workload::thrift::ChannelSettings{};

  EXPECT_THAT(
      [&] {
        packsim::buildWorkloadRank(
            &simCtx_, 0, batchCfg, 0, smcb_, rmcb_, rccb_);
      },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("cannot be combined with channelSettings")));
}

// Only PULL is rejected; a PUSH workload with channelSettings still builds.
TEST_F(AllToAllPushPullTest, PushWithChannelSettingsBuilds) {
  auto workCfg = buildWorkCfg(
      "Push_Channels", packsim::workload::thrift::WorkloadMode::PUSH, 2, 2);
  workCfg.channelSettings() = packsim::workload::thrift::ChannelSettings{};

  EXPECT_NO_THROW(createRank(workCfg));
}

TEST_F(AllToAllPushPullTest, PullModeReadsCompleteOutOfOrder) {
  auto workCfg = buildWorkCfg(
      "Pull_OOO", packsim::workload::thrift::WorkloadMode::PULL, 2, 4);
  auto rank = createRank(workCfg);

  rank.run();
  scheduler_.run();

  auto readMsgs = filterMessages(recvMsgs_, /*isWrite=*/false, /*isRead=*/true);
  ASSERT_EQ(readMsgs.size(), 4) << "Expected 4 READ messages (4 chunks)";

  std::sort(readMsgs.begin(), readMsgs.end(), [](auto* a, auto* b) {
    return a->connectionMessageId > b->connectionMessageId;
  });

  for (auto* readMsg : readMsgs) {
    EXPECT_NO_THROW(rank.requestAcked(readMsg))
        << "requestAcked should handle chunk " << readMsg->connectionMessageId
        << " out of order";
  }

  auto writeMsgs =
      filterMessages(sentMsgs_, /*isWrite=*/true, /*isRead=*/false);
  EXPECT_EQ(writeMsgs.size(), 0) << "PULL mode should not post WRITEs";

  // PULL mode raises rank-complete after a 1ms drain delay.
  // Run the scheduler to process the deferred event.
  scheduler_.run();

  EXPECT_TRUE(rankCompleted_) << "Rank should complete after all READs";
}

// The READs finish 5us in. That is the rank's end time; only the
// rank-complete event waits out the 1ms drain.
TEST_F(AllToAllPushPullTest, PullModeEndTimeIsLastReadCompletion) {
  constexpr uint64_t kReadsDoneNs = 5'000;
  auto workCfg = buildWorkCfg(
      "Pull_EndTime", packsim::workload::thrift::WorkloadMode::PULL, 2, 2);
  auto rank = createRank(workCfg);

  rank.run();
  scheduler_.run();

  auto readMsgs = filterMessages(recvMsgs_, /*isWrite=*/false, /*isRead=*/true);
  ASSERT_EQ(readMsgs.size(), 2) << "Expected 2 READ messages (2 chunks)";

  simCtx_.schedule(packsim::Time::fromNs(kReadsDoneNs), [&rank, &readMsgs]() {
    for (auto* readMsg : readMsgs) {
      rank.requestAcked(readMsg);
    }
  });
  scheduler_.run();

  ASSERT_TRUE(rankCompleted_) << "Rank should complete after the drain delay";
  const auto result = rank.getThriftResult();
  EXPECT_EQ(*result.startTimeNs(), uint64_t{0});
  EXPECT_EQ(*result.endTimeNs(), kReadsDoneNs);
  EXPECT_EQ(rankCompletedNs_, kReadsDoneNs + kPullDrainDelayNs);
}

TEST_F(AllToAllPushPullTest, GetExpectedBytesExcludesNotifyOverhead) {
  // 4 endpoints, 3 peers, messageSize = 1024 / (4 * 1) = 256
  // dataBytes = 256 * 1 = 256 per peer, no notify overhead
  // expectedBytes = 3 * 256 = 768
  for (auto mode :
       {packsim::workload::thrift::WorkloadMode::PUSH,
        packsim::workload::thrift::WorkloadMode::PULL}) {
    auto workCfg = buildWorkCfg("Bytes", mode, 4, 1);
    auto rank = createRank(workCfg);
    EXPECT_EQ(rank.getExpectedBytes(), 768)
        << "mode=" << folly::to_underlying(mode);
  }
}

TEST_F(AllToAllPushPullTest, PushModeFirstWriteWaitsForStartDelay) {
  constexpr uint64_t kStartDelayNs = 500;
  auto workCfg = buildWorkCfg(
      "Push_Staggered", packsim::workload::thrift::WorkloadMode::PUSH, 2, 2);
  workCfg.explicitStaggeredStartTimeNs() = {kStartDelayNs, 0};
  std::vector<uint64_t> writeTimesNs;
  smcb_ = [this, &writeTimesNs](packsim::Message* msg) {
    if (msg->isWrite) {
      writeTimesNs.emplace_back(simCtx_.nowNs());
    }
  };
  auto rank = createRank(workCfg);

  rank.run();
  scheduler_.run();

  EXPECT_THAT(writeTimesNs, ElementsAre(kStartDelayNs, kStartDelayNs));
}

TEST_F(AllToAllPushPullTest, PullModeFirstReadWaitsForStartDelay) {
  constexpr uint64_t kStartDelayNs = 500;
  auto workCfg = buildWorkCfg(
      "Pull_Staggered", packsim::workload::thrift::WorkloadMode::PULL, 2, 2);
  workCfg.explicitStaggeredStartTimeNs() = {kStartDelayNs, 0};
  std::vector<uint64_t> readTimesNs;
  rmcb_ = [this, &readTimesNs](packsim::Message* msg) {
    if (msg->isRead) {
      readTimesNs.emplace_back(simCtx_.nowNs());
    }
  };
  auto rank = createRank(workCfg);

  rank.run();
  scheduler_.run();

  EXPECT_THAT(readTimesNs, ElementsAre(kStartDelayNs, kStartDelayNs));
}

} // namespace packsim
