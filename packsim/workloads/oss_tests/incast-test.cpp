/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/sim-fix.h"
#include "packsim/workloads/incast.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

namespace {

packsim::workload::thrift::Workload makeIncastWorkload(
    uint32_t numEndpoints,
    uint32_t numSources) {
  packsim::workload::thrift::Workload w;
  w.name() = "IncastResolve";
  w.numEndpoints() = numEndpoints;
  packsim::workload::thrift::Incast incast;
  incast.numSources() = numSources;
  w.config()->incast() = incast;
  return w;
}

} // namespace

class IncastWorkloadRankTest : public packsim::WorkloadRankTestHarness,
                               public ::testing::TestWithParam<
                                   std::tuple<uint32_t, uint32_t, uint32_t>> {
 public:
  void SetUp() override {
    workCfg_.name() = "Incast";
    workCfg_.workloadId() = 0;
    auto numRanks = std::get<0>(GetParam());
    workCfg_.numEndpoints() = numRanks;

    auto incast = packsim::workload::thrift::Incast();
    incast.sizeBytes() = std::get<1>(GetParam());
    incast.numChunks() = std::get<2>(GetParam());
    incast.numSources() = numRanks - 1;

    workCfg_.config()->incast() = incast;

    run();
  }
};

TEST_P(IncastWorkloadRankTest, Validate) {
  validateStats();
}

INSTANTIATE_TEST_SUITE_P(
    IncastWorkloadRankTestSweep,
    IncastWorkloadRankTest,
    ::testing::Combine(
        ::testing::Values(8, 128, 256),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 3, 7, 15)));

class IncastNumSourcesRankTest
    : public packsim::WorkloadRankTestHarness,
      public ::testing::TestWithParam<std::tuple<uint32_t, uint32_t>> {
 public:
  static constexpr uint64_t kSizeBytes = 4096;

  void SetUp() override {
    workCfg_.name() = "IncastNumSources";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto incast = packsim::workload::thrift::Incast();
    incast.sizeBytes() = kSizeBytes;
    incast.numChunks() = 2;
    incast.numSources() = std::get<1>(GetParam());
    workCfg_.config()->incast() = incast;

    run();
  }
};

TEST_P(IncastNumSourcesRankTest, EveryRankCompletes) {
  validateStats();
  const auto [numRanks, numSourcesCfg] = GetParam();
  const uint32_t numSources = numSourcesCfg == 0 ? numRanks - 1 : numSourcesCfg;
  EXPECT_EQ(rankStats_.at(0).rcvdBytes, kSizeBytes * numSources)
      << "Rank 0 receives sizeBytes from each of ranks 1..numSources";
  for (uint32_t r = numSources + 1; r < numRanks; ++r) {
    EXPECT_EQ(rankStats_.at(r).sentMsgs, 0u)
        << "Rank " << r << " is beyond numSources and sends nothing";
  }
}

INSTANTIATE_TEST_SUITE_P(
    IncastNumSourcesSweep,
    IncastNumSourcesRankTest,
    ::testing::Values(
        std::make_tuple(4U, 0U),
        std::make_tuple(4U, 1U),
        std::make_tuple(4U, 2U),
        std::make_tuple(8U, 0U),
        std::make_tuple(8U, 5U)));

class IncastMessageSizeTest : public packsim::WorkloadRankTestHarness,
                              public ::testing::Test {
 protected:
  // 4 GiB, one more than the largest uint32_t.
  static constexpr uint64_t kSizeBytes = uint64_t{1} << 32;

  // Two ranks: rank 1 sends rank 0 kSizeBytes in numChunks messages.
  void configure(const uint32_t numChunks) {
    workCfg_.name() = "IncastMessageSize";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 2;

    auto incast = packsim::workload::thrift::Incast();
    incast.sizeBytes() = kSizeBytes;
    incast.numChunks() = numChunks;
    workCfg_.config()->incast() = incast;
  }
};

// 4 GiB does not fit in 32 bits, but each of its 16 messages of 256 MiB does.
TEST_F(IncastMessageSizeTest, SendsASizeThatDoesNotFitUint32InSmallerMessages) {
  configure(/*numChunks=*/16);

  run();

  validateStats();
  EXPECT_EQ(rankStats_.at(0).rcvdBytes, kSizeBytes);
  EXPECT_EQ(rankStats_.at(0).rcvdMsgs, 16u);
}

// A message size is a uint32_t, so one message cannot carry 4 GiB.
TEST_F(IncastMessageSizeTest, ThrowsWhenMessageSizeDoesNotFitUint32) {
  configure(/*numChunks=*/1);

  EXPECT_THAT(
      [&]() { run(); },
      ThrowsMessage<std::runtime_error>(HasSubstr("ceilDivide32 overflowed")));
}

/**
 * Unit tests for IncastRank's PUSH (one-sided WRITE) mode that exercise the
 * rank in isolation without the multi-rank simulation harness. These cover:
 *   - constructor validation (unsupported workload modes throw)
 *   - run() short-circuit paths (sender posts writes; destination and
 *     out-of-range ranks complete immediately with no messages)
 *   - outstandingWrites_ accounting via requestAcked() driving complete()
 *   - getExpectedBytes() per-rank classification
 */
class IncastPushModeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    sentMsgs_.clear();
    postedRecvs_.clear();
    rankCompleted_ = false;
    smcb_ = [this](packsim::Message* msg) {
      sentMsgs_.emplace_back(std::make_unique<packsim::Message>(*msg));
    };
    // The WorkloadRank base class posts notify recvs in its constructor for
    // every peer. IncastRank PUSH mode does not post any *data* recvs (this
    // is enforced by the run() short-circuit), so we just record the
    // construction-time notify recvs and never expect data recvs of size
    // sendSize_.
    rmcb_ = [this](packsim::Message* msg) {
      postedRecvs_.emplace_back(std::make_unique<packsim::Message>(*msg));
    };
    rccb_ = [this](uint32_t, uint32_t) { rankCompleted_ = true; };
  }

  packsim::workload::thrift::Workload buildWorkCfg(
      uint32_t numEndpoints,
      uint32_t numSources,
      uint64_t sizeBytes,
      uint32_t numChunks,
      packsim::workload::thrift::WorkloadMode mode) {
    packsim::workload::thrift::Workload workCfg;
    workCfg.name() = "IncastPushTest";
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

    auto incast = packsim::workload::thrift::Incast();
    incast.sizeBytes() = sizeBytes;
    incast.numChunks() = numChunks;
    incast.numSources() = numSources;
    incast.workloadMode() = mode;
    workCfg.config()->incast() = incast;

    std::vector<packsim::workload::thrift::Workload> workCfgs({workCfg});
    packsim::buildQPNMap(workCfgs);
    uint32_t nextQpn = 0;
    packsim::buildNicQPNMap(workCfgs, nextQpn);
    return std::move(workCfgs.at(0));
  }

  packsim::IncastRank createRank(
      packsim::workload::thrift::Workload& workCfg,
      uint32_t rank) {
    packsim::WorkloadRankParams params{
        .simCtx = &simCtx_,
        .workloadId = 0,
        .workCfg = workCfg,
        .rank = rank,
        .smcb = smcb_,
        .rmcb = rmcb_,
        .rccb = rccb_,
        .parent = nullptr};
    return packsim::IncastRank(params);
  }

  packsim::Scheduler scheduler_;
  packsim::SimContext simCtx_{&scheduler_, 0};
  std::vector<std::unique_ptr<packsim::Message>> sentMsgs_;
  std::vector<std::unique_ptr<packsim::Message>> postedRecvs_;
  bool rankCompleted_{false};
  std::function<void(packsim::Message*)> smcb_;
  std::function<void(packsim::Message*)> rmcb_;
  std::function<void(uint32_t, uint32_t)> rccb_;
};

TEST_F(IncastPushModeTest, ConstructorRejectsUnsupportedMode) {
  auto workCfg = buildWorkCfg(
      /*numEndpoints=*/4,
      /*numSources=*/3,
      /*sizeBytes=*/1024,
      /*numChunks=*/1,
      packsim::workload::thrift::WorkloadMode::PULL);
  EXPECT_THAT(
      [&]() { createRank(workCfg, /*rank=*/0); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("IncastRank only supports SENDRCV and PUSH modes")));
}

TEST_F(IncastPushModeTest, ConstructorRejectsZeroNumChunks) {
  auto workCfg = buildWorkCfg(
      /*numEndpoints=*/4,
      /*numSources=*/3,
      /*sizeBytes=*/1024,
      /*numChunks=*/0,
      packsim::workload::thrift::WorkloadMode::SENDRCV);
  EXPECT_THAT(
      [&]() { createRank(workCfg, /*rank=*/0); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload IncastPushTest: Incast numChunks must be at least 1, got 0")));
}

TEST_F(IncastPushModeTest, PushModeSenderPostsWritesAndCompletesAfterAcks) {
  // 4 ranks, 3 sources (ranks 1..3), 2 chunks of 1024 bytes total.
  constexpr uint32_t kNumChunks = 2;
  constexpr uint64_t kSendSize = 1024;
  auto workCfg = buildWorkCfg(
      /*numEndpoints=*/4,
      /*numSources=*/3,
      kSendSize,
      kNumChunks,
      packsim::workload::thrift::WorkloadMode::PUSH);
  // Sender rank 1 should post kNumChunks WRITE messages targeting rank 0.
  auto rank = createRank(workCfg, /*rank=*/1);
  rank.run();

  ASSERT_EQ(sentMsgs_.size(), kNumChunks)
      << "Expected one WRITE per chunk in PUSH mode";
  for (const auto& msg : sentMsgs_) {
    EXPECT_TRUE(msg->isWrite) << "PUSH mode should post WRITEs";
    EXPECT_EQ(msg->dstRank, 0u) << "All writes target rank 0";
    EXPECT_EQ(msg->srcRank, 1u);
  }
  // Verify no *data* recvs were posted by IncastRank::run() (PUSH mode does
  // not exchange notifies or recvs). Only notify-sized recvs from the base
  // class constructor (postRecvNotify) are allowed.
  for (const auto& msg : postedRecvs_) {
    EXPECT_EQ(msg->sizeBytes, packsim::kNotifyMsgSize)
        << "PUSH mode IncastRank should not post data recvs";
  }
  EXPECT_FALSE(rank.complete())
      << "Should not complete while outstandingWrites_ > 0";
  EXPECT_FALSE(rankCompleted_);

  // Ack each write; rank should complete only after the last ack.
  rank.requestAcked(sentMsgs_[0].get());
  EXPECT_FALSE(rank.complete()) << "outstandingWrites_ still > 0";
  EXPECT_FALSE(rankCompleted_);

  rank.requestAcked(sentMsgs_[1].get());
  EXPECT_TRUE(rank.complete());
  EXPECT_TRUE(rankCompleted_);
}

TEST_F(IncastPushModeTest, PushModeSenderFirstWriteWaitsForStartDelay) {
  constexpr uint64_t kStartDelayNs = 500;
  auto workCfg = buildWorkCfg(
      /*numEndpoints=*/4,
      /*numSources=*/3,
      /*sizeBytes=*/1024,
      /*numChunks=*/2,
      packsim::workload::thrift::WorkloadMode::PUSH);
  workCfg.explicitStaggeredStartTimeNs() = {0, kStartDelayNs, 0, 0};
  std::vector<uint64_t> writeTimesNs;
  smcb_ = [this, &writeTimesNs](packsim::Message* /*msg*/) {
    writeTimesNs.emplace_back(simCtx_.nowNs());
  };
  // Rank 1 is a sender; its start delay is kStartDelayNs.
  auto rank = createRank(workCfg, /*rank=*/1);

  rank.run();
  scheduler_.run();

  EXPECT_THAT(writeTimesNs, ElementsAre(kStartDelayNs, kStartDelayNs));
}

TEST_F(IncastPushModeTest, PushModeDestinationCompletesImmediately) {
  auto workCfg = buildWorkCfg(
      /*numEndpoints=*/4,
      /*numSources=*/3,
      /*sizeBytes=*/1024,
      /*numChunks=*/2,
      packsim::workload::thrift::WorkloadMode::PUSH);
  // Rank 0 is the destination; in PUSH mode it does no work.
  auto rank = createRank(workCfg, /*rank=*/0);
  rank.run();

  EXPECT_TRUE(sentMsgs_.empty())
      << "Destination should not post any messages in PUSH mode";
  EXPECT_TRUE(rank.complete());
  EXPECT_TRUE(rankCompleted_);
}

TEST_F(IncastPushModeTest, PushModeOutOfRangeRankCompletesImmediately) {
  // 8 ranks but only 2 sources (ranks 1..2); rank 5 has no work.
  auto workCfg = buildWorkCfg(
      /*numEndpoints=*/8,
      /*numSources=*/2,
      /*sizeBytes=*/1024,
      /*numChunks=*/2,
      packsim::workload::thrift::WorkloadMode::PUSH);
  auto rank = createRank(workCfg, /*rank=*/5);
  rank.run();

  EXPECT_TRUE(sentMsgs_.empty())
      << "Rank beyond numSources should not post any messages";
  EXPECT_TRUE(rank.complete());
  EXPECT_TRUE(rankCompleted_);
}

TEST_F(IncastPushModeTest, GetExpectedBytesPushModeBySenderRank) {
  constexpr uint32_t kNumRanks = 4;
  constexpr uint32_t kNumSources = 3;
  constexpr uint64_t kSendSize = 1024;
  auto workCfg = buildWorkCfg(
      kNumRanks,
      kNumSources,
      kSendSize,
      /*numChunks=*/2,
      packsim::workload::thrift::WorkloadMode::PUSH);
  EXPECT_EQ(createRank(workCfg, /*rank=*/0).getExpectedBytes(), 0u)
      << "Destination has no inbound recvs in PUSH mode";
  for (uint32_t r = 1; r <= kNumSources; ++r) {
    EXPECT_EQ(createRank(workCfg, r).getExpectedBytes(), kSendSize)
        << "Sender rank " << r << " writes sendSize_ bytes";
  }
}

TEST_F(IncastPushModeTest, GetExpectedBytesPushModeOutOfRangeRankReturnsZero) {
  constexpr uint32_t kNumRanks = 8;
  constexpr uint32_t kNumSources = 2;
  auto workCfg = buildWorkCfg(
      kNumRanks,
      kNumSources,
      /*sizeBytes=*/1024,
      /*numChunks=*/1,
      packsim::workload::thrift::WorkloadMode::PUSH);
  for (uint32_t r = kNumSources + 1; r < kNumRanks; ++r) {
    EXPECT_EQ(createRank(workCfg, r).getExpectedBytes(), 0u)
        << "Out-of-range rank " << r << " has no work in PUSH mode";
  }
}

TEST_F(IncastPushModeTest, PushModeZeroSourcesSendsFromEveryOtherRank) {
  // numSources = 0 makes every rank other than rank 0 a sender.
  constexpr uint32_t kNumChunks = 2;
  constexpr uint64_t kSendSize = 1024;
  auto workCfg = buildWorkCfg(
      /*numEndpoints=*/4,
      /*numSources=*/0,
      kSendSize,
      kNumChunks,
      packsim::workload::thrift::WorkloadMode::PUSH);
  auto rank = createRank(workCfg, /*rank=*/3);
  EXPECT_EQ(rank.getExpectedBytes(), kSendSize);
  rank.run();

  ASSERT_EQ(sentMsgs_.size(), kNumChunks);
  for (const auto& msg : sentMsgs_) {
    EXPECT_TRUE(msg->isWrite);
    EXPECT_EQ(msg->dstRank, 0u);
    EXPECT_EQ(msg->srcRank, 3u);
  }
}

TEST(IncastResolveNumSourcesTest, ZeroMeansEveryOtherRank) {
  EXPECT_EQ(IncastRank::resolveNumSources(makeIncastWorkload(4, 0)), 3u);
}

TEST(IncastResolveNumSourcesTest, KeepsExplicitCountBelowMax) {
  EXPECT_EQ(IncastRank::resolveNumSources(makeIncastWorkload(4, 2)), 2u);
}

TEST(IncastResolveNumSourcesTest, KeepsExplicitCountAtMax) {
  EXPECT_EQ(IncastRank::resolveNumSources(makeIncastWorkload(4, 3)), 3u);
}

TEST(IncastResolveNumSourcesTest, RejectsSourcesNotBelowEndpoints) {
  EXPECT_THAT(
      [] { IncastRank::resolveNumSources(makeIncastWorkload(4, 4)); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("numSources 4")));
}

TEST(IncastResolveNumSourcesTest, RejectsFewerThanTwoEndpoints) {
  EXPECT_THAT(
      [] { IncastRank::resolveNumSources(makeIncastWorkload(1, 0)); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("needs at least 2 endpoints")));
}

} // namespace packsim
