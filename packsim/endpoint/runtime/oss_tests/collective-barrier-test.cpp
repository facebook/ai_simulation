/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <atomic>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/endpoint/runtime/collective-barrier.h"

namespace packsim {

TEST(CollectiveBarrier, TestCollectiveBarrier) {
  constexpr uint32_t kNumEndpoints = 12;
  constexpr uint32_t kNumWorkloads = 5;
  // Ranks complete at a delay drawn from [0, kMaxCompletionDelayNs).
  constexpr uint32_t kMaxCompletionDelayNs = 1000;
  constexpr uint32_t kSeed = 12345;

  struct MockWorkloadRank {
    void run() {
      simCtx->schedule(
          Time::fromNs(completionDelayNs), &MockWorkloadRank::complete, this);
    }

    void complete() {
      (*ranksCompleted)++;
      auto const workloadId = *workCfg->workloadId();
      completedCb(workloadId);
    }

    std::function<void(uint32_t /* workload ID */)> completedCb;
    SimContext* simCtx{};
    uint32_t completionDelayNs{};
    uint32_t rank{};
    packsim::workload::thrift::Workload* workCfg{};
    std::atomic_int* ranksCompleted{};
  };

  struct MockWorkloadNode {
    void workloadCompleted(uint32_t workloadId) {
      XLOGF(
          INFO, "At node {} workload {} completed.", simCtx->name, workloadId);
      workloadBarrier.at(workloadId)->arrive([this, workloadId]() {
        XLOGF(
            INFO,
            "All ranks completed at {} for workload {}",
            simCtx->name,
            workloadId);
        EXPECT_EQ(ranksCompletedList->at(workloadId), numEndpoints);
        ++releasedList->at(workloadId);
      });
    }

    std::unique_ptr<SimContext> simCtx;
    folly::F14FastMap<
        uint32_t /* workload ID */,
        std::unique_ptr<CollectiveBarrier>>
        workloadBarrier;
    folly::F14FastMap<
        uint32_t /* workload ID */,
        std::unique_ptr<MockWorkloadRank>>
        workloads;
    std::vector<std::atomic_int>* ranksCompletedList{};
    // Barrier callbacks run so far, per workload.
    std::vector<std::atomic_int>* releasedList{};
    int numEndpoints{};
  };

  Scheduler scheduler;

  std::vector<std::atomic_int> ranksCompletedList(kNumWorkloads);
  std::vector<std::atomic_int> releasedList(kNumWorkloads);
  std::mt19937 rng(kSeed);
  std::uniform_int_distribution<uint32_t> delayNs(0, kMaxCompletionDelayNs - 1);

  std::vector<packsim::workload::thrift::Workload> workCfgs;
  for (uint32_t workloadId = 0; workloadId < kNumWorkloads; ++workloadId) {
    auto& workCfg = workCfgs.emplace_back();
    workCfg.workloadId() = workloadId;
    for (uint32_t rank = 0; rank < kNumEndpoints; ++rank) {
      workCfg.rankToId()->emplace(
          rank, std::vector<uint32_t>{0, 1, 2, 3, rank});
    }
  }

  auto conditionalMap = folly::F14FastMap<std::string, uint64_t>{};

  std::vector<MockWorkloadNode> endpoints(kNumEndpoints);

  for (uint32_t rank = 0; rank < std::ssize(endpoints); ++rank) {
    auto& endpoint = endpoints.at(rank);
    endpoint.simCtx = std::make_unique<SimContext>(
        &scheduler,
        fmt::format("test_context_{}", rank),
        rank,
        &conditionalMap);
    endpoint.ranksCompletedList = &ranksCompletedList;
    endpoint.releasedList = &releasedList;
    endpoint.numEndpoints = kNumEndpoints;
    for (uint32_t workloadId = 0; workloadId < kNumWorkloads; ++workloadId) {
      endpoint.workloads[workloadId] = std::make_unique<MockWorkloadRank>();
      auto workloadRank = endpoint.workloads.at(workloadId).get();
      workloadRank->simCtx = endpoint.simCtx.get();
      workloadRank->completionDelayNs = delayNs(rng);
      workloadRank->rank = rank;
      workloadRank->ranksCompleted = &ranksCompletedList.at(workloadId);
      workloadRank->completedCb = [endpointPtr =
                                       &endpoint](uint32_t workloadId) {
        endpointPtr->workloadCompleted(workloadId);
      };
      workloadRank->workCfg = &workCfgs.at(workloadId);
      endpoint.simCtx->scheduleNow(&MockWorkloadRank::run, workloadRank);
    }
  }

  for (uint32_t workloadId = 0; workloadId < kNumWorkloads; ++workloadId) {
    endpoints.at(0).workloadBarrier[workloadId] =
        std::make_unique<CollectiveBarrier>(
            endpoints.at(0).simCtx.get(),
            endpoints.at(0).simCtx.get(),
            0,
            &workCfgs.at(workloadId));
    for (uint32_t rank = 1; rank < kNumEndpoints; ++rank) {
      endpoints.at(rank).workloadBarrier[workloadId] =
          std::make_unique<CollectiveBarrier>(
              endpoints.at(0).simCtx.get(),
              endpoints.at(rank).simCtx.get(),
              rank,
              &workCfgs.at(workloadId),
              endpoints.at(0).workloadBarrier.at(workloadId).get());
    }
  }

  scheduler.run();

  for (uint32_t workloadId = 0; workloadId < kNumWorkloads; ++workloadId) {
    EXPECT_EQ(releasedList.at(workloadId).load(), int{kNumEndpoints})
        << "workload " << workloadId;
  }
}

// Two endpoints of one workload, each with its own context; endpoint 0 leads.
class CollectiveBarrierArrivalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    workCfg_.workloadId() = 0;
    for (uint32_t rank = 0; rank < kNumRanks; ++rank) {
      workCfg_.rankToId()->emplace(rank, std::vector<uint32_t>{rank});
      contexts_.emplace_back(
          std::make_unique<SimContext>(
              &scheduler_,
              fmt::format("barrier_context_{}", rank),
              rank,
              &conditionalMap_));
    }
  }

  // A barrier for `rank` on endpoint `endpoint`'s context.
  CollectiveBarrier& addBarrier(const uint32_t endpoint, const uint32_t rank) {
    CollectiveBarrier* const leader =
        barriers_.empty() ? nullptr : barriers_.front().get();
    return *barriers_.emplace_back(
        std::make_unique<CollectiveBarrier>(
            contexts_.front().get(),
            contexts_.at(endpoint).get(),
            rank,
            &workCfg_,
            leader));
  }

  static constexpr uint32_t kNumRanks = 2;
  Scheduler scheduler_;
  folly::F14FastMap<std::string, uint64_t> conditionalMap_;
  packsim::workload::thrift::Workload workCfg_;
  std::vector<std::unique_ptr<SimContext>> contexts_;
  std::vector<std::unique_ptr<CollectiveBarrier>> barriers_;
};

TEST_F(CollectiveBarrierArrivalTest, RejectsARankArrivingTwice) {
  CollectiveBarrier& leader = addBarrier(/*endpoint=*/0, /*rank=*/0);
  contexts_.front()->scheduleNow([&leader]() {
    leader.arrive([]() {});
    leader.arrive([]() {});
  });

  EXPECT_THAT(
      [this]() { scheduler_.run(); },
      ::testing::ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "Rank 0 of workload ID 0 arrived at the barrier more than once.")));
}

TEST_F(CollectiveBarrierArrivalTest, RejectsARankOutsideTheWorkload) {
  constexpr uint32_t kForeignRank = 5;
  CollectiveBarrier& leader = addBarrier(/*endpoint=*/0, /*rank=*/0);
  CollectiveBarrier& foreign = addBarrier(/*endpoint=*/1, kForeignRank);
  contexts_.front()->scheduleNow([&leader]() { leader.arrive([]() {}); });
  contexts_.at(1)->scheduleNow([&foreign]() { foreign.arrive([]() {}); });

  EXPECT_THAT(
      [this]() { scheduler_.run(); },
      ::testing::ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "Rank 5 completed for workload ID 0 was not part of the original "
          "config.")));
}

} // namespace packsim
