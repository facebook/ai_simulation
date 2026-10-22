/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/transport/transport.h"
#include "packsim/workloads/rank/workload-rank.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

// Minimal concrete rank: getDataQPCountFromTotal is a pure helper, so no QPN
// maps are configured and the rank never sends anything.
class BareRank : public WorkloadRank {
 public:
  explicit BareRank(const WorkloadRankParams& params) : WorkloadRank(params) {}

  void run() override {}

  uint64_t getExpectedBytes() override {
    return 0;
  }

  bool complete() override {
    return true;
  }

  uint32_t dataQPCountFromTotal(const size_t totalQPs) const {
    return getDataQPCountFromTotal(totalQPs);
  }
};

// Deliberately does not reuse WorkloadRankTestHarness: the helper under test
// needs no QPN topology, so the harness' buildRanks() Arrange would be dead
// weight here.
class DataQpCountTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const WorkloadRankParams params{
        .simCtx = &simCtx_,
        .workloadId = 0,
        .workCfg = workCfg_,
        .rank = 0,
        .smcb = noopSubmitMessage_,
        .rmcb = noopSubmitMessage_,
        .rccb = noopOnRankComplete_,
        .parent = nullptr};
    rank_ = std::make_unique<BareRank>(params);
  }

  WorkloadRankParams::SubmitMessageFn noopSubmitMessage_{[](Message*) {}};
  WorkloadRankParams::OnRankCompleteCallback noopOnRankComplete_{
      [](uint32_t, uint32_t) {}};
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  packsim::workload::thrift::Workload workCfg_;
  std::unique_ptr<BareRank> rank_;
};

TEST_F(DataQpCountTest, DataQpCountReservesTheLastQpForNotify) {
  // Act + Assert
  EXPECT_EQ(rank_->dataQPCountFromTotal(2), 1u);
  EXPECT_EQ(rank_->dataQPCountFromTotal(5), 4u);
  EXPECT_EQ(rank_->dataQPCountFromTotal(64), 63u);
}

TEST_F(DataQpCountTest, DataQpCountFloorsAtOneForDegenerateTotals) {
  // Act + Assert: a single QP is shared between data and notify, and an empty
  // QP list still reports one data QP rather than underflowing to 2^32-1.
  EXPECT_EQ(rank_->dataQPCountFromTotal(1), 1u);
  EXPECT_EQ(rank_->dataQPCountFromTotal(0), 1u);
}

TEST_F(DataQpCountTest, DataQpCountAcceptsTheLargestUint32Count) {
  // Arrange
  constexpr uint32_t kMaxDataQpCount = std::numeric_limits<uint32_t>::max();

  // Act + Assert
  EXPECT_EQ(
      rank_->dataQPCountFromTotal(size_t{kMaxDataQpCount} + 1),
      kMaxDataQpCount);
}

TEST_F(DataQpCountTest, DataQpCountThrowsWhenItDoesNotFitUint32) {
  // Arrange: one QP more than the largest total whose data-QP count fits.
  constexpr size_t kOverflowingTotal =
      size_t{std::numeric_limits<uint32_t>::max()} + 2;

  // Act + Assert: the count throws rather than wrapping to a small value.
  EXPECT_THAT(
      [this] { rank_->dataQPCountFromTotal(kOverflowingTotal); },
      ThrowsMessage<std::range_error>(
          HasSubstr("Overflow during arithmetic conversion")));
}

} // namespace packsim
