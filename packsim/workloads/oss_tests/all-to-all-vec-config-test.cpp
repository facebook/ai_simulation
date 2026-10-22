/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/workloads/all-to-all-vec.h"

namespace wt = packsim::workload::thrift;

namespace packsim {
namespace {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// VectorizedAllToAllRank validates its vectorizedSizesBytes matrix in the
// constructor, before anything is indexed: this rank reads its send row
// sizes[rank][*] and its recv column sizes[*][rank], so a non-square matrix
// would index out of bounds.
//
// all-to-all-vec-workload-rank-test.cpp drives a full simulation with a
// well-formed square matrix, so none of these branches are reachable from it.
// Constructing the rank directly is enough -- the throws all happen before any
// simulation state is touched.
//
// Note the brace-init: `VectorizedAllToAllRank{makeParams()}`, not
// `VectorizedAllToAllRank(makeParams())`. The parenthesised form is a
// most-vexing parse -- it declares a function named `makeParams` returning the
// rank rather than constructing one -- so every assertion below would pass
// vacuously, including the EXPECT_NO_THROW ones. Sibling suites are not exposed
// to this because their constructors take more than one argument.
class AllToAllVecConfigTest : public ::testing::Test {
 protected:
  static constexpr uint32_t kNumRanks = 4;
  static constexpr uint32_t kWorkloadId = 0;
  static constexpr uint32_t kRank = 0;

  // A well-formed numRanks x numRanks matrix of zeroes.
  static std::vector<std::vector<uint32_t>> makeSquareSizes(uint32_t numRanks) {
    return std::vector<std::vector<uint32_t>>(
        numRanks, std::vector<uint32_t>(numRanks, 0));
  }

  void setConfig(
      std::vector<std::vector<uint32_t>> sizes,
      std::vector<std::vector<uint64_t>> delaysNs = {}) {
    wt::AllToAll a2a;
    a2a.sizeBytes() = 0;
    a2a.numChunks() = 1;
    a2a.vectorizedSizesBytes() = std::move(sizes);
    a2a.msgDelaysNs() = std::move(delaysNs);
    workCfg_.name() = "A2AV";
    workCfg_.workloadId() = kWorkloadId;
    workCfg_.numEndpoints() = kNumRanks;
    workCfg_.config()->allToAll() = std::move(a2a);
  }

  WorkloadRankParams makeParams() {
    return WorkloadRankParams{
        .simCtx = &simCtx_,
        .workloadId = kWorkloadId,
        .workCfg = workCfg_,
        .rank = kRank,
        .smcb = noopSubmitMessage_,
        .rmcb = noopSubmitMessage_,
        .rccb = noopOnRankComplete_,
        .parent = nullptr,
    };
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  wt::Workload workCfg_;
  WorkloadRankParams::SubmitMessageFn noopSubmitMessage_ = [](Message*) {};
  WorkloadRankParams::OnRankCompleteCallback noopOnRankComplete_ =
      [](uint32_t, uint32_t) {};
};

TEST_F(AllToAllVecConfigTest, ASquareMatrixIsAccepted) {
  setConfig(makeSquareSizes(kNumRanks));

  EXPECT_NO_THROW(VectorizedAllToAllRank{makeParams()});
}

// Too few rows: the recv-column read sizes[i][rank] would run off the end.
TEST_F(AllToAllVecConfigTest, TooFewRowsIsRejected) {
  setConfig(makeSquareSizes(kNumRanks - 1));

  EXPECT_THAT(
      [this]() { VectorizedAllToAllRank{makeParams()}; },
      ThrowsMessage<std::runtime_error>(HasSubstr(
          "vectorizedSizesBytes has 3 rows but the workload has 4 ranks")));
}

TEST_F(AllToAllVecConfigTest, TooManyRowsIsRejected) {
  setConfig(makeSquareSizes(kNumRanks + 1));

  EXPECT_THAT(
      [this]() { VectorizedAllToAllRank{makeParams()}; },
      ThrowsMessage<std::runtime_error>(HasSubstr(
          "vectorizedSizesBytes has 5 rows but the workload has 4 ranks")));
}

// Right number of rows, wrong width on one of them -- the send-row read
// sizes[rank][i] would run off the end of that row.
TEST_F(AllToAllVecConfigTest, ARaggedRowIsRejectedAndIdentified) {
  auto sizes = makeSquareSizes(kNumRanks);
  sizes[2].pop_back();
  setConfig(std::move(sizes));

  EXPECT_THAT(
      [this]() { VectorizedAllToAllRank{makeParams()}; },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("vectorizedSizesBytes row 2 has 3 columns")));
}

// A rank never sends to itself, so whatever the diagonal holds would be counted
// as expected bytes that never arrive.
TEST_F(AllToAllVecConfigTest, ANonZeroDiagonalIsRejected) {
  auto sizes = makeSquareSizes(kNumRanks);
  sizes[2][2] = 64;
  setConfig(std::move(sizes));

  EXPECT_THAT(
      [this]() { VectorizedAllToAllRank{makeParams()}; },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Workload A2AV: vectorizedSizesBytes[2][2] is 64, but a rank sends "
          "nothing to itself, so the diagonal must be 0")));
}

// ── msgDelaysNs shape ────────────────────────────────────────────────────────

// The delay matrix is optional: an empty one means no per-message delays and
// skips the shape check entirely.
TEST_F(AllToAllVecConfigTest, AnEmptyDelayMatrixIsAccepted) {
  setConfig(makeSquareSizes(kNumRanks), /*delaysNs=*/{});

  EXPECT_NO_THROW(VectorizedAllToAllRank{makeParams()});
}

TEST_F(AllToAllVecConfigTest, AMatchingDelayMatrixIsAccepted) {
  setConfig(
      makeSquareSizes(kNumRanks),
      std::vector<std::vector<uint64_t>>(
          kNumRanks, std::vector<uint64_t>(kNumRanks, 0)));

  EXPECT_NO_THROW(VectorizedAllToAllRank{makeParams()});
}

// A populated delay matrix must match the size matrix exactly; a shorter one
// is rejected rather than silently defaulting the missing entries to zero.
TEST_F(AllToAllVecConfigTest, ADelayMatrixWithTooFewRowsIsRejected) {
  setConfig(
      makeSquareSizes(kNumRanks),
      std::vector<std::vector<uint64_t>>(
          kNumRanks - 1, std::vector<uint64_t>(kNumRanks, 0)));

  EXPECT_THAT(
      [this]() { VectorizedAllToAllRank{makeParams()}; },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Inconsistent msgDelaysNs vector size")));
}

TEST_F(AllToAllVecConfigTest, ADelayMatrixWithARaggedRowIsRejected) {
  auto delaysNs = std::vector<std::vector<uint64_t>>(
      kNumRanks, std::vector<uint64_t>(kNumRanks, 0));
  delaysNs[1].pop_back();
  setConfig(makeSquareSizes(kNumRanks), std::move(delaysNs));

  EXPECT_THAT(
      [this]() { VectorizedAllToAllRank{makeParams()}; },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Inconsistent msgDelaysNs vector size")));
}

} // namespace
} // namespace packsim
