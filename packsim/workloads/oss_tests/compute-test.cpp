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
#include <optional>
#include <stdexcept>
#include <tuple>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/sim-fix.h"
#include "packsim/workloads/compute.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

class ComputeWorkloadRankTest : public packsim::WorkloadRankTestHarness,
                                public ::testing::TestWithParam<std::tuple<
                                    uint32_t /* # endpoints */,
                                    uint64_t /* delay */,
                                    uint64_t /* jitter */>> {
 public:
  void SetUp() override {
    workCfg_.name() = "Compute";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = std::get<0>(GetParam());

    auto compute = packsim::workload::thrift::Compute();
    compute.computeTimeNs() = std::get<1>(GetParam());
    if (std::get<2>(GetParam()) > 0) {
      compute.computeTimeJitterNs() = std::get<2>(GetParam());
    }
    workCfg_.config()->compute() = compute;

    run();
  }
};

TEST_P(ComputeWorkloadRankTest, Validate) {
  validateStats();
}

INSTANTIATE_TEST_SUITE_P(
    ComputeWorkloadRankTestSweep,
    ComputeWorkloadRankTest,
    ::testing::Values(
        std::make_tuple(64, 100, 0),
        std::make_tuple(1024, 100, 10),
        std::make_tuple(4, 5, 10),
        std::make_tuple(1, 0, 0)));

/**
 * Unit tests exercising ComputeRank in isolation (without the multi-rank
 * simulation harness). These cover the constructor-validation and
 * unsupported-callback branches that the parameterized happy-path sweep above
 * does not reach:
 *   - a non-compute config throws at construction
 *   - requestReceived()/requestAcked() always throw (compute ranks never
 *     participate in message exchange)
 *   - getExpectedBytes() is zero (compute ranks transfer no bytes)
 */
class ComputeRankUnitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    smcb_ = [this](packsim::Message* msg) {
      sentMsgs_.emplace_back(std::make_unique<packsim::Message>(*msg));
    };
    rmcb_ = [this](packsim::Message* msg) {
      postedRecvs_.emplace_back(std::make_unique<packsim::Message>(*msg));
    };
    rccb_ = [this](uint32_t, uint32_t) { rankCompleted_ = true; };
  }

  static void populateRankMaps(packsim::workload::thrift::Workload& workCfg) {
    std::map<uint32_t, std::vector<uint32_t>> rankToId;
    std::map<uint32_t, std::vector<std::vector<uint32_t>>> rankToIdList;
    const uint32_t numEndpoints = *workCfg.numEndpoints();
    for (uint32_t i = 0; i < numEndpoints; ++i) {
      rankToId[i] = {i};
      rankToIdList[i] = {{i}};
    }
    workCfg.rankToId() = rankToId;
    workCfg.rankToIdList() = rankToIdList;
  }

  static void finalizeQpnMaps(packsim::workload::thrift::Workload& workCfg) {
    std::vector<packsim::workload::thrift::Workload> workCfgs({workCfg});
    packsim::buildQPNMap(workCfgs);
    uint32_t nextQpn = 0;
    packsim::buildNicQPNMap(workCfgs, nextQpn);
    workCfg = std::move(workCfgs.at(0));
  }

  packsim::workload::thrift::Workload buildComputeWorkCfg(
      uint64_t computeTimeNs,
      std::optional<uint64_t> jitterNs) {
    packsim::workload::thrift::Workload workCfg;
    workCfg.name() = "ComputeUnitTest";
    workCfg.workloadId() = 0;
    workCfg.numEndpoints() = 2;
    populateRankMaps(workCfg);

    auto compute = packsim::workload::thrift::Compute();
    compute.computeTimeNs() = computeTimeNs;
    if (jitterNs.has_value()) {
      compute.computeTimeJitterNs() = *jitterNs;
    }
    workCfg.config()->compute() = compute;
    finalizeQpnMaps(workCfg);
    return workCfg;
  }

  packsim::workload::thrift::Workload buildIncastWorkCfg() {
    packsim::workload::thrift::Workload workCfg;
    workCfg.name() = "WrongTypeForCompute";
    workCfg.workloadId() = 0;
    workCfg.numEndpoints() = 2;
    populateRankMaps(workCfg);

    auto incast = packsim::workload::thrift::Incast();
    incast.sizeBytes() = 1024;
    incast.numChunks() = 1;
    incast.numSources() = 1;
    workCfg.config()->incast() = incast;
    finalizeQpnMaps(workCfg);
    return workCfg;
  }

  packsim::ComputeRank createRank(
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
    return packsim::ComputeRank(params);
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

TEST_F(ComputeRankUnitTest, ConstructorRejectsNonComputeConfig) {
  auto workCfg = buildIncastWorkCfg();
  EXPECT_THAT(
      [&]() { createRank(workCfg, /*rank=*/0); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Invalid config passed down to ComputeRank")));
}

TEST_F(ComputeRankUnitTest, ConstructorRejectsNonzeroSize) {
  auto workCfg = buildComputeWorkCfg(/*computeTimeNs=*/100, std::nullopt);
  workCfg.config()->compute()->sizeBytes() = 1;
  EXPECT_THAT(
      [&]() { createRank(workCfg, /*rank=*/0); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Workload size for compute workload must be zero")));
}

TEST_F(ComputeRankUnitTest, RequestReceivedAlwaysThrows) {
  auto workCfg = buildComputeWorkCfg(/*computeTimeNs=*/100, std::nullopt);
  auto rank = createRank(workCfg, /*rank=*/0);
  EXPECT_THAT(
      [&]() { rank.requestReceived(nullptr); },
      ThrowsMessage<std::runtime_error>(HasSubstr("should never be called")));
}

TEST_F(ComputeRankUnitTest, RequestAckedAlwaysThrows) {
  auto workCfg = buildComputeWorkCfg(/*computeTimeNs=*/100, std::nullopt);
  auto rank = createRank(workCfg, /*rank=*/0);
  EXPECT_THAT(
      [&]() { rank.requestAcked(nullptr); },
      ThrowsMessage<std::runtime_error>(HasSubstr("should never be called")));
}

TEST_F(ComputeRankUnitTest, GetExpectedBytesIsZero) {
  auto workCfg = buildComputeWorkCfg(/*computeTimeNs=*/100, std::nullopt);
  auto rank = createRank(workCfg, /*rank=*/0);
  EXPECT_EQ(rank.getExpectedBytes(), 0u);
}

} // namespace packsim
