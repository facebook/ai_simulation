/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

namespace {
constexpr uint32_t kNumRanks = 2;
// The only NIC with QPs. NIC 0 has none, as when it cables to a rack-local
// switch that cannot reach the peer.
constexpr uint32_t kQpNicIdx = 1;
} // namespace

class WorkloadRankNotifyNicTest : public WorkloadRankTestHarness,
                                  public ::testing::Test {
 public:
  void SetUp() override {
    workCfg_.name() = "NOTIFY_NIC_TEST";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = kNumRanks;

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 1024;
    a2a.numChunks() = 1;
    a2a.numGpusPerNvDomain() = 1;
    workCfg_.config()->allToAll() = a2a;

    // Two NIC addresses per rank, and one QP template per direction on NIC
    // kQpNicIdx only; the second direction gives the notify its own QP.
    for (uint32_t rank = 0; rank < kNumRanks; ++rank) {
      workCfg_.rankToIdList()->emplace(
          rank, std::vector<std::vector<uint32_t>>{{rank, 1}, {rank, 2}});
      packsim::thrift::QueuePairTemplate qpTemplate;
      qpTemplate.ranks()->srcRank() = rank;
      qpTemplate.ranks()->dstRank() = (rank + 1) % kNumRanks;
      qpTemplate.nics()->srcNicIdx() = kQpNicIdx;
      qpTemplate.nics()->dstNicIdx() = kQpNicIdx;
      workCfg_.qpTemplates()->emplace_back(std::move(qpTemplate));
    }
    run();
  }
};

TEST_F(WorkloadRankNotifyNicTest, NoMessageTravelsOnTheNicWithoutQps) {
  for (uint32_t rank = 0; rank < kNumRanks; ++rank) {
    EXPECT_FALSE(rankStats_.at(rank).sentMsgsByEndpoint.contains(0))
        << "rank " << rank << " sent a message on NIC 0";
    EXPECT_FALSE(rankStats_.at(rank).rcvdMsgsByEndpoint.contains(0))
        << "rank " << rank << " received a message on NIC 0";
  }
}

} // namespace packsim
