/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

// When numChunks exceeds maxChunksInFlight, the send-side progress() is
// called multiple times. The QP round-robin assignment must be persistent
// across calls so it stays in sync with the recv side, which posts all
// chunks in a single pass during run().
class AllToAllNicAwareChunkedTest : public packsim::WorkloadRankTestHarness,
                                    public ::testing::TestWithParam<std::tuple<
                                        uint32_t /* endpoints */,
                                        uint32_t /* chunks */,
                                        uint32_t /* maxInFlight */,
                                        uint32_t /* numNicPairs */>> {
 public:
  void SetUp() override {
    auto [endpoints, chunks, maxInFlight, numNicPairs] = GetParam();
    workCfg_.name() = "A2A_NIC_CHUNKED";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = endpoints;

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 1024 * chunks;
    a2a.numChunks() = chunks;
    a2a.maxChunksInFlight() = maxInFlight;
    a2a.numGpusPerNvDomain() = 1;
    customize(a2a);
    workCfg_.config()->allToAll() = a2a;

    // Create QP templates with numNicPairs NIC pairs per rank pair.
    // Each rank needs numNicPairs endpoint IDs in rankToIdList so
    // sendWithQPIndex can resolve the NIC index to a FlexDeviceId.
    workCfg_.rankToId()->clear();
    workCfg_.rankToIdList()->clear();
    for (uint32_t rank = 0; rank < endpoints; ++rank) {
      workCfg_.rankToId()->insert({rank, {rank, 0}});
      std::vector<std::vector<uint32_t>> epIds;
      epIds.reserve(numNicPairs);
      for (uint32_t nic = 0; nic < numNicPairs; ++nic) {
        epIds.push_back({rank, nic});
      }
      workCfg_.rankToIdList()->emplace(rank, epIds);
    }
    for (uint32_t src = 0; src < endpoints; ++src) {
      for (uint32_t dst = 0; dst < endpoints; ++dst) {
        if (src == dst) {
          continue;
        }
        for (uint32_t nic = 0; nic < numNicPairs; ++nic) {
          packsim::thrift::QueuePairTemplate tmpl;
          tmpl.ranks()->srcRank() = src;
          tmpl.ranks()->dstRank() = dst;
          tmpl.nics()->srcNicIdx() = nic;
          tmpl.nics()->dstNicIdx() = nic;
          tmpl.ports()->srcPorts() = {0};
          tmpl.ports()->dstPorts() = {0};
          workCfg_.qpTemplates()->emplace_back(std::move(tmpl));
        }
      }
    }

    run();
  }

 protected:
  /**
   * Adjust the AllToAll config before the ranks are built. The base leaves it
   * as the parameters set it.
   *
   * @param a2a  The config the ranks will run.
   */
  virtual void customize(packsim::workload::thrift::AllToAll& /*a2a*/) {}
};

} // namespace packsim
