/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"

namespace packsim {

/**
 * Queue pair templates and simulations for the sim-fix tests.
 */
class SimFixTestFixture {
 public:
  /**
   * A queue pair template from rank srcRank's NIC srcNicIdx to rank dstRank's
   * NIC dstNicIdx.
   *
   * @param srcRank    Source rank.
   * @param dstRank    Destination rank.
   * @param srcNicIdx  Index of the source rank's NIC.
   * @param dstNicIdx  Index of the destination rank's NIC.
   * @param srcPorts   The template's source ports.
   * @param dstPorts   The template's destination ports.
   *
   * @return           The template.
   */
  static packsim::thrift::QueuePairTemplate createQpTemplate(
      uint32_t srcRank,
      uint32_t dstRank,
      uint32_t srcNicIdx,
      uint32_t dstNicIdx,
      std::vector<uint16_t> srcPorts,
      std::vector<uint16_t> dstPorts) {
    packsim::thrift::QueuePairTemplate qpTemplate;

    packsim::thrift::RankPair rankPair;
    rankPair.srcRank() = srcRank;
    rankPair.dstRank() = dstRank;
    qpTemplate.ranks() = std::move(rankPair);

    packsim::thrift::NicPair nicPair;
    nicPair.srcNicIdx() = srcNicIdx;
    nicPair.dstNicIdx() = dstNicIdx;
    qpTemplate.nics() = std::move(nicPair);

    packsim::thrift::QpnPortConfig portConfig;
    portConfig.srcPorts() = std::move(srcPorts);
    portConfig.dstPorts() = std::move(dstPorts);
    qpTemplate.ports() = std::move(portConfig);

    return qpTemplate;
  }

  /**
   * A simulation of one 4-rank all-gather, "ag", over 2 rails. The split
   * assigns globalRank = railRank * numRails + rail, so rail r owns global
   * ranks {r, r + 2}.
   *
   * @param qpTemplates  The workload's queue pair templates.
   *
   * @return             The simulation config.
   */
  static packsim::thrift::Simulation makeTwoRailAllGatherSimCfg(
      std::vector<packsim::thrift::QueuePairTemplate> qpTemplates) {
    packsim::thrift::Simulation simCfg;

    auto& wkld = simCfg.newWorkloadSchedule()->workloads()->emplace_back();
    wkld.name() = "ag";
    packsim::workload::thrift::AllGather agW;
    agW.sizeBytes() = 1000000;
    agW.numRails() = 2;
    // No thrift default: an unset algorithm is UNKNOWN, which genArAgRsPeers
    // rejects. Every real config sets RING.
    agW.algorithm() = packsim::workload::thrift::AllReduceAlgorithm::RING;
    wkld.config()->allGather() = std::move(agW);
    wkld.numEndpoints() = 4;
    wkld.rankToId() = {{0, {0}}, {1, {1}}, {2, {2}}, {3, {3}}};
    wkld.qpTemplates() = std::move(qpTemplates);

    return simCfg;
  }
};

} // namespace packsim
