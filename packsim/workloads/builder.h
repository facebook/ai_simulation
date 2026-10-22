/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

/**
 * Build the rank that runs one rank of a workload.
 *
 * @param simCtx          Simulation context the rank runs in.
 * @param workloadId      ID of the workload.
 * @param w               The workload.
 * @param rank            The rank to build.
 * @param sendMessageCb   Function the rank posts its sends to.
 * @param recvMessageCb   Function the rank posts its receives to.
 * @param rankCompleteCb  Called with the workload ID and rank when the rank
 *                        completes.
 * @param parent          The rank that owns this one, such as a batch
 *                        workload's rank for each of its children; nullptr for
 *                        an owner rank.
 *
 * @return                The rank.
 * @throw                 std::invalid_argument for a workload type or
 *                        collective algorithm that has no rank, including an
 *                        unset config and values outside the enums, or for a
 *                        config the rank cannot run.
 */
std::unique_ptr<WorkloadRank> buildWorkloadRank(
    SimContext* simCtx,
    uint32_t workloadId,
    const packsim::workload::thrift::Workload& w,
    uint32_t rank,
    const std::function<void(Message*)>& sendMessageCb,
    const std::function<void(Message*)>& recvMessageCb,
    const std::function<void(uint32_t, uint32_t)>& rankCompleteCb,
    WorkloadRank* parent = nullptr);

/**
 * List the peers each rank of a workload exchanges data with.
 *
 * @param w  The workload.
 *
 * @return   The peer ranks of each rank, indexed by rank.
 * @throw    std::invalid_argument for a workload type or collective algorithm
 *           that has no peer pattern, including an unset config and values
 *           outside the enums, or for a config its pattern cannot serve.
 */
std::vector<std::vector<uint32_t>> getRankPeers(
    const packsim::workload::thrift::Workload& w);

} // namespace packsim
