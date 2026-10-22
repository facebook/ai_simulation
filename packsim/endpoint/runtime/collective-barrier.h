/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <tuple>
#include <vector>

#include "packsim/kernel/sim-context.h"

namespace packsim {

/**
 * A collective barrier that can be used to coordinate events across all ranks
 * for a specific workload. One of our use cases here is to start the next
 * iteration of a workload once all ranks from previous iteration finish.
 *
 * Leader is the node that will coordinate all ranks for a given workload. We
 * usually select the rank 0 of each workload to be used as leader for that
 * workload. The synchronization across multiple threads happens through the
 * PDES global scheduler and hence no mutex is needed.
 *
 * NOTE: For the leader node, the pointer to barrier does not need to be
 * provided since it is the same node. It can be set to nullptr or left out.
 */
class CollectiveBarrier {
 public:
  CollectiveBarrier(
      SimContext* const leaderContext,
      SimContext* const context,
      uint32_t const rank,
      const packsim::workload::thrift::Workload* workCfg,
      CollectiveBarrier* const leaderBarrier = nullptr);

  /**
   * Let the barrier know that the associated rank has arrived at the barrier
   * and will be expecting the provided callback to be called once all other
   * ranks also arrive at the barrier.
   *
   * NOTE: The barrier here will not be blocking. Instead, you should make it so
   * that it is the last thing that runs before the contents inside the callback
   * are executed. The event that runs arrive should conclude all operations
   * once it calls arrive. Blocking a thread on a PDES is highly discouraged as
   * it will suspend any other contexts assigned to the same thread and hence we
   * are not doing that.
   *
   * @param callback The function to be called once all ranks arrive at the
   * barrier
   */
  void arrive(std::function<void()> callback);

 protected:
  /**
   * Informs the barrier that the associated workload rank completed on the
   * provided context and provides a pointer to the context on which the
   * callback needs to be called.
   *
   * @param rank     The rank that has arrived at the barrier
   * @param context  The context on which the callback should be called
   * @param callback The function to be called for this rank
   */
  void postRankArrived(
      uint32_t const rank,
      SimContext* const context,
      std::function<void()> callback);

 private:
  SimContext* const leaderContext_;
  SimContext* const context_;
  uint32_t const rank_;
  const packsim::workload::thrift::Workload* workCfg_;
  CollectiveBarrier* const leaderBarrier_;

  std::vector<std::tuple<
      uint32_t /* rank */,
      SimContext*,
      std::function<void()> /* callback */>>
      rankState_;
};

} // namespace packsim
