/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_set>

#include <folly/portability/GTestProd.h>

#include "packsim/endpoint/runtime/dependency-tracker.h"
#include "packsim/endpoint/runtime/global-dag-coordinator.h"
#include "packsim/endpoint/runtime/workload-node.h"
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/random.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/time.h"
#include "packsim/topology/flex-topology.h"

namespace packsim {

// @oss-disable

class PackSimSimulation {
 public:
  explicit PackSimSimulation(packsim::thrift::Simulation* simCfg);
  ~PackSimSimulation();

  PackSimSimulation(const PackSimSimulation&) = delete;
  PackSimSimulation& operator=(const PackSimSimulation&) = delete;
  PackSimSimulation(PackSimSimulation&&) = delete;
  PackSimSimulation& operator=(PackSimSimulation&&) = delete;

  void buildWorkloadRanks();

  Scheduler* getScheduler() noexcept {
    return &scheduler_;
  }

  FlexTopology* getFlexTopology() noexcept {
    return &flexTopo_;
  }

  void workloadRankComplete(const FlexDeviceId& id);

  bool isWorkloadComplete() const noexcept;

  void run();

  void verifySimulationMetrics();

  uint64_t totalBytesRcvd() const;
  uint64_t totalBytesRcvdExpected() const;
  WorkloadNode* getEndpoint(const FlexDeviceId& idx);

  packsim::thrift::SimulationResult getThriftResult();

  bool shouldOutputResults() const noexcept;

  bool finished() const noexcept {
    return complete_;
  }

 private:
  /**
   * Body of the progress thread `run()` starts: logs progress every
   * `--packsim_report_interval_sec`, reports to the event sink every
   * `--packsim_log_progress_sec`, checks for a stall when
   * `--packsim_detect_stall` is set, and stops the run once it passes
   * `--packsim_max_wall_time_sec`.
   *
   * @param stopToken    Requested by `run()` as soon as the event loop
   *                     returns, which wakes the thread from its wait at once.
   */
  void logProgress(const std::stop_token& stopToken);
  std::string getTraceEventsJson() const;
  void getWorkloadResults(packsim::thrift::SimulationResult& res) const;
  Random random_;
  packsim::thrift::Simulation* simCfg_;
  Scheduler scheduler_;
  packsim::thrift::RequestStats reqStats_;
  FlexTopology flexTopo_;

  uint64_t desEvents_{0};

  std::mutex workloadRankMutex_;
  std::unordered_set<FlexDeviceId> incompleteWorkloadRanks_;
  Time lastRankComplete_;

  std::chrono::time_point<std::chrono::system_clock> startTime_;
  std::chrono::time_point<std::chrono::system_clock> endTime_;
  std::jthread progressThread_;
  std::atomic_bool complete_{false};

  bool thriftResultConsumed_{false};

  const std::unique_ptr<DependencyInfos> depInfos_;
  std::unique_ptr<GlobalDagCoordinator> globalDagCoordinator_;
  // @oss-disable

  // Stall detection state for logProgress().
  uint32_t stallCount_{0};
  uint64_t prevBytesRcvd_{0};
  // Set by checkForStall() on the progress thread; read on the simulation
  // thread after scheduler_.run() returns to trigger the state dump there.
  std::atomic_bool stalled_{false};

  // Returns true if the simulation was stopped due to a stall.
  bool checkForStall(
      uint64_t bytesRcvd,
      uint64_t bytesRcvdExpected,
      double completionFraction);

  // Set by checkWallTimeLimit() on the progress thread; read on the simulation
  // thread after scheduler_.run() returns to fail the run there.
  std::atomic_bool timedOut_{false};

  /**
   * Stop the simulation once its event loop has run longer than
   * `--packsim_max_wall_time_sec`. A limit of 0 never stops it.
   *
   * @param elapsed               Wall-clock time since the event loop started.
   * @param completionFraction    Share of the expected bytes delivered, e.g.
   *                              0.412, for the log line.
   *
   * @return                      True if the simulation was stopped.
   */
  bool checkWallTimeLimit(
      std::chrono::duration<double> elapsed,
      double completionFraction);
  /**
   * Ask every endpoint's transport to log its worst incomplete QP
   * (Transport::logWorstIncompleteQP()). Must run on the simulation thread
   * after scheduler_.run() returns, never from the progress thread, since it
   * walks live transport state the event loop mutates.
   */
  void dumpIncompleteState();

  // =============== Friend Tests ===============
  FOLLY_GTEST_FRIEND_TEST(
      PackSimSimulationTest,
      TestIncompleteWorkloadsLogWarning);
  FOLLY_GTEST_FRIEND_TEST(
      CheckForStallTest,
      TriggersAfterThreeZeroProgressIntervals);
  FOLLY_GTEST_FRIEND_TEST(CheckForStallTest, CounterResetsOnRenewedProgress);
  FOLLY_GTEST_FRIEND_TEST(CheckForStallTest, IgnoredWhenNoBytesExpected);
  FOLLY_GTEST_FRIEND_TEST(CheckForStallTest, TriggersWhenNoBytesEverDelivered);
  FOLLY_GTEST_FRIEND_TEST(CheckWallTimeLimitTest, NeverStopsWithoutALimit);
  FOLLY_GTEST_FRIEND_TEST(CheckWallTimeLimitTest, DoesNotStopBeforeTheLimit);
  FOLLY_GTEST_FRIEND_TEST(CheckWallTimeLimitTest, StopsOnceTheLimitIsReached);
};

} // namespace packsim
