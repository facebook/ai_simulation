/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/random.h"
#include "packsim/simulator/packsim-simulation.h"
#include "packsim/util/builders.h"

namespace packsim {

namespace {

// Ranks in each of the incasts buildTwoIncastSim() places.
constexpr uint32_t kRanksPerIncast = 16;
// Bytes each incast source sends to rank 0.
constexpr uint32_t kIncastBytes = 4096;

// Two incasts, `TestWorkload_0` and `TestWorkload_1`, on disjoint accelerators
// of a 32-accelerator four-zone fabric.
packsim::thrift::Simulation buildTwoIncastSim() {
  Random random;
  return buildFourZoneClosWorkloadSim(
      random,
      {
          .workloadType = packsim::thrift::WorkloadType::INCAST,
          .numAccel = 2 * kRanksPerIncast,
          .sizeBytes = kIncastBytes,
          .numCopies = 2,
          .numChunks = 1,
      });
}

packsim::workload::thrift::WorkloadDag makeDag(
    std::map<std::string, std::set<std::string>> dependencies) {
  packsim::workload::thrift::WorkloadDag dag;
  dag.workloadDependencies() = std::move(dependencies);
  return dag;
}

// Runs both incasts as one DAG, the second after the first, with per-message
// tracking on, repeating the DAG at least `minIterations` times.
packsim::thrift::SimulationResult runIncastDag(const uint32_t minIterations) {
  auto simCfg = buildTwoIncastSim();
  auto& schedule = *simCfg.newWorkloadSchedule();
  auto& workloads = *schedule.workloads();
  // A node starts a dependent rank only once its dependency completes on the
  // same node, so the second incast takes the first one's accelerators.
  workloads.at(1).rankToId() = *workloads.at(0).rankToId();
  schedule.workloadDags().ensure().emplace_back(makeDag(
      {{"TestWorkload_0", {}}, {"TestWorkload_1", {"TestWorkload_0"}}}));
  schedule.minIterationsPerDag() = minIterations;
  simCfg.networkLevelFeatureConfig().ensure().enablePerMessageTracking() = true;

  PackSimSimulation sim(&simCfg);
  sim.run();
  return sim.getThriftResult();
}

} // namespace

TEST(DagIterationsTest, MinIterationsPerDagRunsEveryIterationOfTheDag) {
  const auto once = runIncastDag(/* minIterations= */ 1);
  const auto twice = runIncastDag(/* minIterations= */ 2);

  EXPECT_EQ(
      *twice.stats()->totalBytesDelivered(),
      2 * *once.stats()->totalBytesDelivered());
  const auto& workloads = *twice.workloads();
  EXPECT_EQ(workloads.size(), 4u);
  for (const auto* name :
       {"TestWorkload_0",
        "TestWorkload_1",
        "TestWorkload_0_ITER_2",
        "TestWorkload_1_ITER_2"}) {
    ASSERT_TRUE(workloads.contains(name)) << name << " has no result";
    EXPECT_EQ(workloads.at(name).perRank()->size(), kRanksPerIncast) << name;
  }
  EXPECT_GE(
      *workloads.at("TestWorkload_0_ITER_2").startTimeNs(),
      *workloads.at("TestWorkload_1").endTimeNs())
      << "the second iteration starts only after the whole DAG finished its "
         "first";
}

TEST(DagIterationsTest, PerMessageTrackingMatchesEveryReceiveInEveryIteration) {
  const auto result = runIncastDag(/* minIterations= */ 2);

  std::map<uint64_t, size_t> recordsPerIteration;
  for (const auto& [name, workload] : *result.workloads()) {
    for (const auto& record : *workload.perMessageResults()) {
      EXPECT_EQ(*record.iteration(), *workload.iteration())
          << name << " holds a message from another iteration";
      ++recordsPerIteration[*record.iteration()];
      EXPECT_GE(*record.recvTimeNs(), 0)
          << name << ": message " << *record.messageIndex() << " from rank "
          << *record.srcRank() << " to rank " << *record.dstRank()
          << " in iteration " << *record.iteration() << " was never received";
    }
  }
  ASSERT_EQ(recordsPerIteration.size(), 2u);
  EXPECT_GT(recordsPerIteration.at(1), 0u);
  EXPECT_EQ(recordsPerIteration.at(1), recordsPerIteration.at(2));
}

TEST(DagIterationsTest, FasterDagRepeatsUntilEveryDagReachesMinIterations) {
  auto simCfg = buildTwoIncastSim();
  auto& schedule = *simCfg.newWorkloadSchedule();
  // The second incast moves sixteen times the bytes of the first.
  schedule.workloads()->at(1).config()->incast()->sizeBytes() =
      16 * kIncastBytes;
  auto& dags = schedule.workloadDags().ensure();
  dags.emplace_back(makeDag({{"TestWorkload_0", {}}}));
  dags.emplace_back(makeDag({{"TestWorkload_1", {}}}));
  schedule.minIterationsPerDag() = 1;

  PackSimSimulation sim(&simCfg);
  sim.run();

  const auto result = sim.getThriftResult();
  const auto& workloads = *result.workloads();
  EXPECT_TRUE(workloads.contains("TestWorkload_0_ITER_2"))
      << "the small incast repeats while the large one runs its first "
         "iteration";
  EXPECT_FALSE(workloads.contains("TestWorkload_1_ITER_2"))
      << "no DAG repeats once every DAG has completed minIterationsPerDag";
  for (const auto& [name, workload] : workloads) {
    EXPECT_EQ(workload.perRank()->size(), kRanksPerIncast)
        << name << " did not complete on every rank";
  }
}

} // namespace packsim
