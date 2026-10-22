/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/portability/GFlags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/endpoint/runtime/dependency-info-builder.h"
#include "packsim/endpoint/runtime/dependency-tracker.h"
#include "packsim/endpoint/runtime/workload-node.h"
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/random.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/simulator/packsim-simulation.h"
#include "packsim/topology/flex-topology.h"
#include "packsim/util/builders.h"

DECLARE_uint64(packsim_max_wall_time_sec);
DECLARE_uint64(packsim_report_interval_sec);

using ::testing::HasSubstr;
using ::testing::Pair;
using ::testing::ThrowsMessage;
using ::testing::UnorderedElementsAre;

namespace packsim {

TEST(PackSimSimulationTest, TestExtractDependencyInfosDagsWithRails) {
  packsim::thrift::Simulation simCfg;

  uint32_t internalId = 0;

  for (int workloadId = 0; workloadId < 2; ++workloadId) {
    for (int rail = 0; rail < 2; ++rail) {
      packsim::thrift::OriginalWorkloadInfo info;
      info.originalWorkloadName() = fmt::format("workload{}", workloadId);
      info.internalWorkloadId() = internalId++;
      simCfg.newWorkloadSchedule()->workloadNameInfos()->emplace(
          fmt::format("workload{}-rail{}", workloadId, rail), std::move(info));
    }
  }

  for (int workloadId = 2; workloadId < 11; ++workloadId) {
    packsim::thrift::OriginalWorkloadInfo info;
    info.originalWorkloadName() = fmt::format("workload{}", workloadId);
    info.internalWorkloadId() = internalId++;
    simCfg.newWorkloadSchedule()->workloadNameInfos()->emplace(
        fmt::format("workload{}", workloadId), std::move(info));
  }

  simCfg.newWorkloadSchedule()->workloadDags() = {};

  {
    packsim::workload::thrift::WorkloadDag dag;
    dag.workloadDependencies() = {{"workload2", {"workload0", "workload1"}}};
    simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(std::move(dag));
  }
  {
    packsim::workload::thrift::WorkloadDag dag;
    dag.workloadDependencies() = {
        {"workload6", {"workload3", "workload4", "workload5"}},
        {"workload9", {"workload7", "workload8", "workload6"}}};
    simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(std::move(dag));
  }
  {
    packsim::workload::thrift::WorkloadDag dag;
    dag.workloadDependencies() = {{"workload10", {}}};
    simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(std::move(dag));
  }

  simCfg.newWorkloadSchedule()->minIterationsPerDag() = 10;

  auto depInfos = DependencyInfoBuilder::build(simCfg);

  EXPECT_THAT(
      depInfos->workloadIdDependencies,
      UnorderedElementsAre(
          Pair(4, UnorderedElementsAre(0, 1, 2, 3)),
          Pair(8, UnorderedElementsAre(5, 6, 7)),
          Pair(11, UnorderedElementsAre(8, 9, 10))));

  EXPECT_THAT(
      depInfos->workloadIdDependents,
      UnorderedElementsAre(
          Pair(0, UnorderedElementsAre(4)),
          Pair(1, UnorderedElementsAre(4)),
          Pair(2, UnorderedElementsAre(4)),
          Pair(3, UnorderedElementsAre(4)),
          Pair(5, UnorderedElementsAre(8)),
          Pair(6, UnorderedElementsAre(8)),
          Pair(7, UnorderedElementsAre(8)),
          Pair(8, UnorderedElementsAre(11)),
          Pair(9, UnorderedElementsAre(11)),
          Pair(10, UnorderedElementsAre(11))));

  EXPECT_THAT(
      depInfos->dagList.at(0).allWorkloadIds,
      UnorderedElementsAre(0, 1, 2, 3, 4));

  EXPECT_THAT(
      depInfos->dagList.at(0).startingWorkloadIds,
      UnorderedElementsAre(0, 1, 2, 3));

  EXPECT_THAT(
      depInfos->dagList.at(0).finishingWorkloadIds, UnorderedElementsAre(4));

  EXPECT_THAT(
      depInfos->dagList.at(1).allWorkloadIds,
      UnorderedElementsAre(5, 6, 7, 8, 9, 10, 11));

  EXPECT_THAT(
      depInfos->dagList.at(1).startingWorkloadIds,
      UnorderedElementsAre(5, 6, 7, 9, 10));

  EXPECT_THAT(
      depInfos->dagList.at(1).finishingWorkloadIds, UnorderedElementsAre(11));

  EXPECT_THAT(depInfos->dagList.at(2).allWorkloadIds, UnorderedElementsAre(12));

  EXPECT_THAT(
      depInfos->dagList.at(2).startingWorkloadIds, UnorderedElementsAre(12));

  EXPECT_THAT(
      depInfos->dagList.at(2).finishingWorkloadIds, UnorderedElementsAre(12));

  EXPECT_THAT(
      depInfos->workloadToDagId,
      UnorderedElementsAre(
          Pair(0, 0),
          Pair(1, 0),
          Pair(2, 0),
          Pair(3, 0),
          Pair(4, 0),
          Pair(5, 1),
          Pair(6, 1),
          Pair(7, 1),
          Pair(8, 1),
          Pair(9, 1),
          Pair(10, 1),
          Pair(11, 1),
          Pair(12, 2)));

  ASSERT_TRUE(depInfos->minIterationsPerDag.has_value());
  EXPECT_EQ(depInfos->minIterationsPerDag.value(), 10);
}

TEST(PackSimSimulationTest, TestExtractDependencyInfosInvalidDagWorkloadName) {
  packsim::thrift::Simulation simCfg;

  uint32_t internalId = 0;

  for (int workloadId = 0; workloadId < 4; ++workloadId) {
    packsim::thrift::OriginalWorkloadInfo info;
    info.originalWorkloadName() = fmt::format("workload{}", workloadId);
    info.internalWorkloadId() = internalId++;
    simCfg.newWorkloadSchedule()->workloadNameInfos()->emplace(
        fmt::format("workload{}", workloadId), std::move(info));
  }

  {
    simCfg.newWorkloadSchedule()->workloadDags() = {};

    {
      packsim::workload::thrift::WorkloadDag dag;
      dag.workloadDependencies() = {{"workload6", {"workload0", "workload1"}}};
      simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(
          std::move(dag));
    }

    EXPECT_THAT(
        [&]() { DependencyInfoBuilder::build(simCfg); },
        ThrowsMessage<std::invalid_argument>(HasSubstr(
            "Workload name workload6 used in DAG is missing from input workloads list.")));
  }

  {
    simCfg.newWorkloadSchedule()->workloadDags() = {};

    {
      packsim::workload::thrift::WorkloadDag dag;
      dag.workloadDependencies() = {{"workload2", {"workload7", "workload1"}}};
      simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(
          std::move(dag));
    }

    EXPECT_THAT(
        [&]() { DependencyInfoBuilder::build(simCfg); },
        ThrowsMessage<std::invalid_argument>(HasSubstr(
            "Workload name workload7 used in DAG is missing from input workloads list.")));
  }
}

TEST(
    PackSimSimulationTest,
    TestExtractDependencyInfosWorkloadUsedInMultipleDags) {
  packsim::thrift::Simulation simCfg;

  uint32_t internalId = 0;

  for (int workloadId = 0; workloadId < 4; ++workloadId) {
    packsim::thrift::OriginalWorkloadInfo info;
    info.originalWorkloadName() = fmt::format("workload{}", workloadId);
    info.internalWorkloadId() = internalId++;
    simCfg.newWorkloadSchedule()->workloadNameInfos()->emplace(
        fmt::format("workload{}", workloadId), std::move(info));
  }

  {
    simCfg.newWorkloadSchedule()->workloadDags() = {};

    {
      packsim::workload::thrift::WorkloadDag dag;
      dag.workloadDependencies() = {{"workload2", {"workload0", "workload1"}}};
      simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(
          std::move(dag));
    }
    {
      packsim::workload::thrift::WorkloadDag dag;
      dag.workloadDependencies() = {{"workload3", {"workload2"}}};
      simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(
          std::move(dag));
    }

    EXPECT_THAT(
        [&]() { DependencyInfoBuilder::build(simCfg); },
        ThrowsMessage<std::invalid_argument>(HasSubstr(
            "Workload ID 2 used in DAG is already used by another DAG.")));
  }

  {
    simCfg.newWorkloadSchedule()->workloadDags() = {};

    {
      packsim::workload::thrift::WorkloadDag dag;
      dag.workloadDependencies() = {{"workload2", {"workload0", "workload1"}}};
      simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(
          std::move(dag));
    }
    {
      packsim::workload::thrift::WorkloadDag dag;
      dag.workloadDependencies() = {{"workload2", {"workload3"}}};
      simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(
          std::move(dag));
    }

    EXPECT_THAT(
        [&]() { DependencyInfoBuilder::build(simCfg); },
        ThrowsMessage<std::invalid_argument>(HasSubstr(
            "Workload ID 2 used in DAG is already used by another DAG.")));
  }
}

TEST(
    PackSimSimulationTest,
    TestVerifySimulationMetricsMissingBytesNotAllowIncompleteWorkloads) {
  // With allowIncompleteWorkloads=false, should throw when missing bytes
  auto simCfg = packsim::buildFourZoneClosConfig(2);

  simCfg.allowIncompleteWorkloads() = false;
  PackSimSimulation sim(&simCfg);

  if (!sim.getFlexTopology()->simCtxs_.empty()) {
    sim.getFlexTopology()->simCtxs_[0]->dataRcvd = 1000;
    sim.getFlexTopology()->simCtxs_[0]->dataRcvdExpected = 2000;
  }

  ASSERT_THAT(
      [&]() { sim.verifySimulationMetrics(); },
      ThrowsMessage<std::runtime_error>(HasSubstr("Missing bytes: 1000")));
}

TEST(
    PackSimSimulationTest,
    TestVerifySimulationMetricsMissingBytesAllowIncompleteWorkloads) {
  // With allowIncompleteWorkloads=true, should NOT throw when missing bytes
  auto simCfg = packsim::buildFourZoneClosConfig(2);

  simCfg.allowIncompleteWorkloads() = true;
  PackSimSimulation sim(&simCfg);

  if (!sim.getFlexTopology()->simCtxs_.empty()) {
    sim.getFlexTopology()->simCtxs_[0]->dataRcvd = 1000;
    sim.getFlexTopology()->simCtxs_[0]->dataRcvdExpected = 2000;
  }

  EXPECT_NO_THROW({ sim.verifySimulationMetrics(); });
}

TEST(
    PackSimSimulationTest,
    TestVerifySimulationMetricsExtraBytesNotAllowIncompleteWorkloads) {
  // With allowIncompleteWorkloads=false, should throw when extra bytes
  auto simCfg = packsim::buildFourZoneClosConfig(2);

  simCfg.allowIncompleteWorkloads() = false;
  PackSimSimulation sim(&simCfg);

  if (!sim.getFlexTopology()->simCtxs_.empty()) {
    sim.getFlexTopology()->simCtxs_[0]->dataRcvd = 3000;
    sim.getFlexTopology()->simCtxs_[0]->dataRcvdExpected = 2000;
  }

  ASSERT_THAT(
      [&]() { sim.verifySimulationMetrics(); },
      ThrowsMessage<std::runtime_error>(HasSubstr("Extra bytes: 1000")));
}

TEST(
    PackSimSimulationTest,
    TestVerifySimulationMetricsExtraBytesAllowIncompleteWorkloads) {
  // With allowIncompleteWorkloads=true, should NOT throw when extra bytes
  auto simCfg = packsim::buildFourZoneClosConfig(2);

  simCfg.allowIncompleteWorkloads() = true;
  PackSimSimulation sim(&simCfg);

  if (!sim.getFlexTopology()->simCtxs_.empty()) {
    sim.getFlexTopology()->simCtxs_[0]->dataRcvd = 3000;
    sim.getFlexTopology()->simCtxs_[0]->dataRcvdExpected = 2000;
  }

  EXPECT_NO_THROW({ sim.verifySimulationMetrics(); });
}

TEST(PackSimSimulationTest, TestVerifySimulationMetricsCompleteWorkloads) {
  auto simCfg = packsim::buildFourZoneClosConfig(2);
  // No mismatch, should not throw regardless of flag

  simCfg.allowIncompleteWorkloads() = false;
  PackSimSimulation sim(&simCfg);

  if (!sim.getFlexTopology()->simCtxs_.empty()) {
    sim.getFlexTopology()->simCtxs_[0]->dataRcvd = 1000;
    sim.getFlexTopology()->simCtxs_[0]->dataRcvdExpected = 1000;
  }

  EXPECT_NO_THROW({ sim.verifySimulationMetrics(); });
}

TEST(PackSimSimulationTest, TestIncompleteWorkloadsLogWarning) {
  Random random;

  auto simCfg = packsim::buildFourZoneClosWorkloadSim(
      random,
      {
          .workloadType = packsim::thrift::WorkloadType::ALL_TO_ALL,
          .numAccel = 128,
          .sizeBytes = 1024,
          .numCopies = 1,
          .unit = packsim::thrift::SchedulingUnit::ENDPOINT,
          .method = packsim::thrift::SchedulingMethod::SEQUENTIAL,
      });

  simCfg.allowIncompleteWorkloads() = true;
  PackSimSimulation sim(&simCfg);

  ASSERT_FALSE(sim.isWorkloadComplete());
  EXPECT_NO_THROW({ sim.verifySimulationMetrics(); });
}

TEST(PackSimSimulationTest, TestIncompleteWorkloadsThrow) {
  Random random;

  auto simCfg = packsim::buildFourZoneClosWorkloadSim(
      random,
      {
          .workloadType = packsim::thrift::WorkloadType::ALL_TO_ALL,
          .numAccel = 128,
          .sizeBytes = 1024,
          .numCopies = 1,
          .unit = packsim::thrift::SchedulingUnit::ENDPOINT,
          .method = packsim::thrift::SchedulingMethod::SEQUENTIAL,
      });

  simCfg.allowIncompleteWorkloads() = false;
  PackSimSimulation sim(&simCfg);

  ASSERT_FALSE(sim.isWorkloadComplete());
  ASSERT_THAT(
      [&]() { sim.verifySimulationMetrics(); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("128 workload ranks didn't complete")));
}

// The progress thread must not hold up the end of a run: with a report
// interval far longer than the simulation, running it and tearing it down
// still finish promptly instead of waiting for the next report.
TEST(PackSimSimulationTest, RunAndTeardownDoNotWaitForTheNextProgressReport) {
  gflags::FlagSaver flagSaver;
  constexpr uint64_t kReportIntervalSec = 3600;
  FLAGS_packsim_report_interval_sec = kReportIntervalSec;
  auto simCfg = packsim::buildFourZoneClosConfig(2);

  const auto start = std::chrono::steady_clock::now();
  {
    PackSimSimulation sim(&simCfg);
    auto& simCtxs = sim.getFlexTopology()->simCtxs_;
    ASSERT_FALSE(simCtxs.empty());
    // Keep the event loop busy long enough that the progress thread is
    // already waiting for its first report when run() returns. A run that
    // ends before the thread first looks never exercises the wait at all.
    simCtxs.front()->schedule(Time::fromNs(1), [] {
      /* sleep override */ std::this_thread::sleep_for(
          std::chrono::milliseconds(500));
    });
    sim.run();
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;

  // A bound far above the run itself and far below the interval, so neither
  // a slow machine nor a sanitizer build can make it flaky.
  constexpr std::chrono::seconds kFarBelowTheInterval{120};
  EXPECT_LT(elapsed, kFarBelowTheInterval)
      << "run() plus teardown took "
      << std::chrono::duration_cast<std::chrono::seconds>(elapsed).count()
      << " s with a " << kReportIntervalSec << " s report interval";
}

// The CPU load stats read every context the topology owns, so the simulation
// prints them before any of its members is destroyed.
TEST(PackSimSimulationTest, PrintsPdesCpuLoadWhenDestroyed) {
  gflags::FlagSaver flagSaver;
  ASSERT_FALSE(
      gflags::SetCommandLineOption("pdes_print_cpu_load", "true").empty());
  auto simCfg = packsim::buildFourZoneClosConfig(2);
  auto sim = std::make_unique<PackSimSimulation>(&simCfg);
  auto& simCtxs = sim->getFlexTopology()->simCtxs_;
  ASSERT_FALSE(simCtxs.empty());
  simCtxs.front()->schedule(Time::fromNs(1), [] {});
  sim->run();

  testing::internal::CaptureStdout();
  sim.reset();
  EXPECT_THAT(
      testing::internal::GetCapturedStdout(),
      HasSubstr("CPU Time for thread 0 contexts: "));
}

TEST(PackSimSimulationTest, TotalBytesRcvdSumsAcrossSimContexts) {
  auto simCfg = packsim::buildFourZoneClosConfig(2);
  PackSimSimulation sim(&simCfg);

  auto& simCtxs = sim.getFlexTopology()->simCtxs_;
  ASSERT_GE(simCtxs.size(), 2u)
      << "need multiple sim contexts to exercise the cross-context sum";
  uint64_t expected = 0;
  for (size_t i = 0; i < simCtxs.size(); ++i) {
    const uint64_t bytes = (i + 1) * 100;
    simCtxs[i]->dataRcvd = bytes;
    expected += bytes;
  }
  EXPECT_EQ(sim.totalBytesRcvd(), expected);
}

TEST(PackSimSimulationTest, TotalBytesRcvdExpectedSumsAcrossSimContexts) {
  auto simCfg = packsim::buildFourZoneClosConfig(2);
  PackSimSimulation sim(&simCfg);

  auto& simCtxs = sim.getFlexTopology()->simCtxs_;
  ASSERT_GE(simCtxs.size(), 2u)
      << "need multiple sim contexts to exercise the cross-context sum";
  uint64_t expected = 0;
  for (size_t i = 0; i < simCtxs.size(); ++i) {
    const uint64_t bytes = (i + 1) * 1000;
    simCtxs[i]->dataRcvdExpected = bytes;
    expected += bytes;
  }
  EXPECT_EQ(sim.totalBytesRcvdExpected(), expected);
}

TEST(PackSimSimulationTest, WorkloadRankCompleteEmptiesIncompleteSet) {
  Random random;
  auto simCfg = packsim::buildFourZoneClosWorkloadSim(
      random,
      {
          .workloadType = packsim::thrift::WorkloadType::ALL_TO_ALL,
          .numAccel = 128,
          .sizeBytes = 1024,
          .numCopies = 1,
          .unit = packsim::thrift::SchedulingUnit::ENDPOINT,
          .method = packsim::thrift::SchedulingMethod::SEQUENTIAL,
      });
  simCfg.allowIncompleteWorkloads() = true;
  PackSimSimulation sim(&simCfg);

  ASSERT_FALSE(sim.isWorkloadComplete())
      << "freshly built workload ranks start incomplete";

  // Marking every workload node's rank complete must drain the incomplete set.
  for (const auto& node : sim.getFlexTopology()->getAllWorkloadNodes()) {
    sim.workloadRankComplete(node->getFlexId());
  }

  EXPECT_TRUE(sim.isWorkloadComplete());
}

TEST(PackSimSimulationTest, RejectsALinkWithZeroDelay) {
  auto simCfg = packsim::buildFourZoneClosConfig(2);
  auto& links = *simCfg.flex_topology()->links();
  ASSERT_FALSE(links.empty());
  auto& [linkName, link] = *links.begin();
  link.delayConfig()->delayNs() = 0;
  const std::string expected = fmt::format(
      "FlexLink '{}' has delayConfig.delayNs 0; every link needs a delay of "
      "at least 1 ns",
      linkName);

  EXPECT_THAT(
      [&]() { PackSimSimulation sim(&simCfg); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(expected)));
}

TEST(PackSimSimulationTest, RejectsAZeroConditionalTraceInterval) {
  auto simCfg = packsim::buildFourZoneClosConfig(2);
  (*simCfg.conditionalTraceIntervalsNs())["qp-trace"] = 0;

  EXPECT_THAT(
      [&]() { PackSimSimulation sim(&simCfg); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "Simulation.conditionalTraceIntervalsNs['qp-trace'] is 0")));
}

// Each RoCE transport checks its NIC config's DCQCN parameters when it is
// built, so a bad one fails here, with no message sent; a DCQCN controller is
// only built at a queue pair's first message.
TEST(PackSimSimulationTest, RejectsADcqcnMinimumRateAboveTheNicSpeed) {
  // Twice the 100 Gbps NIC speed of the four-zone Clos fabric.
  constexpr double kMinRateAboveTheNicSpeedMbps = 200'000;
  // Two racks of 16 accelerators per zone, so the fabric has endpoints.
  constexpr uint32_t kNumAccel = 128;
  auto simCfg = packsim::buildFourZoneClosConfig({
      .numAccel = kNumAccel,
      .congestionControl = packsim::RoceCongestionControl::DCQCN,
  });
  for (auto& [nicName, nicCfg] : *simCfg.nicConfigMap()) {
    nicCfg.dcqcnCfg()->rpMinRate() = kMinRateAboveTheNicSpeedMbps;
  }

  EXPECT_THAT(
      [&]() { PackSimSimulation sim(&simCfg); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "DcqcnConfig.rpMinRate 200000 Mbps must be at least 0.000001 Mbps "
          "(1 bps) and at most the NIC speed of 100000 Mbps")));
}

class CheckForStallTest : public ::testing::Test {
 protected:
  packsim::thrift::Simulation simCfg_ = packsim::buildFourZoneClosConfig(2);
  PackSimSimulation sim_{&simCfg_};
};

TEST_F(CheckForStallTest, TriggersAfterThreeZeroProgressIntervals) {
  EXPECT_FALSE(sim_.checkForStall(
      /*bytesRcvd=*/1000,
      /*bytesRcvdExpected=*/2000,
      /*completionFraction=*/0.5))
      << "delivering new bytes is forward progress, not a stall";
  EXPECT_FALSE(sim_.checkForStall(1000, 2000, 0.5))
      << "one zero-progress interval alone is not yet a stall";
  EXPECT_FALSE(sim_.checkForStall(1000, 2000, 0.5))
      << "two zero-progress intervals is not yet a stall";
  EXPECT_TRUE(sim_.checkForStall(1000, 2000, 0.5))
      << "three consecutive zero-progress intervals must trip stall detection "
         "so a deadlocked simulation stops instead of running to CI timeout";
}

TEST_F(CheckForStallTest, CounterResetsOnRenewedProgress) {
  EXPECT_FALSE(sim_.checkForStall(1000, 2000, 0.5));
  EXPECT_FALSE(sim_.checkForStall(1000, 2000, 0.5));
  EXPECT_FALSE(sim_.checkForStall(1500, 2000, 0.75))
      << "renewed delivery must reset the stall counter so a brief pause "
         "followed by progress is never misread as a stall";
  EXPECT_FALSE(sim_.checkForStall(1500, 2000, 0.75))
      << "counting must restart from the reset, not the pre-progress run";
  EXPECT_FALSE(sim_.checkForStall(1500, 2000, 0.75))
      << "two zero-progress intervals after the reset is not yet a stall";
  EXPECT_TRUE(sim_.checkForStall(1500, 2000, 0.75))
      << "three zero-progress intervals after the reset must trip detection";
}

TEST_F(CheckForStallTest, IgnoredWhenNoBytesExpected) {
  for (int i = 0; i < 5; ++i) {
    EXPECT_FALSE(sim_.checkForStall(
        /*bytesRcvd=*/0, /*bytesRcvdExpected=*/0, /*completionFraction=*/1.0))
        << "a simulation expecting no bytes (e.g. receive-only endpoint) must "
           "never be flagged as stalled regardless of zero delivery";
  }
}

TEST_F(CheckForStallTest, TriggersWhenNoBytesEverDelivered) {
  // A run that never delivers a byte (total deadlock) must still trip, but only
  // after a grace longer than the mid-stream threshold so a run merely slow to
  // deliver its first byte at startup is not misflagged.
  constexpr int kMaxIntervals = 100;
  int intervals = 0;
  bool tripped = false;
  for (; intervals < kMaxIntervals && !tripped; ++intervals) {
    tripped = sim_.checkForStall(
        /*bytesRcvd=*/0,
        /*bytesRcvdExpected=*/2000,
        /*completionFraction=*/0.0);
  }
  EXPECT_TRUE(tripped)
      << "a run that never delivers a byte must trip stall detection so a fully "
         "deadlocked simulation stops instead of hanging to the CI timeout";
  EXPECT_GT(intervals, 3)
      << "the no-delivery grace must exceed the mid-stream stall threshold so a "
         "run still waiting for its first byte at startup is not prematurely "
         "flagged as deadlocked";
}

class CheckWallTimeLimitTest : public ::testing::Test {
 protected:
  gflags::FlagSaver flagSaver_;
  packsim::thrift::Simulation simCfg_ = packsim::buildFourZoneClosConfig(2);
  PackSimSimulation sim_{&simCfg_};
};

TEST_F(CheckWallTimeLimitTest, NeverStopsWithoutALimit) {
  FLAGS_packsim_max_wall_time_sec = 0;

  EXPECT_FALSE(sim_.checkWallTimeLimit(std::chrono::hours(1000), 0.5))
      << "a limit of 0 means none, however long the run has gone";
  EXPECT_FALSE(sim_.timedOut_);
}

TEST_F(CheckWallTimeLimitTest, DoesNotStopBeforeTheLimit) {
  FLAGS_packsim_max_wall_time_sec = 60;

  EXPECT_FALSE(
      sim_.checkWallTimeLimit(std::chrono::duration<double>(59.9), 0.5));
  EXPECT_FALSE(sim_.timedOut_);
}

TEST_F(CheckWallTimeLimitTest, StopsOnceTheLimitIsReached) {
  FLAGS_packsim_max_wall_time_sec = 60;

  EXPECT_TRUE(sim_.checkWallTimeLimit(std::chrono::duration<double>(60), 0.5));
  EXPECT_TRUE(sim_.timedOut_)
      << "run() reads this flag to fail the run once the event loop returns";
}

// End to end: a run that never ends on its own is stopped by the limit, and
// fails with an error naming it.
TEST(PackSimSimulationTest, RunPastTheWallTimeLimitFailsNamingTheLimit) {
  gflags::FlagSaver flagSaver;
  FLAGS_packsim_report_interval_sec = 1;
  FLAGS_packsim_max_wall_time_sec = 1;
  auto simCfg = packsim::buildFourZoneClosConfig(2);
  PackSimSimulation sim(&simCfg);
  auto& simCtxs = sim.getFlexTopology()->simCtxs_;
  ASSERT_FALSE(simCtxs.empty());
  SimContext* const ctx = simCtxs.front().get();

  // An event that keeps rescheduling itself, like a timer that outlives its
  // workload. Capped at 30 s, so a limit that never fires fails the test
  // instead of hanging it.
  constexpr int kMaxTicks = 600;
  int ticks = 0;
  std::function<void()> tick = [&]() {
    /* sleep override */ std::this_thread::sleep_for(
        std::chrono::milliseconds(50));
    if (++ticks < kMaxTicks) {
      ctx->schedule(Time::fromNs(1000), tick);
    }
  };
  ctx->schedule(Time::fromNs(1), tick);

  EXPECT_THAT(
      [&]() { sim.run(); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Simulation exceeded its wall-clock limit of 1 s")));
  EXPECT_LT(ticks, kMaxTicks) << "the limit, not the cap, ended the run";
}

} // namespace packsim
