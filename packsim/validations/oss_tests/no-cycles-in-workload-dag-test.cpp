/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "packsim/validations/no-cycles-in-workload-dag.h"

namespace packsim::details {

TEST(NoCyclesInWorkloadDagTest, EmptySimCfg) {
  thrift::Simulation simCfg;

  auto r = NoCyclesInWorkloadDag().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(NoCyclesInWorkloadDagTest, Success) {
  thrift::Simulation simCfg;

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

  auto r = NoCyclesInWorkloadDag().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(NoCyclesInWorkloadDagTest, HasCycles) {
  thrift::Simulation simCfg;

  simCfg.newWorkloadSchedule()->workloadDags() = {};

  {
    packsim::workload::thrift::WorkloadDag dag;
    dag.workloadDependencies() = {{"workload2", {"workload0", "workload1"}}};
    simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(std::move(dag));
  }
  {
    packsim::workload::thrift::WorkloadDag dag;
    dag.workloadDependencies() = {
        {"w03", {"w04", "w05", "w06"}},
        {"w06", {"w07", "w08", "w09"}},
        {"w07", {"w10", "w11", "w12"}},
        {"w11", {"w13", "w14", "w15"}},
        {"w14", {"w06", "w16", "w17"}}};
    simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(std::move(dag));
  }
  {
    packsim::workload::thrift::WorkloadDag dag;
    dag.workloadDependencies() = {
        {"w11", {"w13", "w14", "w15"}}, {"w14", {"w14", "w16", "w17"}}};
    simCfg.newWorkloadSchedule()->workloadDags()->emplace_back(std::move(dag));
  }

  auto r = NoCyclesInWorkloadDag().validate(simCfg, /*ignoreWarnings=*/false);
  ASSERT_TRUE(r.has_value());

  EXPECT_EQ(
      r->errors,
      (std::set<std::string>{
          "Cycle detected in 1st dag: w06->w14->w11->w07->w06",
          "Cycle detected in 2nd dag: w14->w14"}));
}

} // namespace packsim::details
