/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/util/simple-json-field-checker.h"

using apache::thrift::SimpleJSONSerializer;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Optional;

namespace packsim::io {

namespace {

// Reads `json` into a config as `deserializeConfig` does, then reports the keys
// reading it dropped.
std::optional<std::vector<std::string>> findUnknownFieldsOf(
    const std::string& json) {
  return SimpleJsonFieldChecker::findUnknownFields(
      json,
      SimpleJSONSerializer::deserialize<packsim::thrift::Simulation>(json));
}

// A config with an optional field, a union arm and a map keyed by structs,
// which Simple JSON writes with objects as keys, so its Simple JSON is not
// standard JSON.
packsim::thrift::Simulation makeConfig() {
  packsim::thrift::RankPair ranks;
  ranks.srcRank() = 0;
  ranks.dstRank() = 1;
  packsim::thrift::NicPair nics;
  nics.srcNicIdx() = 0;
  nics.dstNicIdx() = 2;
  packsim::workload::thrift::QPNPair qpns;
  qpns.local() = 7;
  qpns.remote() = 9;
  packsim::workload::thrift::AllToAll allToAll;
  allToAll.sizeBytes() = 1024;

  packsim::workload::thrift::Workload workload;
  workload.name() = "w";
  workload.config()->allToAll() = std::move(allToAll);
  workload.nicQpnPairMap()[ranks][nics].emplace_back(qpns);

  packsim::thrift::Simulation cfg;
  cfg.networkLevelFeatureConfig().ensure().enablePerMessageTracking() = true;
  cfg.newWorkloadSchedule()->workloads()->emplace_back(std::move(workload));
  return cfg;
}

} // namespace

// Simple JSON reading skips every key the schema does not define, so these pin
// which keys the checker reports and the paths that name them. The unknown
// names are invented so that no version of the schema defines them.

TEST(SimpleJsonFieldCheckerTest, ConfigWrittenFromTheSchemaHasNoUnknownFields) {
  EXPECT_THAT(
      findUnknownFieldsOf(
          SimpleJSONSerializer::serialize<std::string>(makeConfig())),
      Optional(IsEmpty()));
}

// A hand-written config sets only the fields it needs and is laid out freely.
TEST(SimpleJsonFieldCheckerTest, HandWrittenConfigHasNoUnknownFields) {
  EXPECT_THAT(
      findUnknownFieldsOf(R"({
        "name": "clean",
        "random": {"seed": 7},
        "switchConfigMap": {"RTSW": {"pipelineDelayNs": 10}}
      })"),
      Optional(IsEmpty()));
}

TEST(SimpleJsonFieldCheckerTest, ReportsTopLevelKeysTheSchemaDoesNotDefine) {
  EXPECT_THAT(
      findUnknownFieldsOf(
          R"({"name":"x","notASimulationField":{"a":1},"otherUnknownField":3})"),
      Optional(ElementsAre("notASimulationField", "otherUnknownField")));
}

// A path names a map entry by its key and a list element by its index. A union
// arm the schema does not define is reported like any other key: the union
// reads back empty.
TEST(SimpleJsonFieldCheckerTest, ReportsNestedKeysByTheirPath) {
  EXPECT_THAT(
      findUnknownFieldsOf(R"({
        "switchConfigMap": {"RTSW": {"notASwitchConfigField": 1}},
        "newWorkloadSchedule": {"workloads": [
          {
            "name": "a",
            "config": {
              "allToAll": {"sizeBytes": 8, "notAnAllToAllField": 1}
            }
          },
          {"name": "b", "config": {"notAWorkloadType": {}}}
        ]}
      })"),
      Optional(ElementsAre(
          "newWorkloadSchedule.workloads[0].config.allToAll.notAnAllToAllField",
          "newWorkloadSchedule.workloads[1].config.notAWorkloadType",
          "switchConfigMap.RTSW.notASwitchConfigField")));
}

// The reader skips a key whose value is null whether or not the schema defines
// it, so a null sets nothing and is not reported.
TEST(SimpleJsonFieldCheckerTest, IgnoresKeysWhoseValueIsNull) {
  EXPECT_THAT(
      findUnknownFieldsOf(
          R"({"networkLevelFeatureConfig":null,"notASimulationField":null})"),
      Optional(IsEmpty()));
}

// Simple JSON as `packsim` writes it is not standard JSON when a map is keyed
// by structs, and is still checked.
TEST(SimpleJsonFieldCheckerTest, ChecksSimpleJsonWithStructKeyedMaps) {
  std::string json = SimpleJSONSerializer::serialize<std::string>(makeConfig());
  const std::string workloadName = R"("name":"w")";
  const auto at = json.find(workloadName);
  ASSERT_NE(at, std::string::npos);
  json.insert(at + workloadName.size(), R"(,"notAWorkloadField":1)");

  EXPECT_THAT(
      findUnknownFieldsOf(json),
      Optional(
          ElementsAre("newWorkloadSchedule.workloads[0].notAWorkloadField")));
}

TEST(SimpleJsonFieldCheckerTest, InputThatIsNotJsonCannotBeChecked) {
  EXPECT_FALSE(
      SimpleJsonFieldChecker::findUnknownFields(
          "not json", packsim::thrift::Simulation{})
          .has_value());
}

} // namespace packsim::io
