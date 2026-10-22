/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/validations/validator.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/validations/validation.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

namespace {

// Builds a Simulation whose FlexTopology has one node stored under a key that
// differs from the node's name, which fails NodeNameAndKeyShouldMatch.
thrift::Simulation makeConfigWithNodeKeyMismatch() {
  thrift::Simulation simCfg;
  thrift::Node node;
  node.name() = "valid-node-name";
  simCfg.flex_topology()->nodes()->emplace(*node.name() + "?", node);
  return simCfg;
}

// Builds a Simulation whose single node is stored under a key equal to its
// name, which passes NodeNameAndKeyShouldMatch.
thrift::Simulation makeConfigWithMatchingNodeKey() {
  thrift::Simulation simCfg;
  thrift::Node node;
  node.name() = "valid-node-name";
  simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  return simCfg;
}

bool hasResultNamed(
    const std::vector<ValidationResult>& results,
    const std::string& name) {
  return std::ranges::any_of(
      results, [&](const ValidationResult& r) { return r.name == name; });
}

} // namespace

TEST(ValidatorTest, RunAllReportsFailingValidation) {
  const auto simCfg = makeConfigWithNodeKeyMismatch();

  const auto results = runValidations(simCfg, ValidatorOptions{});

  EXPECT_TRUE(hasResultNamed(results, "NodeNameAndKeyShouldMatch"));
}

TEST(ValidatorTest, RunSingleValidationReturnsOnlyThatValidation) {
  const auto simCfg = makeConfigWithNodeKeyMismatch();

  const auto results = runValidations(
      simCfg, ValidatorOptions{.validationToRun = "NodeNameAndKeyShouldMatch"});

  ASSERT_EQ(1u, results.size());
  EXPECT_EQ("NodeNameAndKeyShouldMatch", results.front().name);
}

TEST(ValidatorTest, RunSingleValidationOnValidConfigReturnsEmpty) {
  const auto simCfg = makeConfigWithMatchingNodeKey();

  const auto results = runValidations(
      simCfg, ValidatorOptions{.validationToRun = "NodeNameAndKeyShouldMatch"});

  EXPECT_TRUE(results.empty());
}

TEST(ValidatorTest, UnknownValidationThrows) {
  const thrift::Simulation simCfg;

  EXPECT_THAT(
      [&] {
        runValidations(
            simCfg, ValidatorOptions{.validationToRun = "DoesNotExist"});
      },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Unknown validation requested")));
}

} // namespace packsim
