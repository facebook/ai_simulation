/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "packsim/validations/register-validations.h"
#include "packsim/validations/validation.h"

namespace packsim {

TEST(ValidationTest, CheckRegisteredValidations) {
  registerValidations();
  auto validations = Validation::getValidations();

  std::set<std::string> names = std::accumulate(
      validations.begin(),
      validations.end(),
      std::set<std::string>{},
      [](auto result, Validation const* v) {
        result.emplace(v->getName());
        return std::move(result);
      });

  EXPECT_EQ(
      names,
      (std::set<std::string>{
          "ChannelWorkloadShouldNotMixQpSources",
          "LinkNodeShouldMatch",
          // @oss-disable
          "NicConfigShouldExist",
          "NoCyclesInWorkloadDag",
          "NodeNameAndKeyShouldMatch"}));
}

TEST(ValidationTest, NodeNameAndKeyShouldMatch) {
  registerValidations();

  thrift::Simulation simCfg;
  {
    thrift::Node node;
    node.name() = "invalid-node-name";
    simCfg.flex_topology()->nodes()->emplace(
        fmt::format("{}?", *node.name()), node);
  }
  {
    thrift::Node node;
    node.name() = "valid-node-name";
    simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  }
  auto errors = std::set<std::string>{
      "The key for the node invalid-node-name in the map FlexTopology::nodes is not the same, it is invalid-node-name?"};

  auto v = "NodeNameAndKeyShouldMatch";
  auto r = Validation::runOne(simCfg, v);
  EXPECT_EQ(r->errors, errors);

  auto results = Validation::runAll(simCfg);
  auto it =
      std::find_if(results.begin(), results.end(), [v](auto const& result) {
        return result.name == v;
      });
  EXPECT_EQ(it->errors, errors);
}

struct TestValidation : public Validation {
  TestValidation(std::string const& name, bool errors, bool warnings)
      : Validation(name, "test validator"),
        errors_(errors),
        warnings_(warnings) {}

  std::optional<ValidationResult> validate(
      thrift::Simulation const& /* simCfg */,
      bool const ignoreWarnings) const final {
    invoked_++;

    if (errors_ == false && (warnings_ == false || ignoreWarnings)) {
      return std::nullopt;
    }

    return ValidationResult{
        .name = getName(),
        .description = getDescription(),
        .errors = getErrors(),
        .warnings = getWarnings()};
  }

  std::set<std::string> getErrors() const {
    return errors_
        ? std::set<std::string>{{fmt::format("error in {0}", getName())}}
        : std::set<std::string>{};
  }

  std::set<std::string> getWarnings() const {
    return errors_
        ? std::set<std::string>{{fmt::format("warning in {0}", getName())}}
        : std::set<std::string>{};
  }

  const bool errors_;
  const bool warnings_;

  mutable unsigned invoked_{0};

  static unsigned getInvoked(
      std::map<std::string, std::unique_ptr<Validation>> const& validators,
      std::string const& name) {
    // Reference cast: throws std::bad_cast on a non-TestValidation entry
    // rather than yielding nullptr for the caller to dereference.
    return dynamic_cast<TestValidation const&>(*validators.at(name)).invoked_;
  }
};

TEST(ValidationTest, PassAllTests) {
  thrift::Simulation simCfg;

  std::map<std::string, std::unique_ptr<Validation>> validators;

  validators.emplace(
      "v1",
      std::make_unique<TestValidation>(
          "v1", /*errors=*/false, /*warnings=*/false));
  validators.emplace(
      "v2",
      std::make_unique<TestValidation>(
          "v2", /*errors=*/false, /*warnings=*/false));

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/true, /*ignoreWarnings=*/true);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 1);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 1);
  }

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/true, /*ignoreWarnings=*/false);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 2);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 2);
  }

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/false, /*ignoreWarnings=*/true);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 3);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 3);
  }

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/false, /*ignoreWarnings=*/false);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 4);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 4);
  }
  {
    auto results = Validation::runOneValidation(
        validators, simCfg, "v1", /*ignoreWarnings=*/false);
    EXPECT_FALSE(results.has_value());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 5);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 4);
  }
  {
    auto results = Validation::runOneValidation(
        validators, simCfg, "v1", /*ignoreWarnings=*/true);
    EXPECT_FALSE(results.has_value());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 6);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 4);
  }
}

TEST(ValidationTest, FailAll) {
  thrift::Simulation simCfg;

  std::map<std::string, std::unique_ptr<Validation>> validators;

  validators.emplace(
      "v1",
      std::make_unique<TestValidation>(
          "v1", /*errors=*/true, /*warnings=*/true));
  validators.emplace(
      "v2",
      std::make_unique<TestValidation>(
          "v2", /*errors=*/true, /*warnings=*/false));

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/false, /*ignoreWarnings=*/false);
    EXPECT_EQ(results.size(), 2);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 1);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 1);
  }

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/true, /*ignoreWarnings=*/false);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 2);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 1);
  }
  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/false, /*ignoreWarnings=*/true);
    EXPECT_EQ(results.size(), 2);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 3);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 2);
  }

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/true, /*ignoreWarnings=*/true);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 4);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 2);
  }

  {
    auto results = Validation::runOneValidation(
        validators, simCfg, "v1", /*ignoreWarnings=*/false);
    EXPECT_TRUE(results.has_value());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 5);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 2);
  }
  {
    auto results = Validation::runOneValidation(
        validators, simCfg, "v1", /*ignoreWarnings=*/true);
    EXPECT_TRUE(results.has_value());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 6);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 2);
  }
}

TEST(ValidationTest, WarningsAndErrors) {
  thrift::Simulation simCfg;

  std::map<std::string, std::unique_ptr<Validation>> validators;

  // warning but no errors
  validators.emplace(
      "v1",
      std::make_unique<TestValidation>(
          "v1", /*errors=*/false, /*warnings=*/true));

  // both warnings and errors
  validators.emplace(
      "v2",
      std::make_unique<TestValidation>(
          "v2", /*errors=*/true, /*warnings=*/true));

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/false, /*ignoreWarnings=*/false);
    EXPECT_EQ(results.size(), 2);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 1);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 1);
  }

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/true, /*ignoreWarnings=*/false);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 2);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 1);
  }
  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/false, /*ignoreWarnings=*/true);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 3);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 2);
  }

  {
    auto results = Validation::runAllValidations(
        validators, simCfg, /*stopOnError=*/true, /*ignoreWarnings=*/true);
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 4);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 3);
  }

  {
    auto results = Validation::runOneValidation(
        validators, simCfg, "v1", /*ignoreWarnings=*/false);
    EXPECT_TRUE(results.has_value());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 5);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 3);
  }
  {
    auto results = Validation::runOneValidation(
        validators, simCfg, "v1", /*ignoreWarnings=*/true);
    EXPECT_FALSE(results.has_value());
    EXPECT_EQ(TestValidation::getInvoked(validators, "v1"), 6);
    EXPECT_EQ(TestValidation::getInvoked(validators, "v2"), 3);
  }
}

} // namespace packsim
