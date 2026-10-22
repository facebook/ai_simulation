/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <set>
#include <string>

#include <gtest/gtest.h>

#include "packsim/validations/nic-config-should-exist.h"

namespace packsim::details {

TEST(NicConfigShouldExistTest, EmptySimCfg) {
  thrift::Simulation simCfg;

  auto r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(NicConfigShouldExistTest, DescriptionSpellsOutTheDefaultNicConfigKey) {
  EXPECT_EQ(
      NicConfigShouldExist().getDescription(),
      "thrift::EndpointConfig::nicConfig should be present in the map "
      "Simulation::nicConfigMap, or be \"default\" when that map is empty "
      "(Simulation.nic supplies it).");
}

TEST(NicConfigShouldExistTest, Success) {
  thrift::Simulation simCfg;
  {
    thrift::Node node;
    node.type() = thrift::NodeType::ACCEL;
    node.name() = "name";
    node.endpointCfgs()->emplace_back();
    node.endpointCfgs()->back().nicConfig() = "default";

    simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  }

  {
    simCfg.nicConfigMap()["default"] = thrift::NicConfig{};
  }

  auto r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(NicConfigShouldExistTest, EmptyMapAcceptsTheLegacyNicAsDefault) {
  thrift::Simulation simCfg;
  thrift::Node node;
  node.type() = thrift::NodeType::ACCEL;
  node.name() = "name";
  node.endpointCfgs()->emplace_back();
  node.endpointCfgs()->back().nicConfig() = "default";
  simCfg.flex_topology()->nodes()->emplace(*node.name(), node);

  auto r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(NicConfigShouldExistTest, EmptyMapRejectsANonDefaultNicConfig) {
  thrift::Simulation simCfg;
  thrift::Node node;
  node.type() = thrift::NodeType::ACCEL;
  node.name() = "name";
  node.endpointCfgs()->emplace_back();
  node.endpointCfgs()->back().nicConfig() = "test";
  simCfg.flex_topology()->nodes()->emplace(*node.name(), node);

  auto r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/false);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(
      r->errors,
      (std::set<std::string>{
          "The endpoints for the node name references a nic config test, which is not present in the Simulation::nicConfigMap"}));
}

TEST(NicConfigShouldExistTest, EmptyMapAcceptsANodeWithoutEndpoints) {
  thrift::Simulation simCfg;
  thrift::Node node;
  node.type() = thrift::NodeType::ACCEL;
  node.name() = "name";
  simCfg.flex_topology()->nodes()->emplace(*node.name(), node);

  auto r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(NicConfigShouldExistTest, NoAccelNodes) {
  thrift::Simulation simCfg;
  {
    thrift::Node node;
    node.type() = thrift::NodeType::SWITCH;
    node.name() = "name";

    simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  }

  auto r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(NicConfigShouldExistTest, NotFound) {
  thrift::Simulation simCfg;
  {
    simCfg.nicConfigMap()["default"] = thrift::NicConfig{};
    simCfg.nicConfigMap()["test"] = thrift::NicConfig{};
    simCfg.nicConfigMap()["not-used"] = thrift::NicConfig{};
  }

  {
    thrift::Node node;
    node.type() = thrift::NodeType::ACCEL;
    node.name() = "node-name";
    node.endpointCfgs()->emplace_back();
    node.endpointCfgs()->back().nicConfig() = "test";

    node.endpointCfgs()->emplace_back();
    node.endpointCfgs()->back().nicConfig() = "does-not-exist";

    simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  }

  {
    thrift::Node node;
    node.type() = thrift::NodeType::ACCEL;
    node.name() = "node-using-default-config";

    simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  }

  auto r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_EQ(
      r->errors,
      (std::set<std::string>{
          "The endpoints for the node node-name references a nic config does-not-exist, which is not present in the Simulation::nicConfigMap"}));

  EXPECT_EQ(
      r->warnings,
      (std::set<std::string>{
          "The nic config not-used is not used in any endpoints"}));

  r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/true);
  EXPECT_EQ(
      r->errors,
      (std::set<std::string>{
          "The endpoints for the node node-name references a nic config does-not-exist, which is not present in the Simulation::nicConfigMap"}));

  EXPECT_EQ(r->warnings, std::set<std::string>{});
}

TEST(NicConfigShouldExistTest, NoDefault) {
  thrift::Simulation simCfg;
  {
    simCfg.nicConfigMap()["test"] = thrift::NicConfig{};
    simCfg.nicConfigMap()["not-used"] = thrift::NicConfig{};
  }

  {
    thrift::Node node;

    node.name() = "node-name";
    node.endpointCfgs()->emplace_back();

    node.endpointCfgs()->emplace_back();
    node.endpointCfgs()->back().nicConfig() = "test";

    simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  }

  {
    thrift::Node node;
    node.type() = thrift::NodeType::ACCEL;
    node.name() = "node-using-default-config";

    simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  }

  auto r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_EQ(
      r->errors,
      (std::set<std::string>{
          "No endpoints for the node node-using-default-config present, so using default which is not present in the Simulation::nicConfigMap",
          "The endpoints for the node node-name references a nic config default, which is not present in the Simulation::nicConfigMap"}));

  EXPECT_EQ(
      r->warnings,
      (std::set<std::string>{
          "The nic config not-used is not used in any endpoints"}));

  r = NicConfigShouldExist().validate(simCfg, /*ignoreWarnings=*/true);
  EXPECT_EQ(
      r->errors,
      (std::set<std::string>{
          "No endpoints for the node node-using-default-config present, so using default which is not present in the Simulation::nicConfigMap",
          "The endpoints for the node node-name references a nic config default, which is not present in the Simulation::nicConfigMap"}));

  EXPECT_EQ(r->warnings, std::set<std::string>{});
}

} // namespace packsim::details
