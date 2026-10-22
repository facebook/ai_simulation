/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <set>
#include <string>

#include <gtest/gtest.h>

#include "packsim/validations/node-name-and-key-should-match.h"

namespace packsim::details {

TEST(NodeNameAndKeyShouldMatchTest, EmptySimCfg) {
  thrift::Simulation simCfg;

  auto r =
      NodeNameAndKeyShouldMatch().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(NodeNameAndKeyShouldMatchTest, Match) {
  thrift::Simulation simCfg;
  {
    thrift::Node node;
    node.name() = "name";
    simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  }
  auto r =
      NodeNameAndKeyShouldMatch().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(NodeNameAndKeyShouldMatchTest, MisMatch) {
  thrift::Simulation simCfg;
  {
    thrift::Node node;
    node.name() = "invalid-node-name-2";
    simCfg.flex_topology()->nodes()->emplace(*node.name() + "?", node);
  }
  {
    thrift::Node node;
    node.name() = "invalid-node-name-1";
    simCfg.flex_topology()->nodes()->emplace(*node.name() + "?", node);
  }
  {
    thrift::Node node;
    node.name() = "valid-node-name";
    simCfg.flex_topology()->nodes()->emplace(*node.name(), node);
  }
  auto r =
      NodeNameAndKeyShouldMatch().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_EQ(
      r->errors,
      (std::set<std::string>{
          "The key for the node invalid-node-name-1 in the map FlexTopology::nodes is not the same, it is invalid-node-name-1?",
          "The key for the node invalid-node-name-2 in the map FlexTopology::nodes is not the same, it is invalid-node-name-2?"}));
}

} // namespace packsim::details
