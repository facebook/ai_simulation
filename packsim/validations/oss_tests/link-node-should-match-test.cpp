/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "packsim/validations/link-node-should-match.h"

namespace packsim::details {

TEST(LinkNodeShouldMatchTest, EmptySimCfg) {
  thrift::Simulation simCfg;

  auto r = LinkNodeShouldMatch().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(LinkNodeShouldMatchTest, Success) {
  thrift::Simulation simCfg;

  static std::string name = "accel";

  auto accelNode = [&simCfg]() -> thrift::Node* {
    return &(simCfg.flex_topology()->nodes()->find(name)->second);
  };

  {
    thrift::Node node;
    node.name() = name;
    node.endpointCfgs()->emplace_back();

    simCfg.flex_topology()->nodes()->emplace(node.name().value(), node);
  }

  {
    thrift::Node node;
    node.name() = "switch";
    simCfg.flex_topology()->nodes()->emplace(node.name().value(), node);
  }

  // Node::lpmSpec.defaultRoute.links
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    accelNode()->lpmSpec()->defaultRoute()->links()->emplace_back(
        link.linkId().value());
  }

  // Node::lpmSpec.prefixTable.links
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    thrift::LpmTableEntry entry;
    entry.links()->emplace_back(link.linkId().value());

    auto ipAddress = std::string("0.0.0.0.0");
    accelNode()->lpmSpec()->prefixTable()->emplace(ipAddress, entry);
  }

  // Node::endpointCfgs.linkIds
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    accelNode()->endpointCfgs()->front().linkIds()->emplace_back(
        link.linkId().value());
  }
  // Node::endpointCfgs.lpmSpec.defaultRoute.links
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    accelNode()
        ->endpointCfgs()
        ->front()
        .lpmSpec()
        ->defaultRoute()
        ->links()
        ->emplace_back(link.linkId().value());
  }

  // Node::endpointCfgs.lpmSpec.prefixTable.links
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    thrift::LpmTableEntry entry;
    entry.links()->emplace_back(link.linkId().value());

    auto ipAddress = std::string("1.0.0.0.0");
    accelNode()->endpointCfgs()->front().lpmSpec()->prefixTable()->emplace(
        ipAddress, entry);
  }

  auto r = LinkNodeShouldMatch().validate(simCfg, /*ignoreWarnings=*/false);
  EXPECT_FALSE(r.has_value());
}

TEST(LinkNodeShouldMatchTest, Fail) {
  thrift::Simulation simCfg;

  static std::string name = "accel-1";

  auto accelNode = [&simCfg]() -> thrift::Node* {
    return &(simCfg.flex_topology()->nodes()->find(name)->second);
  };

  {
    thrift::Node node;
    node.name() = name;
    node.endpointCfgs()->emplace_back();

    simCfg.flex_topology()->nodes()->emplace(node.name().value(), node);
  }

  {
    thrift::Node node;
    node.name() = "switch";
    simCfg.flex_topology()->nodes()->emplace(node.name().value(), node);
  }

  // Node::lpmSpec.defaultRoute.links
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "switch";
    link.node2() = "accel-1";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    accelNode()->lpmSpec()->defaultRoute()->links()->emplace_back(
        link.linkId().value());
  }
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel-2";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    accelNode()->lpmSpec()->defaultRoute()->links()->emplace_back(
        link.linkId().value());
  }

  // Node::lpmSpec.prefixTable.links
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel-1";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    thrift::LpmTableEntry entry;
    entry.links()->emplace_back(link.linkId().value());

    auto ipAddress = std::string("0.0.0.0.0");
    accelNode()->lpmSpec()->prefixTable()->emplace(ipAddress, entry);
  }
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel-2";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    thrift::LpmTableEntry entry;
    entry.links()->emplace_back(link.linkId().value());

    auto ipAddress = std::string("1.2.3.4.5.6");
    accelNode()->lpmSpec()->prefixTable()->emplace(ipAddress, entry);
  }

  // Node::endpointCfgs.linkIds
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel-1";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    accelNode()->endpointCfgs()->front().linkIds()->emplace_back(
        link.linkId().value());
  }
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel-2";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    accelNode()->endpointCfgs()->front().linkIds()->emplace_back(
        link.linkId().value());
  }

  // Node::endpointCfgs.lpmSpec.defaultRoute.links
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel-1";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    accelNode()
        ->endpointCfgs()
        ->front()
        .lpmSpec()
        ->defaultRoute()
        ->links()
        ->emplace_back(link.linkId().value());
  }
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel-2";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    accelNode()
        ->endpointCfgs()
        ->front()
        .lpmSpec()
        ->defaultRoute()
        ->links()
        ->emplace_back(link.linkId().value());
  }

  // Node::endpointCfgs.lpmSpec.prefixTable.links
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel-1";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    thrift::LpmTableEntry entry;
    entry.links()->emplace_back(link.linkId().value());

    auto ipAddress = std::string("1.0.0.0.0");
    accelNode()->endpointCfgs()->front().lpmSpec()->prefixTable()->emplace(
        ipAddress, entry);
  }
  {
    thrift::FlexLink link;

    link.linkId() = simCfg.flex_topology()->links()->size();
    link.node1() = "accel-2";
    link.node2() = "switch";

    simCfg.flex_topology()->links()->emplace(link.linkId().value(), link);

    thrift::LpmTableEntry entry;
    entry.links()->emplace_back(link.linkId().value());

    auto ipAddress = std::string("7.8.9.10.11.12");
    accelNode()->endpointCfgs()->front().lpmSpec()->prefixTable()->emplace(
        ipAddress, entry);
  }

  auto r = LinkNodeShouldMatch().validate(simCfg, /*ignoreWarnings=*/false);
  ASSERT_TRUE(r.has_value());

  EXPECT_EQ(
      r->errors,
      (std::set<std::string>{
          "The link id 1 for the node accel-1 at lpmSpec.defaultRoute.links.1 has node1 = accel-2 and node2 = switch",
          "The link id 3 for the node accel-1 at lpmSpec.prefixTable.1.2.3.4.5.6.links.0 has node1 = accel-2 and node2 = switch",
          "The link id 5 for the node accel-1 at endpointCfgs.0.linkIds.1 has node1 = accel-2 and node2 = switch",
          "The link id 7 for the node accel-1 at endpointCfgs.0.lpmSpec.defaultRoute.links.1 has node1 = accel-2 and node2 = switch",
          "The link id 9 for the node accel-1 at endpointCfgs.0.lpmSpec.prefixTable.7.8.9.10.11.12.links.0 has node1 = accel-2 and node2 = switch"}));
}

} // namespace packsim::details
