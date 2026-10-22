/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "packsim/validations/validation.h"

namespace packsim::details {

class LinkNodeShouldMatch : public Validation {
 public:
  LinkNodeShouldMatch()
      : Validation(
            "LinkNodeShouldMatch",
            "The links for a node should have the node as one of the two nodes in the link (FlexLink::node1, FlexLink::node2)") {
  }

  /**
   * For each node, check that all the links specified should have the node as
   * one of the two nodes. The various link ids referenced in the node are
   *   1. Node::lpmSpec.prefixTable.links
   *   2. Node::lpmSpec.defaultRoute.links
   *   3. Node::endpointCfgs.linkIds
   *   4. Node::endpointCfgs.lpmSpec.prefixTable.links
   *   5. Node::endpointCfgs.lpmSpec.defaultRoute.links
   *
   * @param simCfg           The simulation configuration to validate
   * @param ignoreWarnings   Whether to ignore validation warnings
   * @return                 Validation result if any issues found, std::nullopt
   *                         otherwise
   */
  std::optional<ValidationResult> validate(
      thrift::Simulation const& simCfg,
      bool const ignoreWarnings) const final;
};

} // namespace packsim::details
