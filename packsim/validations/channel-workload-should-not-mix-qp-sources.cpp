/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/validations/channel-workload-should-not-mix-qp-sources.h"

#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace packsim::details {

namespace {

// The fields of `workload` that number its notify QPs apart from its channel
// QPs, as they would be named in an error.
std::vector<std::string> listSeparatelyNumberedQpSources(
    workload::thrift::Workload const& workload) {
  std::vector<std::string> sources;
  if (!workload.qpTemplates()->empty()) {
    sources.emplace_back("qpTemplates");
  }
  // @oss-disable
    // @oss-disable
  // @oss-disable
  if (!workload.nicQpnPairMap()->empty() && workload.qpnPairMap()->empty()) {
    sources.emplace_back("a nicQpnPairMap without its qpnPairMap");
  }
  return sources;
}

} // namespace

std::optional<ValidationResult> ChannelWorkloadShouldNotMixQpSources::validate(
    thrift::Simulation const& simCfg,
    bool const /* ignoreWarnings */) const {
  std::set<std::string> errors;
  for (const auto& workload : *simCfg.newWorkloadSchedule()->workloads()) {
    if (!workload.channelSettings().has_value()) {
      continue;
    }
    const auto sources = listSeparatelyNumberedQpSources(workload);
    if (!sources.empty()) {
      errors.emplace(
          fmt::format(
              "Workload {0} sets channelSettings together with {1}. Its channel QPs come from qpnPairMap, numbered apart from {1}, so a channel QP can also be a notify QP, and building the simulation would throw. Drop {1}, or drop channelSettings.",
              *workload.name(),
              fmt::join(sources, " and ")));
    }
  }

  if (errors.empty()) {
    return std::nullopt;
  }

  return ValidationResult{
      .name = getName(),
      .description = getDescription(),
      .errors = std::move(errors),
      .warnings = {}};
}

} // namespace packsim::details
