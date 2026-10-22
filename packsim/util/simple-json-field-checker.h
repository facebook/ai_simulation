/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "packsim/if/gen-cpp2/config_types.h"

namespace packsim::io {

/**
 * SimpleJsonFieldChecker finds the keys of a Simple JSON config that reading
 * it dropped.
 *
 * Simple JSON reading skips every key the schema does not define, union arms
 * included, so a config written with a misspelled field, or against a
 * different version of the schema, loads without the settings those keys hold.
 *
 * Example usage:
 *
 *      auto cfg = apache::thrift::SimpleJSONSerializer::deserialize<
 *          packsim::thrift::Simulation>(json);
 *      auto unknown = SimpleJsonFieldChecker::findUnknownFields(json, cfg);
 */
class SimpleJsonFieldChecker {
 public:
  /**
   * Find the keys of `inputJson` that reading it into `parsed` dropped.
   *
   * Re-serializes `parsed` and reports each key of `inputJson` that is missing
   * at the same place in the result, as a path such as
   * `newWorkloadSchedule.workloads[0].config.allToAll.sizeByte`. A key whose
   * value is `null` sets nothing and is not reported. A map key the schema
   * writes back differently, such as `"01"` for 1, is reported.
   *
   * Compact and binary configs identify fields by id rather than by name, so
   * there is no such check for them.
   *
   * @param inputJson   The Simple JSON `parsed` was read from.
   * @param parsed      The config read from `inputJson`.
   *
   * @return            The path of every dropped key, sorted, or
   *                    `std::nullopt` if `inputJson` is neither standard JSON
   *                    nor Simple JSON without whitespace, so it cannot be
   *                    checked.
   */
  static std::optional<std::vector<std::string>> findUnknownFields(
      const std::string& inputJson,
      const packsim::thrift::Simulation& parsed);
};

} // namespace packsim::io
