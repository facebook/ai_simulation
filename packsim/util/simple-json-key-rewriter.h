/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

namespace packsim::io {

/**
 * SimpleJsonKeyRewriter turns Thrift Simple JSON into standard JSON.
 *
 * Simple JSON writes a map keyed by structs, unions or containers with the
 * key's object or array where JSON requires a string, e.g. `nicQpnPairMap`'s
 * `{{"srcRank":0,"dstRank":1}:...}`. Every other key it writes is a string,
 * primitives included. The rewriter writes each such map as a list of
 * `{"key", "value"}` objects, the form Thrift's JSON5 protocol reads, and
 * copies everything else byte for byte.
 *
 * Example usage:
 *
 *      std::string json = SimpleJsonKeyRewriter::toStandardJson(
 *          apache::thrift::SimpleJSONSerializer::serialize<std::string>(result));
 */
class SimpleJsonKeyRewriter {
 public:
  /**
   * Rewrite Simple JSON as standard JSON.
   *
   * @param simpleJson    `SimpleJSONSerializer` output, e.g.
   *                      `{"m":{{"a":1}:[2]}}`.
   *
   * @return              Standard JSON, e.g.
   *                      `{"m":[{"key":{"a":1},"value":[2]}]}`.
   *
   * @throw std::invalid_argument if `simpleJson` is not a Simple JSON value.
   */
  static std::string toStandardJson(const std::string& simpleJson);
};

} // namespace packsim::io
