/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/simple-json-field-checker.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <iterator>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/json/dynamic.h>
#include <folly/json/json.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "packsim/util/simple-json-key-rewriter.h"

using apache::thrift::SimpleJSONSerializer;

namespace {

// Parses `json` as standard JSON, keeping numbers as strings since only keys
// are compared, or returns std::nullopt if it is not standard JSON.
std::optional<folly::dynamic> tryParseJson(const std::string& json) {
  folly::json::serialization_opts opts;
  opts.parse_numbers_as_strings = true;
  try {
    return folly::parseJson(json, opts);
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

// Parses Simple JSON as the standard JSON `SimpleJsonKeyRewriter` makes of it,
// which requires input with no whitespace.
std::optional<folly::dynamic> tryParseRewritten(const std::string& simpleJson) {
  try {
    return tryParseJson(
        packsim::io::SimpleJsonKeyRewriter::toStandardJson(simpleJson));
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

// Parses Simple JSON. A map keyed by structs or lists makes it invalid JSON,
// so input that does not parse as it is gets parsed as rewritten.
std::optional<folly::dynamic> parseSimpleJson(const std::string& json) {
  if (auto parsed = tryParseJson(json)) {
    return parsed;
  }
  return tryParseRewritten(json);
}

/**
 * UnknownKeyCollector walks a parsed config beside its re-serialized form and
 * collects the path of every key the re-serialized form lacks.
 */
class UnknownKeyCollector {
 public:
  /**
   * Collect the keys of `input` missing from `known` at the same place.
   *
   * Arrays are compared index by index. A list keeps its order through the
   * round trip; a set, or a map rewritten as a list, need not, and a
   * reordering could report an optional field that some of its elements set
   * and others do not.
   *
   * @param input     A value as read from the config.
   * @param known     The same value as re-serialized from the schema.
   */
  void collect(const folly::dynamic& input, const folly::dynamic& known) {
    const size_t parentLength = path_.size();
    if (input.isObject() && known.isObject()) {
      for (const auto& [key, value] : input.items()) {
        // The reader skips a key whose value is null whether or not the
        // schema defines it.
        if (value.isNull()) {
          continue;
        }
        const std::string& name = key.getString();
        if (parentLength > 0) {
          path_ += '.';
        }
        path_ += name;
        if (const auto* const knownValue = known.get_ptr(name)) {
          collect(value, *knownValue);
        } else {
          unknown_.emplace_back(path_);
        }
        path_.resize(parentLength);
      }
    } else if (input.isArray() && known.isArray()) {
      const size_t count = std::min(input.size(), known.size());
      for (size_t i = 0; i < count; ++i) {
        const auto& element = input[i];
        if (!element.isObject() && !element.isArray()) {
          continue;
        }
        fmt::format_to(std::back_inserter(path_), "[{}]", i);
        collect(element, known[i]);
        path_.resize(parentLength);
      }
    }
  }

  /**
   * Sort the collected paths and hand them over.
   *
   * @return          The path of every key the re-serialized form lacks,
   *                  sorted.
   */
  std::vector<std::string> sortedPaths() && {
    std::sort(unknown_.begin(), unknown_.end());
    return std::move(unknown_);
  }

 private:
  // Path from the root to the values being compared, e.g. `workloads[0].name`.
  std::string path_;
  // Paths of the keys the re-serialized form lacks.
  std::vector<std::string> unknown_;
};

} // namespace

namespace packsim::io {

std::optional<std::vector<std::string>>
SimpleJsonFieldChecker::findUnknownFields(
    const std::string& inputJson,
    const packsim::thrift::Simulation& parsed) {
  const auto input = parseSimpleJson(inputJson);
  if (!input.has_value()) {
    return std::nullopt;
  }
  // No struct a config reaches is marked @thrift.TerseWrite. One that was
  // would leave its default-valued fields out of this re-serialization, and
  // each such field the input sets to its default would be reported.
  const auto known =
      tryParseRewritten(SimpleJSONSerializer::serialize<std::string>(parsed));
  if (!known.has_value()) {
    return std::nullopt;
  }
  UnknownKeyCollector collector;
  collector.collect(*input, *known);
  return std::move(collector).sortedPaths();
}

} // namespace packsim::io
