/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/validations/validation.h"

#include <algorithm>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <folly/logging/xlog.h>

namespace packsim {

std::vector<ValidationResult> Validation::runAllValidations(
    std::map<std::string, std::unique_ptr<Validation>> const& validators,
    thrift::Simulation const& simCfg,
    bool const stopOnError,
    bool const ignoreWarnings) {
  bool stop = false;
  return std::accumulate(
      validators.begin(),
      validators.end(),
      std::vector<ValidationResult>{},
      [&](auto result, auto const& kv) {
        if (!stop) {
          if (auto r = kv.second->validate(simCfg, ignoreWarnings); r) {
            result.emplace_back(std::move(r.value()));
            stop = stopOnError;
          }
        }
        return std::move(result);
      });
}

std::optional<ValidationResult> Validation::runOneValidation(
    std::map<std::string, std::unique_ptr<Validation>> const& validators,
    thrift::Simulation const& simCfg,
    std::string const& validation,
    bool const ignoreWarnings) {
  const auto pos = validators.find(validation);

  if (pos == validators.end()) {
    XLOGF(ERR, "Invalid validation requested: {}", validation);
    return std::nullopt;
  }

  return pos->second->validate(simCfg, ignoreWarnings);
}

void Validation::print(std::vector<ValidationResult> const& results) {
  std::ranges::for_each(results, [](packsim::ValidationResult const& v) {
    XLOGF(ERR, "{}", v.name);
    std::ranges::for_each(
        v.errors, [](auto const& e) { XLOGF(ERR, " {}", e); });
    std::ranges::for_each(
        v.warnings, [](auto const& w) { XLOGF(WARN, " {}", w); });
  });
}

std::map<std::string, std::unique_ptr<Validation>>&
Validation::getAllRegisteredValidators() {
  static std::map<std::string, std::unique_ptr<Validation>> v;

  return v;
}

} // namespace packsim
