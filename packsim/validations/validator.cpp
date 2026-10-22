/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/validations/validator.h"

#include <algorithm>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "packsim/validations/register-validations.h"

namespace packsim {

std::vector<ValidationResult> runValidations(
    const thrift::Simulation& simCfg,
    const ValidatorOptions& options) {
  registerValidations();

  if (options.validationToRun.empty()) {
    return Validation::runAll(
        simCfg,
        /*stopOnError=*/options.stopOnError,
        /*ignoreWarnings=*/options.ignoreWarnings);
  }

  const auto validations = Validation::getValidations();
  const bool isKnownValidation =
      std::ranges::any_of(validations, [&](const Validation* v) {
        return v->getName() == options.validationToRun;
      });
  if (!isKnownValidation) {
    throw std::invalid_argument(
        fmt::format(
            "Unknown validation requested: {}", options.validationToRun));
  }

  std::vector<ValidationResult> results;
  if (auto r = Validation::runOne(
          simCfg,
          options.validationToRun,
          /*ignoreWarnings=*/options.ignoreWarnings);
      r) {
    results.emplace_back(std::move(r.value()));
  }
  return results;
}

} // namespace packsim
