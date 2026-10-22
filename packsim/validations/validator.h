/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <vector>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/validations/validation.h"

namespace packsim {

// Options controlling how the packsim config validations are run.
struct ValidatorOptions {
  // When true, validations that report only warnings (no errors) are treated as
  // passing and their warnings are dropped from the returned results.
  bool ignoreWarnings = true;

  // When true, stop running as soon as a validation reports an error.
  bool stopOnError = false;

  // When non-empty, run only the validation with this exact name; when empty,
  // run every registered validation.
  std::string validationToRun{};
};

/**
 * Run the packsim configuration validations against a simulation config.
 *
 * Registers all known validations (idempotent) and runs either every registered
 * validation or a single named one, as directed by `options`.
 *
 * @param simCfg   The simulation config to validate.
 * @param options  Options selecting which validations run and how.
 *
 * @return         The result for every validation that failed; empty when all
 *                 selected validations passed.
 *
 * @throw std::invalid_argument if `options.validationToRun` is non-empty but
 *        names a validation that is not registered.
 */
std::vector<ValidationResult> runValidations(
    const thrift::Simulation& simCfg,
    const ValidatorOptions& options);

} // namespace packsim
