/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "packsim/if/gen-cpp2/config_types.h"

namespace packsim {

struct ValidationResult {
  std::string name;
  std::string description;
  std::set<std::string> errors;
  std::set<std::string> warnings;
};

class Validation {
 public:
  /**
   * Run all validation and return the validation results with errors and/or
   * warnings.
   *
   * @param simCfg         the simulation config
   * @param stopOnError    stop running validation on first error
   * @param ignoreWarnings returns the validation results with errors only
   *
   * @return the vector of results (i.e., errors/warnings) for failed
   *         validations
   */
  static std::vector<ValidationResult> runAll(
      thrift::Simulation const& simCfg,
      bool const stopOnError = false,
      bool const ignoreWarnings = false) {
    return runAllValidations(
        getAllRegisteredValidators(), simCfg, stopOnError, ignoreWarnings);
  }

  /**
   * Run the requested validation and return the validation result with errors
   * and/or warnings if validation failed otherwise std::nullopt.
   *
   * @param simCfg         the simulation config
   * @param validation     the requested validation to run
   * @param ignoreWarnings returns the validation results with errors only
   *
   * @return the validation result (i.e., errors/warnings) if the validation
   *         failed otherwise std::nullopt
   */
  static std::optional<ValidationResult> runOne(
      thrift::Simulation const& simCfg,
      std::string const& validation,
      bool const ignoreWarnings = false) {
    return runOneValidation(
        getAllRegisteredValidators(), simCfg, validation, ignoreWarnings);
  }

  /**
   * Register a validation.
   *
   * @param validator the validation to register
   *
   * @throw std::runtime_error if a validation with the same name already exists
   */
  static void registerValidation(std::unique_ptr<Validation> validator) {
    auto name = validator->getName();
    if (getAllRegisteredValidators().contains(name)) {
      throw std::runtime_error(fmt::format("Duplicate validation : {}", name));
    }

    getAllRegisteredValidators().emplace(std::move(name), std::move(validator));
  }

  static std::optional<ValidationResult> runOneValidation(
      std::map<std::string, std::unique_ptr<Validation>> const& validators,
      thrift::Simulation const& simCfg,
      std::string const& validation,
      bool const ignoreWarnings);

  static std::vector<ValidationResult> runAllValidations(
      std::map<std::string, std::unique_ptr<Validation>> const& validators,
      thrift::Simulation const& simCfg,
      bool const stopOnError,
      bool const ignoreWarnings);

  // Returns the name of the validation.
  std::string const& getName() const noexcept {
    return name_;
  }

  // Returns a short description of the validation.
  std::string const& getDescription() const noexcept {
    return description_;
  }

  // Returns the collection of validations.
  static std::vector<Validation const*> getValidations() {
    return std::accumulate(
        getAllRegisteredValidators().begin(),
        getAllRegisteredValidators().end(),
        std::vector<Validation const*>{},
        [](std::vector<Validation const*> result, auto const& kv) {
          result.emplace_back(kv.second.get());
          return result;
        });
  }

  // Print the results.
  static void print(std::vector<ValidationResult> const& results);

  virtual ~Validation() = default;

 protected:
  Validation(const std::string& name, const std::string& description)
      : name_(name), description_(description) {}

  /**
   * Derived class run the respective validation. Returns a value if the
   * validation has failed.
   *
   * @param simCfg         the simulation config
   * @param ignoreWarnings returns the validation results with errors only
   *
   * @return the validation result if the validation failed, otherwise
   *         std::nullopt
   */
  virtual std::optional<ValidationResult> validate(
      thrift::Simulation const& simCfg,
      bool const ignoreWarnings) const = 0;

 private:
  // Name of the validation.
  const std::string name_;
  // A short description of the validation.
  const std::string description_;

  /**
   * Returns the static map of all the registered validations.
   * Register all the validations through static initialization
   * in the constructor.
   *
   * @return the static map of all registered validations
   */
  static std::map<std::string, std::unique_ptr<Validation>>&
  getAllRegisteredValidators();
};
} // namespace packsim
