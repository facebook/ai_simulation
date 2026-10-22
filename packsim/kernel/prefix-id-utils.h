/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <initializer_list>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/String.h>

namespace packsim {

// Create a dot-separated string key from integer components.
// e.g. makePrefixKey({1, 2, 3, 4}) returns "1.2.3.4"
inline std::string makePrefixKey(std::initializer_list<uint32_t> vals) {
  return fmt::format("{}", fmt::join(vals, "."));
}

// Create a dot-separated string key from a vector of integers.
inline std::string makePrefixKey(const std::vector<uint32_t>& vals) {
  return fmt::format("{}", fmt::join(vals, "."));
}

} // namespace packsim
