/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <type_traits>

#include <folly/CPortability.h>

namespace packsim {

template <typename T1, typename T2>
FOLLY_ALWAYS_INLINE bool beforeOrEqual(T1 a, T2 b) noexcept {
  static_assert(sizeof(T1) == sizeof(T2), "Types must have same width");
  static_assert(std::is_unsigned_v<T1>, "T1 must be unsigned");
  static_assert(std::is_unsigned_v<T2>, "T2 must be unsigned");
  // Storing the difference in T1 wraps it at T1's width even when a and b are
  // promoted to int.
  const T1 diff = a - b - 1;
  return diff >> (sizeof(T1) * 8 - 1);
}

template <typename T1, typename T2>
FOLLY_ALWAYS_INLINE bool before(T1 a, T2 b) noexcept {
  static_assert(sizeof(T1) == sizeof(T2), "Types must have same width");
  static_assert(std::is_unsigned_v<T1>, "T1 must be unsigned");
  static_assert(std::is_unsigned_v<T2>, "T2 must be unsigned");
  // Storing the difference in T1 wraps it at T1's width even when a and b are
  // promoted to int.
  const T1 diff = a - b;
  return diff >> (sizeof(T1) * 8 - 1);
}

template <typename T1, typename T2>
FOLLY_ALWAYS_INLINE bool after(T1 a, T2 b) noexcept {
  return !beforeOrEqual(a, b);
}

template <typename T1, typename T2>
FOLLY_ALWAYS_INLINE bool afterOrEqual(T1 a, T2 b) noexcept {
  return !before(a, b);
}

} // namespace packsim
