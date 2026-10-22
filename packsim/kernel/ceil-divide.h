/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <stdexcept>

#include <folly/Math.h>

namespace packsim {

// Not FOLLY_ALWAYS_INLINE: force-inlining the throwing overflow check would
// bloat every caller.
inline uint32_t ceilDivide32(uint64_t a, uint64_t b) {
  const uint64_t result = folly::divCeil(a, b);
  if (result > UINT32_MAX) {
    throw std::runtime_error("ceilDivide32 overflowed");
  }
  return static_cast<uint32_t>(result);
}

} // namespace packsim
