/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <limits>
#include <stdexcept>

namespace packsim {

/**
 * Counts the buffer cells a packet occupies for one fixed cell size without a
 * hardware division: the cell size's reciprocal is computed once, and each
 * count is a multiplication. The count is the ceiling of `bytes / cellSize`
 * for every 32-bit byte count and cell size, with no wrap near `UINT32_MAX`.
 *
 * Example usage:
 *
 *      const CellDivider divider(254);
 *      // A 1500-byte packet in 254-byte cells occupies 6 cells.
 *      const uint32_t cells = divider.getCellsFromBytes(1500);
 */
class CellDivider {
 public:
  // A divider for one-byte cells, which counts every byte as a cell.
  constexpr CellDivider() noexcept = default;

  /**
   * @param cellSize   Size of each cell in bytes.
   *
   * @throw            std::invalid_argument if `cellSize` is 0.
   */
  explicit CellDivider(const uint32_t cellSize)
      : reciprocal_(getReciprocal(cellSize)) {}

  /**
   * Given a number of bytes, return the number of cells needed: the ceiling of
   * `bytes / cellSize`.
   *
   * @param bytes   Number of bytes to pack.
   *
   * @return        Number of cells required.
   */
  [[nodiscard]] uint32_t getCellsFromBytes(
      const uint32_t bytes) const noexcept {
    if (reciprocal_ == 0) {
      return bytes;
    }
    // The high half of the product is `bytes / cellSize` rounded down, and the
    // low half is below the reciprocal exactly when that division is exact.
    const auto product = static_cast<unsigned __int128>(reciprocal_) * bytes;
    const auto quotient = static_cast<uint32_t>(product >> 64);
    const bool hasRemainder = static_cast<uint64_t>(product) >= reciprocal_;
    return quotient + (hasRemainder ? 1U : 0U);
  }

 private:
  /**
   * ceil(2^64 / cellSize). With it, the high half of `reciprocal * bytes` is
   * `bytes / cellSize` rounded down for every 32-bit `bytes` and `cellSize`
   * (Lemire, Kaser and Kurz, "Faster Remainder by Direct Computation", 2019).
   *
   * @param cellSize   Size of each cell in bytes.
   *
   * @return           The reciprocal, or 0 for one-byte cells, where it is 2^64
   *                   and does not fit.
   *
   * @throw            std::invalid_argument if `cellSize` is 0.
   */
  static uint64_t getReciprocal(const uint32_t cellSize) {
    if (cellSize == 0) {
      throw std::invalid_argument(
          "Switch buffer cellSizeBytes is 0; a buffer cell must hold at least "
          "one byte");
    }
    // Wraps to 0 for a one-byte cell.
    return std::numeric_limits<uint64_t>::max() / cellSize + 1;
  }

  // The cell size's reciprocal from `getReciprocal`; 0 for one-byte cells.
  uint64_t reciprocal_{0};
};

} // namespace packsim
