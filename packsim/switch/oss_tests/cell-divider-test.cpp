/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <tuple>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/switch/cell-divider.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace {
constexpr uint32_t kMaxBytes = std::numeric_limits<uint32_t>::max();

// One-byte cells, whose reciprocal does not fit in 64 bits; small odd sizes;
// the usual buffer cell sizes and their power-of-two neighbours; and the
// largest cell sizes 32 bits can hold.
constexpr std::array<uint32_t, 14> kCellSizes{
    1U,
    2U,
    3U,
    7U,
    64U,
    254U,
    255U,
    256U,
    511U,
    512U,
    4096U,
    65535U,
    2147483648U,
    kMaxBytes};

// The largest frame a port carries: a 9000-byte jumbo frame.
constexpr uint32_t kMaxFrameBytes = 9000;

// The reference count: the ceiling of `bytes / cellSize` by plain division, in
// 64 bits so that `bytes + cellSize - 1` cannot wrap.
uint32_t getReferenceCells(const uint32_t bytes, const uint32_t cellSize) {
  return static_cast<uint32_t>((uint64_t{bytes} + cellSize - 1) / cellSize);
}
} // namespace

TEST(CellDividerGetCellsFromBytes, ComputesKnownCellCounts) {
  const CellDivider divider(254);
  EXPECT_EQ(divider.getCellsFromBytes(0), 0U);
  EXPECT_EQ(divider.getCellsFromBytes(1), 1U);
  EXPECT_EQ(divider.getCellsFromBytes(254), 1U);
  EXPECT_EQ(divider.getCellsFromBytes(255), 2U);
  EXPECT_EQ(divider.getCellsFromBytes(508), 2U);
  EXPECT_EQ(divider.getCellsFromBytes(1500), 6U);
}

TEST(CellDividerGetCellsFromBytes, DoesNotWrapNearUint32Max) {
  // `bytes + cellSize - 1` would wrap in 32 bits here.
  EXPECT_EQ(CellDivider(2).getCellsFromBytes(kMaxBytes), 2147483648U);
}

TEST(CellDividerGetCellsFromBytes, MatchesWideDivisionAroundCellBoundaries) {
  for (const uint32_t cellSize : kCellSizes) {
    const CellDivider divider(cellSize);
    const uint64_t size = cellSize;
    for (const uint64_t bytes :
         {uint64_t{0},
          uint64_t{1},
          size - 1,
          size,
          size + 1,
          2 * size - 1,
          2 * size,
          2 * size + 1,
          uint64_t{kMaxFrameBytes},
          uint64_t{kMaxBytes} - 1,
          uint64_t{kMaxBytes}}) {
      if (bytes > kMaxBytes) {
        continue;
      }
      const auto narrowBytes = static_cast<uint32_t>(bytes);
      EXPECT_EQ(
          divider.getCellsFromBytes(narrowBytes),
          getReferenceCells(narrowBytes, cellSize))
          << "bytes=" << bytes << " cellSize=" << cellSize;
    }
  }
}

TEST(CellDividerGetCellsFromBytes, MatchesWideDivisionForEveryJumboFrameSize) {
  for (const uint32_t cellSize : {1U, 64U, 254U, 511U, 512U, 4096U}) {
    const CellDivider divider(cellSize);
    for (uint32_t bytes = 0; bytes <= kMaxFrameBytes; ++bytes) {
      ASSERT_EQ(
          divider.getCellsFromBytes(bytes), getReferenceCells(bytes, cellSize))
          << "bytes=" << bytes << " cellSize=" << cellSize;
    }
  }
}

TEST(CellDividerGetCellsFromBytes, MatchesWideDivisionForRandomInputs) {
  constexpr uint64_t kSeed = 12345;
  constexpr int kDraws = 100000;
  // Half the draws use a cell size up to 64 KiB, where real buffers are, and
  // half any 32-bit cell size.
  constexpr uint32_t kMaxSmallCellSize = 65536;
  std::mt19937_64 rng(kSeed);
  std::uniform_int_distribution<uint32_t> anyBytes(0, kMaxBytes);
  std::uniform_int_distribution<uint32_t> smallCellSize(1, kMaxSmallCellSize);
  std::uniform_int_distribution<uint32_t> anyCellSize(1, kMaxBytes);
  for (int draw = 0; draw < kDraws; ++draw) {
    const uint32_t cellSize =
        (draw % 2 == 0) ? smallCellSize(rng) : anyCellSize(rng);
    const uint32_t bytes = anyBytes(rng);
    ASSERT_EQ(
        CellDivider(cellSize).getCellsFromBytes(bytes),
        getReferenceCells(bytes, cellSize))
        << "bytes=" << bytes << " cellSize=" << cellSize;
  }
}

TEST(CellDividerGetCellsFromBytes, CountsEveryByteForOneByteCells) {
  const CellDivider divider(1);
  for (const uint32_t bytes : {0U, 1U, 2U, kMaxFrameBytes, kMaxBytes}) {
    EXPECT_EQ(divider.getCellsFromBytes(bytes), bytes) << "bytes=" << bytes;
  }
}

TEST(CellDividerGetCellsFromBytes, DefaultCountsEveryByte) {
  const CellDivider divider;
  for (const uint32_t bytes : {0U, 1U, 2U, kMaxFrameBytes, kMaxBytes}) {
    EXPECT_EQ(divider.getCellsFromBytes(bytes), bytes) << "bytes=" << bytes;
  }
}

TEST(CellDividerConstructor, RejectsZeroCellSize) {
  EXPECT_THAT(
      [] { std::ignore = CellDivider(0); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("cellSizeBytes is 0")));
}

} // namespace packsim
