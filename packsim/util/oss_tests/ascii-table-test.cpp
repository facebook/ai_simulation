/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/ascii-table.h"

#include <stdexcept>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// A table with no rows still renders its header between two separators.
TEST(AsciiTableTest, RendersHeadersWithNoRows) {
  const AsciiTable table({"Route", "Links"});

  EXPECT_EQ(
      table.toString(),
      "+-------+-------+\n"
      "| Route | Links |\n"
      "+-------+-------+\n"
      "+-------+-------+");
}

// The grid packsim's flex-node dump produces: header, rows, closing separator
// and no trailing newline.
TEST(AsciiTableTest, RendersGridWithRows) {
  AsciiTable table({"Route", "Links", "Tiebreak"});
  table.addRow({"a, b", "l1", "HASH"});
  table.addRow({"default", "(no links)", "ROUND_ROBIN"});

  EXPECT_EQ(
      table.toString(),
      "+---------+------------+-------------+\n"
      "| Route   | Links      | Tiebreak    |\n"
      "+---------+------------+-------------+\n"
      "| a, b    | l1         | HASH        |\n"
      "| default | (no links) | ROUND_ROBIN |\n"
      "+---------+------------+-------------+");
}

// A column is as wide as its widest entry, header included.
TEST(AsciiTableTest, SizesEachColumnToItsWidestEntry) {
  AsciiTable table({"Short", "H"});
  table.addRow({"x", "a much longer cell"});

  EXPECT_EQ(
      table.toString(),
      "+-------+--------------------+\n"
      "| Short | H                  |\n"
      "+-------+--------------------+\n"
      "| x     | a much longer cell |\n"
      "+-------+--------------------+");
}

// A cell containing newlines occupies several lines inside its own row, and
// the other columns in that row are blank-padded to match.
TEST(AsciiTableTest, RendersMultiLineCellAcrossSeveralLines) {
  AsciiTable table({"Key", "Value"});
  table.addRow({"k", "first\nsecond"});

  EXPECT_EQ(
      table.toString(),
      "+-----+--------+\n"
      "| Key | Value  |\n"
      "+-----+--------+\n"
      "| k   | first  |\n"
      "|     | second |\n"
      "+-----+--------+");
}

// An empty cell is rendered as padding, not skipped.
TEST(AsciiTableTest, RendersEmptyCellAsPadding) {
  AsciiTable table({"Key", "Value"});
  table.addRow({"k", ""});

  EXPECT_EQ(
      table.toString(),
      "+-----+-------+\n"
      "| Key | Value |\n"
      "+-----+-------+\n"
      "| k   |       |\n"
      "+-----+-------+");
}

// A row whose width does not match the header is a caller error.
TEST(AsciiTableTest, ThrowsWhenRowWidthDoesNotMatchHeaders) {
  AsciiTable table({"Route", "Links"});

  EXPECT_THAT(
      [&table]() { table.addRow({"only-one"}); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("Expect 2 cells but got 1")));
}

} // namespace packsim
