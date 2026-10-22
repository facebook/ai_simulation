/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace packsim {

/**
 * Fixed-width ASCII grid for human-readable diagnostic dumps.
 *
 * Renders a `+---+` / `| a |` grid. Every column is sized to its widest line
 * and left-aligned. Deliberately minimal: no sorting, no per-column alignment,
 * no CSV or Markdown export, no width limit, and no ANSI-colour width
 * handling -- add them only when something needs them.
 */
class AsciiTable {
 public:
  /**
   * @param headers One header per column. Fixes the table's column count.
   */
  explicit AsciiTable(const std::vector<std::string>& headers);

  /**
   * Append a row.
   *
   * @param cells   One entry per column.
   *
   * @throw         std::invalid_argument if the count does not match the
   *                headers.
   */
  void addRow(const std::vector<std::string>& cells);

  /**
   * Render the table: a separator line, the header row, a separator, one line
   * per row, and a closing separator with no trailing newline.
   *
   * A cell containing newlines occupies several lines within its own row, so
   * a row is rendered as tall as its tallest cell.
   *
   * @return        The rendered grid, with no trailing newline.
   */
  std::string toString() const;

 private:
  // A cell split on newlines; one entry per rendered line.
  using Cell = std::vector<std::string>;
  using Row = std::vector<Cell>;

  static Row toRow(const std::vector<std::string>& cells);
  static std::string separator(const std::vector<size_t>& widths);

  std::vector<size_t> columnWidths() const;
  std::string renderRow(const Row& row, const std::vector<size_t>& widths)
      const;

  Row headers_;
  std::vector<Row> rows_;
};

} // namespace packsim
