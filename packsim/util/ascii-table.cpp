/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/ascii-table.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include <fmt/format.h>

namespace packsim {

AsciiTable::AsciiTable(const std::vector<std::string>& headers)
    : headers_(toRow(headers)) {}

void AsciiTable::addRow(const std::vector<std::string>& cells) {
  if (cells.size() != headers_.size()) {
    throw std::invalid_argument(
        fmt::format(
            "Expect {} cells but got {}", headers_.size(), cells.size()));
  }
  rows_.emplace_back(toRow(cells));
}

AsciiTable::Row AsciiTable::toRow(const std::vector<std::string>& cells) {
  Row row;
  row.reserve(cells.size());
  for (const auto& cell : cells) {
    Cell lines;
    size_t start = 0;
    while (true) {
      const size_t end = cell.find('\n', start);
      if (end == std::string::npos) {
        lines.emplace_back(cell, start, cell.size() - start);
        break;
      }
      lines.emplace_back(cell, start, end - start);
      start = end + 1;
    }
    row.emplace_back(std::move(lines));
  }
  return row;
}

std::vector<size_t> AsciiTable::columnWidths() const {
  std::vector<size_t> widths(headers_.size(), 0);
  const auto widen = [&widths](const Row& row) {
    for (size_t col = 0; col < row.size(); ++col) {
      for (const auto& line : row[col]) {
        widths[col] = std::max(widths[col], line.size());
      }
    }
  };

  widen(headers_);
  for (const auto& row : rows_) {
    widen(row);
  }
  return widths;
}

std::string AsciiTable::separator(const std::vector<size_t>& widths) {
  std::string out{"+"};
  for (const size_t width : widths) {
    // One space of padding either side of the widest cell.
    out.append(width + 2, '-');
    out.push_back('+');
  }
  return out;
}

std::string AsciiTable::renderRow(
    const Row& row,
    const std::vector<size_t>& widths) const {
  size_t height = 1;
  for (const auto& cell : row) {
    height = std::max(height, cell.size());
  }

  std::string out;
  for (size_t line = 0; line < height; ++line) {
    out.push_back('|');
    for (size_t col = 0; col < row.size(); ++col) {
      out.push_back(' ');
      // Cells shorter than the row's tallest are blank on their extra lines.
      const size_t written = line < row[col].size() ? row[col][line].size() : 0;
      if (written > 0) {
        out.append(row[col][line]);
      }
      out.append(widths[col] - written, ' ');
      out.append(" |");
    }
    out.push_back('\n');
  }
  return out;
}

std::string AsciiTable::toString() const {
  const std::vector<size_t> widths = columnWidths();
  const std::string sep = separator(widths);

  std::string out = sep;
  out.push_back('\n');
  out.append(renderRow(headers_, widths));
  out.append(sep);
  out.push_back('\n');
  for (const auto& row : rows_) {
    out.append(renderRow(row, widths));
  }
  out.append(sep);
  return out;
}

} // namespace packsim
