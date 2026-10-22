/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <folly/io/IOBuf.h>

#include "packsim/if/gen-cpp2/config_types.h"

namespace packsim::io {
enum class SerializationType {
  // No output requested
  NONE,
  // Simple human readable format
  JSON,
  // Compact format, small size
  COMPACT,
  // Closer to memory representation, might be more performant
  BINARY
};

enum class CompressionType {
  // don't apply any compression
  NONE,
  // GZIP output
  GZIP,
  // ZSTD
  ZSTD
};

struct FileType {
  /**
   * Classify a file name by its trailing suffixes: `.json`, `.cf` or `.bin`,
   * optionally followed by `.gz` or `.zstd`, matched case-insensitively. An
   * empty name selects no serialization and no compression.
   *
   * @param filename  The file or blob path to classify.
   *
   * @throw std::invalid_argument  If the name has no supported suffix or
   *                               contains a NUL byte.
   */
  explicit FileType(const std::string& filename);
  SerializationType serializationType{SerializationType::NONE};
  CompressionType compressionType{CompressionType::NONE};

  /**
   * Test if the file types are same.
   */
  bool operator==(FileType const& that) const noexcept {
    return serializationType == that.serializationType &&
        compressionType == that.compressionType;
  }

  /**
   * Test if the file types are different.
   */
  bool operator!=(FileType const& that) const noexcept {
    return !(*this == that);
  }
};

packsim::thrift::Simulation deserializeConfig(
    folly::IOBuf* buf,
    const FileType& type);
packsim::thrift::Simulation readConfigFromFile(const std::string& path);
} // namespace packsim::io
