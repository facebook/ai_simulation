/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/filesystem-blob-store.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <fmt/format.h>
#include <folly/FileUtil.h>
#include <folly/Range.h>
#include <folly/String.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>

namespace packsim::io {

namespace {

// `<root>/<path>` with `.` and `..` collapsed. Throws std::invalid_argument if
// `path` holds a NUL byte, is absolute or climbs above `root`.
std::filesystem::path resolveUnderRoot(
    const std::filesystem::path& root,
    const std::string& path) {
  // The file is opened through `c_str()`, which stops at the first NUL, so the
  // checks below would not see the name that is opened.
  if (path.find('\0') != std::string::npos) {
    throw std::invalid_argument(
        fmt::format(
            "Blob path '{}' must not contain a NUL byte",
            folly::cEscape<std::string>(path)));
  }
  const std::filesystem::path requested{path};
  const auto normal = requested.lexically_normal();
  // lexically_normal leaves `..` only at the front of a relative path, and only
  // when the path climbs above where it starts.
  if (requested.has_root_path() ||
      (!normal.empty() && *normal.begin() == "..")) {
    throw std::invalid_argument(
        fmt::format(
            "Blob path '{}' must be relative and stay under the store root '{}'",
            path,
            root.string()));
  }
  return root / normal;
}

} // namespace

FilesystemBlobStore::FilesystemBlobStore(std::filesystem::path root)
    : root_{std::move(root)} {}

std::unique_ptr<folly::IOBuf> FilesystemBlobStore::read(
    const std::string& path) {
  const auto fullPath = resolveUnderRoot(root_, path);
  XLOGF(DBG, "Reading blob from {}", fullPath.string());

  std::string contents;
  if (!folly::readFile(fullPath.c_str(), contents)) {
    throw std::runtime_error(
        fmt::format("Failed to read blob at {}", fullPath.string()));
  }
  // fromString takes ownership of the string's buffer rather than copying it,
  // so readFile's fill is the only copy the read path pays.
  return folly::IOBuf::fromString(std::move(contents));
}

void FilesystemBlobStore::write(
    const std::string& path,
    std::unique_ptr<folly::IOBuf> contents,
    std::chrono::hours /* ttl */) {
  const auto fullPath = resolveUnderRoot(root_, path);
  XLOGF(DBG, "Writing blob to {}", fullPath.string());

  const auto parent = fullPath.parent_path();
  if (!parent.empty()) {
    // The error_code overload is what avoids throwing on the benign race where
    // a concurrent writer creates the directory between the check and the call.
    std::error_code ec;
    std::filesystem::create_directories(parent, ec);
    if (ec && !std::filesystem::is_directory(parent)) {
      throw std::runtime_error(
          fmt::format(
              "Failed to create directory {}: {}",
              parent.string(),
              ec.message()));
    }
  }

  // coalesce() flattens a chained IOBuf into one contiguous range and hands it
  // back, which is what writeFile wants -- a chain would otherwise write only
  // its first buffer.
  const auto bytes = contents->coalesce();
  if (!folly::writeFile(folly::StringPiece(bytes), fullPath.c_str())) {
    throw std::runtime_error(
        fmt::format("Failed to write blob at {}", fullPath.string()));
  }
}

} // namespace packsim::io
