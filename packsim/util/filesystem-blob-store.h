/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include <folly/io/IOBuf.h>

#include "packsim/util/blob-store.h"

namespace packsim::io {

/**
 * IBlobStore backed by the local filesystem.
 *
 * This is the open-source default. `IBlobStore` addresses an object as
 * "<bucket>/<object path>", which maps onto a filesystem as a directory named
 * for the bucket under a root: a path is resolved as `<root>/<path>`, with the
 * bucket becoming its first component, so the same string addresses the same
 * object under either store.
 *
 * A path must stay under the root. `read` and `write` reject an absolute path,
 * one whose `..` components climb above the root and one holding a NUL byte, so
 * a path taken from a config or a flag cannot reach files elsewhere. The check
 * is lexical: `.` and `..` are collapsed without consulting the filesystem, and
 * a symlink under the root is followed wherever it points.
 *
 * Example usage:
 *
 *      FilesystemBlobStore store("/tmp/packsim");
 *      // reads /tmp/packsim/my_bucket/configs/sim.json
 *      auto buf = store.read("my_bucket/configs/sim.json");
 *
 * NOTE: A filesystem has no expiry, so `write`'s `ttl` is accepted and
 * ignored -- see the note on `write`.
 */
class FilesystemBlobStore final : public IBlobStore {
 public:
  /**
   * @param root  Directory every blob path is resolved against. Defaults to
   *              the process working directory, which makes a bare
   *              `FilesystemBlobStore{}` behave like relative paths on disk.
   */
  explicit FilesystemBlobStore(std::filesystem::path root = ".");

  /**
   * Read the file at `<root>/<path>`.
   *
   * @param path  Object path, "<bucket>/<object path>".
   *
   * @return      The file's contents.
   *
   * @throw std::invalid_argument If `path` is absolute or climbs above the
   *                              root.
   * @throw std::runtime_error If the file is missing or cannot be read.
   */
  std::unique_ptr<folly::IOBuf> read(const std::string& path) override;

  /**
   * Write contents to `<root>/<path>`, creating any missing parent
   * directories and overwriting a file already there.
   *
   * @param path      Object path, "<bucket>/<object path>".
   * @param contents  Bytes to store.
   * @param ttl       Ignored. A filesystem does not expire files, and
   *                  silently deleting the user's data later would be a worse
   *                  answer than keeping it: retention is the caller's
   *                  business here. Kept in the signature because it is part
   *                  of the `IBlobStore` contract.
   *
   * @throw std::invalid_argument If `path` is absolute or climbs above the
   *                              root; nothing is written or created.
   * @throw std::runtime_error If the write fails.
   */
  void write(
      const std::string& path,
      std::unique_ptr<folly::IOBuf> contents,
      std::chrono::hours ttl) override;

 private:
  // Directory every blob path is resolved against.
  std::filesystem::path root_;
};

} // namespace packsim::io
