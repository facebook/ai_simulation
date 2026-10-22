/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <memory>
#include <string>

#include <folly/io/IOBuf.h>

namespace packsim::io {

/**
 * Object-store seam for the blobs packsim reads and writes outside the local
 * filesystem: replay configs and simulation results.
 *
 * The concrete store is injected rather than constructed here, so that only
 * the code implementing this interface depends on which object store is in
 * use, and so that tests can substitute a fake instead of reaching the
 * network.
 *
 * A path is "<bucket>/<object path>". Splitting it is the implementation's
 * job, because what leads the path is store-specific.
 */
class IBlobStore {
 public:
  virtual ~IBlobStore() = default;

  /**
   * Read the object stored at a path.
   *
   * @param path  Object path, "<bucket>/<object path>".
   *
   * @return      The object's contents.
   *
   * @throw std::exception If the object is missing or cannot be read.
   */
  virtual std::unique_ptr<folly::IOBuf> read(const std::string& path) = 0;

  /**
   * Write contents to a path, creating any missing parent directories and
   * overwriting an object already stored there.
   *
   * @param path      Object path, "<bucket>/<object path>".
   * @param contents  Bytes to store.
   * @param ttl       How long the store should retain the object.
   *
   * @throw std::exception If the write fails.
   */
  virtual void write(
      const std::string& path,
      std::unique_ptr<folly::IOBuf> contents,
      std::chrono::hours ttl) = 0;
};

} // namespace packsim::io
