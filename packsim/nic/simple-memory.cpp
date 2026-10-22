/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/nic/simple-memory.h"

#include <cmath>

#include <folly/logging/xlog.h>

namespace packsim {
uint64_t SimpleMemory::getReadDelayNs(
    const size_t readSize,
    const uint64_t timeNs) {
  return getReadDelayNs(readSize, timeNs, readLatencyNs_);
}

uint64_t SimpleMemory::getReadDelayNs(
    const size_t readSize,
    const uint64_t timeNs,
    const double latencyNs) {
  const double time = timeNs;

  // Delay solely due to memory bandwidth
  const double bwDelay = readSize / readBytesPerNs_;
  const auto prevLastReadCompleteNs = lastReadCompleteNs_;
  // If last read completes after read latency, can ignore the latency
  if ((time + latencyNs) < lastReadCompleteNs_) {
    lastReadCompleteNs_ += bwDelay;
  } else {
    lastReadCompleteNs_ = time + bwDelay + latencyNs;
  }
  // if the read size is too small, we need to add a minimum delay
  // to avoid a race condition between 2 packets on the wire as they
  // will be both available to be sent at the same time.
  if (std::floor(lastReadCompleteNs_) == std::floor(prevLastReadCompleteNs)) {
    lastReadCompleteNs_ += 1.0;
  } else if (
      std::floor(lastReadCompleteNs_) < std::floor(prevLastReadCompleteNs)) {
    XLOGF(FATAL, "lastReadCompleteNs_ is less than prevLastReadCompleteNs_");
  }
  return lastReadCompleteNs_ - time;
}

uint64_t SimpleMemory::getWriteDelayNs(
    const size_t writeSize,
    const uint64_t timeNs) {
  return getWriteDelayNs(writeSize, timeNs, writeLatencyNs_);
}

uint64_t SimpleMemory::getWriteDelayNs(
    const size_t writeSize,
    const uint64_t timeNs,
    const double latencyNs) {
  const double time = timeNs;
  // Delay solely due to memory bandwidth
  const double bwDelay = writeSize / writeBytesPerNs_;
  const auto prevLastWriteCompleteNs = lastWriteCompleteNs_;
  // If last write completes after write latency, can ignore the latency
  if ((time + latencyNs) < lastWriteCompleteNs_) {
    lastWriteCompleteNs_ += bwDelay;
  } else {
    lastWriteCompleteNs_ = time + bwDelay + latencyNs;
  }
  if (std::floor(lastWriteCompleteNs_) == std::floor(prevLastWriteCompleteNs)) {
    lastWriteCompleteNs_ += 1.0;
  } else if (
      std::floor(lastWriteCompleteNs_) < std::floor(prevLastWriteCompleteNs)) {
    XLOGF(FATAL, "lastWriteCompleteNs_ is less than prevLastWriteCompleteNs_");
  }
  return lastWriteCompleteNs_ - time;
}
} // namespace packsim
