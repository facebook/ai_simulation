/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>

namespace packsim {

/**
 * Simple memory model with configurable bandwidth and latency.
 */
class SimpleMemory {
 public:
  /**
   * Construct a SimpleMemory with symmetric read/write parameters.
   *
   * @param readWriteBytesPerNs Bandwidth for both reads and writes
   * @param readWriteLatencyNs  Latency for both reads and writes
   */
  explicit SimpleMemory(double readWriteBytesPerNs, double readWriteLatencyNs)
      : readBytesPerNs_(readWriteBytesPerNs),
        writeBytesPerNs_(readWriteBytesPerNs),
        readLatencyNs_(readWriteLatencyNs),
        writeLatencyNs_(readWriteLatencyNs) {}

  /**
   * Construct a SimpleMemory with separate read/write parameters.
   *
   * @param readBytesPerNs   Read bandwidth in bytes per nanosecond
   * @param writeBytesPerNs  Write bandwidth in bytes per nanosecond
   * @param readLatencyNs    Read latency in nanoseconds
   * @param writeLatencyNs   Write latency in nanoseconds
   */
  SimpleMemory(
      double readBytesPerNs,
      double writeBytesPerNs,
      double readLatencyNs,
      double writeLatencyNs)
      : readBytesPerNs_(readBytesPerNs),
        writeBytesPerNs_(writeBytesPerNs),
        readLatencyNs_(readLatencyNs),
        writeLatencyNs_(writeLatencyNs) {}

  /**
   * Get the delay for a read operation.
   *
   * @param readSize Size of the read in bytes
   * @param timeNs   Current time in nanoseconds
   * @return Delay in nanoseconds until read completes
   */
  uint64_t getReadDelayNs(size_t readSize, uint64_t timeNs);

  /**
   * Get the delay for a read whose fixed latency the caller supplies, for a
   * memory whose latency depends on the address read.
   *
   * @param readSize   Size of the read in bytes
   * @param timeNs     Current time in nanoseconds
   * @param latencyNs  Fixed latency of this read in nanoseconds, used in place
   *                   of the configured read latency
   * @return Delay in nanoseconds until read completes
   */
  [[nodiscard]] uint64_t
  getReadDelayNs(size_t readSize, uint64_t timeNs, double latencyNs);

  /**
   * Get the delay for a write operation.
   *
   * @param writeSize Size of the write in bytes
   * @param timeNs    Current time in nanoseconds
   * @return Delay in nanoseconds until write completes
   */
  uint64_t getWriteDelayNs(size_t writeSize, uint64_t timeNs);

  /**
   * Get the delay for a write whose fixed latency the caller supplies, for a
   * memory whose latency depends on the address written.
   *
   * @param writeSize  Size of the write in bytes
   * @param timeNs     Current time in nanoseconds
   * @param latencyNs  Fixed latency of this write in nanoseconds, used in place
   *                   of the configured write latency
   * @return Delay in nanoseconds until write completes
   */
  [[nodiscard]] uint64_t
  getWriteDelayNs(size_t writeSize, uint64_t timeNs, double latencyNs);

  /** @return Read latency in nanoseconds */
  double getReadLatencyNs() const noexcept {
    return readLatencyNs_;
  }

  /** @return Read bandwidth in bytes per nanosecond */
  double getReadBytesPerNs() const noexcept {
    return readBytesPerNs_;
  }

  /**
   * Compute time to fetch a packet from memory (latency + bandwidth).
   *
   * @param packetSizeBytes Size of the packet in bytes
   * @return Time in nanoseconds to fetch the packet
   */
  uint64_t computePacketFetchTimeNs(size_t packetSizeBytes) const noexcept {
    return readLatencyNs_ + packetSizeBytes / readBytesPerNs_;
  }

  /**
   * Compute minimum queue depth needed to keep link busy during memory fetch.
   * Returns the number of packets that can be transmitted during one packet
   * fetch.
   *
   * @param linkBps          Link bandwidth in bits per second
   * @param packetSizeBytes  Size of packets in bytes
   * @return Minimum queue depth (returns 1 if linkBps is 0 to avoid division
   *         by zero)
   */
  size_t computeMinQueueDepthForLink(uint64_t linkBps, size_t packetSizeBytes)
      const noexcept {
    if (linkBps == 0) {
      return 1;
    }
    uint64_t fetchTimeNs = computePacketFetchTimeNs(packetSizeBytes);
    uint64_t packetBits = packetSizeBytes * 8;
    double transmitTimeNs = packetBits / (linkBps / 1e9);
    return std::ceil(fetchTimeNs / transmitTimeNs);
  }

 private:
  // Read bandwidth in bytes per nanosecond
  double readBytesPerNs_{1};
  // Write bandwidth in bytes per nanosecond
  double writeBytesPerNs_{1};
  // Time when the last read operation completed
  double lastReadCompleteNs_{0};
  // Time when the last write operation completed
  double lastWriteCompleteNs_{0};
  // Read latency in nanoseconds
  double readLatencyNs_{500};
  // Write latency in nanoseconds
  double writeLatencyNs_{500};
};

} // namespace packsim
