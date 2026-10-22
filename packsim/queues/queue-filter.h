/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <string>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

/**
 * Base class of queue filters.
 */
class QueueFilter {
 public:
  /**
   * Selects which queue to push a packet into.
   *
   * @param pkt The packet to be queued
   * @return    The queue index to push to
   */
  virtual uint8_t selectQueueToPush(const PacketPtr& pkt) const = 0;

  /**
   * Colorizes a packet for QoS processing.
   *
   * @param pkt The packet to colorize
   */
  virtual void colorize(const PacketPtr& pkt) const = 0;

  virtual ~QueueFilter() = 0;
};

/**
 * Subclass of filter that does filtering based on DSCP values.
 */
class DscpQueueFilter : public QueueFilter {
 public:
  virtual ~DscpQueueFilter() override {}
  DscpQueueFilter(const DscpQueueFilter&) = delete;
  DscpQueueFilter& operator=(const DscpQueueFilter&) = delete;
  DscpQueueFilter(DscpQueueFilter&&) = delete;
  DscpQueueFilter& operator=(DscpQueueFilter&&) = delete;
  /**
   * @param filterCfg DSCP-to-queue and DSCP-to-color mappings.
   * @throw std::invalid_argument if either mapping has a DSCP key of 64 or
   *        more.
   */
  explicit DscpQueueFilter(const packsim::thrift::QueueFilterConfig& filterCfg);

  /**
   * @param pkt The packet to be queued
   * @return    The queue index to push to
   */
  uint8_t selectQueueToPush(const PacketPtr& pkt) const override;

  /**
   * @param pkt The packet to colorize
   */
  void colorize(const PacketPtr& pkt) const override;

 private:
  // Throws std::invalid_argument naming `mapName` if `dscp` is not a DSCP
  // value.
  static void requireDscpKey(const std::string& mapName, uint32_t dscp);

  // DSCP is a 6-bit field (RFC 2474), so no DSCP value reaches this.
  static constexpr uint32_t kNumDscpValues = 64;

  packsim::thrift::QueueFilterConfig filterCfg_;
};

} // namespace packsim
