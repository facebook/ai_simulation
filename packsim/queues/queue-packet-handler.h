/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cmath>
#include <cstdint>
#include <memory>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/util/periodic-update-value.h"

namespace packsim {

class Queues;

enum class QueuePacketHandlerType : uint8_t {
  UNKNOWN = 0,
  DONOTHING = 1,
  WRED = 2, // weighted random early detection
  ECN = 3,
  VOQ_ECN = 4,
  WRED_ECN = 5,
};

class WREDAverageQueueLength : public packsim::PeriodicUpdateValue<uint64_t> {
 public:
  WREDAverageQueueLength(
      SimContext* simCtx,
      double exp,
      Time updatePeriod,
      Time startTime)
      : packsim::PeriodicUpdateValue<uint64_t>(
            simCtx,
            0,
            updatePeriod,
            startTime) {
    coef_ = 1 / std::exp2(exp);
  }

  void updateValueOnce() override {
    if (period_.getNs() > 0) {
      value_ = (1 - coef_) * value_ + qLen_ * coef_;
    } else {
      value_ = qLen_;
    }
  }

  void updateValueNTimes(uint64_t n) override {
    for (uint64_t i = 0; i < n; ++i) {
      const uint64_t previous = value_;
      updateValueOnce();
      // qLen_ is fixed during a catch-up, so a value that one update leaves
      // unchanged stays unchanged for the rest of it.
      if (value_ == previous) {
        return;
      }
    }
  }

  void setQLen(uint64_t val) {
    qLen_ = val;
  }

 protected:
  double coef_{1};
  uint64_t qLen_{0};
};

/**
 * Base class of queue packet handler.
 *
 * Handles a packet after filtering/scheduling. Queues will call these
 * functions after selected the queue to push/pop and have additional
 * actions on the packet (drop/modify/etc.).
 * Returns a bool to indicate whether Queues should push/pop the packet.
 */
class QueuePacketHandler {
 public:
  QueuePacketHandler(
      SimContext* simCtx,
      uint8_t id,
      const packsim::thrift::WredEcnConfig& cfg,
      Queues* queues);
  /**
   * Packet should be dropped if returns false.
   *
   * @param pkt  The packet to handle
   * @return     Whether the packet should be kept (true) or dropped (false)
   */
  virtual bool handlePacketAfterFiltering(const PacketPtr& pkt) = 0;

  /**
   * Returns true if the packet was ECN marked in this method, false otherwise.
   *
   * @param pkt  The packet to handle
   * @return     Whether the packet was ECN marked
   */
  virtual bool handlePacketAfterScheduling(const PacketPtr& pkt) = 0;
  virtual ~QueuePacketHandler() = 0;
  virtual QueuePacketHandlerType getQueuePacketHandlerType() = 0;
  void setQLen(uint64_t val) {
    avgQLen_.setQLen(val);
  }

  // Each queue has a packet handler
  uint8_t qId;

 protected:
  SimContext* const simCtx_;
  WREDAverageQueueLength avgQLen_;

  uint32_t minThresholdWRED_;
  uint32_t maxThresholdWRED_;
  // drop curve in percentage
  uint32_t dropPercentageWRED_;
  uint32_t minThresholdECN_;
  uint32_t maxThresholdECN_;
  uint32_t markPercentageECN_;
};

/** Default subclass that does nothing. */
class DoNothingPacketHandler : public QueuePacketHandler {
 public:
  DoNothingPacketHandler(
      SimContext* simCtx,
      uint8_t id,
      const packsim::thrift::WredEcnConfig& cfg,
      Queues* queues);
  virtual ~DoNothingPacketHandler() override = default;
  DoNothingPacketHandler(const DoNothingPacketHandler&) = delete;
  DoNothingPacketHandler& operator=(const DoNothingPacketHandler&) = delete;
  DoNothingPacketHandler(DoNothingPacketHandler&&) = delete;
  DoNothingPacketHandler& operator=(DoNothingPacketHandler&&) = delete;
  bool handlePacketAfterFiltering(const PacketPtr& /*pkt*/) override {
    return true;
  }

  bool handlePacketAfterScheduling(const PacketPtr& /*pkt*/) override {
    return false;
  }

  QueuePacketHandlerType getQueuePacketHandlerType() override {
    return QueuePacketHandlerType::DONOTHING;
  }
};

/**
 * Subclass of packet handler that does WRED (Weighted Random Early Detection).
 *
 * WRED maintains an average queue length. When a packet is going to push into
 * this queue, it compares the average queue length with thresholds. If smaller
 * than min, push the packet; if greater than max, drop. Otherwise drop the
 * packet with a probability.
 */
class WREDQueuePacketHandler : public QueuePacketHandler {
 public:
  WREDQueuePacketHandler(
      SimContext* simCtx,
      uint8_t id,
      const packsim::thrift::WredEcnConfig& cfg,
      Queues* queues);
  virtual ~WREDQueuePacketHandler() override = default;
  WREDQueuePacketHandler(const WREDQueuePacketHandler&) = delete;
  WREDQueuePacketHandler& operator=(const WREDQueuePacketHandler&) = delete;
  WREDQueuePacketHandler(WREDQueuePacketHandler&&) = delete;
  WREDQueuePacketHandler& operator=(WREDQueuePacketHandler&&) = delete;
  // WRED first check avg queue length, if below min, push; if above max,
  // drop; Otherwise, drop the packet with a drop probability
  bool handlePacketAfterFiltering(const PacketPtr& pkt) override;
  bool handlePacketAfterScheduling(const PacketPtr& pkt) override;
  QueuePacketHandlerType getQueuePacketHandlerType() override;
  // Sets the WRED thresholds and drop percentage.
  void setParams(uint32_t minThres, uint32_t maxThres, uint32_t dropPer);
};

/** Subclass of packet handler that does ECN (Explicit Congestion Notification).
 */
class ECNQueuePacketHandler : public QueuePacketHandler {
 public:
  ECNQueuePacketHandler(
      SimContext* simCtx,
      uint8_t id,
      const packsim::thrift::WredEcnConfig& cfg,
      Queues* queues);
  virtual ~ECNQueuePacketHandler() override = default;
  ECNQueuePacketHandler(const ECNQueuePacketHandler&) = delete;
  ECNQueuePacketHandler& operator=(const ECNQueuePacketHandler&) = delete;
  ECNQueuePacketHandler(ECNQueuePacketHandler&&) = delete;
  ECNQueuePacketHandler& operator=(ECNQueuePacketHandler&&) = delete;
  bool handlePacketAfterFiltering(const PacketPtr& pkt) override;
  bool handlePacketAfterScheduling(const PacketPtr& pkt) override;
  QueuePacketHandlerType getQueuePacketHandlerType() override;
  // Sets the ECN thresholds and mark percentage.
  void setParams(uint32_t minThres, uint32_t maxThres, uint32_t markPer);
};

/**
 * Subclass of packet handler that does VOQ (Virtual Output Queue) based ECN.
 */
class VoqECNQueuePacketHandler : public QueuePacketHandler {
 public:
  VoqECNQueuePacketHandler(
      SimContext* simCtx,
      uint8_t id,
      const packsim::thrift::WredEcnConfig& cfg,
      Queues* queues);
  virtual ~VoqECNQueuePacketHandler() override = default;
  VoqECNQueuePacketHandler(const VoqECNQueuePacketHandler&) = delete;
  VoqECNQueuePacketHandler& operator=(const VoqECNQueuePacketHandler&) = delete;
  VoqECNQueuePacketHandler(VoqECNQueuePacketHandler&&) = delete;
  VoqECNQueuePacketHandler& operator=(VoqECNQueuePacketHandler&&) = delete;
  bool handlePacketAfterFiltering(const PacketPtr& pkt) override;
  bool handlePacketAfterScheduling(const PacketPtr& pkt) override;
  QueuePacketHandlerType getQueuePacketHandlerType() override;
  bool markEcnPacket(const PacketPtr& pkt, uint64_t queLength);
  // Sets the ECN thresholds and mark percentage.
  void setParams(uint32_t minThres, uint32_t maxThres, uint32_t markPer);
};

/**
 * Subclass of packet handler that picks WRED or ECN per packet from its ECN
 * field. A Not-ECT packet follows the WRED drop curve on enqueue and is never
 * marked; any other packet is admitted and follows the ECN mark curve on
 * dequeue.
 */
class WREDECNQueuePacketHandler : public QueuePacketHandler {
 public:
  WREDECNQueuePacketHandler(
      SimContext* simCtx,
      uint8_t id,
      const packsim::thrift::WredEcnConfig& cfg,
      Queues* queues);
  virtual ~WREDECNQueuePacketHandler() override = default;
  WREDECNQueuePacketHandler(const WREDECNQueuePacketHandler&) = delete;
  WREDECNQueuePacketHandler& operator=(const WREDECNQueuePacketHandler&) =
      delete;
  WREDECNQueuePacketHandler(WREDECNQueuePacketHandler&&) = delete;
  WREDECNQueuePacketHandler& operator=(WREDECNQueuePacketHandler&&) = delete;
  bool handlePacketAfterFiltering(const PacketPtr& pkt) override;
  bool handlePacketAfterScheduling(const PacketPtr& pkt) override;
  QueuePacketHandlerType getQueuePacketHandlerType() override;
  // Sets the WRED and ECN thresholds and percentages.
  void setParams(
      uint32_t minThresWRED,
      uint32_t maxThresWRED,
      uint32_t dropPerWRED,
      uint32_t minThresECN,
      uint32_t maxThresECN,
      uint32_t markPerECN);
};

std::unique_ptr<QueuePacketHandler> buildHandler(
    SimContext* simCtx,
    uint8_t id,
    const packsim::thrift::WredEcnConfig& cfg,
    Queues* queues);

} // namespace packsim
