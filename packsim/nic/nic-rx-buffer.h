/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <folly/CppAttributes.h>
#include <folly/portability/GTestProd.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/cbfc/credit-observer.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/queues/queues.h"

namespace packsim {

class NicRxBuffer {
 public:
  /**
   * Build the buffer from `nicConfig`: it drains at `speed`, uses `writeEff`
   * as both efficiency bounds, and passes the perCos lists, queue filter and
   * queue scheduler to the other constructor.
   *
   * @param simCtx     The simulation context that schedules the drains.
   * @param nicConfig  The NIC config to read.
   *
   * @throw std::invalid_argument  If speed is not a data rate,
   *                               perCosBufferSize has fewer entries than
   *                               perCosXoff, or queueFilterCfg maps a DSCP
   *                               that DscpQueueFilter rejects.
   * @throw std::runtime_error     If perCosXoff and perCosXon differ in
   *                               length, perCosXoff has 0 or more than 8
   *                               entries, or the queue filter or scheduler
   *                               type is unknown.
   */
  explicit NicRxBuffer(
      SimContext* simCtx,
      const packsim::thrift::NicConfig* nicConfig);
  /**
   * Build a buffer with one CoS queue per `xoffBytes` entry. `sizeBytes` is
   * not enforced here; the buffer never drops a packet.
   *
   * @param simCtx         The simulation context that schedules the drains.
   * @param drainRate      The rate the buffer drains at, before efficiency.
   * @param minEff         Lower bound of the drain efficiency, in percent,
   *                       sampled once per drained packet.
   * @param maxEff         Upper bound of the drain efficiency, in percent. 0
   *                       replaces both bounds with 85 and 95.
   * @param sizeBytes      Buffer size in bytes per CoS queue. Needs at least
   *                       one entry per queue.
   * @param xoffBytes      PFC XOFF threshold in bytes per CoS queue.
   * @param xonBytes       PFC XON threshold in bytes per CoS queue. A negative
   *                       XOFF or XON entry disables PFC on that queue.
   * @param filterCfg      Maps each packet to its CoS queue.
   * @param schedulerType  Picks the next queue to drain.
   *
   * @throw std::invalid_argument  If sizeBytes has fewer entries than
   *                               xoffBytes, or filterCfg maps a DSCP that
   *                               DscpQueueFilter rejects.
   * @throw std::runtime_error     If xoffBytes and xonBytes differ in length,
   *                               xoffBytes has 0 or more than 8 entries, or
   *                               the filter or scheduler type is unknown.
   */
  NicRxBuffer(
      SimContext* simCtx,
      DataRate drainRate,
      double minEff,
      double maxEff,
      const std::vector<uint32_t>& sizeBytes,
      const std::vector<int32_t>& xoffBytes,
      const std::vector<int32_t>& xonBytes,
      const packsim::thrift::QueueFilterConfig& filterCfg,
      packsim::thrift::SchedulingAlgorithmType schedulerType);
  virtual ~NicRxBuffer() = default;

  using OnPfcStateChangedCallback = std::function<void(uint8_t, PfcState)>;
  using OnPacketReleasedCallback = std::function<void(const PacketPtr&)>;
  std::pair<uint8_t, bool> queuePacket(const PacketPtr& pkt);
  void setOnPfcStateChangedCallback(const OnPfcStateChangedCallback& cb);
  void setOnPacketReleasedCallback(const OnPacketReleasedCallback& cb);
  // Set the observer notified when a packet is stored and when it drains
  // (UEC §5.2.5.7). Non-owning: `observer` must outlive this buffer unless it
  // is cleared with nullptr first.
  void setCreditObserver(cbfc::ICreditObserver* FOLLY_NULLABLE observer);
  PfcState getPfcState(uint8_t queueIdx) const;
  bool allQueuesEmpty();
  uint8_t filter(const PacketPtr& pkt);
  void schedulePop();

 private:
  // XOFF and XON thresholds of one CoS queue, in bytes of queue occupancy.
  struct PfcThresholds {
    uint32_t xoffBytes;
    uint32_t xonBytes;
  };

  void updatePfc(uint8_t queueIdx);
  DataRate getEffectiveRate();
  void doPop(uint8_t queueIdx);

  SimContext* const simCtx_;
  DataRate drainRate_;
  double minEff_{85};
  double maxEff_{95};

  // One entry per CoS queue; `std::nullopt` disables PFC on that queue.
  std::vector<std::optional<PfcThresholds>> pfcThresholds_;

  // Raise the buffer's own events.
  void onPfcStateChanged(uint8_t queueIdx, PfcState state) {
    XCHECK(onPfcStateChanged_)
        << "no pfc-state-changed subscriber registered on rx buffer";
    onPfcStateChanged_(queueIdx, state);
  }
  void onPacketReleased(const PacketPtr& pkt) {
    XCHECK(onPacketReleased_)
        << "no packet-released subscriber registered on rx buffer";
    onPacketReleased_(pkt);
  }

  std::vector<PfcState> pfcStates_{PfcState::XON};
  OnPfcStateChangedCallback onPfcStateChanged_;

  // Never cleared on a CBFC disable: `EthernetPort::disableCbfc` detaches
  // the bridge, leaving this pointer installed but inert.
  cbfc::ICreditObserver* FOLLY_NULLABLE creditObserver_{nullptr};

  std::unique_ptr<Queues> cosQueues_;
  OnPacketReleasedCallback onPacketReleased_;
  bool isSending_{false};

  FOLLY_GTEST_FRIEND_TEST(NicRxBufferTest, TestEfficiency);
  FOLLY_GTEST_FRIEND_TEST(NicRxBufferTest, TestPfcCallback);
};

} // namespace packsim
