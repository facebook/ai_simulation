/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/nic/nic-rx-buffer.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <utility>

#include <fmt/format.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/time.h"

namespace packsim {

namespace {
// Default write-efficiency bounds (percent) applied when NicConfig.writeEff is
// 0.
constexpr double kDefaultMinEffPct = 85;
constexpr double kDefaultMaxEffPct = 95;
// Efficiency is expressed as a percentage; divide by this to get a fraction.
constexpr double kPercentDivisor = 100.0;
} // namespace

NicRxBuffer::NicRxBuffer(
    SimContext* simCtx,
    const packsim::thrift::NicConfig* nicConfig)
    : NicRxBuffer(
          simCtx,
          DataRate(*nicConfig->speed()),
          *nicConfig->writeEff(),
          *nicConfig->writeEff(),
          *nicConfig->perCosBufferSize(),
          *nicConfig->perCosXoff(),
          *nicConfig->perCosXon(),
          *nicConfig->queueFilterCfg(),
          *nicConfig->queueScheduleCfg()) {}

NicRxBuffer::NicRxBuffer(
    SimContext* simCtx,
    DataRate drainRate,
    const double minEff,
    const double maxEff,
    const std::vector<uint32_t>& sizeBytes,
    const std::vector<int32_t>& xoffBytes,
    const std::vector<int32_t>& xonBytes,
    const packsim::thrift::QueueFilterConfig& filterCfg,
    const packsim::thrift::SchedulingAlgorithmType schCfg)
    : simCtx_(simCtx),
      drainRate_(std::move(drainRate)),
      minEff_(minEff),
      maxEff_(maxEff) {
  if (xoffBytes.size() != xonBytes.size()) {
    throw std::runtime_error("Sizes of XOFF and XON vectors must match");
  }
  if (sizeBytes.size() < xoffBytes.size()) {
    throw std::invalid_argument(
        fmt::format(
            "NicConfig.perCosBufferSize needs an entry for each of the {} CoS "
            "queues perCosXoff defines, but has {}",
            xoffBytes.size(),
            sizeBytes.size()));
  }

  const size_t numQueues = xoffBytes.size();
  pfcThresholds_.reserve(numQueues);
  for (size_t i = 0; i < numQueues; ++i) {
    if ((xoffBytes[i] < 0) || (xonBytes[i] < 0)) {
      pfcThresholds_.emplace_back(std::nullopt);
    } else {
      pfcThresholds_.emplace_back(
          PfcThresholds{
              .xoffBytes = static_cast<uint32_t>(xoffBytes[i]),
              .xonBytes = static_cast<uint32_t>(xonBytes[i])});
    }
  }
  pfcStates_.resize(numQueues, PfcState::XON);
  const std::vector<packsim::thrift::PortQueueConfig> portQueueCfg(numQueues);
  cosQueues_ =
      std::make_unique<Queues>(simCtx_, filterCfg, schCfg, portQueueCfg);

  if (maxEff_ == 0) {
    minEff_ = kDefaultMinEffPct;
    maxEff_ = kDefaultMaxEffPct;
  }
}

uint8_t NicRxBuffer::filter(const PacketPtr& pkt) {
  return cosQueues_->filter(pkt);
}

std::pair<uint8_t, bool> NicRxBuffer::queuePacket(const PacketPtr& pkt) {
  const auto [queueIdx, enqueue] = cosQueues_->pushPacket(pkt);
  if (!enqueue) {
    return {0, false};
  }

  // Every store must be balanced by exactly one `onPacketDrained` in `doPop`;
  // a path that discards a queued packet without draining it strands its
  // credit.
  if (creditObserver_) {
    creditObserver_->onPacketStored(*pkt);
  }

  updatePfc(queueIdx);
  schedulePop();
  return {queueIdx, true};
}

void NicRxBuffer::setOnPfcStateChangedCallback(
    const OnPfcStateChangedCallback& cb) {
  onPfcStateChanged_ = cb;
}

void NicRxBuffer::setOnPacketReleasedCallback(
    const OnPacketReleasedCallback& cb) {
  onPacketReleased_ = cb;
}

void NicRxBuffer::setCreditObserver(
    cbfc::ICreditObserver* FOLLY_NULLABLE observer) {
  creditObserver_ = observer;
}

bool NicRxBuffer::allQueuesEmpty() {
  bool empty = true;
  for (uint8_t cos = 0; cos < cosQueues_->getNumQueues(); ++cos) {
    if (cosQueues_->getQueueLengthInBytes(cos) != 0) {
      empty = false;
    }
  }
  return empty;
}

void NicRxBuffer::schedulePop() {
  if (isSending_ || allQueuesEmpty()) {
    return;
  }
  const auto nextQueueIdx = cosQueues_->nextQueue();
  if (!nextQueueIdx.has_value()) {
    return;
  }
  const uint8_t queueIdx = *nextQueueIdx;
  isSending_ = true;
  const auto bps = getEffectiveRate();
  if (cosQueues_->isQueueEmpty(queueIdx)) {
    throw std::runtime_error("Empty() returned false but queue is empty");
  }
  PacketPtr pkt;
  cosQueues_->peekPacketInQueue(queueIdx, &pkt);
  const Time delay = bps.bytesToTime(pkt->getSize());
  simCtx_->schedule(delay, &NicRxBuffer::doPop, this, queueIdx);
}

void NicRxBuffer::doPop(const uint8_t queueIdx) {
  PacketPtr pkt;
  if (!cosQueues_->popPacketFromQueue(queueIdx, pkt)) {
    throw std::runtime_error("Queue scheduled for draining is empty");
  }
  if (creditObserver_) {
    creditObserver_->onPacketDrained(*pkt);
  }
  onPacketReleased(pkt);
  isSending_ = false;
  updatePfc(queueIdx);
  schedulePop();
}

void NicRxBuffer::updatePfc(const uint8_t queueIdx) {
  const auto pfcState = pfcStates_.at(queueIdx);
  const auto& thresholds = pfcThresholds_.at(queueIdx);
  const uint32_t usedBytes = cosQueues_->getQueueLengthInBytes(queueIdx);
  bool doCallback{false};

  if (!thresholds.has_value()) {
    return;
  }

  if ((pfcState == PfcState::XON) && (usedBytes >= thresholds->xoffBytes)) {
    pfcStates_.at(queueIdx) = PfcState::XOFF;
    doCallback = true;
  } else if (
      (pfcState == PfcState::XOFF) && (usedBytes <= thresholds->xonBytes)) {
    pfcStates_.at(queueIdx) = PfcState::XON;
    doCallback = true;
  }

  if (doCallback) {
    onPfcStateChanged(queueIdx, pfcStates_.at(queueIdx));
  }
}

DataRate NicRxBuffer::getEffectiveRate() {
  const auto eff = simCtx_->random.sampleUniform<double>(minEff_, maxEff_);
  return DataRate((drainRate_.getBps() * (eff / kPercentDivisor)));
}

PfcState NicRxBuffer::getPfcState(const uint8_t queueIdx) const {
  return pfcStates_.at(queueIdx);
}

} // namespace packsim
