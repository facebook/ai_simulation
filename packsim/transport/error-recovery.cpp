/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/transport/error-recovery.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/container/F14Set.h>
#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>

#include "packsim/if/gen-cpp2/roce_v2_config_constants.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/transport/roce-send-info.h"

namespace packsim {

namespace {

// Balance gap-recovery latency against periodic-NACK wire overhead.
const Time kNackResendInterval = Time::fromUs(100);
// Avoid scheduling two NACKs on the same tick after RNR back-off rounding.
const Time kRnrResendJitter = Time::fromUs(1);

} // namespace

GoBackNRecovery::GoBackNRecovery(
    uint8_t rnrTimeIdx,
    ErrorRecoverySocket& socket,
    SimContext& simCtx)
    : GoBackNRecovery(rnrTimeIdx, socket, simCtx, Options{}) {}

GoBackNRecovery::GoBackNRecovery(
    uint8_t rnrTimeIdx,
    ErrorRecoverySocket& socket,
    SimContext& simCtx,
    const Options& options)
    : socket_(&socket),
      simCtx_(&simCtx),
      retransmitTimeout_(options.retransmitTimeout),
      maxRetransmitTimeoutAttempts_(options.maxRetransmitTimeoutAttempts),
      selectiveGapRepair_(options.selectiveGapRepair),
      inOrderDelivery_(options.inOrderDelivery),
      rnrTimeIdx_(rnrTimeIdx) {
  // RoceTransportBase rejects a bad index from the config first, naming the
  // config field; this covers a GoBackNRecovery built directly.
  const size_t numRnrTimes =
      packsim::thrift::roce_v2_config_constants::kGoBackNRnrFieldToTimeUs()
          .size();
  if (rnrTimeIdx_ >= numRnrTimes) {
    throw std::invalid_argument(
        fmt::format(
            "GoBackNRecovery rnrTimeIdx {} is out of range: it indexes "
            "kGoBackNRnrFieldToTimeUs, valid range [0, {}]",
            rnrTimeIdx_,
            numRnrTimes - 1));
  }
  if (retransmitTimeout_.isZero()) {
    throw std::invalid_argument("Retransmit timeout must be positive");
  }
  if (maxRetransmitTimeoutAttempts_ == 0) {
    throw std::invalid_argument(
        "Maximum retransmit timeout attempts must be positive");
  }
}

GoBackNRecovery::~GoBackNRecovery() {
  retransmitEvent_.cancel();
  nackEvent_.cancel();
  rnrEvent_.cancel();
}

bool GoBackNRecovery::isSendBlocked() const {
  return simCtx_->getTimeUntilEvent(rnrEvent_) > Time(0);
}

PsnAction GoBackNRecovery::checkPsn(
    uint32_t& expectedPsn,
    const uint32_t receivedPsn,
    const bool oooSupport) {
  if (oooSupport && !selectiveGapRepair_) {
    return PsnAction::ACCEPT;
  }
  if (receivedPsn < expectedPsn) {
    return PsnAction::ACK_DUPLICATE;
  }
  if (receivedPsn == expectedPsn) {
    ++expectedPsn;
    while (bufferedPsns_.erase(expectedPsn) > 0) {
      ++expectedPsn;
    }
    if (nackEvent_.isActive()) {
      XLOGF(DBG2, "Got PSN {} — cancelling NACK timer", receivedPsn);
      nackEvent_.cancel();
    }
    return oooSupport && !bufferedPsns_.empty()
        ? PsnAction::ACCEPT_AND_REQUEST_NACK
        : PsnAction::ACCEPT;
  }
  if (oooSupport) {
    const bool isNewPacket = bufferedPsns_.emplace(receivedPsn).second;
    if (!nackEvent_.isActive()) {
      return isNewPacket ? PsnAction::BUFFER_AND_REQUEST_NACK
                         : PsnAction::SEND_NACK;
    }
    return isNewPacket ? PsnAction::BUFFER_WITHOUT_ACK : PsnAction::DROP;
  }
  if (!nackEvent_.isActive()) {
    XLOGF(
        DBG2,
        "PSN gap detected: expected {}, got {}",
        expectedPsn,
        receivedPsn);
    return PsnAction::SEND_NACK;
  }
  return PsnAction::DROP;
}

void GoBackNRecovery::onPacketScheduled(const RoceSendInfo& info) {
  if (!info.isRetransmit) {
    reTxQ_.emplace_back(info);
    reTxPtr_ = reTxQ_.size();
    if (acksAreCumulative() && info.isLastPkt && !retransmitEvent_.isActive()) {
      retransmitTimeoutAttempts_ = 0;
      resetRetransmitTimeout();
    }
  }
}

AckResult GoBackNRecovery::onAck(const uint32_t psn) {
  if (psn <= lastReceivedPsn_) {
    // A later message's higher-PSN ACK may advance recovery state before this
    // reordered ACK completes an earlier message.
    return AckResult{};
  }
  AckResult result;
  for (uint32_t i = lastReceivedPsn_ + 1; i <= psn; ++i) {
    if (reTxQ_.empty()) {
      throw std::runtime_error("Retransmit queue empty during ACK processing");
    }
    if (reTxQ_.front().psn != i) {
      throw std::runtime_error(
          fmt::format(
              "Retransmit queue PSN mismatch: expected {}, got {}",
              i,
              reTxQ_.front().psn));
    }
    const auto& info = reTxQ_.front();
    if (acksAreCumulative() && info.isLastPkt) {
      if (info.sequenceNum == std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error(
            "Sequence number wrapped around during ACK processing");
      }
      result.nextIncompleteSeqNum = std::max(
          result.nextIncompleteSeqNum.value_or(0), info.sequenceNum + 1);
    }
    reTxQ_.pop_front();
    if (reTxPtr_ > 0) {
      --reTxPtr_;
    }
  }
  lastReceivedPsn_ = psn;
  if (acksAreCumulative()) {
    retransmitTimeoutAttempts_ = 0;
    resetRetransmitTimeout();
  }
  return result;
}

void GoBackNRecovery::onNack(
    const uint32_t expectedPsn,
    const std::optional<uint8_t> rnrBackoffIdx) {
  for (uint32_t i = lastReceivedPsn_ + 1; i < expectedPsn; ++i) {
    if (reTxQ_.empty()) {
      throw std::runtime_error("Retransmit queue empty during NACK processing");
    }
    if (reTxQ_.front().psn != i) {
      throw std::runtime_error(
          fmt::format(
              "Retransmit queue PSN mismatch: expected {}, got {}",
              i,
              reTxQ_.front().psn));
    }
    reTxQ_.pop_front();
  }
  // The NACK's expected PSN acknowledges every PSN before it.
  const bool acksNewPsns = lastReceivedPsn_ < (expectedPsn - 1);
  if (acksNewPsns) {
    lastReceivedPsn_ = expectedPsn - 1;
  }
  reTxPtr_ = 0;
  retransmitTrigger_ = RetransmitTrigger::NACK;
  retransmitPacketLimit_ =
      selectiveGapRepair_ && !rnrBackoffIdx.has_value() ? 1 : UINT32_MAX;
  if (acksAreCumulative()) {
    // Progress restarts the timeout retry budget, as an ACK does. So does an
    // RNR NACK: the responder is alive but not ready, which, as in InfiniBand,
    // does not count against that budget.
    if (acksNewPsns || rnrBackoffIdx.has_value()) {
      retransmitTimeoutAttempts_ = 0;
    }
    resetRetransmitTimeout();
  }

  // RNR back-off: defer wakeup; otherwise wake the socket immediately.
  if (rnrBackoffIdx.has_value()) {
    rnrEvent_.cancel();
    rnrEvent_ = simCtx_->schedule(
        rnrFieldToTime(*rnrBackoffIdx),
        &ErrorRecoverySocket::wakeupForSend,
        socket_);
  } else {
    socket_->wakeupForSend();
  }
}

void GoBackNRecovery::onMissingMessage(uint32_t seqNum, uint32_t psn) {
  if (nackEvent_.isActive()) {
    return;
  }
  if (selectiveGapRepair_) {
    nackEvent_ = simCtx_->schedule(
        kNackResendInterval,
        &GoBackNRecovery::scheduleNackResend,
        this,
        seqNum,
        psn,
        std::optional<uint8_t>{});
    return;
  }
  scheduleNackResend(seqNum, psn, std::nullopt);
}

void GoBackNRecovery::deferAck(const uint32_t seqNum, const uint32_t psn) {
  deferredAcks_.emplace_back(seqNum, psn);
}

std::vector<std::pair<uint32_t, uint32_t>>
GoBackNRecovery::takeReadyDeferredAcks(const uint32_t expectedPsn) {
  std::vector<std::pair<uint32_t, uint32_t>> ready;
  // Called on every accepted packet, usually with nothing ready: partition in
  // place so the common case allocates nothing and keeps deferredAcks_ intact.
  const auto readyEnd = std::stable_partition(
      deferredAcks_.begin(),
      deferredAcks_.end(),
      [expectedPsn](const std::pair<uint32_t, uint32_t>& ack) {
        return ack.second < expectedPsn;
      });
  if (readyEnd == deferredAcks_.begin()) {
    return ready;
  }
  ready.assign(deferredAcks_.begin(), readyEnd);
  deferredAcks_.erase(deferredAcks_.begin(), readyEnd);
  return ready;
}

void GoBackNRecovery::onSendRnrRequested(uint32_t psn) {
  if (nackEvent_.isActive()) {
    return;
  }
  scheduleNackResend(psn, psn, rnrTimeIdx_);
}

void GoBackNRecovery::scheduleNackResend(
    uint32_t seqNum,
    uint32_t psn,
    const std::optional<uint8_t> rnrBackoffIdx) {
  // Self-rescheduling: each invocation emits one NACK and queues itself to
  // run again after `resendTime`. checkPsn() cancels either a gap- or
  // RNR-triggered chain when it accepts the expected PSN; the destructor
  // cancels any chain still pending.
  socket_->emitNackPacket(seqNum, psn, rnrBackoffIdx);
  const auto resendTime = rnrBackoffIdx.has_value()
      ? rnrFieldToTime(*rnrBackoffIdx) + kRnrResendJitter
      : kNackResendInterval;
  const auto now = simCtx_->now();
  XLOGF(
      DBG2,
      "Sending NACK at time {}; resend at {}",
      now.getNs(),
      (now + resendTime).getNs());
  nackEvent_ = simCtx_->schedule(
      resendTime,
      &GoBackNRecovery::scheduleNackResend,
      this,
      seqNum,
      psn,
      rnrBackoffIdx);
}

GoBackNRecovery::RetransmitBatch GoBackNRecovery::getRetransmitBatch(
    const uint64_t quota) {
  RetransmitBatch batch;
  while (batch.totalBytes < quota && retransmitPacketLimit_ > 0) {
    if (reTxQ_.size() < reTxPtr_) {
      throw std::runtime_error("Retransmit pointer exceeds queue size");
    }
    if (reTxQ_.empty() || reTxPtr_ == reTxQ_.size()) {
      break;
    }
    auto info = reTxQ_[reTxPtr_];
    if (!batch.packets.empty() && batch.totalBytes + info.packetSize > quota) {
      break;
    }
    info.isRetransmit = true;
    reTxPtr_++;
    --retransmitPacketLimit_;
    batch.totalBytes += info.packetSize;
    info.callbackOnSend = false;
    batch.packets.emplace_back(info);
  }
  if (!batch.packets.empty()) {
    batch.packets.back().callbackOnSend = true;
    if (!retransmitTrigger_.has_value()) {
      throw std::runtime_error("Retransmit batch has no trigger");
    }
    const uint64_t packetCount = batch.packets.size();
    retransmitStats_.retxPkts += packetCount;
    retransmitStats_.retxBytes += batch.totalBytes;
    switch (*retransmitTrigger_) {
      case RetransmitTrigger::NACK:
        retransmitStats_.retxByNack += packetCount;
        break;
      case RetransmitTrigger::TIMER:
        retransmitStats_.retxByTimer += packetCount;
        break;
    }
  }
  XLOGF(
      DBG2,
      "Retransmit batch: {} bytes ({} packets)",
      batch.totalBytes,
      batch.packets.size());
  return batch;
}

bool GoBackNRecovery::hasRetransmitPending() const noexcept {
  return !reTxQ_.empty() && reTxPtr_ != reTxQ_.size() &&
      retransmitPacketLimit_ > 0;
}

void GoBackNRecovery::resetRetransmitTimeout() {
  // The isLastPkt scan below is O(reTxQ_) and runs on every ACK while
  // acksAreCumulative(). Kept as a scan rather than a maintained counter: the
  // counter would have to stay in sync across every reTxQ_ push and pop, and
  // a desync silently mis-schedules the retransmit timer. Revisit if reTxQ_
  // depth ever makes this measurable.
  if (failed_) {
    return;
  }
  retransmitEvent_.cancel();
  const bool ackPending =
      std::any_of(reTxQ_.begin(), reTxQ_.end(), [](const RoceSendInfo& info) {
        return info.isLastPkt;
      });
  if (ackPending) {
    retransmitEvent_ = simCtx_->schedule(
        retransmitTimeout_, &GoBackNRecovery::onRetransmitTimeout, this);
  }
}

void GoBackNRecovery::onRetransmitTimeout() {
  // Rewinding to the tail deliberately overrides any NACK-driven retransmit
  // still nominally in flight: the timer only fires after a full timeout with
  // no ACK progress (resetRetransmitTimeout() re-arms it on every ACK), which
  // is exactly when the stalled batch is not making progress and a tail poke
  // is needed to solicit an ACK. Any gap skipped this way is re-NACKed by the
  // receiver, and retransmitTimeoutAttempts_ bounds the retries.
  if (retransmitTimeoutAttempts_ >= maxRetransmitTimeoutAttempts_) {
    failRecovery();
    return;
  }
  auto tailIndex = reTxQ_.size();
  while (tailIndex > 0) {
    --tailIndex;
    if (reTxQ_[tailIndex].isLastPkt) {
      ++retransmitTimeoutAttempts_;
      reTxPtr_ = tailIndex;
      retransmitTrigger_ = RetransmitTrigger::TIMER;
      retransmitPacketLimit_ = 1;
      socket_->wakeupForSend();
      retransmitEvent_ = simCtx_->schedule(
          retransmitTimeout_, &GoBackNRecovery::onRetransmitTimeout, this);
      return;
    }
  }
  // No isLastPkt entry left: resetRetransmitTimeout() only arms this timer
  // while one exists, so reaching here means the queue drained between the
  // arm and the fire. Nothing to poke and nothing to re-arm.
}

void GoBackNRecovery::failRecovery() {
  failed_ = true;
  nackEvent_.cancel();
  rnrEvent_.cancel();
}

Time GoBackNRecovery::rnrFieldToTime(const uint8_t rnrField) {
  return Time::fromUs(
      packsim::thrift::roce_v2_config_constants::kGoBackNRnrFieldToTimeUs().at(
          rnrField));
}

} // namespace packsim
