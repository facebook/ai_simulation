/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/transport/roce-base.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <utility>

#include <fmt/format.h>
#include <folly/logging/xlog.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/roce_v2_config_constants.h"
#include "packsim/if/gen-cpp2/transport_stats_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/link-layer/cbfc/nic-rx-credit-bridge.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/packet/roce.h"

namespace packsim {

namespace {

// UDP destination port reserved for RoCEv2 (IANA-assigned).
constexpr uint16_t kRoceV2UdpPort = 4791;

// Size of an ACK/NACK/CNP control packet payload.
constexpr uint32_t kControlPacketBytes = 4;

// IPv6 hop limit. A retransmission goes out with bit 7 set (192 == 0x80 | 64):
// that is the "TTL-marked" flag described beside `kHopLimitTtldBit` in
// `link-layer/eth-port.cpp`, which the per-port `*TtldPkts` counters
// count. Those counters are not retransmission-specific: a frame left at the
// default hop limit of 255 carries the bit too.
constexpr uint8_t kRetransmitHopLimit = 192;
constexpr uint8_t kDefaultHopLimit = 64;

// DCQCN uses a higher maxActiveSends to accommodate rate-limited sockets.
constexpr uint32_t kDcqcnMaxActiveSends = 256;

/**
 * Reject a Go-Back-N RNR (receiver not ready) timer index outside the RNR
 * timer table, which would otherwise throw `std::out_of_range` at the first RNR
 * NACK, possibly long into a run.
 *
 * @param roceCfg  The RoCE transport config to check.
 *
 * @throw std::invalid_argument  If Go-Back-N is selected and `rnrTimeIdx` does
 *                               not index `kGoBackNRnrFieldToTimeUs`.
 */
void validateRnrTimeIdx(const RoceTransportConfig& roceCfg) {
  if (roceCfg.errorRecovery != RoceTransportConfig::ErrorRecovery::GO_BACK_N) {
    return;
  }
  const size_t tableSize =
      packsim::thrift::roce_v2_config_constants::kGoBackNRnrFieldToTimeUs()
          .size();
  if (roceCfg.rnrTimeIdx >= tableSize) {
    throw std::invalid_argument(
        fmt::format(
            "RoCE Go-Back-N config rnrTimeIdx {} is out of range: it indexes "
            "kGoBackNRnrFieldToTimeUs, valid range [0, {}]",
            roceCfg.rnrTimeIdx,
            tableSize - 1));
  }
}

uint64_t flexDeviceIdSum(const FlexDeviceId& id) noexcept {
  uint64_t sum = 0;
  for (const uint16_t value : id.values) {
    sum += value;
  }
  return sum;
}

uint64_t devicePairKey(
    const FlexDeviceId& srcId,
    const FlexDeviceId& dstId) noexcept {
  return flexDeviceIdSum(srcId) + flexDeviceIdSum(dstId);
}

} // namespace

RoceTransportConfig RoceTransportConfig::fromThrift(
    const packsim::thrift::TransportConfig& transport) {
  using TransportType = packsim::thrift::TransportConfig::Type;
  using CcType = packsim::thrift::RoceV2CongestionControlConfig::Type;
  using LrType = packsim::thrift::RoceV2LossRecoveryConfig::Type;

  RoceTransportConfig config;
  switch (transport.getType()) {
    case TransportType::roceV2: {
      // Each concern is selected independently by its own sub-union; an unset
      // arm is a config error.
      const auto& roceThriftConfig = transport.get_roceV2();
      switch (roceThriftConfig.congestionControl()->getType()) {
        case CcType::dummy:
          break;
        case CcType::dcqcn:
          config.congestionControl = CongestionControl::DCQCN;
          break;
        case CcType::__EMPTY__:
          throw std::invalid_argument(
              "unsupported RoCE congestion control config");
      }
      switch (roceThriftConfig.lossRecovery()->getType()) {
        case LrType::dummy:
          break;
        case LrType::goBackN: {
          config.errorRecovery = ErrorRecovery::GO_BACK_N;
          const auto& goBackN = roceThriftConfig.lossRecovery()->get_goBackN();
          config.rnrTimeIdx = *goBackN.rnrTimeIdx();
          config.selectiveGapRepair = *goBackN.selectiveGapRepair();
          break;
        }
        case LrType::__EMPTY__:
          throw std::invalid_argument("unsupported RoCE loss recovery config");
      }
      return config;
    }
    // @oss-disable
    // @oss-disable
    // @oss-disable
    // @oss-disable
      // @oss-disable
    case TransportType::__EMPTY__:
      throw std::invalid_argument("transport config is unset");
  }
  // @oss-disable
  // The out-of-range-enum guard after an exhaustive switch. // @oss-enable
  throw std::invalid_argument("not a RoCE-family transport config");
}

// ---------------------------------------------------------------------------
// RoceSocketBase
// ---------------------------------------------------------------------------

RoceSocketBase::RoceSocketBase(
    RoceTransportBase* transport,
    const FlexDeviceId& src,
    const FlexDeviceId& dst,
    const uint16_t srcPort,
    const uint32_t localQPN,
    const uint32_t remoteQPN,
    int32_t /* smtu */,
    const packsim::workload::thrift::Workload* workload,
    const ErrorRecoveryFactory& errorRecoveryFactory,
    const CongestionControlFactory& congestionControlFactory)
    : Socket(transport, src, dst, srcPort, localQPN, remoteQPN, workload),
      egressPortKey_{devicePairKey(src, dst)} {
  dstPort_ = kRoceV2UdpPort;
  dataCos_ = transport->dataCos_;
  ackCos_ = transport->ackCos_;
  cnpCos_ = transport->cnpCos_;
  // Wire OOO support from nicConfig first; the strategy may force-enable it
  // below (e.g. byte-count completion needs OOO regardless of NIC config).
  outOfOrderPktSupport_ = *transport->nicConfig_->outofOrderPktSupport();
  if (errorRecoveryFactory) {
    errorRecovery_ = errorRecoveryFactory(*this, *simCtx_);
    if (errorRecovery_ && errorRecovery_->useByteCountCompletion()) {
      outOfOrderPktSupport_ = true;
    }
  }
  if (congestionControlFactory) {
    congestionControl_ = congestionControlFactory(*this, *simCtx_);
  }
}

RoceTransportBase* RoceSocketBase::transport() const {
  return static_cast<RoceTransportBase*>(transport_);
}

void RoceSocketBase::send(Message* msg) {
  if (workload_ == nullptr) {
    workload_ = msg->workload;
  }

  msg->sequenceNumber = txSequenceNumber_++;
  const auto maxQpDepth = transport()->maxQpDepth_;
  if (maxQpDepth && queuedMsgs_.size() >= *maxQpDepth) {
    throw std::runtime_error(
        fmt::format("Exceeded maximum send queue depth of {}", *maxQpDepth));
  }
  queuedMsgs_.emplace_back(msg);
  transport()->socketReady(this);
}

void RoceSocketBase::recv(Message* msg) {
  // A PULL workload posts each READ as a receive with isRead set; without RDMA
  // READ it would wait for a SEND that never comes.
  if (msg->isRead) {
    throw std::invalid_argument(
        "The RoCEv2 transport does not model RDMA READ, so it cannot take a "
        "PULL workload's READ; use the SENDRCV workload mode, or PUSH if the "
        "receiving NICs leave outofOrderPktSupport off");
  }
  if (workload_ == nullptr) {
    workload_ = msg->workload;
  }

  msg->sequenceNumber = rxSequenceNumber_++;
  const auto maxQpDepth = transport()->maxQpDepth_;
  if (maxQpDepth && expectedMsgs_.size() >= *maxQpDepth) {
    throw std::runtime_error(
        fmt::format("Exceeded maximum recv queue depth of {}", *maxQpDepth));
  }
  expectedMsgs_.emplace_back(msg);
}

std::tuple<bool, uint32_t> RoceSocketBase::sendBytes(uint32_t quota) {
  if (isQpFailed()) {
    return std::make_tuple(false, uint32_t{0});
  }

  // Apply DCQCN token budget if congestion control is active.
  if (congestionControl_) {
    // adjustQuota() never grants more than the uint32_t quota it is given.
    quota = static_cast<uint32_t>(congestionControl_->adjustQuota(quota));
    if (quota == 0) {
      return std::make_tuple(false, quota);
    }
  }

  if (queuedMsgs_.empty()) {
    throw std::runtime_error("Tried to send from empty QP");
  }
  bool moreToSend = quota > 0;
  uint32_t sentBytes = 0;
  while (moreToSend) {
    auto msg = queuedMsgs_.front();
    const auto msgBytesRemain = msg->sizeBytes - msgBytesSent_;
    const uint32_t remainingQuota = quota - sentBytes;
    uint32_t sendSize;
    bool endOfMsg;
    if (msgBytesRemain <= remainingQuota) {
      sendSize = msgBytesRemain;
      sentMsgs_.emplace_back(queuedMsgs_.front());
      queuedMsgs_.pop_front();
      msgBytesSent_ = 0;
      endOfMsg = true;
    } else {
      sendSize = remainingQuota;
      endOfMsg = false;
      msgBytesSent_ += sendSize;
    }
    sentBytes += sendSize;

    moreToSend = sentBytes < quota && !queuedMsgs_.empty();
    XLOGF(
        DBG,
        "Sending {}/{} bytes - ({} bytes remaining)",
        sendSize,
        msg->sizeBytes,
        msgBytesRemain - sendSize);
    fetchDataAndSend(
        sendSize,
        msg->sequenceNumber,
        endOfMsg,
        !moreToSend,
        msg->isWrite,
        msg->isWriteImm,
        msg->writeImmData,
        msg->sendOrWriteData);
  }

  // Consume DCQCN tokens for the bytes we sent.
  if (congestionControl_) {
    congestionControl_->onBytesSent(sentBytes);
  }

  return std::tuple(true, sentBytes);
}

void RoceSocketBase::fetchDataAndSend(
    const uint32_t bytes,
    const uint32_t sequenceNum,
    const bool endOfMsg,
    const bool endOfQuotaChunk,
    const bool isWrite,
    const bool isWriteImm,
    const uint32_t writeImmData,
    const uint32_t sendOrWriteData) {
  XLOGF(
      DBG2,
      "Host {}, Sequence {}: send {} bytes to {}",
      transport()->id_.toString(),
      sequenceNum,
      bytes,
      getDstId().toString());
  uint32_t bytesLeft = bytes;
  bool isFirstPkt = true;
  uint64_t readLatency = 0;

  while (bytesLeft > 0 || isFirstPkt) {
    const uint32_t pktSize = std::min(bytesLeft, uint32_t{kMaxRdmaPayload});
    const bool isLastPkt = (bytesLeft == pktSize);
    readLatency = transport_->readBytesFromMemory(pktSize);
    const SendInfo info(
        {.packetSize = pktSize,
         .isFirstPkt = isFirstPkt,
         .isLastPkt = isLastPkt && endOfMsg,
         .isAck = false,
         .isNack = false,
         .isWriteImm = isWriteImm,
         .isWrite = isWrite,
         .psn = txPsn_++,
         .sequenceNum = sequenceNum,
         .callbackOnSend = isLastPkt && endOfQuotaChunk,
         .writeImmData = writeImmData,
         .sendOrWriteData = sendOrWriteData});
    scheduleSend(readLatency, info);
    isFirstPkt = false;
    bytesLeft -= pktSize;
  }
  if (bytesLeft != 0) {
    throw std::runtime_error(
        fmt::format(
            "Bytes remaining after sending all packets: {}", bytesLeft));
  }
}

void RoceSocketBase::scheduleSend(
    const uint64_t delayNs,
    const SendInfo& info) {
  simCtx_->schedule(
      Time::fromNs(delayNs), &RoceSocketBase::createAndSendPkt, this, info);

  // Record sent packet for potential retransmission (error recovery).
  if (errorRecovery_) {
    errorRecovery_->onPacketScheduled(info);
  }
}

void RoceSocketBase::roceEncap(
    const PacketPtr& pkt,
    const SendInfo& info,
    const uint32_t psn) const {
  auto rocePkt = pkt.as<RocePacket>();
  rocePkt->conn = conn_.get();
  rocePkt->payloadSize = info.packetSize;

  if (info.isCnp) {
    rocePkt->opcode = BthOpcode::CNP;
  } else {
    rocePkt->setOpcode(info.isFirstPkt, info.isLastPkt);
  }
  rocePkt->isAck = info.isAck;
  rocePkt->isNack = info.isNack;
  rocePkt->isCnp = info.isCnp;
  if ((info.nackTime != UINT8_MAX) && info.isNack) {
    rocePkt->nackTimer = info.nackTime;
  }
  rocePkt->ackReq = !info.isAck && !info.isNack && info.isLastPkt;
  rocePkt->psn = psn;
  rocePkt->sequenceNum = info.sequenceNum;
  rocePkt->isWrite = info.isWrite;
  rocePkt->isWriteImm = info.isWriteImm;
  rocePkt->sendOrWriteData = info.sendOrWriteData;
  // Non-null exactly when this QP was built with a loss-recovery strategy.
  rocePkt->lossRecoveryEnabled = errorRecovery_ != nullptr;
}

void RoceSocketBase::udpEncap(const PacketPtr& pkt) const {
  pkt.as<UdpPacket>()->srcPort = srcPort_;
  pkt.as<UdpPacket>()->dstPort = dstPort_;
}

void RoceSocketBase::ipv6Encap(const PacketPtr& pkt, const SendInfo& info)
    const {
  auto ipPkt = pkt.as<Ipv6Packet>();
  ipPkt->hopLimit = info.isRetransmit ? kRetransmitHopLimit : kDefaultHopLimit;

  if (info.isCnp) {
    ipPkt->setDscpFromCos(cnpCos_);
  } else if (info.isAck) {
    ipPkt->setDscpFromCos(ackCos_);
  } else if (info.isNack) {
    ipPkt->setDscpFromCos(ackCos_);
  } else if (info.isControlPkt) {
    ipPkt->setDscpFromCos(ackCos_);
  } else {
    ipPkt->setDscpFromCos(dataCos_);
  }
}

void RoceSocketBase::writeImmEncap(const PacketPtr& pkt, const SendInfo& info)
    const {
  pkt.as<RocePacket>()->immData = info.writeImmData;
}

void RoceSocketBase::createAndSendPkt(const SendInfo& info) {
  if (isQpFailed()) {
    return;
  }

  const uint32_t psn = info.isAck && errorRecovery_
      ? errorRecovery_->adjustAckPsn(info.psn, expectedPsn_)
      : info.psn;
  auto pkt = createPacket<PacketType::ROCE>(info.packetSize);
  if (info.isWriteImm) {
    writeImmEncap(pkt, info);
  }
  roceEncap(pkt, info, psn);
  udpEncap(pkt);
  ipv6Encap(pkt, info);
  sendPkt(pkt, info.callbackOnSend, info.isAck, info.isNack, info.isCnp);
}

void RoceSocketBase::sendPkt(
    const PacketPtr& pkt,
    const bool callbackOnSend,
    const bool isAck,
    const bool isNack,
    const bool isCnp) {
  if (isAck) {
    *stats_.sentAckPkts() += 1;
    auto& cosStat = transport()->cosStats_.at(ackCos_);
    *cosStat.sentAckPkts() += 1;
  } else if (isNack) {
    *stats_.sentNackPkts() += 1;
    auto& cosStat = transport()->cosStats_.at(ackCos_);
    *cosStat.sentNackPkts() += 1;
  } else if (isCnp) {
    *stats_.sentCnpPkts() += 1;
    auto& cosStat = transport()->cosStats_.at(cnpCos_);
    *cosStat.sentCnpPkts() += 1;
  } else {
    *stats_.sentDataPkts() += 1;
    *stats_.sentDataBytes() += pkt->getSize();
    auto& cosStat = transport()->cosStats_.at(dataCos_);
    *cosStat.sentDataBytes() += pkt->getSize();
    *cosStat.sentDataPkts() += 1;
  }

  if (callbackOnSend) {
    transport()->sendPacketWithCallback(this, pkt);
  } else {
    transport()->sendPacket(this, pkt);
  }
}

void RoceSocketBase::onPacketSent(const PacketPtr& /* pkt */) {}

void RoceSocketBase::onPacketRcvd(const PacketPtr& pktPtr) {
  // Route every received packet through congestion control (before packet-type
  // dispatch). An ECN-based strategy (DCQCN) emits a CNP toward the sender when
  // the ECN-CE mark is set; others may track per-packet arrival state.
  const bool isCeMarked =
      pktPtr.as<Ipv6Packet>()->ecn == Ipv6Packet::EcnType::ECN_CE;
  if (congestionControl_) {
    congestionControl_->onPacketReceived(isCeMarked);
  }

  RocePacket* pkt = pktPtr.as<RocePacket>();
  if (isCeMarked) {
    countEcnRcvd(*pkt);
  }

  const auto sendSeqNum = pkt->sequenceNum;
  const bool isLastPkt = pkt->isLastPkt();

  if (pkt->isAck) {
    return ackRcvd(sendSeqNum, pkt->psn);
  }

  if (pkt->isNack) {
    return nackRcvd(pkt->psn, pkt->nackTimer);
  }

  if (pkt->isCnp) {
    return cnpRcvd();
  }

  bool ackRequested = pkt->ackReq;
  // Already-delivered PSNs are resolved here, before the expected-message
  // lookup below. A duplicate must be ACKed, not answered with an RNR: RNR
  // tells the sender to back off and retransmit data the receiver already
  // has. The lookup would RNR precisely because the message it re-delivers
  // has already completed and left expectedMsgs_.
  if (errorRecovery_ && pkt->psn < expectedPsn_ &&
      !isPsnOk(sendSeqNum, pkt->psn, ackRequested)) {
    return;
  }

  const auto receiveSeqNum = sendSeqNum - rxWritesReceived_;
  if (!pkt->isWrite) {
    const auto match = [receiveSeqNum](Message* msg) {
      return msg->sequenceNumber == receiveSeqNum;
    };
    const auto msgIt =
        std::find_if(expectedMsgs_.begin(), expectedMsgs_.end(), match);
    if (msgIt == expectedMsgs_.end()) {
      XLOGF(
          DBG,
          "Packet SQN {} does not match any of the expected SQNs (size: {} bytes): {}",
          receiveSeqNum,
          pkt->getSize(),
          *pkt);
      for (const auto& msg : expectedMsgs_) {
        XLOGF(DBG, "* Expected message: {}", msg->toString());
      }
      return sendRnr(pkt->psn);
    }
  }

  if (!isPsnOk(sendSeqNum, pkt->psn, ackRequested)) {
    XLOGF(
        DBG,
        "Got out of order packet Seq Num: {}, PSN: {}",
        sendSeqNum,
        pkt->psn);
    return;
  }

  if (pkt->isWrite && outOfOrderPktSupport_) {
    throw std::runtime_error("Write packets not supported with OOO");
  } else if (pkt->isWrite && pkt->isLastPkt()) {
    ++rxWritesReceived_;
  }

  if (!transport()->writePacketToMemory(this, pktPtr)) {
    throw std::runtime_error("NIC memory doesn't have room for packet");
  }

  simCtx_->dataRcvd += pkt->payloadSize;

  const auto encapPktSize = pkt->getSize();
  *stats_.recvdDataPkts() += 1;
  *stats_.recvdDataBytes() += encapPktSize;

  auto& cosStat = transport()->cosStats_.at(dataCos_);
  *cosStat.recvdDataBytes() += encapPktSize;
  *cosStat.recvdDataPkts() += 1;

  if (ackRequested) {
    emitAckPacket(sendSeqNum, pkt->psn);
  }

  if (pkt->isWrite) {
    return;
  }

  bytesReceivedPerMsg_[receiveSeqNum] += pkt->payloadSize;

  Message* expMsg;

  if (outOfOrderPktSupport_) {
    const auto match = [receiveSeqNum](Message* msg) {
      return msg->sequenceNumber == receiveSeqNum;
    };
    const auto it =
        std::find_if(expectedMsgs_.begin(), expectedMsgs_.end(), match);
    expMsg = *it;
    const auto recvSize = bytesReceivedPerMsg_.at(receiveSeqNum);
    const auto msgSize = expMsg->sizeBytes;
    if (msgSize != recvSize) {
      return;
    }
    expectedMsgs_.erase(it);
  } else {
    if (!isLastPkt) {
      return;
    }
    expMsg = expectedMsgs_.front();
    if (expMsg->sequenceNumber != receiveSeqNum) {
      throw std::runtime_error("Got wrong sequence number");
    }
    expectedMsgs_.pop_front();
  }

  const auto recvSize = bytesReceivedPerMsg_.at(receiveSeqNum);
  const auto msgSize = expMsg->sizeBytes;
  if (msgSize != recvSize && !pkt->isWriteImm) {
    throw std::runtime_error(
        fmt::format(
            "Node {} expected {} bytes for message {}, received {} bytes",
            transport()->id_.toString(),
            msgSize,
            receiveSeqNum,
            recvSize));
  }
  if (pkt->isWriteImm) {
    expMsg->writeImmData = pkt->immData;
  }

  expMsg->sendOrWriteData = pkt->sendOrWriteData;

  bytesReceivedPerMsg_.erase(receiveSeqNum);
  finalPacketToMsg_[pktPtr] = expMsg;
}

bool RoceSocketBase::isPsnOk(
    const uint32_t seqNum,
    const uint32_t psn,
    bool& ackRequested) {
  if (errorRecovery_) {
    const auto result =
        errorRecovery_->checkPsn(expectedPsn_, psn, outOfOrderPktSupport_);
    const auto emitReadyDeferredAcks = [&] {
      for (const auto& [ackSeqNum, ackPsn] :
           errorRecovery_->takeReadyDeferredAcks(expectedPsn_)) {
        emitAckPacket(ackSeqNum, ackPsn);
      }
    };
    switch (result) {
      case PsnAction::ACCEPT:
        emitReadyDeferredAcks();
        return true;
      case PsnAction::BUFFER:
        // BUFFER: strategy buffers the OOO packet for later in-order
        // delivery; the socket still accepts it so byte-count completion
        // can advance.
        return true;
      case PsnAction::ACCEPT_AND_REQUEST_NACK:
        emitReadyDeferredAcks();
        errorRecovery_->onMissingMessage(seqNum, expectedPsn_);
        return true;
      case PsnAction::BUFFER_WITHOUT_ACK:
        if (ackRequested) {
          errorRecovery_->deferAck(seqNum, psn);
        }
        ackRequested = false;
        return true;
      case PsnAction::BUFFER_AND_REQUEST_NACK:
        if (ackRequested) {
          errorRecovery_->deferAck(seqNum, psn);
        }
        ackRequested = false;
        errorRecovery_->onMissingMessage(seqNum, expectedPsn_);
        return true;
      case PsnAction::SEND_NACK:
        errorRecovery_->onMissingMessage(seqNum, expectedPsn_);
        return false;
      case PsnAction::ACK_DUPLICATE:
        if (ackRequested) {
          // Re-ACKs the duplicate's own PSN. A strategy that wants the
          // cumulative watermark instead overrides ErrorRecovery::adjustAckPsn,
          // which createAndSendPkt() applies to every outgoing ACK; Go-Back-N
          // deliberately keeps per-packet PSNs.
          emitAckPacket(seqNum, psn);
        }
        return false;
      case PsnAction::DROP:
        return false;
    }
  }

  // Default: optimistic mode — PSN must match exactly.
  if (outOfOrderPktSupport_) {
    return true;
  }
  if (psn != expectedPsn_) {
    throw std::runtime_error(
        fmt::format(
            "OptimisticRoce: PSN mismatch actual={} != expected={}",
            psn,
            expectedPsn_));
  }
  ++expectedPsn_;
  return true;
}

void RoceSocketBase::emitAckPacket(const uint32_t seqNum, const uint32_t psn) {
  const SendInfo info({
      .packetSize = kControlPacketBytes,
      .isFirstPkt = true,
      .isLastPkt = true,
      .isAck = true,
      .isNack = false,
      .psn = psn,
      .sequenceNum = seqNum,
  });
  createAndSendPkt(info);
}

void RoceSocketBase::sendRnr(const uint32_t psn) {
  if (errorRecovery_) {
    errorRecovery_->onSendRnrRequested(psn);
    return;
  }
  throw std::runtime_error("Send RNR not implemented without error recovery");
}

void RoceSocketBase::emitNackPacket(
    const uint32_t seqNum,
    const uint32_t psn,
    const std::optional<uint8_t> rnrBackoffIdx) {
  const SendInfo info(
      {.packetSize = kControlPacketBytes,
       .isFirstPkt = true,
       .isLastPkt = true,
       .isAck = false,
       .isNack = true,
       .nackTime = rnrBackoffIdx.value_or(UINT8_MAX),
       .psn = psn,
       .sequenceNum = seqNum});
  createAndSendPkt(info);
}

void RoceSocketBase::wakeupForSend() {
  transport()->socketReady(this);
}

void RoceSocketBase::emitCongestionNotification() {
  const SendInfo info(
      {.packetSize = kControlPacketBytes,
       .isFirstPkt = true,
       .isLastPkt = true,
       .isAck = false,
       .isCnp = true});
  createAndSendPkt(info);
}

uint64_t RoceSocketBase::executeRetransmit(const uint64_t quota) {
  if (!errorRecovery_) {
    return 0;
  }
  const auto batch = errorRecovery_->getRetransmitBatch(quota);
  uint32_t minPsn = UINT32_MAX;
  uint32_t maxPsn = 0;
  for (const auto& info : batch.packets) {
    minPsn = std::min(info.psn, minPsn);
    maxPsn = std::max(info.psn, maxPsn);
    const auto readLatency = transport_->readBytesFromMemory(info.packetSize);
    simCtx_->schedule(
        Time::fromNs(readLatency),
        &RoceSocketBase::createAndSendPkt,
        this,
        info);
  }
  if (!batch.packets.empty()) {
    XLOGF(
        DBG2,
        "Retransmitted {} bytes ({} packets; psn {} to {})",
        batch.totalBytes,
        batch.packets.size(),
        minPsn,
        maxPsn);
  }
  return batch.totalBytes;
}

void RoceSocketBase::cancelTimersIfComplete() {
  if (!congestionControl_) {
    return;
  }
  const bool sendsDone =
      queuedMsgs_.empty() && sentMsgs_.empty() && completedSendMsgs_.empty();
  const bool recvsDone = expectedMsgs_.empty() && completedRecvMsgs_.empty();
  if (sendsDone && recvsDone) {
    congestionControl_->cancelTimers();
  }
}

Message* RoceSocketBase::removeSentMessage(const uint32_t seqNum) {
  if (sentMsgs_.empty()) {
    throw std::runtime_error("Received ACK with no outstanding sent messages");
  }
  if (!outOfOrderPktSupport_) {
    if (sentMsgs_.front()->sequenceNumber != seqNum) {
      throw std::runtime_error("Sequence number doesn't match ACK");
    }
    Message* const msg = sentMsgs_.front();
    sentMsgs_.pop_front();
    return msg;
  }

  const auto it = std::find_if(
      sentMsgs_.begin(), sentMsgs_.end(), [seqNum](const Message* msg) {
        return msg->sequenceNumber == seqNum;
      });
  if (it == sentMsgs_.end()) {
    throw std::runtime_error("Expected sequence number not found!!");
  }
  Message* const msg = *it;
  sentMsgs_.erase(it);
  return msg;
}

void RoceSocketBase::completeAckedMessages(
    const uint32_t seqNum,
    const AckResult& result) {
  if (!result.nextIncompleteSeqNum.has_value()) {
    Message* const msg = removeSentMessage(seqNum);
    completedSendMsgs_.try_emplace(msg->sequenceNumber, msg);
  } else {
    // In cumulative mode the strategy-provided watermark is authoritative;
    // seqNum only identifies the packet carrying the ACK and need not match an
    // outstanding message.
    const uint32_t nextIncompleteSeqNum = *result.nextIncompleteSeqNum;
    for (auto it = sentMsgs_.begin(); it != sentMsgs_.end();) {
      Message* const msg = *it;
      if (msg->sequenceNumber < nextIncompleteSeqNum) {
        it = sentMsgs_.erase(it);
        completedSendMsgs_.try_emplace(msg->sequenceNumber, msg);
      } else {
        ++it;
      }
    }
  }

  while (!completedSendMsgs_.empty()) {
    const auto it = completedSendMsgs_.find(nextSendCqeSequenceNum_);
    if (it == completedSendMsgs_.end()) {
      break;
    }
    onMessageAcked(it->second);
    ++nextSendCqeSequenceNum_;
    completedSendMsgs_.erase(it);
  }
}

void RoceSocketBase::ackRcvd(const uint32_t seqNum, const uint32_t psn) {
  if (isQpFailed()) {
    return;
  }

  const AckResult result =
      errorRecovery_ ? errorRecovery_->onAck(psn) : AckResult{};
  const bool completionAlreadyApplied = result.nextIncompleteSeqNum.has_value()
      ? *result.nextIncompleteSeqNum <= nextSendCqeSequenceNum_
      : seqNum < nextSendCqeSequenceNum_ || completedSendMsgs_.contains(seqNum);
  if (!result.isStale && !completionAlreadyApplied) {
    completeAckedMessages(seqNum, result);
  }

  *stats_.recvdAckPkts() += 1;
  *transport()->cosStats_.at(ackCos_).recvdAckPkts() += 1;

  cancelTimersIfComplete();
  transport()->socketReady(this);
}

void RoceSocketBase::nackRcvd(
    const uint32_t expectedPsn,
    const uint8_t nackTime) {
  if (isQpFailed()) {
    return;
  }

  *stats_.recvdNackPkts() += 1;
  auto& cosStat = transport()->cosStats_.at(ackCos_);
  *cosStat.recvdNackPkts() += 1;

  if (errorRecovery_) {
    const auto myId = getSrcId().toString();
    const auto partnerId = getDstId().toString();
    XLOGF(
        DBG2,
        "{} recvd NACK from {} with expPsn {}; last sent {}",
        myId,
        partnerId,
        expectedPsn,
        txPsn_);
    const std::optional<uint8_t> rnrBackoffIdx =
        nackTime == UINT8_MAX ? std::nullopt : std::optional<uint8_t>{nackTime};
    errorRecovery_->onNack(expectedPsn, rnrBackoffIdx);
  }
}

void RoceSocketBase::countEcnSent() noexcept {
  *stats_.sentEcnPkts() += 1;
}

void RoceSocketBase::countEcnRcvd(const RocePacket& pkt) {
  auto* const transport = this->transport();
  *stats_.recvdEcnPkts() += 1;
  *transport->cosStats_.at(transport->getCosForPacketType(pkt))
       .recvdEcnPkts() += 1;
}

void RoceSocketBase::cnpRcvd() {
  *stats_.recvdCnpPkts() += 1;
  auto& cosStat = transport()->cosStats_.at(cnpCos_);
  *cosStat.recvdCnpPkts() += 1;

  if (congestionControl_) {
    congestionControl_->onCongestionSignal();
  }
}

void RoceSocketBase::onPacketWrittenToMemory(const PacketPtr& pkt) {
  {
    const auto it = finalPacketToMsg_.find(pkt);
    if (it == finalPacketToMsg_.end()) {
      return;
    }
    if (it->second->sequenceNumber == nextRecvCqeSequenceNum_) {
      onMessageReceived(it->second);
      ++nextRecvCqeSequenceNum_;
    } else {
      completedRecvMsgs_.try_emplace(it->second->sequenceNumber, it->second);
    }
    finalPacketToMsg_.erase(it);
  }

  while (!completedRecvMsgs_.empty()) {
    const auto it = completedRecvMsgs_.find(nextRecvCqeSequenceNum_);
    if (it == completedRecvMsgs_.end()) {
      break;
    }
    onMessageReceived(it->second);
    nextRecvCqeSequenceNum_++;
    completedRecvMsgs_.erase(it);
  }

  cancelTimersIfComplete();
}

// ---------------------------------------------------------------------------
// RoceTransportBase
// ---------------------------------------------------------------------------

RoceTransportBase::RoceTransportBase(
    SimContext* simCtx,
    const FlexDeviceId& id,
    const packsim::thrift::NicConfig* FOLLY_NONNULL nicConfig,
    const RoceTransportConfig& roceCfg)
    : Transport(simCtx, id, nicConfig),
      defaultQueueToMemory_(simCtx, nicConfig),
      // NicConfig::maxQpDepth is signed; a value of zero or less means no
      // limit.
      maxQpDepth_(
          *nicConfig->maxQpDepth() > 0
              ? std::optional<size_t>{static_cast<size_t>(
                    *nicConfig->maxQpDepth())}
              : std::nullopt),
      dataCos_(*nicConfig->dataCos()),
      ackCos_(*nicConfig->ackCos()),
      cnpCos_(*nicConfig->dcqcnCfg()->npCnpPrio()),
      roceCfg_(roceCfg) {
  validateRnrTimeIdx(roceCfg_);
  defaultQueueToMemory_.setOnPacketReleasedCallback(
      [this](const PacketPtr& pkt) { onPacketWrittenToMemory(pkt); });
  // Only a transport driven without an endpoint, as in the transport tests,
  // stores packets here, and it subscribes through
  // setOnPfcStateChangedCallback. An endpoint hands every packet to
  // onPacketRcvd with the port it arrived on, which builds that port's buffer
  // before the socket stores the packet, so an endpoint-wired transport never
  // queues a packet here.
  defaultQueueToMemory_.setOnPfcStateChangedCallback(
      [this](uint8_t cos, PfcState state) { onPfcStateChanged(cos, state); });
  size_t numQs = std::max(1UL, nicConfig_->queueCfg()->size());
  const uint8_t maxCos = std::max({dataCos_, ackCos_, cnpCos_});
  numQs = std::max(numQs, size_t{maxCos} + 1);
  cosStats_.resize(numQs);

  if (roceCfg_.congestionControl ==
      RoceTransportConfig::CongestionControl::DCQCN) {
    DcqcnCongestionControl::validateConfig(
        *nicConfig->dcqcnCfg(), DataRate(*nicConfig->speed()));
    maxActiveSends_ = kDcqcnMaxActiveSends;
  }

  // Compute RoCE packet encapsulation overhead (header sizes + Ethernet
  // framing + preamble). Used by DcqcnCongestionControl for token-budget
  // calculations. createPacket<ROCE>(0) returns a packet with headers
  // only (no payload):
  //   Ethernet(22) + IPv6(40) + UDP(8) + BTH(24) = 94 bytes headerSize.
  // getPhyFrameSize() adds the 8-byte preamble (PHY-layer prefix on the
  // wire); IFG is excluded since the token budget represents data + L2
  // header + preamble, not between-frame idle.
  const auto emptyPkt = createPacket<PacketType::ROCE>(0);
  encapBytes_ = emptyPkt->getPhyFrameSize();
}

RoceSocketBase* RoceTransportBase::makeSocket(
    const FlexDeviceId& dst,
    const uint32_t localQPN,
    const uint32_t remoteQPN,
    const uint16_t port,
    const packsim::workload::thrift::Workload* workload) {
  // Build a factory so the strategy can take non-null references to the
  // socket and SimContext at construction time.
  RoceSocketBase::ErrorRecoveryFactory errorRecoveryFactory;
  if (roceCfg_.errorRecovery == RoceTransportConfig::ErrorRecovery::GO_BACK_N) {
    errorRecoveryFactory =
        [rnrTimeIdx = roceCfg_.rnrTimeIdx,
         selectiveGapRepair = roceCfg_.selectiveGapRepair](
            RoceSocketBase& socket,
            SimContext& simCtx) -> std::unique_ptr<ErrorRecovery> {
      return std::make_unique<GoBackNRecovery>(
          rnrTimeIdx,
          socket,
          simCtx,
          GoBackNRecovery::Options{
              .selectiveGapRepair = selectiveGapRepair,
              .inOrderDelivery = !socket.outOfOrderPktSupport(),
          });
    };
  }

  // Build a congestion-control factory so the DCQCN controller is constructed
  // inside the socket ctor (like error recovery), taking references to the
  // owning socket and SimContext. The controller wires its own token-bucket
  // wakeup (routed through the socket) and tracer snapshots in its constructor.
  RoceSocketBase::CongestionControlFactory congestionControlFactory;
  if (roceCfg_.congestionControl ==
      RoceTransportConfig::CongestionControl::DCQCN) {
    congestionControlFactory =
        [dcqcnCfg = *nicConfig_->dcqcnCfg(),
         src = id_,
         dst,
         speed = DataRate(*nicConfig_->speed()),
         mtu = *nicConfig_->mtu(),
         encapBytes = encapBytes_](
            CongestionControlSocket& socket,
            SimContext& simCtx) -> std::unique_ptr<CongestionControl> {
      return std::make_unique<DcqcnCongestionControl>(
          socket, simCtx, dcqcnCfg, src, dst, speed, mtu, encapBytes);
    };
  }

  // Create socket with both strategy factories.
  auto socket = std::make_unique<RoceSocketBase>(
      this,
      id_,
      dst,
      port,
      localQPN,
      remoteQPN,
      4200,
      workload,
      errorRecoveryFactory,
      congestionControlFactory);
  auto rawPtr = socket.get();

  socketIndices_.emplace(rawPtr, sockets_.size());
  sockets_.emplace_back(std::move(socket));
  return rawPtr;
}

Socket* RoceTransportBase::connect(
    const FlexDeviceId& dst,
    const uint16_t port,
    const uint32_t localQPN,
    const uint32_t remoteQPN,
    uint8_t /* priority */,
    const packsim::workload::thrift::Workload* workload) {
  const auto it = qpnToSocket_.find(localQPN);
  if (it != qpnToSocket_.end()) {
    return it->second;
  }

  auto socket = makeSocket(dst, localQPN, remoteQPN, port, workload);
  // Interpose rather than hand over a copy of the member: a copy taken here
  // freezes whatever the transport had at connect time, so a subscriber
  // registered afterwards would never reach this socket.
  socket->setOnMessageReceivedCallback(
      [this](Message* msg) { onMessageReceived(msg); });
  socket->setOnMessageAckedCallback(
      [this](Message* msg) { onMessageAcked(msg); });

  idToSocket_[{dst, localQPN}] = socket;
  qpnToSocket_[localQPN] = socket;
  return socket;
}

void RoceTransportBase::socketReady(RoceSocketBase* socket) {
  // Add socket to active set for round-robin scheduling.
  const auto it = socketIndices_.find(socket);
  if (it != socketIndices_.end()) {
    activeRoceSockets_.emplace(it->second, socket);
  }
  sendNext();
}

bool RoceTransportBase::trySendOnSocket(RoceSocketBase* socket) {
  auto* recovery = socket->getErrorRecovery();

  // Skip sockets blocked by the strategy (e.g. GoBackN RNR back-off).
  if (recovery && recovery->isSendBlocked()) {
    return false;
  }

  // Skip sockets a congestion-control strategy is holding back (e.g. a
  // window/back-off scheme). DCQCN leaves this at the default and throttles via
  // adjustQuota() instead.
  const auto* cc = socket->getCongestionControl();
  if (cc && cc->isSendBlocked()) {
    return false;
  }

  // Prioritize retransmission over new sends (error recovery).
  if (recovery && recovery->hasRetransmitPending()) {
    return socket->executeRetransmit(byteQuota_) > 0;
  }

  if (socket->queuedMsgs() > 0) {
    const auto [messageSent, sentBytes] = socket->sendBytes(byteQuota_);
    return sentBytes > 0 && messageSent;
  }

  return false;
}

void RoceTransportBase::sendNext() {
  if (activeSends_ == maxActiveSends_) {
    return;
  }
  if (sockets_.empty()) {
    throw std::runtime_error("No sockets present");
  }

  auto iter = nextSocketIndex_
      ? activeRoceSockets_.lower_bound(*nextSocketIndex_)
      : activeRoceSockets_.begin();

  // Stop when every active socket has been visited without progress.
  size_t socketsWithoutProgress = 0;

  while (activeSends_ < maxActiveSends_ && !activeRoceSockets_.empty() &&
         socketsWithoutProgress < activeRoceSockets_.size()) {
    if (iter == activeRoceSockets_.end()) {
      iter = activeRoceSockets_.begin();
    }

    auto* socket = iter->second;

    const bool sent = trySendOnSocket(socket);
    if (sent) {
      activeSends_++;
      socketsWithoutProgress = 0;
      XLOGF(DBG3, "Active Sends @ node {}: {}", id_.toString(), activeSends_);
    }

    if (socket->hasWork()) {
      if (!sent) {
        socketsWithoutProgress++;
      }
      ++iter;
    } else {
      iter = activeRoceSockets_.erase(iter);
    }
  }

  nextSocketIndex_ = (iter != activeRoceSockets_.end())
      ? std::make_optional(iter->first)
      : std::nullopt;
}

size_t RoceTransportBase::egressPortSlot(
    const FlexDeviceId& srcId,
    const FlexDeviceId& dstId,
    const size_t numPorts) noexcept {
  return egressPortSlot(devicePairKey(srcId, dstId), numPorts);
}

size_t RoceTransportBase::egressPortSlot(
    const uint64_t devicePairKey,
    const size_t numPorts) noexcept {
  XCHECK_GT(numPorts, 0u) << "numPorts must be positive";

  // devicePairKey is computed before Endpoint temporarily stamps port-level
  // addresses. EthernetPort::deliverData restores socketLevelConn before
  // packetRcvd, so a reactively-created receive socket sees those original IDs
  // swapped. Both directions of a QP pair therefore select the same slot. That
  // is required, not cosmetic:
  // routePacketByEgressNicPort rewrites the destination address from
  // dstNicPort, and the port-to-switch wiring is planar, so a packet leaving
  // src port N can only be delivered to dst port N.
  //
  // Keyed on device identity rather than QPNs so the slot is independent of how
  // QPNs are allocated. QPN-derived indices inherit the allocation stride --
  // two per QP pair, times the NIC count, times the QPs per NIC pair -- and any
  // stride sharing a factor with the port count leaves ports unreachable.
  return devicePairKey % numPorts;
}

bool RoceTransportBase::sendPacket(
    RoceSocketBase* socket,
    const PacketPtr& pkt) {
  if (!portSendPacketFns_.empty()) {
    auto portIt = portSendPacketFns_.begin();
    std::advance(
        portIt,
        egressPortSlot(socket->egressPortKey_, portSendPacketFns_.size()));

    // Checked cast: as<RocePacket>() is an unchecked static_cast, and sibling
    // packet types share the UdpPacket base, so an unchecked write would land
    // egressNicPort on a type that does not have one.
    auto* rocePkt = pkt.tryAs<RocePacket>();
    if (rocePkt != nullptr) {
      rocePkt->egressNicPort = portIt->first;
      rocePkt->dstNicPort = portIt->first;
    }
    return portIt->second(pkt);
  }
  return sendPacketFn(pkt);
}

void RoceTransportBase::addPort(
    const uint16_t egressPort,
    EthernetPort* /* ethPort */,
    const SendPacketFn& portSendPacketFn) {
  portSendPacketFns_.insert_or_assign(egressPort, portSendPacketFn);
}

bool RoceTransportBase::sendPacketWithCallback(
    RoceSocketBase* socket,
    const PacketPtr& pkt) {
  packetTxCbSocket_[pkt] = socket;
  return sendPacket(socket, pkt);
}

bool RoceTransportBase::writePacketToMemory(
    RoceSocketBase* /* socket */,
    const PacketPtr& pkt) {
  const auto [inQueue, pktEnqueued] = queuePacketToMemory(pkt);
  return pktEnqueued;
}

void RoceTransportBase::onPacketWrittenToMemory(const PacketPtr& pkt) {
  // A socket is unregistered when its workload rank completes, which can
  // happen while this packet waits in the RX buffer; nothing of that socket's
  // waits for the packet then.
  const auto it = qpnToSocket_.find(pkt.as<RocePacket>()->dstQPN());
  if (it == qpnToSocket_.end()) {
    return;
  }
  it->second->onPacketWrittenToMemory(pkt);
}

uint8_t RoceTransportBase::getCosForPacketType(
    const RocePacket& pkt) const noexcept {
  if (pkt.isAck || pkt.isNack) {
    return ackCos_;
  }
  return pkt.isCnp ? cnpCos_ : dataCos_;
}

void RoceTransportBase::countEcnSent(const RocePacket& pkt) {
  *cosStats_.at(getCosForPacketType(pkt)).sentEcnPkts() += 1;
  // Only a marked packet pays for this lookup. The QPN alone does not identify
  // the sender: an iterative workload unregisters its sockets when a rank
  // completes, while their packets can still be queued at the NIC, and a later
  // socket may reuse the QPN. Sockets are never destroyed, so a match on the
  // connection the packet was built with is exact.
  const std::lock_guard<std::mutex> lock(socketMutex_);
  const auto it = qpnToSocket_.find(pkt.srcQPN());
  if (it == qpnToSocket_.end()) {
    return;
  }
  const IPConnection* const socketConn = it->second->getConnection();
  if (pkt.conn == socketConn || pkt.socketLevelConn == socketConn) {
    static_cast<RoceSocketBase*>(it->second)->countEcnSent();
  }
}

bool RoceTransportBase::onPacketSent(
    const PacketPtr& pkt,
    EthernetPort* /* ethPort */) {
  // Every RoCE packet that left the NIC comes back here, with or without a
  // send callback, carrying any ECN-CE mark the NIC's own egress queue set.
  if (const auto* rocePkt = pkt.tryAs<RocePacket>();
      rocePkt != nullptr && rocePkt->ecn == Ipv6Packet::EcnType::ECN_CE) {
    countEcnSent(*rocePkt);
  }
  const auto it = packetTxCbSocket_.find(pkt);
  if (it != packetTxCbSocket_.end()) {
    it->second->onPacketSent(pkt);
    packetTxCbSocket_.erase(it);
    if (activeSends_ == 0) {
      throw std::runtime_error("activeSends_ underflow in onPacketSent");
    }
    activeSends_--;
    XLOGF(DBG, "Active Sends @ node {}: {}", id_.toString(), activeSends_);
    sendNext();
  }
  return true;
}

void RoceTransportBase::onPacketDropped(const PacketPtr& pkt) {
  const auto it = packetTxCbSocket_.find(pkt);
  if (it == packetTxCbSocket_.end()) {
    return;
  }
  packetTxCbSocket_.erase(it);
  if (activeSends_ == 0) {
    throw std::runtime_error("activeSends_ underflow in onPacketDropped");
  }
  --activeSends_;
  sendNext();
}

bool RoceTransportBase::onPacketRcvd(
    const PacketPtr& pkt,
    EthernetPort* ethPort) {
  if (queueToMemory(ethPort) == nullptr) {
    throw std::runtime_error("No queue found for port");
  }
  RocePacket* rocePkt = pkt.as<RocePacket>();

  findOrMakeSocket(rocePkt->srcId(), rocePkt->dstQPN(), rocePkt->srcQPN())
      ->onPacketRcvd(pkt);
  return true;
}

packsim::thrift::NicSummary RoceTransportBase::getNicStatsSummaryResults()
    const {
  packsim::thrift::NicSummary summary;
  summary.flexId() = id_.asVec();
  auto& nicCosStats = *summary.nicStats()->perCosStats();
  nicCosStats.resize(cosStats_.size());
  for (size_t i = 0; i < cosStats_.size(); i++) {
    nicCosStats.at(i) = cosStats_.at(i);
  }

  auto& perDestStats = *summary.nicStats()->perDestStats();
  perDestStats.reserve(sockets_.size());
  packsim::thrift::RoceV2TransportStats roceV2Stats;
  for (const auto& s : sockets_) {
    packsim::thrift::IntfPktStatsEntry entry;
    entry.destId() = s->getDstId().asVec();
    entry.intfPktStats() = s->getStats();
    perDestStats.emplace_back(std::move(entry));

    const auto* roceSocket = static_cast<const RoceSocketBase*>(s.get());
    if (const auto* recovery = roceSocket->getErrorRecovery()) {
      const auto retransmitStats = recovery->getRetransmitStats();
      *roceV2Stats.retxPkts() += retransmitStats.retxPkts;
      *roceV2Stats.retxBytes() += retransmitStats.retxBytes;
      *roceV2Stats.retxByTimer() += retransmitStats.retxByTimer;
      *roceV2Stats.retxByNack() += retransmitStats.retxByNack;
    }
  }

  if (roceCfg_.errorRecovery == RoceTransportConfig::ErrorRecovery::GO_BACK_N) {
    summary.nicStats()->transport()->roceV2() = std::move(roceV2Stats);
  }

  return summary;
}

NicRxBuffer* RoceTransportBase::queueToMemory(EthernetPort* ethPort) {
  if (ethPort == nullptr) {
    return &defaultQueueToMemory_;
  }

  const auto it = queueToMemoryPerPort_.find(ethPort->getPortNum());
  if (it != queueToMemoryPerPort_.end()) {
    return it->second.get();
  }

  const auto dataRate = ethPort->getDataRate();
  auto buf = std::make_unique<NicRxBuffer>(
      simCtx_,
      dataRate,
      *nicConfig_->writeEff(),
      *nicConfig_->writeEff(),
      *nicConfig_->perCosBufferSize(),
      *nicConfig_->perCosXoff(),
      *nicConfig_->perCosXon(),
      *nicConfig_->queueFilterCfg(),
      *nicConfig_->queueScheduleCfg());
  // A port's receive buffer pauses that port's peer.
  buf->setOnPfcStateChangedCallback(
      [&port = *ethPort](uint8_t cos, PfcState state) {
        port.setRxPfcState(cos, state);
      });
  // Install even when the bridge is detached: it is inert until `enableCbfc`
  // reattaches it, and nothing re-installs an observer on an already-built
  // buffer. `defaultQueueToMemory_` gets no observer because
  // `RoceTransportBase::onPacketRcvd` builds the per-port buffer before any
  // packet is stored, so the RoCE receive path never reaches it.
  if (auto* creditObserver = ethPort->rxCreditBridge()) {
    buf->setCreditObserver(creditObserver);
  }
  buf->setOnPacketReleasedCallback(
      [this](const PacketPtr& pkt) { onPacketWrittenToMemory(pkt); });
  auto ptr = buf.get();
  queueToMemoryPerPort_[ethPort->getPortNum()] = std::move(buf);
  return ptr;
}

NicRxBuffer* RoceTransportBase::queueToMemory(const uint16_t portNum) {
  if (!queueToMemoryPerPort_.empty()) {
    return queueToMemoryPerPort_.at(portNum).get();
  }
  return &defaultQueueToMemory_;
}

std::pair<int, bool> RoceTransportBase::queuePacketToMemory(
    const PacketPtr& pkt) {
  return queueToMemory(pkt.as<EthernetPacket>()->tag.port)->queuePacket(pkt);
}

} // namespace packsim
