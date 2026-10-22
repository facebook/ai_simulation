/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/queues/queue-packet-handler.h"

#include <cmath>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

#include <fmt/format.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/random.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

namespace {
// Divisor converting a percentage (0-100) into a probability fraction (0-1).
constexpr double kPercentDivisor = 100.0;
// Largest drop or mark percentage a WredEcnConfig accepts.
constexpr uint32_t kMaxPercentage = 100;

// Throws std::invalid_argument if `percentage`, read from the WredEcnConfig
// field `fieldName`, is above 100.
void validatePercentage(
    const uint32_t percentage,
    const std::string& fieldName) {
  if (percentage > kMaxPercentage) {
    throw std::invalid_argument(
        fmt::format(
            "WredEcnConfig.{} must be a percentage from 0 to {}, got {}",
            fieldName,
            kMaxPercentage,
            percentage));
  }
}

// Throws std::invalid_argument if cfg's exp or averagingPeriod is negative or
// non-finite, or averagingPeriod is 2^64 or more.
WREDAverageQueueLength buildAverageQueueLength(
    SimContext* simCtx,
    const packsim::thrift::WredEcnConfig& cfg) {
  const double exponent = *cfg.exp();
  if (!std::isfinite(exponent) || exponent < 0) {
    throw std::invalid_argument(
        fmt::format(
            "WredEcnConfig.exp must be finite and >= 0, got {}", exponent));
  }
  const double averagingPeriod = *cfg.averagingPeriod();
  // 2^64, the smallest double the uint64_t cast below cannot represent.
  constexpr double kUint64Limit = 18446744073709551616.0;
  if (!std::isfinite(averagingPeriod) || averagingPeriod < 0 ||
      averagingPeriod >= kUint64Limit) {
    throw std::invalid_argument(
        fmt::format(
            "WredEcnConfig.averagingPeriod must be finite, >= 0 and below "
            "2^64, got {}",
            averagingPeriod));
  }
  const auto periodNs = static_cast<uint64_t>(averagingPeriod);
  return WREDAverageQueueLength(
      simCtx,
      exponent,
      Time::fromNs(periodNs),
      Time::fromNs(simCtx->random.sampleUniform<uint64_t>(0, periodNs)));
}
} // namespace

QueuePacketHandler::QueuePacketHandler(
    SimContext* simCtx,
    const uint8_t id,
    const packsim::thrift::WredEcnConfig& cfg,
    Queues* /* queues */)
    : simCtx_(simCtx), avgQLen_(buildAverageQueueLength(simCtx, cfg)) {
  validatePercentage(*cfg.dropRateWRED(), "dropRateWRED");
  validatePercentage(*cfg.markRateECN(), "markRateECN");
  qId = id;

  minThresholdWRED_ = *cfg.minQSizeWRED();
  maxThresholdWRED_ = *cfg.maxQSizeWRED();
  dropPercentageWRED_ = *cfg.dropRateWRED();
  minThresholdECN_ = *cfg.minQSizeECN();
  maxThresholdECN_ = *cfg.maxQSizeECN();
  markPercentageECN_ = *cfg.markRateECN();
}

QueuePacketHandler::~QueuePacketHandler() = default;

DoNothingPacketHandler::DoNothingPacketHandler(
    SimContext* simCtx,
    const uint8_t id,
    const packsim::thrift::WredEcnConfig& cfg,
    Queues* queues)
    : QueuePacketHandler(simCtx, id, cfg, queues) {}

////////////////// WRED packet handler functions
WREDQueuePacketHandler::WREDQueuePacketHandler(
    SimContext* simCtx,
    const uint8_t id,
    const packsim::thrift::WredEcnConfig& cfg,
    Queues* queues)
    : QueuePacketHandler(simCtx, id, cfg, queues) {}

bool WREDQueuePacketHandler::handlePacketAfterFiltering(
    const PacketPtr& /* pkt */) {
  const auto qLen = avgQLen_(); // Get current value of queue length
  bool admit;
  if (qLen < minThresholdWRED_) {
    admit = true;
  } else if (qLen >= maxThresholdWRED_) {
    admit = false;
  } else {
    const double delta = qLen - minThresholdWRED_;
    const double dropProbPct =
        (dropPercentageWRED_ * delta) / (maxThresholdWRED_ - minThresholdWRED_);
    // This function is expected to return true if the packet should be
    // _allowed_ to continue, which happens with probability 1 - dropProb
    admit = simCtx_->random.sampleBernoulli(1 - dropProbPct / kPercentDivisor);
  }

  return admit;
}

QueuePacketHandlerType WREDQueuePacketHandler::getQueuePacketHandlerType() {
  return QueuePacketHandlerType::WRED;
}

// WRED does not drop packet after pop
bool WREDQueuePacketHandler::handlePacketAfterScheduling(
    const PacketPtr& /* pkt */) {
  return false;
}

void WREDQueuePacketHandler::setParams(
    const uint32_t minThres,
    const uint32_t maxThres,
    const uint32_t dropPer) {
  minThresholdWRED_ = minThres;
  maxThresholdWRED_ = maxThres;
  dropPercentageWRED_ = dropPer;
}

////////////////// ECN packet handler functions
ECNQueuePacketHandler::ECNQueuePacketHandler(
    SimContext* simCtx,
    const uint8_t id,
    const packsim::thrift::WredEcnConfig& cfg,
    Queues* queues)
    : QueuePacketHandler(simCtx, id, cfg, queues) {}

QueuePacketHandlerType ECNQueuePacketHandler::getQueuePacketHandlerType() {
  return QueuePacketHandlerType::ECN;
}

// ECN does not mark packet when it is pushed
bool ECNQueuePacketHandler::handlePacketAfterFiltering(
    const PacketPtr& /* pkt */) {
  return true;
}

// Similar logic as WRED, but only mark the packet instead of drop. Marks CE
// whatever the packet's ECN codepoint, Not-ECT included.
bool ECNQueuePacketHandler::handlePacketAfterScheduling(const PacketPtr& pkt) {
  bool markPkt = false;
  const uint64_t qLen = avgQLen_();
  if (qLen >= maxThresholdECN_) {
    markPkt = true;
  } else {
    if (qLen >= minThresholdECN_) {
      const double delta = qLen - minThresholdECN_;
      const double markProbPct =
          (markPercentageECN_ * delta) / (maxThresholdECN_ - minThresholdECN_);
      markPkt = simCtx_->random.sampleBernoulli(markProbPct / kPercentDivisor);
    }
  }
  if (markPkt) {
    pkt.as<Ipv6Packet>()->ecn = Ipv6Packet::EcnType::ECN_CE;
  }
  return markPkt;
}

void ECNQueuePacketHandler::setParams(
    const uint32_t minThres,
    const uint32_t maxThres,
    const uint32_t markPer) {
  minThresholdECN_ = minThres;
  maxThresholdECN_ = maxThres;
  markPercentageECN_ = markPer;
}

////////////////// Voq ECN packet handler functions
VoqECNQueuePacketHandler::VoqECNQueuePacketHandler(
    SimContext* simCtx,
    const uint8_t id,
    const packsim::thrift::WredEcnConfig& cfg,
    Queues* queues)
    : QueuePacketHandler(simCtx, id, cfg, queues) {}

// VoqECN does not mark packet when it is pushed
bool VoqECNQueuePacketHandler::handlePacketAfterFiltering(
    const PacketPtr& /* pkt */) {
  return true;
}

bool VoqECNQueuePacketHandler::handlePacketAfterScheduling(
    const PacketPtr& /* pkt */) {
  return false;
}

bool VoqECNQueuePacketHandler::markEcnPacket(
    const PacketPtr& pkt,
    const uint64_t queLength) {
  bool markPkt = false;
  if (queLength >= maxThresholdECN_) {
    markPkt = true;
  } else {
    if (queLength >= minThresholdECN_) {
      const double delta = queLength - minThresholdECN_;
      const double markProbPct =
          (markPercentageECN_ * delta) / (maxThresholdECN_ - minThresholdECN_);
      markPkt = simCtx_->random.sampleBernoulli(markProbPct / kPercentDivisor);
    }
  }
  if (markPkt) {
    pkt.as<Ipv6Packet>()->ecn = Ipv6Packet::EcnType::ECN_CE;
  }
  return markPkt;
}

void VoqECNQueuePacketHandler::setParams(
    const uint32_t minThres,
    const uint32_t maxThres,
    const uint32_t markPer) {
  minThresholdECN_ = minThres;
  maxThresholdECN_ = maxThres;
  markPercentageECN_ = markPer;
}

QueuePacketHandlerType VoqECNQueuePacketHandler::getQueuePacketHandlerType() {
  return QueuePacketHandlerType::VOQ_ECN;
}

////////////////// WRED+ECN packet handler functions
WREDECNQueuePacketHandler::WREDECNQueuePacketHandler(
    SimContext* simCtx,
    const uint8_t id,
    const packsim::thrift::WredEcnConfig& cfg,
    Queues* queues)
    : QueuePacketHandler(simCtx, id, cfg, queues) {}

bool WREDECNQueuePacketHandler::handlePacketAfterFiltering(
    const PacketPtr& pkt) {
  const uint64_t qLen = avgQLen_();
  const auto ecn = pkt.as<Ipv6Packet>()->ecn;
  bool admit{true};
  if (ecn == Ipv6Packet::EcnType::ECN_NOT_ECT) {
    if (qLen < minThresholdWRED_) {
      admit = true;
    } else if (qLen >= maxThresholdWRED_) {
      admit = false;
    } else {
      const double delta = qLen - minThresholdWRED_;
      const double dropProbPct = (dropPercentageWRED_ * delta) /
          (maxThresholdWRED_ - minThresholdWRED_);
      // This function is expected to return true if the packet should be
      // _allowed_ to continue, which happens with probability 1 - dropProb
      admit =
          simCtx_->random.sampleBernoulli(1 - dropProbPct / kPercentDivisor);
    }
  }

  return admit;
}

bool WREDECNQueuePacketHandler::handlePacketAfterScheduling(
    const PacketPtr& pkt) {
  const uint64_t qLen = avgQLen_();

  const auto ecn = pkt.as<Ipv6Packet>()->ecn;
  if (ecn == Ipv6Packet::EcnType::ECN_NOT_ECT) {
    return false;
  }

  bool markPkt = false;
  if (qLen >= maxThresholdECN_) {
    markPkt = true;
  } else {
    if (qLen >= minThresholdECN_) {
      const double delta = qLen - minThresholdECN_;
      const double markProbPct =
          (markPercentageECN_ * delta) / (maxThresholdECN_ - minThresholdECN_);
      markPkt = simCtx_->random.sampleBernoulli(markProbPct / kPercentDivisor);
    }
  }
  if (markPkt) {
    pkt.as<Ipv6Packet>()->ecn = Ipv6Packet::EcnType::ECN_CE;
  }
  return markPkt;
}

QueuePacketHandlerType WREDECNQueuePacketHandler::getQueuePacketHandlerType() {
  return QueuePacketHandlerType::WRED_ECN;
}

void WREDECNQueuePacketHandler::setParams(
    const uint32_t minThresWRED,
    const uint32_t maxThresWRED,
    const uint32_t dropPerWRED,
    const uint32_t minThresECN,
    const uint32_t maxThresECN,
    const uint32_t markPerECN) {
  minThresholdWRED_ = minThresWRED;
  maxThresholdWRED_ = maxThresWRED;
  dropPercentageWRED_ = dropPerWRED;
  minThresholdECN_ = minThresECN;
  maxThresholdECN_ = maxThresECN;
  markPercentageECN_ = markPerECN;
}

std::unique_ptr<QueuePacketHandler> buildHandler(
    SimContext* simCtx,
    const uint8_t id,
    const packsim::thrift::WredEcnConfig& cfg,
    Queues* queues) {
  switch (*cfg.queueProfileCfg()) {
    case packsim::thrift::QueueProfileType::NONE:
      return std::make_unique<DoNothingPacketHandler>(simCtx, id, cfg, queues);
    case packsim::thrift::QueueProfileType::WRED:
      return std::make_unique<WREDQueuePacketHandler>(simCtx, id, cfg, queues);
    case packsim::thrift::QueueProfileType::ECN:
      return std::make_unique<ECNQueuePacketHandler>(simCtx, id, cfg, queues);
    case packsim::thrift::QueueProfileType::VOQ_ECN:
      return std::make_unique<VoqECNQueuePacketHandler>(
          simCtx, id, cfg, queues);
    case packsim::thrift::QueueProfileType::WRED_ECN:
      return std::make_unique<WREDECNQueuePacketHandler>(
          simCtx, id, cfg, queues);
    default:
      throw std::runtime_error("Unknown queue packet handler type");
  }
}

} // namespace packsim
