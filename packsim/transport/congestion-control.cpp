/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/transport/congestion-control.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <stdexcept>
#include <string>

#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/trace.h"
#include "packsim/transport/rate-limiter.h"
#include "packsim/transport/roce-constants.h"

namespace packsim {

namespace {
constexpr double kBitsPerGbps = 1'000'000'000.0;
constexpr uint64_t kMbpsToBps = 1'000'000;
// DcqcnConfig.rpDceTcpG is g in units of 1/1024.
constexpr double kDceTcpGScale = 1024.0;
} // namespace

void DcqcnCongestionControl::validateConfig(
    const packsim::thrift::DcqcnConfig& cfg,
    const DataRate& nicSpeed) {
  const double g = cfg.rpDceTcpG().value() / kDceTcpGScale;
  if (!((g >= 0.0) && (g <= 1.0))) {
    throw std::runtime_error("Invalid rpDceTcpG value: g must be in [0, 1]");
  }
  const double minRateBps = cfg.rpMinRate().value() * kMbpsToBps;
  // A cut never takes the rate below rpMinRate, so a minimum below 1 bps would
  // let the rate reach 0 (an infinite time per bit), and the rate must stay
  // within the line rate.
  if (!(minRateBps >= 1 && minRateBps <= nicSpeed.getBps())) {
    throw std::invalid_argument(
        fmt::format(
            "DcqcnConfig.rpMinRate {} Mbps must be at least 0.000001 Mbps (1 bps) and at most the NIC speed of {} Mbps",
            cfg.rpMinRate().value(),
            nicSpeed.getBps() / static_cast<double>(kMbpsToBps)));
  }
}

DcqcnCongestionControl::DcqcnCongestionControl(
    CongestionControlSocket& socket,
    SimContext& simCtx,
    const packsim::thrift::DcqcnConfig& cfg,
    const FlexDeviceId& src,
    const FlexDeviceId& dst,
    const DataRate& nicSpeed,
    const uint32_t smtu,
    const uint32_t encapBytes)
    : socket_(&socket),
      simCtx_(&simCtx),
      srcHost_(src),
      dstHost_(dst),
      targetRate_(nicSpeed),
      currRate_(nicSpeed),
      maxRate_(nicSpeed),
      encapBytes_(encapBytes) {
  validateConfig(cfg, nicSpeed);
  g_ = cfg.rpDceTcpG().value() / kDceTcpGScale;
  alphaTimerPeriod_ = Time::fromUs(cfg.rpDceTcpRtt().value());
  rateIncreaseTimerPeriod_ = Time::fromUs(cfg.rpTimerReset().value());
  clampTgtRate_ = cfg.rpClampTgtRate().value();
  minRate_ =
      DataRate(static_cast<uint64_t>(cfg.rpMinRate().value() * kMbpsToBps));
  rpThreshold_ = *cfg.rpThreshold();
  rateAddIncrease_ = DataRate(cfg.rpAiRate().value() * kMbpsToBps);
  rateReduceMonitorPeriod_ = Time::fromUs(*cfg.rpRateReduceMonitorPeriod());

  alpha_ =
      cfg.rpInitialAlpha().value() / (std::pow(2, (cfg.rpGd().value() - 1)));

  tokenBucketRl_ = std::make_unique<TokenBucketRateLimiter>(
      &simCtx,
      *cfg.rateLimiterBucketSize(),
      currRate_,
      *cfg.rateLimiterRefreshPeriodNs(),
      *cfg.dcqcnEnable(),
      dst);
  tokenBucketRl_->setNumBytesAvailable(smtu);
  // Replenished rate-limiter tokens wake the socket to try sending again.
  tokenBucketRl_->setOnBytesAvailableCallback(
      [this](const FlexDeviceId&) { socket_->wakeupForSend(); });

  if (*cfg.dcqcnEnable()) {
    alphaResumeEvent_ = simCtx_->schedule(
        alphaTimerPeriod_, &DcqcnCongestionControl::timerAlphaUpdate, this);
    rpTimerEvent_ = simCtx_->schedule(
        rateIncreaseTimerPeriod_,
        &DcqcnCongestionControl::rateIncreaseTimer,
        this);
  }
  registerTracerSnapshots();
}

DcqcnCongestionControl::~DcqcnCongestionControl() {
  // Timers should already be cancelled when the socket runs out of work;
  // this is just defensive.
  cancelRateTimers();
}

void DcqcnCongestionControl::onPacketReceived(const bool ecnCe) {
  if (ecnCe) {
    socket_->emitCongestionNotification();
  }
}

void DcqcnCongestionControl::registerTracerSnapshots() {
  simCtx_->tracer.registerSnapshot(
      fmt::format(
          "{}->{}_TARGET_RATE", srcHost_.toString(), dstHost_.toString()),
      [this]() { return targetRate_.getBps(); });
  simCtx_->tracer.registerSnapshot(
      fmt::format(
          "{}->{}_CURRENT_RATE", srcHost_.toString(), dstHost_.toString()),
      [this]() { return currRate_.getBps(); });
}

uint32_t DcqcnCongestionControl::sendSizeToTokens(const uint32_t bytes) const {
  const auto numPkts = static_cast<double>(bytes) / kMaxRdmaPayload;
  const auto hdrBytes = numPkts * encapBytes_;
  return bytes + hdrBytes;
}

double DcqcnCongestionControl::getAvailTokens() const {
  return tokenBucketRl_->getMaxBytesAllowed();
}

uint64_t DcqcnCongestionControl::getByteQuotaFromTokens(
    const uint64_t tokens) const {
  const uint64_t fullSizePktTokens = kMaxRdmaPayload + encapBytes_;
  const uint64_t maxFullPkts = tokens / fullSizePktTokens;
  uint64_t byteQuota = maxFullPkts * kMaxRdmaPayload;
  const uint64_t remTokens = tokens - maxFullPkts * fullSizePktTokens;
  if (remTokens > encapBytes_) {
    byteQuota += (remTokens - encapBytes_);
  }
  return byteQuota;
}

size_t DcqcnCongestionControl::adjustQuota(const size_t requestedQuota) const {
  return std::min(requestedQuota, getByteQuotaFromTokens(getAvailTokens()));
}

void DcqcnCongestionControl::onBytesSent(const uint32_t dataBytes) {
  tokenBucketRl_->consumeTokens(sendSizeToTokens(dataBytes));
}

void DcqcnCongestionControl::onCongestionSignal() {
  cnpAlphaUpdate();
  if (!rateReduceMonitorTimerRunning_) {
    startRateReduceMonitorTimer();
    cnpRateUpdate();
  }
}

void DcqcnCongestionControl::cancelTimers() {
  cancelRateTimers();
}

void DcqcnCongestionControl::cancelRateTimers() {
  alphaResumeEvent_.cancel();
  rpTimerEvent_.cancel();
  tokenBucketRl_->cancelRefreshEvent();
}

void DcqcnCongestionControl::startRateReduceMonitorTimer() {
  rateReduceMonitorTimerRunning_ = true;
  simCtx_->schedule(
      rateReduceMonitorPeriod_,
      &DcqcnCongestionControl::stopRateReduceMonitorTimer,
      this);
}

void DcqcnCongestionControl::stopRateReduceMonitorTimer() {
  rateReduceMonitorTimerRunning_ = false;
}

/*This function is scheduled every alpha timer. If cnp was received during that
time, alpha increased or else reduced
*/
void DcqcnCongestionControl::timerAlphaUpdate() {
  XLOGF(
      DBG,
      "At time={}, Src Host={}, to Dst Host={}, Alpha Update Period Elapsed",
      simCtx_->nowNs(),
      srcHost_.toString(),
      dstHost_.toString());
  if (gotCnpDuringAlphaInterval_) {
    alpha_ = (1 - g_) + (alpha_ * g_);
  } else {
    alpha_ = alpha_ * g_;
  }
  gotCnpDuringAlphaInterval_ = false;
  alphaResumeEvent_.cancel();
  alphaResumeEvent_ = simCtx_->schedule(
      alphaTimerPeriod_, &DcqcnCongestionControl::timerAlphaUpdate, this);
}

void DcqcnCongestionControl::cnpAlphaUpdate() {
  gotCnpDuringAlphaInterval_ = true;
}

/*
Here all rate increase counters get reset and
CNP received is noted - for alpha reduction purposes
Sending Rate is cut
*/
void DcqcnCongestionControl::cnpRateUpdate() {
  // If set, when receiving a CNP, the target rate is always updated
  // to be the current rate (contrary to original algorithm)
  if (clampTgtRate_) {
    targetRate_ = currRate_;
  } else {
    if (numTimeBasedRateIncrEvents_ != 0) {
      targetRate_ = currRate_;
    }
  }
  numTimeBasedRateIncrEvents_ = 0;

  XLOGF(
      DBG,
      "At time={}, Received CNP, before, alpha={}, g={}, curr rate={}",
      simCtx_->nowNs(),
      alpha_,
      g_,
      currRate_.getBps());
  const double reducedRate = (currRate_.getBps() * (1 - (alpha_ / 2.0)));
  const double updRate =
      std::max(static_cast<double>(minRate_.getBps()), reducedRate);
  XLOGF(
      DBG,
      "At time={}, Received CNP, reducedRate={}, updRate={}, minRate={}, alpha={}",
      simCtx_->nowNs(),
      reducedRate,
      updRate,
      minRate_.getBps(),
      alpha_);
  currRate_ = DataRate(updRate);
  XLOGF(
      DBG,
      "curr={}, min={}, max={}",
      currRate_.getBps(),
      minRate_.getBps(),
      maxRate_.getBps());
  if (!((currRate_ >= minRate_) && (currRate_ <= maxRate_))) {
    throw std::runtime_error("Current Rate out of valid range");
  }
  rpTimerEvent_.cancel();
  rpTimerEvent_ = simCtx_->schedule(
      rateIncreaseTimerPeriod_,
      &DcqcnCongestionControl::rateIncreaseTimer,
      this);
  // enter fast recovery phase
  rpStage_ = RpStageType::FAST_RECOVERY;
  tokenBucketRl_->setRateParam(currRate_);
  XLOGF(
      DBG,
      "{},{},{},Reason=CNP Received,{},{},{}",
      simCtx_->nowNs(),
      srcHost_.toString(),
      dstHost_.toString(),
      (currRate_.getBps() / kBitsPerGbps),
      (targetRate_.getBps() / kBitsPerGbps),
      alpha_);
}

void DcqcnCongestionControl::rateIncreaseTimer() {
  XLOGF(
      DBG,
      "At time={}, Src Host={}, to Dst Host={}, Rate Increase Timer Event, increase Time Stage ={}",
      simCtx_->nowNs(),
      srcHost_.toString(),
      dstHost_.toString(),
      numTimeBasedRateIncrEvents_);
  rpTimerEvent_.cancel();
  rpTimerEvent_ = simCtx_->schedule(
      rateIncreaseTimerPeriod_,
      &DcqcnCongestionControl::rateIncreaseTimer,
      this);
  numTimeBasedRateIncrEvents_++;
  if (rpStage_ == RpStageType::FAST_RECOVERY) {
    fastRecoveryTime();
  } else {
    activeIncreaseTime();
  }
}

void DcqcnCongestionControl::fastRecoveryTime() {
  XLOGF(
      DBG,
      "At time={}, Src Host={}, to Dst Host={}, Before In Fast Recovery Time Stage={}, curr rate={} target rate={}",
      simCtx_->nowNs(),
      srcHost_.toString(),
      dstHost_.toString(),
      numTimeBasedRateIncrEvents_,
      currRate_.getBps(),
      targetRate_.getBps());
  tokenBucketRl_->setRateParam(currRate_);
  currRate_ = DataRate(currRate_.getBps() / 2 + targetRate_.getBps() / 2);
  if (numTimeBasedRateIncrEvents_ >= rpThreshold_) {
    rpStage_ = RpStageType::AI;
  }
  XLOGF(
      DBG,
      "At time={}, Src Host={}, to Dst Host={}, After In Fast Recovery Time Stage={}, curr rate={} target rate={}",
      simCtx_->nowNs(),
      srcHost_.toString(),
      dstHost_.toString(),
      numTimeBasedRateIncrEvents_,
      currRate_.getBps(),
      targetRate_.getBps());
  tokenBucketRl_->setRateParam(currRate_);
  XLOGF(
      DBG,
      "{},{},{},Reason=FR Rate Increase,{},{},{}",
      simCtx_->nowNs(),
      srcHost_.toString(),
      dstHost_.toString(),
      (currRate_.getBps() / kBitsPerGbps),
      (targetRate_.getBps() / kBitsPerGbps),
      alpha_);
}

void DcqcnCongestionControl::activeIncreaseTime() {
  XLOGF(
      DBG,
      "At time={}, Src Host={}, to Dst Host={}, Before In Active Increase Time Stage={}, curr rate={} target rate={}",
      simCtx_->nowNs(),
      srcHost_.toString(),
      dstHost_.toString(),
      numTimeBasedRateIncrEvents_,
      currRate_.getBps(),
      targetRate_.getBps());

  // The target never exceeds the line rate, so neither does the current rate,
  // which averages it with a rate at or below the line rate.
  targetRate_ = std::min(
      DataRate(targetRate_.getBps() + rateAddIncrease_.getBps()), maxRate_);

  currRate_ = DataRate(currRate_.getBps() / 2 + targetRate_.getBps() / 2);
  XLOGF(
      DBG,
      "After At time={}, Src Host={}, to Dst Host={}, After In Active Increase Time Stage={}, curr rate={} target rate={}",
      simCtx_->nowNs(),
      srcHost_.toString(),
      dstHost_.toString(),
      numTimeBasedRateIncrEvents_,
      currRate_.getBps(),
      targetRate_.getBps());
  tokenBucketRl_->setRateParam(currRate_);
  XLOGF(
      DBG,
      "{},{},{},Reason=AI Rate Increase,{},{},{}",
      simCtx_->nowNs(),
      srcHost_.toString(),
      dstHost_.toString(),
      (currRate_.getBps() / kBitsPerGbps),
      (targetRate_.getBps() / kBitsPerGbps),
      alpha_);
}

} // namespace packsim
