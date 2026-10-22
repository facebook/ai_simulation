/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include <folly/memory/not_null.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/nic_config_types.h"
#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/transport/rate-limiter.h"

namespace packsim {

/**
 * Narrow socket-side interface for congestion-control strategies (mirrors
 * ErrorRecoverySocket). The socket owns packet construction and transport
 * wakeups; the strategy owns its algorithm state.
 */
class CongestionControlSocket {
 public:
  virtual ~CongestionControlSocket() = default;
  CongestionControlSocket(const CongestionControlSocket&) = delete;
  CongestionControlSocket& operator=(const CongestionControlSocket&) = delete;
  CongestionControlSocket(CongestionControlSocket&&) = delete;
  CongestionControlSocket& operator=(CongestionControlSocket&&) = delete;

  /** Emit one congestion-notification packet (CNP) toward the sender. */
  virtual void emitCongestionNotification() = 0;

  /**
   * Notify the transport the socket may be able to send again (rate-limiter
   * tokens replenished, congestion window opened, ...).
   */
  virtual void wakeupForSend() = 0;

 protected:
  CongestionControlSocket() = default;
};

/**
 * Abstract base for RoCE congestion-control strategies.
 *
 * Subclasses (e.g. DcqcnCongestionControl) implement a specific algorithm.
 * The owning socket gates every send on adjustQuota() and reports sent bytes
 * via onBytesSent() (algorithm-agnostic: rate-based token buckets or
 * window-based cwnd both fit). Congestion feedback arrives through the
 * defaulted hooks — each algorithm overrides only the signals it uses.
 */
class CongestionControl {
 public:
  virtual ~CongestionControl() = default;
  CongestionControl(const CongestionControl&) = delete;
  CongestionControl& operator=(const CongestionControl&) = delete;
  CongestionControl(CongestionControl&&) = delete;
  CongestionControl& operator=(CongestionControl&&) = delete;

  // ---- Send gating (every strategy implements) ----
  /**
   * Bytes the socket may send now.
   *
   * @param requestedQuota  Bytes the socket wants to send.
   *
   * @return                Bytes the socket may send (<= requestedQuota; may
   *                        be 0).
   */
  virtual size_t adjustQuota(size_t requestedQuota) const = 0;

  /**
   * Account for bytes just sent (consume tokens / add in-flight).
   *
   * @param dataBytes  Number of data bytes just sent.
   */
  virtual void onBytesSent(uint32_t dataBytes) = 0;

  // ---- Congestion feedback (override only what the algorithm uses) ----
  /**
   * A congestion signal arrived at the sender (in RoCE, a CNP). DCQCN cuts
   * rate here. Default: ignore.
   */
  virtual void onCongestionSignal() {}

  /**
   * A packet arrived at the receiver. The strategy may emit feedback via its
   * socket (DCQCN emits a CNP). Default: ignore.
   *
   * @param ecnCe  True if the packet carried an ECN-CE (Explicit Congestion
   *               Notification, Congestion Experienced) mark.
   */
  virtual void onPacketReceived(bool /* ecnCe */) {}

  // ---- Scheduler / lifecycle ----
  /**
   * Whether the socket should skip sending entirely right now — a stronger
   * signal than adjustQuota() returning 0. DCQCN relies on adjustQuota() == 0;
   * a window/back-off strategy may override.
   *
   * @return  True if the socket should defer sending. Default: false.
   */
  virtual bool isSendBlocked() const noexcept {
    return false;
  }

  /** Cancel recurring timers so the sim can quiesce. Default: none. */
  virtual void cancelTimers() {}

 protected:
  CongestionControl() = default;
};

// Rate-increase stage of the DCQCN reaction point. Fast recovery converges the
// current rate back toward the target; active increase then probes for more.
enum class RpStageType : uint8_t {
  FAST_RECOVERY = 0,
  AI = 1,
};

/**
 * DCQCN congestion control for RoCE sockets.
 *
 * Owns the whole reaction-point algorithm: the alpha and rate-increase timers,
 * the fast-recovery/active-increase stage machine, and the token bucket that
 * paces sends at the current rate. The owning socket gates every send on
 * adjustQuota() and feeds back congestion through onCongestionSignal() (a CNP
 * arrived at the sender) and onPacketReceived() (an ECN-CE mark to reflect).
 *
 * Constructed via inversion of control: the owning socket and SimContext are
 * passed by reference so the controller can wire its own token-bucket wakeup
 * (routed through the socket) and register its tracer snapshots at build time.
 */
class DcqcnCongestionControl final : public CongestionControl {
 public:
  /**
   * Builds the controller at the line rate and, when DCQCN is enabled, starts
   * its alpha and rate-increase timers.
   *
   * @param socket      The socket the controller paces and wakes.
   * @param simCtx      The socket's simulation context.
   * @param cfg         The DCQCN parameters.
   * @param src         The sending host, for traces.
   * @param dst         The receiving host, for traces.
   * @param nicSpeed    The line rate: the initial, and highest, rate.
   * @param smtu        The initial token-bucket allowance, in bytes.
   * @param encapBytes  Per-packet encapsulation bytes the pacing charges.
   *
   * @throw std::runtime_error or std::invalid_argument if `validateConfig`
   *        rejects `cfg`.
   */
  DcqcnCongestionControl(
      CongestionControlSocket& socket,
      SimContext& simCtx,
      const packsim::thrift::DcqcnConfig& cfg,
      const FlexDeviceId& src,
      const FlexDeviceId& dst,
      const DataRate& nicSpeed,
      uint32_t smtu,
      uint32_t encapBytes);
  ~DcqcnCongestionControl() override;

  DcqcnCongestionControl(const DcqcnCongestionControl&) = delete;
  DcqcnCongestionControl& operator=(const DcqcnCongestionControl&) = delete;
  DcqcnCongestionControl(DcqcnCongestionControl&&) = delete;
  DcqcnCongestionControl& operator=(DcqcnCongestionControl&&) = delete;

  /**
   * Check the DCQCN parameters a controller is built with against the line
   * rate. The RoCE transport calls it when it is built, so that a bad config
   * fails when the simulation is built, before a queue pair carries a message.
   *
   * @param cfg       The DCQCN parameters.
   * @param nicSpeed  The line rate.
   *
   * @throw std::runtime_error if `rpDceTcpG` is outside [0, 1024];
   *        std::invalid_argument if `rpMinRate` is below 1 bps (0, negative or
   *        NaN) or above `nicSpeed`.
   */
  static void validateConfig(
      const packsim::thrift::DcqcnConfig& cfg,
      const DataRate& nicSpeed);

  /**
   * Adjust the requested send quota based on available DCQCN tokens.
   *
   * @param requestedQuota  The maximum bytes the socket wants to send.
   *
   * @return The actual bytes the socket is allowed to send (may be 0).
   */
  size_t adjustQuota(size_t requestedQuota) const override;

  /**
   * Consume tokens after sending bytes.
   *
   * @param dataBytes  The number of data bytes that were sent.
   */
  void onBytesSent(uint32_t dataBytes) override;

  /**
   * Handle a congestion signal from the network (in RoCE, a received CNP).
   * Triggers DCQCN alpha update and rate reduction.
   */
  void onCongestionSignal() override;

  // A packet arrived at the receiver; emit a CNP toward the sender when it
  // carried an ECN-CE mark.
  void onPacketReceived(bool ecnCe) override;

  // Cancel all recurring DCQCN timers so the simulation can finish.
  void cancelTimers() override;

  // ---- Diagnostics (read-only accessors) ----
  const DataRate& currentRate() const noexcept {
    return currRate_;
  }
  const DataRate& targetRate() const noexcept {
    return targetRate_;
  }
  double alpha() const noexcept {
    return alpha_;
  }
  RpStageType rpStage() const noexcept {
    return rpStage_;
  }
  // Rate-increase timer ticks since the last CNP reset the counter.
  uint32_t numRateIncreaseEvents() const noexcept {
    return numTimeBasedRateIncrEvents_;
  }
  // Rate currently programmed into the pacing token bucket. Tracks
  // currentRate() except while a stage transition is mid-flight.
  DataRate rateLimit() const {
    return tokenBucketRl_->getRateParam();
  }

 private:
  // ---- Reaction-point algorithm ----
  // Cut the sending rate and re-enter fast recovery (a CNP arrived).
  void cnpRateUpdate();
  // Note the CNP for the next alpha update.
  void cnpAlphaUpdate();
  // Alpha timer: raise alpha if a CNP arrived this interval, else decay it.
  void timerAlphaUpdate();
  // Rate-increase timer: advance the stage machine.
  void rateIncreaseTimer();
  void fastRecoveryTime();
  void activeIncreaseTime();
  // Non-virtual so the destructor can reuse it.
  void cancelRateTimers();

  // ---- Socket-facing quota accounting ----
  uint32_t sendSizeToTokens(uint32_t bytes) const;
  uint64_t getByteQuotaFromTokens(uint64_t tokens) const;
  double getAvailTokens() const;
  void startRateReduceMonitorTimer();
  void stopRateReduceMonitorTimer();
  // Register TARGET_RATE / CURRENT_RATE tracer snapshots on simCtx_.
  void registerTracerSnapshots();

  folly::not_null<CongestionControlSocket*> socket_;
  folly::not_null<SimContext*> simCtx_;
  std::unique_ptr<TokenBucketRateLimiter> tokenBucketRl_;
  FlexDeviceId srcHost_;
  FlexDeviceId dstHost_;

  DataRate targetRate_;
  DataRate currRate_;
  // NIC speed
  DataRate maxRate_;
  double alpha_{1.0};
  // Next alpha-update tick (timerAlphaUpdate); re-armed on every tick,
  // cancelled by cancelRateTimers().
  Scheduler::EventHandle alphaResumeEvent_;
  // Next reaction-point rate-increase tick (rateIncreaseTimer); re-armed on
  // every tick and on a CNP rate cut, cancelled by cancelRateTimers().
  Scheduler::EventHandle rpTimerEvent_;
  uint32_t numTimeBasedRateIncrEvents_{0};
  RpStageType rpStage_{RpStageType::FAST_RECOVERY};
  bool gotCnpDuringAlphaInterval_{false};

  // Config values cached at construction (avoid repeated thrift field access).
  // rpDceTcpG / 1024.0
  double g_;
  // Time::fromUs(rpDceTcpRtt)
  Time alphaTimerPeriod_;
  // Time::fromUs(rpTimerReset)
  Time rateIncreaseTimerPeriod_;
  // rpMinRate in bps
  DataRate minRate_;
  // rpAiRate in bps
  DataRate rateAddIncrease_;
  // rpThreshold
  uint32_t rpThreshold_;
  // rpClampTgtRate
  bool clampTgtRate_;

  uint32_t encapBytes_;
  Time rateReduceMonitorPeriod_;
  bool rateReduceMonitorTimerRunning_{false};
};

} // namespace packsim
