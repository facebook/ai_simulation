/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/transport/congestion-control.h"

#include <cstddef>
#include <cstdint>
#include <memory>

#include <gtest/gtest.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"

namespace packsim {
namespace {

// Minimal CongestionControl that implements only the pure-virtual send-gating
// surface and leaves every feedback/lifecycle hook at its base default. Used to
// pin the defaulted behavior of the CongestionControl ABC.
class TestCongestionControl final : public CongestionControl {
 public:
  size_t adjustQuota(size_t requestedQuota) const override {
    return requestedQuota;
  }
  void onBytesSent(uint32_t) override {}
};

// Records the CNP emissions the DcqcnCongestionControl drives through its
// owning socket (from onPacketReceived). wakeupForSend is an inert override —
// the token replenishment -> wakeup path is covered by dcqcn-timer-cancel-test.
class TestCongestionControlSocket final : public CongestionControlSocket {
 public:
  void emitCongestionNotification() override {
    ++cnpCount_;
  }
  void wakeupForSend() override {}
  int cnpCount() const noexcept {
    return cnpCount_;
  }

 private:
  int cnpCount_{0};
};

// Unit tests for DcqcnCongestionControl. The dcqcn-*-test suite covers the
// reaction-point rate machine (alpha decay, fast-recovery/active-increase
// stages); these tests cover the socket-facing surface: quota accounting,
// token consumption, and CNP-driven rate reduction through its public API.
class DcqcnCongestionControlTest : public ::testing::Test {
 protected:
  // Minimal DCQCN config; all other fields keep their thrift defaults (mirrors
  // the config used by dcqcn-cnp-test).
  static packsim::thrift::DcqcnConfig buildConfig() {
    packsim::thrift::DcqcnConfig cfg;
    cfg.dcqcnEnable() = true;
    cfg.rpRateReduceMonitorPeriod() = 2000;
    cfg.rpMinRate() = 1.0;
    return cfg;
  }

  void SetUp() override {
    controller_ = std::make_unique<DcqcnCongestionControl>(
        socket_,
        simCtx_,
        buildConfig(),
        /*src=*/FlexDeviceId({1, 10}),
        /*dst=*/FlexDeviceId({1, 20}),
        /*nicSpeed=*/DataRate("200Gbps"),
        /*smtu=*/kSmtuBytes,
        /*encapBytes=*/kEncapBytes);
  }

  static constexpr uint32_t kSmtuBytes = 4096;
  static constexpr uint32_t kEncapBytes = 58;

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  // Declared before controller_ so it outlives the DcqcnCongestionControl that
  // holds a reference to it.
  TestCongestionControlSocket socket_;
  std::unique_ptr<DcqcnCongestionControl> controller_;
};

TEST_F(DcqcnCongestionControlTest, StartsUnthrottledAtNicSpeed) {
  // DCQCN opens at line rate: current and target both start at the NIC speed,
  // in fast recovery, with no rate-increase ticks yet.
  EXPECT_EQ(controller_->currentRate(), DataRate("200Gbps"));
  EXPECT_EQ(controller_->targetRate(), DataRate("200Gbps"));
  EXPECT_EQ(controller_->rpStage(), RpStageType::FAST_RECOVERY);
  EXPECT_EQ(controller_->numRateIncreaseEvents(), 0u);
  controller_->cancelTimers();
}

TEST_F(DcqcnCongestionControlTest, AdjustQuotaReturnsZeroForZeroRequest) {
  // min(0, availableTokens) is always 0, regardless of token state.
  EXPECT_EQ(controller_->adjustQuota(0), 0u);
}

TEST_F(
    DcqcnCongestionControlTest,
    AdjustQuotaClampsToRequestWhenTokensAvailable) {
  // The token bucket is rate-gated and grants nothing at t=0, so we advance
  // simulation time (via a no-op event that pins the clock forward) to let it
  // accrue tokens before checking the clamp.
  simCtx_.schedule(Time::fromUs(100), []() {});
  scheduler_.run(Time::fromUs(200));

  constexpr size_t kSmallRequest = 128;
  const size_t quota = controller_->adjustQuota(kSmallRequest);
  // With tokens available the request is granted in full: the result is
  // non-zero (distinguishing correct clamping from an always-0 bug) and never
  // exceeds the request.
  EXPECT_GT(quota, 0u);
  EXPECT_LE(quota, kSmallRequest);
}

TEST_F(DcqcnCongestionControlTest, OnCongestionSignalReducesCurrentRate) {
  const auto beforeRate = controller_->currentRate().getBps();
  controller_->onCongestionSignal();
  const auto afterRate = controller_->currentRate().getBps();
  EXPECT_LT(afterRate, beforeRate);
  controller_->cancelTimers();
}

TEST_F(DcqcnCongestionControlTest, OnPacketReceivedEmitsCnpOnlyForEcnCe) {
  // A non-ECN packet draws no feedback; an ECN-CE packet makes DCQCN emit one
  // CNP toward the sender via its socket.
  controller_->onPacketReceived(/*ecnCe=*/false);
  EXPECT_EQ(socket_.cnpCount(), 0);

  controller_->onPacketReceived(/*ecnCe=*/true);
  EXPECT_EQ(socket_.cnpCount(), 1);
}

TEST_F(DcqcnCongestionControlTest, CancelTimersStopsTimerDrivenRateChanges) {
  // onCongestionSignal reduces the rate and arms the DCQCN timers (including
  // the time-based rate-increase timer). After cancelTimers, no timer remains
  // to move the rate, so draining the scheduler well past any timer period must
  // leave the rate unchanged. (Without the cancel, the rate-increase timer
  // would fire during the run and raise the rate.)
  controller_->onCongestionSignal();
  const auto rateAfterCnp = controller_->currentRate().getBps();
  controller_->cancelTimers();
  scheduler_.run(Time::fromMs(5.0));
  EXPECT_EQ(controller_->currentRate().getBps(), rateAfterCnp);
}

// The CongestionControl ABC ships defaulted feedback/lifecycle hooks so a
// strategy overrides only the signals it uses. A subclass that overrides only
// the send-gating surface must inherit inert no-op defaults for the rest.
TEST(CongestionControlAbcTest, DefaultHooksAreInertNoOps) {
  TestCongestionControl cc;
  CongestionControl& base = cc;

  EXPECT_FALSE(base.isSendBlocked());
  base.onCongestionSignal();
  base.onPacketReceived(/*ecnCe=*/true);
  base.onPacketReceived(/*ecnCe=*/false);
  base.cancelTimers();

  // Send gating routes through the concrete override.
  EXPECT_EQ(base.adjustQuota(1234u), 1234u);
  base.onBytesSent(1234u);
}

} // namespace
} // namespace packsim
