/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/transport/congestion-control.h"
#include "packsim/transport/oss_tests/dcqcn-test-fixture.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

class DcqcnFrAiTest : public DcqcnTestBase {
 public:
  void SetUp() override {
    makeController(
        /*minRateMbps=*/0.5,
        /*src=*/FlexDeviceId({1, 20}),
        /*dst=*/FlexDeviceId({1, 10}));
  }
};

// Test for CNP->FR->AI->AI
TEST_P(DcqcnFrAiTest, TestCnpFrAi) {
  const DataRate lineRate("200Gbps");
  EXPECT_EQ(cc_->currentRate(), lineRate);
  EXPECT_EQ(cc_->targetRate(), lineRate);
  auto alpha =
      cfg_.rpInitialAlpha().value() / (std::pow(2, (cfg_.rpGd().value() - 1)));
  EXPECT_EQ(cc_->alpha(), alpha);
  auto oldCurrRate = cc_->currentRate();
  auto oldAlpha = cc_->alpha();
  cc_->onCongestionSignal();
  EXPECT_EQ(cc_->alpha(), oldAlpha);
  auto scaledAlpha = alpha / 2.0;
  auto updatedCurrRate = DataRate((1 - scaledAlpha) * oldCurrRate.getBps());
  EXPECT_EQ(cc_->currentRate(), updatedCurrRate);
  EXPECT_EQ(cc_->targetRate(), oldCurrRate);
  EXPECT_EQ(cc_->rpStage(), RpStageType::FAST_RECOVERY); // in FR
  EXPECT_EQ(cc_->numRateIncreaseEvents(), 0); // reset stage counts for time
  simCtx_.schedule(
      Time::fromUs(cfg_.rpTimerReset().value() + 1),
      &DcqcnFrAiTest::checkExpectedTimeStage,
      this,
      (1));
  auto updCurrRate =
      DataRate(updatedCurrRate.getBps() / 2 + cc_->targetRate().getBps() / 2);
  auto updTarRate = oldCurrRate;
  simCtx_.schedule(
      Time::fromUs(cfg_.rpTimerReset().value() + 1),
      &DcqcnFrAiTest::checkExpectedRates,
      this,
      updCurrRate,
      updTarRate); // FR phase
  simCtx_.schedule(
      Time::fromUs(2 * cfg_.rpTimerReset().value() + 1),
      &DcqcnFrAiTest::checkExpectedTimeStage,
      this,
      (2));
  // AI phase: the target is already the line rate, which caps it.
  updTarRate = lineRate;
  updCurrRate = DataRate(updCurrRate.getBps() / 2 + updTarRate.getBps() / 2);
  simCtx_.schedule(
      Time::fromUs(2 * cfg_.rpTimerReset().value() + 1),
      &DcqcnFrAiTest::checkExpectedRates,
      this,
      updCurrRate,
      updTarRate);
  simCtx_.schedule(
      Time::fromUs(3 * cfg_.rpTimerReset().value() + 1),
      &DcqcnFrAiTest::checkExpectedTimeStage,
      this,
      (3));
  updTarRate = lineRate; // AI phase, still capped
  updCurrRate = DataRate(updCurrRate.getBps() / 2 + updTarRate.getBps() / 2);
  simCtx_.schedule(
      Time::fromUs(3 * cfg_.rpTimerReset().value() + 1),
      &DcqcnFrAiTest::checkExpectedRates,
      this,
      updCurrRate,
      updTarRate);
  scheduler_.run(Time::fromMs(1.0));
}

// A CNP cuts the rate to at least rpMinRate, which must not exceed the line
// rate.
TEST(DcqcnConstructionTest, MinRateAboveTheNicSpeedIsRejected) {
  Scheduler scheduler;
  SimContext simCtx{&scheduler, 0};
  NoopCongestionControlSocket socket;
  packsim::thrift::DcqcnConfig cfg;
  cfg.rpMinRate() = 300'000;

  EXPECT_THAT(
      [&] {
        DcqcnCongestionControl cc(
            socket,
            simCtx,
            cfg,
            FlexDeviceId({1, 20}),
            FlexDeviceId({1, 10}),
            /*nicSpeed=*/DataRate("200Gbps"),
            /*smtu=*/4096,
            /*encapBytes=*/58);
      },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "DcqcnConfig.rpMinRate 300000 Mbps must be at least 0.000001 Mbps "
          "(1 bps) and at most the NIC speed of 200000 Mbps")));
}

// A minimum of 0 would let a run of CNPs cut the rate to 0 bps.
TEST(DcqcnConstructionTest, MinRateOfZeroIsRejected) {
  Scheduler scheduler;
  SimContext simCtx{&scheduler, 0};
  NoopCongestionControlSocket socket;
  packsim::thrift::DcqcnConfig cfg;
  cfg.rpMinRate() = 0;

  EXPECT_THAT(
      [&] {
        DcqcnCongestionControl cc(
            socket,
            simCtx,
            cfg,
            FlexDeviceId({1, 20}),
            FlexDeviceId({1, 10}),
            /*nicSpeed=*/DataRate("200Gbps"),
            /*smtu=*/4096,
            /*encapBytes=*/58);
      },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "DcqcnConfig.rpMinRate 0 Mbps must be at least 0.000001 Mbps")));
}

// Drives a controller with clamped targets (rpClampTgtRate) through two CNPs,
// so the target drops below the line rate, then one fast-recovery tick (which
// moves it to active increase, rpThreshold being 1) and one active-increase
// tick, and records the rates after each tick.
class DcqcnActiveIncreaseTest : public ::testing::Test {
 protected:
  static constexpr uint64_t kMbpsToBps = 1'000'000;

  struct Rates {
    DataRate target;
    DataRate current;
  };

  void runCnpCnpFrAi(const double aiRateMbps) {
    cfg_.dcqcnEnable() = true;
    cfg_.rpClampTgtRate() = true;
    cfg_.rpAiRate() = aiRateMbps;
    cc_ = std::make_unique<DcqcnCongestionControl>(
        socket_,
        simCtx_,
        cfg_,
        FlexDeviceId({1, 20}),
        FlexDeviceId({1, 10}),
        lineRate_,
        /*smtu=*/4096,
        /*encapBytes=*/58);
    // The second CNP comes after rpRateReduceMonitorPeriod (4 us), so it cuts
    // again, and restarts the rate-increase timer from its own time.
    const auto timerPeriodUs = static_cast<uint64_t>(*cfg_.rpTimerReset());
    constexpr uint64_t kSecondCnpUs = 10;
    cc_->onCongestionSignal();
    simCtx_.schedule(
        Time::fromUs(kSecondCnpUs), [this] { cc_->onCongestionSignal(); });
    simCtx_.schedule(Time::fromUs(kSecondCnpUs + timerPeriodUs + 1), [this] {
      afterFr_ = Rates{cc_->targetRate(), cc_->currentRate()};
      stageAfterFr_ = cc_->rpStage();
    });
    simCtx_.schedule(
        Time::fromUs(kSecondCnpUs + 2 * timerPeriodUs + 1),
        [this] { afterAi_ = Rates{cc_->targetRate(), cc_->currentRate()}; });
    scheduler_.run(Time::fromUs(kSecondCnpUs + 2 * timerPeriodUs + 2));
  }

  const DataRate lineRate_{"200Gbps"};
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  NoopCongestionControlSocket socket_;
  packsim::thrift::DcqcnConfig cfg_;
  std::unique_ptr<DcqcnCongestionControl> cc_;
  Rates afterFr_;
  Rates afterAi_;
  RpStageType stageAfterFr_{RpStageType::FAST_RECOVERY};
};

TEST_F(DcqcnActiveIncreaseTest, ActiveIncreaseRaisesTheTargetByTheAiRate) {
  constexpr double kAiRateMbps = 1000;

  runCnpCnpFrAi(kAiRateMbps);

  ASSERT_EQ(stageAfterFr_, RpStageType::AI) << "precondition: FR moved to AI";
  ASSERT_LT(
      afterFr_.target.getBps() +
          static_cast<uint64_t>(kAiRateMbps) * kMbpsToBps,
      lineRate_.getBps())
      << "precondition: the increase stays below the line rate";
  const DataRate expectedTarget(
      afterFr_.target.getBps() +
      static_cast<uint64_t>(kAiRateMbps) * kMbpsToBps);
  EXPECT_EQ(afterAi_.target, expectedTarget);
  EXPECT_EQ(
      afterAi_.current,
      DataRate(afterFr_.current.getBps() / 2 + expectedTarget.getBps() / 2));
}

TEST_F(DcqcnActiveIncreaseTest, ActiveIncreaseStopsTheTargetAtTheLineRate) {
  constexpr double kAiRateMbps = 150'000;

  runCnpCnpFrAi(kAiRateMbps);

  ASSERT_EQ(stageAfterFr_, RpStageType::AI) << "precondition: FR moved to AI";
  ASSERT_GT(
      afterFr_.target.getBps() +
          static_cast<uint64_t>(kAiRateMbps) * kMbpsToBps,
      lineRate_.getBps())
      << "precondition: the increase would pass the line rate";
  EXPECT_EQ(afterAi_.target, lineRate_);
  EXPECT_EQ(
      afterAi_.current,
      DataRate(afterFr_.current.getBps() / 2 + lineRate_.getBps() / 2));
}

INSTANTIATE_TEST_SUITE_P(
    DcqcnFrAiTestSweep,
    DcqcnFrAiTest,
    ::testing::Values(80));

} // namespace packsim
