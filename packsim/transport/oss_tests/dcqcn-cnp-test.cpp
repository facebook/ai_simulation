/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <cstdint>

#include <folly/logging/xlog.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/time.h"
#include "packsim/transport/oss_tests/dcqcn-test-fixture.h"

namespace packsim {

class DcqcnCnpTest : public DcqcnTestBase {
 public:
  void SetUp() override {
    makeController(
        /*minRateMbps=*/1.0,
        /*src=*/FlexDeviceId({1, 10}),
        /*dst=*/FlexDeviceId({1, 20}));
  }

  // Deliver one CNP and assert the reaction point cut the rate, clamped the
  // target to the pre-CNP rate, and re-entered fast recovery.
  void sendCnpAndCheck() {
    const auto oldCurrRate = cc_->currentRate();
    const auto oldAlpha = cc_->alpha();
    XLOGF(
        DBG,
        "At time={}, CNP send from test, old curr rate={}, alpha={}, g={}",
        simCtx_.nowNs(),
        oldCurrRate.getBps(),
        oldAlpha,
        (dceG_ / 1024.0));
    cc_->onCongestionSignal();
    EXPECT_EQ(cc_->alpha(), oldAlpha);
    const auto scaledAlpha = oldAlpha / 2.0;
    const auto updCurrRateFr =
        DataRate((1 - scaledAlpha) * oldCurrRate.getBps());
    EXPECT_EQ(cc_->currentRate(), updCurrRateFr);
    EXPECT_EQ(cc_->targetRate(), oldCurrRate);
    EXPECT_EQ(cc_->rpStage(), RpStageType::FAST_RECOVERY); // in FR
    EXPECT_EQ(cc_->numRateIncreaseEvents(), 0); // reset stage counts for time
  }
};

TEST_P(DcqcnCnpTest, TestDefaults) {
  EXPECT_EQ(cfg_.dcqcnEnable().value(), true);
  EXPECT_EQ(cfg_.rpDceTcpRtt().value(), 1);
  EXPECT_EQ(cfg_.rpDceTcpG().value(), 1019);
  EXPECT_EQ(cfg_.rpInitialAlpha().value(), 1023);
}

TEST_P(DcqcnCnpTest, TestFrToCnp) {
  EXPECT_EQ(cc_->currentRate(), DataRate("200Gbps"));
  EXPECT_EQ(cc_->targetRate(), DataRate("200Gbps"));
  XLOGF(DBG, "At time={}, check alpha", simCtx_.nowNs(), cc_->alpha());
  auto alphaFrac =
      cfg_.rpInitialAlpha().value() / std::pow(2, (cfg_.rpGd().value() - 1));
  EXPECT_EQ(cc_->alpha(), alphaFrac);
  EXPECT_EQ(cc_->rpStage(), RpStageType::FAST_RECOVERY); // in FR
  checkExpectedTimeStage(0);
  simCtx_.schedule(
      Time::fromUs(cfg_.rpTimerReset().value() + 1),
      &DcqcnCnpTest::checkExpectedTimeStage,
      this,
      (1));

  auto updTarRate = cc_->currentRate().getBps();
  auto updCurrRate = DataRate(
      cc_->currentRate().getBps() / 2 + cc_->targetRate().getBps() / 2);
  simCtx_.schedule(
      Time::fromUs(cfg_.rpTimerReset().value() + 1),
      &DcqcnCnpTest::checkExpectedRates,
      this,
      updCurrRate,
      DataRate(updTarRate)); // FR phase

  // FR->FR due to CNP
  simCtx_.schedule(
      Time::fromUs(cfg_.rpTimerReset().value() + 5),
      &DcqcnCnpTest::sendCnpAndCheck,
      this);
  scheduler_.run(Time::fromMs(1.0));
}

INSTANTIATE_TEST_SUITE_P(
    DcqcnCnpTestSweep,
    DcqcnCnpTest,
    ::testing::Values(80));

} // namespace packsim
