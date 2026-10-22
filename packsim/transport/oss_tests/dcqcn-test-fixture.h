/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>

#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/transport/congestion-control.h"

namespace packsim {

// Inert socket: the DCQCN reaction-point suites drive the rate machine
// directly and never exercise the CNP-emission or send-wakeup paths.
class NoopCongestionControlSocket final : public CongestionControlSocket {
 public:
  void emitCongestionNotification() override {}
  void wakeupForSend() override {}
};

/**
 * Shared scaffolding for the DCQCN reaction-point suites (dcqcn-cnp-test,
 * dcqcn-fr-test): a simulation context, an inert socket, and the controller
 * under test. Subclasses call makeController() from SetUp() with the endpoints
 * and minimum rate their scenario needs.
 */
class DcqcnTestBase : public ::testing::TestWithParam<double> {
 public:
  void checkExpectedTimeStage(uint32_t timeStage) {
    EXPECT_EQ(timeStage, cc_->numRateIncreaseEvents());
  }

  void checkExpectedRates(
      const DataRate& expCurrRate,
      const DataRate& expTarRate) {
    EXPECT_EQ(expCurrRate, cc_->currentRate());
    EXPECT_EQ(expCurrRate, cc_->rateLimit());
    EXPECT_EQ(expTarRate, cc_->targetRate());
  }

 protected:
  // Minimal DCQCN config; every other field keeps its thrift default.
  static packsim::thrift::DcqcnConfig buildConfig(const double minRateMbps) {
    packsim::thrift::DcqcnConfig cfg;
    cfg.dcqcnEnable() = true;
    cfg.rpRateReduceMonitorPeriod() = 2000;
    cfg.rpMinRate() = minRateMbps;
    return cfg;
  }

  void makeController(
      const double minRateMbps,
      const FlexDeviceId& src,
      const FlexDeviceId& dst) {
    cfg_ = buildConfig(minRateMbps);
    cc_ = std::make_unique<DcqcnCongestionControl>(
        socket_,
        simCtx_,
        cfg_,
        src,
        dst,
        /*nicSpeed=*/DataRate("200Gbps"),
        /*smtu=*/kSmtuBytes,
        /*encapBytes=*/kEncapBytes);
    dceG_ = cfg_.rpDceTcpG().value();
    rpInitialAlpha_ = cfg_.rpInitialAlpha().value();
    rpGd_ = cfg_.rpGd().value();
  }

  static constexpr uint32_t kSmtuBytes = 4096;
  static constexpr uint32_t kEncapBytes = 58;

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  // Declared before cc_ so it outlives the controller holding a reference.
  NoopCongestionControlSocket socket_;
  packsim::thrift::DcqcnConfig cfg_;
  std::unique_ptr<DcqcnCongestionControl> cc_;
  double dceG_{};
  double rpGd_{};
  uint32_t rpInitialAlpha_{};
};

} // namespace packsim
