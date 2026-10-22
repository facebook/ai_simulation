/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <optional>
#include <string>

#include <folly/portability/GFlags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/link-layer/link-layer-scheduler.h"
#include "packsim/link-layer/oss_tests/lambda-port-observer.h"

namespace packsim::wire {

/**
 * Two `EthernetPort`s joined by a zero-delay channel on one PDES
 * context, shared by the link-layer scheduler suites.
 */
class LinkLayerSchedulerTest : public ::testing::Test {
 protected:
  // VC1 lossless, DSCP CS3 -> VC1, 64-byte credits. The CF/CC timers are left
  // at their thrift defaults, which at 100 Gbps are CF_min = 800 B = 64 ns,
  // CF_max = 16384 B = 1310.72 ns and CC_msg = 100 us. Only CF_min is shorter
  // than the 400 to 600 ns horizons the CF_Update tests run to, so it is
  // the only default that can shift a measured result.
  static packsim::thrift::CbfcConfig makeTestCbfcConfig() {
    packsim::thrift::CbfcConfig cfg;
    cfg.numVcs() = 2;
    cfg.isLosslessPerVc() = {false, true};
    packsim::thrift::VcMapEntry entry;
    entry.selector() = packsim::thrift::VcMapSelector::DSCP;
    entry.mask() = 0x3F;
    entry.value() = 0x18; // DSCP CS3 -> VC1
    entry.vcId() = 1;
    cfg.vcMap() = {entry};
    cfg.expectedCreditSizeBytes() = 64;
    return cfg;
  }

  void SetUp() override {
    // Pin to 1 PDES thread for deterministic dispatch ordering. The
    // multi-threaded default is fine for simulations but breaks any test
    // that observes ordering of events at the same sim time.
    gflags::SetCommandLineOption("pdes_threads", "1");
    schedulerOpt_.emplace();
    schedulerOpt_->buildNewEngine();
    ctxOpt_.emplace(&schedulerOpt_.value(), 0u);

    channel_ = std::make_unique<packsim::EthernetChannel>(/*delayNs=*/0);
    port1_ = std::make_unique<packsim::EthernetPort>(&ctx(), "100Gbps");
    port1_->setObserver(&observerPort1_);
    port2_ = std::make_unique<packsim::EthernetPort>(&ctx(), "100Gbps");
    port2_->setObserver(&observerPort2_);
    // Handlers are left unset here: `setObserver` above satisfies the
    // `deliverToMacClient` check, and `LambdaPortObserver` no-ops any
    // handler a test does not assign. Individual tests bind the one they
    // need.
    port1_->attach(channel_.get());
    port2_->attach(channel_.get());
  }

  void TearDown() override {
    port1_.reset();
    port2_.reset();
    channel_.reset();
    ctxOpt_.reset();
    schedulerOpt_.reset();
  }

  Scheduler& scheduler() {
    return schedulerOpt_.value();
  }

  SimContext& ctx() {
    return ctxOpt_.value();
  }

  std::unique_ptr<LinkLayerScheduler> makeDriver() {
    return std::make_unique<LinkLayerScheduler>(
        &ctx(),
        channel_.get(),
        port1_.get(),
        &port1_->txArbiter(),
        packsim::DataRate("100Gbps"));
  }

  // Matching on the offending field's name keeps each rejection test pinned to
  // the specific check it covers rather than to "something threw".
  void expectRejected(
      const packsim::thrift::CbfcConfig& cfg,
      const std::string& field) {
    EXPECT_THAT(
        [&] { port1_->enableCbfc(cfg); },
        ::testing::ThrowsMessage<std::invalid_argument>(
            ::testing::HasSubstr(field)));
  }

  folly::gflags::FlagSaver flagSaver_;
  std::optional<Scheduler> schedulerOpt_;
  std::optional<SimContext> ctxOpt_;
  std::unique_ptr<packsim::EthernetChannel> channel_;
  LambdaPortObserver observerPort1_;
  std::unique_ptr<packsim::EthernetPort> port1_;
  LambdaPortObserver observerPort2_;
  std::unique_ptr<packsim::EthernetPort> port2_;
};

} // namespace packsim::wire
