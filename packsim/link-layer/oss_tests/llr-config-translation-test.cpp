/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/eth-port.h"

#include <cstdint>
#include <memory>
#include <stdexcept>

#include <folly/portability/GFlags.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/llr/llr-config-translation.h"
#include "packsim/link-layer/llr/llr-types.h"

namespace packsim {
namespace {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// Mirrors the `PinPdesThreads` helper in `eth-port-test`: caps the
// PDES worker pool before the fixture's `Scheduler` field is constructed,
// so sanitizer runs don't spawn one worker per host core.
struct PinPdesThreads {
  PinPdesThreads() {
    gflags::SetCommandLineOption("pdes_threads", "4");
  }
};

// Validation and sentinel translation performed by
// `llr::LlrConfigTranslator`, the single point where the caller-facing
// `thrift::LlrConfig` becomes the `llr::LlrConfig` the LLR module runs on.
// The translator is pure, so this fixture needs no simulation state.
class LlrConfigTranslationTest : public ::testing::Test {
 protected:
  // Representative port rate. Deliberately not the `LlrConfig` default of
  // 800 Gbps, so `LinkBpsTracksTheSuppliedRateNotTheLlrDefault` can tell the
  // supplied value from the default.
  static constexpr uint64_t kLinkBps = 100'000'000'000ULL;

  // Thrift defaults for every field the test under scrutiny does not set.
  static thrift::LlrConfig baseConfig() {
    return thrift::LlrConfig{};
  }

  static llr::LlrConfig translate(const thrift::LlrConfig& config) {
    return llr::LlrConfigTranslator::translate(
        config, kLinkBps, /*enableGlobalTimer=*/false);
  }
};

// The port's own rejection path: `enableLlr` must not leave a port
// half-enabled when the translator throws. Needs a real port, and so a
// scheduler and a simulation context.
class LlrEnablementTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scheduler_.buildNewEngine();
    port_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
  }

  folly::gflags::FlagSaver flagSaver_;
  PinPdesThreads pinPdesThreads_;
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<EthernetPort> port_;
};

TEST_F(LlrConfigTranslationTest, ReplayCtMaxZeroMapsToNoLimitSentinel) {
  // Arrange: the thrift default, meaning "never FLUSH on replay count".
  auto config = baseConfig();
  config.replayCtMax() = 0;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert: without the translation this would mean "FLUSH on first replay".
  EXPECT_EQ(cfg.replayCtMax, llr::kReplayCtMaxNoLimit);
}

TEST_F(LlrConfigTranslationTest, ReplayCtMaxNegativeMapsToNoLimitSentinel) {
  // Arrange
  auto config = baseConfig();
  config.replayCtMax() = -7;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert
  EXPECT_EQ(cfg.replayCtMax, llr::kReplayCtMaxNoLimit);
}

TEST_F(LlrConfigTranslationTest, ReplayCtMaxJustBelowSentinelIsHonored) {
  // Arrange: the largest value that is still a real replay limit.
  auto config = baseConfig();
  config.replayCtMax() = llr::kReplayCtMaxNoLimit - 1;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert: honored verbatim, not folded into the "no limit" sentinel.
  EXPECT_EQ(cfg.replayCtMax, llr::kReplayCtMaxNoLimit - 1);
}

TEST_F(LlrConfigTranslationTest, ReplayCtMaxEqualToSentinelThrows) {
  // Arrange
  thrift::LlrConfig config;
  config.replayCtMax() = llr::kReplayCtMaxNoLimit;

  // Act + Assert: 255 is the sentinel, so it cannot also be a real limit.
  EXPECT_THAT(
      [&] { translate(config); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("collides with the 'no maximum' sentinel")));
}

TEST_F(LlrConfigTranslationTest, ReplayCtMaxAboveSentinelThrows) {
  // Arrange: a value that a plain `static_cast<uint8_t>` would wrap.
  auto config = baseConfig();
  config.replayCtMax() = 300;

  // Act + Assert
  EXPECT_THAT(
      [&] { translate(config); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("replayCtMax (300)")));
}

TEST_F(LlrConfigTranslationTest, AckByteThresholdZeroMapsToSpecMinimum) {
  // Arrange: 0 would make the ACK loop period a zero-length interval.
  auto config = baseConfig();
  config.ackByteThreshold() = 0;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert
  EXPECT_EQ(cfg.ctlosTargetSpacingBytes, llr::kCtlosTargetSpacingMinBytes);
}

TEST_F(LlrConfigTranslationTest, AckByteThresholdBelowSpecMinimumIsHonored) {
  // Arrange: a positive value under the UEC minimum spacing.
  auto config = baseConfig();
  config.ackByteThreshold() = 1;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert: only 0 is replaced by the spec minimum and only the top end is
  // rejected; a small positive value is passed through as-is.
  EXPECT_EQ(cfg.ctlosTargetSpacingBytes, 1u);
  EXPECT_LT(cfg.ctlosTargetSpacingBytes, llr::kCtlosTargetSpacingMinBytes);
}

TEST_F(LlrConfigTranslationTest, AckByteThresholdAtSpecMaximumIsHonored) {
  // Arrange: the largest accepted value (UEC ctlos_target_spacing max).
  auto config = baseConfig();
  config.ackByteThreshold() = llr::kCtlosTargetSpacingMaxBytes;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert
  EXPECT_EQ(cfg.ctlosTargetSpacingBytes, llr::kCtlosTargetSpacingMaxBytes);
}

TEST_F(LlrConfigTranslationTest, AckByteThresholdAboveSpecMaximumThrows) {
  // Arrange: one byte past the ceiling.
  auto config = baseConfig();
  config.ackByteThreshold() = llr::kCtlosTargetSpacingMaxBytes + 1;

  // Act + Assert
  EXPECT_THAT(
      [&] { translate(config); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("exceeds the UEC spec maximum")));
}

TEST_F(LlrConfigTranslationTest, TimersAtZeroMapToEffectivelyDisabled) {
  // Arrange: thrift 0 means "disabled", but `Time{}` means "fires now".
  auto config = baseConfig();
  config.dataAgeTimerMaxNs() = 0;
  config.pcsLostStatusTimerMaxNs() = 0;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert
  EXPECT_EQ(cfg.dataAgeTimerMax, llr::kEffectivelyDisabledLlrTimer);
  EXPECT_EQ(cfg.pcsLostStatusTimerMax, llr::kEffectivelyDisabledLlrTimer);
}

TEST_F(LlrConfigTranslationTest, PositiveTimersArePassedThroughAsNanoseconds) {
  // Arrange
  auto config = baseConfig();
  config.dataAgeTimerMaxNs() = 1234;
  config.pcsLostStatusTimerMaxNs() = 5678;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert
  EXPECT_EQ(cfg.dataAgeTimerMax, Time::fromNs(1234));
  EXPECT_EQ(cfg.pcsLostStatusTimerMax, Time::fromNs(5678));
}

TEST_F(LlrConfigTranslationTest, LinkBpsTracksTheSuppliedRateNotTheLlrDefault) {
  // Arrange: the caller supplies 100Gbps, not the `LlrConfig` default of
  // 800Gbps that would shorten the derived ACK period.
  const auto config = baseConfig();

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert
  EXPECT_EQ(cfg.linkBps, kLinkBps);
  EXPECT_NE(cfg.linkBps, llr::kLlrDefaultLinkBps800Gbps);
}

TEST_F(
    LlrConfigTranslationTest,
    OutstandingSeqMaxIsIndependentOfTxBufferBytes) {
  // Arrange: a byte budget far below the spec seq ceiling, which must not
  // be reinterpreted as a sequence-number budget.
  auto config = baseConfig();
  config.txBufferMaxBytes() = 4096;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert: the byte fields follow the input, the seq ceiling does not.
  EXPECT_EQ(cfg.txBufferMaxBytes, 4096u);
  EXPECT_EQ(cfg.outstandingDataMax, 4096u);
  EXPECT_EQ(cfg.outstandingSeqMax, llr::LlrSeq::kSeqHalf);
}

TEST_F(
    LlrConfigTranslationTest,
    ReInitOnDiscardDefaultMatchesLlrConfigDefault) {
  // Arrange: `reInitOnDiscard` left unset.
  const auto config = baseConfig();

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert: an unset field recovers from FLUSH through INIT, as a
  // default-constructed `llr::LlrConfig` does.
  EXPECT_TRUE(cfg.reInitOnDiscard);
  EXPECT_EQ(cfg.reInitOnDiscard, llr::LlrConfig{}.reInitOnDiscard);
}

TEST_F(LlrConfigTranslationTest, ReInitOnDiscardFalseIsPassedThrough) {
  // Arrange
  auto config = baseConfig();
  config.reInitOnDiscard() = false;

  // Act
  const llr::LlrConfig cfg = translate(config);

  // Assert
  EXPECT_FALSE(cfg.reInitOnDiscard);
}

TEST_F(LlrEnablementTest, EnableLlrRejectsSentinelAndLeavesLlrOff) {
  // Arrange
  thrift::LlrConfig config;
  config.replayCtMax() = llr::kReplayCtMaxNoLimit;

  // Act + Assert: validation runs before the module is constructed, so the
  // port must not end up half-enabled. The message contract itself is pinned
  // by `ReplayCtMaxEqualToSentinelThrows`.
  EXPECT_THROW(
      port_->enableLlr(config, /*enableGlobalTimer=*/false),
      std::invalid_argument);
  EXPECT_FALSE(port_->isLlrEnabled());
}

} // namespace
} // namespace packsim
