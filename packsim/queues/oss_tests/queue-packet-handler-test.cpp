/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/queues/queue-packet-handler.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {
namespace {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;
using QueueProfileType = packsim::thrift::QueueProfileType;

// Tests for buildHandler (the queue-packet-handler factory) and the trivial
// DoNothingPacketHandler. The WRED/ECN/VoqECN handlers' packet-handling logic
// lives in wred-ecn-packet-handler-test.cpp.
class QueuePacketHandlerFactoryTest : public ::testing::Test {
 protected:
  // A fully-populated config; buildHandler only reads queueProfileCfg() but the
  // base handler constructor dereferences the WRED/ECN threshold fields.
  static packsim::thrift::WredEcnConfig buildConfig(QueueProfileType type) {
    packsim::thrift::WredEcnConfig cfg;
    cfg.queueProfileCfg() = type;
    cfg.exp() = 1;
    cfg.averagingPeriod() = 1000;
    cfg.minQSizeWRED() = 10;
    cfg.maxQSizeWRED() = 100;
    cfg.dropRateWRED() = 50;
    cfg.minQSizeECN() = 10;
    cfg.maxQSizeECN() = 100;
    cfg.markRateECN() = 50;
    return cfg;
  }

  std::unique_ptr<QueuePacketHandler> build(QueueProfileType type) {
    return buildHandler(
        &simCtx_, /*id=*/0, buildConfig(type), /*queues=*/nullptr);
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
};

TEST_F(QueuePacketHandlerFactoryTest, BuildsDoNothingForNoneProfile) {
  EXPECT_EQ(
      build(QueueProfileType::NONE)->getQueuePacketHandlerType(),
      QueuePacketHandlerType::DONOTHING);
}

TEST_F(QueuePacketHandlerFactoryTest, BuildsWredForWredProfile) {
  EXPECT_EQ(
      build(QueueProfileType::WRED)->getQueuePacketHandlerType(),
      QueuePacketHandlerType::WRED);
}

TEST_F(QueuePacketHandlerFactoryTest, BuildsEcnForEcnProfile) {
  EXPECT_EQ(
      build(QueueProfileType::ECN)->getQueuePacketHandlerType(),
      QueuePacketHandlerType::ECN);
}

TEST_F(QueuePacketHandlerFactoryTest, BuildsVoqEcnForVoqEcnProfile) {
  EXPECT_EQ(
      build(QueueProfileType::VOQ_ECN)->getQueuePacketHandlerType(),
      QueuePacketHandlerType::VOQ_ECN);
}

TEST_F(QueuePacketHandlerFactoryTest, BuildsWredEcnForWredEcnProfile) {
  EXPECT_EQ(
      build(QueueProfileType::WRED_ECN)->getQueuePacketHandlerType(),
      QueuePacketHandlerType::WRED_ECN);
}

TEST_F(QueuePacketHandlerFactoryTest, ThrowsForUnknownProfile) {
  EXPECT_THAT(
      [this]() { build(QueueProfileType{99}); },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Unknown queue packet handler type")));
}

TEST_F(QueuePacketHandlerFactoryTest, RejectsNegativeExp) {
  auto cfg = buildConfig(QueueProfileType::WRED);
  cfg.exp() = -1.0;
  EXPECT_THAT(
      [&]() { buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("WredEcnConfig.exp")));
}

TEST_F(QueuePacketHandlerFactoryTest, RejectsNanExp) {
  auto cfg = buildConfig(QueueProfileType::ECN);
  cfg.exp() = std::numeric_limits<double>::quiet_NaN();
  EXPECT_THAT(
      [&]() { buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("WredEcnConfig.exp")));
}

TEST_F(QueuePacketHandlerFactoryTest, RejectsNegativeAveragingPeriod) {
  auto cfg = buildConfig(QueueProfileType::WRED_ECN);
  cfg.averagingPeriod() = -1.0;
  EXPECT_THAT(
      [&]() { buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("WredEcnConfig.averagingPeriod")));
}

TEST_F(QueuePacketHandlerFactoryTest, RejectsInfiniteAveragingPeriod) {
  // Every profile builds the averaging tracker, including NONE.
  auto cfg = buildConfig(QueueProfileType::NONE);
  cfg.averagingPeriod() = std::numeric_limits<double>::infinity();
  EXPECT_THAT(
      [&]() { buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("WredEcnConfig.averagingPeriod")));
}

TEST_F(QueuePacketHandlerFactoryTest, RejectsAveragingPeriodTooLargeForUint64) {
  auto cfg = buildConfig(QueueProfileType::WRED_ECN);
  // 2^64: finite, but past the largest value the period's uint64_t holds.
  cfg.averagingPeriod() = 18446744073709551616.0;
  EXPECT_THAT(
      [&]() { buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "WredEcnConfig.averagingPeriod must be finite, >= 0 and "
          "below 2^64")));
}

TEST_F(QueuePacketHandlerFactoryTest, RejectsDropRateAboveOneHundredPercent) {
  auto cfg = buildConfig(QueueProfileType::WRED);
  cfg.dropRateWRED() = 101;
  EXPECT_THAT(
      [&]() { buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "WredEcnConfig.dropRateWRED must be a percentage from 0 to 100, "
          "got 101")));
}

TEST_F(QueuePacketHandlerFactoryTest, RejectsMarkRateAboveOneHundredPercent) {
  auto cfg = buildConfig(QueueProfileType::ECN);
  cfg.markRateECN() = 101;
  EXPECT_THAT(
      [&]() { buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "WredEcnConfig.markRateECN must be a percentage from 0 to 100, "
          "got 101")));
}

TEST_F(
    QueuePacketHandlerFactoryTest,
    RejectsRateAboveOneHundredPercentUnderNoneProfile) {
  // Every profile checks both rates, including NONE, which reads neither.
  auto cfg = buildConfig(QueueProfileType::NONE);
  cfg.markRateECN() = 101;
  EXPECT_THAT(
      [&]() { buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "WredEcnConfig.markRateECN must be a percentage from 0 to 100, "
          "got 101")));
}

TEST_F(QueuePacketHandlerFactoryTest, AcceptsFractionalExpAndZeroPeriod) {
  auto cfg = buildConfig(QueueProfileType::WRED);
  cfg.exp() = 0.5;
  cfg.averagingPeriod() = 0;
  EXPECT_NO_THROW(buildHandler(&simCtx_, /*id=*/0, cfg, /*queues=*/nullptr));
}

TEST_F(QueuePacketHandlerFactoryTest, DoNothingHandlerAdmitsAndDoesNotMark) {
  auto handler = build(QueueProfileType::NONE);
  // DoNothing ignores the packet: it always admits on filtering and never marks
  // on scheduling.
  PacketPtr pkt;
  EXPECT_TRUE(handler->handlePacketAfterFiltering(pkt));
  EXPECT_FALSE(handler->handlePacketAfterScheduling(pkt));
}

} // namespace
} // namespace packsim
