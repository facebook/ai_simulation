/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <source_location>
#include <tuple>
#include <utility>
#include <vector>

#include <folly/container/F14Map.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/kernel/ceil-divide.h"
#include "packsim/packet/pfc.h"
#include "packsim/switch/buffer-tracker.h"
#include "packsim/switch/simple-switch-buffer.h"

using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

namespace {
// A shared-cell limit that no test reaches.
constexpr uint32_t kUnlimitedCells = INT32_MAX;
} // namespace

TEST(SwitchBufferMathTests, CeilDivide) {
  for (uint64_t i = 1; i < 20; i++) {
    for (uint64_t j = 1; j < 20; j++) {
      auto b = ceilDivide32(i, j);
      auto correct = std::ceil(i / static_cast<double>(j));
      EXPECT_EQ(b, correct);
    }
  }
}

class DynShareTests
    : public ::testing::TestWithParam<
          std::tuple<uint16_t, uint8_t, bool, uint16_t, uint8_t>> {
 public:
  void SetUp() override {
    lossless_ = std::get<2>(GetParam());
    bufCfg_ = buildConfig(lossless_);
    buf_ = std::make_unique<SimpleSwitchBuffer>(
        &simCtx_, bufCfg_, /*useIngress=*/true);
    egrBuf_ = std::make_unique<SimpleSwitchBuffer>(
        &simCtx_, bufCfg_, /*useIngress=*/false);
    buf_->setOnPfcStateChangedCallback(
        [this](uint16_t port, uint8_t pg, PfcState state) {
          lastPfcState_ = std::make_tuple(port, pg, state);
        });
    port_ = std::get<0>(GetParam());
    pg_ = std::get<1>(GetParam());
    lastPfcState_ = std::make_tuple(port_, pg_, PfcState::XON);

    egrPort_ = std::get<3>(GetParam());
    egrQueue_ = std::get<4>(GetParam());

    cellSize_ = kCellSizeBytes;

    bufStats_ = &buf_->bufferStats();
    portStats_ = &buf_->portStats(port_);
    queueStats_ = &buf_->queueStats(port_, pg_);
    portTracker_ = &buf_->portTracker(port_);
    queueTracker_ = &buf_->queueTracker(port_, pg_);

    egrBufStats_ = &egrBuf_->bufferStats();
    egrPortStats_ = &egrBuf_->portStats(egrPort_);
    egrQueueStats_ = &egrBuf_->queueStats(egrPort_, egrQueue_);
    egrPortTracker_ = &egrBuf_->portTracker(egrPort_);
    egrQueueTracker_ = &egrBuf_->queueTracker(egrPort_, egrQueue_);
  }

  // Assert the most recent PFC callback state equals `state` for this
  // port/pg. Takes the caller's source_location and injects it via ScopedTrace
  // so a failure still reports the call site (this was previously a macro,
  // purely to get call-site line reporting).
  void expectPfcState(
      PfcState state,
      std::source_location loc = std::source_location::current()) const {
    const testing::ScopedTrace trace(
        loc.file_name(), static_cast<int>(loc.line()), "expectPfcState");
    EXPECT_EQ(lastPfcState_, std::make_tuple(port_, pg_, state));
  }

 protected:
  static constexpr uint32_t kCellSizeBytes = 254;

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  packsim::thrift::SwitchBufferConfig bufCfg_;
  std::unique_ptr<SimpleSwitchBuffer> buf_;
  std::unique_ptr<SimpleSwitchBuffer> egrBuf_;
  bool lossless_{};
  uint16_t port_{}, egrPort_{};
  uint8_t pg_{}, egrQueue_{};
  uint32_t cellSize_{};
  std::tuple<uint16_t, uint8_t, PfcState> lastPfcState_;
  PortTracker* portTracker_{};
  QueueTracker* queueTracker_{};
  PortTracker* egrPortTracker_{};
  QueueTracker* egrQueueTracker_{};

  SwitchStats* bufStats_{};
  SwitchStats* portStats_{};
  SwitchStats* queueStats_{};

  SwitchStats* egrBufStats_{};
  SwitchStats* egrPortStats_{};
  SwitchStats* egrQueueStats_{};

  static packsim::thrift::SwitchBufferConfig buildConfig(bool isLossless) {
    constexpr uint32_t numPorts = 32;
    constexpr uint32_t bufSizeMiB = 32;
    constexpr uint32_t pgRsvdCells = 3;
    constexpr uint32_t portRsvdCells = 5;
    constexpr uint32_t hdrmPoolCells = 1024;
    constexpr uint32_t portXonHystCells = 7;
    constexpr uint32_t pgXonHystCells = 9;
    constexpr uint32_t numPgs = 8;
    std::vector<double> alpha;
    std::vector<bool> canUsePortRsvd;
    std::vector<uint32_t> maxHdrmCells;
    std::vector<bool> lossless;
    std::vector<bool> pfc;
    std::vector<double> egrAlpha{
        0.25, 0.5, 0.125, 0.25, 0.125, 0.25, 0.125, 0.5};
    std::vector<uint32_t> egrQueueRsvdCells{3, 4, 6, 5, 4, 6, 8, 7};
    std::vector<uint32_t> egrResumeHyst{10, 20, 5, 8, 6, 11, 12, 14};

    if (isLossless) {
      alpha = {0.5, 0.25, 0.125, 0.5, 0.25, 0.5, 0.125, 0.25};
      canUsePortRsvd = {true, false, false, false, false, false, false, false};
      maxHdrmCells = {128, 64, 0, 10, 20, 30, 40, 50};
      lossless = {true, true, false, true, true, true, true, true};
      pfc = {true, false, false, true, true, true, true, true};
    } else {
      alpha = {0.5, 0.75, 1.0, 0.5, 0.75, 0.5, 0.75, 0.5};
      canUsePortRsvd = {false, false, false, false, false, false, false, false};
      maxHdrmCells = {0, 0, 0, 0, 0, 0, 0, 0};
      lossless = {false, false, false, false, false, false, false, false};
      pfc = {false, false, false, false, false, false, false, false};
    }

    packsim::thrift::SwitchBufferConfig sbc;
    sbc.cellSizeBytes() = kCellSizeBytes;
    sbc.totalCells() = bufSizeMiB * 1024 * 1024 / kCellSizeBytes;
    sbc.headroomPoolCells() = hdrmPoolCells;

    for (uint32_t idx = 0; idx < numPorts; idx++) {
      packsim::thrift::PortBufferConfig pbc;
      pbc.port() = idx;
      pbc.portRsvdCells() = portRsvdCells;
      pbc.portMaxSharedCells() = kUnlimitedCells;
      pbc.xonHystCells() = portXonHystCells;

      for (uint32_t pg = 0; pg < numPgs; pg++) {
        packsim::thrift::PriorityGroupBufferConfig pgbc;
        pgbc.priority() = pg;
        pgbc.rsvdCells() = pgRsvdCells;
        // Only priority 0 can use port reserved space
        pgbc.canUsePortRsvd() = canUsePortRsvd.at(pg);
        pgbc.dynamicSharing() = true;
        pgbc.alpha() = alpha.at(pg);
        pgbc.maxSharedCells() = kUnlimitedCells;
        pgbc.xonHystCells() = pgXonHystCells;
        pgbc.maxHdrmCells() = maxHdrmCells.at(pg);
        pgbc.lossless() = lossless.at(pg);
        pgbc.pfc() = pfc.at(pg);
        pbc.pgs()->emplace_back(pgbc);
      }

      sbc.ports()[idx] = pbc;
    }
    for (uint32_t idx = 0; idx < (2 * numPorts); idx++) {
      packsim::thrift::PortBufferConfig pbcEgr;
      pbcEgr.port() = idx;
      pbcEgr.portMaxSharedCells() = kUnlimitedCells;
      for (uint32_t que = 0; que < numPgs; que++) {
        packsim::thrift::EgressQueueBufferConfig egrCfg;
        // egress side
        egrCfg.cos() = que;
        egrCfg.rsvdCells() = egrQueueRsvdCells.at(que);
        egrCfg.dynamicSharing() = true;
        egrCfg.alpha() = egrAlpha.at(que);
        egrCfg.maxSharedCells() = kUnlimitedCells;
        egrCfg.resumeHystCells() = egrResumeHyst.at(que);
        pbcEgr.ucQueues()->emplace_back(egrCfg);
      }

      if (!sbc.ports()->contains(idx)) {
        pbcEgr.port() = idx;
        pbcEgr.isEgrPort() = true;
        pbcEgr.isIngrPort() = false;
        sbc.ports()[idx] = pbcEgr;
      } else {
        auto pbc = sbc.ports()[idx];
        pbc.isEgrPort() = true;
        pbc.ucQueues() = *pbcEgr.ucQueues();
        sbc.ports()[idx] = pbc;
      }
    }

    return sbc;
  }
};

TEST_P(DynShareTests, TestRsvdPortAllocation) {
  if (!lossless_) {
    return;
  }
  const auto& pgCfg = bufCfg_.ports()->at(port_).pgs()->at(pg_);
  auto pgRsvdCells = pgCfg.rsvdCells().value();
  auto portRsvdCells = bufCfg_.ports()->at(port_).portRsvdCells().value();
  auto totalRsvdCells = pgRsvdCells + portRsvdCells;
  bool canUsePortRsvd = pgCfg.canUsePortRsvd().value();
  auto avail = queueTracker_->getAvailRsvdCells();

  // Verify size reported by GetAvailRsvdCellsIngr
  if (canUsePortRsvd) {
    EXPECT_EQ(avail, totalRsvdCells);
  } else {
    EXPECT_EQ(avail, pgRsvdCells);
  }

  // Split a packet across pg reservd and port reserved
  auto pktCells = totalRsvdCells - 1;
  EXPECT_TRUE(buf_->admitPacket(port_, pg_, pktCells * cellSize_));

  // Should have used up all the PG cells
  EXPECT_EQ(queueStats_->usedQueueRsvdCells, pgRsvdCells);

  if (canUsePortRsvd) {
    // If we can use the port's reserved space, we have 1 cell of reserved
    // left
    EXPECT_EQ(queueTracker_->getAvailRsvdCells(), 1);
    EXPECT_EQ(portStats_->usedPortRsvdCells, portRsvdCells - 1);
    EXPECT_EQ(portStats_->usedSharedCells, 0);
  } else {
    EXPECT_EQ(queueTracker_->getAvailRsvdCells(), 0);
    EXPECT_EQ(portStats_->usedPortRsvdCells, 0);
    EXPECT_EQ(portStats_->usedSharedCells, portRsvdCells - 1);
  }

  // Drain the port
  buf_->removePacket(port_, pg_, pktCells * cellSize_);
  EXPECT_EQ(portStats_->usedCells, 0);

  // Split a packet across reserved and shared
  pktCells = 2 * totalRsvdCells - 1;
  EXPECT_TRUE(buf_->admitPacket(port_, pg_, pktCells * cellSize_ - 1));

  // Shouldn't have any reserved cells left in either case
  EXPECT_EQ(queueTracker_->getAvailRsvdCells(), 0);

  if (canUsePortRsvd) {
    EXPECT_EQ(queueStats_->usedQueueRsvdCells, pgRsvdCells);
    EXPECT_EQ(portStats_->usedPortRsvdCells, portRsvdCells);
    EXPECT_EQ(portStats_->usedSharedCells, totalRsvdCells - 1);
  } else {
    EXPECT_EQ(queueStats_->usedQueueRsvdCells, pgRsvdCells);
    EXPECT_EQ(portStats_->usedPortRsvdCells, 0);
    EXPECT_EQ(queueStats_->usedSharedCells, totalRsvdCells - 1 + portRsvdCells);
  }
}

TEST_P(DynShareTests, TestPfcWithXonHystCells) {
  if (!lossless_) {
    return;
  }

  // This test specifically validates PFC behavior with xonHystCells mechanism
  // Verify that xonHystCells is set
  ASSERT_TRUE(queueTracker_->xonHystCells.has_value())
      << "TestPfcWithXonHystCells expects xonHystCells to be configured";

  // Fill reserved space
  auto size = cellSize_ * queueTracker_->getAvailRsvdCells();
  EXPECT_TRUE(buf_->admitPacket(port_, pg_, size));
  EXPECT_EQ(queueTracker_->getAvailRsvdCells(), 0);

  // Check that alpha calculation is doing what it should
  double alpha = bufCfg_.ports()->at(port_).pgs()->at(pg_).alpha().value();
  auto sharedPoolCells =
      bufStats_->availSharedCells + bufStats_->usedSharedCells;
  double alphaFree = alpha * sharedPoolCells;
  EXPECT_EQ(alphaFree, queueTracker_->getAvailSharedCells());

  // Fill the shared space such that alpha * free == # of cells used (so any
  // future cells go to headroom)
  uint32_t prePauseCells = std::floor(sharedPoolCells * alpha / (1 + alpha));
  EXPECT_TRUE(buf_->admitPacket(port_, pg_, prePauseCells * cellSize_));
  // Shouldn't be able to put any more cells into shared space for this queue:
  EXPECT_EQ(queueTracker_->getAvailSharedCells(), 0);

  // Check that we still haven't reduced available headroom
  EXPECT_EQ(queueStats_->usedHdrmCells, 0);
  EXPECT_EQ(portStats_->usedHdrmCells, 0);

  // Check that port isn't paused yet -- haven't used any headroom
  expectPfcState(PfcState::XON);

  // Save total used cells to make sure a dropped packet doesn't change
  // anything
  auto usedPreAdmit = bufStats_->usedCells;

  // Any further cells should be dropped if PG isn't lossless; otherwise
  // should be put into the headroom
  EXPECT_EQ(buf_->admitPacket(port_, pg_, 1), queueTracker_->lossless);

  if (!queueTracker_->lossless) {
    EXPECT_EQ(bufStats_->usedCells, usedPreAdmit);
    EXPECT_EQ(queueStats_->usedHdrmCells, 0);
    EXPECT_EQ(portStats_->usedHdrmCells, 0);
    // End of test for lossy PGs
    return;
  }

  // Everything after this is only for lossless PGs

  // Still no more shared cells available
  EXPECT_EQ(queueTracker_->getAvailSharedCells(), 0);
  // Should be using one headroom cell now
  EXPECT_EQ(bufStats_->usedHdrmCells, 1);
  // Port should be paused now, if PFC is enabled
  auto pausedIfPfc = queueTracker_->pfc ? PfcState::XOFF : PfcState::XON;
  expectPfcState(pausedIfPfc);

  // Drain headroom
  buf_->removePacket(port_, pg_, 1);
  EXPECT_EQ(bufStats_->usedHdrmCells, 0);
  EXPECT_EQ(queueStats_->usedHdrmCells, 0);
  EXPECT_EQ(portStats_->usedHdrmCells, 0);
  expectPfcState(pausedIfPfc);

  // Test xonHystCells-based resume: Drain to just before XON threshold
  auto dynXonHyst = queueTracker_->xonHystCells.value();
  // Solve for Q in `alpha * (B - Q) - xonHyst == Q` to get:
  auto xonCells =
      std::floor(((alpha * sharedPoolCells) - dynXonHyst) / (1 + alpha));
  // Send out enough cells so that we're one above Xon cells
  auto sendCells = portStats_->usedSharedCells - xonCells - 1;
  buf_->removePacket(port_, pg_, sendCells * cellSize_);

  // Make sure PFC PGs are still paused
  expectPfcState(pausedIfPfc);

  // Make sure sending one more cell actually unpauses us
  buf_->removePacket(port_, pg_, cellSize_);
  expectPfcState(PfcState::XON);

  // Verify high water mark is not used with xonHystCells
  EXPECT_EQ(queueTracker_->stats.watermarkXoffSharedCells, 0);
}

TEST_P(DynShareTests, TestPfcWithResumeOffsetCells) {
  if (!lossless_) {
    return;
  }

  // Build a new config with resumeOffsetCells instead of xonHystCells
  auto testCfg = buildConfig(/*isLossless=*/true);
  constexpr uint32_t resumeOffsetCells = 8; // 8 cells resume offset

  // Check if this PG supports PFC
  const auto& pgCfg = testCfg.ports()->at(port_).pgs()->at(pg_);
  const bool hasPfc = pgCfg.pfc().value();
  const bool isLossless = pgCfg.lossless().value();

  if (!hasPfc || !isLossless) {
    return; // Skip non-PFC or non-lossless PGs
  }

  // Update the config to use resumeOffsetCells
  for (auto& [portNum, portCfg] : *testCfg.ports()) {
    for (auto& pgCfgLocal : *portCfg.pgs()) {
      pgCfgLocal.xonHystCells().reset(); // Clear xonHystCells
      pgCfgLocal.resumeOffsetCells() = resumeOffsetCells;
    }
  }

  // Create new buffer with resumeOffsetCells config
  auto testBuf = std::make_unique<SimpleSwitchBuffer>(
      &simCtx_, testCfg, /*useIngress=*/true);
  PfcState lastTestPfcState = PfcState::XON;
  testBuf->setOnPfcStateChangedCallback(
      [&lastTestPfcState](
          uint16_t /* port */, uint8_t /* pg */, PfcState state) {
        lastTestPfcState = state;
      });

  auto& testQueueTracker = testBuf->queueTracker(port_, pg_);
  auto& testBufStats = testBuf->bufferStats();

  // Verify resumeOffsetCells is set and xonHystCells is not
  ASSERT_TRUE(testQueueTracker.resumeOffsetCells.has_value());
  EXPECT_FALSE(testQueueTracker.xonHystCells.has_value());
  EXPECT_EQ(testQueueTracker.resumeOffsetCells.value(), resumeOffsetCells);

  // Fill reserved space
  const auto size = cellSize_ * testQueueTracker.getAvailRsvdCells();
  EXPECT_TRUE(testBuf->admitPacket(port_, pg_, size));
  EXPECT_EQ(testQueueTracker.getAvailRsvdCells(), 0);

  // Check that alpha calculation is doing what it should
  const double alpha =
      testCfg.ports()->at(port_).pgs()->at(pg_).alpha().value();
  const auto sharedPoolCells =
      testBufStats.availSharedCells + testBufStats.usedSharedCells;
  const double alphaFree = alpha * sharedPoolCells;
  EXPECT_EQ(alphaFree, testQueueTracker.getAvailSharedCells());

  // Fill the shared space such that alpha * free == # of cells used (so any
  // future cells go to headroom)
  const uint32_t prePauseCells =
      std::floor(sharedPoolCells * alpha / (1 + alpha));
  EXPECT_TRUE(testBuf->admitPacket(port_, pg_, prePauseCells * cellSize_));
  // Shouldn't be able to put any more cells into shared space for this queue:
  EXPECT_EQ(testQueueTracker.getAvailSharedCells(), 0);

  // Check that we still haven't reduced available headroom
  EXPECT_EQ(testBufStats.usedHdrmCells, 0);

  // Check that port isn't paused yet -- haven't used any headroom
  EXPECT_EQ(lastTestPfcState, PfcState::XON);

  // Trigger PFC by using headroom
  EXPECT_TRUE(testBuf->admitPacket(port_, pg_, cellSize_));

  // Still no more shared cells available
  EXPECT_EQ(testQueueTracker.getAvailSharedCells(), 0);
  // Should be using one headroom cell now
  EXPECT_EQ(testBufStats.usedHdrmCells, 1);
  // Port should be paused now
  EXPECT_EQ(lastTestPfcState, PfcState::XOFF);

  // Record the high water mark
  const uint32_t recordedHighWater =
      testQueueTracker.stats.watermarkXoffSharedCells;
  EXPECT_EQ(recordedHighWater, testBufStats.usedSharedCells);
  EXPECT_GT(recordedHighWater, 0);

  // Drain headroom
  testBuf->removePacket(port_, pg_, cellSize_);
  EXPECT_EQ(testBufStats.usedHdrmCells, 0);
  // Should still be paused (headroom drained but shared buffer not drained
  // enough)
  EXPECT_EQ(lastTestPfcState, PfcState::XOFF);

  // Test resumeOffsetCells-based resume: Calculate XON threshold
  // XON should trigger when: currentSharedUsage <= (highWater -
  // resumeOffsetCells)
  const uint32_t targetSharedUsage = recordedHighWater >= resumeOffsetCells
      ? recordedHighWater - resumeOffsetCells
      : 0;

  // Drain to just above the XON threshold
  const uint32_t currentSharedUsage = testBufStats.usedSharedCells;
  if (currentSharedUsage > targetSharedUsage + 1) {
    const uint32_t drainCells = currentSharedUsage - targetSharedUsage - 1;
    testBuf->removePacket(port_, pg_, drainCells * cellSize_);
    EXPECT_EQ(lastTestPfcState, PfcState::XOFF); // Still in XOFF
  }

  // Drain one more cell to trigger XON
  testBuf->removePacket(port_, pg_, cellSize_);
  EXPECT_EQ(lastTestPfcState, PfcState::XON);

  // Verify high water mark is reset after XON transition
  EXPECT_EQ(testQueueTracker.stats.watermarkXoffSharedCells, 0);
}

TEST_P(DynShareTests, TestIngrEgrAdmissionCheck) {
  if (lossless_) {
    return;
  }
  // Setup Egress settings such that egress thresholds are hit before ingress
  // threhsolds. Setup config for Ingress Rsvd is < egress Rsvd
  auto egrAvailRsvdCells = egrQueueTracker_->getAvailRsvdCells();
  auto pgAvailRsvdCells = queueTracker_->getAvailRsvdCells();
  auto egrQRsvdCells = egrQueueStats_->availQueueRsvdCells;

  auto pktSizeCells1 = egrAvailRsvdCells;
  auto pktSize = cellSize_ * pktSizeCells1;
  auto ingrUsedSharedCells = egrAvailRsvdCells > pgAvailRsvdCells
      ? egrAvailRsvdCells - pgAvailRsvdCells
      : 0;
  EXPECT_TRUE(buf_->admitPacket(port_, pg_, pktSize));
  EXPECT_TRUE(egrBuf_->admitPacket(egrPort_, egrQueue_, pktSize));

  // Fill reserved space
  EXPECT_EQ(queueTracker_->getAvailRsvdCells(), 0);
  EXPECT_EQ(egrQueueTracker_->getAvailRsvdCells(), 0);
  EXPECT_EQ(egrQueueStats_->availQueueRsvdCells, 0);
  EXPECT_EQ(egrQueueStats_->usedQueueRsvdCells, egrQRsvdCells);
  EXPECT_EQ(egrQueueStats_->usedSharedCells, 0);
  EXPECT_EQ(egrPortStats_->usedSharedCells, 0);
  EXPECT_EQ(egrPortStats_->usedCells, egrAvailRsvdCells);

  // Check that alpha calculation is doing what it should
  double alpha = queueTracker_->alpha;
  double egrAlpha = egrQueueTracker_->alpha;
  auto ingrSharedPoolCells =
      bufStats_->availSharedCells + bufStats_->usedSharedCells;
  auto egrSharedPoolCells =
      egrBufStats_->availSharedCells + egrBufStats_->usedSharedCells;

  auto availIngrSharedCells = ingrSharedPoolCells - ingrUsedSharedCells;
  int alphaFree = (alpha * availIngrSharedCells) - ingrUsedSharedCells;
  EXPECT_EQ(alphaFree, queueTracker_->getAvailSharedCells());

  double egrAlphaFree = std::floor(egrAlpha * egrSharedPoolCells);
  EXPECT_EQ(egrAlphaFree, egrQueueTracker_->getAvailSharedCells());

  EXPECT_EQ(queueStats_->usedHdrmCells, 0);
  EXPECT_EQ(portStats_->usedHdrmCells, 0);
  EXPECT_EQ(queueStats_->usedSharedCells, ingrUsedSharedCells);
  EXPECT_EQ(portStats_->usedSharedCells, ingrUsedSharedCells);

  // Fill the shared space such that alpha * free == # of cells used (so any
  // future cells go to headroom)
  uint32_t preTailDropCells =
      std::floor(egrSharedPoolCells * egrAlpha / (1 + egrAlpha)) + 1;
  auto pktSizeCells2 = preTailDropCells;
  pktSize = preTailDropCells * cellSize_;
  EXPECT_EQ(alphaFree, queueTracker_->getAvailSharedCells());
  EXPECT_TRUE(buf_->admitPacket(port_, pg_, pktSize));
  EXPECT_TRUE(egrBuf_->admitPacket(egrPort_, egrQueue_, pktSize));

  // Check that we still haven't reduced available headroom
  EXPECT_EQ(queueStats_->usedHdrmCells, 0);
  EXPECT_EQ(portStats_->usedHdrmCells, 0);

  // Check that port isn't paused yet -- haven't used any headroom
  expectPfcState(PfcState::XON);
  EXPECT_EQ(egrPortStats_->usedCells, pktSizeCells1 + pktSizeCells2);
}

TEST_P(DynShareTests, TestResumeOffsetCells) {
  if (!lossless_) {
    return;
  }

  // Build a new config with resumeOffsetCells instead of xonHystCells
  auto testCfg = buildConfig(/*isLossless=*/true);
  constexpr uint32_t resumeOffsetCells = 8; // 8 cells resume offset

  // Check if this PG supports PFC
  const auto& pgCfg = testCfg.ports()->at(port_).pgs()->at(pg_);
  const bool hasPfc = pgCfg.pfc().value();
  const bool isLossless = pgCfg.lossless().value();

  // Update the config to use resumeOffsetCells
  for (auto& [portNum, portCfg] : *testCfg.ports()) {
    for (auto& pgCfgLocal : *portCfg.pgs()) {
      pgCfgLocal.xonHystCells().reset(); // Clear xonHystCells
      pgCfgLocal.resumeOffsetCells() = resumeOffsetCells;
    }
  }

  // Create new buffer with resumeOffsetCells config
  auto testBuf = std::make_unique<SimpleSwitchBuffer>(
      &simCtx_, testCfg, /*useIngress=*/true);
  PfcState lastTestPfcState = PfcState::XON;
  testBuf->setOnPfcStateChangedCallback(
      [&lastTestPfcState](
          uint16_t /* port */, uint8_t /* pg */, PfcState state) {
        lastTestPfcState = state;
      });

  auto& testQueueTracker = testBuf->queueTracker(port_, pg_);
  auto& testBufStats = testBuf->bufferStats();

  // Verify resumeOffsetCells is set and xonHystCells is not
  ASSERT_TRUE(testQueueTracker.resumeOffsetCells.has_value());
  EXPECT_FALSE(testQueueTracker.xonHystCells.has_value());
  EXPECT_EQ(testQueueTracker.resumeOffsetCells.value(), resumeOffsetCells);

  // For non-PFC or non-lossless PGs, just verify basic functionality
  if (!hasPfc || !isLossless) {
    // Verify we can still admit and remove packets normally
    const auto size = cellSize_ * testQueueTracker.getAvailRsvdCells();
    EXPECT_TRUE(testBuf->admitPacket(port_, pg_, size));
    testBuf->removePacket(port_, pg_, size);
    // PFC should always stay in XON state for non-PFC PGs
    EXPECT_EQ(lastTestPfcState, PfcState::XON);
    return;
  }

  // Rest of test is for PFC-enabled, lossless PGs only

  // Fill reserved space
  const auto size = cellSize_ * testQueueTracker.getAvailRsvdCells();
  EXPECT_TRUE(testBuf->admitPacket(port_, pg_, size));

  // Fill shared space to trigger headroom usage
  const double alpha =
      testCfg.ports()->at(port_).pgs()->at(pg_).alpha().value();
  const auto sharedPoolCells =
      testBufStats.availSharedCells + testBufStats.usedSharedCells;
  const uint32_t prePauseCells =
      std::floor(sharedPoolCells * alpha / (1 + alpha));
  EXPECT_TRUE(testBuf->admitPacket(port_, pg_, prePauseCells * cellSize_));

  // Verify not paused yet (no headroom used)
  EXPECT_EQ(lastTestPfcState, PfcState::XON);

  // Trigger PFC by using headroom
  EXPECT_TRUE(testBuf->admitPacket(port_, pg_, cellSize_));
  EXPECT_EQ(lastTestPfcState, PfcState::XOFF);

  // Record the high water mark
  const uint32_t recordedHighWater =
      testQueueTracker.stats.watermarkXoffSharedCells;
  EXPECT_EQ(recordedHighWater, testBufStats.usedSharedCells);
  EXPECT_GT(recordedHighWater, 0);

  // Drain headroom but remain in XOFF
  testBuf->removePacket(port_, pg_, cellSize_);
  EXPECT_EQ(testBufStats.usedHdrmCells, 0);
  EXPECT_EQ(lastTestPfcState, PfcState::XOFF);

  // Calculate how many bytes to drain to reach XON threshold
  // XON should trigger when: currentSharedUsage <= (highWater -
  // resumeOffsetCells)
  const uint32_t targetSharedUsage = recordedHighWater >= resumeOffsetCells
      ? recordedHighWater - resumeOffsetCells
      : 0;

  // Drain to just above the XON threshold
  const uint32_t currentSharedUsage = testBufStats.usedSharedCells;
  if (currentSharedUsage > targetSharedUsage + 1) {
    const uint32_t drainCells = currentSharedUsage - targetSharedUsage - 1;
    testBuf->removePacket(port_, pg_, drainCells * cellSize_);
    EXPECT_EQ(lastTestPfcState, PfcState::XOFF); // Still in XOFF
  }

  // Drain one more cell to trigger XON
  testBuf->removePacket(port_, pg_, cellSize_);
  EXPECT_EQ(lastTestPfcState, PfcState::XON);

  // Verify high water mark is reset
  EXPECT_EQ(testQueueTracker.stats.watermarkXoffSharedCells, 0);
}

TEST_P(DynShareTests, TestResumeOffsetCellsMultipleCycles) {
  if (!lossless_) {
    return;
  }

  // Build config with resumeOffsetCells
  auto testCfg = buildConfig(/*isLossless=*/true);
  constexpr uint32_t resumeOffsetCells = 4;

  // Check if this PG supports PFC
  const auto& pgCfg = testCfg.ports()->at(port_).pgs()->at(pg_);
  const bool hasPfc = pgCfg.pfc().value();
  const bool isLossless = pgCfg.lossless().value();

  for (auto& [portNum, portCfg] : *testCfg.ports()) {
    for (auto& pgCfgLocal : *portCfg.pgs()) {
      pgCfgLocal.xonHystCells().reset();
      pgCfgLocal.resumeOffsetCells() = resumeOffsetCells;
    }
  }

  auto testBuf = std::make_unique<SimpleSwitchBuffer>(
      &simCtx_, testCfg, /*useIngress=*/true);
  PfcState lastTestPfcState = PfcState::XON;
  testBuf->setOnPfcStateChangedCallback(
      [&lastTestPfcState](
          uint16_t /* port */, uint8_t /* pg */, PfcState state) {
        lastTestPfcState = state;
      });

  auto& testQueueTracker = testBuf->queueTracker(port_, pg_);
  auto& testBufStats = testBuf->bufferStats();

  // For non-PFC or non-lossless PGs, verify basic multi-packet operations
  if (!hasPfc || !isLossless) {
    // Verify we can handle multiple cycles of packet admission/removal
    for (int cycle = 0; cycle < 3; cycle++) {
      const auto size = cellSize_ * 5;
      EXPECT_TRUE(testBuf->admitPacket(port_, pg_, size));
      testBuf->removePacket(port_, pg_, size);
    }
    // PFC should always stay in XON state for non-PFC PGs
    EXPECT_EQ(lastTestPfcState, PfcState::XON);
    EXPECT_EQ(testQueueTracker.stats.watermarkXoffSharedCells, 0);
    return;
  }

  // Rest of test is for PFC-enabled, lossless PGs only

  // First cycle: Fill and drain
  auto rsvdSize = cellSize_ * testQueueTracker.getAvailRsvdCells();
  EXPECT_TRUE(testBuf->admitPacket(port_, pg_, rsvdSize));

  const double alpha =
      testCfg.ports()->at(port_).pgs()->at(pg_).alpha().value();
  const auto sharedPoolCells =
      testBufStats.availSharedCells + testBufStats.usedSharedCells;
  const uint32_t prePauseCells =
      std::floor(sharedPoolCells * alpha / (1 + alpha));
  EXPECT_TRUE(testBuf->admitPacket(port_, pg_, prePauseCells * cellSize_));

  // Trigger XOFF
  EXPECT_TRUE(testBuf->admitPacket(port_, pg_, cellSize_));
  EXPECT_EQ(lastTestPfcState, PfcState::XOFF);
  const uint32_t firstHighWater =
      testQueueTracker.stats.watermarkXoffSharedCells;
  EXPECT_GT(firstHighWater, 0);

  // Drain to trigger XON
  testBuf->removePacket(port_, pg_, cellSize_); // Remove headroom
  EXPECT_EQ(testBufStats.usedHdrmCells, 0);

  const uint32_t targetSharedUsage1 = firstHighWater >= resumeOffsetCells
      ? firstHighWater - resumeOffsetCells
      : 0;

  // Drain enough to trigger XON
  uint32_t currentShared = testBufStats.usedSharedCells;
  if (currentShared > targetSharedUsage1) {
    testBuf->removePacket(
        port_, pg_, (currentShared - targetSharedUsage1) * cellSize_);
  }
  EXPECT_EQ(lastTestPfcState, PfcState::XON);
  EXPECT_EQ(testQueueTracker.stats.watermarkXoffSharedCells, 0);

  // Second cycle: Fill shared to nearly full, then trigger XOFF with headroom
  // We need to ensure there's enough space for at least one small fill +
  // headroom
  const uint32_t sharedAvail = testQueueTracker.getAvailSharedCells();

  // Only proceed with second cycle if we have space available
  if (sharedAvail >=
      11) { // Need at least 11 cells (10 for small fill + 1 for headroom)
    // Use up some of the available shared space
    constexpr uint32_t smallFill = 10;
    EXPECT_TRUE(testBuf->admitPacket(port_, pg_, smallFill * cellSize_));

    // Trigger XOFF by using headroom
    EXPECT_TRUE(testBuf->admitPacket(port_, pg_, cellSize_));
    EXPECT_EQ(lastTestPfcState, PfcState::XOFF);

    const uint32_t secondHighWater =
        testQueueTracker.stats.watermarkXoffSharedCells;
    EXPECT_GT(secondHighWater, 0);
    // Second high water should be different (lower) than first
    EXPECT_LT(secondHighWater, firstHighWater);
  }
}

TEST(SwitchBufferConfigTests, TestMutualExclusionValidation) {
  Scheduler scheduler;
  SimContext simCtx{&scheduler, 0};

  packsim::thrift::PriorityGroupBufferConfig pgCfg;
  pgCfg.priority() = 0U;
  pgCfg.rsvdCells() = 10U;
  pgCfg.canUsePortRsvd() = false;
  pgCfg.dynamicSharing() = true;
  pgCfg.alpha() = 0.5;
  pgCfg.maxSharedCells() = 1000U;
  pgCfg.maxHdrmCells() = 100U;
  pgCfg.lossless() = true;
  pgCfg.pfc() = true;

  // Test 1: Both xonHystCells and resumeOffsetCells set - should throw
  pgCfg.xonHystCells() = 10U;
  pgCfg.resumeOffsetCells() = 8U;

  EXPECT_THAT(
      [&] {
        QueueTracker tracker(&simCtx);
        tracker.initialize(pgCfg);
      },
      ThrowsMessage<std::runtime_error>(HasSubstr(
          "xonHystCells and resumeOffsetCells cannot be set at the same time")));

  // Test 2: Only xonHystCells set - should succeed
  pgCfg.xonHystCells() = 10U;
  pgCfg.resumeOffsetCells().reset();

  EXPECT_NO_THROW({
    QueueTracker tracker(&simCtx);
    tracker.initialize(pgCfg);
    EXPECT_TRUE(tracker.xonHystCells.has_value());
    EXPECT_FALSE(tracker.resumeOffsetCells.has_value());
  });

  // Test 3: Only resumeOffsetCells set - should succeed
  pgCfg.xonHystCells().reset();
  pgCfg.resumeOffsetCells() = 8U;

  EXPECT_NO_THROW({
    QueueTracker tracker(&simCtx);
    tracker.initialize(pgCfg);
    EXPECT_FALSE(tracker.xonHystCells.has_value());
    EXPECT_TRUE(tracker.resumeOffsetCells.has_value());
  });

  // Test 4: Neither set - should succeed (no PFC resume)
  pgCfg.xonHystCells().reset();
  pgCfg.resumeOffsetCells().reset();

  EXPECT_NO_THROW({
    QueueTracker tracker(&simCtx);
    tracker.initialize(pgCfg);
    EXPECT_FALSE(tracker.xonHystCells.has_value());
    EXPECT_FALSE(tracker.resumeOffsetCells.has_value());
  });
}

// Sweep across first and last ports (plus random ones: 1 and 8) and all PGs
INSTANTIATE_TEST_SUITE_P(
    DynSharePortPgSweepTests,
    DynShareTests,
    ::testing::Combine(
        ::testing::Values(0, 1, 8, 31),
        ::testing::Values(0, 1, 2, 3, 4, 5, 6, 7),
        ::testing::Values(true),
        ::testing::Values(0), // NA
        ::testing::Values(0))); // NA

INSTANTIATE_TEST_SUITE_P(
    DynSharePortEgressSweepTests,
    DynShareTests,
    ::testing::Combine(
        ::testing::Values(0, 1, 8, 31),
        ::testing::Values(0, 1, 2, 3, 4, 5, 6, 7),
        ::testing::Values(false),
        ::testing::Values(0, 10, 36, 63),
        ::testing::Values(0, 1, 5, 7)));

// SwitchStats::serializeState() writes cellSizeBytes for the buffer, port and
// queue alike, so a level whose cellSize is left at its 0 default serializes a
// 0 that silently zeroes every cells->bytes conversion downstream.
//
// Not parameterized on DynShareTests: cellSize is the same in that fixture's
// lossless and lossy configs, so its sweep would run this check 544 times over
// one port/queue pair per case. Walking every port and queue once instead is
// both cheaper and stricter -- it would catch a seeding loop that skipped a
// port, which a single-pair check cannot.
TEST(SwitchBufferCellSizeTest, TestCellSizeSetAtEveryLevel) {
  constexpr uint16_t kNumPorts = 4;
  constexpr uint8_t kNumQueues = 8;
  constexpr uint32_t kCellSizeBytes = 254;
  constexpr uint32_t kBufSizeMiB = 32;

  packsim::thrift::SwitchBufferConfig sbc;
  sbc.cellSizeBytes() = kCellSizeBytes;
  sbc.totalCells() = kBufSizeMiB * 1024 * 1024 / kCellSizeBytes;
  for (uint16_t portNum = 0; portNum < kNumPorts; ++portNum) {
    packsim::thrift::PortBufferConfig pbc;
    pbc.port() = portNum;
    pbc.portMaxSharedCells() = kUnlimitedCells;
    pbc.isIngrPort() = true;
    pbc.isEgrPort() = true;
    // Ingress reads pgs, egress reads ucQueues -- populate both so the same
    // config drives either direction.
    for (uint8_t queue = 0; queue < kNumQueues; ++queue) {
      packsim::thrift::PriorityGroupBufferConfig pgbc;
      pgbc.priority() = queue;
      pgbc.maxSharedCells() = kUnlimitedCells;
      pbc.pgs()->emplace_back(pgbc);

      packsim::thrift::EgressQueueBufferConfig eqbc;
      eqbc.cos() = queue;
      eqbc.maxSharedCells() = kUnlimitedCells;
      pbc.ucQueues()->emplace_back(eqbc);
    }
    sbc.ports()[portNum] = pbc;
  }

  Scheduler scheduler;
  SimContext simCtx{&scheduler, 0};
  // Ingress and egress are separate BufferTracker instances seeded by the same
  // code path, so both are checked.
  for (const bool useIngress : {true, false}) {
    SimpleSwitchBuffer buf(&simCtx, sbc, useIngress);
    EXPECT_EQ(buf.bufferStats().cellSize, kCellSizeBytes)
        << "useIngress=" << useIngress;
    for (uint16_t portNum = 0; portNum < kNumPorts; ++portNum) {
      EXPECT_EQ(buf.portStats(portNum).cellSize, kCellSizeBytes)
          << "useIngress=" << useIngress << " port=" << portNum;
      for (uint8_t queue = 0; queue < kNumQueues; ++queue) {
        EXPECT_EQ(buf.queueStats(portNum, queue).cellSize, kCellSizeBytes)
            << "useIngress=" << useIngress << " port=" << portNum
            << " queue=" << +queue;
      }
    }
  }
}

// Every queue event bumps the cumulative counters of its queue, its port and
// its buffer, so a serialized port and buffer must report the totals of their
// children. Crossing 4 GiB takes a few thousand packets, so these run once
// rather than across the DynShareTests parameter sweep.
class BufferSerializeTests : public ::testing::Test {
 public:
  void SetUp() override {
    buf_ = std::make_unique<SimpleSwitchBuffer>(
        &simCtx_, buildConfig(), /*useIngress=*/true);
  }

 protected:
  // 4 ports x 8 queues is the smallest shape that exercises both aggregation
  // steps, queues -> port and ports -> buffer. DynShareTests' 32-port config
  // would work too, but these tests care only about how many bytes each level
  // counts, so a config carrying no PFC, headroom or reservation thresholds
  // states that directly.
  static constexpr uint16_t kNumPorts = 4;
  static constexpr uint8_t kNumQueues = 8;
  static constexpr uint32_t kCellSizeBytes = 254;
  static constexpr uint32_t kBufSizeMiB = 32;

  // A lossy switch buffer with one unconstrained shared pool. Lossless and PFC
  // are off so admission is never paused: a paused queue would make the totals
  // depend on drain timing rather than on what the tests admitted.
  static packsim::thrift::SwitchBufferConfig buildConfig() {
    packsim::thrift::SwitchBufferConfig sbc;
    sbc.cellSizeBytes() = kCellSizeBytes;
    sbc.totalCells() = kBufSizeMiB * 1024 * 1024 / kCellSizeBytes;

    for (uint16_t portNum = 0; portNum < kNumPorts; ++portNum) {
      packsim::thrift::PortBufferConfig pbc;
      pbc.port() = portNum;
      pbc.portMaxSharedCells() = kUnlimitedCells;

      for (uint8_t queue = 0; queue < kNumQueues; ++queue) {
        packsim::thrift::PriorityGroupBufferConfig pgbc;
        pgbc.priority() = queue;
        pgbc.dynamicSharing() = true;
        pgbc.alpha() = 1.0;
        pgbc.maxSharedCells() = kUnlimitedCells;
        pgbc.lossless() = false;
        pgbc.pfc() = false;
        pbc.pgs()->emplace_back(std::move(pgbc));
      }

      sbc.ports()[portNum] = std::move(pbc);
    }

    return sbc;
  }

  // The serialized port carrying `portNum`, or nullptr if the buffer has none.
  static const packsim::switches::thrift::Port* findPort(
      const packsim::switches::thrift::Buffer& serBuf,
      uint16_t portNum) {
    for (const auto& serPort : *serBuf.ports()) {
      if (*serPort.portNum() == portNum) {
        return &serPort;
      }
    }
    return nullptr;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<SimpleSwitchBuffer> buf_;
};

TEST_F(BufferSerializeTests, TestEveryCumulativeCounterSumsItsChildren) {
  // Drive all seven cumulative counters: a level that bumped the wrong field --
  // say wredDroppedPkts picking up droppedPkts -- must not pass.
  //
  // Every queue gets a different size and a different count of each event, so
  // no level can satisfy these assertions by copying one child and scaling it,
  // and no two counters hold the same total.
  //
  // Drops, WRED and ECN are driven through QueueTracker directly: admitPacket
  // only reaches droppedPacket once the pool is exhausted, and driving each
  // queue's counters directly keeps the test about the aggregation, not the
  // admission policy that triggers it.
  uint64_t admittedPkts = 0;
  uint64_t admittedBytes = 0;
  uint64_t droppedPkts = 0;
  uint64_t droppedBytes = 0;
  uint64_t wredDroppedPkts = 0;
  uint64_t wredDroppedBytes = 0;
  uint64_t ecnMarkedPkts = 0;

  for (uint16_t portNum = 0; portNum < kNumPorts; ++portNum) {
    for (uint8_t queue = 0; queue < kNumQueues; ++queue) {
      const uint32_t queueIdx = uint32_t{portNum} * kNumQueues + queue;
      const uint32_t admitBytes = 1000 + 64 * queueIdx;
      const uint32_t dropBytes = 111 + 8 * queueIdx;
      const uint32_t wredBytes = 222 + 16 * queueIdx;
      const uint32_t drops = 1 + queueIdx % 3;
      const uint32_t wreds = 2 + queueIdx % 4;
      const uint32_t ecns = 3 + queueIdx % 5;

      ASSERT_TRUE(buf_->admitPacket(portNum, queue, admitBytes));
      ++admittedPkts;
      admittedBytes += admitBytes;

      auto& tracker = buf_->queueTracker(portNum, queue);
      for (uint32_t i = 0; i < drops; ++i) {
        tracker.droppedPacket(dropBytes);
      }
      droppedPkts += drops;
      droppedBytes += uint64_t{drops} * dropBytes;

      for (uint32_t i = 0; i < wreds; ++i) {
        tracker.wredDroppedPacket(wredBytes);
      }
      wredDroppedPkts += wreds;
      wredDroppedBytes += uint64_t{wreds} * wredBytes;

      for (uint32_t i = 0; i < ecns; ++i) {
        tracker.ecnMarkedPacket();
      }
      ecnMarkedPkts += ecns;
    }
  }

  const auto serBuf = buf_->serializeState();

  // One entry per cumulative counter, so the assertions below stay a single
  // loop rather than seven near-identical copies.
  struct Counter {
    const char* name;
    std::function<uint64_t(const packsim::switches::thrift::BufferStats& stats)>
        get;
    uint64_t expectedTotal;
  };
  const std::vector<Counter> counters{
      {"admittedPkts",
       [](const auto& s) { return *s.admittedPkts(); },
       admittedPkts},
      {"admittedBytes",
       [](const auto& s) { return *s.admittedBytes(); },
       admittedBytes},
      {"droppedPkts",
       [](const auto& s) { return *s.droppedPkts(); },
       droppedPkts},
      {"droppedBytes",
       [](const auto& s) { return *s.droppedBytes(); },
       droppedBytes},
      {"wredDroppedPkts",
       [](const auto& s) { return *s.wredDroppedPkts(); },
       wredDroppedPkts},
      {"wredDroppedBytes",
       [](const auto& s) { return *s.wredDroppedBytes(); },
       wredDroppedBytes},
      {"ecnMarkedPkts",
       [](const auto& s) { return *s.ecnMarkedPkts(); },
       ecnMarkedPkts},
  };

  for (const auto& [name, get, expectedTotal] : counters) {
    uint64_t bufferSum = 0;
    for (const auto& serPort : *serBuf.ports()) {
      uint64_t portSum = 0;
      for (const auto& serQueue : *serPort.queues()) {
        portSum += get(*serQueue.bufStats());
      }
      EXPECT_EQ(get(*serPort.bufStats()), portSum)
          << name << " at port " << *serPort.portNum();
      bufferSum += portSum;
    }
    EXPECT_EQ(get(*serBuf.bufStats()), bufferSum) << name << " at buffer";
    // Pin the absolute total as well, so a buffer that summed consistently
    // wrong children still fails.
    EXPECT_EQ(get(*serBuf.bufStats()), expectedTotal) << name << " total";
  }
}

TEST_F(BufferSerializeTests, TestByteCountersSurviveThe32BitWrap) {
  // Drive it all through one queue, so the queue, its port and the buffer each
  // count more than 4 GiB.

  // 5 GiB, one wrap past the limit.
  constexpr uint64_t kTargetBytes = 5ULL << 30;
  constexpr uint32_t kPktBytes = 1U << 20; // 1 MiB, well inside a 32 MiB pool
  // Drops never occupy the pool, so each can be far larger than an admission.
  constexpr uint32_t kDropBytes = 1U << 30;
  constexpr uint16_t kPortNum = 0;
  constexpr uint8_t kQueueNum = 0;

  uint64_t admittedBytes = 0;
  uint64_t admittedPkts = 0;
  while (admittedBytes < kTargetBytes) {
    ASSERT_TRUE(buf_->admitPacket(kPortNum, kQueueNum, kPktBytes));
    buf_->removePacket(kPortNum, kQueueNum, kPktBytes);
    admittedBytes += kPktBytes;
    ++admittedPkts;
  }
  ASSERT_GT(admittedBytes, UINT32_MAX);

  auto& tracker = buf_->queueTracker(kPortNum, kQueueNum);
  uint64_t droppedBytes = 0;
  while (droppedBytes < kTargetBytes) {
    tracker.droppedPacket(kDropBytes);
    tracker.wredDroppedPacket(kDropBytes);
    droppedBytes += kDropBytes;
  }

  const auto serBuf = buf_->serializeState();
  const auto* serPort = findPort(serBuf, kPortNum);
  ASSERT_NE(serPort, nullptr);
  const auto& serQueue = serPort->queues()->at(kQueueNum);
  ASSERT_EQ(*serQueue.queueNum(), kQueueNum);

  const std::vector<
      std::pair<const char*, const packsim::switches::thrift::BufferStats*>>
      levels{
          {"queue", &*serQueue.bufStats()},
          {"port", &*serPort->bufStats()},
          {"buffer", &*serBuf.bufStats()},
      };
  for (const auto& [level, stats] : levels) {
    EXPECT_EQ(*stats->admittedBytes(), admittedBytes) << level;
    EXPECT_EQ(*stats->admittedPkts(), admittedPkts) << level;
    EXPECT_EQ(*stats->droppedBytes(), droppedBytes) << level;
    EXPECT_EQ(*stats->wredDroppedBytes(), droppedBytes) << level;
  }

  // Every packet was removed again, so this is throughput over the run and not
  // occupancy -- the buffer never held anything close to 5 GiB.
  EXPECT_EQ(*serBuf.bufStats()->usedCells(), 0U);
}

// Ports serialize in ascending port order, whatever order the buffer holds
// them in, so a run and its replay write the same result. The port numbers
// are scattered so a hash map cannot hand them back ascending by chance.
TEST_F(BufferSerializeTests, TestPortsSerializeInAscendingPortOrder) {
  auto sbc = buildConfig();
  const auto templatePort = sbc.ports()->at(0);
  sbc.ports()->clear();
  for (const uint32_t portNum : {40u, 3u, 17u, 29u, 8u, 61u, 12u, 55u}) {
    auto pbc = templatePort;
    pbc.port() = portNum;
    sbc.ports()[portNum] = std::move(pbc);
  }
  SimpleSwitchBuffer buf(&simCtx_, sbc, /*useIngress=*/true);

  const auto serBuf = buf.serializeState();

  std::vector<uint32_t> serializedPortNums;
  for (const auto& serPort : *serBuf.ports()) {
    serializedPortNums.emplace_back(*serPort.portNum());
  }
  EXPECT_THAT(
      serializedPortNums, ElementsAre(3u, 8u, 12u, 17u, 29u, 40u, 55u, 61u));
}

class SwitchBufferCbfcTest : public ::testing::Test {
 protected:
  SimpleSwitchBuffer& makeBuffer(uint32_t rsvdCells, uint32_t maxSharedCells) {
    packsim::thrift::SwitchBufferConfig cfg;
    cfg.cellSizeBytes() = 100U;
    cfg.totalCells() = 100U;
    cfg.headroomPoolCells() = 10U;
    packsim::thrift::PriorityGroupBufferConfig pg;
    pg.priority() = 0U;
    pg.rsvdCells() = rsvdCells;
    pg.maxSharedCells() = maxSharedCells;
    pg.maxHdrmCells() = 10U;
    pg.lossless() = true;
    pg.pfc() = false;
    packsim::thrift::PortBufferConfig portCfg;
    portCfg.port() = 0U;
    portCfg.isIngrPort() = true;
    portCfg.pgs() = {pg};
    cfg.ports()[0U] = portCfg;
    buf_.emplace(&simCtx_, cfg, /*useIngress=*/true);
    return *buf_;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  // Declared after `simCtx_`, so it is destroyed before the context it
  // borrows.
  std::optional<SimpleSwitchBuffer> buf_;
};

// A buffer holds trackers only for the queues its ports configure, so a queue
// past them aborts instead of reaching another port's trackers or past the
// buffer's storage.
TEST_F(SwitchBufferCbfcTest, QueuePastTheConfiguredOnesAborts) {
  auto& buf = makeBuffer(/*rsvdCells=*/1, /*maxSharedCells=*/1);

  EXPECT_DEATH(buf.queueTracker(0, 1), "queuesPerPort_");
}

// An unmanaged queue ignores `trafficClass`: lossy traffic still reaches
// headroom, and `heldCreditsOut` gets the packet's whole cost rather than its
// headroom share (400 B over 100 B cells = 4).
TEST_F(SwitchBufferCbfcTest, UnmanagedQueueStillLetsLossyTrafficUseHeadroom) {
  auto& buf = makeBuffer(/*rsvdCells=*/1, /*maxSharedCells=*/1);

  auto& q = buf.queueTracker(0, 0);
  ASSERT_FALSE(q.cbfcManaged) << "default must be the legacy path";

  uint16_t held = 0;
  EXPECT_TRUE(buf.admitPacket(
      0,
      0,
      400,
      {.trafficClass = BufferTrafficClass::LOSSY, .heldCreditsOut = &held}));
  EXPECT_GT(q.stats.usedHdrmCells, 0u);
  EXPECT_EQ(4u, held)
      << "an unmanaged queue holds the packet's whole cost until drain";
}

// Per `BufferTrafficClass`, a lossy packet may not use PG headroom, so it is
// refused once PG minimum and the shared pool are full.
TEST_F(SwitchBufferCbfcTest, CbfcManagedQueueRefusesLossyTrafficHeadroom) {
  auto& buf = makeBuffer(/*rsvdCells=*/1, /*maxSharedCells=*/1);
  buf.setPortCbfcManaged(0);
  ASSERT_TRUE(buf.queueTracker(0, 0).cbfcManaged);

  uint16_t lossy = 0;
  EXPECT_FALSE(buf.admitPacket(
      0,
      0,
      400,
      {.trafficClass = BufferTrafficClass::LOSSY, .heldCreditsOut = &lossy}));
}

// Lossless traffic reaches headroom, and the credits it records are consumed
// exactly once on release.
TEST_F(SwitchBufferCbfcTest, CbfcManagedQueueAdmitsLosslessTrafficToHeadroom) {
  auto& buf = makeBuffer(/*rsvdCells=*/1, /*maxSharedCells=*/1);
  buf.setPortCbfcManaged(0);

  uint16_t lossless = 0;
  ASSERT_TRUE(buf.admitPacket(
      0,
      0,
      400,
      {.trafficClass = BufferTrafficClass::LOSSLESS,
       .heldCreditsOut = &lossless}));
  EXPECT_GT(lossless, 0u) << "lossless traffic reaches credited headroom";

  buf.removePacket(0, 0, 400, lossless);
  EXPECT_EQ(0u, buf.queueTracker(0, 0).stats.usedCells);
}

// An unmanaged queue releases headroom first regardless of where the departing
// packet landed. On a CBFC-managed queue that per-packet attribution has to be
// exact, or CBFC frees credits for space the packet never occupied.
TEST_F(SwitchBufferCbfcTest, CbfcManagedReleaseIsExactPerPacket) {
  auto& buf = makeBuffer(/*rsvdCells=*/2, /*maxSharedCells=*/0);
  buf.setPortCbfcManaged(0);
  auto& q = buf.queueTracker(0, 0);

  uint16_t a = 0;
  ASSERT_TRUE(buf.admitPacket(
      0,
      0,
      200,
      {.trafficClass = BufferTrafficClass::LOSSLESS, .heldCreditsOut = &a}));
  ASSERT_EQ(0u, a) << "packet A fits in the reserved region, holding nothing";
  uint16_t b = 0;
  ASSERT_TRUE(buf.admitPacket(
      0,
      0,
      200,
      {.trafficClass = BufferTrafficClass::LOSSLESS, .heldCreditsOut = &b}));
  ASSERT_GT(b, 0u) << "packet B reaches headroom";
  const uint32_t hdrmWithBoth = q.stats.usedHdrmCells;

  // An unmanaged queue would drain B's headroom cells here, making headroom
  // look empty while B is still in it.
  buf.removePacket(0, 0, 200, a);
  EXPECT_EQ(hdrmWithBoth, q.stats.usedHdrmCells)
      << "releasing a reserved-region packet freed headroom it never used";

  buf.removePacket(0, 0, 200, b);
  EXPECT_EQ(0u, q.stats.usedHdrmCells);
  EXPECT_EQ(0u, q.stats.usedCells);
}

// The defaults and caps documented on the buffer structs in
// switch_config.thrift: a cap of 0 grants nothing, and a packet that does not
// fit every tier it may use is dropped.
class SwitchBufferCapTest : public ::testing::Test {
 protected:
  // One ingress port, 0, whose only PG is `pg`, in a 100-cell buffer of
  // 100 B cells with no headroom pool.
  static packsim::thrift::SwitchBufferConfig makeConfig(
      const packsim::thrift::PriorityGroupBufferConfig& pg) {
    packsim::thrift::SwitchBufferConfig cfg;
    cfg.cellSizeBytes() = 100U;
    cfg.totalCells() = 100U;
    packsim::thrift::PortBufferConfig portCfg;
    portCfg.port() = 0U;
    portCfg.pgs() = {pg};
    cfg.ports()[0U] = portCfg;
    return cfg;
  }

  SimpleSwitchBuffer& build(const packsim::thrift::SwitchBufferConfig& cfg) {
    buf_.emplace(&simCtx_, cfg, /*useIngress=*/true);
    return *buf_;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  // Declared after `simCtx_`, so it is destroyed before the context it
  // borrows.
  std::optional<SimpleSwitchBuffer> buf_;
};

// The port's reserved cells, its shared cap and the headroom pool are opened
// up, so only the PG's own defaults (no reserved cells, no use of the port's
// reserved cells, no shared or headroom cells) can refuse the packet.
TEST_F(SwitchBufferCapTest, DefaultPgAdmitsNothing) {
  auto cfg = makeConfig(packsim::thrift::PriorityGroupBufferConfig{});
  cfg.ports()->at(0U).portRsvdCells() = 10U;
  cfg.ports()->at(0U).portMaxSharedCells() = kUnlimitedCells;
  cfg.headroomPoolCells() = 10U;
  auto& buf = build(cfg);

  EXPECT_FALSE(buf.admitPacket(0, 0, 100));
  EXPECT_EQ(1u, buf.queueStats(0, 0).droppedPkts);
  EXPECT_EQ(0u, buf.queueStats(0, 0).usedCells);
}

TEST_F(SwitchBufferCapTest, PortSharedCapOfZeroBlocksPgSharedCells) {
  packsim::thrift::PriorityGroupBufferConfig pg;
  pg.maxSharedCells() = kUnlimitedCells;
  auto& buf = build(makeConfig(pg));

  EXPECT_FALSE(buf.admitPacket(0, 0, 100))
      << "portMaxSharedCells = 0 must grant no shared cells";
}

TEST_F(SwitchBufferCapTest, UnlimitedPortSharedCapAdmitsIntoPgSharedCells) {
  packsim::thrift::PriorityGroupBufferConfig pg;
  pg.maxSharedCells() = kUnlimitedCells;
  auto cfg = makeConfig(pg);
  cfg.ports()->at(0U).portMaxSharedCells() = kUnlimitedCells;
  auto& buf = build(cfg);

  EXPECT_TRUE(buf.admitPacket(0, 0, 100));
  EXPECT_EQ(1u, buf.queueStats(0, 0).usedSharedCells);
}

TEST_F(SwitchBufferCapTest, PacketBeyondLosslessPgHeadroomIsDropped) {
  packsim::thrift::PriorityGroupBufferConfig pg;
  pg.maxHdrmCells() = 1U;
  // pfc = true would need a PFC-state subscriber on the buffer.
  pg.pfc() = false;
  auto cfg = makeConfig(pg);
  cfg.headroomPoolCells() = 10U;
  auto& buf = build(cfg);

  ASSERT_TRUE(buf.admitPacket(0, 0, 100));
  ASSERT_EQ(1u, buf.queueStats(0, 0).usedHdrmCells);

  EXPECT_FALSE(buf.admitPacket(0, 0, 100));
  EXPECT_EQ(1u, buf.queueStats(0, 0).droppedPkts);
  EXPECT_EQ(1u, buf.queueStats(0, 0).usedHdrmCells);
}

// Every byte-to-cell conversion divides by the cell size.
TEST_F(SwitchBufferCapTest, ZeroCellSizeIsRejected) {
  auto cfg = makeConfig(packsim::thrift::PriorityGroupBufferConfig{});
  cfg.cellSizeBytes() = 0U;

  EXPECT_THAT(
      [&] { build(cfg); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "SwitchBufferConfig (ingress buffer): cellSizeBytes is 0")));
}

TEST_F(
    SwitchBufferCapTest,
    HeadroomAndReservedCellsAboveTotalCellsAreRejected) {
  packsim::thrift::PriorityGroupBufferConfig pg;
  pg.rsvdCells() = 30U;
  auto cfg = makeConfig(pg);
  cfg.headroomPoolCells() = 50U;
  cfg.ports()->at(0U).portRsvdCells() = 30U;

  EXPECT_THAT(
      [&] { build(cfg); },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "SwitchBufferConfig (ingress buffer): headroomPoolCells 50 plus "
          "portRsvdCells 30 plus queue rsvdCells 30 is 110 cells, more than "
          "totalCells 100")));
}

// Summed in 32 bits, 2^32 - 1 headroom cells plus 1 reserved cell would wrap
// to 0 and fit.
TEST_F(SwitchBufferCapTest, ReservedCellSumIsNotTruncatedTo32Bits) {
  auto cfg = makeConfig(packsim::thrift::PriorityGroupBufferConfig{});
  cfg.headroomPoolCells() = UINT32_MAX;
  cfg.ports()->at(0U).portRsvdCells() = 1U;

  EXPECT_THAT(
      [&] { build(cfg); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr("is 4294967296 cells, more than totalCells 100")));
}

TEST_F(SwitchBufferCapTest, ReservedCellsFillingTheBufferLeaveNoSharedCells) {
  packsim::thrift::PriorityGroupBufferConfig pg;
  pg.rsvdCells() = 30U;
  auto cfg = makeConfig(pg);
  cfg.headroomPoolCells() = 40U;
  cfg.ports()->at(0U).portRsvdCells() = 30U;

  auto& buf = build(cfg);

  EXPECT_EQ(0u, buf.bufferStats().availSharedCells);
}

} // namespace packsim
