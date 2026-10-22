/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/eth-port.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include <folly/portability/GFlags.h>
#include <gtest/gtest.h>

#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/llr/llr.h"
#include "packsim/link-layer/oss_tests/lambda-port-observer.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

namespace {
// Helper whose constructor sets `pdes_threads=4` during member-init,
// before any test-fixture field that constructs a `Scheduler`. Pinned
// rather than left to the default, so the test runs on 4 threads whatever
// `useSmallDefaultThreadCount()` resolves the default to. Under
// ThreadSanitizer the instrumentation cost is superlinear in thread count,
// so a large count slows these tests until they time out. `=4` keeps
// multi-thread coverage (coroutine cross-thread migration, atomic ordering,
// race detection by tsan) while staying fast. Sibling LLR test fixtures
// (`llr-tx-test`, `llr-rx-test`, `llr-timers-test`) follow the same
// pattern.
struct PinPdesThreads {
  PinPdesThreads() {
    gflags::SetCommandLineOption("pdes_threads", "4");
  }
};

// Build the `thrift::LlrConfig` shape that tests in this file expect.
// Both `skipInitNegotiation` and `reInitOnDiscard` are pinned to `true`
// explicitly — even though the thrift schema currently defaults
// both to `true`, pinning here keeps these tests stable
// across schema edits. Other fields fall through to thrift defaults
// (`replayCtMax=0` = unlimited, both timers disabled).
thrift::LlrConfig makeTestLlrConfig(
    uint32_t ackByteThreshold,
    uint64_t txBufferMaxBytes = 512 * 1024,
    uint64_t retryTimeoutNs = 100000) {
  thrift::LlrConfig config;
  config.ackByteThreshold() = ackByteThreshold;
  config.txBufferMaxBytes() = txBufferMaxBytes;
  config.retryTimeoutNs() = retryTimeoutNs;
  config.skipInitNegotiation() = true;
  config.reInitOnDiscard() = true;
  return config;
}
} // namespace

// ============================================================================
// LLR EthernetPort Tests
// ============================================================================

class LlrEthernetPortTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scheduler_.buildNewEngine();

    port1_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
    port1_->setObserver(&observerPort1_);
    port2_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
    port2_->setObserver(&observerPort2_);

    // Set up minimal callbacks to prevent crashes
    observerPort1_.setOnPacketSentCallback(
        [](EthernetPort&, const PacketPtr&) {});
    observerPort1_.setOnPacketRcvdCallback(
        [](EthernetPort&, const PacketPtr&) {});
    observerPort2_.setOnPacketSentCallback(
        [](EthernetPort&, const PacketPtr&) {});
    observerPort2_.setOnPacketRcvdCallback(
        [](EthernetPort&, const PacketPtr&) {});

    observerPort1_.setOnPacketDroppedOnSendCallback(
        [](EthernetPort&,
           const PacketPtr&,
           uint8_t,
           packsim::switches::thrift::PacketDropReason) {});
    observerPort1_.setOnPacketDroppedOnReceiveCallback(
        [](EthernetPort&,
           const PacketPtr&,
           uint8_t,
           packsim::switches::thrift::PacketDropReason) {});
    observerPort2_.setOnPacketDroppedOnSendCallback(
        [](EthernetPort&,
           const PacketPtr&,
           uint8_t,
           packsim::switches::thrift::PacketDropReason) {});
    observerPort2_.setOnPacketDroppedOnReceiveCallback(
        [](EthernetPort&,
           const PacketPtr&,
           uint8_t,
           packsim::switches::thrift::PacketDropReason) {});

    // Create and attach channel (BER = 1.0 means no drops)
    packsim::thrift::FlexLinkDelayConfig delayCfg;
    delayCfg.delayNs() = 0;
    channel_ = std::make_unique<EthernetChannel>(delayCfg, 0, 1.0);
    port1_->attach(channel_.get());
    port2_->attach(channel_.get());
  }

  void TearDown() override {
    port1_.reset();
    port2_.reset();
    channel_.reset();
  }

  // Member init order matters: `flagSaver_` snapshots the gflag state
  // BEFORE `pinPdesThreads_` overwrites it, so the prior value is
  // restored when this fixture is destroyed. `pinPdesThreads_` then
  // sets `pdes_threads=4` BEFORE `scheduler_` field-init constructs
  // the PDES engine.
  folly::gflags::FlagSaver flagSaver_;
  PinPdesThreads pinPdesThreads_;
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  LambdaPortObserver observerPort1_;
  std::unique_ptr<EthernetPort> port1_;
  LambdaPortObserver observerPort2_;
  std::unique_ptr<EthernetPort> port2_;
  std::unique_ptr<EthernetChannel> channel_;
};

TEST_F(LlrEthernetPortTest, EnableLlrCreatesState) {
  EXPECT_FALSE(port1_->isLlrEnabled());

  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(1000, 512 * 1024, 100000), /*enableGlobalTimer=*/false);
  EXPECT_TRUE(port1_->isLlrEnabled());

  port1_->disableLlr();
  EXPECT_FALSE(port1_->isLlrEnabled());
}

TEST_F(LlrEthernetPortTest, TestInjectLlrAckWithEnabledLlrRemovesPackets) {
  // High threshold to prevent auto-ACKs, disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(16384, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);
  port2_->enableLlr(
      makeTestLlrConfig(16384, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  // Send packets
  auto pkt1 = createPacket<PacketType::IPV6>(1000);
  auto pkt2 = createPacket<PacketType::IPV6>(1000);
  port1_->send(pkt1);
  port1_->send(pkt2);
  scheduler_.run(Time::fromUs(100));

  EXPECT_EQ(packetsReceived, 2);
  ASSERT_EQ(port1_->llr()->outstandingSeq(), 2u);

  // The frames carry seqs 0 and 1, so an ACK of seq 1 covers both.
  port1_->llr()->rxCtlos(LlrCtlosType::LLR_ACK, 1, /*initData=*/0);

  EXPECT_EQ(port1_->llr()->outstandingSeq(), 0u);
}

TEST_F(
    LlrEthernetPortTest,
    TestInjectLlrAckDoesNotReFireTxCallbackForAckedPackets) {
  // The port observer's `onPacketSent` fires once per frame the layer above
  // sent, on its first transmission: never for a replay, nor for a CtlOS or
  // MAC control frame the port generates itself (see
  // `IPortObserver::onPacketSent`). An inbound LLR_ACK that advances
  // `acked_seq` and drains the replay buffer MUST NOT re-fire it, and ACKs
  // don't trigger replays.
  port1_->enableLlr(
      makeTestLlrConfig(1000, 512 * 1024, 100000), /*enableGlobalTimer=*/false);

  int txCallbackCount = 0;
  observerPort1_.setOnPacketSentCallback(
      [&txCallbackCount](EthernetPort&, const PacketPtr&) {
        ++txCallbackCount;
      });

  auto pkt1 = createPacket<PacketType::IPV6>(1000);
  auto pkt2 = createPacket<PacketType::IPV6>(1000);

  port1_->send(pkt1);
  port1_->send(pkt2);
  scheduler_.run(Time::fromUs(100));
  ASSERT_EQ(txCallbackCount, 2);

  // Inject LLR_ACK and confirm txCallback is NOT re-fired.
  port1_->llr()->rxCtlos(LlrCtlosType::LLR_ACK, 1, /*initData=*/0);
  scheduler_.run(Time::fromUs(100));
  EXPECT_EQ(txCallbackCount, 2);
}

TEST_F(LlrEthernetPortTest, LlrDisabledAfterEnableStillAllowsTransmission) {
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(1000, 512 * 1024, 100000), /*enableGlobalTimer=*/false);
  port1_->disableLlr();

  bool packetReceived = false;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetReceived](EthernetPort&, const PacketPtr&) {
        packetReceived = true;
      });

  auto pkt = createPacket<PacketType::IPV6>(1000);
  port1_->send(pkt);
  scheduler_.run(Time::fromUs(100));

  EXPECT_TRUE(packetReceived);
}

TEST_F(LlrEthernetPortTest, MultiplePacketsTransmitWithLlrEnabled) {
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(1000, 512 * 1024, 100000), /*enableGlobalTimer=*/false);
  port2_->enableLlr(
      makeTestLlrConfig(1000, 512 * 1024, 100000), /*enableGlobalTimer=*/false);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  constexpr int kNumPackets = 10;
  for (int i = 0; i < kNumPackets; ++i) {
    auto pkt = createPacket<PacketType::IPV6>(1000);
    port1_->send(pkt);
  }
  scheduler_.run(Time::fromUs(100));

  EXPECT_EQ(packetsReceived, kNumPackets);
}

TEST_F(LlrEthernetPortTest, LlrWithZeroAckThresholdStillWorks) {
  // Edge case: a zero threshold falls back to the spec minimum CtlOS spacing
  // (`kCtlosTargetSpacingMinBytes`, 400 B)
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(0, 512 * 1024, 100000), /*enableGlobalTimer=*/false);
  port2_->enableLlr(
      makeTestLlrConfig(0, 512 * 1024, 100000), /*enableGlobalTimer=*/false);

  bool packetReceived = false;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetReceived](EthernetPort&, const PacketPtr&) {
        packetReceived = true;
      });

  auto pkt = createPacket<PacketType::IPV6>(1000);
  port1_->send(pkt);
  scheduler_.run(Time::fromUs(100));

  EXPECT_TRUE(packetReceived);
}

TEST_F(LlrEthernetPortTest, LlrWithSmallTxBufferBlocksOversizedPackets) {
  // TX buffer backpressure now blocks packets when buffer is full
  constexpr uint64_t kSmallBuffer = 100; // Very small buffer
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(1000, kSmallBuffer, 100000),
      /*enableGlobalTimer=*/false);

  bool packetReceived = false;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetReceived](EthernetPort&, const PacketPtr&) {
        packetReceived = true;
      });

  // Large packet that won't fit in LLR buffer - should be blocked by
  // backpressure
  auto pkt = createPacket<PacketType::IPV6>(5000);
  port1_->send(pkt);
  scheduler_.run(Time::fromUs(100));

  // Packet should be blocked (not transmitted) due to buffer backpressure
  EXPECT_FALSE(packetReceived);
}

TEST_F(LlrEthernetPortTest, TxBufferBackpressureBlocksSending) {
  // Enable LLR with a small buffer that can only hold 1-2 packets
  // and a high ACK threshold so ACKs are not automatically sent
  constexpr uint64_t kSmallBuffer = 2000; // Can hold ~1-2 1000-byte packets
  constexpr uint64_t kHighAckThreshold =
      16384; // Spec max — suppresses byte-trigger auto-ACKs in tests that send
             // fewer bytes than this
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(kHighAckThreshold, kSmallBuffer, 100000),
      /*enableGlobalTimer=*/false);
  port2_->enableLlr(
      makeTestLlrConfig(kHighAckThreshold, kSmallBuffer, 100000),
      /*enableGlobalTimer=*/false);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  // Send multiple packets - some should be blocked due to buffer backpressure
  constexpr int kNumPackets = 5;
  for (int i = 0; i < kNumPackets; ++i) {
    auto pkt = createPacket<PacketType::IPV6>(1000);
    port1_->send(pkt);
  }
  scheduler_.run(Time::fromUs(100));

  // Due to backpressure, not all packets should be received
  // (exact number depends on buffer size and packet overhead)
  EXPECT_LT(packetsReceived, kNumPackets);
  EXPECT_GT(packetsReceived, 0);
}

TEST_F(LlrEthernetPortTest, TxBufferBackpressureResumesSendingAfterAck) {
  // Enable LLR with a small buffer
  constexpr uint64_t kSmallBuffer = 1500; // Can hold ~1 packet
  constexpr uint64_t kHighAckThreshold =
      16384; // Spec max — suppresses byte-trigger auto-ACKs in tests that send
             // fewer bytes than this
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(kHighAckThreshold, kSmallBuffer, 100000),
      /*enableGlobalTimer=*/false);
  port2_->enableLlr(
      makeTestLlrConfig(kHighAckThreshold, kSmallBuffer, 100000),
      /*enableGlobalTimer=*/false);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  // Send multiple packets - buffer will fill up
  constexpr int kNumPackets = 3;
  for (int i = 0; i < kNumPackets; ++i) {
    auto pkt = createPacket<PacketType::IPV6>(1000);
    port1_->send(pkt);
  }
  scheduler_.run(Time::fromUs(100));
  int receivedBeforeAck = packetsReceived;

  // Process ACK to free buffer space - this should resume sending
  port1_->llr()->rxCtlos(LlrCtlosType::LLR_ACK, 0, /*initData=*/0);
  scheduler_.run(Time::fromUs(100));

  // After ACK, more packets should be sent/received
  EXPECT_GT(packetsReceived, receivedBeforeAck);
}

TEST_F(LlrEthernetPortTest, OutOfOrderPacketIsDropped) {
  constexpr uint64_t kAckThreshold =
      16384; // Spec max — no auto-ACKs at test packet volume
  // Disable global timer for testing
  port2_->enableLlr(
      makeTestLlrConfig(kAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  // With INIT negotiation skipped, port2 expects seq 0 first, so a frame
  // carrying seq 1 arrives with seq 0 missing.
  auto pkt = createPacket<PacketType::IPV6>(1000);
  llr::Llr::stampLlrHeader(pkt, llr::FrameType::LLR_ELIGIBLE, /*seq=*/1);
  port2_->receive(std::move(pkt));
  scheduler_.run(Time::fromUs(100));

  EXPECT_EQ(packetsReceived, 0);
  EXPECT_EQ(port2_->llr()->stats().rxMissingSeq, 1u);
}

TEST_F(LlrEthernetPortTest, RetryPacketDoesNotInvokeTxCallbackOnTransmission) {
  constexpr uint64_t kHighAckThreshold =
      16384; // Spec max — suppresses byte-trigger auto-ACKs in tests that send
             // fewer bytes than this
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(kHighAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);
  port2_->enableLlr(
      makeTestLlrConfig(kHighAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);

  int txCallbackCount = 0;
  observerPort1_.setOnPacketSentCallback(
      [&txCallbackCount](EthernetPort&, const PacketPtr&) {
        ++txCallbackCount;
      });

  // Send initial packet - should trigger txCallback
  auto pkt = createPacket<PacketType::IPV6>(1000);
  port1_->send(pkt);
  scheduler_.run(Time::fromUs(100));
  // Pin pre-replay state — frame admitted, txCallback fired once, no
  // replays yet. Without these asserts a regression that drops the
  // frame on admission would trivially pass the final EXPECT_EQ below
  // (callback count would still be 1, because no replay would have
  // happened).
  ASSERT_EQ(txCallbackCount, 1);
  ASSERT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  ASSERT_EQ(port1_->llr()->outstandingSeq(), 1u);
  ASSERT_EQ(port1_->llr()->stats().txReplay, 0u);

  // Advance past replay_timer max (retryTimeoutNs=100us above) so the
  // timer naturally expires with the held frame still unacked.
  // `txRunAdvance` then transitions to REPLAY and the scheduler
  // re-emits the held frame via `pullNextReplayFrame()`. The
  // txCallback is invoked only for fresh frames (not replays).
  scheduler_.run(Time::fromUs(150));

  // Confirm a replay actually happened (stat advances) AND the callback
  // was NOT re-fired.
  EXPECT_GE(port1_->llr()->stats().txReplay, 1u);
  EXPECT_EQ(txCallbackCount, 1);
}

TEST_F(LlrEthernetPortTest, LowAckThresholdDeliversEveryPacket) {
  // Enable LLR with very low threshold to trigger ACKs frequently
  constexpr uint64_t kLowAckThreshold = 100; // ACK after 100 bytes
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(kLowAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);
  port2_->enableLlr(
      makeTestLlrConfig(kLowAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  // Send multiple packets rapidly. With a 100-byte threshold each frame
  // triggers its own byte-threshold LLR_ACK post on port2; delivery must be
  // unaffected.
  for (int i = 0; i < 5; ++i) {
    auto pkt = createPacket<PacketType::IPV6>(500);
    port1_->send(pkt);
  }
  scheduler_.run(Time::fromUs(100));

  // All packets should be received (ACK mechanism should work)
  EXPECT_EQ(packetsReceived, 5);
  EXPECT_TRUE(port2_->isLlrEnabled());
  EXPECT_EQ(port2_->llr()->stats().txAckByteTrigger, 5u);
}

TEST_F(LlrEthernetPortTest, TestInjectLlrAckWithEmptyBuffer) {
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(1000, 512 * 1024, 100000), /*enableGlobalTimer=*/false);

  port1_->llr()->rxCtlos(LlrCtlosType::LLR_ACK, 5, /*initData=*/0);
  EXPECT_TRUE(port1_->isLlrEnabled());
}

TEST_F(LlrEthernetPortTest, TestInjectLlrAckWithOldSeqNum) {
  // Disable global timer for testing
  port1_->enableLlr(
      makeTestLlrConfig(1000, 512 * 1024, 100000), /*enableGlobalTimer=*/false);

  auto pkt = createPacket<PacketType::IPV6>(1000);
  port1_->send(pkt);
  scheduler_.run(Time::fromUs(100));

  port1_->llr()->rxCtlos(LlrCtlosType::LLR_ACK, 10, /*initData=*/0);
  scheduler_.run(Time::fromUs(100));

  port1_->llr()->rxCtlos(LlrCtlosType::LLR_ACK, 5, /*initData=*/0);
  EXPECT_TRUE(port1_->isLlrEnabled());
}

// Outbound ACK/NACK emission goes through the spec state machines,
// not through standalone sender APIs. Coverage lives in:
//   - llr-rx-test.cpp::LlrRxTest::LlrAckAdvancesBufferAndAckedSeq
//       — inbound LLR_ACK behavior.
//   - llr-rx-test.cpp::LlrRxTest::MissingFrameDropsAndPulsesNack
//       — AN-SM transition that drives outbound LLR_NACK.
//   - llr-tx-test.cpp::LlrTxTest::NackReceivedTransitionsThroughReplay
//       — TX-side response to a received NACK.

// ============================================================================
// EthernetPort Tests
// ============================================================================

TEST(EthernetPortTest, BerDroppedPacketIncrementsStatsAndCallsCallback) {
  Scheduler scheduler;
  SimContext simCtx{&scheduler, 0};

  LambdaPortObserver observerPort1;
  EthernetPort port1(&simCtx, "100Gbps");
  port1.setObserver(&observerPort1);
  LambdaPortObserver observerPort2;
  EthernetPort port2(&simCtx, "100Gbps");
  port2.setObserver(&observerPort2);

  observerPort1.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort1.setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});

  // BER drops happen on receive and do NOT call sendDrop
  // (since the drop didn't happen in a switch buffer)
  observerPort1
      .setOnPacketDroppedOnSendCallback( // NOLINTNEXTLINE(clang-diagnostic-missing-noreturn)
          [](EthernetPort&,
             const PacketPtr&,
             uint8_t,
             packsim::switches::thrift::PacketDropReason) {
            throw std::runtime_error(
                "Send drop callback should not be called for BER drops");
          });
  observerPort2
      .setOnPacketDroppedOnSendCallback( // NOLINTNEXTLINE(clang-diagnostic-missing-noreturn)
          [](EthernetPort&,
             const PacketPtr&,
             uint8_t,
             packsim::switches::thrift::PacketDropReason) {
            throw std::runtime_error(
                "Send drop callback should not be called for BER drops");
          });

  // BER drops are reported to the receiving port's observer.
  std::vector<packsim::switches::thrift::PacketDropReason> receiveDropReasons;
  observerPort2.setOnPacketDroppedOnReceiveCallback(
      [&receiveDropReasons](
          EthernetPort&,
          const PacketPtr&,
          uint8_t,
          packsim::switches::thrift::PacketDropReason reason) {
        receiveDropReasons.emplace_back(reason);
      });

  packsim::thrift::FlexLinkDelayConfig delayCfg;
  delayCfg.delayNs() = 0;

  constexpr double log10Ber = -1.0;
  EthernetChannel channel(delayCfg, 0, log10Ber);
  port1.attach(&channel);
  port2.attach(&channel);

  const auto pkt = createPacket<PacketType::IPV6>(1000);
  // BER drops are recorded on receiver (port2), not sender (port1)
  auto initialDropPkts = port2.getPortStats(0).dropPkts;
  auto initialDropBytes = port2.getPortStats(0).dropBytes;

  simCtx.random.setSeed(12345);

  constexpr int kNumPackets = 100;
  bool packetDropped = false;
  for (int i = 0; i < kNumPackets; ++i) {
    port1.send(pkt);
    scheduler.run();

    // Check receiver's stats for BER drops
    const auto stats = port2.getPortStats(0);
    if (stats.dropPkts > initialDropPkts) {
      packetDropped = true;
      EXPECT_EQ(stats.dropPkts, initialDropPkts + 1);
      EXPECT_EQ(stats.dropBytes, initialDropBytes + pkt->getSize());
      break;
    }

    initialDropPkts = stats.dropPkts;
    initialDropBytes = stats.dropBytes;
  }

  EXPECT_TRUE(packetDropped);
  const std::vector<packsim::switches::thrift::PacketDropReason>
      expectedReceiveDropReasons{
          packsim::switches::thrift::PacketDropReason::BER_ERROR};
  EXPECT_EQ(receiveDropReasons, expectedReceiveDropReasons);
}

// A control ordered set has no CoS to charge and no observer expecting it, so
// dropping one must touch neither. This covers the CBFC CF_Update as well as
// the LLR kinds: charging CF_Update to queue 0 was only ever a side effect of
// `getCosFor` defaulting there for a non-IP frame.
class CtlosDropTest : public ::testing::Test {
 protected:
  // `log10Ber` is per test: the BER stage runs before the link-down stage, so
  // a link-down case has to disable BER or it never reaches the code it means
  // to exercise. Any positive value disables BER drops.
  void build(double log10Ber) {
    scheduler_.buildNewEngine();
    peer_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
    port_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
    peer_->setObserver(&observerPeer_);
    port_->setObserver(&observerPort_);
    for (auto* port : {peer_.get(), port_.get()}) {
      std::vector<packsim::thrift::PortQueueConfig> queueCfg(8);
      port->setPortQueueConfig(queueCfg);
    }
    observerPort_.setOnPacketDroppedOnReceiveCallback(
        [this](
            EthernetPort&,
            const PacketPtr&,
            uint8_t,
            packsim::switches::thrift::PacketDropReason) { ++dropCallbacks_; });

    packsim::thrift::FlexLinkDelayConfig delayCfg;
    delayCfg.delayNs() = 0;
    channel_ = std::make_unique<EthernetChannel>(
        delayCfg, /*flapDelayNs=*/0, log10Ber);
    peer_->attach(channel_.get());
    port_->attach(channel_.get());
  }

  uint64_t totalDropPkts() const {
    uint64_t total = 0;
    for (uint8_t cos = 0; cos < 8; ++cos) {
      total += port_->getPortStats(cos).dropPkts;
    }
    return total;
  }

  void deliver(PacketPtr pkt) {
    port_->receive(pkt);
    scheduler_.run();
  }

  // `pGood` is (1 - ber)^bits, so at ber 0.794 an 8-byte ordered set survives
  // with probability ~1e-44. The drop is deterministic for test purposes.
  static constexpr double kAlwaysDrop = -0.1;
  static constexpr double kNeverDrop = 1.0;

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<EthernetChannel> channel_;
  // Declared before the ports so they are destroyed after them: a port holds
  // a non-owning pointer to its observer.
  LambdaPortObserver observerPeer_;
  LambdaPortObserver observerPort_;
  std::unique_ptr<EthernetPort> peer_;
  std::unique_ptr<EthernetPort> port_;
  uint64_t dropCallbacks_{0};
};

TEST_F(CtlosDropTest, BitErrorDropOfAnOrderedSetIsSilent) {
  build(kAlwaysDrop);

  deliver(createPacket<PacketType::CTLOS_LLR>(0));
  deliver(createPacket<PacketType::CTLOS_CF_UPDATE>(0));

  EXPECT_EQ(totalDropPkts(), 0u);
  EXPECT_EQ(dropCallbacks_, 0u);
}

TEST_F(CtlosDropTest, BitErrorDropOfADataFrameIsStillCounted) {
  // Discrimination: the same fixture must count an ordinary frame, or the
  // test above would pass with drops disabled entirely.
  build(kAlwaysDrop);

  deliver(createPacket<PacketType::IPV6>(1000));

  EXPECT_EQ(totalDropPkts(), 1u);
  EXPECT_EQ(dropCallbacks_, 1u);
}

TEST_F(CtlosDropTest, LinkDownDropOfAnOrderedSetIsSilent) {
  build(kNeverDrop);
  port_->setChannelState(ChannelState::DOWN);
  // The transition itself drains the transmit queues, which records its own
  // drops; only what the ordered sets add is under test here.
  const uint64_t baseline = totalDropPkts();
  const uint64_t baselineCallbacks = dropCallbacks_;

  deliver(createPacket<PacketType::CTLOS_LLR>(0));
  deliver(createPacket<PacketType::CTLOS_CF_UPDATE>(0));

  EXPECT_EQ(totalDropPkts(), baseline);
  EXPECT_EQ(dropCallbacks_, baselineCallbacks);
}

TEST_F(CtlosDropTest, LinkDownDropOfADataFrameIsStillCounted) {
  build(kNeverDrop);
  port_->setChannelState(ChannelState::DOWN);
  const uint64_t baseline = totalDropPkts();
  const uint64_t baselineCallbacks = dropCallbacks_;

  deliver(createPacket<PacketType::IPV6>(1000));

  EXPECT_EQ(totalDropPkts(), baseline + 1);
  EXPECT_EQ(dropCallbacks_, baselineCallbacks + 1);
}

TEST(EthernetPortTest, ChannelDownDropsPacketsAndCallsReceiveCallback) {
  Scheduler scheduler;
  scheduler.buildNewEngine();
  SimContext simCtx{&scheduler, 0};

  LambdaPortObserver observerPort1;
  EthernetPort port1(&simCtx, "100Gbps");
  port1.setObserver(&observerPort1);
  LambdaPortObserver observerPort2;
  EthernetPort port2(&simCtx, "100Gbps");
  port2.setObserver(&observerPort2);

  observerPort1.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort1.setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});

  observerPort1.setOnPacketDroppedOnSendCallback(
      [](EthernetPort&,
         const PacketPtr&,
         uint8_t,
         packsim::switches::thrift::PacketDropReason) {});

  bool receiveDropCalled = false;
  observerPort2.setOnPacketDroppedOnReceiveCallback(
      [&receiveDropCalled](
          EthernetPort&,
          const PacketPtr&,
          uint8_t,
          packsim::switches::thrift::PacketDropReason) {
        receiveDropCalled = true;
      });

  packsim::thrift::FlexLinkDelayConfig delayCfg;
  delayCfg.delayNs() = 0;

  EthernetChannel channel(delayCfg, 0, 1.0);
  port1.attach(&channel);
  port2.attach(&channel);

  auto pkt = createPacket<PacketType::IPV6>(1000);
  auto initialDropPkts = port2.getPortStats(0).dropPkts;
  auto initialDropBytes = port2.getPortStats(0).dropBytes;

  // Set channel state to DOWN on receiver before packet arrives
  port2.setChannelState(ChannelState::DOWN);

  // Send packet - should be dropped on receive
  port1.send(pkt);
  scheduler.run();

  // Assert: Packet was dropped on receiver
  auto stats = port2.getPortStats(0);
  EXPECT_EQ(stats.dropPkts, initialDropPkts + 1);
  EXPECT_EQ(stats.dropBytes, initialDropBytes + pkt->getSize());
  EXPECT_TRUE(receiveDropCalled);
}

TEST(EthernetPortTest, ChannelUpAfterDownAllowsTransmission) {
  Scheduler scheduler;
  scheduler.buildNewEngine();
  SimContext simCtx{&scheduler, 0};

  LambdaPortObserver observerPort1;
  EthernetPort port1(&simCtx, "100Gbps");
  port1.setObserver(&observerPort1);
  LambdaPortObserver observerPort2;
  EthernetPort port2(&simCtx, "100Gbps");
  port2.setObserver(&observerPort2);

  bool receiveCallbackCalled = false;
  observerPort1.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort1.setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketRcvdCallback(
      [&receiveCallbackCalled](EthernetPort&, const PacketPtr&) {
        receiveCallbackCalled = true;
      });

  observerPort1.setOnPacketDroppedOnSendCallback(
      [](EthernetPort&,
         const PacketPtr&,
         uint8_t,
         packsim::switches::thrift::PacketDropReason) {});
  observerPort2
      .setOnPacketDroppedOnReceiveCallback( // NOLINTNEXTLINE(clang-diagnostic-missing-noreturn)
          [](EthernetPort&,
             const PacketPtr&,
             uint8_t,
             packsim::switches::thrift::PacketDropReason) {
            throw std::runtime_error(
                "Receive drop callback should not be called when channel is UP");
          });

  packsim::thrift::FlexLinkDelayConfig delayCfg;
  delayCfg.delayNs() = 0;

  EthernetChannel channel(delayCfg, 0, 1.0);
  port1.attach(&channel);
  port2.attach(&channel);

  auto pkt = createPacket<PacketType::IPV6>(1000);

  // Initially set channel DOWN, then UP
  port2.setChannelState(ChannelState::DOWN);
  port2.setChannelState(ChannelState::UP);

  // Send packet - should be successfully delivered
  port1.send(pkt);
  scheduler.run();

  // Assert: Packet was received successfully
  EXPECT_TRUE(receiveCallbackCalled);
}

TEST(EthernetPortTest, ChannelDownOnSendDropsPacket) {
  Scheduler scheduler;
  scheduler.buildNewEngine();
  SimContext simCtx{&scheduler, 0};

  LambdaPortObserver observerPort1;
  EthernetPort port1(&simCtx, "100Gbps");
  port1.setObserver(&observerPort1);
  LambdaPortObserver observerPort2;
  EthernetPort port2(&simCtx, "100Gbps");
  port2.setObserver(&observerPort2);

  observerPort1.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort1.setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});

  bool sendDropCalled = false;
  observerPort1.setOnPacketDroppedOnSendCallback(
      [&sendDropCalled](
          EthernetPort&,
          const PacketPtr&,
          uint8_t,
          packsim::switches::thrift::PacketDropReason) {
        sendDropCalled = true;
      });
  observerPort1.setOnPacketDroppedOnReceiveCallback(
      [](EthernetPort&,
         const PacketPtr&,
         uint8_t,
         packsim::switches::thrift::PacketDropReason) {});
  observerPort2.setOnPacketDroppedOnSendCallback(
      [](EthernetPort&,
         const PacketPtr&,
         uint8_t,
         packsim::switches::thrift::PacketDropReason) {});
  observerPort2.setOnPacketDroppedOnReceiveCallback(
      [](EthernetPort&,
         const PacketPtr&,
         uint8_t,
         packsim::switches::thrift::PacketDropReason) {});

  packsim::thrift::FlexLinkDelayConfig delayCfg;
  delayCfg.delayNs() = 0;

  EthernetChannel channel(delayCfg, 0, 1.0);
  port1.attach(&channel);
  port2.attach(&channel);

  auto pkt = createPacket<PacketType::IPV6>(1000);

  // Set channel state to DOWN on sender
  port1.setChannelState(ChannelState::DOWN);

  // Send packet - should be dropped on send
  port1.send(pkt);
  scheduler.run();

  // Assert: Send drop callback was called
  EXPECT_TRUE(sendDropCalled);
}

// ============================================================================
// TX-queue space tests
// ============================================================================

// Two ports on a zero-delay channel, with port1_'s queues marked full past
// kMaxQSizeThresholdBytes and free again at kMinQSizeThresholdBytes once a
// test applies filterCfg_. Each test subscribes and configures in its own
// order, then attaches the ports.
class EthernetPortTxQueueSpaceTest : public ::testing::Test {
 protected:
  static constexpr uint32_t kMinQSizeThresholdBytes = 1000;
  static constexpr uint32_t kMaxQSizeThresholdBytes = 2000;
  static constexpr uint32_t kPktSizeBytes = 1000;
  // Long enough to send every queued frame: five 1000 B frames take well under
  // 1 us at 100 Gbps.
  static constexpr uint64_t kDrainDeadlineUs = 100;

  void SetUp() override {
    scheduler_.buildNewEngine();
    port1_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
    port1_->setObserver(&observerPort1_);
    port2_ = std::make_unique<EthernetPort>(&simCtx_, "100Gbps");
    port2_->setObserver(&observerPort2_);
    observerPort1_.setOnPacketSentCallback(
        [](EthernetPort&, const PacketPtr&) {});
    observerPort1_.setOnPacketDroppedOnSendCallback(
        [](EthernetPort&,
           const PacketPtr&,
           uint8_t,
           packsim::switches::thrift::PacketDropReason) {});
    observerPort2_.setOnPacketRcvdCallback(
        [](EthernetPort&, const PacketPtr&) {});
    filterCfg_.minQSizeThreshold() = kMinQSizeThresholdBytes;
    filterCfg_.maxQSizeThreshold() = kMaxQSizeThresholdBytes;
  }

  // Attaches both ports to a zero-delay channel that drops nothing.
  void attachPorts() {
    packsim::thrift::FlexLinkDelayConfig delayCfg;
    delayCfg.delayNs() = 0;
    channel_ = std::make_unique<EthernetChannel>(
        delayCfg, /*flapDelayNs=*/0, /*log10Ber=*/1.0);
    port1_->attach(channel_.get());
    port2_->attach(channel_.get());
  }

  // Without running the scheduler, queues more than kMaxQSizeThresholdBytes
  // on port1_'s queue 0.
  void fillQueueZeroPastItsMaxThreshold() {
    constexpr int kNumPkts = 5;
    for (int i = 0; i < kNumPkts; ++i) {
      port1_->send(
          createPacket<PacketType::IPV6>(/*payloadSize=*/kPktSizeBytes));
    }
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, /*seed=*/0};
  LambdaPortObserver observerPort1_;
  std::unique_ptr<EthernetPort> port1_;
  LambdaPortObserver observerPort2_;
  std::unique_ptr<EthernetPort> port2_;
  std::unique_ptr<EthernetChannel> channel_;
  packsim::thrift::QueueFilterConfig filterCfg_;
};

TEST_F(EthernetPortTxQueueSpaceTest, LinkDownDrainRestoresTxQueueAvailability) {
  int numSpaceEvents = 0;
  port1_->setOnTxQueueSpaceAvailableCallback(
      [&numSpaceEvents](uint8_t) { ++numSpaceEvents; });
  port1_->setQueueFilterConfig(filterCfg_);
  attachPorts();
  fillQueueZeroPastItsMaxThreshold();
  ASSERT_FALSE(port1_->isTxQueueAvailable(/*cos=*/0));

  port1_->setChannelState(ChannelState::DOWN);

  EXPECT_TRUE(port1_->isTxQueueAvailable(/*cos=*/0));
  EXPECT_EQ(numSpaceEvents, 1);
}

TEST_F(
    EthernetPortTxQueueSpaceTest,
    QueueConfiguredBeforeSubscribingIsMarkedFullPastItsMaxThreshold) {
  // The endpoint's order: queues first, subscriber after.
  port1_->setQueueFilterConfig(filterCfg_);
  port1_->setOnTxQueueSpaceAvailableCallback([](uint8_t) {});
  attachPorts();

  fillQueueZeroPastItsMaxThreshold();

  EXPECT_FALSE(port1_->isTxQueueAvailable(/*cos=*/0));
}

TEST_F(
    EthernetPortTxQueueSpaceTest,
    SubscriberSetAfterTheQueueConfigHearsTheQueueDrain) {
  port1_->setQueueFilterConfig(filterCfg_);
  int numSpaceEvents = 0;
  port1_->setOnTxQueueSpaceAvailableCallback(
      [&numSpaceEvents](uint8_t) { ++numSpaceEvents; });
  attachPorts();
  fillQueueZeroPastItsMaxThreshold();

  scheduler_.run(Time::fromUs(kDrainDeadlineUs));

  EXPECT_TRUE(port1_->isTxQueueAvailable(/*cos=*/0));
  EXPECT_EQ(numSpaceEvents, 1);
}

TEST(EthernetPortTest, BerDisabledNeverDropsPackets) {
  Scheduler scheduler;
  scheduler.buildNewEngine();
  SimContext simCtx{&scheduler, 0};

  LambdaPortObserver observerPort1;
  EthernetPort port1(&simCtx, "100Gbps");
  port1.setObserver(&observerPort1);
  LambdaPortObserver observerPort2;
  EthernetPort port2(&simCtx, "100Gbps");
  port2.setObserver(&observerPort2);

  observerPort1.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort1.setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
  observerPort2.setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});

  observerPort1.setOnPacketDroppedOnSendCallback(
      [](EthernetPort&,
         const PacketPtr&,
         uint8_t,
         packsim::switches::thrift::PacketDropReason) {});
  observerPort1.setOnPacketDroppedOnReceiveCallback(
      [](EthernetPort&,
         const PacketPtr&,
         uint8_t,
         packsim::switches::thrift::PacketDropReason) {});
  observerPort2.setOnPacketDroppedOnSendCallback(
      [](EthernetPort&,
         const PacketPtr&,
         uint8_t,
         packsim::switches::thrift::PacketDropReason) {});
  observerPort2
      .setOnPacketDroppedOnReceiveCallback( // NOLINTNEXTLINE(clang-diagnostic-missing-noreturn)
          [](EthernetPort&,
             const PacketPtr&,
             uint8_t,
             packsim::switches::thrift::PacketDropReason) {
            throw std::runtime_error(
                "Receive drop callback should not be called when BER is disabled");
          });

  packsim::thrift::FlexLinkDelayConfig delayCfg;
  delayCfg.delayNs() = 0;

  // BER = 1.0 (disabled)
  constexpr double log10Ber = 1.0;
  EthernetChannel channel(delayCfg, 0, log10Ber);
  port1.attach(&channel);
  port2.attach(&channel);

  auto pkt = createPacket<PacketType::IPV6>(1000);
  auto initialDropPkts = port2.getPortStats(0).dropPkts;

  // Send 100 packets
  for (int i = 0; i < 100; ++i) {
    port1.send(pkt);
    scheduler.run();
  }

  // Assert: No packets should be dropped
  auto stats = port2.getPortStats(0);
  EXPECT_EQ(stats.dropPkts, initialDropPkts);
}

// Two ports on one channel, with nothing but bit errors to lose frames to.
class RxBerOverrideTest : public ::testing::Test {
 protected:
  // The channel applies `channelLog10Ber`, error-free unless a fixture below
  // says otherwise.
  explicit RxBerOverrideTest(
      const double channelLog10Ber = kNoBitErrorsLog10Ber)
      : channel_{makeDelayConfig(), 0, channelLog10Ber} {}

  // A log10 bit error rate high enough to corrupt every frame sent here.
  static constexpr double kCorruptingLog10Ber = -1.0;
  // Bit errors off: a bit error rate of 10.
  static constexpr double kNoBitErrorsLog10Ber = 1.0;
  // Frames sent in each direction by sendFrames().
  static constexpr uint64_t kNumFrames = 10;

  void SetUp() override {
    for (auto* observer : {&observer1_, &observer2_}) {
      observer->setOnPacketSentCallback([](EthernetPort&, const PacketPtr&) {});
      observer->setOnPacketRcvdCallback([](EthernetPort&, const PacketPtr&) {});
      observer->setOnPacketDroppedOnReceiveCallback(
          [](EthernetPort&,
             const PacketPtr&,
             uint8_t,
             packsim::switches::thrift::PacketDropReason) {});
    }
    port1_.setObserver(&observer1_);
    port2_.setObserver(&observer2_);
    port1_.attach(&channel_);
    port2_.attach(&channel_);
  }

  // Send kNumFrames frames from `from` to the other port, running each to
  // completion.
  void sendFrames(EthernetPort& from) {
    for (uint64_t i = 0; i < kNumFrames; ++i) {
      from.send(createPacket<PacketType::IPV6>(1000));
      scheduler_.run();
    }
  }

  static packsim::thrift::FlexLinkDelayConfig makeDelayConfig() {
    packsim::thrift::FlexLinkDelayConfig delayCfg;
    delayCfg.delayNs() = 0;
    return delayCfg;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  LambdaPortObserver observer1_;
  LambdaPortObserver observer2_;
  EthernetPort port1_{&simCtx_, "100Gbps"};
  EthernetPort port2_{&simCtx_, "100Gbps"};
  EthernetChannel channel_;
};

// The same, on a channel whose own rate corrupts every frame.
class RxBerOverrideOnACorruptingChannelTest : public RxBerOverrideTest {
 protected:
  RxBerOverrideOnACorruptingChannelTest()
      : RxBerOverrideTest(kCorruptingLog10Ber) {}
};

// The override is per port: frames into the overriding port are corrupted,
// frames out of it cross the channel's error-free direction untouched.
TEST_F(RxBerOverrideTest, AnOverrideAppliesOnlyToTheFramesItsPortReceives) {
  port2_.setRxLog10BerOverride(kCorruptingLog10Ber);

  sendFrames(port1_);
  sendFrames(port2_);

  EXPECT_EQ(kNumFrames, port2_.getPortStats(0).dropPkts);
  EXPECT_EQ(0u, port1_.getPortStats(0).dropPkts);
}

TEST_F(
    RxBerOverrideOnACorruptingChannelTest,
    ClearingTheOverrideRestoresTheChannelBitErrorRate) {
  port2_.setRxLog10BerOverride(kNoBitErrorsLog10Ber);
  port2_.setRxLog10BerOverride(std::nullopt);

  sendFrames(port1_);

  EXPECT_EQ(kNumFrames, port2_.getPortStats(0).dropPkts);
}

TEST(EthernetPortTest, UsingPortLevelAddressesReturnsTrueWhenPortIndexIsZero) {
  Scheduler scheduler;
  SimContext simCtx{&scheduler, 0};
  EthernetPort port(&simCtx, "100Gbps");

  // Set portIndex to 0 (valid port index)
  port.setPortIndex(0);

  auto rocePkt = createPacket<PacketType::ROCE>(100);
  EXPECT_TRUE(port.usingPortLevelAddresses(rocePkt));

  // Create IPV6 packet - should NOT use port-level addresses
  auto ipv6Pkt = createPacket<PacketType::IPV6>(100);
  EXPECT_FALSE(port.usingPortLevelAddresses(ipv6Pkt));
}

TEST(EthernetPortTest, UsingPortLevelAddressesReturnsFalseWhenPortIndexNotSet) {
  Scheduler scheduler;
  SimContext simCtx{&scheduler, 0};
  EthernetPort port(&simCtx, "100Gbps");

  // Do not set portIndex (remains nullopt)

  auto rocePkt = createPacket<PacketType::ROCE>(100);
  EXPECT_FALSE(port.usingPortLevelAddresses(rocePkt));
}

TEST_F(LlrEthernetPortTest, PortLevelDeliveryRestoresSocketConnection) {
  constexpr int kPortIndex = 5;
  port2_->setPortIndex(kPortIndex);

  const FlexDeviceId srcId{{1, 0, 1, 2, 1, 0, 0, 0}};
  const FlexDeviceId dstId{{1, 0, 1, 3, 1, 0, 0, 0}};
  FlexDeviceId portSrcId = srcId;
  FlexDeviceId portDstId = dstId;
  portSrcId.values[kPortIndex] = 2;
  portDstId.values[kPortIndex] = 2;
  QPConnection socketConn(srcId, dstId, 2, 8);
  QPConnection portConn(portSrcId, portDstId, 2, 8);

  auto pkt = createPacket<PacketType::ROCE>(100);
  auto* rocePkt = pkt.as<RocePacket>();
  rocePkt->conn = &portConn;
  rocePkt->socketLevelConn = &socketConn;

  bool received = false;
  observerPort2_.setOnPacketRcvdCallback(
      [&](EthernetPort&, const PacketPtr& receivedPkt) {
        const auto* receivedRocePkt = receivedPkt.as<RocePacket>();
        EXPECT_EQ(receivedRocePkt->conn, &socketConn);
        EXPECT_EQ(receivedRocePkt->srcId(), srcId);
        EXPECT_EQ(receivedRocePkt->dstId(), dstId);
        received = true;
      });

  port1_->send(pkt);
  scheduler_.run();

  EXPECT_TRUE(received);
}

// ============================================================================
// LLR Periodic ACK Timer Tests (UE Spec CtlOS Target Spacing)
// ============================================================================

TEST_F(LlrEthernetPortTest, PeriodicAckTimerSendsAckAfterInterval) {
  constexpr uint64_t kAckThreshold = 16384; // Spec max
  port1_->enableLlr(
      makeTestLlrConfig(kAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/true);
  port2_->enableLlr(
      makeTestLlrConfig(kAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/true);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  // Send a single small packet — too small to reach ackByteThreshold
  auto pkt = createPacket<PacketType::IPV6>(100);
  port1_->send(pkt);
  scheduler_.run(Time::fromUs(100));
  EXPECT_EQ(packetsReceived, 1);

  // Once the frame has arrived, port2's `ackCtlosLoop` posts an LLR_ACK at
  // every `ackCtlosTimer_` expiry, and port1 should observe them.
  EXPECT_GT(port1_->llr()->stats().rxAckCtlos, 0u);
}

TEST_F(LlrEthernetPortTest, PeriodicAckTimerDisabledWhenGlobalTimerOff) {
  constexpr uint64_t kAckThreshold = 16384; // Spec max
  // Disable global timer
  port1_->enableLlr(
      makeTestLlrConfig(kAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);
  port2_->enableLlr(
      makeTestLlrConfig(kAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  // Send a small packet
  port1_->send(createPacket<PacketType::IPV6>(100));
  scheduler_.run(Time::fromUs(100));
  EXPECT_EQ(packetsReceived, 1);

  // With global timer disabled, no periodic ACK should fire.
  scheduler_.run(Time::fromUs(100));
  EXPECT_EQ(port2_->llr()->stats().txAckCtlos, 0u);
}

TEST_F(LlrEthernetPortTest, PeriodicAckTimerSelfReschedules) {
  constexpr uint64_t kAckThreshold = 16384; // Spec max
  port1_->enableLlr(
      makeTestLlrConfig(kAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/true);
  port2_->enableLlr(
      makeTestLlrConfig(kAckThreshold, 512 * 1024, 100000),
      /*enableGlobalTimer=*/true);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  // Send three packets; each advances next_rx_seq on port2. `ackCtlosLoop`
  // then posts one cumulative LLR_ACK per timer expiry, not one per packet.
  port1_->send(createPacket<PacketType::IPV6>(100));
  port1_->send(createPacket<PacketType::IPV6>(100));
  port1_->send(createPacket<PacketType::IPV6>(100));
  scheduler_.run(Time::fromUs(100));
  EXPECT_EQ(packetsReceived, 3);
  EXPECT_GT(port2_->llr()->stats().txAckCtlos, 0u);
  EXPECT_GT(port2_->llr()->stats().txAckPeriodic, 3u);
  EXPECT_EQ(port2_->llr()->stats().txAckByteTrigger, 0u);
}

// Regression: every LLR ACK CtlOS that this port transmits — including
// ACKs spliced (PCS-level) into an in-flight DATA frame by
// `LinkLayerScheduler::sendDataFrame` — must be counted in
// `portStats_[0].txPkts / txBytes`. CtlOS stats are bumped at pull time
// inside `TxArbiter::nextCtlosFrame()`; a regression that drops the
// pull-time bump would silently uncount spliced ACKs.
//
// Test setup forces splicing by saturating both directions of a single
// link with large LLR DATA frames and `ackByteThreshold=0` (= ACK on
// every received frame). Each side's outgoing DATA frame is in flight
// long enough that the peer's freshly-arrived DATA triggers an ACK while
// our own DATA is still serializing — which is exactly the splice path.
// Saturated bidirectional 4 kB transfers at 100 Gbps force at least one
// CtlOS ACK to land mid-DATA — exercises the splice path. Fixture's
// single-queue config keeps all TX on portStats_[0]. `ackByteThreshold=0`
// translates to the 400 B spec minimum (`kCtlosTargetSpacingMinBytes`) in
// `LlrConfigTranslator::translate`.
TEST_F(LlrEthernetPortTest, SplicedCtlosCountedInPortStats) {
  ASSERT_FALSE(port1_->isLlrEnabled());
  ASSERT_FALSE(port2_->isLlrEnabled());

  constexpr uint32_t kPacketSizeBytes = 4000;
  constexpr int kSimRunDurationUs = 500;
  constexpr int kNumPackets = 20;

  port1_->enableLlr(
      makeTestLlrConfig(/*ackByteThreshold=*/0),
      /*enableGlobalTimer=*/true);
  port2_->enableLlr(
      makeTestLlrConfig(/*ackByteThreshold=*/0),
      /*enableGlobalTimer=*/true);

  for (int i = 0; i < kNumPackets; ++i) {
    port1_->send(createPacket<PacketType::IPV6>(kPacketSizeBytes));
    port2_->send(createPacket<PacketType::IPV6>(kPacketSizeBytes));
  }
  scheduler_.run(Time::fromUs(kSimRunDurationUs));

  const auto port1Stats = port1_->llr()->stats();
  const auto port2Stats = port2_->llr()->stats();

  // Sanity: at least one ACK each direction.
  ASSERT_GT(port1Stats.txAckCtlos, 0u);
  ASSERT_GT(port2Stats.txAckCtlos, 0u);

  // txPkts equals the data frames plus every LLR CtlOS sent, spliced or
  // not. The port sends both ACK and NACK CtlOS, so both counters are
  // included to keep the equality stable if a future scenario injects
  // errors.
  EXPECT_EQ(
      port1_->getPortStats(0).txPkts,
      kNumPackets + port1Stats.txAckCtlos + port1Stats.txNackCtlos);
  EXPECT_EQ(
      port2_->getPortStats(0).txPkts,
      kNumPackets + port2Stats.txAckCtlos + port2Stats.txNackCtlos);
  EXPECT_GT(port1_->getPortStats(0).txBytes, kNumPackets * kPacketSizeBytes);
  EXPECT_GT(port2_->getPortStats(0).txBytes, kNumPackets * kPacketSizeBytes);
}

// ============================================================================
// INIT/FLUSH State Machine Tests
// ============================================================================

TEST_F(LlrEthernetPortTest, EnableLlrWithConfigSkipInitSetsAdvanceState) {
  packsim::thrift::LlrConfig config;
  config.ackByteThreshold() = 1000;
  config.skipInitNegotiation() = true;

  port1_->enableLlr(config, /*enableGlobalTimer=*/false);

  EXPECT_TRUE(port1_->isLlrEnabled());
  EXPECT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  EXPECT_EQ(port1_->llr()->ackNackState(), llr::AckNackState::SEND_ACKS);
}

TEST_F(LlrEthernetPortTest, EnableLlrWithConfigInitEntersInitState) {
  packsim::thrift::LlrConfig config;
  config.ackByteThreshold() = 1000;
  config.skipInitNegotiation() = false;

  port1_->enableLlr(config, /*enableGlobalTimer=*/false);

  // `Llr` starts in LLR_OFF and transitions to INIT once the broadcast of
  // setLlrModeRemote(true) is dispatched. Run briefly so the transition
  // happens. With no peer, the SM stays in INIT.
  scheduler_.run(Time::fromUs(1));
  EXPECT_TRUE(port1_->isLlrEnabled());
  EXPECT_EQ(port1_->llr()->txState(), llr::TxState::INIT);
  EXPECT_EQ(port1_->llr()->ackNackState(), llr::AckNackState::OFF);
}

TEST_F(LlrEthernetPortTest, InitHandshakeCompletesAndDataFlows) {
  packsim::thrift::LlrConfig config;
  config.ackByteThreshold() = 16384;
  config.skipInitNegotiation() = false;

  port1_->enableLlr(config, /*enableGlobalTimer=*/false);
  port2_->enableLlr(config, /*enableGlobalTimer=*/false);

  // Run scheduler to process the LLR_INIT / LLR_INIT_ECHO handshake. Each
  // port's `Llr` posts LLR_INIT while in INIT (`initCtlosLoop`), and the
  // port sends it through `TxArbiter::nextCtlosFrame`. The peer's `Llr`
  // answers with LLR_INIT_ECHO, and on seeing the echo the original `Llr`
  // moves INIT → ADVANCE.
  scheduler_.run(Time::fromUs(100));

  EXPECT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  EXPECT_EQ(port2_->llr()->txState(), llr::TxState::ADVANCE);
  EXPECT_EQ(port1_->llr()->ackNackState(), llr::AckNackState::SEND_ACKS);
  EXPECT_EQ(port2_->llr()->ackNackState(), llr::AckNackState::SEND_ACKS);

  // Both peers must have observed each other's INITs and INIT_ECHOs.
  EXPECT_GT(port1_->llr()->stats().rxInitCtlos, 0u);
  EXPECT_GT(port2_->llr()->stats().rxInitCtlos, 0u);
  EXPECT_GT(port1_->llr()->stats().rxInitEchoCtlos, 0u);
  EXPECT_GT(port2_->llr()->stats().rxInitEchoCtlos, 0u);

  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  port1_->send(createPacket<PacketType::IPV6>(1000));
  scheduler_.run(Time::fromUs(100));

  EXPECT_EQ(packetsReceived, 1);
}

TEST_F(LlrEthernetPortTest, DataBlockedDuringInitState) {
  packsim::thrift::LlrConfig config;
  config.ackByteThreshold() = 16384;
  config.skipInitNegotiation() = false;

  // Only enable port1 with INIT — port2 stays without LLR
  // so no INIT_ECHO will be sent back
  port1_->enableLlr(config, /*enableGlobalTimer=*/false);

  // Port1 is in INIT, DATA should be queued but not transmitted
  port1_->send(createPacket<PacketType::IPV6>(1000));
  // Run briefly — DATA should NOT be transmitted
  scheduler_.run(Time::fromUs(10));

  EXPECT_EQ(port1_->llr()->txState(), llr::TxState::INIT);
  // No DATA sequence numbers should have been assigned (DATA is blocked).
  EXPECT_EQ(port1_->llr()->txSeq(), 0u);
  // TX buffer should be empty (no DATA frames admitted yet).
  EXPECT_EQ(port1_->llr()->outstandingSeq(), 0u);
}

TEST_F(LlrEthernetPortTest, DefaultConfigSkipsInitHandshake) {
  // Default config has skipInitNegotiation=true
  packsim::thrift::LlrConfig config;
  config.ackByteThreshold() = 16384;

  port1_->enableLlr(config, /*enableGlobalTimer=*/false);
  port2_->enableLlr(config, /*enableGlobalTimer=*/false);

  // Both should be directly in ADVANCE; with skipInitNegotiation=true,
  // no LLR_INIT CtlOS is emitted on either side.
  EXPECT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  EXPECT_EQ(port2_->llr()->txState(), llr::TxState::ADVANCE);
  EXPECT_EQ(port1_->llr()->stats().txInitCtlos, 0u);
  EXPECT_EQ(port2_->llr()->stats().txInitCtlos, 0u);

  // DATA should flow immediately
  int packetsReceived = 0;
  observerPort2_.setOnPacketRcvdCallback(
      [&packetsReceived](EthernetPort&, const PacketPtr&) {
        ++packetsReceived;
      });

  port1_->send(createPacket<PacketType::IPV6>(1000));
  scheduler_.run(Time::fromUs(100));

  EXPECT_EQ(packetsReceived, 1);
}

TEST_F(LlrEthernetPortTest, FlushOnReplayExhaustionReinitsWhenConfigured) {
  // With replay_ct_max==1, a replay_timer expiry that produces no
  // forward progress flips replay_ct to max, and the next expiry
  // trips the (replay_timer_expired & replay_ct_is_max) edge from
  // ADVANCE to FLUSH (UEC §5.1.5 Fig 5-3). With reInitOnDiscard=true
  // the SM re-enters INIT, completes the LLR_INIT / LLR_INIT_ECHO
  // handshake with `port2_`, and returns to ADVANCE. We assert the
  // post-handshake landing state plus `outstandingSeq() == 0` to
  // confirm FLUSH actually discarded the buffer (rather than the test
  // trivially passing because we never left ADVANCE).
  packsim::thrift::LlrConfig config;
  config.ackByteThreshold() = 16384;
  config.skipInitNegotiation() = true;
  config.replayCtMax() = 1;
  config.reInitOnDiscard() = true;

  port1_->enableLlr(config, /*enableGlobalTimer=*/false);
  port2_->enableLlr(config, /*enableGlobalTimer=*/false);

  EXPECT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);

  // Send a packet so there's a frame in the replay buffer.
  port1_->send(createPacket<PacketType::IPV6>(1000));
  scheduler_.run(Time::fromUs(10));
  // Pin pre-replay state — frame admitted, still ADVANCE — so a future
  // bug that immediately ADVANCE→FLUSHes is caught here instead of
  // hiding behind the post-FLUSH assertions below.
  ASSERT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  ASSERT_EQ(port1_->llr()->outstandingSeq(), 1u);

  // First timer expiry (past retryTimeoutNs=100us default):
  // ADVANCE→REPLAY, replays the frame (re-arming the timer via
  // notifyReplayFrameTransmitted), replay_ct=1. After the batch drains
  // the SM returns to ADVANCE with replay_ct retained.
  scheduler_.run(Time::fromUs(150));
  // Pin intermediate state so a bug that skips REPLAY and dives
  // straight to FLUSH on the first expiry is caught.
  ASSERT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  ASSERT_EQ(port1_->llr()->outstandingSeq(), 1u);

  // Second timer expiry: replay_ct_is_max so ADVANCE→FLUSH; with
  // reInitOnDiscard=true, FLUSH discards the buffer and re-enters INIT,
  // then the LLR_INIT/LLR_INIT_ECHO handshake completes within the run
  // window and the SM lands back in ADVANCE. The spec-meaningful FLUSH
  // side effect is the buffer discard; check outstandingSeq=0 to confirm
  // the FLUSH path actually ran (a REPLAY-only path would have left the
  // frame in the buffer).
  scheduler_.run(Time::fromUs(150));

  EXPECT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  EXPECT_EQ(port1_->llr()->outstandingSeq(), 0u);
  EXPECT_TRUE(port1_->isLlrEnabled());
}

// Direct coverage for the counters that back the thrift `LlrStats`
// fields. The integration tests above hit these counters transitively,
// but these assertions pin the wiring explicitly so a regression in
// `transitionTxStateTo` (init/flush counters) or the replay emit site
// (replay bytes) is caught here instead of leaking into the
// bit-identical sweep as a quiet stat drift.
TEST_F(LlrEthernetPortTest, StatsCountersAdvanceOnFlushAndReplay) {
  packsim::thrift::LlrConfig config;
  config.ackByteThreshold() = 16384;
  config.skipInitNegotiation() = true;
  config.replayCtMax() = 1;
  config.reInitOnDiscard() = true;

  port1_->enableLlr(config, /*enableGlobalTimer=*/false);
  port2_->enableLlr(config, /*enableGlobalTimer=*/false);

  auto pkt = createPacket<PacketType::IPV6>(1000);
  const uint64_t pktSize = pkt->getSize();
  port1_->send(pkt);
  scheduler_.run(Time::fromUs(10));
  ASSERT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  ASSERT_EQ(port1_->llr()->stats().txReplay, 0u);
  ASSERT_EQ(port1_->llr()->stats().txReplayBytes, 0u);
  ASSERT_EQ(port1_->llr()->stats().txFlushEntries, 0u);

  // First replay-timer expiry: ADVANCE→REPLAY, replays the held frame,
  // txReplay + txReplayBytes both advance.
  scheduler_.run(Time::fromUs(150));
  EXPECT_GE(port1_->llr()->stats().txReplay, 1u);
  EXPECT_GE(port1_->llr()->stats().txReplayBytes, pktSize);

  // Second expiry: replay_ct_is_max → ADVANCE→FLUSH; with
  // reInitOnDiscard=true the SM re-enters INIT (incrementing both
  // txFlushEntries AND txInitEntries) before landing back in ADVANCE.
  scheduler_.run(Time::fromUs(150));
  EXPECT_GE(port1_->llr()->stats().txFlushEntries, 1u);
  EXPECT_GE(port1_->llr()->stats().txInitEntries, 1u);
}

// Direct coverage for `Llr::outstandingSeqPeak()` / `outstandingDataPeak()`
// — confirms the peaks reach the high-water and don't decrease after
// the replay buffer drains. Uses a 10 ms retry timeout so the replay
// timer cannot fire during the 100 µs scheduler windows below; if the
// timer fired, the resulting replay batch would not change the peak
// (replays don't tryAdd) but would change `outstandingSeq()` accounting
// timing and could mask the post-ACK drain assertion.
TEST_F(LlrEthernetPortTest, OutstandingPeakAccessorsTrackHighWater) {
  port1_->enableLlr(
      makeTestLlrConfig(16384, 512 * 1024, 10'000'000),
      /*enableGlobalTimer=*/false);
  port2_->enableLlr(
      makeTestLlrConfig(16384, 512 * 1024, 10'000'000),
      /*enableGlobalTimer=*/false);

  EXPECT_EQ(port1_->llr()->outstandingSeqPeak(), 0u);
  EXPECT_EQ(port1_->llr()->outstandingDataPeak(), 0u);

  auto pkt1 = createPacket<PacketType::IPV6>(1000);
  auto pkt2 = createPacket<PacketType::IPV6>(1000);
  const uint64_t pkt1Size = pkt1->getSize();
  const uint64_t pkt2Size = pkt2->getSize();
  port1_->send(pkt1);
  port1_->send(pkt2);
  scheduler_.run(Time::fromUs(100));

  EXPECT_EQ(port1_->llr()->outstandingSeqPeak(), 2u);
  EXPECT_EQ(port1_->llr()->outstandingDataPeak(), pkt1Size + pkt2Size);

  // Drain the buffer with an ACK that covers both seqs (0 and 1). Peaks
  // must NOT decrease after the drain.
  port1_->llr()->rxCtlos(LlrCtlosType::LLR_ACK, 1, /*initData=*/0);
  scheduler_.run(Time::fromUs(100));

  EXPECT_EQ(port1_->llr()->outstandingSeq(), 0u);
  EXPECT_EQ(port1_->llr()->outstandingSeqPeak(), 2u);
  EXPECT_EQ(port1_->llr()->outstandingDataPeak(), pkt1Size + pkt2Size);
}

TEST_F(LlrEthernetPortTest, FlushOnReplayExhaustionParksInFlushOff) {
  // Companion to FlushOnReplayExhaustion (above). With
  // reInitOnDiscard=false, the FLUSH state does NOT re-enter INIT —
  // it parks waiting for managementReset (UEC §5.1.5). Confirms the
  // configuration knob and ensures the FLUSH-parked path isn't
  // accidentally broken by changes to FLUSH handling.
  packsim::thrift::LlrConfig config;
  config.ackByteThreshold() = 16384;
  config.skipInitNegotiation() = true;
  config.replayCtMax() = 1;
  config.reInitOnDiscard() = false;

  port1_->enableLlr(config, /*enableGlobalTimer=*/false);
  port2_->enableLlr(config, /*enableGlobalTimer=*/false);

  port1_->send(createPacket<PacketType::IPV6>(1000));
  scheduler_.run(Time::fromUs(10));
  ASSERT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);

  // First timer expiry (past retryTimeoutNs=100us default):
  // ADVANCE→REPLAY→ADVANCE, replay_ct=1.
  scheduler_.run(Time::fromUs(150));
  ASSERT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);

  // Second timer expiry: replay_ct_is_max → ADVANCE→FLUSH.
  scheduler_.run(Time::fromUs(150));

  // With reInitOnDiscard=false the SM stays in FLUSH (does NOT re-enter
  // INIT). LLR remains enabled — the FLUSH-parked path is intended to
  // wait for managementReset rather than tearing down the module.
  EXPECT_EQ(port1_->llr()->txState(), llr::TxState::FLUSH);
  EXPECT_TRUE(port1_->isLlrEnabled());
}

TEST_F(LlrEthernetPortTest, FlushOnReplayExhaustionReinitsUnderThriftDefault) {
  // FlushOnReplayExhaustionReinitsWhenConfigured with `reInitOnDiscard` left
  // at its thrift default: the port must recover through INIT rather than
  // stay in FLUSH with every data frame blocked.
  packsim::thrift::LlrConfig config;
  config.ackByteThreshold() = 16384;
  config.skipInitNegotiation() = true;
  config.replayCtMax() = 1;

  port1_->enableLlr(config, /*enableGlobalTimer=*/false);
  port2_->enableLlr(config, /*enableGlobalTimer=*/false);

  port1_->send(createPacket<PacketType::IPV6>(1000));
  scheduler_.run(Time::fromUs(10));
  ASSERT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  ASSERT_EQ(port1_->llr()->outstandingSeq(), 1u);

  // First replay-timer expiry replays the frame; the second finds
  // replay_ct at max and enters FLUSH.
  scheduler_.run(Time::fromUs(150));
  scheduler_.run(Time::fromUs(150));

  EXPECT_GE(port1_->llr()->stats().txFlushEntries, 1u);
  EXPECT_GE(port1_->llr()->stats().txInitEntries, 1u);
  EXPECT_EQ(port1_->llr()->txState(), llr::TxState::ADVANCE);
  EXPECT_EQ(port1_->llr()->outstandingSeq(), 0u);
  EXPECT_TRUE(port1_->isLlrEnabled());
}

// ============================================================================
// `TxArbiter` pull API (consumed by `wire::LinkLayerScheduler`)
//
// Direct unit tests for `TxArbiter::nextOutboundFrame()` / `nextCtlosFrame()` /
// `onFrameTransmitted()`. The integration coverage comes from running the
// full test suite — these tests pin the branch-level invariants of the
// pull-API contract so regressions surface independently of the scheduler
// internals.
//
// CtlOS-slot tests replace the port-installed `onPost` callback with a no-op
// before posting. Each post XCHECKs that a callback is installed, so clearing
// it instead would abort the test, and the production callback wakes the
// scheduler, which would consume the CtlOS before the test can pull it. The
// `LLR_REPLAY` branch is exercised only via integration (it requires
// `TxArbiter::replayInFlight_` > 0, set by a prior `nextOutboundFrame()`
// pull through the snapshot pipeline).
// ============================================================================

TEST_F(LlrEthernetPortTest, NextOutboundFrameReturnsNulloptOnEmpty) {
  // Sanity: a freshly-constructed port with no posted work has nothing
  // to hand to the wire driver.
  EXPECT_FALSE(port1_->txArbiter().nextOutboundFrame().has_value());
  EXPECT_FALSE(port1_->txArbiter().nextCtlosFrame().has_value());
}

TEST_F(LlrEthernetPortTest, NextCtlosFrameReturnsNulloptWhenLlrDisabled) {
  // No LLR → no CtlOS source exists, regardless of any other state.
  ASSERT_FALSE(port1_->isLlrEnabled());
  EXPECT_FALSE(port1_->txArbiter().nextCtlosFrame().has_value());
}

TEST_F(LlrEthernetPortTest, NextCtlosFrameReturnsAckAfterPost) {
  port1_->enableLlr(
      makeTestLlrConfig(16384, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);
  // Replace the port-installed onPost callback with a no-op so the post
  // below doesn't wake the scheduler and drain the CtlOS before the test pulls
  // it. A no-op rather than an empty function: the slots require a subscriber,
  // and doing nothing is exactly what this test wants it to do.
  port1_->llr()->ctlosSlots().setOnPostCallback([] {});
  port1_->llr()->ctlosSlots().postAck(/*ackNackSeq=*/42);

  auto frame = port1_->txArbiter().nextCtlosFrame();
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->kind, TxArbiter::TxKind::CTL_OS);
  EXPECT_NE(frame->pkt, nullptr);

  // Queue should be drained — second pull returns nullopt.
  EXPECT_FALSE(port1_->txArbiter().nextCtlosFrame().has_value());
}

TEST_F(LlrEthernetPortTest, NextCtlosFrameBumpsTxStatsAtPullTime) {
  // CtlOS stats are bumped at PULL time inside `nextCtlosFrame`, not on
  // `onFrameTransmitted` — `onFrameTransmitted(CTL_OS)` is a deliberate
  // no-op.
  port1_->enableLlr(
      makeTestLlrConfig(16384, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);
  // A no-op rather than an empty function: the slots require a subscriber, and
  // doing nothing is what suppresses the scheduler wake here.
  port1_->llr()->ctlosSlots().setOnPostCallback([] {});
  port1_->llr()->ctlosSlots().postAck(/*ackNackSeq=*/1);

  const auto txPktsBefore = port1_->getPortStats(/*cos=*/0).txPkts;
  const auto txBytesBefore = port1_->getPortStats(/*cos=*/0).txBytes;

  auto frame = port1_->txArbiter().nextCtlosFrame();
  ASSERT_TRUE(frame.has_value());
  const auto frameSize = frame->pkt->getSize();

  // Pull-time stat bump.
  const auto psAfterPull = port1_->getPortStats(/*cos=*/0);
  EXPECT_EQ(psAfterPull.txPkts, txPktsBefore + 1);
  EXPECT_EQ(psAfterPull.txBytes, txBytesBefore + frameSize);

  // onFrameTransmitted(CTL_OS) is a no-op — stats stay where they were
  // after the pull.
  port1_->txArbiter().onFrameTransmitted(*frame);
  const auto psAfterTx = port1_->getPortStats(/*cos=*/0);
  EXPECT_EQ(psAfterTx.txPkts, psAfterPull.txPkts);
  EXPECT_EQ(psAfterTx.txBytes, psAfterPull.txBytes);
}

TEST_F(LlrEthernetPortTest, OnFrameTransmittedEgressBumpsTxStats) {
  // Non-LLR egress path: onFrameTransmitted(EGRESS) bumps per-CoS stats
  // for the packet's CoS.
  auto frame = TxArbiter::OutboundFrame{
      .pkt = createPacket<PacketType::IPV6>(500),
      .kind = TxArbiter::TxKind::EGRESS,
  };
  const auto cos = port1_->getPktCos(frame.pkt);
  const auto txBytesBefore = port1_->getPortStats(cos).txBytes;
  const auto txPktsBefore = port1_->getPortStats(cos).txPkts;
  const auto frameSize = frame.pkt->getSize();

  port1_->txArbiter().onFrameTransmitted(frame);

  const auto ps = port1_->getPortStats(cos);
  EXPECT_EQ(ps.txPkts, txPktsBefore + 1);
  EXPECT_EQ(ps.txBytes, txBytesBefore + frameSize);
}

TEST_F(LlrEthernetPortTest, OnFrameTransmittedLlrNewFiresTxCallbackOnce) {
  // LLR_NEW = first transmission of an LLR_ELIGIBLE frame. The port
  // observer's `onPacketSent` MUST fire exactly once so the switch-buffer
  // accounting sees the cell-free notify.
  port1_->enableLlr(
      makeTestLlrConfig(16384, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);

  int txCallbackCount = 0;
  observerPort1_.setOnPacketSentCallback(
      [&txCallbackCount](EthernetPort&, const PacketPtr&) {
        ++txCallbackCount;
      });

  auto frame = TxArbiter::OutboundFrame{
      .pkt = createPacket<PacketType::IPV6>(500),
      .kind = TxArbiter::TxKind::LLR_NEW,
  };
  port1_->txArbiter().onFrameTransmitted(frame);
  EXPECT_EQ(txCallbackCount, 1);
}

TEST_F(LlrEthernetPortTest, OnFrameTransmittedLlrPassthroughFiresTxCallback) {
  // LLR_PASSTHROUGH = LLR_INELIGIBLE pulled through LLR (BEST_EFFORT or
  // LLR_OFF). Same upstream-notify semantics as a fresh transmit: fires
  // txCallback once, no replay-buffer entry to maintain.
  port1_->enableLlr(
      makeTestLlrConfig(16384, 512 * 1024, 100000),
      /*enableGlobalTimer=*/false);

  int txCallbackCount = 0;
  observerPort1_.setOnPacketSentCallback(
      [&txCallbackCount](EthernetPort&, const PacketPtr&) {
        ++txCallbackCount;
      });

  auto frame = TxArbiter::OutboundFrame{
      .pkt = createPacket<PacketType::IPV6>(500),
      .kind = TxArbiter::TxKind::LLR_PASSTHROUGH,
  };
  port1_->txArbiter().onFrameTransmitted(frame);
  EXPECT_EQ(txCallbackCount, 1);
}

} // namespace packsim
