/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <folly/ScopeGuard.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/cbfc/credit-observer.h"
#include "packsim/link-layer/cbfc/nic-rx-credit-bridge.h"
#include "packsim/nic/nic-rx-buffer.h"
#include "packsim/packet/pfc.h"

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace packsim {

namespace {
// Counts store/drain notifications so a test can assert the credit observer is
// wired to the buffer's enqueue and dequeue points.
class RecordingCreditObserver : public cbfc::ICreditObserver {
 public:
  void onPacketStored(const Packet& /*pkt*/) override {
    ++stored;
  }
  void onPacketDrained(const Packet& /*pkt*/) override {
    ++drained;
  }
  int stored{0};
  int drained{0};
};
// VC 1 is the only lossless VC, and DSCP CS3 (0x18) maps to it -- the value
// `makeUdpPacket(..., /* cos= */ 3)` sets, so every CBFC test below runs on the
// lossless VC.
packsim::thrift::CbfcConfig makeLosslessCbfcConfig() {
  packsim::thrift::CbfcConfig cfg;
  cfg.numVcs() = 2;
  cfg.isLosslessPerVc() = {false, true};
  packsim::thrift::VcMapEntry rule;
  rule.selector() = packsim::thrift::VcMapSelector::DSCP;
  rule.mask() = 0x3F;
  rule.value() = 0x18;
  rule.vcId() = 1;
  cfg.vcMap() = {rule};
  cfg.expectedCreditSizeBytes() = 64;
  cfg.pktOvhdBytes() = 0;
  return cfg;
}

// Every Cbfc built by EthernetPort gets both wake subscribers from
// wireCbfcWakes. A test that builds one directly satisfies the same contract.
void wireNoopWakes(cbfc::Cbfc& c) {
  c.setOnWakeCallback([] {});
  c.setOnCtlosReadyCallback([] {});
}

} // namespace
class NicRxBufferTest
    : public ::testing::TestWithParam<std::tuple<double, uint8_t>> {
 public:
  void SetUp() override {
    scheduler_.buildNewEngine();

    dr_ = DataRate("100Gbps");
    eff_ = std::get<0>(GetParam());
    queueIdx_ = std::get<1>(GetParam());
    effDr_ = DataRate(dr_.getBps() * eff_ / 100.0);

    packsim::thrift::SchedulingAlgorithmType schedulerType =
        packsim::thrift::SchedulingAlgorithmType::RR;
    q_ = std::make_unique<NicRxBuffer>(
        &simCtx_,
        dr_,
        eff_,
        eff_,
        size_,
        xoff_,
        xon_,
        buildFilterConfig(),
        schedulerType);
    q_->setOnPacketReleasedCallback(
        [this](PacketPtr pkt) { outCallback(std::move(pkt)); });
    q_->setOnPfcStateChangedCallback([this](uint8_t queueIdx, PfcState state) {
      pfcCallback(queueIdx, state);
    });
    lastPacketTime_.resize(8);
    lastPfcTime_.resize(8);
  }

 protected:
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};
  std::unique_ptr<NicRxBuffer> q_;
  DataRate dr_;
  DataRate effDr_;
  double eff_{};
  packsim::thrift::QueueFilterConfig filterCfg_;
  uint8_t queueIdx_{};
  // total buffer size = 2MB
  std::vector<uint32_t>
      size_{262144, 131072, 393216, 262144, 393216, 131072, 262144, 262144};
  std::vector<int32_t>
      xoff_{249036, 124518, 373555, 249036, 373555, 124518, 249036, 249036};
  std::vector<int32_t>
      xon_{244836, 120318, 369355, 244836, 369355, 120318, 244836, 244836};
  static packsim::thrift::QueueFilterConfig buildFilterConfig() {
    packsim::thrift::QueueFilterConfig cfg;
    cfg.fieldToQueue() = {
        {56, 7}, {48, 6}, {40, 5}, {32, 4}, {24, 3}, {16, 2}, {8, 1}, {0, 0}};
    return cfg;
  }

  std::vector<PfcState> lastPfcState_{
      PfcState::XON,
      PfcState::XON,
      PfcState::XON,
      PfcState::XON,
      PfcState::XON,
      PfcState::XON,
      PfcState::XON,
      PfcState::XON};
  std::vector<bool>
      pfcSet_{false, false, false, false, false, false, false, false};
  std::vector<Time> lastPfcTime_;

  PacketPtr lastPacket_;
  std::vector<Time> lastPacketTime_;
  std::vector<PacketPtr> pkts_;

  void pfcCallback(uint8_t queueIdx, PfcState state) {
    pfcSet_.at(queueIdx) = true;
    lastPfcState_.at(queueIdx) = state;
    lastPfcTime_.at(queueIdx) = simCtx_.now();
  }

  void outCallback(PacketPtr pkt) {
    lastPacket_ = pkt;
    auto queueIdx = q_->filter(pkt);
    lastPacketTime_.at(queueIdx) = simCtx_.now();
  }

  uint32_t queuePacket(uint32_t size, uint32_t cos) {
    auto pkt = createPacket<PacketType::UDP>(size);
    auto pktSize = pkt->getSize();
    pkt.as<UdpPacket>()->setDscpFromCos(cos);
    q_->queuePacket(pkt);
    pkts_.emplace_back(pkt);
    return pktSize;
  }
};

TEST_P(NicRxBufferTest, TestPfcCallback) {
  size_t size{0};
  constexpr uint32_t pktSize{1000};
  size_t actPktSize{0};
  EXPECT_EQ(pfcSet_.at(queueIdx_), false);
  EXPECT_EQ(q_->getPfcState(queueIdx_), PfcState::XON);
  while (size + pktSize + 40 < static_cast<size_t>(xoff_.at(queueIdx_))) {
    size += queuePacket(pktSize, queueIdx_);
    if (actPktSize == 0) {
      actPktSize = size;
    }
    EXPECT_EQ(pfcSet_.at(queueIdx_), false);
  }
  EXPECT_EQ(pfcSet_.at(queueIdx_), false);
  EXPECT_EQ(q_->getPfcState(queueIdx_), PfcState::XON);

  size += queuePacket(pktSize, queueIdx_);
  EXPECT_EQ(pfcSet_.at(queueIdx_), true);
  EXPECT_EQ(q_->getPfcState(queueIdx_), PfcState::XOFF);
  EXPECT_EQ(lastPfcState_.at(queueIdx_), PfcState::XOFF);

  scheduler_.run();
  EXPECT_EQ(q_->getPfcState(queueIdx_), PfcState::XON);

  size_t bytesToDrain = size - static_cast<size_t>(xon_.at(queueIdx_));
  size_t pktsToDrain = bytesToDrain / actPktSize;
  if (pktsToDrain * actPktSize < bytesToDrain) {
    ++pktsToDrain;
  }
  auto expTime = effDr_.bytesToTime(pktsToDrain * (actPktSize));
  EXPECT_NEAR(expTime.getNs(), lastPfcTime_.at(queueIdx_).getNs(), 1);
}

TEST_P(NicRxBufferTest, TestEfficiency) {
  constexpr size_t numPkts{33};
  constexpr uint32_t pktSize{1160};
  size_t totalSize{0};

  for (size_t i = 0; i < numPkts; i++) {
    totalSize += queuePacket(pktSize, queueIdx_);
  }
  scheduler_.run();
  auto expTime = effDr_.bytesToTime(totalSize);
  EXPECT_NEAR(expTime.getNs(), lastPacketTime_.at(queueIdx_).getNs(), 1);
}

TEST_P(NicRxBufferTest, CreditObserverNotifiedOnStoreAndDrain) {
  RecordingCreditObserver obs;
  q_->setCreditObserver(&obs);

  queuePacket(1000, queueIdx_);
  // The drain runs on a scheduled event, so only the store has fired.
  EXPECT_EQ(1, obs.stored);
  EXPECT_EQ(0, obs.drained);

  scheduler_.run(); // drive doPop -> drain
  EXPECT_EQ(1, obs.drained);
}

INSTANTIATE_TEST_SUITE_P(
    NicRxBufferTestEffSweep,
    NicRxBufferTest,
    ::testing::Combine(
        ::testing::Values(80, 95, 100),
        ::testing::Range(uint8_t{0}, uint8_t{7})));

// Non-parameterized fixture for behaviors that do not depend on the
// efficiency/queue sweep: construction, filtering, queue accounting, and the
// PFC-disabled path.
class NicRxBufferBehaviorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scheduler_.buildNewEngine();
  }

  static packsim::thrift::QueueFilterConfig buildFilterConfig() {
    packsim::thrift::QueueFilterConfig cfg;
    // DSCP (= cos * 8) -> queue index.
    cfg.fieldToQueue() = {
        {0, 0}, {8, 1}, {16, 2}, {24, 3}, {32, 4}, {40, 5}, {48, 6}, {56, 7}};
    return cfg;
  }

  std::unique_ptr<NicRxBuffer> makeBuffer(
      const std::vector<uint32_t>& sizeBytes,
      const std::vector<int32_t>& xoffBytes,
      const std::vector<int32_t>& xonBytes,
      const packsim::thrift::SchedulingAlgorithmType schedulerType =
          packsim::thrift::SchedulingAlgorithmType::RR) {
    auto buf = std::make_unique<NicRxBuffer>(
        &simCtx_,
        DataRate("100Gbps"),
        /* minEff= */ 100.0,
        /* maxEff= */ 100.0,
        sizeBytes,
        xoffBytes,
        xonBytes,
        buildFilterConfig(),
        schedulerType);
    // RoceTransportBase wires this on every production buffer
    // (roce-base.cpp), so a directly built one satisfies the same contract.
    buf->setOnPacketReleasedCallback([](const PacketPtr&) {});
    return buf;
  }

  PacketPtr makeUdpPacket(uint32_t payloadSize, uint32_t cos) {
    auto pkt = createPacket<PacketType::UDP>(payloadSize);
    pkt.as<UdpPacket>()->setDscpFromCos(cos);
    return pkt;
  }

  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, 0};

  // 8-queue buffer configuration (2 MB total), matching the sweep fixture.
  const std::vector<uint32_t> kSizeBytes_{
      262144,
      131072,
      393216,
      262144,
      393216,
      131072,
      262144,
      262144};
  const std::vector<int32_t> kXoffBytes_{
      249036,
      124518,
      373555,
      249036,
      373555,
      124518,
      249036,
      249036};
  const std::vector<int32_t> kXonBytes_{
      244836,
      120318,
      369355,
      244836,
      369355,
      120318,
      244836,
      244836};
};

TEST_F(NicRxBufferBehaviorTest, ConstructorThrowsOnMismatchedXoffXonSizes) {
  EXPECT_THAT(
      [&]() {
        NicRxBuffer buffer(
            &simCtx_,
            DataRate("100Gbps"),
            /* minEff= */ 100.0,
            /* maxEff= */ 100.0,
            /* sizeBytes= */ std::vector<uint32_t>{1000, 1000},
            /* xoffBytes= */ std::vector<int32_t>{900, 900},
            /* xonBytes= */ std::vector<int32_t>{800}, // one entry short
            buildFilterConfig(),
            packsim::thrift::SchedulingAlgorithmType::RR);
      },
      ThrowsMessage<std::runtime_error>(
          HasSubstr("Sizes of XOFF and XON vectors must match")));
}

TEST_F(NicRxBufferBehaviorTest, ConstructorThrowsOnShortBufferSizeList) {
  EXPECT_THAT(
      [&]() {
        NicRxBuffer buffer(
            &simCtx_,
            DataRate("100Gbps"),
            /* minEff= */ 100.0,
            /* maxEff= */ 100.0,
            /* sizeBytes= */ std::vector<uint32_t>{1000}, // one entry short
            /* xoffBytes= */ std::vector<int32_t>{900, 900},
            /* xonBytes= */ std::vector<int32_t>{800, 800},
            buildFilterConfig(),
            packsim::thrift::SchedulingAlgorithmType::RR);
      },
      ThrowsMessage<std::invalid_argument>(HasSubstr(
          "perCosBufferSize needs an entry for each of the 2 CoS queues")));
}

TEST_F(NicRxBufferBehaviorTest, FilterMapsCosToConfiguredQueue) {
  auto buffer = makeBuffer(kSizeBytes_, kXoffBytes_, kXonBytes_);
  std::vector<uint32_t> mappedQueues;
  mappedQueues.reserve(8);
  for (uint32_t cos = 0; cos < 8; ++cos) {
    mappedQueues.emplace_back(
        buffer->filter(makeUdpPacket(/* payloadSize= */ 1000, cos)));
  }
  // Per the filter config, cos c routes to queue c.
  std::vector<uint32_t> expectedQueues(8);
  std::iota(expectedQueues.begin(), expectedQueues.end(), 0u);
  EXPECT_EQ(mappedQueues, expectedQueues);
}

TEST_F(NicRxBufferBehaviorTest, QueuePacketReturnsQueueIndexAndEnqueued) {
  auto buffer = makeBuffer(kSizeBytes_, kXoffBytes_, kXonBytes_);
  const auto [queueIdx, enqueued] =
      buffer->queuePacket(makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 3));
  EXPECT_EQ(queueIdx, 3);
  EXPECT_TRUE(enqueued);
}

TEST_F(NicRxBufferBehaviorTest, AllQueuesEmptyReflectsBufferContents) {
  auto buffer = makeBuffer(kSizeBytes_, kXoffBytes_, kXonBytes_);
  EXPECT_TRUE(buffer->allQueuesEmpty());
  buffer->queuePacket(makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 3));
  EXPECT_FALSE(buffer->allQueuesEmpty());
  // Draining the scheduled pop empties the buffer again.
  scheduler_.run();
  EXPECT_TRUE(buffer->allQueuesEmpty());
}

TEST_F(NicRxBufferBehaviorTest, NegativeXoffXonDisablesPfc) {
  // Queue 0 has negative xoff/xon, which disables PFC for that queue; queue 1
  // keeps normal thresholds.
  auto buffer = makeBuffer(
      /* sizeBytes= */ std::vector<uint32_t>{1'000'000, 1'000'000},
      /* xoffBytes= */ std::vector<int32_t>{-1, 5000},
      /* xonBytes= */ std::vector<int32_t>{-1, 4000});
  bool pfcFiredForQueue0 = false;
  buffer->setOnPfcStateChangedCallback(
      [&](uint8_t queueIdx, PfcState /*state*/) {
        if (queueIdx == 0) {
          pfcFiredForQueue0 = true;
        }
      });

  // Queue ~20 KB into queue 0 (cos 0) without running the scheduler so the
  // packets accumulate. That exceeds queue 1's 5 KB xoff, so a PFC-enabled
  // queue would have tripped XOFF; queue 0 with xoff=-1 must stay XON
  // regardless.
  for (int i = 0; i < 20; ++i) {
    buffer->queuePacket(makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 0));
  }

  EXPECT_EQ(buffer->getPfcState(0), PfcState::XON);
  EXPECT_FALSE(pfcFiredForQueue0);
}

// The buffer does not enforce `sizeBytes`: with PFC off, a queue already
// holding more than its size still accepts every packet, and all of them
// drain.
TEST_F(NicRxBufferBehaviorTest, QueuePastBufferSizeStillEnqueues) {
  constexpr int kNumPkts = 5;
  int released = 0;
  NicRxBuffer buffer(
      &simCtx_,
      DataRate("100Gbps"),
      /* minEff= */ 100.0,
      /* maxEff= */ 100.0,
      /* sizeBytes= */ std::vector<uint32_t>{2000},
      /* xoffBytes= */ std::vector<int32_t>{-1},
      /* xonBytes= */ std::vector<int32_t>{-1},
      buildFilterConfig(),
      packsim::thrift::SchedulingAlgorithmType::RR);
  buffer.setOnPacketReleasedCallback([&](const PacketPtr&) { ++released; });

  // The scheduler does not run until every packet is queued, so the queue
  // holds all five 1000 B payloads at once.
  for (int i = 0; i < kNumPkts; ++i) {
    const auto [queueIdx, enqueued] = buffer.queuePacket(
        makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 0));
    EXPECT_EQ(queueIdx, 0);
    EXPECT_TRUE(enqueued);
  }

  scheduler_.run();
  EXPECT_EQ(kNumPkts, released);
}

TEST_F(NicRxBufferBehaviorTest, ConstructsFromNicConfig) {
  // Exercises the NicConfig delegating constructor (the production path).
  packsim::thrift::NicConfig nicConfig;
  nicConfig.speed() = "100Gbps";
  nicConfig.writeEff() = 100.0;
  nicConfig.perCosBufferSize() = kSizeBytes_;
  nicConfig.perCosXoff() = kXoffBytes_;
  nicConfig.perCosXon() = kXonBytes_;
  nicConfig.queueFilterCfg() = buildFilterConfig();
  nicConfig.queueScheduleCfg() = packsim::thrift::SchedulingAlgorithmType::RR;

  NicRxBuffer buffer(&simCtx_, &nicConfig);
  buffer.setOnPacketReleasedCallback([](const PacketPtr&) {});

  // The NicConfig-built buffer filters and enqueues like the explicit one.
  EXPECT_EQ(
      buffer.filter(makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 5)), 5u);
  const auto [queueIdx, enqueued] =
      buffer.queuePacket(makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 5));
  EXPECT_EQ(queueIdx, 5);
  EXPECT_TRUE(enqueued);
}

// A packet that fills another CoS queue while a drain is in flight waits for
// that drain; the queue scheduler then chooses the next packet.
class NicRxBufferMultiCosTest : public NicRxBufferBehaviorTest,
                                public ::testing::WithParamInterface<
                                    packsim::thrift::SchedulingAlgorithmType> {
};

TEST_P(NicRxBufferMultiCosTest, DrainsQueuesThatFillDuringADrain) {
  auto buffer = makeBuffer(kSizeBytes_, kXoffBytes_, kXonBytes_, GetParam());
  std::vector<std::pair<const Packet*, Time>> released;
  buffer->setOnPacketReleasedCallback([&](const PacketPtr& pkt) {
    released.emplace_back(pkt.get(), simCtx_.now());
  });

  // `first` starts draining on arrival; the other two arrive while it drains.
  const auto first = makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 1);
  const auto second = makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 1);
  const auto urgent = makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 3);
  buffer->queuePacket(first);
  buffer->queuePacket(second);
  buffer->queuePacket(urgent);

  ASSERT_NO_THROW(scheduler_.run());

  // RR rotates past CoS 1, and WRR and SP favour CoS 3, so every scheduler
  // releases `urgent` before `second`.
  const uint64_t drainFs =
      DataRate("100Gbps").bytesToTime(first->getSize()).getFs();
  ASSERT_EQ(released.size(), 3u);
  EXPECT_EQ(released.at(0).first, first.get());
  EXPECT_EQ(released.at(1).first, urgent.get());
  EXPECT_EQ(released.at(2).first, second.get());
  EXPECT_EQ(released.at(0).second.getFs(), drainFs);
  EXPECT_EQ(released.at(1).second.getFs(), 2 * drainFs);
  EXPECT_EQ(released.at(2).second.getFs(), 3 * drainFs);
  EXPECT_TRUE(buffer->allQueuesEmpty());
}

INSTANTIATE_TEST_SUITE_P(
    QueueSchedulers,
    NicRxBufferMultiCosTest,
    ::testing::Values(
        packsim::thrift::SchedulingAlgorithmType::RR,
        packsim::thrift::SchedulingAlgorithmType::WRR,
        packsim::thrift::SchedulingAlgorithmType::SP),
    [](const ::testing::TestParamInfo<packsim::thrift::SchedulingAlgorithmType>&
           info) -> std::string {
      return apache::thrift::util::enumNameSafe(info.param);
    });

// Credit-bridge wiring reacts to store and drain events, not to how fast the
// buffer drains, so these cases are not swept over efficiency and queue index.
// Every packet below carries CoS 3, which the filter config maps to queue 3 and
// `makeLosslessCbfcConfig` maps to the lossless VC.
class NicRxBufferCreditBridgeTest : public NicRxBufferBehaviorTest {
 protected:
  void SetUp() override {
    NicRxBufferBehaviorTest::SetUp();
    buffer_ = makeBuffer(kSizeBytes_, kXoffBytes_, kXonBytes_);
  }

  std::unique_ptr<NicRxBuffer> buffer_;
};

TEST_F(
    NicRxBufferCreditBridgeTest,
    CreditBridgeHoldsTheCreditUntilTheBufferDrains) {
  const auto cfg = makeLosslessCbfcConfig();
  cbfc::Cbfc cbfc{&simCtx_, cfg, cbfc::kCbfcDefaultLinkBps800Gbps};
  wireNoopWakes(cbfc);
  cbfc::NicRxCreditBridge bridge{&cbfc};
  buffer_->setCreditObserver(&bridge);
  SCOPE_EXIT {
    buffer_->setCreditObserver(nullptr);
  };

  auto pkt = makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 3);
  ASSERT_TRUE(cbfc.isLosslessPacket(*pkt));

  // These two calls mirror `Endpoint::onPacketRcvd`, which counts the arrival
  // and then offers the packet to whatever runs next.
  cbfc.onPacketStored(*pkt);
  bridge.arm(*pkt);
  const uint64_t freedOnArrival = cbfc.stats().creditsFreed;

  buffer_->queuePacket(pkt);

  EXPECT_TRUE(bridge.disarmAndWasClaimed())
      << "a buffered packet must be claimed, or the endpoint frees its credit "
         "while the packet is still in NIC memory";
  EXPECT_EQ(freedOnArrival, cbfc.stats().creditsFreed);

  scheduler_.run(); // drive doPop -> drain
  EXPECT_GT(cbfc.stats().creditsFreed, freedOnArrival)
      << "the credit must be returned when the buffer actually drains";
}

// An unclaimed packet leaves the endpoint to free the credit itself, which is
// correct for ACK/NACK/CNP/PING and for any transport with no receive buffer.
// The fixture's buffer is deliberately never offered this packet.
TEST_F(NicRxBufferCreditBridgeTest, CreditBridgeDoesNotClaimAPacketItNeverSaw) {
  const auto cfg = makeLosslessCbfcConfig();
  cbfc::Cbfc cbfc{&simCtx_, cfg, cbfc::kCbfcDefaultLinkBps800Gbps};
  wireNoopWakes(cbfc);
  cbfc::NicRxCreditBridge bridge{&cbfc};

  auto pkt = createPacket<PacketType::UDP>(1000);
  bridge.arm(*pkt);
  EXPECT_FALSE(bridge.disarmAndWasClaimed());
}

// A detached bridge stays inert; see `NicRxCreditBridge::detach`.
TEST_F(NicRxBufferCreditBridgeTest, DetachedCreditBridgeIgnoresLaterDrains) {
  const auto cfg = makeLosslessCbfcConfig();
  cbfc::Cbfc cbfc{&simCtx_, cfg, cbfc::kCbfcDefaultLinkBps800Gbps};
  wireNoopWakes(cbfc);
  cbfc::NicRxCreditBridge bridge{&cbfc};
  buffer_->setCreditObserver(&bridge);
  SCOPE_EXIT {
    buffer_->setCreditObserver(nullptr);
  };

  auto pkt = makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 3);
  cbfc.onPacketStored(*pkt);
  bridge.arm(*pkt);
  buffer_->queuePacket(pkt);
  ASSERT_TRUE(bridge.disarmAndWasClaimed());

  bridge.detach(); // as `EthernetPort::disableCbfc` does
  const uint64_t freedAtDetach = cbfc.stats().creditsFreed;

  scheduler_.run(); // the buffer still drains through the installed bridge
  EXPECT_EQ(freedAtDetach, cbfc.stats().creditsFreed)
      << "a detached bridge must not report to the module it was cut from";
}

// A CBFC re-enable must revive the SAME bridge. Replacing it would free the
// object the transport's `NicRxBuffer` still points at, which is the very
// use-after-free `detach` exists to avoid.
TEST_F(
    NicRxBufferCreditBridgeTest,
    ReattachedCreditBridgeReportsToTheNewModule) {
  const auto cfg = makeLosslessCbfcConfig();
  cbfc::Cbfc first{&simCtx_, cfg, cbfc::kCbfcDefaultLinkBps800Gbps};
  wireNoopWakes(first);
  cbfc::NicRxCreditBridge bridge{&first};
  buffer_->setCreditObserver(&bridge);
  SCOPE_EXIT {
    buffer_->setCreditObserver(nullptr);
  };
  bridge.detach(); // CBFC disabled

  cbfc::Cbfc second{
      &simCtx_,
      cfg,
      cbfc::kCbfcDefaultLinkBps800Gbps}; // CBFC re-enabled with a fresh module
  wireNoopWakes(second);
  bridge.reattach(&second);

  auto pkt = makeUdpPacket(/* payloadSize= */ 1000, /* cos= */ 3);
  second.onPacketStored(*pkt);
  bridge.arm(*pkt);
  buffer_->queuePacket(pkt);
  ASSERT_TRUE(bridge.disarmAndWasClaimed());

  const uint64_t before = second.stats().creditsFreed;
  scheduler_.run();
  EXPECT_GT(second.stats().creditsFreed, before)
      << "the reattached bridge must report to the module it was revived with";
  EXPECT_EQ(0u, first.stats().creditsFreed)
      << "and must not report to the module it was detached from";
}

} // namespace packsim
