/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc.h"

#include <functional>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim::cbfc {
namespace {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

// One CF_min window for every config built here: PackSim's floor of 800 B at
// the default 800 Gbps.
const Time kCfMinWindow = Time::fromNs(8);

// One CF_max period at the Tbl 5-27 floor of 16 kB and 800 Gbps: 163.84 ns,
// which has no whole-nanosecond spelling.
const Time kCfMaxWindow = Time::fromPs(163'840);

// One CC_msg period: the Tbl 5-27 floor of 1 us.
const Time kCcMsgWindow = Time::fromUs(1);

// Every config here runs at the module's default rate, which makes
// `kCfMinWindow` 8 ns.
constexpr uint64_t kLinkBps = kCbfcDefaultLinkBps800Gbps;

packsim::thrift::VcMapEntry dscpRule(uint32_t value, uint8_t vcId) {
  packsim::thrift::VcMapEntry rule;
  rule.selector() = packsim::thrift::VcMapSelector::DSCP;
  rule.mask() = 0x3F;
  rule.value() = value;
  rule.vcId() = vcId;
  return rule;
}

// VC1 is lossless and maps from DSCP CS3 (0x18). One credit is 64 bytes with
// no per-packet overhead. `cfMinTimerBytes` keeps its default, PackSim's
// floor of 800 B, so CF_Updates are spaced `kCfMinWindow` apart.
packsim::thrift::CbfcConfig makeConfig() {
  packsim::thrift::CbfcConfig cfg;
  cfg.numVcs() = 2;
  cfg.isLosslessPerVc() = {false, true};
  cfg.vcMap() = {dscpRule(0x18, /*vcId=*/1)};
  cfg.expectedCreditSizeBytes() = 64;
  cfg.pktOvhdBytes() = 0;
  return cfg;
}

// A 0-payload UDP packet is 70 bytes -> ceil(70/64) == 2 credits.
PacketPtr makeLosslessPkt() {
  auto pkt = createPacket<PacketType::UDP>(0);
  pkt.as<UdpPacket>()->setDscpFromCos(3); // DSCP CS3 -> VC1 (lossless)
  return pkt;
}

PacketPtr makeBestEffortPkt() {
  auto pkt = createPacket<PacketType::UDP>(0);
  pkt.as<UdpPacket>()->setDscpFromCos(0); // DSCP 0 -> no rule -> best-effort
  return pkt;
}

// Four lossless VCs, one per DSCP class selector CS0..CS3, give the
// report-slot tests more dirty VCs than the two slots of one CF_Update can
// carry. CF_min spacing is the same `kCfMinWindow` as `makeConfig`.
packsim::thrift::CbfcConfig makeMultiVcConfig() {
  packsim::thrift::CbfcConfig cfg;
  cfg.numVcs() = 4;
  cfg.isLosslessPerVc() = {true, true, true, true};
  cfg.vcMap() = {
      dscpRule(0x00, /*vcId=*/0),
      dscpRule(0x08, /*vcId=*/1),
      dscpRule(0x10, /*vcId=*/2),
      dscpRule(0x18, /*vcId=*/3),
  };
  cfg.expectedCreditSizeBytes() = 64;
  cfg.pktOvhdBytes() = 0;
  return cfg;
}

// CoS n maps to DSCP CSn, which `makeMultiVcConfig` maps to VC n.
PacketPtr makeVcPkt(uint8_t vc) {
  auto pkt = createPacket<PacketType::UDP>(0);
  pkt.as<UdpPacket>()->setDscpFromCos(vc);
  return pkt;
}

// Every Cbfc built by EthernetPort gets both wake subscribers from
// wireCbfcWakes. A test that builds one directly has to satisfy the same
// contract; a test asserting on wakes installs its own over these.
void wireNoopWakes(Cbfc& cbfc) {
  cbfc.setOnWakeCallback([] {});
  cbfc.setOnCtlosReadyCallback([] {});
}

} // namespace

// Simulation bootstrap shared by both fixtures below.
class CbfcSimFixture {
 protected:
  CbfcSimFixture() {
    scheduler_.buildNewEngine();
  }
  // The CF_min gate measures from time 0, so a report pulled at the very start
  // of a test is suppressed for one spacing window, as is each report after a
  // previous one. `Scheduler::run` takes a duration, so successive calls each
  // advance one more window. Call this before the credit return, not after:
  // then the first report is not withheld, and tests that count
  // `onCtlosReady` callbacks see only their own wakes.
  void advancePastCfMinWindow() {
    scheduler_.run(kCfMinWindow);
  }
  Scheduler scheduler_;
  SimContext ctx_{&scheduler_, 0};
};

class CbfcTest : public ::testing::Test, protected CbfcSimFixture {};

// An out-of-range config field must be a recoverable error the caller can
// report against the link, not a process abort that takes down a whole
// simulation for one bad link. One case per rejected field, so a failure names
// the field that regressed.
struct ConfigRejectionCase {
  // gtest appends this to the parameterized test name, so it must contain
  // alphanumeric characters only.
  std::string name;
  // This puts exactly one field of a valid `makeConfig()` out of range.
  std::function<void(packsim::thrift::CbfcConfig&)> mutate;
  // The thrown `std::invalid_argument` message must contain this substring,
  // normally the name of the offending field.
  std::string expectedInMessage;
};

class CbfcConfigRejectionTest
    : public ::testing::TestWithParam<ConfigRejectionCase>,
      protected CbfcSimFixture {};

TEST_P(CbfcConfigRejectionTest, RejectsTheFieldByThrowing) {
  auto cfg = makeConfig();
  GetParam().mutate(cfg);
  // The message, not just the type: a type-only check passes even when the
  // wrong field is the one that got rejected.
  EXPECT_THAT(
      [&] { Cbfc c(&ctx_, cfg, kLinkBps); },
      ThrowsMessage<std::invalid_argument>(
          HasSubstr(GetParam().expectedInMessage)));
}

INSTANTIATE_TEST_SUITE_P(
    OutOfRangeFields,
    CbfcConfigRejectionTest,
    ::testing::Values(
        ConfigRejectionCase{
            "CfMaxTimerBytesZero",
            [](packsim::thrift::CbfcConfig& c) { c.cfMaxTimerBytes() = 0; },
            "cfMaxTimerBytes"},
        // Tbl 5-27 gives CF_max a 16 kB..1 MB range with a minimum granularity
        // of 16 kB, so both bounds and the step are enforced.
        ConfigRejectionCase{
            "CfMaxTimerBytesAboveRange",
            [](packsim::thrift::CbfcConfig& c) {
              c.cfMaxTimerBytes() = kCfMaxTimerMaxBytes + kCfMaxTimerMinBytes;
            },
            "outside the UEC 1.0.3 Tbl 5-27 range"},
        ConfigRejectionCase{
            "CfMaxTimerBytesOffGranularity",
            [](packsim::thrift::CbfcConfig& c) {
              c.cfMaxTimerBytes() = kCfMaxTimerMinBytes + 1;
            },
            "minimum granularity"},
        // PackSim floors CF_min at 800 B of wire time, so a link that would
        // report more often than that is rejected.
        ConfigRejectionCase{
            "CfMinTimerBytesBelowFloor",
            [](packsim::thrift::CbfcConfig& c) {
              c.cfMinTimerBytes() = kCfMinTimerMinBytes - 1;
            },
            "cfMinTimerBytes"},
        // Each timer is individually in range here; only their order is wrong.
        // An inverted pair is unsatisfiable rather than merely odd, so it is
        // rejected instead of clamped.
        ConfigRejectionCase{
            "CfMinTimerBytesAboveCfMax",
            [](packsim::thrift::CbfcConfig& c) {
              c.cfMaxTimerBytes() = kCfMaxTimerMinBytes;
              c.cfMinTimerBytes() = kCfMaxTimerMinBytes + 1;
            },
            "exceeds cfMaxTimerBytes"},
        // 0 is the "derive from the receiver" sentinel and is legal, so the
        // rejected values are the ones no S_CreditSize field could carry.
        ConfigRejectionCase{
            "CreditSizeBytesAboveWireField",
            [](packsim::thrift::CbfcConfig& c) {
              c.expectedCreditSizeBytes() = kMaxCreditSizeBytes + 1;
            },
            "expectedCreditSizeBytes"},
        ConfigRejectionCase{
            "CcMsgTimerZero",
            [](packsim::thrift::CbfcConfig& c) { c.ccMsgTimerNs() = 0; },
            "ccMsgTimerNs"},
        ConfigRejectionCase{
            "UnsupportedPktOvhd",
            [](packsim::thrift::CbfcConfig& c) {
              c.pktOvhdBytes() = kCbfcLimits.maxPktOvhdBytes + 1;
            },
            "pktOvhdBytes"},
        // Tbl 5-27 requires at least one lossless VC; CBFC with none gates
        // nothing.
        ConfigRejectionCase{
            "NoLosslessVcs",
            [](packsim::thrift::CbfcConfig& c) {
              c.isLosslessPerVc() = {false, false};
            },
            "marks no VC lossless"},
        ConfigRejectionCase{
            "LosslessPerVcWrongLength",
            [](packsim::thrift::CbfcConfig& c) {
              c.isLosslessPerVc() = {false, true, true};
            },
            "isLosslessPerVc"},
        // `mask` and `value` are matched against a 6-bit DSCP or 4-bit PCP/DEI
        // field. A value wider than the selected field silently matches
        // nothing.
        ConfigRejectionCase{
            "VcMapValueWiderThanTheSelectedField",
            [](packsim::thrift::CbfcConfig& c) { c.vcMap()[0].value() = 0x40; },
            "vcMap value"},
        ConfigRejectionCase{
            "VcMapMaskWiderThanTheSelectedField",
            [](packsim::thrift::CbfcConfig& c) { c.vcMap()[0].mask() = 0x40; },
            "vcMap mask"},
        ConfigRejectionCase{
            "VcMapNamesAnUnknownVc",
            [](packsim::thrift::CbfcConfig& c) {
              c.vcMap() = {dscpRule(0x18, /*vcId=*/9)};
            },
            "vcMap"},
        ConfigRejectionCase{
            "NumVcsAboveTheSupportedLimit",
            [](packsim::thrift::CbfcConfig& c) {
              c.numVcs() = kCbfcLimits.maxVcs + 1u;
            },
            "numVcs"},
        ConfigRejectionCase{
            "TooManyVcGroups",
            [](packsim::thrift::CbfcConfig& c) {
              c.vcGroups() = std::vector<packsim::thrift::VcGrpEntry>(
                  size_t{kCbfcLimits.maxVcGroups} + 1);
            },
            "vcGroups"},
        ConfigRejectionCase{
            "AVcInTwoVcGroups",
            [](packsim::thrift::CbfcConfig& c) {
              packsim::thrift::VcGrpEntry a;
              a.vcIds() = {1};
              a.limitCredits() = 4;
              packsim::thrift::VcGrpEntry b;
              b.vcIds() = {1};
              b.limitCredits() = 8;
              c.vcGroups() = {a, b};
            },
            "more than one"}),
    [](const ::testing::TestParamInfo<ConfigRejectionCase>& info) {
      return info.param.name;
    });

// `linkBps` is a constructor argument, not a config field: the
// byte-denominated CF_min and CF_max timers cannot be converted to wire time
// at a zero rate.
TEST_F(CbfcTest, RejectsAZeroLinkRate) {
  const auto cfg = makeConfig();
  EXPECT_THAT(
      [&] { Cbfc c(&ctx_, cfg, /*linkBps=*/0); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("linkBps")));
}

// The bootstrap setter rejects an unresolved sentinel for the reason given on
// `Cbfc::setCreditSize`: it would silently fall back to a 1 B credit.
TEST_F(CbfcTest, SetCreditSizeRejectsZeroByThrowing) {
  const auto cfg = makeConfig();
  Cbfc c(&ctx_, cfg, kLinkBps);
  wireNoopWakes(c);
  EXPECT_THAT(
      [&] { c.setCreditSize(0); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("credit size is zero")));
  EXPECT_EQ(*cfg.expectedCreditSizeBytes(), c.creditSizeBytes())
      << "a rejected resolution must not disturb the size in force";
}

// `setCreditSize` overrides the credit size at sim-init bootstrap, so the new
// value has to be what every later reader sees -- including `pktCreditsFor`,
// which charges the gate.
TEST_F(CbfcTest, ResolvedCreditSizeIsTheOneInForce) {
  auto cfg = makeConfig();
  cfg.expectedCreditSizeBytes() = 64;
  Cbfc c(&ctx_, cfg, kLinkBps);
  wireNoopWakes(c);
  c.setCreditSize(254);
  EXPECT_EQ(254u, c.creditSizeBytes());

  // A 70 B packet is 1 credit at 254 B and 2 at the configured 64 B, so the
  // gate's arithmetic distinguishes the two.
  c.initSenderPortLimit(1);
  ASSERT_TRUE(c.initSenderReservations());
  auto pkt = makeLosslessPkt();
  EXPECT_TRUE(c.canTransmit(*pkt))
      << "the gate is still charging at the pre-resolution credit size";
}

// The CC_Update production path: the periodic timer queues a range, the pop
// fills it from the sender ledger, and VCs past `numVcs` zero-fill.
TEST_F(CbfcTest, CcMsgTimerQueuesARangePoppedWithSenderCounters) {
  auto cfg = makeConfig();
  cfg.ccMsgTimerNs() = kCcMsgTimerMinNs; // 1 us, the Tbl 5-27 floor
  Cbfc c(&ctx_, cfg, kLinkBps);
  wireNoopWakes(c);
  c.initSenderPortLimit(8);
  ASSERT_TRUE(c.initSenderReservations());
  c.start();

  // Consume 2 credits on VC1 so the popped range carries a non-zero counter.
  auto pkt = makeLosslessPkt();
  ASSERT_TRUE(c.canTransmit(*pkt));
  c.onTransmit(*pkt);

  ASSERT_FALSE(c.popCcUpdate().has_value()) << "nothing queued before a tick";
  scheduler_.run(2U * kCcMsgWindow);

  auto cc = c.popCcUpdate();
  ASSERT_TRUE(cc.has_value());
  auto* payload = (*cc).as<EthernetCcPacket>();
  EXPECT_EQ(CbfcCcMsgType::VC_0_15, payload->msgType);
  EXPECT_EQ(0u, payload->sVcCc.at(0)) << "VC0 sent nothing";
  EXPECT_EQ(2u, payload->sVcCc.at(1)) << "VC1's S_VC_CC after one 2-credit pkt";
  for (size_t i = *cfg.numVcs(); i < payload->sVcCc.size(); ++i) {
    EXPECT_EQ(0u, payload->sVcCc.at(i)) << "VC " << i << " past numVcs";
  }
  EXPECT_EQ(1u, c.stats().ccUpdatesSent);
  EXPECT_FALSE(c.popCcUpdate().has_value()) << "the range pops exactly once";
  c.shutdown();
}

// Every configured VC is reported: a link with more than 16 VCs gets a
// VC_16_31 report too, so the highest VC's counter reaches the peer and its
// leaked credits can be reconciled (§5.2.5.6.4).
TEST_F(CbfcTest, CcUpdateCoversEveryConfiguredVc) {
  const uint8_t numVcs = kCbfcLimits.maxVcs;
  const uint8_t topVc = numVcs - 1;
  auto cfg = makeConfig();
  cfg.numVcs() = numVcs;
  cfg.isLosslessPerVc() = std::vector<bool>(numVcs, true);
  cfg.vcMap() = {dscpRule(0x18, /*vcId=*/topVc)};
  cfg.ccMsgTimerNs() = kCcMsgTimerMinNs; // 1 us, the Tbl 5-27 floor
  Cbfc c(&ctx_, cfg, kLinkBps);
  wireNoopWakes(c);
  c.initSenderPortLimit(8);
  ASSERT_TRUE(c.initSenderReservations());
  c.start();
  auto pkt = makeLosslessPkt(); // DSCP CS3 -> topVc
  ASSERT_TRUE(c.canTransmit(*pkt));
  c.onTransmit(*pkt); // 2 credits

  scheduler_.run(2U * kCcMsgWindow);
  std::vector<PacketPtr> reports;
  while (auto cc = c.popCcUpdate()) {
    reports.emplace_back(std::move(*cc));
  }

  constexpr size_t kVcsPerReport = 16; // UEC Tbl 5-22
  ASSERT_EQ(reports.size(), (numVcs + kVcsPerReport - 1) / kVcsPerReport);
  const auto& topReport = *reports.back().as<EthernetCcPacket>();
  EXPECT_EQ(
      topReport.msgType,
      topVc < kVcsPerReport ? CbfcCcMsgType::VC_0_15 : CbfcCcMsgType::VC_16_31);
  EXPECT_EQ(2u, topReport.sVcCc.at(topVc % kVcsPerReport));
  c.shutdown();
}

// A second tick on top of an unpulled range must not queue a duplicate, but
// must still raise the edge -- the TX driver is edge-triggered, so a missed
// edge would strand the queued range for the rest of the run.
TEST_F(CbfcTest, CcMsgTimerDeduplicatesButStillWakesEveryTick) {
  auto cfg = makeConfig();
  cfg.ccMsgTimerNs() = kCcMsgTimerMinNs; // 1 us, the Tbl 5-27 floor
  Cbfc c(&ctx_, cfg, kLinkBps);
  wireNoopWakes(c);
  int wakes = 0;
  c.setOnWakeCallback([&]() { ++wakes; });
  c.start();

  scheduler_.run(3U * kCcMsgWindow + kCfMinWindow); // three ticks
  EXPECT_GE(wakes, 3) << "every tick must raise the edge, not just the first";

  ASSERT_TRUE(c.popCcUpdate().has_value());
  EXPECT_FALSE(c.popCcUpdate().has_value())
      << "three ticks over one unpulled range must queue exactly one";
  c.shutdown();
}

// A CC_Update is built when it is pulled, not when the range is marked, so a
// superseded tick loses nothing: the surviving mark reports the ledger as of
// pull time.
TEST_F(CbfcTest, ACcUpdateReportsTheLedgerAtPullTimeNotAtMarkTime) {
  auto cfg = makeConfig();
  cfg.ccMsgTimerNs() = kCcMsgTimerMinNs; // 1 us, the Tbl 5-27 floor
  Cbfc sender(&ctx_, cfg, kLinkBps);
  wireNoopWakes(sender);
  sender.initSenderPortLimit(64);
  ASSERT_TRUE(sender.initSenderReservations());
  sender.start();

  // One tick marks the range while the ledger is still empty.
  scheduler_.run(kCcMsgWindow + kCfMinWindow); // one tick

  // Consume after the mark and before the pull.
  auto pkt = makeLosslessPkt();
  ASSERT_TRUE(sender.canTransmit(*pkt));
  sender.onTransmit(*pkt); // 2 credits on VC1

  auto cc = sender.popCcUpdate();
  ASSERT_TRUE(cc.has_value());
  EXPECT_EQ(2u, (*cc).as<EthernetCcPacket>()->sVcCc.at(1))
      << "the report must carry the ledger at pull time; reporting the state "
         "at mark time would lose everything consumed after the mark";
  sender.shutdown();
}

// A queued-but-unpulled range must not survive shutdown: after a restart it
// would go out on the first pull, ahead of the restarted timer's first tick.
TEST_F(CbfcTest, ShutdownDiscardsAQueuedCcUpdate) {
  auto cfg = makeConfig();
  cfg.ccMsgTimerNs() = kCcMsgTimerMinNs; // 1 us, the Tbl 5-27 floor
  Cbfc c(&ctx_, cfg, kLinkBps);
  wireNoopWakes(c);
  c.start();
  scheduler_.run(2U * kCcMsgWindow); // at least one tick has queued a range

  c.shutdown();
  EXPECT_FALSE(c.popCcUpdate().has_value())
      << "a stale range survived shutdown";
}

// The CF_Update side of shutdown must match the CC_Update side: a VC left
// dirty would be reported on the first pull after a restart, ahead of the
// restarted CF_min/CF_max cadence.
TEST_F(CbfcTest, ShutdownDiscardsADirtyCfReport) {
  advancePastCfMinWindow();
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);
  ASSERT_TRUE(receiver.popCfUpdate().has_value())
      << "the drain must produce a report for this test to mean anything";

  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);
  receiver.shutdown();

  // The pull above advanced the CF_min window; step past it again so this
  // assertion turns on the discarded dirty flag rather than on spacing.
  advancePastCfMinWindow();
  EXPECT_FALSE(receiver.popCfUpdate().has_value())
      << "a dirty VC survived shutdown";
}

// `popCfUpdate` does not consult the run state, so a drain reported after
// shutdown would otherwise hand the port a report from a stopped module.
TEST_F(CbfcTest, ADrainAfterShutdownProducesNoCfReport) {
  // Without this the CF_min gate suppresses the pull on its own and the
  // assertion below passes whether or not the module is stopped.
  advancePastCfMinWindow();
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  receiver.shutdown();

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);

  EXPECT_FALSE(receiver.popCfUpdate().has_value())
      << "a stopped module produced a CF_Update";
  // The credits are accounted for, not lost: a restart's CF_max refresh
  // reports them.
  EXPECT_EQ(2u, receiver.stats().creditsFreed);
}

// A module that has never been started is a valid state and `running_` is
// false there too, so the shutdown suppression must key on `stopped_`. This is
// a regression guard against keying it on `!running_`, which would silence
// every unstarted receiver; it passes with or without the suppression itself.
TEST_F(CbfcTest, ANeverStartedModuleStillReportsFreedCredits) {
  advancePastCfMinWindow();
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);

  EXPECT_TRUE(receiver.popCfUpdate().has_value());
}

// The CF_Update payload has to survive the trip through a wire frame.
TEST_F(CbfcTest, BuildCfUpdatePacketCarriesThePayload) {
  advancePastCfMinWindow();
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);
  auto payload = receiver.popCfUpdate();
  ASSERT_TRUE(payload.has_value());

  auto frame = Cbfc::buildCfUpdatePacket(*payload);
  EXPECT_EQ(PacketType::CTLOS_CF_UPDATE, frame->pktType)
      << "a CF_Update is an ordered set, not an Ethernet frame";
  auto* cf = frame.as<CfUpdateFrame>();
  ASSERT_NE(nullptr, cf);
  EXPECT_EQ(payload->vc1, cf->cfUpdate.vc1);
  EXPECT_EQ(payload->cf1, cf->cfUpdate.cf1);
  EXPECT_EQ(CtlosFrame::kCtlosFrameSizeBytes, frame->getSize());
}

TEST_F(CbfcTest, CreditGateBlocksThenCfUpdateUnblocks) {
  advancePastCfMinWindow();
  const auto cfg = makeConfig();
  Cbfc sender(&ctx_, cfg, kLinkBps);
  wireNoopWakes(sender);
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  sender.initSenderPortLimit(4); // 4 credits == 2 packets
  ASSERT_TRUE(sender.initSenderReservations());

  auto p1 = makeLosslessPkt();
  ASSERT_TRUE(sender.canTransmit(*p1));
  sender.onTransmit(*p1);
  receiver.onPacketStored(*p1);

  auto p2 = makeLosslessPkt();
  ASSERT_TRUE(sender.canTransmit(*p2));
  sender.onTransmit(*p2);
  receiver.onPacketStored(*p2);

  auto p3 = makeLosslessPkt();
  EXPECT_FALSE(sender.canTransmit(*p3));

  receiver.onPacketDrained(*p1);
  auto cf = receiver.popCfUpdate();
  ASSERT_TRUE(cf.has_value());
  EXPECT_EQ(1u, cf->vc1);
  EXPECT_EQ(2u, cf->cf1);

  sender.rxCfUpdate(*cf);
  EXPECT_TRUE(sender.canTransmit(*p3));
  EXPECT_EQ(2u, sender.stats().creditsReturned);
}

TEST_F(CbfcTest, BestEffortPacketBypassesGate) {
  const auto cfg = makeConfig();
  Cbfc sender(&ctx_, cfg, kLinkBps);
  wireNoopWakes(sender);
  sender.initSenderPortLimit(0); // no lossless credits at all
  ASSERT_TRUE(sender.initSenderReservations());

  auto be = makeBestEffortPkt();
  EXPECT_TRUE(sender.canTransmit(*be));
  sender.onTransmit(*be);
  EXPECT_EQ(0u, sender.stats().creditsConsumed);
}

TEST_F(CbfcTest, MacControlFramesBypassGateWithNoCredits) {
  const auto cfg = makeConfig();
  Cbfc sender(&ctx_, cfg, kLinkBps);
  wireNoopWakes(sender);
  sender.initSenderPortLimit(0); // no credits at all
  ASSERT_TRUE(sender.initSenderReservations());

  // A lossless packet is the control: with zero credits the gate must shut.
  auto blocked = makeLosslessPkt();
  ASSERT_FALSE(sender.canTransmit(*blocked))
      << "zero credits must block ordinary lossless traffic";

  auto pfc = createPacket<PacketType::ETHERNET_PFC>();
  auto cc = createPacket<PacketType::ETHERNET_CC>();
  EXPECT_TRUE(sender.canTransmit(*pfc)) << "PFC is credit-exempt (§5.2.9)";
  EXPECT_TRUE(sender.canTransmit(*cc)) << "CC_Update is credit-exempt (§5.2.9)";
  EXPECT_EQ(0u, sender.stats().creditsConsumed);
}

TEST_F(CbfcTest, PooledCreditLimitGatesAcrossVcs) {
  const auto cfg = makeConfig();
  Cbfc sender(&ctx_, cfg, kLinkBps);
  wireNoopWakes(sender);
  sender.initSenderPortLimit(4); // 4 credits == two 2-credit packets

  auto p1 = makeLosslessPkt();
  ASSERT_TRUE(sender.canTransmit(*p1));
  sender.onTransmit(*p1);
  auto p2 = makeLosslessPkt();
  ASSERT_TRUE(sender.canTransmit(*p2));
  sender.onTransmit(*p2);

  // The shared port credit pool is exhausted -> the next lossless packet is
  // gated even though no single VC has its own limit.
  auto p3 = makeLosslessPkt();
  EXPECT_FALSE(sender.canTransmit(*p3));
  EXPECT_EQ(4u, sender.stats().creditsConsumed);
}

// `isLosslessPacket` lets the switch classify a drop without re-implementing
// the VC mapping.
TEST_F(CbfcTest, ClassifiesLosslessVc) {
  const auto cfg = makeConfig();
  Cbfc cbfc(&ctx_, cfg, kLinkBps);
  wireNoopWakes(cbfc);

  auto lossless = makeLosslessPkt();
  auto bestEffort = makeBestEffortPkt();
  EXPECT_TRUE(cbfc.isLosslessPacket(*lossless));
  EXPECT_FALSE(cbfc.isLosslessPacket(*bestEffort));
}

TEST_F(CbfcTest, CountsGateBlockedEvents) {
  const auto cfg = makeConfig();
  Cbfc cbfc(&ctx_, cfg, kLinkBps);
  wireNoopWakes(cbfc);

  EXPECT_EQ(0u, cbfc.stats().gateBlockedCount);
  cbfc.onGateBlocked();
  EXPECT_EQ(1u, cbfc.stats().gateBlockedCount);
}

TEST_F(CbfcTest, CountsLosslessVcBufferFullDrops) {
  const auto cfg = makeConfig();
  Cbfc cbfc(&ctx_, cfg, kLinkBps);
  wireNoopWakes(cbfc);

  EXPECT_EQ(0u, cbfc.stats().losslessVcBufferFullDrops);
  cbfc.onLosslessVcBufferFullDrop();
  EXPECT_EQ(1u, cbfc.stats().losslessVcBufferFullDrops);
}

TEST_F(CbfcTest, CcUpdateReclaimsLeakedCreditsAtReceiver) {
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  // Receiver stored two lossless packets (4 credits), but the sender consumed
  // 6 (a 2-credit packet was lost). A CC_Update carrying S_VC_CC=6 lets the
  // receiver reclaim the 2 leaked credits.
  auto a = makeLosslessPkt();
  auto b = makeLosslessPkt();
  receiver.onPacketStored(*a);
  receiver.onPacketStored(*b);

  EthernetCcPacket cc;
  cc.msgType = CbfcCcMsgType::VC_0_15;
  cc.sVcCc.at(1) = 6; // S_VC_CC for VC1
  receiver.rxCcUpdate(cc);

  EXPECT_EQ(2u, receiver.stats().leakedCreditsReclaimed);
}

// The reclaim path has the same shut-down rule as the drain path: a CC_Update
// still in flight when the link tears CBFC down must not leave a report the
// port can pull from a stopped module.
TEST_F(CbfcTest, ACcUpdateAfterShutdownProducesNoCfReport) {
  // As in `ADrainAfterShutdownProducesNoCfReport`: without this the CF_min
  // gate alone satisfies the assertion below.
  advancePastCfMinWindow();
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  auto a = makeLosslessPkt();
  auto b = makeLosslessPkt();
  receiver.onPacketStored(*a);
  receiver.onPacketStored(*b);
  receiver.shutdown();

  EthernetCcPacket cc;
  cc.msgType = CbfcCcMsgType::VC_0_15;
  cc.sVcCc.at(1) = 6; // sender consumed 6, receiver saw 4: 2 leaked
  receiver.rxCcUpdate(cc);

  // The reclaim is still accounted for, so a restart's CF_max refresh reports
  // it; only the report from the stopped module is suppressed.
  EXPECT_EQ(2u, receiver.stats().leakedCreditsReclaimed);
  EXPECT_FALSE(receiver.popCfUpdate().has_value())
      << "a stopped module produced a CF_Update from a CC_Update reclaim";
}

// Guards the Tbl 5-21 repeated-report rule; the rationale is on the branch
// that implements it, at the end of `Cbfc::packDirtyReports`.
TEST_F(CbfcTest, SingleDirtyVcRepeatsItsReportInsteadOfEmittingAnEmptySlot) {
  advancePastCfMinWindow();
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);

  auto cf = receiver.popCfUpdate();
  ASSERT_TRUE(cf.has_value());
  EXPECT_EQ(1u, cf->vc1);
  EXPECT_EQ(2u, cf->cf1);
  // Both slots name the real VC — never VC 0, which is best-effort here and
  // whose R_VC_CF the receiver has never advanced.
  EXPECT_EQ(cf->vc1, cf->vc2);
  EXPECT_EQ(cf->cf1, cf->cf2);
}

// `packDirtyReports` repeats slot 1 in slot 2 when only one VC is dirty.
// Applying the copy must be inert: the first application advances `S_VC_CF` to
// that value, so the second sees a forward distance of zero. Otherwise one
// received CF_Update returns its credits twice.
TEST_F(CbfcTest, RepeatedCfReportReturnsCreditsExactlyOnce) {
  advancePastCfMinWindow();
  const auto cfg = makeConfig();
  Cbfc sender(&ctx_, cfg, kLinkBps);
  wireNoopWakes(sender);
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  sender.initSenderPortLimit(4);
  ASSERT_TRUE(sender.initSenderReservations());

  auto pkt = makeLosslessPkt();
  ASSERT_TRUE(sender.canTransmit(*pkt));
  sender.onTransmit(*pkt); // 2 credits out
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);

  auto cf = receiver.popCfUpdate();
  ASSERT_TRUE(cf.has_value());
  ASSERT_EQ(cf->vc1, cf->vc2) << "this test only means anything on a repeat";

  sender.rxCfUpdate(*cf);
  EXPECT_EQ(2u, sender.stats().creditsReturned)
      << "the repeated slot must be inert, not a second return";
  // A double-return would have driven S_VC_CU below zero and wrapped it, which
  // shows up as the gate refusing a packet the sender has full credit for.
  EXPECT_TRUE(sender.canTransmit(*pkt));
}

// A fixed vc=0 scan start lets two continuously dirty low VCs hold both report
// slots forever, so every higher VC's credits are never returned and that
// lossless VC stalls for good. On a port with many VCs this strands all but
// two of them.
TEST_F(CbfcTest, CfUpdateRotatesReportSlotsAcrossVcs) {
  advancePastCfMinWindow();
  const auto cfg = makeMultiVcConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  // Dirty all four VCs.
  for (uint8_t vc = 0; vc < 4; ++vc) {
    auto pkt = makeVcPkt(vc);
    receiver.onPacketStored(*pkt);
    receiver.onPacketDrained(*pkt);
  }

  auto first = receiver.popCfUpdate();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(0u, first->vc1);
  EXPECT_EQ(1u, first->vc2);

  // Re-dirty only the two VCs that were just reported — the starvation setup.
  for (uint8_t vc = 0; vc < 2; ++vc) {
    auto pkt = makeVcPkt(vc);
    receiver.onPacketStored(*pkt);
    receiver.onPacketDrained(*pkt);
  }

  advancePastCfMinWindow();
  auto second = receiver.popCfUpdate();
  ASSERT_TRUE(second.has_value());
  // Without the cursor the scan restarts at 0 and reports {0,1} again; VC2 and
  // VC3 would never be reported however many times this is polled.
  EXPECT_EQ(2u, second->vc1);
  EXPECT_EQ(3u, second->vc2);
}

// The port's pump pulls again on each wake, so waking inline would hand the
// second payload to the port before the first had been returned, putting the
// two CF_Updates on the wire in the wrong order.
TEST_F(CbfcTest, MoreDirtyVcsThanOneCtlosCarriesDoNotReEnterPopCfUpdate) {
  const auto cfg = makeMultiVcConfig(); // 4 VCs, 2 reports per CtlOS
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  int depth = 0;
  int maxDepth = 0;
  int pulled = 0;
  receiver.setOnCtlosReadyCallback([&]() {
    ++depth;
    maxDepth = std::max(maxDepth, depth);
    while (auto payload = receiver.popCfUpdate()) {
      ++pulled;
    }
    --depth;
  });
  receiver.start();

  // Each drain must land with the CF_min window already open, so that the wake
  // it raises actually emits from inside the callback. Drain everything at once
  // and CF_min withholds every report, leaving the depth assertion vacuous.
  for (uint8_t vc = 0; vc < 4; ++vc) {
    advancePastCfMinWindow();
    auto pkt = makeVcPkt(vc);
    receiver.onPacketStored(*pkt);
    receiver.onPacketDrained(*pkt);
  }

  ASSERT_EQ(4, pulled) << "no report was emitted from inside the callback, so "
                          "the re-entrancy assertion below proves nothing";
  EXPECT_EQ(1, maxDepth) << "the pump re-entered popCfUpdate mid-emission";
  receiver.shutdown();
}

TEST_F(CbfcTest, ASuppressedReportIsWithheldNotDiscarded) {
  auto cfg = makeConfig();
  cfg.cfMinTimerBytes() = 800; // 8 ns at the default 800 Gbps
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  std::vector<Time> wakes;
  receiver.setOnCtlosReadyCallback([&]() { wakes.emplace_back(ctx_.now()); });
  receiver.start();

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt); // wake #1, from the drain itself
  ASSERT_FALSE(receiver.popCfUpdate().has_value()) << "CF_min must suppress";
  ASSERT_EQ(1u, wakes.size());

  // CF_max is 163.84 ns here, so nothing inside 100 ns can wake it.
  scheduler_.run(Time::fromNs(100));

  EXPECT_EQ(1u, wakes.size()) << "CF_min expiry must not wake on its own";
  EXPECT_TRUE(receiver.popCfUpdate().has_value())
      << "the withheld report must survive until the window opens";
  receiver.shutdown();
}

TEST_F(CbfcTest, CfMaxDeliversAReportCfMinWithheld) {
  auto cfg = makeConfig();
  cfg.cfMinTimerBytes() = 800;
  cfg.cfMaxTimerBytes() = kCfMaxTimerMinBytes;
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  std::vector<Time> wakes;
  receiver.setOnCtlosReadyCallback([&]() { wakes.emplace_back(ctx_.now()); });
  receiver.start();

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt); // wake #1, inside the CF_min window
  ASSERT_FALSE(receiver.popCfUpdate().has_value());
  ASSERT_EQ(1u, wakes.size());

  scheduler_.run(kCfMaxWindow + kCfMinWindow);

  ASSERT_EQ(2u, wakes.size()) << "CF_max never refreshed the withheld report";
  EXPECT_EQ(kCfMaxWindow, wakes.at(1));
  EXPECT_TRUE(receiver.popCfUpdate().has_value());
  receiver.shutdown();
}

TEST_F(CbfcTest, CfMaxDrainsTwoVcsPerPeriodNotAll) {
  auto cfg = makeMultiVcConfig(); // 4 lossless VCs, 2 reports per CtlOS
  cfg.cfMaxTimerBytes() = kCfMaxTimerMinBytes;
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  int pulled = 0;
  receiver.setOnCtlosReadyCallback([&]() {
    if (receiver.popCfUpdate()) {
      ++pulled;
    }
  });
  receiver.start();

  // At t=0, so every report is withheld and CF_max is the only thing that can
  // release them.
  for (uint8_t vc = 0; vc < 4; ++vc) {
    auto pkt = makeVcPkt(vc);
    receiver.onPacketStored(*pkt);
    receiver.onPacketDrained(*pkt);
  }
  ASSERT_EQ(0, pulled) << "CF_min should have withheld every report";

  scheduler_.run(kCfMaxWindow + kCfMinWindow);
  EXPECT_EQ(1, pulled);
  EXPECT_TRUE(receiver.popCfUpdate().has_value())
      << "the other two VCs are still owed a report after one CF_max period";

  receiver.shutdown();
}

// An idle poll must not arm a spacing wake, or every poll burns an event.
TEST_F(CbfcTest, IdlePollInsideCfMinWindowArmsNoWake) {
  auto cfg = makeConfig();
  cfg.cfMinTimerBytes() = 800;
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  std::vector<Time> wakes;
  receiver.setOnCtlosReadyCallback([&]() { wakes.emplace_back(ctx_.now()); });
  receiver.start();

  for (int i = 0; i < 5; ++i) {
    ASSERT_FALSE(receiver.popCfUpdate().has_value());
  }
  scheduler_.run(Time::fromNs(100));

  EXPECT_TRUE(wakes.empty());
  receiver.shutdown();
}

// The optional credit-accumulation gate batches credit return: a
// drain only makes its VC eligible once the credits freed since that VC's
// last report reach the threshold.
TEST_F(CbfcTest, CreditThresholdDefersCfUpdateUntilAccumulated) {
  advancePastCfMinWindow();
  auto cfg = makeConfig(); // 2 credits per packet
  cfg.cfCreditThresholdCredits() = 6;
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt); // accumulated 2
  ASSERT_FALSE(receiver.popCfUpdate().has_value()) << "2 < 6";
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt); // accumulated 4
  ASSERT_FALSE(receiver.popCfUpdate().has_value()) << "4 < 6";

  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt); // accumulated 6 -> crosses
  auto report = receiver.popCfUpdate();
  ASSERT_TRUE(report.has_value());
  EXPECT_EQ(1u, report->vc1);
  // R_VC_CF is cumulative; only the gate's accumulator is a delta.
  EXPECT_EQ(6u, report->cf1);

  // The accumulator resets where the dirty flag clears, so the next packet
  // starts a fresh window rather than latching the VC permanently eligible.
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);
  advancePastCfMinWindow();
  EXPECT_FALSE(receiver.popCfUpdate().has_value())
      << "one packet re-crossed the threshold, so the accumulator did not "
         "reset when the dirty flag cleared";
}

// The threshold gate belongs on the producer side, in `onCreditsFreed`: a
// pop-side filter over `cfDirty_` would also swallow the §5.2.5.7.3 CF_max
// lost-update refresh, so a VC holding sub-threshold residue would never
// return it.
TEST_F(CbfcTest, CreditThresholdDoesNotSuppressCfMaxRefresh) {
  auto cfg = makeConfig(); // CF_max 16384 B -> 163.84 ns at 800 Gbps
  cfg.cfCreditThresholdCredits() = 1000; // unreachable by one 2-credit packet
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  receiver.start();

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);
  ASSERT_FALSE(receiver.popCfUpdate().has_value()) << "2 << 1000";

  scheduler_.run(Time::fromNs(200)); // let CF_max fire
  auto report = receiver.popCfUpdate();
  ASSERT_TRUE(report.has_value()) << "CF_max must report sub-threshold residue";
  EXPECT_EQ(1u, report->vc1);
  EXPECT_EQ(2u, report->cf1);
  receiver.shutdown();
}

// Crossing the credit threshold makes a VC eligible; it does not let it jump
// the CF_min queue.
TEST_F(CbfcTest, CreditThresholdStillObeysCfMinSpacing) {
  auto cfg = makeConfig();
  cfg.cfMinTimerBytes() = 800; // 8 ns at the default 800 Gbps
  cfg.cfCreditThresholdCredits() = 2; // one packet crosses it
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  std::vector<Time> wakes;
  receiver.setOnCtlosReadyCallback([&]() { wakes.emplace_back(ctx_.now()); });
  receiver.start();

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt); // crosses at t=0 -> dirty + wake #1
  ASSERT_EQ(1u, wakes.size());
  // lastCfSentTime_ starts at 0 and now() is 0, so even the first report is
  // spaced out.
  ASSERT_FALSE(receiver.popCfUpdate().has_value()) << "CF_min must suppress";

  scheduler_.run(Time::fromNs(100));
  EXPECT_EQ(1u, wakes.size())
      << "CF_min expiry must not wake; the drain's is the only one";
  auto first = receiver.popCfUpdate();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(2u, first->cf1);

  // Still at t=8 ns: a fresh crossing cannot produce a back-to-back emit.
  receiver.onPacketStored(*pkt);
  receiver.onPacketDrained(*pkt);
  EXPECT_FALSE(receiver.popCfUpdate().has_value())
      << "a threshold crossing must not shorten CF_min spacing";
  receiver.shutdown();
}

// R_VC_CF is a 15-bit cyclic counter, so an active VC passes through exactly 0
// every 2^15 credits. Keying the CF_max lost-update refresh off a value test
// drops such a VC from the refresh permanently.
TEST_F(CbfcTest, CfMaxRefreshesVcWhoseFreedCounterWrappedToZero) {
  advancePastCfMinWindow();
  const auto cfg = makeConfig(); // CF_max 16384 B -> 163.84 ns at 800 Gbps
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  receiver.start();

  // 2 credits per packet x 16384 == 32768 == the 15-bit modulus, so R_VC_CF[1]
  // lands back on exactly 0.
  auto pkt = makeLosslessPkt();
  for (int i = 0; i < 16384; ++i) {
    receiver.onPacketDrained(*pkt);
  }

  auto wrapped = receiver.popCfUpdate();
  ASSERT_TRUE(wrapped.has_value());
  EXPECT_EQ(1u, wrapped->vc1);
  ASSERT_EQ(0u, wrapped->cf1) << "setup must land the counter exactly on zero";

  // Model that CF_Update as lost: no sender ever sees it. CF_max must
  // re-report.
  scheduler_.run(Time::fromNs(200));

  auto refresh = receiver.popCfUpdate();
  ASSERT_TRUE(refresh.has_value())
      << "CF_max refresh skipped a VC whose freed counter had wrapped to 0";
  EXPECT_EQ(1u, refresh->vc1);
  EXPECT_EQ(0u, refresh->cf1);
  receiver.shutdown();
}

// UEC §5.2.5.7.4's Implementation Note calls a LostCredits above 2^14-1
// (`kMaxLostCredits`) a serious error. Applying one anyway would risk a
// silent loss: R_VC_CF is 15-bit (Tbl 5-18), so a reclaim that large on top
// of the credits a drain frees before the next report can reach 2^15. The
// sender reads that advance as zero and returns no credits.
TEST_F(CbfcTest, OversizedCcUpdateIsCountedAndNotApplied) {
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);

  auto pkt = makeLosslessPkt();
  receiver.onPacketStored(*pkt); // R_VC_CC[1] = 2

  EthernetCcPacket cc;
  cc.msgType = CbfcCcMsgType::VC_0_15;
  cc.sVcCc.at(1) = 2 + kMaxLostCredits + 1;
  receiver.rxCcUpdate(cc);

  EXPECT_EQ(1u, receiver.stats().ccUpdateLostCreditsOutOfRangeCount);
  EXPECT_EQ(0u, receiver.stats().leakedCreditsReclaimed);
  // Past the CF_min window, so a report would be emitted had the reclaim
  // applied.
  advancePastCfMinWindow();
  EXPECT_FALSE(receiver.popCfUpdate().has_value());
}

// End-to-end: because the oversized update moved nothing, R_VC_CF still holds
// only what the drain freed, so the next CF_Update returns the sender's 2
// credits and the gate reopens.
TEST_F(CbfcTest, OversizedCcUpdateDoesNotStrandSenderCreditReturns) {
  advancePastCfMinWindow();
  const auto cfg = makeConfig();
  Cbfc sender(&ctx_, cfg, kLinkBps);
  wireNoopWakes(sender);
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  sender.initSenderPortLimit(2); // exactly one 2-credit packet
  ASSERT_TRUE(sender.initSenderReservations());

  auto pkt = makeLosslessPkt();
  ASSERT_TRUE(sender.canTransmit(*pkt));
  sender.onTransmit(*pkt);
  ASSERT_FALSE(sender.canTransmit(*pkt))
      << "the VC should now be out of credit";
  receiver.onPacketStored(*pkt);

  // A corrupt/oversized CC_Update arrives before the drain.
  EthernetCcPacket cc;
  cc.msgType = CbfcCcMsgType::VC_0_15;
  cc.sVcCc.at(1) = 2 + kMaxLostCredits + 1;
  receiver.rxCcUpdate(cc);

  receiver.onPacketDrained(*pkt);
  auto cf = receiver.popCfUpdate();
  ASSERT_TRUE(cf.has_value());
  sender.rxCfUpdate(*cf);
  EXPECT_TRUE(sender.canTransmit(*pkt))
      << "the oversized CC_Update poisoned R_VC_CF and stranded the sender";
}

// A timer chain that survived `shutdown()` would run beside the one the next
// `start()` arms, and every wake would be counted twice; a restart makes that
// visible. `shutdown()` rules it out by destroying both loop frames and then
// cancelling both stopwatches.
//
// Running past a tick first covers a shutdown after the timers have fired and
// re-armed.
TEST_F(CbfcTest, RestartAfterTimersHaveFiredArmsExactlyOneChain) {
  auto cfg = makeConfig();
  // The Tbl 5-27 floor, which is the shortest legal CF_max period.
  cfg.cfMaxTimerBytes() = kCfMaxTimerMinBytes;
  cfg.ccMsgTimerNs() = 100'000; // 100 us: never fires inside the window
  Cbfc cbfc(&ctx_, cfg, kLinkBps);
  wireNoopWakes(cbfc);

  // CF_max refreshes VCs that have freed credits, so the probe needs one to
  // refresh; on a link that has never freed anything the timer still re-arms
  // but deliberately raises no CtlOS edge. Freed before the counter is
  // installed, since freeing raises an edge of its own.
  auto freed = makeLosslessPkt();
  cbfc.onCreditsFreed(*freed, 2);

  int wakes = 0;
  cbfc.setOnCtlosReadyCallback([&]() { ++wakes; });

  cbfc.start();
  // Three CF_max periods, so it has fired and re-armed before the shutdown.
  scheduler_.run(3U * kCfMaxWindow);
  ASSERT_GT(wakes, 0) << "CF_max never fired; the window is mis-sized";

  cbfc.shutdown();
  cbfc.start();
  const int before = wakes;
  scheduler_.run(kCfMaxWindow + kCfMinWindow); // exactly one CF_max boundary

  EXPECT_EQ(1, wakes - before)
      << "the pre-shutdown timer chain survived and is now running in "
         "parallel with the restarted one";
}

// The same guarantee before any tick, while neither timer has fired yet.
TEST_F(CbfcTest, RestartAfterShutdownArmsExactlyOneCfMaxChain) {
  auto cfg = makeConfig();
  cfg.cfMaxTimerBytes() = kCfMaxTimerMinBytes; // the Tbl 5-27 floor
  cfg.ccMsgTimerNs() = 100'000; // 100 us: never fires inside the window
  Cbfc cbfc(&ctx_, cfg, kLinkBps);
  wireNoopWakes(cbfc);

  // See the sibling test: CF_max raises a CtlOS edge only when some VC has
  // credits to refresh.
  auto freed = makeLosslessPkt();
  cbfc.onCreditsFreed(*freed, 2);

  int wakes = 0;
  cbfc.setOnCtlosReadyCallback([&]() { ++wakes; });

  cbfc.start();
  cbfc.shutdown();
  cbfc.start();
  scheduler_.run(kCfMaxWindow + kCfMinWindow); // exactly one CF_max boundary

  EXPECT_EQ(1, wakes) << "the first start()'s timer chain was not cancelled";
}

// A credit is freed as soon as the packet arrives unless its cell is buffered
// in the headroom, in which case the credit is freed only once that cell
// leaves the receiver's buffer. `onCreditsHeld` frees the part that did not
// use the headroom; the caller releases the held part when the packet leaves.
TEST_F(CbfcTest, HoldingNothingFreesTheWholeCost) {
  const auto cfg = makeConfig(); // 64 B credits, PktOvhd 0
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  auto pkt = makeLosslessPkt(); // 70 B -> 2 credits

  receiver.onCreditsHeld(*pkt, 0);

  EXPECT_EQ(2u, receiver.stats().creditsFreed);
}

TEST_F(CbfcTest, HoldingTheWholeCostFreesNothing) {
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  auto pkt = makeLosslessPkt();

  // Holding the packet's whole 2-credit cost is the all-headroom case, where
  // the credit is freed only after the cell de-queues.
  receiver.onCreditsHeld(*pkt, 2);

  EXPECT_EQ(0u, receiver.stats().creditsFreed);
}

TEST_F(CbfcTest, HoldingPartOfTheCostFreesTheRemainder) {
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  auto pkt = makeLosslessPkt();

  receiver.onCreditsHeld(*pkt, 1);

  EXPECT_EQ(1u, receiver.stats().creditsFreed);
}

// The subtraction is unsigned: a held count above the packet's cost would wrap
// and free a huge number of credits for a packet the buffer still holds.
TEST_F(CbfcTest, HoldingMoreThanTheWholeCostFreesNothing) {
  const auto cfg = makeConfig();
  Cbfc receiver(&ctx_, cfg, kLinkBps);
  wireNoopWakes(receiver);
  auto pkt = makeLosslessPkt();

  receiver.onCreditsHeld(*pkt, 99);

  EXPECT_EQ(0u, receiver.stats().creditsFreed)
      << "an over-large held count wrapped the subtraction";
}

} // namespace packsim::cbfc
