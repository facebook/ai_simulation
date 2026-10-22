/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc-sender-ledger.h"

#include <optional>
#include <stdexcept>
#include <tuple>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/link-layer/cbfc/cbfc-types.h"
#include "packsim/link-layer/cbfc/credit-counter.h"

namespace packsim::cbfc {

namespace {
// Reserves covering the whole pool leave a shared limit of 0, so the gate
// reduces to `S_VC_CU[x] + c <= vcMin[x]` and the per-VC tier can be
// exercised in isolation.
CbfcSenderLedger
makeVcMinLedger(uint8_t numVcs, uint64_t pool, uint64_t vcMin) {
  CbfcSenderLedger ledger(numVcs);
  ledger.setPortCreditLimit(pool);
  ledger.setVcMinCredits(0, vcMin);
  return ledger;
}
} // namespace

TEST(CbfcSenderLedgerTest, PerVcGateBlocksAtLimit) {
  auto ledger = makeVcMinLedger(
      /*numVcs=*/2, /*pool=*/10, /*vcMin=*/10);
  EXPECT_TRUE(ledger.canTransmit(0, 10));
  ledger.onTransmit(0, 10);
  EXPECT_FALSE(ledger.canTransmit(0, 1)); // 10 + 1 > 10
  EXPECT_EQ(10u, ledger.outstandingVc(0));
}

// A per-VC reserve is handed out unconditionally, so without the port-level
// check two VCs could put 16 credits in flight against a 10-credit pool.
// §5.2.5.6.1 permits extra tiers, but `S_P_CL` itself "shall be observed".
TEST(CbfcSenderLedgerTest, PortLimitHoldsWhenReservesOversubscribeThePool) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(10);
  ledger.setVcMinCredits(0, 8);
  ledger.setVcMinCredits(1, 8);
  ASSERT_FALSE(ledger.reservationsFit())
      << "the setup must be infeasible, or this proves nothing";

  EXPECT_TRUE(ledger.canTransmit(0, 8));
  ledger.onTransmit(0, 8);
  EXPECT_EQ(8u, ledger.outstandingPort());

  // VC1's own reserve would cover this, but the port has 2 credits left.
  EXPECT_FALSE(ledger.canTransmit(1, 8));
  EXPECT_TRUE(ledger.canTransmit(1, 2));
}

TEST(CbfcSenderLedgerTest, PooledGateUsesPortLimitAcrossVcs) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(20);
  ledger.onTransmit(0, 15); // S_P_CU = 15
  EXPECT_FALSE(ledger.canTransmit(1, 6)); // 15 + 6 > 20 (pooled)
  EXPECT_TRUE(ledger.canTransmit(1, 5)); // 15 + 5 == 20
  EXPECT_EQ(15u, ledger.outstandingPort());
}

TEST(CbfcSenderLedgerTest, OnTransmitUpdatesConsumedAndInUse) {
  auto ledger = makeVcMinLedger(
      /*numVcs=*/1, /*pool=*/100, /*vcMin=*/100);
  ledger.onTransmit(0, 7);
  EXPECT_EQ(7u, ledger.consumed(0)); // S_VC_CC
  EXPECT_EQ(7u, ledger.outstandingVc(0)); // S_VC_CU
  EXPECT_EQ(7u, ledger.outstandingPort()); // S_P_CU
}

TEST(CbfcSenderLedgerTest, CfUpdateReturnsCreditsAndUnblocks) {
  auto ledger = makeVcMinLedger(
      /*numVcs=*/1, /*pool=*/10, /*vcMin=*/10);
  ledger.onTransmit(0, 10);
  ASSERT_FALSE(ledger.canTransmit(0, 1));
  // Receiver freed 4 credits → CF_Update carries R_VC_CF = 4.
  EXPECT_EQ(4u, ledger.onCfUpdate(0, /*rVcCf=*/4).creditsReturned);
  EXPECT_EQ(6u, ledger.outstandingVc(0)); // S_VC_CU 10 - 4
  EXPECT_EQ(6u, ledger.outstandingPort()); // S_P_CU 10 - 4
  EXPECT_TRUE(ledger.canTransmit(0, 4)); // 6 + 4 == 10
}

TEST(CbfcSenderLedgerTest, CfUpdateIsIdempotentForRepeatedValue) {
  auto ledger = makeVcMinLedger(
      /*numVcs=*/1, /*pool=*/10, /*vcMin=*/10);
  ledger.onTransmit(0, 10);
  EXPECT_EQ(4u, ledger.onCfUpdate(0, 4).creditsReturned);
  // The CF_max timer makes the receiver re-report a VC's freed count even when
  // it has freed nothing, so a duplicate `R_VC_CF` is routine, not out of
  // range.
  const auto repeat = ledger.onCfUpdate(0, 4);
  EXPECT_EQ(0u, repeat.creditsReturned);
  EXPECT_EQ(6u, ledger.outstandingVc(0));
}

// A value numerically below `S_VC_CF` is the wrapped forward distance, not a
// backward step: CBFC is point-to-point, so the wire is FIFO and the
// receiver's freed count never decreases, which makes every residue
// unambiguous (§5.2.5.6.3 is a plain subtraction).
TEST(CbfcSenderLedgerTest, CfUpdateReadsALowerValueAsAWrappedAdvance) {
  auto ledger = makeVcMinLedger(
      /*numVcs=*/1, /*pool=*/10, /*vcMin=*/10);
  ledger.onTransmit(0, 10);
  ASSERT_EQ(
      4u, ledger.onCfUpdate(0, 4).creditsReturned); // advance S_VC_CF to 4

  // (2 - 4) mod 2^15 = 32766, a forward advance, not a backward step.
  const auto wrapped = ledger.onCfUpdate(0, 2);
  EXPECT_EQ(
      CreditCounter(kFreedWidthBits).modulus() - 2, wrapped.creditsReturned);
  EXPECT_EQ(0u, ledger.outstandingVc(0))
      << "release is clamped to what the VC actually held";
}

// `R_VC_CF` rides a 16-bit wire field (`CbfcCfUpdatePayload::cf1`) over a
// 15-bit counter, so a report can carry bits above the counter width. Masking
// reduces this one to the value already latched in `S_VC_CF`.
TEST(CbfcSenderLedgerTest, CfUpdateMasksAnOverWidthWireValue) {
  auto ledger = makeVcMinLedger(
      /*numVcs=*/1, /*pool=*/10, /*vcMin=*/10);
  ledger.onTransmit(0, 10);
  ASSERT_EQ(4u, ledger.onCfUpdate(0, /*rVcCf=*/4).creditsReturned);

  // Same value with a bit set above the 15-bit counter width.
  const uint64_t overWidth = 4 + (uint64_t{1} << kFreedWidthBits);
  const auto repeat = ledger.onCfUpdate(0, overWidth);

  EXPECT_EQ(0u, repeat.creditsReturned);
  EXPECT_EQ(6u, ledger.outstandingVc(0)) << "counters must not move";
}

// §5.2.4.2's half-modulus is not a boundary here: that rule disambiguates a
// cyclic value only when the direction of travel is unknown. On a
// point-to-point link it is known, so the full 15-bit range is usable.
TEST(CbfcSenderLedgerTest, CfUpdateAtTheHalfModulusIsApplied) {
  CbfcSenderLedger ledger(/*numVcs=*/1);
  ledger.setPortCreditLimit(1024);
  ledger.setVcMinCredits(0, 1024);
  ASSERT_TRUE(ledger.reservationsFit());

  const uint64_t kHalf = CreditCounter::halfModulus(kFreedWidthBits); // 2^14

  EXPECT_EQ(kHalf, ledger.onCfUpdate(0, kHalf).creditsReturned);
  // A wire value one past the half-modulus advances `S_VC_CF` by one credit.
  EXPECT_EQ(1u, ledger.onCfUpdate(0, kHalf + 1).creditsReturned);
}

TEST(CbfcSenderLedgerTest, PerVcGateIsIndependentAcrossVcs) {
  auto ledger = makeVcMinLedger(
      /*numVcs=*/2, /*pool=*/20, /*vcMin=*/10);
  ledger.setVcMinCredits(1, 10);
  ledger.onTransmit(0, 10); // exhaust VC0
  EXPECT_FALSE(ledger.canTransmit(0, 1));
  // VC1's gate is unaffected by VC0's consumption.
  EXPECT_TRUE(ledger.canTransmit(1, 10));
  EXPECT_EQ(0u, ledger.outstandingVc(1));
}

TEST(CbfcSenderLedgerTest, PooledCfUpdateOnOneVcUnblocksAnotherVc) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(10);
  ledger.onTransmit(0, 10); // exhaust the shared pool via VC0
  ASSERT_FALSE(ledger.canTransmit(1, 1));
  // A CF_Update on VC0 frees pooled port credit, unblocking VC1 (§5.2.5.6.3
  // port-level decrement feeding the pooled gate §5.2.5.6.1).
  EXPECT_EQ(4u, ledger.onCfUpdate(0, 4).creditsReturned);
  EXPECT_EQ(6u, ledger.outstandingPort());
  EXPECT_TRUE(ledger.canTransmit(1, 4));
}

// A never-configured pool refuses every transmit silently; `reservationsFit()`
// is the separate signal the bootstrap fails initialization on.
TEST(CbfcSenderLedgerTest, AnUnconfiguredPoolIsReportedNotSilent) {
  CbfcSenderLedger ledger(/*numVcs=*/1);
  EXPECT_FALSE(ledger.reservationsFit())
      << "an unconfigured pool must not pass the init feasibility check";
  EXPECT_FALSE(ledger.canTransmit(0, 1));

  ledger.setPortCreditLimit(10);
  EXPECT_TRUE(ledger.reservationsFit());
  EXPECT_TRUE(ledger.canTransmit(0, 1));
}

// A pool configured to 0 is distinct from a never-configured pool: it fits, it
// just admits nothing.
TEST(CbfcSenderLedgerTest, APoolConfiguredToZeroFitsButAdmitsNothing) {
  CbfcSenderLedger ledger(/*numVcs=*/1);
  ledger.setPortCreditLimit(0);
  EXPECT_TRUE(ledger.reservationsFit());
  EXPECT_FALSE(ledger.canTransmit(0, 1));
}

TEST(CbfcSenderLedgerTest, NoVcMinLeavesWholePoolShared) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(20);
  EXPECT_EQ(20u, ledger.sharedCreditLimit());
  EXPECT_TRUE(ledger.reservationsFit());
}

TEST(CbfcSenderLedgerTest, SharedLimitExcludesVcMinAndOvershoot) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(100);
  ledger.setVcMinCredits(0, 10);
  ledger.setVcMinCredits(1, 20);
  ledger.setOvershootReserve(5);
  EXPECT_EQ(65u, ledger.sharedCreditLimit()); // 100 - (10 + 20) - 5
  EXPECT_TRUE(ledger.reservationsFit());
}

TEST(CbfcSenderLedgerTest, VcMinIsReplacedNotAccumulatedOnReset) {
  CbfcSenderLedger ledger(/*numVcs=*/1);
  ledger.setPortCreditLimit(100);
  ledger.setVcMinCredits(0, 10);
  ledger.setVcMinCredits(0, 30);
  EXPECT_EQ(70u, ledger.sharedCreditLimit()); // not 100 - 10 - 30
}

TEST(CbfcSenderLedgerTest, OverReservedPoolIsReportedInfeasible) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(10);
  ledger.setVcMinCredits(0, 8);
  ledger.setOvershootReserve(5); // 8 + 5 > 10
  EXPECT_FALSE(ledger.reservationsFit());
  EXPECT_EQ(0u, ledger.sharedCreditLimit()); // saturates, never wraps
}

TEST(CbfcSenderLedgerTest, VcMinKeepsALosslessVcNonBlocking) {
  // The point of a per-VC reserve: VC 1 keeps forward progress even when VC 0
  // has taken the entire shared pool (UEC 5.2.5.2 non-blocking requirement).
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(20);
  ledger.setVcMinCredits(1, 4);
  ASSERT_EQ(16u, ledger.sharedCreditLimit());

  ledger.onTransmit(0, 16); // VC 0 drains the shared pool
  EXPECT_FALSE(ledger.canTransmit(0, 1)); // VC 0 has no reserve of its own
  EXPECT_TRUE(ledger.canTransmit(1, 4)); // VC 1's reserve is untouched
  EXPECT_FALSE(ledger.canTransmit(1, 5)); // ... and bounded by it
}

// Control for `VcMinKeepsALosslessVcNonBlocking`: with no reserve, the same
// sequence starves VC 1.
TEST(CbfcSenderLedgerTest, NoVcMinStarvesTheSecondVc) {
  CbfcSenderLedger flat(/*numVcs=*/2);
  flat.setPortCreditLimit(20);
  flat.onTransmit(0, 20);
  EXPECT_FALSE(flat.canTransmit(1, 1));
}

TEST(CbfcSenderLedgerTest, DedicatedCreditsAreChargedBeforeShared) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(20);
  ledger.setVcMinCredits(0, 6);
  ASSERT_EQ(14u, ledger.sharedCreditLimit());

  ledger.onTransmit(0, 4); // wholly inside VC 0's reserve
  EXPECT_TRUE(ledger.canTransmit(1, 14)); // shared pool untouched

  ledger.onTransmit(0, 5); // 2 left in reserve, 3 spill to shared
  EXPECT_TRUE(ledger.canTransmit(1, 11)); // 14 - 3
  EXPECT_FALSE(ledger.canTransmit(1, 12));
}

TEST(CbfcSenderLedgerTest, CfUpdateReleasesSharedBeforeDedicated) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(20);
  ledger.setVcMinCredits(0, 6);

  ledger.onTransmit(0, 10); // 6 dedicated + 4 shared
  ASSERT_TRUE(ledger.canTransmit(1, 10)); // 14 - 4
  ASSERT_FALSE(ledger.canTransmit(1, 11));

  // Returning 4 must give the shared pool all of it back, not the reserve.
  EXPECT_EQ(4u, ledger.onCfUpdate(0, /*rVcCf=*/4).creditsReturned);
  EXPECT_TRUE(ledger.canTransmit(1, 14)); // shared fully restored
}

TEST(CbfcSenderLedgerTest, CfUpdateBeyondLocalSplitDoesNotUnderflow) {
  // After a CC_Update reconcile the receiver's R_VC_CF can outrun what this
  // sender charged locally; the split must clamp rather than wrap.
  CbfcSenderLedger ledger(/*numVcs=*/1);
  ledger.setPortCreditLimit(20);
  ledger.setVcMinCredits(0, 6);
  ledger.onTransmit(0, 8); // 6 dedicated + 2 shared

  // Report 11 against the 8 actually charged, so both `std::min` clamps in
  // `onCfUpdate` bind. The reported delta comes back unclamped (see
  // `CfUpdateResult`); the clamping is visible in the counters below.
  EXPECT_EQ(11u, ledger.onCfUpdate(0, /*rVcCf=*/11).creditsReturned);
  EXPECT_EQ(0u, ledger.outstandingPort())
      << "the over-returned 3 credits underflowed the port counter";
  EXPECT_EQ(14u, ledger.sharedCreditLimit());
  EXPECT_TRUE(ledger.canTransmit(0, 14)); // full pool available again
}

// The overshoot reserve is withheld from the port gate as well as from the
// shared pool, so it holds even when a VC's own reserve would cover the
// packet.
TEST(CbfcSenderLedgerTest, OvershootReserveIsWithheldFromThePortGate) {
  CbfcSenderLedger ledger(/*numVcs=*/1);
  ledger.setPortCreditLimit(20);
  ledger.setVcMinCredits(0, 20); // the reserve alone would admit the whole pool
  ledger.setOvershootReserve(5);

  EXPECT_TRUE(ledger.canTransmit(0, 15));
  EXPECT_FALSE(ledger.canTransmit(0, 16))
      << "the port gate admitted into the overshoot reserve";

  ledger.onTransmit(0, 15);
  EXPECT_FALSE(ledger.canTransmit(0, 1))
      << "port limit minus overshoot is exhausted";
}

// The VC group tier exists so several VCs share one budget: one member's
// transmit must reduce another member's headroom.
TEST(CbfcSenderLedgerTest, VcsSharingAGroupShareItsLimit) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(100);
  ledger.setVcGrpLimit(0, 10);
  ledger.assignVcGroup(0, 0);
  ledger.assignVcGroup(1, 0);

  ledger.onTransmit(0, 7);
  EXPECT_TRUE(ledger.canTransmit(1, 3));
  EXPECT_FALSE(ledger.canTransmit(1, 4))
      << "VC1 got headroom VC0 had already taken from the shared group limit";

  // Returning VC0's credits must hand the group budget back to VC1.
  std::ignore = ledger.onCfUpdate(0, /*rVcCf=*/7);
  EXPECT_TRUE(ledger.canTransmit(1, 10));
}

TEST(CbfcSenderLedgerTest, NulloptReturnsAVcToTheUngroupedPath) {
  CbfcSenderLedger ledger(/*numVcs=*/1);
  ledger.setPortCreditLimit(100);
  ledger.setVcGrpLimit(0, 4);
  ledger.assignVcGroup(0, 0);
  EXPECT_FALSE(ledger.canTransmit(0, 5)) << "the group limit should bind";

  ledger.assignVcGroup(0, std::nullopt);
  EXPECT_TRUE(ledger.canTransmit(0, 5))
      << "an ungrouped VC is gated by the pool, not by a group limit";
}

// A VC group is a burst-absorption pool shared by several VCs, sitting between
// each VC's own reserve and the global shared limit; both the group limit and
// the global limit must hold.
TEST(CbfcSenderLedgerTest, VcGrpLimitBlocksBeforeTheGlobalLimitWould) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(100); // global shared limit is generous
  ledger.assignVcGroup(0, 0);
  ledger.setVcGrpLimit(0, 10); // the group is the binding constraint

  EXPECT_TRUE(ledger.canTransmit(0, 10));
  ledger.onTransmit(0, 10);
  // The global pool has 90 credits left, but the group is full.
  EXPECT_FALSE(ledger.canTransmit(0, 1));
  EXPECT_TRUE(ledger.canTransmit(1, 50)) << "an ungrouped VC is unaffected";
}

TEST(CbfcSenderLedgerTest, GlobalLimitBlocksEvenWhenTheGroupWouldAdmit) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(10); // global shared limit is the tight one
  ledger.assignVcGroup(0, 0);
  ledger.setVcGrpLimit(0, 1000); // deliberately oversubscribed

  ledger.onTransmit(0, 10);
  EXPECT_FALSE(ledger.canTransmit(0, 1))
      << "an oversubscribed group limit must not let a VC past the pool";
}

TEST(CbfcSenderLedgerTest, VcGrpCreditsAreReleasedOnCfUpdate) {
  CbfcSenderLedger ledger(/*numVcs=*/1);
  ledger.setPortCreditLimit(100);
  ledger.assignVcGroup(0, 0);
  ledger.setVcGrpLimit(0, 10);

  ledger.onTransmit(0, 10);
  ASSERT_FALSE(ledger.canTransmit(0, 1));
  // A returned shared credit is freed from both the VC and its VC group.
  EXPECT_EQ(4u, ledger.onCfUpdate(0, /*rVcCf=*/4).creditsReturned);
  EXPECT_TRUE(ledger.canTransmit(0, 4))
      << "the group's usage was not decremented on credit return";
}

TEST(CbfcSenderLedgerTest, VcMinIsDrawnBeforeTheGroupPool) {
  CbfcSenderLedger ledger(/*numVcs=*/1);
  ledger.setPortCreditLimit(20);
  ledger.setVcMinCredits(0, 5);
  ledger.assignVcGroup(0, 0);
  ledger.setVcGrpLimit(0, 0); // group may draw nothing from the shared pool

  // Entirely inside its own reserve, so the empty group limit is irrelevant.
  EXPECT_TRUE(ledger.canTransmit(0, 5));
  ledger.onTransmit(0, 5);
  // Now it would need shared credits, and the group forbids them.
  EXPECT_FALSE(ledger.canTransmit(0, 1));
}

// The receiver's R_VC_CF is authoritative and, after a CC_Update reconcile,
// can free more than this side booked as in use. Wrapping the in-use counters
// there would leave them just under 2^20, which reads as a saturated port.
TEST(CbfcSenderLedgerTest, OverReturnSaturatesTheInUseCountersAtZero) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(20);
  ledger.onTransmit(0, 4);
  ASSERT_EQ(4u, ledger.outstandingVc(0));

  // Ten returned against four outstanding.
  std::ignore = ledger.onCfUpdate(0, 10);

  EXPECT_EQ(0u, ledger.outstandingVc(0));
  EXPECT_TRUE(ledger.canTransmit(0, 20))
      << "the port reads as saturated, so the link is stalled";
}

// S_P_CU must stay equal to the sum of the per-VC tier usages. Subtracting the
// raw reported figure on an over-return would drop it below that sum, and the
// port gate would then admit credits another VC still holds in flight.
TEST(CbfcSenderLedgerTest, OverReturnLeavesThePortCounterMatchingTheTiers) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(20);
  ledger.onTransmit(0, 4);
  ledger.onTransmit(1, 6); // still in flight on the other VC
  ASSERT_EQ(10u, ledger.outstandingPort());

  // Ten returned on VC 0 against the four it actually charged.
  std::ignore = ledger.onCfUpdate(0, 10);

  EXPECT_EQ(0u, ledger.outstandingVc(0));
  EXPECT_EQ(6u, ledger.outstandingVc(1)) << "VC 1's credits were not returned";
  EXPECT_EQ(
      ledger.outstandingVc(0) + ledger.outstandingVc(1),
      ledger.outstandingPort())
      << "the port counter no longer equals the sum of the per-VC usages";
}

// An out-of-range VC group must fail at configuration time, not on the transmit
// path halfway through a simulation.
TEST(CbfcSenderLedgerTest, RejectsAVcGroupOutsideTheSupportedRange) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  EXPECT_THAT(
      [&] { ledger.assignVcGroup(/*vc=*/0, /*grp=*/kLedgerMaxVcGroups); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("VC group")));
}

// The pool bounds how far `R_VC_CF` advances between two CF_Updates, and a
// full-modulus advance reads as no advance at all, so `setPortCreditLimit`
// accepts at most 2^15 - 1 -- stricter than Tbl 5-27's TotalCredits cap.
TEST(CbfcSenderLedgerTest, RejectsAPoolThatCanOutrunTheFreedCounter) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  const uint64_t modulus = uint64_t{1} << kFreedWidthBits;
  EXPECT_THAT(
      [&] { ledger.setPortCreditLimit(modulus); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("freed-counter modulus")));
  EXPECT_NO_THROW(ledger.setPortCreditLimit(modulus - 1));
}

// Zero would size every per-VC vector empty, and `checkVc` would then print
// the range as "0..-1".
TEST(CbfcSenderLedgerTest, RejectsAVcCountOutsideTheUecRange) {
  EXPECT_THAT(
      [] { CbfcSenderLedger ledger(/*numVcs=*/0); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("numVcs 0 is outside")));
  EXPECT_THAT(
      [] { CbfcSenderLedger ledger(/*numVcs=*/kUecMaxVcs + 1); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("is outside")));
  EXPECT_NO_THROW(CbfcSenderLedger{/*numVcs=*/kUecMaxVcs});
}

TEST(CbfcSenderLedgerTest, RejectsAVcIdOutsideTheConfiguredRange) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  EXPECT_THAT(
      [&] { ledger.setVcMinCredits(/*vc=*/2, 4); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("VC 2 is outside")));
  EXPECT_THAT(
      [&] { ledger.assignVcGroup(/*vc=*/2, /*grp=*/0); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("VC 2 is outside")));
}

// Shrinking a reserve below its live usage is a documented hazard; an unsigned
// wrap there would leave the VC's shared and group gating silently off.
TEST(
    CbfcSenderLedgerTest,
    ShrinkingAReserveBelowItsUsageDoesNotDisableTheGate) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(20);
  ledger.setVcMinCredits(0, 10);
  ledger.onTransmit(0, 10); // all 10 charged to the reserve

  ledger.setVcMinCredits(0, 2); // now below vcMinUsed_

  // Shared room is 20 - 2 = 18, and VC 0 already holds nothing of it, so a
  // request past the pool must still be refused rather than waved through.
  EXPECT_FALSE(ledger.canTransmit(0, 19));
}

// With no group assigned the three-tier gate must reduce exactly to the
// single-pool gate `S_P_CU + c <= S_P_CL`.
TEST(CbfcSenderLedgerTest, NoGroupsConfiguredGatesOnThePoolAlone) {
  constexpr uint64_t kPool = 20;
  CbfcSenderLedger ledger(/*numVcs=*/2);
  ledger.setPortCreditLimit(kPool);

  for (const uint64_t c : {uint64_t{1}, uint64_t{15}, kPool, kPool + 1}) {
    EXPECT_EQ(c <= kPool, ledger.canTransmit(0, c)) << "c=" << c;
  }
}

// A group whose limit cannot bind must be transparent: the group tier only
// ever subtracts headroom, never adds it.
TEST(CbfcSenderLedgerTest, ANonBindingVcGrpLimitLeavesTheGateUnchanged) {
  constexpr uint64_t kPool = 20;
  CbfcSenderLedger grouped(/*numVcs=*/2);
  grouped.setPortCreditLimit(kPool);
  grouped.setVcGrpLimit(/*grp=*/0, kPool * 2); // strictly above the pool
  grouped.assignVcGroup(/*vc=*/0, /*grp=*/0);

  CbfcSenderLedger flat(/*numVcs=*/2);
  flat.setPortCreditLimit(kPool);

  for (const uint64_t c : {uint64_t{1}, uint64_t{15}, kPool, kPool + 1}) {
    EXPECT_EQ(flat.canTransmit(0, c), grouped.canTransmit(0, c)) << "c=" << c;
  }
}

// Symmetric with `assignVcGroup`'s range check: a group id past the supported
// range must be named here, not surface as a message-less
// `out_of_range` from the `.at()` write inside `setVcGrpLimit`.
TEST(CbfcSenderLedgerTest, RejectsAVcGrpLimitForAGroupOutsideTheRange) {
  CbfcSenderLedger ledger(/*numVcs=*/2);
  EXPECT_THAT(
      [&] { ledger.setVcGrpLimit(/*grp=*/kLedgerMaxVcGroups, /*credits=*/4); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("is outside")));
  EXPECT_NO_THROW(
      ledger.setVcGrpLimit(/*grp=*/kLedgerMaxVcGroups - 1, /*credits=*/4));
}

} // namespace packsim::cbfc
