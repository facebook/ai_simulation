/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc-credit-tracker.h"

#include <stdexcept>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/link-layer/cbfc/cbfc-types.h"
#include "packsim/link-layer/cbfc/credit-counter.h"

namespace packsim::cbfc {

TEST(CbfcCreditTrackerTest, StoreIncrementsConsumed) {
  CbfcCreditTracker tracker(/*numVcs=*/2);
  tracker.onStored(0, 5);
  tracker.onStored(0, 2);
  EXPECT_EQ(7u, tracker.consumed(0));
  EXPECT_EQ(0u, tracker.consumed(1));
}

TEST(CbfcCreditTrackerTest, DrainIncrementsFreed) {
  CbfcCreditTracker tracker(/*numVcs=*/1);
  tracker.onStored(0, 5);
  tracker.onDrained(0, 3);
  EXPECT_EQ(3u, tracker.freed(0));
}

TEST(CbfcCreditTrackerTest, CcUpdateReclaimsLeakedCredits) {
  CbfcCreditTracker tracker(/*numVcs=*/1);
  // Receiver only saw 10 credits stored, but the sender consumed 13 (3
  // credits leaked to link-error drops). The CC_Update carries S_VC_CC = 13.
  tracker.onStored(0, 10);
  EXPECT_EQ(3u, tracker.onCcUpdate(0, /*sVcCc=*/13).lostCredits);
  EXPECT_EQ(3u, tracker.freed(0)); // leaked credits become freed
  EXPECT_EQ(13u, tracker.consumed(0)); // R_VC_CC adopts S_VC_CC
}

TEST(CbfcCreditTrackerTest, CcUpdateNoLeakWhenInSync) {
  CbfcCreditTracker tracker(/*numVcs=*/1);
  tracker.onStored(0, 5);
  EXPECT_EQ(0u, tracker.onCcUpdate(0, /*sVcCc=*/5).lostCredits);
  EXPECT_EQ(0u, tracker.freed(0));
  EXPECT_EQ(5u, tracker.consumed(0));
}

TEST(CbfcCreditTrackerTest, CcUpdateIgnoresBehindValue) {
  CbfcCreditTracker tracker(/*numVcs=*/1);
  tracker.onStored(0, 10);
  // A CC_Update whose S_VC_CC is behind R_VC_CC comes out of the modular
  // subtraction as a large positive residue, so it is rejected out of range
  // rather than applied. See `CbfcCreditTracker::onCcUpdate`.
  EXPECT_EQ(0u, tracker.onCcUpdate(0, /*sVcCc=*/5).lostCredits);
  EXPECT_EQ(0u, tracker.freed(0));
  EXPECT_EQ(10u, tracker.consumed(0));
}

TEST(CbfcCreditTrackerTest, CcUpdateReclaimsLostCreditsAtSpecBound) {
  // `kMaxLostCredits` bounds the reclaim even though the subtraction runs on
  // the wider 20-bit `R_VC_CC` (Tbl 5-18). A value exactly at the cap is still
  // accepted.
  CbfcCreditTracker tracker(/*numVcs=*/1);
  tracker.onStored(0, 100); // R_VC_CC = 100
  const auto res = tracker.onCcUpdate(0, /*sVcCc=*/100 + kMaxLostCredits);
  EXPECT_EQ(kMaxLostCredits, res.lostCredits);
  EXPECT_FALSE(res.lostCreditsOutOfRange);
  EXPECT_EQ(100u + kMaxLostCredits, tracker.consumed(0));
  EXPECT_EQ(kMaxLostCredits, tracker.freed(0));
}

// Symmetric with the sender ledger: a bad VC count must name itself at init,
// not surface as a message-less out_of_range on the first packet.
TEST(CbfcCreditTrackerTest, RejectsAVcCountOutsideTheUecRange) {
  EXPECT_THAT(
      [] { CbfcCreditTracker tracker(/*numVcs=*/0); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("numVcs 0 is outside")));
  EXPECT_THAT(
      [] { CbfcCreditTracker tracker(/*numVcs=*/kUecMaxVcs + 1); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("is outside")));
  EXPECT_NO_THROW(CbfcCreditTracker{/*numVcs=*/kUecMaxVcs});
}

// One credit past `kMaxLostCredits` is the §5.2.5.7.4 serious error: rejected
// whole, not truncated.
TEST(CbfcCreditTrackerTest, CcUpdateRejectsLostCreditsAboveSpecBound) {
  CbfcCreditTracker tracker(/*numVcs=*/1);
  tracker.onStored(0, 100);
  const auto res = tracker.onCcUpdate(0, /*sVcCc=*/100 + kMaxLostCredits + 1);
  EXPECT_EQ(0u, res.lostCredits);
  EXPECT_TRUE(res.lostCreditsOutOfRange);
  // Neither counter moved, so the gap persists: every later CC_Update for this
  // VC carries the same oversized LostCredits and is rejected too.
  EXPECT_EQ(100u, tracker.consumed(0));
  EXPECT_EQ(0u, tracker.freed(0));
}

// §5.2.5.7.4's Implementation Note names two error conditions, LostCredits
// below zero and above 2^14-1. Under modular subtraction the first is
// unrepresentable -- a would-be-negative value returns as a large positive
// residue -- so both collapse into the single upper-bound check. A delta at
// the 20-bit half-modulus is far above 2^14-1 and exercises that check.
TEST(CbfcCreditTrackerTest, CcUpdateReportsADeltaFarAboveTheLostCreditsCap) {
  const uint64_t half = CreditCounter::halfModulus(kCreditWidthBits);
  CbfcCreditTracker tracker(/*numVcs=*/1);
  tracker.onStored(0, 100);
  const auto res = tracker.onCcUpdate(0, /*sVcCc=*/100 + half);
  EXPECT_EQ(0u, res.lostCredits);
  EXPECT_TRUE(res.lostCreditsOutOfRange);
  EXPECT_EQ(100u, tracker.consumed(0));
  EXPECT_EQ(0u, tracker.freed(0));
}

TEST(CbfcCreditTrackerTest, CcUpdateReclaimsAcrossWrap) {
  // R_VC_CC sits near the top of the 20-bit range; a CC_Update whose S_VC_CC
  // has wrapped just past zero yields a small forward delta that is reclaimed.
  const uint64_t mod = CreditCounter(kCreditWidthBits).modulus();
  CbfcCreditTracker tracker(/*numVcs=*/1);
  tracker.onStored(0, mod - 5); // R_VC_CC = 2^20 - 5
  // S_VC_CC wrapped to 3: forward delta = (3 - (mod - 5)) mod mod = 8.
  EXPECT_EQ(8u, tracker.onCcUpdate(0, /*sVcCc=*/3).lostCredits);
  EXPECT_EQ(3u, tracker.consumed(0));
  EXPECT_EQ(8u, tracker.freed(0));
}

TEST(CbfcCreditTrackerValidateTest, RejectsACreditSizeOfZero) {
  EXPECT_THAT(
      [&] { CbfcCreditTracker::validateCreditSize(0, "zero"); },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::HasSubstr("credit size")));
}

TEST(CbfcCreditTrackerValidateTest, RejectsANegativeCreditSize) {
  EXPECT_THAT(
      [&] { CbfcCreditTracker::validateCreditSize(-1, "negative"); },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::HasSubstr("credit size")));
}

TEST(CbfcCreditTrackerValidateTest, RejectsACreditSizeAboveTheWireField) {
  EXPECT_THAT(
      [&] {
        CbfcCreditTracker::validateCreditSize(
            kMaxCreditSizeBytes + 1, "too-wide");
      },
      testing::ThrowsMessage<std::invalid_argument>(
          testing::HasSubstr("credit size")));
}

TEST(CbfcCreditTrackerValidateTest, AcceptsACreditSizeAtTheWireFieldMaximum) {
  EXPECT_NO_THROW(
      CbfcCreditTracker::validateCreditSize(
          kMaxCreditSizeBytes, "at-wire-max"));
}

} // namespace packsim::cbfc
