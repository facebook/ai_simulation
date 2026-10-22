/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/credit-counter.h"

#include <gtest/gtest.h>

namespace packsim::cbfc {

TEST(CreditCounterTest, AddWrapsAtModulus) {
  CreditCounter c(4); // modulus 16
  c.add(10);
  EXPECT_EQ(10u, c.value());
  c.add(10); // 20 mod 16
  EXPECT_EQ(4u, c.value());
}

// Over-return must stop at empty, not wrap to a near-full reading; see
// `CreditCounter::subSaturating` for why the credit gate depends on this.
TEST(CreditCounterTest, SubSaturatingStopsAtZero) {
  CreditCounter c(4); // modulus 16
  c.add(3);

  c.subSaturating(1); // n < value
  EXPECT_EQ(2u, c.value());

  c.subSaturating(2); // n == value
  EXPECT_EQ(0u, c.value());

  c.add(3);
  c.subSaturating(5); // n > value
  EXPECT_EQ(0u, c.value());
}

TEST(CreditCounterTest, SetMasksToWidth) {
  CreditCounter c(4);
  c.set(0xFF);
  EXPECT_EQ(0xFu, c.value());
}

TEST(CreditCounterTest, CreditReturnCycleMatchesSpec) {
  // §5.2.5.6.3 sender CF_Update math on the 15-bit freed counters, as applied
  // by `CbfcSenderLedger::onCfUpdate`: credits_returned = R_VC_CF - S_VC_CF,
  // then S_VC_CF = R_VC_CF. R_VC_CF arrives as a raw wire value, not as a
  // second counter.
  constexpr uint64_t kRVcCf = 175;
  CreditCounter sVcCf(kFreedWidthBits);
  sVcCf.set(100);
  EXPECT_EQ(75u, sVcCf.forwardDistanceTo(kRVcCf));
  sVcCf.set(kRVcCf);
  EXPECT_EQ(175u, sVcCf.value());
}

TEST(CreditCounterTest, ForwardDistanceToMasksOverWidthWireValue) {
  // R_VC_CF is 15 bits (Tbl 5-18) but rides in a 16-bit
  // `CbfcCfUpdatePayload::cf1`, so the raw value must be masked to the counter
  // width before subtracting.
  CreditCounter sVcCf(kFreedWidthBits);
  sVcCf.set(100);
  EXPECT_EQ(75u, sVcCf.forwardDistanceTo(175));
  EXPECT_EQ(
      75u, sVcCf.forwardDistanceTo(175 + (uint64_t{1} << kFreedWidthBits)));
}

TEST(CreditCounterTest, ForwardDistanceToIsPeerMinusThis) {
  CreditCounter behind(10);
  behind.set(256);
  // (768 - 256) mod 1024 == 512.
  EXPECT_EQ(512u, behind.forwardDistanceTo(768));
  // Wraps forward across zero: on a 4-bit counter, 14 to 2 is 4.
  CreditCounter older(4);
  older.set(14);
  EXPECT_EQ(4u, older.forwardDistanceTo(2));
}

TEST(CreditCounterTest, ModulusMatchesWidth) {
  EXPECT_EQ(16u, CreditCounter(4).modulus());
  EXPECT_EQ(
      uint64_t{1} << kCreditWidthBits,
      CreditCounter(kCreditWidthBits).modulus());
  EXPECT_EQ(
      uint64_t{1} << kFreedWidthBits, CreditCounter(kFreedWidthBits).modulus());
}

// Pins the worked example in the `CreditCounter` class docstring, which
// explains why CBFC reads every residue as a forward distance over the
// counter's full range.
TEST(CreditCounterTest, ForwardDistanceUsesTheFullRange) {
  constexpr uint8_t kWidth = 10; // modulus 1024
  CreditCounter counter(kWidth);
  counter.set(256);

  EXPECT_EQ(444u, counter.forwardDistanceTo(700));
  // A delta of exactly the half-modulus (512 of 1024) is still a plain forward
  // distance, not a rejection.
  EXPECT_EQ(512u, counter.forwardDistanceTo(768));
  // A value numerically below this counter wraps rather than going negative.
  EXPECT_EQ(968u, counter.forwardDistanceTo(200));
  // Only an exact multiple of the modulus is indistinguishable from no change.
  EXPECT_EQ(0u, counter.forwardDistanceTo(256));
}

} // namespace packsim::cbfc
