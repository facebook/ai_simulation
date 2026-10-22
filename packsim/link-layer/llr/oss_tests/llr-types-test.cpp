/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "packsim/link-layer/llr/llr-types.h"

namespace packsim::llr {

// These map the LLR enums to the exact strings surfaced in logs and (for
// TxState/AckNackState) reflected through the LlrStats thrift fields, so
// pinning them guards against silent renaming/reordering. An out-of-range value
// must fall through to "?".

TEST(LlrEnumNamesToString, TxState) {
  EXPECT_STREQ(LlrEnumNames::toString(TxState::UNKNOWN), "UNKNOWN");
  EXPECT_STREQ(LlrEnumNames::toString(TxState::LLR_OFF), "LLR_OFF");
  EXPECT_STREQ(LlrEnumNames::toString(TxState::INIT), "INIT");
  EXPECT_STREQ(LlrEnumNames::toString(TxState::ADVANCE), "ADVANCE");
  EXPECT_STREQ(LlrEnumNames::toString(TxState::REPLAY), "REPLAY");
  EXPECT_STREQ(LlrEnumNames::toString(TxState::FLUSH), "FLUSH");
  EXPECT_STREQ(LlrEnumNames::toString(TxState{200}), "?");
}

TEST(LlrEnumNamesToString, AckNackState) {
  EXPECT_STREQ(LlrEnumNames::toString(AckNackState::UNKNOWN), "UNKNOWN");
  EXPECT_STREQ(LlrEnumNames::toString(AckNackState::OFF), "OFF");
  EXPECT_STREQ(LlrEnumNames::toString(AckNackState::SEND_ACKS), "SEND_ACKS");
  EXPECT_STREQ(LlrEnumNames::toString(AckNackState::NACK_SENT), "NACK_SENT");
  EXPECT_STREQ(LlrEnumNames::toString(AckNackState{200}), "?");
}

TEST(LlrEnumNamesToString, FrameBehavior) {
  EXPECT_STREQ(LlrEnumNames::toString(FrameBehavior::UNKNOWN), "UNKNOWN");
  EXPECT_STREQ(LlrEnumNames::toString(FrameBehavior::DISCARD), "DISCARD");
  EXPECT_STREQ(LlrEnumNames::toString(FrameBehavior::BLOCK), "BLOCK");
  EXPECT_STREQ(
      LlrEnumNames::toString(FrameBehavior::BEST_EFFORT), "BEST_EFFORT");
  EXPECT_STREQ(LlrEnumNames::toString(FrameBehavior{200}), "?");
}

TEST(LlrEnumNamesToString, FrameType) {
  EXPECT_STREQ(LlrEnumNames::toString(FrameType::UNKNOWN), "UNKNOWN");
  EXPECT_STREQ(LlrEnumNames::toString(FrameType::LLR_ELIGIBLE), "LLR_ELIGIBLE");
  EXPECT_STREQ(
      LlrEnumNames::toString(FrameType::LLR_INELIGIBLE), "LLR_INELIGIBLE");
  EXPECT_STREQ(LlrEnumNames::toString(FrameType{200}), "?");
}

TEST(LlrEnumNamesToString, FcsStatus) {
  EXPECT_STREQ(LlrEnumNames::toString(FcsStatus::UNKNOWN), "UNKNOWN");
  EXPECT_STREQ(LlrEnumNames::toString(FcsStatus::GOOD), "GOOD");
  EXPECT_STREQ(LlrEnumNames::toString(FcsStatus::POISONED), "POISONED");
  EXPECT_STREQ(LlrEnumNames::toString(FcsStatus::BAD), "BAD");
  EXPECT_STREQ(LlrEnumNames::toString(FcsStatus{200}), "?");
}

TEST(LlrEnumNamesToString, LlrCtlosType) {
  EXPECT_STREQ(LlrEnumNames::toString(LlrCtlosType::LLR_ACK), "LLR_ACK");
  EXPECT_STREQ(LlrEnumNames::toString(LlrCtlosType::LLR_NACK), "LLR_NACK");
  EXPECT_STREQ(LlrEnumNames::toString(LlrCtlosType::LLR_INIT), "LLR_INIT");
  EXPECT_STREQ(
      LlrEnumNames::toString(LlrCtlosType::LLR_INIT_ECHO), "LLR_INIT_ECHO");
  EXPECT_STREQ(LlrEnumNames::toString(LlrCtlosType{200}), "?");
}

TEST(LlrEnumNamesToString, AdmissionResult) {
  EXPECT_STREQ(LlrEnumNames::toString(AdmissionResult::UNKNOWN), "UNKNOWN");
  EXPECT_STREQ(
      LlrEnumNames::toString(AdmissionResult::ADMIT_AND_TX), "ADMIT_AND_TX");
  EXPECT_STREQ(
      LlrEnumNames::toString(AdmissionResult::PASSTHROUGH_TX),
      "PASSTHROUGH_TX");
  EXPECT_STREQ(LlrEnumNames::toString(AdmissionResult::DROP), "DROP");
  EXPECT_STREQ(LlrEnumNames::toString(AdmissionResult::BLOCK), "BLOCK");
  EXPECT_STREQ(LlrEnumNames::toString(AdmissionResult{200}), "?");
}

} // namespace packsim::llr
