/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <queue>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/pdes/Event.h"

namespace packsim::pdes {

TEST(RawEvent, SetSortIdPacksContextIndexAndEventId) {
  RawEvent event;
  event.setSortId(/*ctxIdx=*/3, /*eventId=*/7);
  EXPECT_EQ(event.sortId, (uint64_t{3} << RawEvent::kSortIdEventIdBits) | 7u);
}

TEST(RawEvent, ResetClearsFieldsAndBumpsVersion) {
  int sentinel = 0;
  RawEvent event;
  event.timeFs = 123;
  event.sortId = 456;
  event.isActive = false;
  event.func = [] {
  }; // set non-empty so the post-reset func check is meaningful
  event.context = reinterpret_cast<SimulationContext*>(&sentinel); // non-null
  const uint32_t versionBefore = event.version.load();

  event.reset();

  EXPECT_EQ(event.timeFs, 0);
  EXPECT_EQ(event.sortId, 0u);
  EXPECT_TRUE(event.isActive.load());
  EXPECT_FALSE(static_cast<bool>(event.func));
  EXPECT_EQ(event.context, nullptr);
  EXPECT_EQ(event.version.load(), versionBefore + 1);
}

TEST(Event, DefaultConstructedIsInvalidAndInactive) {
  Event event;
  EXPECT_FALSE(event.isValid());
  EXPECT_FALSE(event.isActive());
  EXPECT_EQ(event.getEventTimeFs(), 0u);
}

TEST(Event, ValidHandleReflectsRawEventState) {
  RawEvent raw;
  raw.timeFs = 42;
  Event event(&raw);

  EXPECT_TRUE(event.isValid());
  EXPECT_TRUE(event.isActive());
  EXPECT_EQ(event.getEventTimeFs(), 42u);
}

TEST(Event, CancelMarksRawEventInactive) {
  RawEvent raw;
  raw.timeFs = 10;
  Event event(&raw);

  event.cancel();

  EXPECT_FALSE(raw.isActive.load());
  EXPECT_FALSE(event.isActive());
}

TEST(Event, StaleHandleAfterResetIsInvalidAndCancelIsNoOp) {
  RawEvent raw;
  raw.timeFs = 99;
  Event event(&raw);
  ASSERT_TRUE(event.isValid());

  // reset() bumps the version, so the previously-cached handle is now stale.
  raw.reset();

  EXPECT_FALSE(event.isValid());
  EXPECT_EQ(event.getEventTimeFs(), 0u);
  EXPECT_FALSE(event.isActive());

  // Cancelling through a stale handle must not touch the recycled RawEvent.
  event.cancel();
  EXPECT_TRUE(raw.isActive.load());
}

TEST(EventComparator, OrdersByAscendingTime) {
  RawEvent late;
  late.timeFs = 30;
  RawEvent early;
  early.timeFs = 10;
  RawEvent mid;
  mid.timeFs = 20;

  std::priority_queue<RawEvent*, std::vector<RawEvent*>, EventComparator> pq;
  pq.push(&late);
  pq.push(&early);
  pq.push(&mid);

  EXPECT_EQ(pq.top(), &early);
  pq.pop();
  EXPECT_EQ(pq.top(), &mid);
  pq.pop();
  EXPECT_EQ(pq.top(), &late);
}

TEST(EventComparator, BreaksTiesByAscendingSortId) {
  RawEvent higherSortId;
  higherSortId.timeFs = 5;
  higherSortId.sortId = 100;
  RawEvent lowerSortId;
  lowerSortId.timeFs = 5;
  lowerSortId.sortId = 50;

  std::priority_queue<RawEvent*, std::vector<RawEvent*>, EventComparator> pq;
  pq.push(&higherSortId);
  pq.push(&lowerSortId);

  EXPECT_EQ(pq.top(), &lowerSortId);
}

} // namespace packsim::pdes
