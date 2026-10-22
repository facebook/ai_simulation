/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <set>
#include <string>

#include <gtest/gtest.h>

#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/queues/queues.h"

namespace packsim {
namespace {

TEST(SimpleQueueTests, TestInitialCounters) {
  auto queue = std::make_unique<SimplePacketQueue>();
  EXPECT_EQ(queue->getCurrentBytes(), 0);
  EXPECT_EQ(queue->getCurrentPackets(), 0);
}

TEST(SimpleQueueTests, TestQueueOperations) {
  auto queue = std::make_unique<SimplePacketQueue>();
  // create a packet and push into the queue
  auto packet =
      createPacket<PacketType::UDP>(128); // std::make_unique<UdpPacket>(128);
  queue->enqueuePacket(packet);
  // check the queue size in bytes and packets
  EXPECT_EQ(queue->getCurrentPackets(), 1);
  EXPECT_EQ(queue->getCurrentBytes(), packet->getSize());
  // create another packet to peek
  PacketPtr packet2;
  queue->peekPacket(&packet2);
  EXPECT_EQ(packet2->getSize(), packet->getSize());
  // after peek, the queue still contains the packet
  EXPECT_EQ(queue->getCurrentPackets(), 1);
  EXPECT_EQ(queue->getCurrentBytes(), packet->getSize());
  // create another packet to pop
  PacketPtr packet3;
  queue->dequeuePacket(&packet3);
  EXPECT_EQ(packet3->getSize(), packet->getSize());
  // after pop, the queue should be empty
  EXPECT_EQ(queue->getCurrentPackets(), 0);
  EXPECT_EQ(queue->getCurrentBytes(), 0);
}

// Cycling one packet through empty visits the `kMinPacketQueueSlots` adjacent
// slots of one buffer: the queue keeps its minimum buffer across every drain
// instead of freeing it and reallocating on the next refill.
TEST(SimpleQueueTests, RefillAfterDrainReusesTheMinimumBuffer) {
  SimplePacketQueue queue;
  std::set<const PacketPtr*> headSlots;
  PacketPtr popped;
  for (uint32_t i = 0; i < 4 * kMinPacketQueueSlots; ++i) {
    queue.enqueuePacket(createPacket<PacketType::UDP>(128));
    headSlots.insert(&queue.queue.front());
    queue.dequeuePacket(&popped);
  }

  ASSERT_EQ(headSlots.size(), kMinPacketQueueSlots);
  EXPECT_EQ(*headSlots.rbegin() - *headSlots.begin(), kMinPacketQueueSlots - 1);
}

} // namespace
} // namespace packsim
