/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <folly/Conv.h>
#include <gtest/gtest.h>

#define LOG_POOL_ALLOCATIONS

#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

TEST(PtrTests, TestPtrAssignment) {
  auto ptr = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  ASSERT_EQ(ptr->payloadSize, 1000);
  ASSERT_EQ(ptr->pktType, packsim::PacketType::UDP);

  auto ptrCopy = ptr;

  ASSERT_EQ(ptrCopy->payloadSize, 1000);
  ASSERT_EQ(ptrCopy->pktType, packsim::PacketType::UDP);

  auto ptrMove = std::move(ptr);

  ASSERT_EQ(ptrMove->payloadSize, 1000);
  ASSERT_EQ(ptrMove->pktType, packsim::PacketType::UDP);
}

TEST(PtrTests, TestPtrConstructor) {
  auto ptr = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  ASSERT_EQ(ptr->payloadSize, 1000);
  ASSERT_EQ(ptr->pktType, packsim::PacketType::UDP);

  auto ptrCopy(ptr);

  ASSERT_EQ(ptrCopy->payloadSize, 1000);
  ASSERT_EQ(ptrCopy->pktType, packsim::PacketType::UDP);

  auto ptrMove(std::move(ptr));

  ASSERT_EQ(ptrMove->payloadSize, 1000);
  ASSERT_EQ(ptrMove->pktType, packsim::PacketType::UDP);
}

TEST(PtrTests, TestPtrEmptyAndCopyAssignment) {
  auto ptr = packsim::PacketPtr();
  ASSERT_EQ(ptr.get(), nullptr);

  ptr = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  ASSERT_EQ(ptr->payloadSize, 1000);
  ASSERT_EQ(ptr->pktType, packsim::PacketType::UDP);
}

TEST(PtrTests, TestPtrEmptyAndMoveAssignment) {
  auto ptr = packsim::PacketPtr();
  ASSERT_EQ(ptr.get(), nullptr);

  auto tempPtr = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  ptr = std::move(tempPtr);

  ASSERT_EQ(ptr->payloadSize, 1000);
  ASSERT_EQ(ptr->pktType, packsim::PacketType::UDP);
}

TEST(PtrTests, TestPtrTakeMultiple) {
  auto ptr1 = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  ASSERT_EQ(ptr1->payloadSize, 1000);
  ASSERT_EQ(ptr1->pktType, packsim::PacketType::UDP);

  auto ptr = packsim::PacketPtr();

  {
    auto ptr2 = packsim::createPacket<packsim::PacketType::UDP>(
        /* payloadSize= */ 2000);

    ASSERT_EQ(ptr2->payloadSize, 2000);
    ASSERT_EQ(ptr2->pktType, packsim::PacketType::UDP);
  }

  {
    auto ptr3 = packsim::createPacket<packsim::PacketType::UDP>(
        /* payloadSize= */ 3000);

    ASSERT_EQ(ptr3->payloadSize, 3000);
    ASSERT_EQ(ptr3->pktType, packsim::PacketType::UDP);

    ptr = ptr3;
  }

  ASSERT_EQ(ptr->payloadSize, 3000);
  ASSERT_EQ(ptr->pktType, packsim::PacketType::UDP);
}

TEST(PtrTests, TestReferenceOperator) {
  auto ptr = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  ASSERT_EQ(ptr->payloadSize, 1000);
  ASSERT_EQ(ptr->pktType, packsim::PacketType::UDP);

  ptr.as<packsim::UdpPacket>()->srcPort = 50000;
  ptr.as<packsim::UdpPacket>()->dstPort = 60000;

  {
    auto& pkt = *ptr.as<packsim::UdpPacket>();

    ASSERT_EQ(pkt.srcPort, 50000);
    ASSERT_EQ(pkt.dstPort, 60000);
  }
}

TEST(PtrTests, TestNullPtr) {
  auto ptr = packsim::PacketPtr();
  ASSERT_EQ(ptr.get(), nullptr);

  ASSERT_TRUE(!ptr);

  ptr = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  ASSERT_EQ(ptr->payloadSize, 1000);
  ASSERT_EQ(ptr->pktType, packsim::PacketType::UDP);

  ASSERT_FALSE(!ptr);
}

TEST(PtrTests, TestConstWithOperators) {
  auto ptr = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  ASSERT_EQ(ptr->payloadSize, 1000);
  ASSERT_EQ(ptr->pktType, packsim::PacketType::UDP);

  ptr.as<packsim::UdpPacket>()->srcPort = 50000;
  ptr.as<packsim::UdpPacket>()->dstPort = 60000;

  auto const constPtr = ptr;

  {
    auto& pkt = *constPtr.as<packsim::UdpPacket>();

    ASSERT_EQ(pkt.srcPort, 50000);
    ASSERT_EQ(pkt.dstPort, 60000);
  }

  {
    ASSERT_EQ(constPtr.as<packsim::UdpPacket>()->srcPort, 50000);
    ASSERT_EQ(constPtr.as<packsim::UdpPacket>()->dstPort, 60000);
  }
}

TEST(PtrTests, TestCastAs) {
  auto ptr = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  ASSERT_EQ(ptr->payloadSize, 1000);
  ASSERT_EQ(ptr->pktType, packsim::PacketType::UDP);

  {
    auto udpPtr = ptr.as<packsim::UdpPacket>();

    udpPtr->srcPort = 50000;
    udpPtr->dstPort = 60000;

    ASSERT_EQ(udpPtr->payloadSize, 1000);
    ASSERT_EQ(udpPtr->pktType, packsim::PacketType::UDP);
    ASSERT_EQ(udpPtr->srcPort, 50000);
    ASSERT_EQ(udpPtr->dstPort, 60000);
  }

  {
    auto packetPtr = ptr.as<packsim::Packet>();

    ASSERT_EQ(packetPtr->payloadSize, 1000);
    ASSERT_EQ(packetPtr->pktType, packsim::PacketType::UDP);
  }
}

TEST(PtrTests, TestCastAsBetweenIntermediateTypes) {
  auto ptr = packsim::createPacket<packsim::PacketType::UDP>(
      /* payloadSize= */ 1000);

  // Set fields on various headers.
  {
    auto ipV6Ptr = ptr.as<packsim::Ipv6Packet>();
    ipV6Ptr->dscp = packsim::Ipv6Packet::DscpType::DSCP_AF22;
  }

  {
    auto ethernetPtr = ptr.as<packsim::EthernetPacket>();
    ethernetPtr->tag.color = packsim::thrift::PacketColor::YELLOW;
  }

  {
    auto udpPtr = ptr.as<packsim::UdpPacket>();
    udpPtr->srcPort = 50000;
    udpPtr->dstPort = 60000;
  }

  // Validate the fields on the headers.
  {
    auto ipV6Ptr = ptr.as<packsim::Ipv6Packet>();
    ASSERT_EQ(ipV6Ptr->dscp, packsim::Ipv6Packet::DscpType::DSCP_AF22);
  }

  {
    auto ethernetPtr = ptr.as<packsim::EthernetPacket>();
    ASSERT_EQ(ethernetPtr->tag.color, packsim::thrift::PacketColor::YELLOW);
  }

  {
    auto packetPtr = ptr.as<packsim::Packet>();

    ASSERT_EQ(packetPtr->payloadSize, 1000);
    ASSERT_EQ(packetPtr->pktType, packsim::PacketType::UDP);
  }

  {
    auto udpPtr = ptr.as<packsim::UdpPacket>();

    ASSERT_EQ(udpPtr->srcPort, 50000);
    ASSERT_EQ(udpPtr->dstPort, 60000);
  }
}

TEST(PtrTests, TestWithSimulator) {
  // This is the only test in the file that runs the simulator, so the engine is
  // built here rather than in a fixture every other test would pay for.
  packsim::Scheduler scheduler;
  packsim::SimContext simCtx(&scheduler, 0);

  class LocalSim {
   public:
    // A local class cannot name a variable of its enclosing function, so the
    // context it schedules on is passed to the constructor. Held by pointer,
    // not reference: `scheduleNow` below takes the object by value, and a
    // reference member would delete the implicit assignment operators.
    explicit LocalSim(packsim::SimContext* ctx) : simCtx_(ctx) {}

    void processRedUdpPacket(
        packsim::PacketPtr packet,
        int i,
        std::shared_ptr<int> totalCalls) {
      ASSERT_EQ(packet.as<packsim::PacketType::UDP>()->srcPort, 10000 + i);
      ASSERT_EQ(packet.as<packsim::PacketType::UDP>()->dstPort, 20000 + 2 * i);
      ASSERT_EQ(
          packet.as<packsim::PacketType::UDP>()->tag.color,
          packsim::thrift::PacketColor::RED);
      packet.as<packsim::PacketType::UDP>()->tag.color =
          packsim::thrift::PacketColor::GREEN;
      simCtx_->schedule(
          packsim::Time::fromNs(folly::to<uint64_t>(20 - i)),
          &LocalSim::processGreenUdpPacket,
          this,
          packet,
          i,
          totalCalls);
      ++(*totalCalls);
    }

    void processGreenUdpPacket(
        packsim::PacketPtr packet,
        int i,
        std::shared_ptr<int> totalCalls) {
      ASSERT_EQ(packet.as<packsim::PacketType::UDP>()->srcPort, 10000 + i);
      ASSERT_EQ(packet.as<packsim::PacketType::UDP>()->dstPort, 20000 + 2 * i);
      ASSERT_EQ(
          packet.as<packsim::PacketType::UDP>()->tag.color,
          packsim::thrift::PacketColor::GREEN);
      ++(*totalCalls);
    }

   private:
    // Simulation context the follow-up event is scheduled on.
    packsim::SimContext* simCtx_;
  };

  LocalSim sim(&simCtx);
  auto totalCalls = std::make_shared<int>(0);

  {
    // Create a bunch of packets and schedule them inside this isolated block.
    for (uint8_t i = 0; i < 10; ++i) {
      auto ptr = packsim::createPacket<packsim::PacketType::UDP>(
          /* payloadSize= */ 1000);

      ptr.as<packsim::PacketType::UDP>()->srcPort = 10000 + i;
      ptr.as<packsim::PacketType::UDP>()->dstPort = 20000 + 2 * i;
      ptr.as<packsim::PacketType::UDP>()->tag.color =
          packsim::thrift::PacketColor::RED;

      simCtx.scheduleNow(
          &LocalSim::processRedUdpPacket, sim, ptr, i, totalCalls);
    }
  }

  scheduler.run();

  ASSERT_EQ(*totalCalls, 20);
}

} // namespace packsim
