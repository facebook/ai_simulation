/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <memory>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/transport_stats_types.h"
#include "packsim/packet/base.h"
#include "packsim/packet/roce.h"
#include "packsim/transport/oss_tests/roce-socket-test-fixture.h"
#include "packsim/transport/roce-base.h"

namespace packsim {

// Packet counters of a RoceSocketBase and its transport: what the socket's own
// stats and the transport's per-CoS stats record for a scripted received or
// sent packet. Data, ACK/NACK and CNP each get their own CoS, so a count under
// the wrong one shows.
class RoceSocketStatsTest : public RoceSocketTestBase {
 public:
  void SetUp() override {
    // Before the base builds the transport, which sizes its per-CoS stats from
    // the NIC config.
    nicCfg_.dataCos() = kDataCos;
    nicCfg_.ackCos() = kAckCos;
    RoceSocketTestBase::SetUp();
    socket_ = makeSocket(/*factory=*/nullptr);
  }

  // The transport's per-CoS counters for `cos`.
  packsim::thrift::IntfPktStats cosStats(uint8_t cos) const {
    return transport_->getNicStatsSummaryResults()
        .nicStats()
        ->perCosStats()
        ->at(cos);
  }

  static PacketPtr makeCeMarked(PacketPtr pkt) {
    pkt.as<Ipv6Packet>()->ecn = Ipv6Packet::EcnType::ECN_CE;
    return pkt;
  }

  static PacketPtr makeDataPacket() {
    auto pkt = createPacket<PacketType::ROCE>(kMsgBytes);
    auto* rocePkt = pkt.as<RocePacket>();
    rocePkt->setOpcode(/*isFirstPkt=*/true, /*isLastPkt=*/true);
    rocePkt->psn = kFirstPsn;
    return pkt;
  }

  static PacketPtr makeCnpPacket() {
    auto pkt = createPacket<PacketType::ROCE>(0);
    pkt.as<RocePacket>()->isCnp = true;
    return pkt;
  }

  // A CE-marked ACK as `socket` would have sent it, for the transport to see
  // leave the NIC.
  static PacketPtr makeSentCeMarkedAck(const Socket& socket) {
    auto pkt = makeCeMarked(createPacket<PacketType::ROCE>(0));
    auto* rocePkt = pkt.as<RocePacket>();
    rocePkt->isAck = true;
    rocePkt->conn = socket.getConnection();
    return pkt;
  }

  static PacketPtr makeNackPacket() {
    auto pkt = createPacket<PacketType::ROCE>(0);
    auto* rocePkt = pkt.as<RocePacket>();
    rocePkt->isNack = true;
    rocePkt->nackTimer = UINT8_MAX;
    return pkt;
  }

 protected:
  static constexpr uint32_t kMsgBytes = 64;
  // The PSN a fresh socket expects first.
  static constexpr uint32_t kFirstPsn = 1;
  static constexpr uint8_t kDataCos = 2;
  static constexpr uint8_t kAckCos = 3;
  // DcqcnConfig.npCnpPrio's default.
  static constexpr uint8_t kCnpCos = 1;
  // Local QPN of the sockets the sent-packet tests register on the transport.
  static constexpr uint32_t kSentQpn = 5;

  std::unique_ptr<RoceSocketBase> socket_;
  Message expectedMsg_{
      .sizeBytes = kMsgBytes,
      .srcIdx = dstId_,
      .dstIdx = srcId_,
  };
};

TEST_F(RoceSocketStatsTest, CeMarkedDataPacketCountsAsReceivedEcn) {
  socket_->recv(&expectedMsg_);

  socket_->onPacketRcvd(makeCeMarked(makeDataPacket()));

  EXPECT_EQ(*socket_->getStats().recvdDataPkts(), 1);
  EXPECT_EQ(*socket_->getStats().recvdEcnPkts(), 1);
  EXPECT_EQ(*cosStats(kDataCos).recvdEcnPkts(), 1);
  EXPECT_EQ(*cosStats(kAckCos).recvdEcnPkts(), 0);
}

TEST_F(RoceSocketStatsTest, UnmarkedDataPacketDoesNotCountEcn) {
  socket_->recv(&expectedMsg_);

  socket_->onPacketRcvd(makeDataPacket());

  EXPECT_EQ(*socket_->getStats().recvdDataPkts(), 1);
  EXPECT_EQ(*socket_->getStats().recvdEcnPkts(), 0);
  EXPECT_EQ(*cosStats(kDataCos).recvdEcnPkts(), 0);
}

TEST_F(RoceSocketStatsTest, CeMarkedCnpCountsEcnUnderCnpCos) {
  socket_->onPacketRcvd(makeCeMarked(makeCnpPacket()));

  EXPECT_EQ(*socket_->getStats().recvdEcnPkts(), 1);
  EXPECT_EQ(*cosStats(kCnpCos).recvdEcnPkts(), 1);
  EXPECT_EQ(*cosStats(kDataCos).recvdEcnPkts(), 0);
}

TEST_F(RoceSocketStatsTest, CeMarkedNackCountsEcnUnderAckCos) {
  socket_->onPacketRcvd(makeCeMarked(makeNackPacket()));

  EXPECT_EQ(*socket_->getStats().recvdEcnPkts(), 1);
  EXPECT_EQ(*cosStats(kAckCos).recvdEcnPkts(), 1);
  EXPECT_EQ(*cosStats(kDataCos).recvdEcnPkts(), 0);
}

TEST_F(RoceSocketStatsTest, ReceivedNackCountsUnderAckCos) {
  socket_->onPacketRcvd(makeNackPacket());

  EXPECT_EQ(*socket_->getStats().recvdNackPkts(), 1);
  EXPECT_EQ(*cosStats(kAckCos).recvdNackPkts(), 1);
  EXPECT_EQ(*cosStats(kCnpCos).recvdNackPkts(), 0);
}

TEST_F(RoceSocketStatsTest, SentCeMarkedAckCountsPerSocketAndUnderAckCos) {
  Socket* const socket = transport_->connect(
      dstId_, /*port=*/1234, kSentQpn, /*remoteQPN=*/kSentQpn);

  transport_->onPacketSent(makeSentCeMarkedAck(*socket), /*ethPort=*/nullptr);

  EXPECT_EQ(*socket->getStats().sentEcnPkts(), 1);
  EXPECT_EQ(*cosStats(kAckCos).sentEcnPkts(), 1);
  EXPECT_EQ(*cosStats(kDataCos).sentEcnPkts(), 0);
}

// An endpoint that routes by port swaps the packet's `conn` for a port-level
// connection and keeps the socket's own in `socketLevelConn`.
TEST_F(
    RoceSocketStatsTest,
    SentCeMarkedPacketWithPortLevelConnCountsPerSocket) {
  Socket* const socket = transport_->connect(
      dstId_, /*port=*/1234, kSentQpn, /*remoteQPN=*/kSentQpn);
  const QPConnection portLevelConn(
      srcId_, dstId_, /*srcQPN=*/kSentQpn, /*dstQPN=*/kSentQpn);
  auto pkt = makeSentCeMarkedAck(*socket);
  pkt.as<RocePacket>()->socketLevelConn = socket->getConnection();
  pkt.as<RocePacket>()->conn = &portLevelConn;

  transport_->onPacketSent(pkt, /*ethPort=*/nullptr);

  EXPECT_EQ(*socket->getStats().sentEcnPkts(), 1);
  EXPECT_EQ(*cosStats(kAckCos).sentEcnPkts(), 1);
}

// An iterative workload unregisters its sockets when a rank completes, while
// their packets can still be queued at the NIC, and a later socket may take the
// same QPN. A marked packet of the retired socket that leaves afterwards still
// counts per CoS, and is counted on neither socket.
TEST_F(RoceSocketStatsTest, SentCeMarkedPacketOfRetiredSocketCountsPerCosOnly) {
  Socket* const retired = transport_->connect(
      dstId_, /*port=*/1234, kSentQpn, /*remoteQPN=*/kSentQpn);
  const auto pkt = makeSentCeMarkedAck(*retired);
  transport_->unregisterSocket(dstId_, kSentQpn);
  Socket* const successor = transport_->connect(
      dstId_, /*port=*/1235, kSentQpn, /*remoteQPN=*/kSentQpn);
  ASSERT_NE(successor, retired);

  EXPECT_NO_THROW(transport_->onPacketSent(pkt, /*ethPort=*/nullptr));

  EXPECT_EQ(*cosStats(kAckCos).sentEcnPkts(), 1);
  EXPECT_EQ(*retired->getStats().sentEcnPkts(), 0);
  EXPECT_EQ(*successor->getStats().sentEcnPkts(), 0);
}

} // namespace packsim
