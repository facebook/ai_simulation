/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/transport/transport.h"
#include "packsim/workloads/parallel_qp/channel.h"
#include "packsim/workloads/parallel_qp/params.h"

namespace packsim {

class ChannelSendRecvTest : public ::testing::Test {
 protected:
  // The callbacks hand back a raw Message* owned by the channel; the null
  // check happens once here so every assertion works on a reference.
  using MessageRefs = std::vector<std::reference_wrapper<Message>>;

  static constexpr uint32_t kMessageSizeBytes = 1000;
  static constexpr uint32_t kWorkloadId = 7;
  static constexpr uint32_t kFirstLocalQPN = 100;
  static constexpr uint32_t kFirstRemoteQPN = 200;
  static constexpr uint32_t kFirstOutstandingSendChunkId = 7;
  static constexpr uint32_t kFirstOutstandingRecvChunkId = 5;
  static constexpr uint32_t kUnexpectedChunkId = 99;

  void SetUp() override {
    rankToIdList_[0] = {{10}};
    rankToIdList_[1] = {{20}};
    smcb_ = [this](Message* msg) { sentMsgs_.emplace_back(deref(msg)); };
    rmcb_ = [this](Message* msg) { recvdMsgs_.emplace_back(deref(msg)); };
  }

  Channel& makeChannel(const uint32_t numQPsPerChannel) {
    std::vector<workload::thrift::QPNPair> qpList;
    for (uint32_t i = 0; i < numQPsPerChannel; i++) {
      workload::thrift::QPNPair qpnPair;
      qpnPair.local() = kFirstLocalQPN + i;
      qpnPair.remote() = kFirstRemoteQPN + i;
      qpList.emplace_back(qpnPair);
    }
    const ChannelParams params{
        .smcb = smcb_,
        .rmcb = rmcb_,
        .numChannels = 1,
        .numQPsPerChannel = numQPsPerChannel,
        .messageSizeBytes = kMessageSizeBytes,
        .workloadId = kWorkloadId,
        .workload = nullptr,
        .rankToIdList = rankToIdList_,
        .qpPairMap = qpPairMap_,
        .rank = 0,
    };
    return channel_.emplace(params, qpList);
  }

  Message& sentMsg(const size_t index) {
    return sentMsgs_.at(index);
  }

  Message& recvdMsg(const size_t index) {
    return recvdMsgs_.at(index);
  }

  // Sends from rank 0 to rank 1 with fixed, uninteresting metadata.
  void sendToRankOne(Channel& channel, const uint32_t sizeBytes) {
    channel.send(
        /*fromRank=*/0,
        /*toRank=*/1,
        sizeBytes,
        /*chunkId=*/0,
        /*channelId=*/0,
        /*endpointId=*/0,
        /*sendDelayNs=*/0,
        /*writeImm=*/false,
        /*write=*/false,
        /*writeImmData=*/0);
  }

  // Queues two single-chunk transfers on one QP, leaving chunk ids 7 then 8
  // outstanding on the send side.
  void sendTwoChunksOnOneQp(Channel& channel) {
    for (const uint32_t chunkId :
         {kFirstOutstandingSendChunkId, kFirstOutstandingSendChunkId + 1}) {
      channel.send(
          /*fromRank=*/0,
          /*toRank=*/1,
          /*sizeBytes=*/1000,
          chunkId,
          /*channelId=*/0,
          /*endpointId=*/0,
          /*sendDelayNs=*/0,
          /*writeImm=*/false,
          /*write=*/false,
          /*writeImmData=*/0);
    }
  }

  // Posts two single-chunk receives on one QP, leaving chunk ids 5 then 6
  // outstanding on the receive side.
  void recvTwoChunksOnOneQp(Channel& channel) {
    for (const uint32_t chunkId :
         {kFirstOutstandingRecvChunkId, kFirstOutstandingRecvChunkId + 1}) {
      channel.recv(
          /*fromRank=*/1,
          /*toRank=*/0,
          /*sizeBytes=*/1000,
          chunkId,
          /*channelId=*/0);
    }
  }

  // Acks every dispatched message in dispatch order; acking refills the
  // per-QP queues, so the loop re-reads size() as new messages appear.
  void ackAllSends(Channel& channel) {
    for (size_t i = 0; i < sentMsgs_.size(); i++) {
      channel.requestAcked(&sentMsg(i));
    }
  }

  // A stand-in completion notification for the first QP pair, as the transport
  // would hand back to the cleanup entry points.
  static Message probeMessage(
      const uint32_t srcRank,
      const uint32_t dstRank,
      const uint32_t connectionMessageId) {
    Message msg;
    msg.srcRank = srcRank;
    msg.dstRank = dstRank;
    msg.localQPN = kFirstLocalQPN;
    msg.remoteQPN = kFirstRemoteQPN;
    msg.connectionMessageId = connectionMessageId;
    return msg;
  }

  static std::vector<uint32_t> sortedSizes(const MessageRefs& messages) {
    std::vector<uint32_t> sizes;
    sizes.reserve(messages.size());
    for (const Message& msg : messages) {
      sizes.emplace_back(msg.sizeBytes);
    }
    std::sort(sizes.begin(), sizes.end());
    return sizes;
  }

  static std::vector<std::pair<uint32_t, uint32_t>> sortedQpPairs(
      const MessageRefs& messages) {
    std::vector<std::pair<uint32_t, uint32_t>> qps;
    qps.reserve(messages.size());
    for (const Message& msg : messages) {
      qps.emplace_back(msg.localQPN, msg.remoteQPN);
    }
    std::sort(qps.begin(), qps.end());
    return qps;
  }

  std::map<uint32_t, std::vector<std::vector<uint32_t>>> rankToIdList_;
  workload::thrift::QPNPairMapMap qpPairMap_;
  ChannelParams::SubmitMessageFn smcb_;
  ChannelParams::SubmitMessageFn rmcb_;
  std::optional<Channel> channel_;
  MessageRefs sentMsgs_;
  MessageRefs recvdMsgs_;

 private:
  static Message& deref(Message* const msg) {
    if (msg == nullptr) {
      throw std::logic_error("channel reported a null message");
    }
    return *msg;
  }
};

TEST_F(ChannelSendRecvTest, SendShrinksOnlyTheLastChunk) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/2);

  // Act
  sendToRankOne(channel, /*sizeBytes=*/2500);
  ackAllSends(channel);

  // Assert: exactly one 500-byte remainder chunk, the rest full size.
  EXPECT_EQ(sortedSizes(sentMsgs_), (std::vector<uint32_t>{500, 1000, 1000}));
}

TEST_F(ChannelSendRecvTest, SendWithExactMultipleProducesOnlyFullChunks) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/2);

  // Act
  sendToRankOne(channel, /*sizeBytes=*/3000);
  ackAllSends(channel);

  // Assert: no short trailing chunk when the size divides evenly.
  EXPECT_EQ(sortedSizes(sentMsgs_), (std::vector<uint32_t>{1000, 1000, 1000}));
}

TEST_F(ChannelSendRecvTest, SendDispatchesOneOutstandingMessagePerQp) {
  // Arrange: 4 chunks spread round-robin over 2 QPs.
  Channel& channel = makeChannel(/*numQPsPerChannel=*/2);

  // Act: no acks yet, so only the head of each QP queue is in flight.
  sendToRankOne(channel, /*sizeBytes=*/4000);

  // Assert
  const std::vector<std::pair<uint32_t, uint32_t>> expectedQps{
      {kFirstLocalQPN, kFirstRemoteQPN},
      {kFirstLocalQPN + 1, kFirstRemoteQPN + 1}};
  EXPECT_EQ(sortedQpPairs(sentMsgs_), expectedQps);
}

TEST_F(ChannelSendRecvTest, SendWithFewerChunksThanQpsLeavesSpareQpsIdle) {
  // Arrange: a single chunk but four available QPs.
  Channel& channel = makeChannel(/*numQPsPerChannel=*/4);

  // Act
  sendToRankOne(channel, /*sizeBytes=*/1000);

  // Assert: only the first QP of the round-robin is used.
  ASSERT_EQ(sentMsgs_.size(), 1);
  EXPECT_EQ(sentMsg(0).localQPN, kFirstLocalQPN);
  EXPECT_EQ(sentMsg(0).remoteQPN, kFirstRemoteQPN);
}

TEST_F(ChannelSendRecvTest, SendCompletedFlipsOnlyOnTheFinalAck) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/2);
  sendToRankOne(channel, /*sizeBytes=*/2500);
  ASSERT_FALSE(channel.hasSendCompleted());

  // Act + Assert: the flag stays down until the third and last ack.
  size_t acked = 0;
  for (size_t i = 0; i < sentMsgs_.size(); i++) {
    channel.requestAcked(&sentMsg(i));
    acked++;
    EXPECT_EQ(channel.hasSendCompleted(), acked == 3)
        << "unexpected completion state after " << acked << " ack(s)";
  }
  EXPECT_EQ(acked, 3);
}

TEST_F(ChannelSendRecvTest, SendOfZeroBytesDispatchesNothingAndCompletes) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/2);

  // Act
  sendToRankOne(channel, /*sizeBytes=*/0);

  // Assert: no chunk means no ack will arrive, so the send is already done.
  EXPECT_TRUE(sentMsgs_.empty());
  EXPECT_TRUE(channel.hasSendCompleted());
}

TEST_F(ChannelSendRecvTest, RecvOfZeroBytesPostsNothingAndCompletes) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/2);

  // Act
  channel.recv(
      /*fromRank=*/1,
      /*toRank=*/0,
      /*sizeBytes=*/0,
      /*chunkId=*/0,
      /*channelId=*/0);

  // Assert: no chunk means no completion will arrive, so the recv is done.
  EXPECT_TRUE(recvdMsgs_.empty());
  EXPECT_TRUE(channel.hasRecvCompleted());
}

TEST_F(ChannelSendRecvTest, SendResolvesEndpointDeviceIdsFromTheRankMap) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);

  // Act
  sendToRankOne(channel, /*sizeBytes=*/1000);

  // Assert: ranks are translated into device ids and tagged as a send.
  ASSERT_EQ(sentMsgs_.size(), 1);
  const Message& msg = sentMsg(0);
  EXPECT_EQ(msg.srcIdx, FlexDeviceId(std::vector<uint32_t>{10}));
  EXPECT_EQ(msg.dstIdx, FlexDeviceId(std::vector<uint32_t>{20}));
  EXPECT_TRUE(msg.send);
  EXPECT_FALSE(msg.recv);
  EXPECT_EQ(msg.workloadId, kWorkloadId);
}

TEST_F(ChannelSendRecvTest, FollowUpChunksKeepTheDestinationEndpoint) {
  // Arrange: two endpoints per rank, and one QP so that 2500 bytes dispatch
  // their first chunk at once and the other two on the acks.
  rankToIdList_[0] = {{10}, {11}};
  rankToIdList_[1] = {{20}, {21}};
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);

  // Act
  channel.send(
      /*fromRank=*/0,
      /*toRank=*/1,
      /*sizeBytes=*/2500,
      /*chunkId=*/0,
      /*channelId=*/0,
      /*endpointId=*/1,
      /*sendDelayNs=*/0,
      /*writeImm=*/false,
      /*write=*/false,
      /*writeImmData=*/0);
  ackAllSends(channel);

  // Assert: every chunk, including the two follow-ups, goes to endpoint 1.
  ASSERT_EQ(sentMsgs_.size(), 3);
  for (const Message& msg : sentMsgs_) {
    EXPECT_EQ(msg.dstEndpointId, 1);
    EXPECT_EQ(msg.dstIdx, FlexDeviceId(std::vector<uint32_t>{21}));
  }
}

TEST_F(
    ChannelSendRecvTest,
    SecondSendDrainsLeftoverChunksFromTheFirstTransfer) {
  // Arrange: one QP and 2500 bytes queues 3 chunks but dispatches only 1,
  // leaving a 1000- and a 500-byte chunk behind.
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);
  sendToRankOne(channel, /*sizeBytes=*/2500);
  ASSERT_EQ(sentMsgs_.size(), 1);

  // Act: a fresh 700-byte transfer on the same channel.
  channel.send(
      /*fromRank=*/0,
      /*toRank=*/1,
      /*sizeBytes=*/700,
      /*chunkId=*/9,
      /*channelId=*/0,
      /*endpointId=*/0,
      /*sendDelayNs=*/0,
      /*writeImm=*/false,
      /*write=*/false,
      /*writeImmData=*/0);

  // Assert: the queue is FIFO and persists across transfers, so the new send
  // dispatches a stale 1000-byte chunk carrying the new chunk id.
  ASSERT_EQ(sentMsgs_.size(), 2);
  EXPECT_EQ(sentMsg(1).sizeBytes, 1000);
  EXPECT_EQ(sentMsg(1).connectionMessageId, 9);

  // And the expected-ack counter was reset to the new transfer's single chunk,
  // so one ack completes the send while two chunks are still queued.
  channel.requestAcked(&sentMsg(1));
  EXPECT_TRUE(channel.hasSendCompleted());
}

TEST_F(ChannelSendRecvTest, AckedCleanupRejectsAnOutOfOrderChunkId) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);
  sendTwoChunksOnOneQp(channel);

  // Act + Assert
  Message probe = probeMessage(
      /*srcRank=*/0, /*dstRank=*/1, kUnexpectedChunkId);
  EXPECT_THAT(
      [&] { channel.requestAckedCleanup(&probe); },
      ::testing::ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "Channel received CQE for chunk 99 to 1, expected 7")));
}

TEST_F(ChannelSendRecvTest, AckedCleanupPopsHeadOnInOrderChunkId) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);
  sendTwoChunksOnOneQp(channel);

  // Act: the head chunk id matches, so it is popped off the FIFO.
  Message probe = probeMessage(
      /*srcRank=*/0, /*dstRank=*/1, kFirstOutstandingSendChunkId);
  channel.requestAckedCleanup(&probe);

  // Assert: the second chunk is now the expected head.
  probe.connectionMessageId = kUnexpectedChunkId;
  EXPECT_THAT(
      [&] { channel.requestAckedCleanup(&probe); },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("expected 8")));
}

TEST_F(ChannelSendRecvTest, AckedCleanupRejectsACqeWithNoChunkOutstanding) {
  // Arrange: clean up both outstanding chunks, emptying the QP's FIFO.
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);
  sendTwoChunksOnOneQp(channel);
  Message probe = probeMessage(
      /*srcRank=*/0, /*dstRank=*/1, kFirstOutstandingSendChunkId);
  channel.requestAckedCleanup(&probe);
  probe.connectionMessageId = kFirstOutstandingSendChunkId + 1;
  channel.requestAckedCleanup(&probe);

  // Act + Assert
  probe.connectionMessageId = kUnexpectedChunkId;
  EXPECT_THAT(
      [&] { channel.requestAckedCleanup(&probe); },
      ::testing::ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "Channel received CQE for chunk 99 to 1, but no chunk is "
          "outstanding")));
}

TEST_F(ChannelSendRecvTest, ReceivedCleanupRejectsAnOutOfOrderChunkId) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);
  recvTwoChunksOnOneQp(channel);

  // Act + Assert
  Message probe = probeMessage(
      /*srcRank=*/1, /*dstRank=*/0, kUnexpectedChunkId);
  EXPECT_THAT(
      [&] { channel.requestReceivedCleanup(&probe); },
      ::testing::ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "Channel received chunk 99 from 1, expected 5")));
}

TEST_F(ChannelSendRecvTest, ReceivedCleanupPopsHeadOnInOrderChunkId) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);
  recvTwoChunksOnOneQp(channel);

  // Act: the head chunk id matches, so it is popped off the FIFO.
  Message probe = probeMessage(
      /*srcRank=*/1, /*dstRank=*/0, kFirstOutstandingRecvChunkId);
  channel.requestReceivedCleanup(&probe);

  // Assert: the second chunk is now the expected head.
  probe.connectionMessageId = kUnexpectedChunkId;
  EXPECT_THAT(
      [&] { channel.requestReceivedCleanup(&probe); },
      ::testing::ThrowsMessage<std::runtime_error>(
          ::testing::HasSubstr("expected 6")));
}

TEST_F(
    ChannelSendRecvTest,
    ReceivedCleanupRejectsAChunkWithNoReceiveOutstanding) {
  // Arrange: clean up both posted receives, emptying the QP's FIFO.
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);
  recvTwoChunksOnOneQp(channel);
  Message probe = probeMessage(
      /*srcRank=*/1, /*dstRank=*/0, kFirstOutstandingRecvChunkId);
  channel.requestReceivedCleanup(&probe);
  probe.connectionMessageId = kFirstOutstandingRecvChunkId + 1;
  channel.requestReceivedCleanup(&probe);

  // Act + Assert
  probe.connectionMessageId = kUnexpectedChunkId;
  EXPECT_THAT(
      [&] { channel.requestReceivedCleanup(&probe); },
      ::testing::ThrowsMessage<std::runtime_error>(::testing::HasSubstr(
          "Channel received chunk 99 from 1, but no receive is outstanding")));
}

TEST_F(ChannelSendRecvTest, RecvPostsEveryChunkUpFrontRoundRobinAcrossQps) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/2);

  // Act: recv posts all chunks immediately rather than one per QP.
  channel.recv(
      /*fromRank=*/1,
      /*toRank=*/0,
      /*sizeBytes=*/2500,
      /*chunkId=*/0,
      /*channelId=*/0);

  // Assert: chunk i lands on QP i % 2, last chunk holds the remainder.
  ASSERT_EQ(recvdMsgs_.size(), 3);
  EXPECT_EQ(recvdMsg(0).localQPN, kFirstLocalQPN);
  EXPECT_EQ(recvdMsg(1).localQPN, kFirstLocalQPN + 1);
  EXPECT_EQ(recvdMsg(2).localQPN, kFirstLocalQPN);
  EXPECT_EQ(recvdMsg(0).sizeBytes, 1000);
  EXPECT_EQ(recvdMsg(1).sizeBytes, 1000);
  EXPECT_EQ(recvdMsg(2).sizeBytes, 500);
}

TEST_F(ChannelSendRecvTest, RecvBuiltMessageIsMarkedReceiveWithNoWriteFlags) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/1);

  // Act
  channel.recv(
      /*fromRank=*/1,
      /*toRank=*/0,
      /*sizeBytes=*/1000,
      /*chunkId=*/5,
      /*channelId=*/2,
      /*endpointId=*/0,
      /*sendDelayNs=*/11);

  // Assert
  ASSERT_EQ(recvdMsgs_.size(), 1);
  const Message& msg = recvdMsg(0);
  EXPECT_FALSE(msg.send);
  EXPECT_TRUE(msg.recv);
  EXPECT_FALSE(msg.isWrite);
  EXPECT_FALSE(msg.isWriteImm);
  EXPECT_EQ(msg.writeImmData, 0);
  EXPECT_EQ(msg.srcIdx, FlexDeviceId(std::vector<uint32_t>{20}));
  EXPECT_EQ(msg.dstIdx, FlexDeviceId(std::vector<uint32_t>{10}));
}

TEST_F(ChannelSendRecvTest, RecvCompletedFlipsOnlyOnTheFinalRequestReceived) {
  // Arrange
  Channel& channel = makeChannel(/*numQPsPerChannel=*/2);
  channel.recv(
      /*fromRank=*/1,
      /*toRank=*/0,
      /*sizeBytes=*/2500,
      /*chunkId=*/0,
      /*channelId=*/0);
  ASSERT_FALSE(channel.hasRecvCompleted());

  // Act + Assert
  channel.requestReceived(&recvdMsg(0));
  EXPECT_FALSE(channel.hasRecvCompleted());
  channel.requestReceived(&recvdMsg(1));
  EXPECT_FALSE(channel.hasRecvCompleted());
  channel.requestReceived(&recvdMsg(2));
  EXPECT_TRUE(channel.hasRecvCompleted());
}

} // namespace packsim
