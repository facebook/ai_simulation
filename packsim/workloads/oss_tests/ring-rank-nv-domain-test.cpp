/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <stdexcept>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/util/sim-fix.h"
#include "packsim/workloads/ar-ag-rs-ring.h"
#include "packsim/workloads/oss_tests/rank-harness.h"

namespace packsim {

class RingRankNvDomainTest : public WorkloadRankTestHarness,
                             public ::testing::Test {
 protected:
  void SetUp() override {
    workCfg_.name() = "RING_NV_DOMAIN_TEST";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 8;
    populateRankToId(workCfg_);
  }

  std::unique_ptr<RingRank> createRingRank(uint32_t rank) {
    auto sendCb = [this](Message* msg) { sendMessage(msg); };
    auto recvCb = [this](Message* msg) { recvMessage(msg); };
    auto completeCb = [this](uint32_t workloadId, uint32_t rank) {
      rankComplete(workloadId, rank);
    };

    WorkloadRankParams params{
        .simCtx = &simCtx_,
        .workloadId = 0,
        .workCfg = workCfg_,
        .rank = rank,
        .smcb = sendCb,
        .rmcb = recvCb,
        .rccb = completeCb,
        .parent = nullptr};
    return std::make_unique<RingRank>(params);
  }

  void setAllReduceConfig(uint64_t sizeBytes, uint32_t numChunks) {
    auto ar = packsim::workload::thrift::AllReduce();
    ar.sizeBytes() = sizeBytes;
    ar.numChunks() = numChunks;
    ar.algorithm() = packsim::workload::thrift::AllReduceAlgorithm::RING;
    workCfg_.config()->allReduce() = ar;
  }

  void setAllGatherConfig(uint64_t sizeBytes, uint32_t numChunks) {
    auto ag = packsim::workload::thrift::AllGather();
    ag.sizeBytes() = sizeBytes;
    ag.numChunks() = numChunks;
    ag.algorithm() = packsim::workload::thrift::AllReduceAlgorithm::RING;
    workCfg_.config()->allGather() = ag;
  }

  void setReduceScatterConfig(uint64_t sizeBytes, uint32_t numChunks) {
    auto rs = packsim::workload::thrift::ReduceScatter();
    rs.sizeBytes() = sizeBytes;
    rs.numChunks() = numChunks;
    rs.algorithm() = packsim::workload::thrift::AllReduceAlgorithm::RING;
    workCfg_.config()->reduceScatter() = rs;
  }

  // Sets up NV domain mapping: ranks 0-3 in domain 0, ranks 4-7 in domain 1
  void setTwoNvDomains() {
    folly::F14FastMap<uint32_t, uint32_t> rankToNvDomain;
    const uint32_t numRanks = *workCfg_.numEndpoints();
    for (uint32_t i = 0; i < numRanks; ++i) {
      rankToNvDomain[i] = i / (numRanks / 2);
    }
    workCfg_.rankToNvDomain() = std::move(rankToNvDomain);
  }

  // Sets up NV domain mapping: each rank in its own domain (no NVLink)
  void setAllDifferentDomains() {
    folly::F14FastMap<uint32_t, uint32_t> rankToNvDomain;
    const uint32_t numRanks = *workCfg_.numEndpoints();
    for (uint32_t i = 0; i < numRanks; ++i) {
      rankToNvDomain[i] = i;
    }
    workCfg_.rankToNvDomain() = std::move(rankToNvDomain);
  }

  // Creates a RingRank with lightweight callbacks for tests that call run().
  // Avoids the harness's rankComplete which checks ranks_.size().
  uint32_t completionCount_{0};

  std::unique_ptr<RingRank> createRingRankForRunTest(uint32_t rank) {
    WorkloadRankParams::SubmitMessageFn sendCb = [](Message*) {};
    WorkloadRankParams::SubmitMessageFn recvCb = [](Message*) {};
    WorkloadRankParams::OnRankCompleteCallback completeCb =
        [this](uint32_t, uint32_t) { ++completionCount_; };

    WorkloadRankParams params{
        .simCtx = &simCtx_,
        .workloadId = 0,
        .workCfg = workCfg_,
        .rank = rank,
        .smcb = sendCb,
        .rmcb = recvCb,
        .rccb = completeCb,
        .parent = nullptr};
    return std::make_unique<RingRank>(params);
  }

  // Build QPN maps so that recv()/send() can resolve QPN pairs.
  // Must be called after setting the workload config.
  void buildQPNMaps() {
    std::vector<packsim::workload::thrift::Workload> workCfgs({workCfg_});
    packsim::buildQPNMap(workCfgs);
    uint32_t nextQpn = 0;
    packsim::buildNicQPNMap(workCfgs, nextQpn);
    workCfg_ = std::move(workCfgs.at(0));
  }
};

// With NV domains, entry ranks receive the same bytes as without NV domains
// (chunk size and count are based on numRanks, not effectiveRanks).
// Exit and interior ranks receive 0 bytes from the network.
TEST_F(RingRankNvDomainTest, AllReduceExpectedBytesWithNvDomain) {
  setAllReduceConfig(1024 * 1024, 1);

  auto rankWithout = createRingRank(0);
  // 8 ranks: chunkSize=131072, totalChunks=(2*8-2)*1=14
  EXPECT_EQ(rankWithout->getExpectedBytes(), 1835008);

  setTwoNvDomains();

  // Rank 0 is ENTRY (left=7 cross-domain): same expected bytes
  auto entryRank = createRingRank(0);
  EXPECT_EQ(entryRank->getExpectedBytes(), 1835008);

  // Rank 3 is EXIT (skipRecv=true): 0 network recv bytes
  auto exitRank = createRingRank(3);
  EXPECT_EQ(exitRank->getExpectedBytes(), 0);

  // Rank 1 is INTERIOR (skipRecv=true): 0 network recv bytes
  auto interiorRank = createRingRank(1);
  EXPECT_EQ(interiorRank->getExpectedBytes(), 0);
}

TEST_F(RingRankNvDomainTest, AllGatherExpectedBytesWithNvDomain) {
  setAllGatherConfig(1024 * 1024, 1);

  auto rankWithout = createRingRank(0);
  // 8 ranks: chunkSize=131072, totalChunks=(8-1)*1=7
  EXPECT_EQ(rankWithout->getExpectedBytes(), 917504);

  setTwoNvDomains();

  // Rank 0 is ENTRY: same expected bytes
  auto entryRank = createRingRank(0);
  EXPECT_EQ(entryRank->getExpectedBytes(), 917504);

  // Rank 3 is EXIT: 0 network recv bytes
  auto exitRank = createRingRank(3);
  EXPECT_EQ(exitRank->getExpectedBytes(), 0);
}

TEST_F(RingRankNvDomainTest, ReduceScatterExpectedBytesWithNvDomain) {
  setReduceScatterConfig(1024 * 1024, 1);

  auto rankWithout = createRingRank(0);
  // 8 ranks: chunkSize=131072, totalChunks=(8-1)*1=7
  EXPECT_EQ(rankWithout->getExpectedBytes(), 917504);

  setTwoNvDomains();

  // Rank 0 is ENTRY: same expected bytes
  auto entryRank = createRingRank(0);
  EXPECT_EQ(entryRank->getExpectedBytes(), 917504);

  // Rank 3 is EXIT: 0 network recv bytes
  auto exitRank = createRingRank(3);
  EXPECT_EQ(exitRank->getExpectedBytes(), 0);
}

// Test that skip flags and neighbors are correctly computed for each rank role.
// Ring: 0->1->2->3->4->5->6->7->0
// Domain 0: ranks 0-3, Domain 1: ranks 4-7
// Cross-domain flows: 3->4 and 7->0
TEST_F(RingRankNvDomainTest, NeighborsSkipSameNvDomain) {
  setAllReduceConfig(1024 * 1024, 1);
  setTwoNvDomains();

  // Rank 0: ENTRY (left=7 cross-domain, right=1 same-domain)
  auto rank0 = createRingRank(0);
  EXPECT_EQ(rank0->left_, 7);
  EXPECT_EQ(rank0->right_, 1);
  EXPECT_TRUE(rank0->skipSend_);
  EXPECT_FALSE(rank0->skipRecv_);

  // Rank 1: INTERIOR (left=0 same, right=2 same)
  auto rank1 = createRingRank(1);
  EXPECT_EQ(rank1->left_, 0);
  EXPECT_EQ(rank1->right_, 2);
  EXPECT_TRUE(rank1->skipSend_);
  EXPECT_TRUE(rank1->skipRecv_);

  // Rank 3: EXIT (left=2 same, right=4 cross-domain)
  auto rank3 = createRingRank(3);
  EXPECT_EQ(rank3->left_, 2);
  EXPECT_EQ(rank3->right_, 4);
  EXPECT_FALSE(rank3->skipSend_);
  EXPECT_TRUE(rank3->skipRecv_);

  // Rank 4: ENTRY (left=3 cross-domain, right=5 same)
  auto rank4 = createRingRank(4);
  EXPECT_EQ(rank4->left_, 3);
  EXPECT_EQ(rank4->right_, 5);
  EXPECT_TRUE(rank4->skipSend_);
  EXPECT_FALSE(rank4->skipRecv_);

  // Rank 7: EXIT (left=6 same, right=0 cross-domain)
  auto rank7 = createRingRank(7);
  EXPECT_EQ(rank7->left_, 6);
  EXPECT_EQ(rank7->right_, 0);
  EXPECT_FALSE(rank7->skipSend_);
  EXPECT_TRUE(rank7->skipRecv_);
}

// When all ranks are in different NV domains, every flow is cross-domain.
// All ranks should behave as FULL (skipSend=false, skipRecv=false),
// identical to when no NV domain map is set.
TEST_F(RingRankNvDomainTest, AllDifferentDomainsNoSkipping) {
  setAllReduceConfig(1024 * 1024, 1);
  setAllDifferentDomains();

  for (uint32_t i = 0; i < *workCfg_.numEndpoints(); ++i) {
    auto rank = createRingRank(i);
    EXPECT_FALSE(rank->skipSend_);
    EXPECT_FALSE(rank->skipRecv_);
    // Same expected bytes as without any NV domain map
    EXPECT_EQ(rank->getExpectedBytes(), 1835008);
  }
}

// requestAcked completes the rank when all sends are done and recvs are done.
TEST_F(RingRankNvDomainTest, RequestAckedCompletesWhenAllSendsDone) {
  setAllReduceConfig(1024 * 1024, 1);
  setTwoNvDomains();

  // Exit rank (skipRecv=true, skipSend=false): recvsDone is always true
  auto rank = createRingRankForRunTest(3);
  EXPECT_TRUE(rank->skipRecv_);
  EXPECT_FALSE(rank->skipSend_);

  // Directly set internal state to simulate "all chunks sent, 1 in flight"
  // because calling run() + pumping the full send/recv pipeline would require
  // wiring up the entire simulation context (QPN maps, scheduler callbacks,
  // neighbor ranks). This test isolates requestAcked's completion logic.
  rank->started_ = true;
  rank->sentChunks_ = rank->totalChunks_;
  rank->chunksInFlight_ = 1;
  auto msg = std::make_unique<Message>();
  msg->connectionMessageId = 0;
  Message* const msgPtr = msg.get();
  rank->sendMsgs_.emplace(0, std::move(msg));

  rank->requestAcked(msgPtr);

  EXPECT_EQ(rank->chunksInFlight_, 0);
  EXPECT_TRUE(rank->sendMsgs_.empty());
  EXPECT_TRUE(rank->complete());
  EXPECT_EQ(completionCount_, 1);
}

// Interior rank (both neighbors in same NV domain) completes immediately
// with no network activity.
TEST_F(RingRankNvDomainTest, InteriorRankCompletesImmediately) {
  setAllReduceConfig(1024 * 1024, 1);
  setTwoNvDomains();

  auto rank = createRingRankForRunTest(1);
  rank->run();

  EXPECT_EQ(completionCount_, 1);
  EXPECT_TRUE(rank->complete());
  EXPECT_EQ(rank->startTimeNs_, rank->endTimeNs_);
}

// A one-rank ring is its own neighbor; with NV domains that neighbor shares
// its domain, so the rank is accepted and completes at once with no network
// activity.
TEST_F(RingRankNvDomainTest, SingleRankWithNvDomainsCompletesImmediately) {
  workCfg_.numEndpoints() = 1;
  setAllReduceConfig(1024 * 1024, 1);
  workCfg_.rankToNvDomain() = folly::F14FastMap<uint32_t, uint32_t>{{0, 0}};

  auto rank = createRingRankForRunTest(0);
  rank->run();

  EXPECT_EQ(completionCount_, 1);
  EXPECT_TRUE(rank->complete());
  EXPECT_EQ(rank->startTimeNs_, rank->endTimeNs_);
  EXPECT_EQ(rank->sentChunks_, 0u);
  EXPECT_EQ(rank->postedRecvs_, 0u);
}

// Entry rank (left cross-domain, right same-domain) posts recvs
// but does not schedule sends.
TEST_F(RingRankNvDomainTest, EntryRankPostsRecvsOnly) {
  setAllReduceConfig(1024 * 1024, 1);
  workCfg_.config()->allReduce()->maxChunksInFlight() = 4;
  buildQPNMaps();
  setTwoNvDomains();

  auto rank = createRingRankForRunTest(0);
  rank->run();

  EXPECT_FALSE(rank->recvMsgs_.empty());
  EXPECT_TRUE(rank->sendMsgs_.empty());
  EXPECT_EQ(rank->queuedChunks_, 0);
  EXPECT_TRUE(rank->started_);
  EXPECT_FALSE(rank->complete());
}

// Exit rank (left same-domain, right cross-domain) queues all chunks
// for sending but posts no recvs.
TEST_F(RingRankNvDomainTest, ExitRankQueuesAllChunksNoRecvs) {
  setAllReduceConfig(1024 * 1024, 1);
  setTwoNvDomains();

  auto rank = createRingRankForRunTest(3);
  rank->run();

  EXPECT_TRUE(rank->recvMsgs_.empty());
  EXPECT_EQ(rank->postedRecvs_, 0);
  EXPECT_FALSE(rank->complete());
}

// Entry rank's requestReceived does not queue sends (skipSend guard).
TEST_F(RingRankNvDomainTest, RequestReceivedSkipsSendForEntryRank) {
  setAllReduceConfig(1024 * 1024, 1);
  buildQPNMaps();
  setTwoNvDomains();

  auto rank = createRingRankForRunTest(0);

  // Manually set state to avoid needing full run() infrastructure
  rank->postedRecvs_ = rank->totalChunks_;
  auto msg = std::make_unique<Message>();
  msg->connectionMessageId = 0;
  Message* const msgPtr = msg.get();
  rank->recvMsgs_.emplace(0, std::move(msg));
  rank->started_ = true;

  rank->requestReceived(msgPtr);

  EXPECT_EQ(rank->queuedChunks_, 0);
  // postedRecvs unchanged: entry rank still posts recvs (it receives from
  // the network), but skipSend prevents forwarding to the right neighbor
  EXPECT_EQ(rank->postedRecvs_, rank->totalChunks_);
}

// trySendChunks sends min(maxChunksInFlight - chunksInFlight, queuedChunks).
// Directly sets internal state rather than calling run() because trySendChunks
// is a low-level helper: run() would also post recvs and schedule via SimCtx,
// making it hard to isolate the chunking/windowing logic under test.
TEST_F(RingRankNvDomainTest, TrySendChunksSendsUpToMaxInFlight) {
  setAllReduceConfig(1024 * 1024, 1);
  buildQPNMaps();

  auto rank = createRingRankForRunTest(0);
  // Manually configure state: 5 chunks queued, max 2 in flight
  rank->queuedChunks_ = 5;
  rank->maxChunksInFlight_ = 2;
  rank->chunksInFlight_ = 0;
  rank->sentChunks_ = 0;

  rank->trySendChunks(true);

  // Should send min(2, 5) = 2 chunks
  EXPECT_EQ(rank->sentChunks_, 2);
  EXPECT_EQ(rank->chunksInFlight_, 2);
  EXPECT_EQ(rank->queuedChunks_, 3);
  EXPECT_EQ(rank->sendMsgs_.size(), 2);
  EXPECT_TRUE(rank->started_);
}

// requestReceived on a full rank (no NV domain skipping) queues a send
// and posts another recv when postedRecvs < totalChunks.
TEST_F(RingRankNvDomainTest, RequestReceivedFullRankQueuesAndPostsRecv) {
  setAllReduceConfig(1024 * 1024, 1);
  buildQPNMaps();

  // No NV domains -> full rank (skipSend=false, skipRecv=false)
  auto rank = createRingRankForRunTest(0);
  EXPECT_FALSE(rank->skipSend_);
  EXPECT_FALSE(rank->skipRecv_);

  // Set up state: started, 1 recv msg ready, postedRecvs < totalChunks
  rank->started_ = true;
  rank->postedRecvs_ = 1;
  rank->sentChunks_ = 0;
  rank->queuedChunks_ = 0;
  auto msg = std::make_unique<Message>();
  msg->connectionMessageId = 0;
  Message* const msgPtr = msg.get();
  rank->recvMsgs_.emplace(0, std::move(msg));

  rank->requestReceived(msgPtr);

  // requestReceived queues 1 chunk, trySendChunks immediately consumes it
  EXPECT_EQ(rank->queuedChunks_, 0);
  EXPECT_EQ(rank->sentChunks_, 1);
  EXPECT_EQ(rank->postedRecvs_, 2);
}

// requestAcked triggers trySendChunks when rank is not yet complete.
TEST_F(RingRankNvDomainTest, RequestAckedTriggersMoreSends) {
  setAllReduceConfig(1024 * 1024, 1);
  buildQPNMaps();
  setTwoNvDomains();

  // Exit rank: skipRecv=true, skipSend=false
  auto rank = createRingRankForRunTest(3);

  // Set up state: some chunks sent, 1 in flight, more queued
  rank->started_ = true;
  rank->sentChunks_ = 2;
  rank->chunksInFlight_ = 1;
  rank->queuedChunks_ = 3;
  rank->maxChunksInFlight_ = 2;
  auto msg = std::make_unique<Message>();
  msg->connectionMessageId = 1;
  Message* const msgPtr = msg.get();
  rank->sendMsgs_.emplace(1, std::move(msg));

  rank->requestAcked(msgPtr);

  // chunksInFlight decremented from 1->0, then trySendChunks sends
  // min(maxChunksInFlight(2) - chunksInFlight(0), queuedChunks(3)) = 2 more
  EXPECT_EQ(rank->chunksInFlight_, 2);
  EXPECT_EQ(rank->sentChunks_, 4);
  EXPECT_EQ(rank->queuedChunks_, 1);
  EXPECT_FALSE(rank->complete());
}

TEST_F(RingRankNvDomainTest, RequestAckedRemovesMatchingOutOfOrderMessage) {
  setAllReduceConfig(1024 * 1024, 1);
  buildQPNMaps();

  auto rank = createRingRankForRunTest(0);
  rank->started_ = true;
  rank->sentChunks_ = 2;
  rank->chunksInFlight_ = 2;

  auto firstMsg = std::make_unique<Message>();
  firstMsg->connectionMessageId = 0;
  rank->sendMsgs_.emplace(0, std::move(firstMsg));
  auto secondMsg = std::make_unique<Message>();
  secondMsg->connectionMessageId = 1;
  Message* const secondMsgPtr = secondMsg.get();
  rank->sendMsgs_.emplace(1, std::move(secondMsg));

  rank->requestAcked(secondMsgPtr);

  EXPECT_TRUE(rank->sendMsgs_.contains(0));
  EXPECT_FALSE(rank->sendMsgs_.contains(1));
  EXPECT_EQ(rank->chunksInFlight_, 1);
}

TEST_F(RingRankNvDomainTest, RequestReceivedRemovesMatchingOutOfOrderMessage) {
  setAllReduceConfig(1024 * 1024, 1);
  buildQPNMaps();
  setTwoNvDomains();

  auto rank = createRingRankForRunTest(0);
  rank->started_ = true;
  rank->postedRecvs_ = rank->totalChunks_;

  auto firstMsg = std::make_unique<Message>();
  firstMsg->connectionMessageId = 0;
  rank->recvMsgs_.emplace(0, std::move(firstMsg));
  auto secondMsg = std::make_unique<Message>();
  secondMsg->connectionMessageId = 1;
  Message* const secondMsgPtr = secondMsg.get();
  rank->recvMsgs_.emplace(1, std::move(secondMsg));

  rank->requestReceived(secondMsgPtr);

  EXPECT_TRUE(rank->recvMsgs_.contains(0));
  EXPECT_FALSE(rank->recvMsgs_.contains(1));
}

// Verify that ENTRY ranks pre-post ALL totalChunks_ recvs when NV domain
// skipping is active. Without this, EXIT ranks (which queue all sends
// upfront) can outpace ENTRY ranks due to the ACK-before-callback race in the
// ROCE transport, causing RNR (Receiver Not Ready) crashes.
//
// The bug required maxChunksInFlight < totalChunks to manifest: ENTRY ranks
// only posted min(totalChunks, maxChunksInFlight) recvs, but EXIT ranks sent
// all totalChunks upfront. The ROCE ACK fires before the requestReceived
// callback posts the next recv, so the sender can get ahead of the receiver.
TEST_F(RingRankNvDomainTest, EntryRankPrePostsAllRecvsWithNvDomain) {
  setAllReduceConfig(1024 * 1024, 1);
  // Override maxChunksInFlight to be less than totalChunks (14) to reproduce
  // the conditions that caused the RNR bug.
  workCfg_.config()->allReduce()->maxChunksInFlight() = 4;
  buildQPNMaps();

  // Without NV domain skipping: FULL rank posts min(totalChunks, 4) = 4 recvs
  auto fullRank = createRingRank(0);
  EXPECT_EQ(fullRank->totalChunks_, 14u);
  EXPECT_EQ(fullRank->maxChunksInFlight_, 4u);
  fullRank->run();
  EXPECT_EQ(fullRank->postedRecvs_, 4u);
  EXPECT_EQ(fullRank->recvMsgs_.size(), 4u);

  setTwoNvDomains();
  buildQPNMaps();

  // With NV domain skipping: ENTRY rank (rank 0) must pre-post ALL 14 recvs
  auto entryRank = createRingRank(0);
  EXPECT_FALSE(entryRank->skipRecv_);
  EXPECT_TRUE(entryRank->skipSend_);
  EXPECT_EQ(entryRank->totalChunks_, 14u);
  EXPECT_EQ(entryRank->maxChunksInFlight_, 4u);
  entryRank->run();
  EXPECT_EQ(entryRank->postedRecvs_, 14u);
  EXPECT_EQ(entryRank->recvMsgs_.size(), 14u);

  // EXIT rank (rank 3) posts no recvs (skipRecv=true)
  auto exitRank = createRingRank(3);
  EXPECT_TRUE(exitRank->skipRecv_);
  exitRank->run();
  EXPECT_EQ(exitRank->postedRecvs_, 0u);
  EXPECT_EQ(exitRank->recvMsgs_.size(), 0u);
}

// RingRank runs only the three ring collectives. The workload builder never
// hands it another type, so this constructs one directly.
TEST_F(RingRankNvDomainTest, RejectsAWorkloadThatIsNotARingCollective) {
  workCfg_.config()->allToAll() = packsim::workload::thrift::AllToAll();

  EXPECT_THAT(
      [this] { createRingRank(/*rank=*/0); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Unsupported workload type")));
}

// A ring collective splits each rank's share into numChunks chunks, so it
// cannot have none.
TEST_F(RingRankNvDomainTest, RejectsARingCollectiveWithNoChunks) {
  setAllReduceConfig(/*sizeBytes=*/1024, /*numChunks=*/0);

  EXPECT_THAT(
      [this] { createRingRank(/*rank=*/0); },
      ::testing::ThrowsMessage<std::invalid_argument>(::testing::HasSubstr(
          "Workload RING_NV_DOMAIN_TEST: Ring numChunks "
          "must be at least 1, got 0")));
}

// The NIC selection mode is a thrift enum, so it can hold a value the IDL does
// not name.
TEST_F(RingRankNvDomainTest, RejectsANicSelectionModeOutsideTheEnum) {
  constexpr int32_t kModeUnknownToThisBinary = 42;
  setAllReduceConfig(/*sizeBytes=*/1024, /*numChunks=*/1);
  workCfg_.config()->allReduce()->nicSelectionMode() =
      static_cast<packsim::workload::thrift::RingNicSelectionMode>(
          kModeUnknownToThisBinary);

  EXPECT_THAT(
      [this] { createRingRank(/*rank=*/0); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Unsupported ring NIC selection mode 42")));
}

} // namespace packsim
