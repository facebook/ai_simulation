/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/workloads/oss_tests/rank-harness.h"

using ::testing::HasSubstr;

namespace packsim {

class WorkloadRankGetDataQPNsTest : public WorkloadRankTestHarness,
                                    public ::testing::Test {
 public:
  void SetUp() override {
    workCfg_.name() = "QPN_TEST";
    workCfg_.workloadId() = 0;
    workCfg_.numEndpoints() = 4;

    auto a2a = packsim::workload::thrift::AllToAll();
    a2a.sizeBytes() = 1024;
    a2a.numChunks() = 1;
    a2a.numGpusPerNvDomain() = 1;
    workCfg_.config()->allToAll() = a2a;

    run();
  }
};

// Tests verify QPN invariants rather than hardcoded values.
// QPN Invariants:
// 1. QPNs are positive integers (> 0)
// 2. Local and remote QPNs form symmetric pairs between communicating ranks
// 3. Different peers from the same source rank have different local QPNs
// 4. QPN pairs are unique per (srcRank, dstRank) combination

// Test that getDataQPNs returns a valid QPN pair with positive values and
// symmetric relationship
TEST_F(WorkloadRankGetDataQPNsTest, GetDataQPNsReturnsCorrectQPNPair) {
  ASSERT_EQ(ranks_.size(), 4);

  // Verify all QPN pairs have positive local and remote QPNs
  for (uint32_t srcRank = 0; srcRank < 4; ++srcRank) {
    auto* rank = ranks_[srcRank].get();
    for (uint32_t dstRank = 0; dstRank < 4; ++dstRank) {
      if (srcRank == dstRank) {
        continue;
      }
      const auto& qpnPair = rank->getDataQPNs(dstRank);
      EXPECT_GT(*qpnPair.local(), 0)
          << "Local QPN should be positive for " << srcRank << "->" << dstRank;
      EXPECT_GT(*qpnPair.remote(), 0)
          << "Remote QPN should be positive for " << srcRank << "->" << dstRank;
    }
  }

  // Verify symmetry: A's local QPN to B == B's remote QPN from A
  for (uint32_t rankA = 0; rankA < 4; ++rankA) {
    for (uint32_t rankB = 0; rankB < 4; ++rankB) {
      if (rankA == rankB) {
        continue;
      }

      const auto& pairAtoB = ranks_[rankA]->getDataQPNs(rankB);
      const auto& pairBtoA = ranks_[rankB]->getDataQPNs(rankA);

      EXPECT_EQ(*pairAtoB.local(), *pairBtoA.remote())
          << "Symmetry violation: rank" << rankA << "->rank" << rankB
          << " local QPN should equal rank" << rankB << "->rank" << rankA
          << " remote QPN";
      EXPECT_EQ(*pairAtoB.remote(), *pairBtoA.local())
          << "Symmetry violation: rank" << rankA << "->rank" << rankB
          << " remote QPN should equal rank" << rankB << "->rank" << rankA
          << " local QPN";
    }
  }
}

// Test that different peers have different local QPNs
TEST_F(WorkloadRankGetDataQPNsTest, GetDataQPNsReturnsDifferentPeerQPNs) {
  ASSERT_EQ(ranks_.size(), 4);
  const auto* rank0 = ranks_[0].get();

  // Collect local QPNs for all peers from rank 0's perspective
  std::unordered_set<uint32_t> localQPNs;
  for (uint32_t peer = 1; peer < 4; ++peer) {
    const auto& qpnPair = rank0->getDataQPNs(peer);
    localQPNs.emplace(*qpnPair.local());
  }

  // Each peer should have a unique local QPN
  EXPECT_EQ(localQPNs.size(), 3)
      << "Rank 0 should have 3 distinct local QPNs for its 3 peers";

  // Verify all rank pairs have unique QPN combinations
  std::set<std::pair<uint32_t, uint32_t>> allQPNPairs;
  for (uint32_t srcRank = 0; srcRank < 4; ++srcRank) {
    for (uint32_t dstRank = 0; dstRank < 4; ++dstRank) {
      if (srcRank == dstRank) {
        continue;
      }
      const auto& qpnPair = ranks_[srcRank]->getDataQPNs(dstRank);
      auto pair = std::make_pair(*qpnPair.local(), *qpnPair.remote());
      allQPNPairs.emplace(pair);
    }
  }

  // With 4 ranks, we have 4*3 = 12 directed pairs
  // QPNs are assigned per-endpoint, so pairs where both endpoints are first
  // assigned the same QPN (e.g., rank pair (0,1), (1,2), (2,3)) have
  // symmetric (local, remote) tuples like (1,1), (3,3), (5,5).
  // Other pairs have asymmetric tuples like (3,1)/(1,3).
  // Total unique (local, remote) pairs: 3 symmetric + 6 asymmetric = 9
  EXPECT_EQ(allQPNPairs.size(), 9) << "Expected 9 unique QPN pairs for 4 ranks";
}

// Test that NIC-aware getDataQPNs returns correct QPN pair
TEST_F(WorkloadRankGetDataQPNsTest, NicAwareGetDataQPNsReturnsCorrectQPNPair) {
  ASSERT_EQ(ranks_.size(), 4);

  // Verify QPN pairs are valid when using NIC indices
  for (uint32_t srcRank = 0; srcRank < 4; ++srcRank) {
    const auto* rank = ranks_[srcRank].get();
    for (uint32_t dstRank = 0; dstRank < 4; ++dstRank) {
      if (srcRank == dstRank) {
        continue;
      }
      // Use NIC index 0 for both source and destination
      const auto& qpnPair = rank->getDataQPNs(dstRank, 0, 0);
      EXPECT_GT(*qpnPair.local(), 0)
          << "Local QPN should be positive for NIC-aware access " << srcRank
          << "->" << dstRank;
      EXPECT_GT(*qpnPair.remote(), 0)
          << "Remote QPN should be positive for NIC-aware access " << srcRank
          << "->" << dstRank;
    }
  }

  // Verify symmetry for NIC-aware access
  for (uint32_t rankA = 0; rankA < 4; ++rankA) {
    for (uint32_t rankB = 0; rankB < 4; ++rankB) {
      if (rankA == rankB) {
        continue;
      }

      const auto& pairAtoB = ranks_[rankA]->getDataQPNs(rankB, 0, 0);
      const auto& pairBtoA = ranks_[rankB]->getDataQPNs(rankA, 0, 0);

      EXPECT_EQ(*pairAtoB.local(), *pairBtoA.remote())
          << "NIC-aware symmetry violation: rank" << rankA << "->rank" << rankB;
      EXPECT_EQ(*pairAtoB.remote(), *pairBtoA.local())
          << "NIC-aware symmetry violation: rank" << rankA << "->rank" << rankB;
    }
  }
}

// With a second NIC pair between ranks 0 and 1, the legacy accessor keeps the
// first NIC pair's data QPN while the NIC-aware one returns the requested NIC
// pair's.
TEST_F(
    WorkloadRankGetDataQPNsTest,
    NicAwareGetDataQPNsReturnsTheRequestedNicPairsQPN) {
  constexpr uint32_t kSecondNicLocalQPN = 1000;
  constexpr uint32_t kSecondNicRemoteQPN = 2000;
  packsim::thrift::RankPair rankPair;
  rankPair.srcRank() = 0;
  rankPair.dstRank() = 1;
  packsim::thrift::NicPair secondNicPair;
  secondNicPair.srcNicIdx() = 1;
  secondNicPair.dstNicIdx() = 1;
  packsim::workload::thrift::QPNPair secondNicQPNs;
  secondNicQPNs.local() = kSecondNicLocalQPN;
  secondNicQPNs.remote() = kSecondNicRemoteQPN;
  workCfg_.nicQpnPairMap()->at(rankPair)[secondNicPair] = {secondNicQPNs};
  // Ranks memoize their QPN selection at construction.
  buildRanks();
  const auto* rank0 = ranks_.at(0).get();

  const auto& legacyPair = rank0->getDataQPNs(1);
  const auto& nicAwarePair = rank0->getDataQPNs(1, 1, 1);

  EXPECT_EQ(nicAwarePair, secondNicQPNs);
  EXPECT_NE(*legacyPair.local(), kSecondNicLocalQPN);
  EXPECT_EQ(legacyPair, rank0->getDataQPNs(1, 0, 0));
}

TEST_F(WorkloadRankGetDataQPNsTest, GetTotalDataQPCountReturnsCorrectCount) {
  ASSERT_EQ(ranks_.size(), 4);
  const auto* rank0 = ranks_[0].get();

  // With the test setup (single NIC per rank, single QP per NIC pair),
  // getTotalDataQPCount should return 1 for each peer
  const uint32_t qpCount = rank0->getTotalDataQPCount(1);
  EXPECT_EQ(qpCount, 1u);

  // Verify consistent across different peers
  EXPECT_EQ(rank0->getTotalDataQPCount(2), 1u);
  EXPECT_EQ(rank0->getTotalDataQPCount(3), 1u);
}

TEST_F(WorkloadRankGetDataQPNsTest, IsNicAwareModeActiveReturnsTrue) {
  ASSERT_EQ(ranks_.size(), 4);
  const auto* rank0 = ranks_[0].get();

  // After SetUp builds nicQpnPairMap, NIC-aware mode should be active
  EXPECT_TRUE(rank0->isNicAwareModeActive());
}

TEST_F(WorkloadRankGetDataQPNsTest, GetDataQPNByIndexThrowsWhenOutOfRange) {
  ASSERT_EQ(ranks_.size(), 4);
  const auto* rank0 = ranks_[0].get();

  const uint32_t totalQPs = rank0->getTotalDataQPCount(1);
  EXPECT_EQ(totalQPs, 1u); // Verify we know the expected count

  uint32_t srcNicIdx = 0;
  uint32_t dstNicIdx = 0;

  // Accessing an index beyond the total QP count should throw with specific
  // error message
  try {
    rank0->getDataQPNByIndexWithNic(1, totalQPs + 100, srcNicIdx, dstNicIdx);
    FAIL() << "Expected std::runtime_error to be thrown";
  } catch (const std::runtime_error& e) {
    std::string errorMsg = e.what();
    EXPECT_THAT(
        errorMsg, HasSubstr("QP index 101 out of range for rank 0 -> peer 1"));
    EXPECT_THAT(errorMsg, HasSubstr("total QPs: 1"));
  }
}

// A wrong peer set stalls the collective (a missed peer never posts its recv),
// so pin the exact set getPeers returns.
TEST_F(WorkloadRankGetDataQPNsTest, GetPeersReturnsAllOtherRanks) {
  ASSERT_EQ(ranks_.size(), 4);
  for (uint32_t srcRank = 0; srcRank < 4; ++srcRank) {
    auto peers = ranks_[srcRank]->getPeers();
    std::set<uint32_t> peerSet(peers.begin(), peers.end());
    EXPECT_EQ(peerSet.size(), peers.size())
        << "getPeers must not return a peer twice for rank " << srcRank
        << "; each (srcRank, dstRank) is a distinct map key";
    std::set<uint32_t> expected;
    for (uint32_t dstRank = 0; dstRank < 4; ++dstRank) {
      if (dstRank != srcRank) {
        expected.emplace(dstRank);
      }
    }
    EXPECT_EQ(peerSet, expected)
        << "rank " << srcRank << " must see exactly its 3 A2A peers";
  }
}

// The range-scan must return the same set as an exhaustive full-map scan
// filtered by srcRank.
TEST_F(WorkloadRankGetDataQPNsTest, GetPeersMatchesFullScan) {
  ASSERT_EQ(ranks_.size(), 4);
  for (uint32_t srcRank = 0; srcRank < 4; ++srcRank) {
    const auto* rank = ranks_[srcRank].get();
    std::set<uint32_t> reference;
    for (const auto& [rankPair, nicMap] : *rank->workCfg_->nicQpnPairMap()) {
      if (*rankPair.srcRank() == srcRank) {
        reference.emplace(*rankPair.dstRank());
      }
    }
    auto peers = rank->getPeers();
    std::set<uint32_t> peerSet(peers.begin(), peers.end());
    EXPECT_EQ(peerSet, reference)
        << "range-scan getPeers must equal the full-map scan for rank "
        << srcRank;
  }
}

} // namespace packsim
