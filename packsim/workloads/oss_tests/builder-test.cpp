/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/workloads/builder.h"
#include "packsim/workloads/oss_tests/builder-test-fixture.h"

namespace wt = packsim::workload::thrift;

namespace packsim {
namespace {

// An AllReduceAlgorithm value the IDL does not name, which deserialization
// keeps.
constexpr int32_t kAlgorithmUnknownToThisBinary = 42;

/**
 * Build rank 0 of a workload, with no simulation context or callbacks.
 *
 * @param w  The workload.
 */
void buildRankZero(const wt::Workload& w) {
  buildWorkloadRank(
      /*simCtx=*/nullptr,
      /*workloadId=*/0,
      w,
      /*rank=*/0,
      /*sendMessageCb=*/{},
      /*recvMessageCb=*/{},
      /*rankCompleteCb=*/{});
}

std::vector<uint32_t> sorted(std::vector<uint32_t> v) {
  std::sort(v.begin(), v.end());
  return v;
}

// Asserts every peer edge is bidirectional: if i lists p, then p lists i.
void expectSymmetricPeers(const std::vector<std::vector<uint32_t>>& peers) {
  for (uint32_t i = 0; i < peers.size(); ++i) {
    for (const uint32_t peer : peers.at(i)) {
      ASSERT_LT(peer, peers.size());
      const auto& reverse = peers.at(peer);
      EXPECT_NE(std::find(reverse.begin(), reverse.end(), i), reverse.end())
          << "rank " << i << " lists peer " << peer << " but not vice versa";
    }
  }
}

TEST(GetRankPeersTest, AllToAllProducesUpperTriangularPeers) {
  auto w = BuilderTestFixture::makeWorkload(4);
  w.config()->allToAll() = wt::AllToAll{};
  const auto peers = getRankPeers(w);
  ASSERT_EQ(peers.size(), 4u);
  // Each rank peers only with higher-numbered ranks; the consumer symmetrizes.
  EXPECT_EQ(peers.at(0), (std::vector<uint32_t>{1, 2, 3}));
  EXPECT_EQ(peers.at(1), (std::vector<uint32_t>{2, 3}));
  EXPECT_EQ(peers.at(2), (std::vector<uint32_t>{3}));
  EXPECT_TRUE(peers.at(3).empty());
}

TEST(GetRankPeersTest, IncastConnectsSourcesToRankZero) {
  auto w = BuilderTestFixture::makeWorkload(4);
  wt::Incast incast;
  incast.numSources() = 2;
  w.config()->incast() = incast;
  const auto peers = getRankPeers(w);
  ASSERT_EQ(peers.size(), 4u);
  EXPECT_EQ(peers.at(0), (std::vector<uint32_t>{1, 2}));
  EXPECT_EQ(peers.at(1), (std::vector<uint32_t>{0}));
  EXPECT_EQ(peers.at(2), (std::vector<uint32_t>{0}));
  EXPECT_TRUE(peers.at(3).empty());
}

TEST(GetRankPeersTest, IncastZeroSourcesConnectsEveryRankToRankZero) {
  auto w = BuilderTestFixture::makeWorkload(4);
  // numSources defaults to 0, which means every rank other than rank 0.
  w.config()->incast() = wt::Incast{};
  const auto peers = getRankPeers(w);
  ASSERT_EQ(peers.size(), 4u);
  EXPECT_EQ(peers.at(0), (std::vector<uint32_t>{1, 2, 3}));
  for (uint32_t rank = 1; rank < 4; ++rank) {
    EXPECT_EQ(peers.at(rank), (std::vector<uint32_t>{0})) << "rank " << rank;
  }
}

TEST(GetRankPeersTest, AllReduceRingConnectsNeighbors) {
  auto w = BuilderTestFixture::makeWorkload(4);
  wt::AllReduce allReduce;
  allReduce.algorithm() = wt::AllReduceAlgorithm::RING;
  w.config()->allReduce() = allReduce;
  const auto peers = getRankPeers(w);
  ASSERT_EQ(peers.size(), 4u);
  // In a ring, each rank's peer set is its two cyclic neighbors.
  EXPECT_EQ(sorted(peers.at(0)), (std::vector<uint32_t>{1, 3}));
  EXPECT_EQ(sorted(peers.at(1)), (std::vector<uint32_t>{0, 2}));
  EXPECT_EQ(sorted(peers.at(2)), (std::vector<uint32_t>{1, 3}));
  EXPECT_EQ(sorted(peers.at(3)), (std::vector<uint32_t>{0, 2}));
}

TEST(GetRankPeersTest, ComputeHasNoPeers) {
  auto w = BuilderTestFixture::makeWorkload(4);
  w.config()->compute() = wt::Compute{};
  EXPECT_TRUE(getRankPeers(w).empty());
}

TEST(GetRankPeersTest, PermutationConnectsEachRankToItsDestination) {
  auto w = BuilderTestFixture::makeWorkload(4);
  wt::Permutation perm;
  perm.dstRanks() = std::vector<uint32_t>{1, 2, 3, 0};
  w.config()->permutation() = perm;
  const auto peers = getRankPeers(w);
  ASSERT_EQ(peers.size(), 4u);
  // Each rank is bidirectionally connected to its destination.
  EXPECT_EQ(sorted(peers.at(0)), (std::vector<uint32_t>{1, 3}));
  EXPECT_EQ(sorted(peers.at(1)), (std::vector<uint32_t>{0, 2}));
  EXPECT_EQ(sorted(peers.at(2)), (std::vector<uint32_t>{1, 3}));
  EXPECT_EQ(sorted(peers.at(3)), (std::vector<uint32_t>{0, 2}));
}

TEST(GetRankPeersTest, AllReduceRecursiveHalvingDoublingPairsByDistance) {
  auto w = BuilderTestFixture::makeWorkload(4);
  wt::AllReduce allReduce;
  allReduce.algorithm() = wt::AllReduceAlgorithm::RECURSIVE_MSG_OPT;
  w.config()->allReduce() = allReduce;
  const auto peers = getRankPeers(w);
  ASSERT_EQ(peers.size(), 4u);
  EXPECT_EQ(sorted(peers.at(0)), (std::vector<uint32_t>{1, 2}));
  EXPECT_EQ(sorted(peers.at(1)), (std::vector<uint32_t>{0, 3}));
  EXPECT_EQ(sorted(peers.at(2)), (std::vector<uint32_t>{0, 3}));
  EXPECT_EQ(sorted(peers.at(3)), (std::vector<uint32_t>{1, 2}));
}

TEST(GetRankPeersTest, AllReduceDoubleBinaryTreeIsSymmetric) {
  auto w = BuilderTestFixture::makeWorkload(8);
  wt::AllReduce allReduce;
  allReduce.algorithm() = wt::AllReduceAlgorithm::DOUBLE_BINARY_TREE;
  w.config()->allReduce() = allReduce;
  const auto peers = getRankPeers(w);
  ASSERT_EQ(peers.size(), 8u);
  expectSymmetricPeers(peers);
  // Every rank participates in the double binary tree.
  for (const auto& rankPeers : peers) {
    EXPECT_FALSE(rankPeers.empty());
  }
}

TEST(GetRankPeersTest, ThrowsForUnsetConfigType) {
  auto w = BuilderTestFixture::makeWorkload(4);
  // No config union arm is set, so the workload type is unhandled.
  EXPECT_THAT(
      [&] { getRankPeers(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Unsupported workload type")));
}

TEST(GetRankPeersTest, ThrowsForUnknownAllReduceAlgorithm) {
  auto w = BuilderTestFixture::makeWorkload(4);
  wt::AllReduce allReduce;
  allReduce.algorithm() = wt::AllReduceAlgorithm::UNKNOWN;
  w.config()->allReduce() = allReduce;
  EXPECT_THAT(
      [&] { getRankPeers(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Unsupported algorithm")));
}

TEST(GetRankPeersTest, ThrowsForAnAllReduceAlgorithmOutsideTheEnum) {
  auto w = BuilderTestFixture::makeWorkload(/*numEndpoints=*/4);
  wt::AllReduce allReduce;
  allReduce.algorithm() =
      static_cast<wt::AllReduceAlgorithm>(kAlgorithmUnknownToThisBinary);
  w.config()->allReduce() = allReduce;
  EXPECT_THAT(
      [&] { getRankPeers(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Unsupported algorithm")));
}

TEST(GetRankPeersTest, ThrowsForNonPowerOfTwoRankCount) {
  // Recursive halving/doubling needs a power-of-2 rank count; 6 is not one.
  auto w = BuilderTestFixture::makeWorkload(6);
  wt::AllReduce allReduce;
  allReduce.algorithm() = wt::AllReduceAlgorithm::RECURSIVE_MSG_OPT;
  w.config()->allReduce() = allReduce;
  EXPECT_THAT(
      [&] { getRankPeers(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Number of ranks must be a power of 2")));
}

TEST(GetRankPeersTest, IncastRejectsMoreSourcesThanOtherRanks) {
  auto w = BuilderTestFixture::makeWorkload(4);
  wt::Incast incast;
  incast.numSources() = 4;
  w.config()->incast() = incast;
  EXPECT_THAT(
      [&] { getRankPeers(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("numSources 4")));
}

TEST(GetRankPeersTest, PermutationRejectsOutOfRangeDestination) {
  auto w = BuilderTestFixture::makeWorkload(4);
  wt::Permutation perm;
  perm.dstRanks() = std::vector<uint32_t>{1, 2, 3, 4};
  w.config()->permutation() = perm;
  EXPECT_THAT(
      [&] { getRankPeers(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("dstRanks[3] = 4")));
}

TEST(GetRankPeersTest, PermutationRejectsWrongDestinationCount) {
  auto w = BuilderTestFixture::makeWorkload(4);
  wt::Permutation perm;
  perm.dstRanks() = std::vector<uint32_t>{1, 2, 0};
  w.config()->permutation() = perm;
  EXPECT_THAT(
      [&] { getRankPeers(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("3 dstRanks entries")));
}

TEST(BuildWorkloadRankTest, ThrowsForAnUnsupportedAllGatherAlgorithm) {
  auto w = BuilderTestFixture::makeWorkload(/*numEndpoints=*/4);
  wt::AllGather allGather;
  allGather.algorithm() = wt::AllReduceAlgorithm::DOUBLE_BINARY_TREE;
  w.config()->allGather() = allGather;
  EXPECT_THAT(
      [&] { buildRankZero(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Unsupported all-gather algorithm")));
}

TEST(BuildWorkloadRankTest, ThrowsForAnUnsupportedReduceScatterAlgorithm) {
  auto w = BuilderTestFixture::makeWorkload(/*numEndpoints=*/4);
  wt::ReduceScatter reduceScatter;
  reduceScatter.algorithm() = wt::AllReduceAlgorithm::DOUBLE_BINARY_TREE;
  w.config()->reduceScatter() = reduceScatter;
  EXPECT_THAT(
      [&] { buildRankZero(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Unsupported reduce-scatter algorithm")));
}

// The algorithm switch sits inside the workload-type switch, so a value
// outside the enum must throw rather than fall into the next workload type.
TEST(BuildWorkloadRankTest, ThrowsForAnAllReduceAlgorithmOutsideTheEnum) {
  auto w = BuilderTestFixture::makeWorkload(/*numEndpoints=*/4);
  wt::AllReduce allReduce;
  allReduce.algorithm() =
      static_cast<wt::AllReduceAlgorithm>(kAlgorithmUnknownToThisBinary);
  w.config()->allReduce() = allReduce;
  EXPECT_THAT(
      [&] { buildRankZero(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Unsupported all-reduce algorithm")));
}

TEST(BuildWorkloadRankTest, ThrowsForUnsetConfigType) {
  const auto w = BuilderTestFixture::makeWorkload(/*numEndpoints=*/4);
  // No config union arm is set, so there is no rank type to build.
  EXPECT_THAT(
      [&] { buildRankZero(w); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("Unsupported workload type")));
}

} // namespace
} // namespace packsim
