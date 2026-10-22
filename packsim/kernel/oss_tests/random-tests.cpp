/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <algorithm>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/random.h"

namespace packsim {

using ::testing::HasSubstr;
using ::testing::ThrowsMessage;

namespace {
// Any fixed seed; a second one that differs from it.
constexpr uint64_t kSeed = 1234;
constexpr uint64_t kOtherSeed = 5678;
// Values to draw from each of two generators to compare their sequences.
constexpr int kNumDraws = 8;

// Returns the first kNumDraws values a generator draws.
std::vector<uint64_t> drawSequence(Random& rng) {
  std::vector<uint64_t> values;
  values.reserve(kNumDraws);
  for (int i = 0; i < kNumDraws; ++i) {
    values.emplace_back(rng.sampleUniform<uint64_t>());
  }
  return values;
}
} // namespace

TEST(Random, IsDeterministicForFixedSeed) {
  Random a(1234);
  Random b(1234);
  std::vector<uint64_t> seqA;
  std::vector<uint64_t> seqB;
  seqA.reserve(8);
  seqB.reserve(8);
  for (int i = 0; i < 8; ++i) {
    seqA.emplace_back(a.sampleUniform<uint64_t>());
    seqB.emplace_back(b.sampleUniform<uint64_t>());
  }
  EXPECT_EQ(seqA, seqB);
}

TEST(Random, SetSeedReplaysSequence) {
  Random rng(42);
  std::vector<uint64_t> firstRun;
  firstRun.reserve(8);
  for (int i = 0; i < 8; ++i) {
    firstRun.emplace_back(rng.sampleUniform<uint64_t>());
  }
  rng.setSeed(42);
  std::vector<uint64_t> replayRun;
  replayRun.reserve(8);
  for (int i = 0; i < 8; ++i) {
    replayRun.emplace_back(rng.sampleUniform<uint64_t>());
  }
  EXPECT_EQ(firstRun, replayRun);
}

TEST(Random, SeedConstructorSeedsLikeSetSeed) {
  Random constructed(kSeed);
  Random reseeded(kOtherSeed);
  reseeded.setSeed(kSeed);

  EXPECT_EQ(drawSequence(constructed), drawSequence(reseeded));
}

TEST(Random, DefaultConstructorReportsTheSeedItDrew) {
  Random drawn;
  Random replay(drawn.getSeed());

  EXPECT_EQ(drawSequence(drawn), drawSequence(replay));
}

TEST(Random, SimulationConfigDeterministicSeedSeedsTheGenerator) {
  thrift::Simulation simCfg;
  simCfg.random()->seed() = kSeed;
  simCfg.random()->useDeterministicSeed() = true;

  Random fromConfig(simCfg);
  Random fromSeed(kSeed);

  EXPECT_EQ(fromConfig.getSeed(), kSeed);
  EXPECT_EQ(drawSequence(fromConfig), drawSequence(fromSeed));
}

TEST(Random, SimulationConfigWithoutDeterministicSeedReportsTheSeedItDrew) {
  thrift::Simulation simCfg;
  simCfg.random()->useDeterministicSeed() = false;

  Random fromConfig(simCfg);
  Random replay(fromConfig.getSeed());

  EXPECT_EQ(drawSequence(fromConfig), drawSequence(replay));
}

TEST(Random, GetSeedReturnsConstructorValue) {
  Random rng(7);
  EXPECT_EQ(rng.getSeed(), 7u);
}

TEST(Random, GetSeedReturnsSetSeedValue) {
  Random rng(7);
  rng.setSeed(99);
  EXPECT_EQ(rng.getSeed(), 99u);
}

TEST(Random, SampleUniformDegenerateRangeReturnsBound) {
  Random rng(1);
  EXPECT_EQ(rng.sampleUniform<int>(5, 5), 5);
}

TEST(Random, SampleUniformStaysWithinInclusiveRange) {
  Random rng(2);
  for (int i = 0; i < 1000; ++i) {
    const int value = rng.sampleUniform<int>(10, 20);
    EXPECT_GE(value, 10);
    EXPECT_LE(value, 20);
  }
}

// An 8-bit type is sampled through a wider distribution; every value of the
// inclusive range, and nothing outside it, comes back.
TEST(Random, SampleUniformUint8CoversItsInclusiveRange) {
  Random rng(3);
  std::set<uint8_t> seen;
  for (int i = 0; i < 1000; ++i) {
    seen.emplace(rng.sampleUniform<uint8_t>(250, 255));
  }
  EXPECT_EQ(seen, (std::set<uint8_t>{250, 251, 252, 253, 254, 255}));
}

TEST(Random, SampleUniformInt8CoversItsInclusiveRange) {
  Random rng(4);
  std::set<int8_t> seen;
  for (int i = 0; i < 1000; ++i) {
    seen.emplace(rng.sampleUniform<int8_t>(-2, 2));
  }
  EXPECT_EQ(seen, (std::set<int8_t>{-2, -1, 0, 1, 2}));
}

TEST(Random, SampleBernoulliRejectsNegativeProbability) {
  Random rng(1);
  EXPECT_THAT(
      [&] { rng.sampleBernoulli(-0.1); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("[0, 1]")));
}

TEST(Random, SampleBernoulliRejectsProbabilityAboveOne) {
  Random rng(1);
  EXPECT_THAT(
      [&] { rng.sampleBernoulli(1.1); },
      ThrowsMessage<std::invalid_argument>(HasSubstr("[0, 1]")));
}

TEST(Random, SampleBernoulliIsDeterministicAtBoundaries) {
  Random rng(1);
  EXPECT_FALSE(rng.sampleBernoulli(0.0));
  EXPECT_TRUE(rng.sampleBernoulli(1.0));
}

TEST(Random, SampleExponentialZeroParamIsZero) {
  Random rng(1);
  EXPECT_EQ(rng.sampleExponential(0.0), 0.0);
}

TEST(Random, SampleExponentialPositiveIsNonNegative) {
  Random rng(3);
  EXPECT_GE(rng.sampleExponential(5.0), 0.0);
}

TEST(Random, SampleExponentialIsDeterministicForFixedSeed) {
  Random a(3);
  Random b(3);
  EXPECT_EQ(a.sampleExponential(5.0), b.sampleExponential(5.0));
}

TEST(Random, ShuffleIsADeterministicPermutation) {
  std::vector<int> base;
  base.reserve(20);
  for (int i = 0; i < 20; ++i) {
    base.emplace_back(i);
  }
  std::vector<int> shuffledA = base;
  std::vector<int> shuffledB = base;
  Random rngA(11);
  Random rngB(11);
  rngA.shuffle(shuffledA.begin(), shuffledA.end());
  rngB.shuffle(shuffledB.begin(), shuffledB.end());
  EXPECT_EQ(shuffledA, shuffledB);
  EXPECT_TRUE(
      std::is_permutation(shuffledA.begin(), shuffledA.end(), base.begin()));
  // With 20 elements the identity permutation is astronomically unlikely, so
  // this confirms shuffle actually reorders rather than being a no-op.
  EXPECT_NE(shuffledA, base);
}

} // namespace packsim
