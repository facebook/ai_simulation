/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "packsim/kernel/xoshiro256-plus-plus.h"

namespace packsim {

static_assert(std::uniform_random_bit_generator<Xoshiro256PlusPlus>);
static_assert(Xoshiro256PlusPlus::min() == 0);
static_assert(
    Xoshiro256PlusPlus::max() == std::numeric_limits<uint64_t>::max());
static_assert(sizeof(Xoshiro256PlusPlus) == 4 * sizeof(uint64_t));

namespace {

using State = std::array<uint64_t, 4>;

// Draws compared with the reference model for each seed.
constexpr size_t kReferenceDraws = 1000;

/**
 * One step of the reference SplitMix64, https://prng.di.unimi.it/splitmix64.c,
 * transcribed with its literals and with the state passed in.
 *
 * @param state  The 64-bit state, advanced in place.
 *
 * @return       The output for the advanced state.
 */
uint64_t referenceSplitMix64(uint64_t& state) {
  uint64_t z = (state += 0x9e3779b97f4a7c15ULL);
  z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
  z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
  return z ^ (z >> 31);
}

/**
 * One step of the reference implementation's next(),
 * https://prng.di.unimi.it/xoshiro256plusplus.c, transcribed with its literals
 * and with the state passed in, so a test can start it anywhere.
 *
 * @param state  The four words of state, advanced in place.
 *
 * @return       The output for the state as it was on entry.
 */
uint64_t referenceNext(State& state) {
  const uint64_t result = std::rotl(state[0] + state[3], 23) + state[0];
  const uint64_t t = state[1] << 17;
  state[2] ^= state[0];
  state[3] ^= state[1];
  state[1] ^= state[2];
  state[0] ^= state[3];
  state[2] ^= t;
  state[3] = std::rotl(state[3], 45);
  return result;
}

/**
 * The state the reference implementation suggests for a 64-bit seed: four
 * successive outputs of a SplitMix64 started from the seed.
 *
 * @param seed   Any value.
 *
 * @return       The four words of state, in order.
 */
State referenceSeededState(uint64_t seed) {
  State state{};
  for (uint64_t& word : state) {
    word = referenceSplitMix64(seed);
  }
  return state;
}

std::vector<uint64_t> drawReference(State state, const size_t count) {
  std::vector<uint64_t> values;
  values.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    values.emplace_back(referenceNext(state));
  }
  return values;
}

std::vector<uint64_t> draw(Xoshiro256PlusPlus& rng, const size_t count) {
  std::vector<uint64_t> values;
  values.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    values.emplace_back(rng());
  }
  return values;
}

} // namespace

// The first outputs of the reference splitmix64.c for seed 0. They anchor the
// seeding model above to the reference itself.
TEST(Xoshiro256PlusPlusTest, ReferenceSeederMatchesReferenceImplementation) {
  const std::vector<uint64_t> expected = {
      0xe220a8397b1dcdafULL,
      0x6e789e6aa1b965f4ULL,
      0x06c45d188009454fULL,
      0xf88bb8a8724c81ecULL,
      0x1b39896a51a8749bULL,
  };
  uint64_t state = 0;
  std::vector<uint64_t> outputs;
  outputs.reserve(expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    outputs.emplace_back(referenceSplitMix64(state));
  }

  EXPECT_EQ(outputs, expected);
}

// Outputs of the reference xoshiro256plusplus.c from the state {1, 2, 3, 4}.
// They anchor the step model above to the reference itself.
TEST(Xoshiro256PlusPlusTest, ReferenceModelMatchesReferenceImplementation) {
  const std::vector<uint64_t> expected = {
      41943041ULL,
      58720359ULL,
      3588806011781223ULL,
      3591011842654386ULL,
      9228616714210784205ULL,
      9973669472204895162ULL,
      14011001112246962877ULL,
      12406186145184390807ULL,
      15849039046786891736ULL,
      10450023813501588000ULL,
  };

  EXPECT_EQ(drawReference({1, 2, 3, 4}, expected.size()), expected);
}

class Xoshiro256PlusPlusSeedTest : public ::testing::TestWithParam<uint64_t> {};

TEST_P(Xoshiro256PlusPlusSeedTest, MatchesReferenceModelSeededFromSplitMix64) {
  const uint64_t seed = GetParam();
  Xoshiro256PlusPlus rng(seed);

  EXPECT_EQ(
      draw(rng, kReferenceDraws),
      drawReference(referenceSeededState(seed), kReferenceDraws));
}

INSTANTIATE_TEST_SUITE_P(
    Seeds,
    Xoshiro256PlusPlusSeedTest,
    ::testing::Values(
        uint64_t{0},
        uint64_t{1},
        uint64_t{0x0123456789abcdefULL},
        std::numeric_limits<uint64_t>::max()));

// What the reference implementation draws for seed 42, seeded as above.
// Simulation results reproduce from a seed only while this holds.
TEST(Xoshiro256PlusPlusTest, PinsTheStreamForAFixedSeed) {
  const std::vector<uint64_t> expected = {
      0xd0764d4f4476689fULL,
      0x519e4174576f3791ULL,
      0xfbe07cfb0c24ed8cULL,
      0xb37d9f600cd835b8ULL,
  };
  Xoshiro256PlusPlus rng(42);

  EXPECT_EQ(draw(rng, expected.size()), expected);
}

// What the reference implementation draws for the largest seed, seeded as
// above: the seed's high 32 bits select the stream too.
TEST(Xoshiro256PlusPlusTest, PinsTheStreamForTheLargestSeed) {
  const std::vector<uint64_t> expected = {
      0x56ccf8ce948e27b2ULL,
      0xe68588432e5a5b90ULL,
      0xe3e9b5a48119ca8bULL,
  };
  Xoshiro256PlusPlus rng(std::numeric_limits<uint64_t>::max());

  EXPECT_EQ(draw(rng, expected.size()), expected);
}

} // namespace packsim
