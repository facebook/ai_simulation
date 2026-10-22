/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <bit>
#include <cstdint>
#include <limits>

#include <folly/random/splitmix64.h>

namespace packsim {

/**
 * xoshiro256++ 1.0, a pseudo-random bit generator with 256 bits of state and a
 * period of 2^256 - 1. Random draws all of its random numbers from one.
 *
 * Written from the reference implementation by David Blackman and Sebastiano
 * Vigna, https://prng.di.unimi.it/xoshiro256plusplus.c, which its authors
 * dedicated to the public domain. Not suitable for cryptography.
 *
 * The constructor fills the state with four successive SplitMix64 outputs from
 * the seed, as the reference implementation suggests for a 64-bit seed. Those
 * four outputs are distinct, so the state is never all zero, the one state the
 * generator never leaves.
 *
 * It meets std::uniform_random_bit_generator, so it drives the <random>
 * distributions directly. Example usage:
 *
 *      Xoshiro256PlusPlus rng(seed);
 *      std::normal_distribution<double> noise(0.0, 1.0);
 *      const double sample = noise(rng);
 *
 * Any seed is valid, 0 included, and equal seeds give equal streams.
 *
 * NOTE: Not thread-safe: operator() advances the state. Give each thread, and
 * each object whose draws must not depend on thread scheduling, its own.
 */
class Xoshiro256PlusPlus {
 public:
  using result_type = uint64_t;

  /**
   * Start the stream that `seed` selects.
   *
   * @param seed   Any value.
   */
  explicit constexpr Xoshiro256PlusPlus(const uint64_t seed) noexcept
      : state_{seededState(seed)} {}

  // Smallest value operator() returns: 0.
  static constexpr result_type min() noexcept {
    return std::numeric_limits<result_type>::min();
  }

  // Largest value operator() returns: 2^64 - 1.
  static constexpr result_type max() noexcept {
    return std::numeric_limits<result_type>::max();
  }

  /**
   * Advance the state and return the next value.
   *
   * @return  The next value, uniform over [min(), max()].
   */
  constexpr result_type operator()() noexcept {
    const uint64_t result =
        std::rotl(state_[0] + state_[3], kOutputRotation) + state_[0];
    const uint64_t shiftedWord = state_[1] << kStateShift;
    state_[2] ^= state_[0];
    state_[3] ^= state_[1];
    state_[1] ^= state_[2];
    state_[0] ^= state_[3];
    state_[2] ^= shiftedWord;
    state_[3] = std::rotl(state_[3], kStateRotation);
    return result;
  }

 private:
  // The parameters of xoshiro256++ 1.0 (Blackman and Vigna, "Scrambled Linear
  // Pseudorandom Number Generators", ACM Transactions on Mathematical Software,
  // 2021): the state transition's shift a and rotation b, and the ++
  // scrambler's rotation r.
  static constexpr int kStateShift = 17;
  static constexpr int kStateRotation = 45;
  static constexpr int kOutputRotation = 23;

  /**
   * The state `seed` selects: four successive SplitMix64 outputs, starting
   * SplitMix64 from `seed`.
   *
   * @param seed   Any value.
   *
   * @return       The four words of state, in order.
   */
  static constexpr std::array<uint64_t, 4> seededState(
      const uint64_t seed) noexcept {
    uint64_t seederState = seed;
    std::array<uint64_t, 4> state{};
    for (uint64_t& word : state) {
      // Advances seederState, so each word gets the next SplitMix64 output.
      word = folly::splitmix64(seederState);
    }
    return state;
  }

  // The four 64-bit words of state; never all zero.
  std::array<uint64_t, 4> state_;
};

} // namespace packsim
