/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <type_traits>

#include <folly/logging/xlog.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/xoshiro256-plus-plus.h"

namespace packsim {

class Random {
 public:
  /**
   * Seeds the generator with a seed drawn from std::random_device.
   *
   * @throw  std::runtime_error if no random device is available.
   */
  Random() : Random(std::random_device{}()) {}

  /**
   * Seeds the generator with the given seed.
   *
   * @param seed  The seed; the same seed replays the same sequence.
   */
  /* implicit */ Random(uint64_t seed) noexcept : seed_(seed), engine_(seed) {}

  /**
   * Seeds the generator as the simulation's random settings ask (see
   * pickSeed()).
   *
   * @param simCfg  The simulation config.
   *
   * @throw         std::runtime_error if the settings ask for a fresh seed and
   *                no random device is available.
   */
  explicit Random(const packsim::thrift::Simulation& simCfg)
      : Random(pickSeed(*simCfg.random())) {}

  void setSeed(uint64_t s) {
    seed_ = s;
    engine_ = Xoshiro256PlusPlus(seed_);
  }

  uint64_t getSeed() const noexcept {
    return seed_;
  }

  template <class RandomIt>
  void shuffle(RandomIt first, RandomIt last) {
    std::shuffle(first, last, engine_);
  }

  template <typename T>
  T sampleUniform(
      T min = std::numeric_limits<T>::min(),
      T max = std::numeric_limits<T>::max()) {
    static_assert(
        std::is_arithmetic_v<T>,
        "Random type must be integral or floating point");
    if constexpr (std::is_integral_v<T>) {
      static_assert(
          !std::is_same_v<T, bool> && !std::is_same_v<T, char> &&
              !std::is_same_v<T, wchar_t> && !std::is_same_v<T, char8_t> &&
              !std::is_same_v<T, char16_t> && !std::is_same_v<T, char32_t>,
          "Random integers must be numbers, not characters or bool");
      // std::uniform_int_distribution accepts only 16-bit and wider integer
      // types, so a narrower one (int8_t, uint8_t) is sampled as int or
      // unsigned and narrowed back; the result lies in [min, max].
      if constexpr (sizeof(T) < sizeof(int16_t)) {
        using Wide = std::conditional_t<std::is_signed_v<T>, int, unsigned>;
        std::uniform_int_distribution<Wide> distrib(min, max);
        return static_cast<T>(distrib(engine_));
      } else {
        std::uniform_int_distribution<T> distrib(min, max);
        return distrib(engine_);
      }
    }
    if constexpr (std::is_floating_point_v<T>) {
      std::uniform_real_distribution<T> distrib(min, max);
      return distrib(engine_);
    }
  }

  template <typename retType = double, typename paramType = double>
  retType sampleNormal(paramType mu = 0, paramType sigma = 1) {
    static_assert(
        std::is_floating_point_v<retType>,
        "Normal return value must be floating point");
    std::normal_distribution<retType> distrib(mu, sigma);
    return distrib(engine_);
  }

  bool sampleBernoulli(double p) {
    if ((p < 0) || (p > 1)) {
      throw std::invalid_argument("Bernoulli parameter must be in [0, 1]");
    }
    std::bernoulli_distribution distrib(p);
    return distrib(engine_);
  }

  double sampleExponential(double timeParam) {
    if (timeParam == 0) {
      return 0;
    }
    std::exponential_distribution<double> distrib(1.0 / timeParam);
    return distrib(engine_);
  }

 protected:
  // The seed the current stream started from.
  uint64_t seed_;
  // The generator every sample comes from.
  Xoshiro256PlusPlus engine_;

 private:
  /**
   * Returns the seed random settings ask for: their own seed when they ask for
   * a deterministic one, otherwise a fresh one from std::random_device. Logs
   * the seed either way.
   *
   * @param settings  The simulation's random settings.
   *
   * @return          The seed to seed the generator with.
   * @throw           std::runtime_error if the settings ask for a fresh seed
   *                  and no random device is available.
   */
  static uint64_t pickSeed(const packsim::thrift::RandomSettings& settings) {
    if (*settings.useDeterministicSeed()) {
      const uint64_t seed = *settings.seed();
      XLOGF(INFO, "Set random seed to {}", seed);
      return seed;
    }
    const uint64_t seed = std::random_device{}();
    XLOGF(INFO, "Using randomly generated seed of {}", seed);
    return seed;
  }
};

} // namespace packsim
