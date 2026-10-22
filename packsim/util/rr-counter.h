/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>

#include "packsim/kernel/sim-context.h"

namespace packsim {

template <typename T>
class RRCounter {
 public:
  RRCounter(SimContext* simCtx, T min, T max)
      : simCtx(simCtx), counter(min), min(min), max(max) {}

  RRCounter(SimContext* simCtx, T min, std::function<T()> maxFunc)
      : simCtx(simCtx), counter(min), min(min), maxFunc(maxFunc) {}

  const T& peek() const noexcept {
    return counter;
  }

  // Post-increment current counter
  T operator()() {
    T ret = counter;
    T maxVal = maxFunc ? maxFunc() : max;
    counter = (counter == maxVal) ? min : (counter + 1);
    return ret;
  }

  void randomize() {
    T maxVal = maxFunc ? maxFunc() : max;
    // Both std::uniform_int_distribution and RRCounter use min and
    // max as inclusive bounds.
    counter = simCtx->random.sampleUniform<T>(min, maxVal);
  }

  SimContext* simCtx{nullptr};
  T counter;
  T min;
  T max;
  std::function<T()> maxFunc;
};

} // namespace packsim
