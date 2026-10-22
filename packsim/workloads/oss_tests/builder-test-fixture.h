/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

#include "packsim/if/gen-cpp2/workload_types.h"

namespace packsim {

/**
 * Workloads for the tests of the workload builder.
 */
class BuilderTestFixture {
 public:
  /**
   * A workload with no config but its endpoint count.
   *
   * @param numEndpoints  Number of endpoints.
   *
   * @return              The workload.
   */
  static packsim::workload::thrift::Workload makeWorkload(
      uint32_t numEndpoints) {
    packsim::workload::thrift::Workload w;
    w.numEndpoints() = numEndpoints;
    return w;
  }
};

} // namespace packsim
