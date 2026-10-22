/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/workloads/oss_tests/all-to-all-workload-rank-parallel-qp-test.h"

namespace packsim {

INSTANTIATE_TEST_SUITE_P(
    AllToAllWorkloadRankParallelQPTestSweep,
    AllToAllWorkloadRankParallelQPTest,
    ::testing::Combine(
        ::testing::Values(64),
        ::testing::Values(1024, 1024 * 1024, 512 * 1024 * 1024),
        ::testing::Values(1, 2, 4),
        ::testing::Values(1, 5),
        ::testing::Values(1, 5, 10)));

} // namespace packsim
