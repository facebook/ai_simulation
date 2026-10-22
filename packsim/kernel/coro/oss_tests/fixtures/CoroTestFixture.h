/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/portability/GFlags.h>
#include <gtest/gtest.h>

namespace packsim::coro::tests {

/**
 * Shared test fixture for coro primitives. Pins the PDES worker pool to a
 * single thread so event ordering is deterministic across the test, and
 * restores the prior gflag state on teardown.
 *
 * Use SetCommandLineOption rather than writing FLAGS_pdes_threads directly:
 * the flag is defined in kernel/scheduler.cpp, so reaching it from this
 * translation unit would mean redeclaring it here.
 */
class CoroTestFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    gflags::SetCommandLineOption("pdes_threads", "1");
  }

 private:
  folly::gflags::FlagSaver flagSaver_;
};

} // namespace packsim::coro::tests
