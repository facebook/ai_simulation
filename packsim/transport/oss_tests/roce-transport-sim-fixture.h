/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"

namespace packsim {

/*
 * A NIC config and simulation context sized so a RoCE transport can actually
 * run: 400Gbps, buffers large enough that nothing back-pressures, DCQCN
 * enabled. Shared by the RoCEv2 suites in this directory.
 */

class RoceTransportSimTest : public ::testing::Test {
 public:
  void SetUp() override {
    // 1us lookahead so DCQCN timers can fire and the sim can drain.
    scheduler_.setLookaheadTimeFs(1'000'000'000);
    nicCfg_.writeEff() = 100;
    nicCfg_.readGbps() = 400;
    nicCfg_.readLatencyNs() = 100;
    nicCfg_.perCosBufferSize() = {1024 * 1024 * 1024, 1024 * 1024 * 1024};
    nicCfg_.perCosXoff() = {1000 * 1024 * 1024, 1000 * 1024 * 1024};
    nicCfg_.perCosXon() = {1000 * 1024 * 1024, 1000 * 1024 * 1024};
    nicCfg_.speed() = "400Gbps";
    *nicCfg_.dcqcnCfg()->dcqcnEnable() = true;
  }

 protected:
  Tracer::ConditionalMap conditionalMap_;
  Scheduler scheduler_;
  SimContext simCtx_{&scheduler_, "SimContext", 0, &conditionalMap_};
  FlexDeviceId srcId_{{1, 0, 0, 1}};
  FlexDeviceId dstId_{{1, 0, 0, 2}};
  packsim::thrift::NicConfig nicCfg_;
};

} // namespace packsim
