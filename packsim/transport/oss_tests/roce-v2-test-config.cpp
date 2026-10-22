/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/transport/oss_tests/roce-v2-test-config.h"

namespace packsim {

packsim::thrift::RoceV2TransportConfig RoceV2TestConfig::makeTransportConfig() {
  packsim::thrift::RoceV2TransportConfig config;
  config.congestionControl()->dummy() =
      packsim::thrift::DummyRoceV2StrategyConfig{};
  config.lossRecovery()->dummy() = packsim::thrift::DummyRoceV2StrategyConfig{};
  return config;
}

void RoceV2TestConfig::configureRoceV2Nic(
    packsim::thrift::NicConfig& nicConfig) {
  nicConfig.transport()->roceV2() = makeTransportConfig();
  nicConfig.perCosBufferSize() = {kCosBufferSizeBytes};
  nicConfig.perCosXon() = {kCosXonBytes};
  nicConfig.perCosXoff() = {kCosXoffBytes};
}

} // namespace packsim
