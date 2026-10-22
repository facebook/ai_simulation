/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "packsim/if/gen-cpp2/nic_config_types.h"
#include "packsim/if/gen-cpp2/roce_v2_config_types.h"

namespace packsim {

/**
 * RoCEv2 transport configuration for the tests in this directory.
 *
 * Every fixture in this directory selects the RoCEv2 transport arm explicitly.
 *
 * A default-constructed `RoceV2TransportConfig` is NOT usable: both of its
 * sub-unions start unset, and `RoceTransportConfig::fromThrift` rejects an
 * unset arm with `std::invalid_argument` ("unsupported RoCE congestion control
 * config" / "unsupported RoCE loss recovery config"). Every fixture therefore
 * has to select an arm explicitly, which is what this helper exists to do.
 */
class RoceV2TestConfig {
 public:
  /**
   * A RoCEv2 transport config with congestion control and loss recovery both
   * explicitly disabled.
   *
   * This is the neutral baseline: no DCQCN, no Go-Back-N, so a test observes
   * the plain RoCEv2 data path without a strategy perturbing pacing or
   * retransmission. Tests that need DCQCN or Go-Back-N should build their own
   * config rather than mutating this one.
   *
   * @return A RoceV2TransportConfig with both sub-unions set to `dummy`.
   */
  static packsim::thrift::RoceV2TransportConfig makeTransportConfig();

  /**
   * Configure a NicConfig for RoCEv2: select the transport arm and fill in the
   * NIC fields RoCEv2 needs.
   *
   * Swapping the transport arm alone is not enough. `RoceTransportBase` owns a
   * `NicRxBuffer`, which sizes its CoS queues from `perCosXoff` and rejects a
   * count of zero ("Must have between 1 and 8 queues"). A fixture that leaves
   * `perCosBufferSize` / `perCosXon` / `perCosXoff` unset therefore aborts in
   * the Endpoint constructor.
   *
   * This sets a single CoS queue, which is what a test wants unless it is
   * specifically exercising multi-CoS behaviour. A test that routes on a CoS
   * above 0 needs to extend all three perCos vectors to cover it --
   * `NicRxBuffer` sizes its queues from `perCosXoff` alone. Existing values are
   * overwritten, so call it before applying any test-specific overrides.
   *
   * @param nicConfig  The NIC config to modify in place.
   */
  static void configureRoceV2Nic(packsim::thrift::NicConfig& nicConfig);

 private:
  // One CoS queue's buffer budget. The absolute sizes are arbitrary -- no test
  // here fills the buffer -- so they are round numbers rather than tuned ones.
  //
  // The ordering is not arbitrary. Nothing validates it (NicRxBuffer's
  // constructor checks only the vectors' lengths), but its PFC hysteresis
  // asserts XOFF at `usedBytes >= xoffBytes` and releases at
  // `usedBytes <= xonBytes`, so XOFF must sit above XON or the two states
  // would oscillate. Both stay below the buffer size.
  static constexpr uint32_t kCosBufferSizeBytes = 512 * 1024;
  static constexpr int32_t kCosXoffBytes = 448 * 1024;
  static constexpr int32_t kCosXonBytes = 384 * 1024;
};

} // namespace packsim
