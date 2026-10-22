/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace packsim {

/**
 * The transports a simulation can be built for.
 *
 * Kept separate from `builders.h` so that code needing only this vocabulary
 * does not have to depend on the builders, which depend on it.
 */
enum class TransportKind {
  ROCE,
  // @oss-disable
};

// Congestion-control algorithm requested for a RoCE transport.
enum class RoceCongestionControl {
  NONE,
  DCQCN,
};

// Loss-recovery algorithm requested for a RoCE transport.
enum class RoceLossRecovery {
  NONE,
  GO_BACK_N,
};

// A transport paired with the algorithms requested alongside it. Which
// transports are available depends on the build; RoCEv2 is always one of them
// and accepts every combination of the two algorithms above.
struct TransportSelection {
  TransportKind transport;
  RoceCongestionControl congestionControl;
  RoceLossRecovery lossRecovery;
};

} // namespace packsim
