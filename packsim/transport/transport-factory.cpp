/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/transport/transport-factory.h"

#include <memory>
#include <stdexcept>

// @oss-disable
#include "packsim/transport/roce-base.h"

namespace packsim {

std::unique_ptr<Transport> makeTransport(const TransportParams& params) {
  const auto& transportCfg = *params.nicConfig.transport();
  const auto transportType = transportCfg.getType();

  if (transportType == packsim::thrift::TransportConfig::Type::__EMPTY__) {
    throw std::invalid_argument("transport config is unset");
  }
  if (transportType == packsim::thrift::TransportConfig::Type::roceV2) {
    // The sub-unions only select the congestion-control / loss-recovery
    // strategies; every combination shares one RoceTransportBase.
    return std::make_unique<RoceTransportBase>(
        params.simCtx,
        params.flexId,
        &params.nicConfig,
        RoceTransportConfig::fromThrift(transportCfg));
  }
  // @oss-disable
  throw std::invalid_argument("unsupported transport type"); // @oss-enable
}

} // namespace packsim
