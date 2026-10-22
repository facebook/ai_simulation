/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <vector>

#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/transport/transport.h"

namespace packsim {

struct ChannelParams {
  // The rank calls this to hand a message to the node that owns it.
  using SubmitMessageFn = std::function<void(Message*)>;

  // The rank fires these upward when one of its requests completes. Same
  // signature as SubmitMessageFn, deliberately distinct types: these travel
  // the opposite direction.
  using OnRequestAckedCallback = std::function<void(Message*)>;
  using OnRequestReceivedCallback = std::function<void(Message*)>;
  const SubmitMessageFn& smcb;
  const SubmitMessageFn& rmcb;
  uint32_t numChannels{0};
  uint32_t numQPsPerChannel{0};
  uint32_t messageSizeBytes{0};
  uint32_t workloadId{0};
  const packsim::workload::thrift::Workload* workload{nullptr};
  std::map<uint32_t, std::vector<std::vector<uint32_t>>> const& rankToIdList;
  packsim::workload::thrift::QPNPairMapMap const& qpPairMap;
  uint32_t rank{0};
};

} // namespace packsim
