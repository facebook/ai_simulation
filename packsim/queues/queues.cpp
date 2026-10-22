/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/queues/queues.h"

#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <vector>

#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/Utility.h>
#include <folly/container/F14Map.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/sim-context.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/queues/queue-filter.h"
#include "packsim/queues/queue-packet-handler.h"
#include "packsim/queues/queue-scheduler.h"

namespace packsim {

namespace {
// A switch port has at most 8 CoS queues.
constexpr uint8_t kMaxNumQueues = 8;
// Number of WRED/ECN colors (e.g. green, yellow, red) per queue.
constexpr size_t kNumColors = 3;
} // namespace

Queues::Queues(
    SimContext* simCtx,
    packsim::thrift::QueueFilterConfig filterCfg,
    const packsim::thrift::SchedulingAlgorithmType schedulerType,
    std::vector<packsim::thrift::PortQueueConfig> handlerCfgs)
    : simCtx_(simCtx) {
  if (handlerCfgs.empty() || (handlerCfgs.size() > kMaxNumQueues)) {
    throw std::runtime_error(
        fmt::format("Must have between 1 and {} queues", kMaxNumQueues));
  }
  numQueues_ = static_cast<uint8_t>(handlerCfgs.size());
  cosQueues_.resize(numQueues_);
  minQSizeThreshold_ = *filterCfg.minQSizeThreshold();
  maxQSizeThreshold_ = *filterCfg.maxQSizeThreshold();
  const auto filterType = *filterCfg.field();
  // initialize filter
  switch (filterType) {
    case packsim::thrift::QueueFilterField::DSCP:
      filter_ = std::make_unique<DscpQueueFilter>(filterCfg);
      break;
    default:
      throw std::runtime_error("Unknown queue filter type");
  }

  useColor_ = !filterCfg.fieldToColor()->empty();

  auto defaultWrrWeights = std::vector<uint32_t>{1, 9, 10, 20, 30, 40, 50, 100};
  // in practice, there are either 8 queues (switch port) or 1; this
  // ensures that the weights vector is the correct size, either
  // extending with weight=100 or chopping.
  defaultWrrWeights.resize(numQueues_, 100);

  // initialize scheduler
  switch (schedulerType) {
    case packsim::thrift::SchedulingAlgorithmType::RR:
      scheduler_ = std::make_unique<RrQueueScheduler>(this);
      break;
    case packsim::thrift::SchedulingAlgorithmType::WRR:
      scheduler_ = std::make_unique<WrrQueueScheduler>(this, defaultWrrWeights);
      break;
    case packsim::thrift::SchedulingAlgorithmType::SP:
      scheduler_ = std::make_unique<SpQueueScheduler>(this);
      break;
    default:
      throw std::runtime_error(
          fmt::format(
              "Unknown queue scheduler type {}; use RR (0), WRR (1) or SP (3)",
              folly::to_underlying(schedulerType)));
  }

  if (useColor_) {
    coloredHandlers_.resize(numQueues_);
    for (auto& handler : coloredHandlers_) {
      handler.resize(kNumColors);
    }
  } else {
    handlers_.resize(numQueues_);
  }

  for (uint8_t i = 0; i < numQueues_; ++i) {
    const auto& queueHandler = handlerCfgs.at(i);

    if (useColor_) {
      for (const auto& [color, cfg] : *queueHandler.coloredWredEcnCfgs()) {
        coloredHandlers_.at(i).at(folly::to_underlying(color)) =
            buildHandler(simCtx_, i, cfg, this);
      }
    } else {
      const auto& cfg = *queueHandler.wredEcnCfg();
      handlers_.at(i) = buildHandler(simCtx_, i, cfg, this);
    }
  }
}

QueuePacketHandler* Queues::getHandler(const PacketPtr& pkt) const {
  const uint8_t cos = filter(pkt);
  return getHandler(pkt, cos);
}

QueuePacketHandler* Queues::getHandler(const PacketPtr& pkt, const uint8_t cos)
    const {
  if (!useColor_) {
    return handlers_.at(cos).get();
  }
  const auto color = pkt.as<EthernetPacket>()->tag.color;
  auto* handler =
      coloredHandlers_.at(cos).at(folly::to_underlying(color)).get();
  if (handler == nullptr) [[unlikely]] {
    throw std::invalid_argument(
        fmt::format(
            "Queue {} has no coloredWredEcnCfgs entry for color {}. A "
            "non-empty QueueFilterConfig.fieldToColor needs one for every "
            "color it can assign, including GREEN for unmapped DSCP values.",
            cos,
            apache::thrift::util::enumNameSafe(color)));
  }
  return handler;
}

} // namespace packsim
