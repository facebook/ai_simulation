/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/endpoint/runtime/message-tracker.h"

#include <cstdint>
#include <utility>
#include <vector>

#include <folly/logging/xlog.h>

namespace packsim {

folly::F14FastMap<uint64_t, std::vector<packsim::thrift::PerMessageResult>>
collectPerMessageResults(const std::vector<const MessageTracker*>& trackers) {
  // Pass 1: collect sends and recvs in one traversal of trackers
  folly::F14FastMap<MessageKey, packsim::thrift::PerMessageResult> allRecords;
  folly::F14FastMap<MessageKey, uint64_t> allRecvTimes;

  for (const auto* tracker : trackers) {
    for (const auto& record : tracker->getSendRecords()) {
      const auto key = makeMessageKey(record);
      XDCHECK(
          !allRecords.contains(key),
          "Duplicate send MessageKey — possible key collision");
      allRecords.emplace(key, record);
    }
    for (const auto& [key, time] : tracker->getRecvTimes()) {
      allRecvTimes[key] = time;
    }
  }

  // Pass 2: merge recv times and group by workloadId
  // Send timestamps are already populated in the records from Pass 1.
  // Here we fill in receive timestamps and group in one traversal.
  // Records without a matching recv are skipped (one-sided RDMA and other
  // send-only patterns are not yet supported for per-message tracking).
  folly::F14FastMap<uint64_t, std::vector<packsim::thrift::PerMessageResult>>
      result;
  for (auto& [key, record] : allRecords) {
    const auto it = allRecvTimes.find(key);
    if (it == allRecvTimes.end()) {
      continue;
    }
    record.recvTimeNs() = static_cast<int64_t>(it->second);
    const auto wId = *record.workloadId();
    result[wId].emplace_back(std::move(record));
  }
  return result;
}

} // namespace packsim
