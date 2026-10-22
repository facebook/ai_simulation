/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <tuple>
#include <vector>

#include <folly/CPortability.h>
#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>

#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/transport/transport.h"

namespace packsim {

// Key: {workloadId, srcRank, dstRank, connectionMessageId, iteration}
using MessageKey = std::tuple<uint64_t, uint64_t, uint64_t, uint64_t, uint64_t>;

/**
 * Per-WorkloadNode tracker for per-message send/recv timestamps.
 * Each node records its own events independently (no synchronization needed).
 * Send records and recv times are stored separately and joined post-sim.
 */
class MessageTracker {
 public:
  /**
   * Called on the sender node in makeSendRequest().
   *
   * @param msg     The message being sent.
   * @param timeNs  The send timestamp in nanoseconds.
   */
  void recordSend(const Message* msg, uint64_t timeNs) {
    packsim::thrift::PerMessageResult record;
    record.workloadId() = msg->workloadId;
    record.srcRank() = msg->srcRank;
    record.dstRank() = msg->dstRank;
    record.messageIndex() = msg->connectionMessageId;
    record.sizeBytes() = msg->sizeBytes;
    record.iteration() = msg->iteration;
    record.sendTimeNs() = timeNs;
    record.recvTimeNs() = -1;
    sendRecords_.emplace_back(std::move(record));
  }

  /**
   * Called on the receiver node in onMessageReceived().
   *
   * @param msg     The message that was received.
   * @param timeNs  The receive timestamp in nanoseconds.
   */
  void recordRecv(const Message* msg, uint64_t timeNs) {
    MessageKey key{
        msg->workloadId,
        msg->srcRank,
        msg->dstRank,
        msg->connectionMessageId,
        msg->iteration};
    XDCHECK(
        !recvTimes_.contains(key),
        "Duplicate recv for MessageKey — possible retransmission or key collision");
    recvTimes_[key] = timeNs;
  }

  const std::vector<packsim::thrift::PerMessageResult>& getSendRecords()
      const noexcept {
    return sendRecords_;
  }

  const folly::F14FastMap<MessageKey, uint64_t>& getRecvTimes() const noexcept {
    return recvTimes_;
  }

 private:
  std::vector<packsim::thrift::PerMessageResult> sendRecords_;
  folly::F14FastMap<MessageKey, uint64_t> recvTimes_;
};

/**
 * Construct a MessageKey from a thrift PerMessageResult record.
 *
 * @param record  The per-message result to derive the key from.
 *
 * @return        The MessageKey identifying the message.
 */
FOLLY_ALWAYS_INLINE MessageKey
makeMessageKey(const packsim::thrift::PerMessageResult& record) {
  return MessageKey{
      *record.workloadId(),
      *record.srcRank(),
      *record.dstRank(),
      *record.messageIndex(),
      *record.iteration()};
}

/**
 * Join send records and recv times from multiple trackers into per-workload
 * result lists. Two-pass algorithm:
 *   1. Collect all send records and recv times from all trackers
 *   2. Merge recv times into send records and group by workloadId
 *
 * @param trackers  The per-node message trackers to aggregate.
 *
 * @return          Per-workload lists of joined per-message results, keyed by
 *                  workloadId.
 */
folly::F14FastMap<uint64_t, std::vector<packsim::thrift::PerMessageResult>>
collectPerMessageResults(const std::vector<const MessageTracker*>& trackers);

} // namespace packsim
