/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <vector>

#include "packsim/if/gen-cpp2/workload_types.h"
// @oss-disable
#include "packsim/workloads/rank/workload-rank.h"

namespace packsim {

class IncastRank : public WorkloadRank {
 public:
  explicit IncastRank(const WorkloadRankParams& params);
  void run() override;
  void requestReceived(Message* msg);
  void requestAcked(Message* msg);
  uint64_t getExpectedBytes() override;
  bool complete() override;
  void progress() override;

  /**
   * Number of ranks that send to rank 0 (ranks 1..count): numSources, or every
   * rank other than rank 0 when numSources is 0.
   *
   * @param w      Incast workload.
   *
   * @return       The number of sending ranks.
   *
   * @throw std::invalid_argument if the workload has fewer than 2 endpoints or
   *        numSources is not below numEndpoints.
   */
  static uint32_t resolveNumSources(const workload::thrift::Workload& w);

 private:
  void sendMsgs();
  // Completes a rank that has no messages to send or receive.
  void completeWithoutWork();
  // True iff the workload is running in one-sided PUSH (WRITE) mode.
  bool isPushMode() const noexcept {
    return workloadMode_ == workload::thrift::WorkloadMode::PUSH;
  }

  // True iff this rank sends to rank 0 (rank in [1, numSrcs_]).
  bool isSender() const noexcept {
    return rank_ >= 1 && rank_ <= numSrcs_;
  }

  // True iff this rank is one of the PUSH-mode senders.
  bool isPushSender() const noexcept {
    return isPushMode() && isSender();
  }
  uint64_t minSendTimeNs_{};
  uint32_t sendSize_;
  uint32_t numChunks_;
  uint32_t numSrcs_;
  // @oss-disable
  uint32_t outstandingRecvs_{0};
  uint32_t outstandingWrites_{0};
  bool notifyRcvd_{false};
  std::deque<std::unique_ptr<Message>> sendMsgs_;
  std::vector<std::deque<std::unique_ptr<Message>>> recvMsgs_;
  workload::thrift::WorkloadMode workloadMode_{
      workload::thrift::WorkloadMode::SENDRCV};
};

} // namespace packsim
