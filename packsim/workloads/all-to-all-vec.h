/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <vector>

#include "packsim/workloads/all-to-all.h"

namespace packsim {

class VectorizedAllToAllRank : public AllToAllRank {
 public:
  explicit VectorizedAllToAllRank(const WorkloadRankParams& params);
  uint32_t getSendSize(uint32_t dstRank, uint32_t chunk) const override;
  uint64_t getSendDelay(uint32_t dstRank, uint32_t chunkId) const override;
  uint32_t getRecvSize(uint32_t srcRank, uint32_t chunk) const override;
  uint64_t getExpectedBytes() override;
  void sendNotifies() override;

 private:
  std::vector<uint32_t> sendSizes_;
  std::vector<uint32_t> recvSizes_;
  std::vector<uint64_t> sendDelays_;
};

} // namespace packsim
