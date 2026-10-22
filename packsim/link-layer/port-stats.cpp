/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/port-stats.h"

namespace packsim {

packsim::switches::thrift::InterfaceStats PortStats::toThrift() const {
  packsim::switches::thrift::InterfaceStats s;
  s.txPkts() = txPkts;
  s.txBytes() = txBytes;
  s.rxPkts() = rxPkts;
  s.rxBytes() = rxBytes;

  s.txXon() = txXon;
  s.txXoff() = txXoff;
  s.rxXon() = rxXon;
  s.rxXoff() = rxXoff;

  s.txPauseDurationNs() = txPauseDurationNs;
  s.rxPauseDurationNs() = rxPauseDurationNs;

  s.ecnCeMarkedPkts() = ecnCeMarkedPkts;
  s.rxEcnCePkts() = rxEcnCePkts;
  s.txEcnCePkts() = txEcnCePkts;

  s.rxTtldPkts() = rxTtldPkts;
  s.txTtldPkts() = txTtldPkts;

  s.dropPkts() = dropPkts;
  s.dropBytes() = dropBytes;

  if (llrStats.has_value()) {
    s.llrStats() = llrStats.value();
  }

  if (cbfcStats.has_value()) {
    s.cbfcStats() = cbfcStats.value();
  }

  return s;
}

} // namespace packsim
