/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/transport/transport.h"

#include <memory>

namespace packsim {

Socket::Socket(
    Transport* transport,
    const FlexDeviceId& src,
    const FlexDeviceId& dst,
    const uint16_t srcPort,
    const uint32_t localQPN,
    const uint32_t remoteQPN,
    const packsim::workload::thrift::Workload* workload)
    : simCtx_(transport->getSimCtx()),
      transport_(transport),
      srcPort_(srcPort),
      conn_(std::make_unique<QPConnection>(src, dst, localQPN, remoteQPN)),
      workload_(workload) {
  if (src == dst) {
    XLOG(WARNING, "Transport is doing a loopback connection");
  }
}

} // namespace packsim
