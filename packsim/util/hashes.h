/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

#include "packsim/kernel/device-id.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

uint64_t folly5TupleHash(const PacketPtr& p, uint32_t salt);

uint64_t folly5TupleHash(
    const FlexDeviceId& src,
    const FlexDeviceId& dst,
    uint16_t srcPort,
    uint16_t dstPort,
    uint32_t salt);

} // namespace packsim
