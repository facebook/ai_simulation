/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace packsim {

/**
 * An LLR Control Ordered Set type, from UEC §5.1.3.1.1 Tbl 5-7 p. 467.
 *
 * LLR DATA is absent because a data frame is an ordinary Ethernet frame
 * carrying `EthernetPacket::llrHeader`, not an ordered set.
 *
 * There is no zero-valued sentinel, because a `LlrCtlosSlots` slot expresses
 * absence with `std::optional`.
 */
enum class LlrCtlosType : uint8_t {
  LLR_ACK,
  LLR_NACK,
  LLR_INIT,
  LLR_INIT_ECHO,
};

/**
 * The contents of an LLR Control Ordered Set.
 *
 * `LlrCtlosSlots` holds one of these for each pending report and
 * `LlrCtlosFrame` carries one on the wire, so the two cannot disagree about
 * what an ordered set contains. CBFC splits the same way, with
 * `CbfcCfUpdatePayload`.
 *
 * A pending report is this plain value rather than a packet because every
 * `LlrCtlosSlots::post*` overwrites its slot to coalesce reports (UEC
 * §5.1.9.1.2 and §5.1.9.1.3). Holding a pooled packet in a slot would
 * allocate and free one per post; the packet is allocated once, when the
 * report is popped.
 */
struct LlrCtlosPayload {
  LlrCtlosType type{LlrCtlosType::LLR_ACK};
  // The sequence number being acknowledged, for LLR_ACK and LLR_NACK. For
  // LLR_INIT and LLR_INIT_ECHO it is the sequence the link starts from.
  uint32_t seq{0};
  // An opaque value that LLR_INIT_ECHO echoes back from the LLR_INIT it
  // answers. LLR_ACK and LLR_NACK leave it zero. The width matches both
  // endpoints: `LlrCtlosSlots::postInit` produces a `uint16_t` and
  // `Llr::rxCtlos` consumes one.
  uint16_t initData{0};
};

} // namespace packsim
