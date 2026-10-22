/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/packet/roce.h"

namespace packsim {

void RocePacket::setOpcode(const bool isFirstPkt, const bool isLastPkt) {
  if (isFirstPkt && isLastPkt) {
    opcode = BthOpcode::SEND_FIRST_AND_LAST;
  } else if (isFirstPkt) {
    opcode = BthOpcode::SEND_FIRST;
  } else if (isLastPkt) {
    opcode = BthOpcode::SEND_LAST;
  } else {
    opcode = BthOpcode::SEND_MIDDLE;
  }
}

bool RocePacket::isLastPkt() const {
  return (opcode == BthOpcode::SEND_FIRST_AND_LAST) ||
      (opcode == BthOpcode::SEND_LAST) || (opcode == BthOpcode::CNP);
}

} // namespace packsim
