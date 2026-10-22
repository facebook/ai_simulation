/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/device-id.h"

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <folly/Conv.h>
#include <folly/lang/Bits.h>

namespace packsim {

FlexDeviceId::FlexDeviceId(const folly::IPAddressV6& ipv6) {
  const auto ba = ipv6.toByteArray();
  for (size_t i = 0; i < kNumFlexDeviceIdFields; ++i) {
    values[i] = folly::Endian::big(folly::loadUnaligned<uint16_t>(&ba[2 * i]));
  }
  isInitialized_ = true;
}

FlexDeviceId::FlexDeviceId(const std::vector<uint32_t>& vals) {
  if (vals.size() > kNumFlexDeviceIdFields) {
    throw std::runtime_error("Too many values for device ID vector");
  }
  for (size_t i = 0; i < vals.size(); ++i) {
    values[i] = folly::to<uint16_t>(vals[i]);
  }
  if (!vals.empty()) {
    isInitialized_ = true;
  }
}

FlexDeviceId::FlexDeviceId(char const* str)
    : FlexDeviceId(folly::IPAddressV6(str)) {}

folly::IPAddressV6 FlexDeviceId::toAddress() const {
  return folly::IPAddressV6(serialize());
}

std::vector<uint32_t> FlexDeviceId::asVec() const {
  return std::vector<uint32_t>(values.begin(), values.end());
}

folly::ByteArray16 FlexDeviceId::serialize() const {
  folly::ByteArray16 ba{{0}};
  for (size_t i = 0; i < values.size(); ++i) {
    folly::storeUnaligned<uint16_t>(&ba[2 * i], folly::Endian::big(values[i]));
  }
  return ba;
}

std::string FlexDeviceId::toString() const {
  return toAddress().str();
}
} // namespace packsim
