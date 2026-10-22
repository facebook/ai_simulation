/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <folly/IPAddress.h>
#include <folly/hash/Hash.h>

namespace packsim {

/**
 * Flexible device identifier represented as a tuple of unsigned 16-bit
 * integers.
 *
 * Supports construction from IPv6 addresses, integer vectors, and string
 * representations. Provides serialization, comparison, and hashing operations.
 */
class FlexDeviceId {
 public:
  /** Default constructor creates an uninitialized device ID. */
  FlexDeviceId() = default;

  /**
   * Constructs a device ID from an IPv6 address.
   *
   * @param ipv6 The IPv6 address to convert
   */
  explicit FlexDeviceId(const folly::IPAddressV6& ipv6);

  /**
   * Constructs a device ID from a vector of integers.
   *
   * @param vals Integer values to initialize the device ID
   * @throws std::runtime_error if vals has more than kNumFlexDeviceIdFields
   *         values
   * @throws folly::ConversionError if a value exceeds 65535
   */
  explicit FlexDeviceId(const std::vector<uint32_t>& vals);

  /**
   * Constructs a device ID from a string representation.
   *
   * @param str String representation of the device ID
   */
  /* implicit */ FlexDeviceId(char const* str);

  /**
   * Serializes the device ID to a 16-byte array.
   *
   * @return Serialized byte array representation
   */
  folly::ByteArray16 serialize() const;

  /**
   * Converts the device ID to a string representation.
   *
   * @return String representation of the device ID
   */
  std::string toString() const;

  /**
   * Converts the device ID to a vector of integers.
   *
   * @return Vector of integer values
   */
  std::vector<uint32_t> asVec() const;

  bool operator==(const FlexDeviceId& other) const noexcept {
    return values == other.values;
  }

  bool operator!=(const FlexDeviceId& other) const noexcept {
    return values != other.values;
  }

  /**
   * Compares device IDs lexicographically starting from the second field.
   *
   * @param  other The device ID to compare against
   * @return       True if this device ID is greater than other
   */
  bool operator>(const FlexDeviceId& other) const noexcept {
    // compare from the second to last int in the vector
    for (size_t idx = 1; idx < values.size(); ++idx) {
      if (values[idx] > other.values[idx]) {
        return true;
      } else if (values[idx] < other.values[idx]) {
        return false;
      }
    }
    return false; // all fields are equal
  }

  /** Returns whether the device ID has been initialized. */
  bool isInitialized() const noexcept {
    return isInitialized_;
  }

  // Currently, a flex device ID is a tuple of 8 unsigned 16-bit integers
  static constexpr size_t kNumFlexDeviceIdFields = 8;

  // Array of unsigned 16-bit integer fields representing the device ID
  std::array<uint16_t, kNumFlexDeviceIdFields> values{0};

 private:
  /**
   * Converts the device ID to an IPv6 address.
   *
   * @return IPv6 address representation
   */
  folly::IPAddressV6 toAddress() const;

  // Tracks whether this device ID has been initialized with valid data
  bool isInitialized_{false};
};

inline std::ostream& operator<<(
    std::ostream& os,
    const FlexDeviceId& deviceId) {
  os << deviceId.toString();
  return os;
}
} // namespace packsim

/**
 * Hash function specialization for FlexDeviceId.
 *
 * Enables use of FlexDeviceId as a key in hashed containers such as
 * folly::F14FastMap / folly::F14FastSet and std::unordered_map /
 * std::unordered_set.
 */
template <>
struct std::hash<packsim::FlexDeviceId> {
  std::size_t operator()(const packsim::FlexDeviceId& devId) const noexcept {
    return folly::hash::hash_range(devId.values.begin(), devId.values.end());
  }
};
