/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// @oss-disable
@thrift.AllowLegacyMissingUris // @oss-enable
package; // @oss-enable

namespace cpp2 packsim.thrift
namespace py3 packsim

include "packsim/if/roce_v2_config.thrift"
// @oss-disable
// @oss-disable
// @oss-disable
include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

// Unsigned integer types. Each keeps the signed wire type of its width and
// generates the unsigned C++ type of that width, so a field changed to one
// still reads data written before the change.
@cpp.Type{name = "::std::uint8_t"}
typedef byte uint8
@cpp.Type{name = "::std::uint16_t"}
typedef i16 uint16
@cpp.Type{name = "::std::uint32_t"}
typedef i32 uint32
@cpp.Type{name = "::std::uint64_t"}
typedef i64 uint64

// @oss-disable
// @oss-disable
// @oss-disable
// @oss-disable
// @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
// @oss-disable

// Ids 1, 2, 3, 4, 6 and 8 held arms that were removed; use `roceV2`. The
// union stays a union because it is the plug-point new transports are added
// at.
// Ids reserved below are never reused for a different type. // @oss-enable
// @oss-disable
@thrift.ReserveIds{ids = [1, 2, 3, 4, 6, 7, 8, 9, 10, 12]} // @oss-enable
union TransportConfig {
  // @oss-disable
  // @oss-disable
  // @oss-disable
  11: roce_v2_config.RoceV2TransportConfig roceV2;
  // @oss-disable
}
