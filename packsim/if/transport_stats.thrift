/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// @oss-disable
include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

// @oss-disable
@thrift.AllowLegacyMissingUris // @oss-enable
package; // @oss-enable

namespace cpp2 packsim.thrift
namespace py3 packsim

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

struct RoceV2TransportStats {
  // Total number of retransmitted packets.
  1: uint64 retxPkts = 0;
  // Total number of retransmitted payload bytes.
  2: uint64 retxBytes = 0;
  // Retransmitted packets triggered by the retransmit timeout.
  3: uint64 retxByTimer = 0;
  // Retransmitted packets triggered by a NACK.
  4: uint64 retxByNack = 0;
}

// Ids 1 and 2 held arms that were removed.
// Ids reserved below are never reused for a different type. // @oss-enable
// @oss-disable
@thrift.ReserveIds{ids = [1, 2, 3, 4]} // @oss-enable
union TransportStats {
  // @oss-disable
  // @oss-disable
  5: RoceV2TransportStats roceV2;
}

@thrift.ReserveIds{ids = [14]}
struct IntfPktStats {
  1: i32 sentDataPkts = 0;
  2: i64 sentDataBytes = 0;
  9: i32 recvdDataPkts = 0;
  10: i64 recvdDataBytes = 0;

  3: i32 sentAckPkts = 0;
  11: i32 recvdAckPkts = 0;

  13: i32 sentNackPkts = 0;
  15: i32 recvdNackPkts = 0;

  4: i32 sentCnpPkts = 0;
  12: i32 recvdCnpPkts = 0;

  // Packets that left this NIC already carrying the ECN-CE (congestion
  // experienced) mark, set by the NIC's own egress queue; marks a switch sets
  // further along are not visible to the sender. Per CoS, every such packet
  // counts under the CoS of its type (data, ACK/NACK or CNP). Per destination,
  // it counts only while its socket is still registered: a packet that leaves
  // after an iterative workload has retired its socket counts per CoS only.
  // Counted by the RoCEv2 transport; other transports may leave it 0.
  16: i32 sentEcnPkts = 0;
  // Every arriving packet carrying the ECN-CE mark, whether the receiver then
  // accepts it or not (a duplicate or out-of-order drop still counts), so it
  // can exceed recvdDataPkts. Per CoS, a packet counts under the receiver's
  // own CoS for its type (data, ACK/NACK or CNP). Counted by the RoCEv2
  // transport; other transports may leave it 0.
  17: i32 recvdEcnPkts = 0;

  5: i32 droppedAckPkts = 0;
  6: i32 droppedDataPkts = 0;
  7: i64 droppedDataBytes = 0;
  8: i32 droppedCnpPkts = 0;
}

// Entry pairing a destination FlexDeviceId with its per-interface packet stats.
struct IntfPktStatsEntry {
  1: list<uint32> destId;
  2: IntfPktStats intfPktStats;
}
