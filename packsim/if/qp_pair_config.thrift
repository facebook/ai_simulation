/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// @oss-disable
include "thrift/annotation/thrift.thrift" // @oss-enable
@thrift.AllowLegacyMissingUris // @oss-enable
package; // @oss-enable

namespace cpp2 packsim.thrift
namespace py3 packsim

include "thrift/annotation/cpp.thrift"

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

/**
 * Pair of ranks representing a communication between srcRank and dstRank.
 */
struct RankPair {
  // Source rank in the collective
  1: uint32 srcRank;
  // Destination rank in the collective
  2: uint32 dstRank;
}

/**
 * Pair of NIC indices representing a communication path between srcNicIdx and dstNicIdx.
 */
struct NicPair {
  // Source NIC index on the source rank
  1: uint32 srcNicIdx;
  // Destination NIC index on the destination rank
  2: uint32 dstNicIdx;
}

/**
 * Port configuration for a specific QPN.
 * Stores which ports on the source and destination NICs can be used for this QPN.
 * RoCEv2 ignores it on an endpoint whose ports are indexed
 * (EndpointConfig.portIdx > 0): the transport picks the egress port from the
 * device pair (RoceTransportBase::egressPortSlot), and the endpoint sends the
 * packet out of that port without consulting this config. Without a port
 * index, the endpoint routes by device address through its LPM table and never
 * reads this config either.
 */
struct QpnPortConfig {
  // List of port indices on the source NIC that can be used
  1: list<uint16> srcPorts;
  // List of port indices on the destination NIC that can be used
  2: list<uint16> dstPorts;
}

/**
 * Represents a queue pair connectivity template between two endpoints.
 * Defines the NIC-to-NIC mapping and the ports available for communication
 * (RoCEv2 ignores the ports; see QpnPortConfig).
 * This is a connectivity specification, not an instantiated QP with assigned QPNs.
 *
 * A template gives its rank pair, and the reverse pair, one QP each on its NIC
 * pair. Notifies take the last QP of a rank pair's first NIC pair, in
 * (srcNicIdx, dstNicIdx) order, and need it to themselves, so that NIC pair
 * needs two templates between the two ranks: for example one in each direction,
 * as examples/incast-2host.json has. Building the simulation throws otherwise.
 */
struct QueuePairTemplate {
  // Source and destination ranks
  1: RankPair ranks;
  // Source and destination NIC indices
  2: NicPair nics;
  // Port configuration (which ports on src/dst NICs can be used). RoCEv2
  // ignores it; see QpnPortConfig.
  3: QpnPortConfig ports;
}
