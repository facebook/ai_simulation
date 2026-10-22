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

const list<uint64> kGoBackNRnrFieldToTimeUs = [
  655360,
  10,
  20,
  30,
  40,
  60,
  80,
  120,
  160,
  240,
  320,
  480,
  640,
  960,
  1280,
  1920,
  2560,
  3840,
  5120,
  7680,
  10240,
  15360,
  20480,
  30720,
  40960,
  61440,
  81920,
  122880,
  163840,
  245760,
  327680,
  491520,
];

struct GoBackNLrConfig {
  // index into the above kGoBackNRnrFieldToTimeUs list
  1: uint8 rnrTimeIdx = 1;
  // How Go-Back-N repairs a PSN gap.
  // false: with NicConfig.outofOrderPktSupport off, the receiver NACKs a gap at
  // once and the sender resends every packet from the missing PSN on. With
  // outofOrderPktSupport on, the receiver accepts every PSN and never NACKs a
  // gap, and the sender treats each ACK as covering every lower PSN: a dropped
  // packet is not retransmitted once the last packet of its message, or of a
  // later one, is acknowledged, so the receiver never completes its message.
  // true: the receiver waits 100 us before NACKing a gap, so brief reordering
  // can resolve first, and the sender resends one packet per NACK. Packets past
  // the gap are kept with outofOrderPktSupport on; with it off they are
  // discarded and repaired one NACK at a time.
  // With outofOrderPktSupport off, or with this true, a retransmit timer also
  // resends the newest unacknowledged message's last packet when ACKs stop
  // arriving, which recovers a lost last packet or ACK.
  // Set this to true when outofOrderPktSupport is on and the fabric can drop
  // packets: a switch buffer that cannot admit one (on a lossy priority group,
  // or on a lossless one whose headroom is used up), a switch or NIC egress
  // queue whose WRED or WRED_ECN profile drops one (RoCEv2 sends packets that
  // are not ECN-capable, and WRED_ECN drops rather than marks those unless an
  // earlier ECN or VOQ_ECN queue has marked them), a switch with no route for
  // one, or a link that is down or has a negative log10Ber without LLR. While
  // SwitchConfig.enableFatalOnDrop is true, its default, a switch buffer drop
  // throws and ends the run instead.
  2: bool selectiveGapRepair = false;
}

struct DcqcnCcConfig {}

struct DummyRoceV2StrategyConfig {}

// Select `dummy` to disable congestion control; an unset union is invalid.
// The `dcqcn` arm selects DCQCN; its parameters come from NicConfig.dcqcnCfg.
union RoceV2CongestionControlConfig {
  1: DummyRoceV2StrategyConfig dummy;
  2: DcqcnCcConfig dcqcn;
}

// Select `dummy` to disable loss recovery; an unset union is invalid.
union RoceV2LossRecoveryConfig {
  1: DummyRoceV2StrategyConfig dummy;
  2: GoBackNLrConfig goBackN;
}

// Selects congestion control and loss recovery independently; for example,
// `dcqcn` can be combined with `goBackN`.
struct RoceV2TransportConfig {
  1: RoceV2CongestionControlConfig congestionControl;
  2: RoceV2LossRecoveryConfig lossRecovery;
}
