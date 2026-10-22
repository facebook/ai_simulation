/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "packsim/if/switch_config.thrift"
include "packsim/if/transport_config.thrift"
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

struct DcqcnConfig {
  //NP related
  // Cos value for CNP packets
  1: uint8 npCnpPrio = 1;

  //RP related

  //Alpha related
  2: double rpDceTcpG = 1019;
  // in us
  3: double rpDceTcpRtt = 1;
  4: double rpInitialAlpha = 1023;

  //Rate Decrease
  // Not read by the simulator.
  5: i32 rpMinDecFac = 50;
  // In Mbps, from 0.000001 (1 bps) up to the NIC speed: a CNP never cuts the
  // rate below it.
  6: double rpMinRate = 1.5;
  7: i32 rpGd = 11;
  // in us
  8: uint32 rpRateReduceMonitorPeriod = 4;

  //Rate Increase
  9: bool rpClampTgtRate = false;
  // Not read by the simulator.
  10: bool rpClampTgtRateAti = true;
  // in us
  11: double rpTimerReset = 300;
  // In units of 64 bytes. Not read by the simulator.
  12: double rpByteReset = 32767;
  // valid values 1 to 31
  13: uint32 rpThreshold = 1;
  // In Mbps. Active increase raises the target rate by this much per timer
  // period, up to the NIC speed.
  14: double rpAiRate = 1;
  // In Mbps. Not read by the simulator.
  15: double rpHaiRate = 50;

  //other related
  // With the `dcqcn` arm of RoceV2CongestionControlConfig, true starts a QP's
  // alpha-update and rate-increase timers when the QP is created. They stop
  // when an ACK or a receive completion leaves the QP with no send or receive
  // outstanding, and later work does not restart them; a CNP that cuts the
  // rate (the QP's first CNP, or one arriving at least
  // rpRateReduceMonitorPeriod after the previous cut) restarts the
  // rate-increase timer. With false neither starts: the rate-increase timer
  // starts at the QP's first CNP, and the alpha-update timer never runs, so
  // alpha keeps its initial value.
  16: bool dcqcnEnable = false;
  // Not read by the simulator.
  17: bool byteCntEnable = false;
  // Burst size in Bytes
  20: uint32 rateLimiterBucketSize = 65536;
  // Token refresh period in ns
  21: double rateLimiterRefreshPeriodNs = 1000.0;
}

// Drop types for NicConfig.pktDropCfg. Drop injection is not modelled, so none
// of these types drops a packet.
enum NicEgrPktDropType {
  NO_DROP = 0,
  N_BURST_PKT_DROP = 1,
  RANDOM_DATA_PKT_DROP = 2,
  RANDOM_ACK_PKT_DROP = 3,
  RANDOM_CNP_PKT_DROP = 4,
}

// Drop injection is not modelled; see NicConfig.pktDropCfg.
struct NicEgrPktDropConfig {
  1: NicEgrPktDropType dropPktType = NO_DROP;
  2: i32 numBurstDrops = 10;
  // # of pkts between two drop intervals
  3: i32 numBurstDropIntrvl = 1048576;
  // 1 out N chance to drop
  4: i32 randDropRange = 100;
}

// NIC size. No behaviour depends on it; `NicConfig.nicSize` // @oss-enable
// defaults to FULL. Ids reserved below are never reused. // @oss-enable
// @oss-disable
// @oss-disable
// @oss-disable
// @oss-disable
@thrift.ReserveIds{ids = [2]} // @oss-enable
enum NicSize {
  UNKNOWN = 0,
  FULL = 1,
  // @oss-disable
}

// TODO: Split this into the packet handling characteristics and the transport separately
// Stored configs may still carry fields 161-167, so their ids stay reserved.
@thrift.ReserveIds{ids = [161, 162, 163, 164, 165, 166, 167]}
struct NicConfig {
  1: double writeEff = 99.5;
  // Total NIC DMA read bandwidth in Gbps (per-NIC, not per-port).
  // For multi-port NICs, set this to the aggregate memory bandwidth
  // available to all ports, e.g. 400 for a 4x100G NIC.
  2: double readGbps = 100.0;
  3: double readLatencyNs = 500.0;
  4: i64 maxQpDepth = -1;
  // No effect with RoCEv2, which ignores QpnPortConfig
  // (if/qp_pair_config.thrift). Where a QP's QpnPortConfig is used, true drops
  // the port pairs it allows that the fabric's forwarding does not connect.
  5: bool allowReachableRoutesOnly = false;
  // NIC RX buffer PFC configuration (per CoS / traffic class).
  // perCosXoff sets the number of CoS queues (1 to 8). perCosXon must have the
  // same length, and perCosBufferSize at least that many entries.
  // Each NIC port has an RX buffer of its own. When a queue in it reaches its
  // XOFF threshold, the NIC sends PFC pause frames out of that port to the
  // switch it links to, providing endpoint-level backpressure independent of
  // switch PFC.

  // RX buffer size in bytes per CoS queue. The RX buffer does not enforce it
  // and never drops a packet. Without CBFC, PFC XOFF (perCosXoff) is the
  // buffer's only backpressure, so a queue with PFC disabled can grow without
  // limit. On a CBFC link, the sum of the positive entries is the receive
  // space the sender is credited, capped at 2^15 - 1 credits with a warning;
  // it bounds the lossless traffic held across all queues together rather
  // than per queue.
  90: list<uint32> perCosBufferSize;

  // PFC XON threshold in bytes per CoS queue. When RX buffer usage drops
  // to or below this value, a PFC XON (resume) frame is sent upstream.
  // Set to a negative value to disable PFC on that CoS queue.
  100: list<i32> perCosXon;

  // PFC XOFF threshold in bytes per CoS queue. When RX buffer usage reaches
  // or exceeds this value, a PFC XOFF (pause) frame is sent upstream.
  // Must be >= perCosXon for the same queue to avoid oscillation.
  // Set to a negative value to disable PFC on that CoS queue.
  110: list<i32> perCosXoff;
  // per-port (not per-NIC) bandwidth
  6: string speed = "200Gbps";
  // Sender (TX) latency in ns per port
  11: uint32 txLatencyNs = 0;
  // Receiver (RX) latency in ns per port
  12: uint32 rxLatencyNs = 0;
  7: uint32 mtu = 4200;
  // Every topology here leaves this at FULL. // @oss-enable
  // @oss-disable
  // @oss-disable
  170: NicSize nicSize = NicSize.FULL;
  // Not read by the simulator: RoCEv2 requests an ACK on the last packet of
  // each message.
  9: i32 ackCoalescingFactor = 2147483647;
  // select between various queue schedulers
  10: switch_config.SchedulingAlgorithmType queueScheduleCfg;
  // Maps packet fields to queues, on both the NIC's egress port and its receive
  // buffer. Leave fieldToColor empty: the receive buffer has no colored
  // WRED/ECN configs, so with a non-empty map every packet it queues throws
  // std::invalid_argument.
  20: switch_config.QueueFilterConfig queueFilterCfg;
  // queue to config mapping per port
  30: list<switch_config.PortQueueConfig> queueCfg;
  40: DcqcnConfig dcqcnCfg;
  // Drop injection is not modelled, so this config drops no packets. The NIC
  // still drops packets that a WRED queueCfg profile refuses (WRED_ECN refuses
  // only packets that are not ECN-capable, such as RoCEv2's), packets that
  // arrive with a bit error (on a link with a negative log10Ber), and packets
  // on a port whose link is down. Only checkSimCfg reads this field, and only
  // Simulation.nic's copy: it rejects a drop type other than NO_DROP when
  // Simulation.nic sets outofOrderPktSupport. A nicConfigMap entry's value has
  // no effect.
  50: NicEgrPktDropConfig pktDropCfg;
  // Accept packets that arrive out of PSN order, as the `spray` tiebreak
  // produces, and complete a received message once all of its bytes arrive.
  // With goBackN loss recovery on a fabric that can drop packets, this also
  // needs GoBackNLrConfig.selectiveGapRepair; see that field. RDMA WRITE is not
  // supported with it on: a received WRITE packet throws.
  // @oss-disable
  60: bool outofOrderPktSupport = false;
  // Cos value for ACK packets
  120: uint8 ackCos = 0;
  140: transport_config.TransportConfig transport;
  // Interval, in ns, of a byte trace for the ports of every link: each interval
  // in which a port's counts changed adds the bytes it sent and received since
  // the last sample, summed over every class of service and including control
  // frames, to SimulationResult.traces as TXBytes_<port>_Port_<n> and
  // RXBytes_<port>_Port_<n>. Only the value in Simulation.nic is read; a
  // nicConfigMap entry's value has no effect. A value that is not positive
  // leaves the trace off. While on, the trace keeps a stalled run from ending;
  // see --packsim_detect_stall.
  150: i64 netDeviceSnapshotIntervalNs = -1;
  // Not read by the simulator.
  160: bool enableChromeTracing = false;
  // Cos value for DATA packets
  130: uint8 dataCos = 0;
}
