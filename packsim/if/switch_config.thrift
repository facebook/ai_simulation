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

cpp_include "folly/container/F14Map.h"

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

// Per-priority-group (PG) ingress buffer configuration for a switch port,
// read for ports with isIngrPort = true in SwitchConfig.bufs when
// SwitchConfig.useIngress is true, which is also when the CBFC credit below
// reads it.
//
// Admission: a packet takes cells from, in order, the PG's rsvdCells, the
// port's portRsvdCells (only if canUsePortRsvd), the shared pool, and the PG's
// headroom (only if lossless). One packet can span several tiers. A packet
// whose cells do not all fit is dropped, lossless or not; see
// SwitchConfig.enableFatalOnDrop. rsvdCells, maxSharedCells and maxHdrmCells
// default to 0 and canUsePortRsvd to false, so a PG left at the defaults
// admits nothing: its first packet is dropped and, with enableFatalOnDrop at
// its default of true, the run throws. Raising maxSharedCells or maxHdrmCells
// alone is not enough while, respectively, the port's portMaxSharedCells or the
// buffer's headroomPoolCells is still at its default of 0.
//
// PFC (Priority Flow Control): a PG with pfc = true sends an XOFF pause frame
// upstream as soon as it holds a headroom cell, and keeps refreshing the pause
// until it sends XON or its link goes down. The sender's queue scheduler skips
// the paused queue meanwhile. The PG sends XON only once its headroom is empty
// and the xonHystCells or resumeOffsetCells condition below holds; with neither
// set it never resumes. The resume condition is checked only when this PG
// admits or releases a packet, so a PG whose queue empties while the condition
// fails stays paused.
struct PriorityGroupBufferConfig {
  // Not read by the simulator: a PG's index is its position in
  // `PortBufferConfig.pgs`.
  // @oss-disable
  1: uint32 priority;

  // Guaranteed reserved cells for this PG. Always available regardless of
  // shared pool pressure.
  10: uint32 rsvdCells = 0;

  // Whether this PG can borrow from the port-level reserved pool
  // (PortBufferConfig.portRsvdCells) when its own rsvdCells are exhausted.
  11: bool canUsePortRsvd = false;

  // Dynamic threshold sharing of the shared pool. When true, a packet may take
  // shared cells only while the PG's shared cells, the packet's included, stay
  // within floor(alpha * F), where F is the buffer's free shared cells before
  // the packet, so the allowance shrinks as the pool fills. When false it can
  // take any free shared cell. maxSharedCells and the port's
  // portMaxSharedCells cap it either way.
  20: bool dynamicSharing = false;

  // Dynamic sharing factor: a higher alpha lets this PG hold more of the
  // shared pool. Admission reads it only when dynamicSharing is true; the
  // resumeOffsetCells resume check reads it either way.
  21: double alpha = 1;

  // Most shared cells this PG can hold at once. 0, the default, gives the PG
  // no shared cells; set 2147483647 for no practical cap. The port's
  // portMaxSharedCells also caps the PG and also defaults to 0.
  22: uint32 maxSharedCells = 0;

  // PFC resume threshold, hysteresis form. Let F be the buffer's free shared
  // cells, and S the shared cells this PG could still take, ignoring its
  // maxSharedCells and portMaxSharedCells caps: floor(alpha * F) minus the
  // PG's shared cells in use (never below 0) when dynamicSharing is true, or F
  // when it is false. A paused PG resumes when its headroom is empty,
  // S >= xonHystCells and S > 0, so xonHystCells = 0 resumes once S is at
  // least 1. Checked only when this PG admits or releases a packet. Setting
  // both this and resumeOffsetCells makes building the switch throw
  // std::runtime_error.
  23: optional uint32 xonHystCells;

  // PFC resume threshold, offset form. Let W be the PG's shared cells in use
  // at the moment it paused, and F the buffer's free shared cells. A paused PG
  // resumes when its headroom is empty, its shared cells in use are at most
  // W - resumeOffsetCells (0 if that is negative), and floor(alpha * F)
  // exceeds its shared cells in use; alpha applies here even when
  // dynamicSharing is false. Checked only when this PG admits or releases a
  // packet. Setting both this and xonHystCells makes building the switch
  // throw std::runtime_error.
  24: optional uint32 resumeOffsetCells;

  // Most headroom cells this PG can hold, drawn from the buffer's
  // headroomPoolCells. Only a lossless PG uses headroom. Headroom absorbs what
  // arrives after the PG pauses its sender, so size it for
  // (link_rate * RTT) / cellSizeBytes. Once headroom is exhausted, a packet
  // that does not fit is dropped; see SwitchConfig.enableFatalOnDrop. On a
  // port with CBFC enabled, the credit the port grants its sender is a sum
  // over the buffers in SwitchConfig.bufs that list the port as an ingress
  // port: for each, the sum of maxHdrmCells over the port's lossless PGs,
  // capped at that buffer's headroomPoolCells (the cap binds the sum, not each
  // PG). The total is capped at 2^15 - 1 credits, with a warning. A switch
  // without SwitchConfig.useIngress grants no credit, so a CBFC link into it
  // fails to build.
  30: uint32 maxHdrmCells = 0;

  // Whether this PG can use headroom, after its reserved and shared cells. On
  // a port with CBFC enabled, only packets CBFC treats as lossless reach it.
  // A PG with lossless = false never uses headroom, so it never pauses its
  // sender. Neither setting prevents drops: a packet that does not fit in the
  // tiers the PG can use is dropped.
  40: bool lossless = true;

  // Whether this PG sends PFC frames upstream: XOFF once it holds a headroom
  // cell, XON when the resume condition above holds. Only a lossless PG uses
  // headroom, so pfc has no effect when lossless is false.
  // On a CBFC link, a pause frame still pauses the queues it // @oss-enable
  // names, on top of CBFC's credits. // @oss-enable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  41: bool pfc = true;
}

// Per-queue egress buffer configuration, read for ports with isEgrPort = true
// in SwitchConfig.egressBufs when SwitchConfig.useEgress is true. Admission
// follows the PG order with fixed choices: the queue can always borrow its
// port's portRsvdCells, always reaches the egress buffer's headroomPoolCells
// with no per-queue cap, and never sends PFC. As for a PG, maxSharedCells = 0
// (the default) gives the queue no shared cells.
struct EgressQueueBufferConfig {
  // Not read by the simulator: a queue's index is its position in
  // `PortBufferConfig.ucQueues`.
  1: uint32 cos;
  // queue min
  10: uint32 rsvdCells = 0;
  20: bool dynamicSharing = true;
  21: double alpha = 1;
  22: uint32 maxSharedCells = 0;
  // Not read by the simulator.
  23: uint32 resumeHystCells = 0;
}

// Per-port buffer configuration on a switch. Each port contains one or more
// priority groups (PGs) for ingress and optionally egress queue configs.
struct PortBufferConfig {
  // Port number on the switch.
  1: uint32 port;

  // Port-level reserved cells. Shared across all PGs on this port when their
  // individual rsvdCells are exhausted (requires canUsePortRsvd=true on the PG).
  10: uint32 portRsvdCells = 0;

  // Most shared cells this port's PGs (egress queues, in an egress buffer) can
  // hold together. 0, the default, gives the port no shared cells whatever
  // each PG's maxSharedCells says; set 2147483647 for no practical cap.
  11: uint32 portMaxSharedCells = 0;

  // Not read by the simulator: PFC resume is decided per PG (see
  // `PriorityGroupBufferConfig`).
  12: uint32 xonHystCells = 0;

  // Priority group configurations for this port's ingress buffer.
  // Each PG defines buffer thresholds and PFC behavior for a traffic class.
  20: list<PriorityGroupBufferConfig> pgs;

  // Queue-to-config mapping per port
  30: list<PortQueueConfig> queueCfg;
  // Queue scheduling algorithm (RR, WRR, SP)
  40: SchedulingAlgorithmType queueScheduleCfg;
  // Maps packet DSCP values to egress queues
  50: QueueFilterConfig queueFilterCfg;

  // Egress unicast queue buffer configs (egress direction only).
  60: list<EgressQueueBufferConfig> ucQueues;
  // Whether this port config applies to ingress
  61: bool isIngrPort = true;
  // Whether this port config applies to egress
  62: bool isEgrPort = false;
}

// Packet field the queue filter matches to choose an egress queue.
//
// Id 2 held DSF, which never had a working filter: it ignored the packet,
// always returned queue 0, and discarded both fieldToQueue and fieldToColor.
// Reserved rather than reusable, because a stored config may still carry the
// value.
@thrift.ReserveIds{ids = [2]}
enum QueueFilterField {
  DSCP = 1,
}

@cpp.EnumType{type = cpp.EnumUnderlyingType.U8}
enum PacketColor {
  GREEN = 0,
  YELLOW = 1,
  RED = 2,
}

struct QueueFilterConfig {
  // field type to use
  1: QueueFilterField field = DSCP;
  // Queue index by DSCP value. DSCP is a 6-bit field, so a key must be below
  // 64. A DSCP with no entry selects queue 0.
  2: FieldToQueueMap fieldToQueue;
  // Packet color by DSCP value, with keys below 64. A DSCP with no entry is
  // colored GREEN. A non-empty map makes the port's queues take their WRED/ECN
  // handlers from PortQueueConfig.coloredWredEcnCfgs.
  3: PacketColorByField fieldToColor;
  4: uint32 minQSizeThreshold = 30000;
  5: uint32 maxQSizeThreshold = 60000;
}

// Top-level switch buffer configuration. Defines the total buffer capacity
// and how it is partitioned between headroom and shared pools.
//
// Buffer partitioning:
//   shared pool = totalCells - headroomPoolCells - every port's portRsvdCells
//     - the rsvdCells of every PG on a port with isIngrPort (ingress buffer)
//     or of every egress queue on a port with isEgrPort (egress buffer).
//   The shared pool is computed when the buffer is built and is not stored in
//   _calcSharedBufferCells. Building the buffer fails if the headroom and
//   reserved cells add up to more than totalCells.
//
// Headroom absorbs packets that are in flight on the wire after a PG pauses
// its sender. If headroomPoolCells is too small, packets that do not fit are
// dropped; see SwitchConfig.enableFatalOnDrop.
// Rule of thumb: headroomPoolCells >= (link_rate * RTT) / cellSizeBytes
// summed across all lossless PGs.
struct SwitchBufferConfig {
  // Size of a single buffer cell in bytes, at least 1. All cell-based
  // thresholds (rsvdCells, maxHdrmCells, etc.) are in units of this size.
  9: uint32 cellSizeBytes = 254;

  // Total cells in this buffer, split as described above.
  20: uint32 totalCells;

  // Headroom pool, shared by the lossless PGs in this buffer, each up to its
  // maxHdrmCells; in an egress buffer every egress queue draws on it with no
  // per-queue cap. A PG with pfc = true pauses its sender as soon as it holds
  // a headroom cell, so this pool must absorb the data in flight during the
  // PFC round trip.
  21: uint32 headroomPoolCells = 0;

  30: PortBufferConfigById ports;
  // Intended to hold the remaining shared buffer after subtracting the
  // reserved and headroom pools, computed at buffer construction. Nothing
  // populates either field today, so both stay 0 on every run; do not read
  // them. Kept because stored configs carry them.
  40: uint32 _calcSharedBufferCells;
  50: uint32 _calcSharedBufferCellsEgr;
}

// Which buffer a port's packets use, as an index into SwitchConfig.bufs
// and SwitchConfig.egressBufs. A port also takes its queueCfg, queueFilterCfg
// and queueScheduleCfg from its PortBufferConfig in one buffer: the singleMap
// entry's buffer in bufs, or in egressBufs on an egress-only switch. On a
// switch that uses both directions, a port with no singleMap entry takes them
// from bufs at the lowest index a pairMap key with the port on its ingress
// side selects.
struct PortToBufMap {
  // Buffer by ingress port when only SwitchConfig.useIngress is set, by egress
  // port when only SwitchConfig.useEgress is set.
  1: map<uint32, uint8> singleMap;
  // Buffer by "ingrPort.egrPort" (e.g., "0.5" for ingress port 0, egress port
  // 5) when both are set; that index selects the buffer in both lists.
  2: map<string, uint8> pairMap;
}

enum QueueProfileType {
  // No ECN or WRED enabled
  NONE = 0,
  // Average queue length based ECN marking
  ECN = 1,
  // Average queue length based WRED drop
  WRED = 2,
  // ECN marking by the length of the queue a packet leaves, read once the
  // packet is dequeued: on a switch port, the virtual output queue (VOQ) for
  // {egress port, queue}. Only switch ports mark; on a NIC queue this profile
  // marks nothing.
  VOQ_ECN = 3,
  // Per packet, by its ECN field: a Not-ECT packet follows the WRED drop curve
  // on enqueue and is never ECN-marked; any other packet is admitted by this
  // profile and follows the ECN mark curve on dequeue.
  WRED_ECN = 4,
}

struct WredEcnConfig {
  1: QueueProfileType queueProfileCfg = NONE;
  // min Threshold in bytes for WRED
  2: uint32 minQSizeWRED = 5000;
  // max Threshold in bytes for WRED
  3: uint32 maxQSizeWRED = 200000;
  // drop probability, in percent from 0 to 100 (e.g., 5 for 5%, not 0.05),
  // that the drop curve approaches: from 0 at a queue length of minQSizeWRED,
  // the probability rises linearly towards this value as the length nears
  // maxQSizeWRED, and is 100% from maxQSizeWRED up; values above 100 are
  // rejected when the queue is built
  4: uint32 dropRateWRED = 100;
  // exponent n of the averaging weight 2^-n given to the current queue length
  // at each update (0 sets the average to the current queue length); fractional
  // values are used as given, and negative or non-finite values are rejected
  // when the queue is built
  5: double exp = 0.0;
  // period between updates of the average queue length, in nanoseconds with
  // the fractional part discarded; each queue's update times are shifted by a
  // random offset in [0, averagingPeriod], and 0 makes the average follow the
  // instantaneous queue length; negative, non-finite and 2^64 or larger values
  // are rejected when the queue is built
  6: double averagingPeriod = 7.0;
  // min Threshold in bytes for ECN
  7: uint32 minQSizeECN = 5000;
  // max Threshold in bytes for ECN
  8: uint32 maxQSizeECN = 200000;
  // mark probability, in percent from 0 to 100 (e.g., 5 for 5%, not 0.05),
  // that the mark curve approaches: from 0 at a queue length of minQSizeECN,
  // the probability rises linearly towards this value as the length nears
  // maxQSizeECN, and is 100% from maxQSizeECN up; values above 100 are
  // rejected when the queue is built
  9: uint32 markRateECN = 100;
}

// Scheduling algorithm across a port's queues.
//
// Id 2 held deficit weighted round-robin, which was never implemented.
// Reserved rather than reusable, because a stored config may still carry the
// value; such a config fails when the port's queues are built.
@thrift.ReserveIds{ids = [2]}
enum SchedulingAlgorithmType {
  // Round-Robin scheduling across queues
  RR = 0,
  // Weighted round-robin with weights fixed by queue index: 1, 9, 10, 20, 30,
  // 40, 50, 100 for queues 0-7 (a port with fewer queues takes the leading
  // entries). The weights are not configurable. When every queue is eligible
  // (backlogged, not paused by PFC and, with CBFC on, not waiting for credit
  // for its head packet), queue i is served weight times per round, so queues
  // 0 and 1 split a link 1:9.
  WRR = 1,
  // Strict priority: the highest-numbered eligible queue is served first.
  SP = 3,
}

@thrift.ReserveIds{ids = [2]}
struct PortQueueConfig {
  // Builds the queue's WRED/ECN handler when the port's
  // QueueFilterConfig.fieldToColor is empty.
  1: WredEcnConfig wredEcnCfg;
  // Builds one WRED/ECN handler per color, in place of wredEcnCfg's, when the
  // port's QueueFilterConfig.fieldToColor is non-empty. Needs an entry for
  // every color the filter can assign, including GREEN, which unmapped DSCP
  // values and non-IPv6 packets get; a packet whose color has no entry throws
  // std::invalid_argument when it is queued.
  3: map<PacketColor, WredEcnConfig> coloredWredEcnCfgs;
}

struct SwitchParams {
  1: uint8 numBuffs;
  2: uint32 numUplinks;
  3: uint32 numDownlinks;

  6: uint32 cellSizeBytes;
  7: uint64 bufSizeBytes;
  8: uint64 rsvdBytesPerPort;
  9: uint64 xonHystBytes;
  10: uint64 hdrmBytesPerPortUl;
  11: uint64 hdrmBytesPerPortDl;
  12: uint64 hdrmPoolPerBuff;

  15: bool dynShareEn;
  16: double alpha;

  17: QueueProfileType queueProfileType;
  18: uint8 numQueues;
  19: uint32 minQSizeWRED;
  20: uint32 maxQSizeWRED;
  // a percentage despite the name, from 0 to 100 (e.g., 5 for 5%, not 0.05);
  // switchParamsToConfig copies it into WredEcnConfig.dropRateWRED and rejects
  // a value that is not a whole number in that range
  21: double dropProbWRED;
  22: uint32 minQSizeECN;
  23: uint32 maxQSizeECN;
  // a percentage despite the name, from 0 to 100 (e.g., 5 for 5%, not 0.05);
  // switchParamsToConfig copies it into WredEcnConfig.markRateECN and rejects
  // a value that is not a whole number in that range
  24: double markProbECN;
  // in nanoseconds despite the name; copied unchanged into
  // WredEcnConfig.averagingPeriod
  25: double avgRefreshPeriodUs;
  // copied unchanged into WredEcnConfig.exp
  26: double exponentialfactorQAvg;
  27: SchedulingAlgorithmType queueSchedulerType;

  30: bool isWedge100 = false;
  // switch pipeline delay(ns)
  31: uint32 pipelineDelayNs;
  32: uint32 expDelayNs;
  33: uint64 rsvdBytesPerPortPg;
  34: bool egrDynShareEn;
  35: double egrAlpha;
  36: uint64 rsvdBytesPerEgrQueue;
  // Intended as the offset between an egress queue's TD threshold (its most
  // shared cells) and its resume threshold. It only sets
  // EgressQueueBufferConfig.resumeHystCells, which the simulator does not
  // read.
  37: uint64 egrResumeHystBytes;
  40: list<bool> isLosslessPerPg;

  // Not read by the simulator or by `switchParamsToConfig`, which sets each
  // PG's `pfc` and `lossless` both from `isLosslessPerPg`. Intended as
  // per-priority-group PFC pause-frame generation, indexed like
  // `isLosslessPerPg`, with empty -- the default -- mirroring it. Separate
  // from `isLosslessPerPg` because a CBFC link may want a lossless PG, which
  // is what earns it headroom, with pause generation off.
  // @oss-disable
  // @oss-disable
  // @oss-disable
  //
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  41: list<bool> isPfcGenEnabledPerPg;
}

struct SwitchConfig {
  // Ingress buffers, modeled when useIngress is set; then at least one is
  // required.
  1: list<SwitchBufferConfig> bufs;
  // Egress buffers, modeled when useEgress is set; then at least one is
  // required, and as many as bufs when useIngress is set too.
  3: list<SwitchBufferConfig> egressBufs;
  2: map<string, list<uint32>> portGroups;
  10: uint32 pipelineDelayNs = 1250;
  11: uint32 expDelayNs = 0;
  20: PortToBufMap portToBufMap;
  21: bool useIngress = true;
  22: bool useEgress = false;
  // Not read by the simulator.
  23: bool egressAdmissionCheckEnable = false;
  // When true, a packet the ingress or egress buffer cannot admit throws
  // std::runtime_error and ends the run. When false the drop is recorded as
  // BUFFER_FULL and the run continues. Drops at the output port queue after
  // admission never throw; these include WRED drops, which results also
  // report as BUFFER_FULL.
  24: bool enableFatalOnDrop = true;
}

@cpp.Type{template = "::folly::F14FastMap"}
typedef map<uint32, PacketColor> PacketColorByField
@cpp.Type{template = "::folly::F14FastMap"}
typedef map<uint8, uint8> FieldToQueueMap
@cpp.Type{template = "::folly::F14FastMap"}
typedef map<uint32, PortBufferConfig> PortBufferConfigById
