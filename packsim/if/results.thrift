/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace cpp2 packsim.thrift
namespace py3 packsim

include "packsim/if/config.thrift"
// @oss-disable
// @oss-disable
// @oss-disable
include "packsim/if/workload.thrift"
include "packsim/if/switches.thrift"
// @oss-disable
include "packsim/if/transport_stats.thrift"
include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

@thrift.AllowLegacyMissingUris
package;

cpp_include "folly/container/F14Map.h"

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

typedef list<uint32> FlexDeviceId

struct DefaultRes {}

@thrift.ReserveIds{ids = [2]} // @oss-enable
union WorkloadSpecificResult {
  // nothing, for backward compatibility
  1: DefaultRes defaultRes;
  // @oss-disable
}

struct PerRankWorkloadResult {
  1: uint64 startTimeNs = 0;
  2: uint64 endTimeNs = 0;
  3: uint32 workloadId = 0;
  4: uint32 rank = 0;
  5: string name = "";
  6: uint32 iteration = 0;
}

// Per-message result record. Each record captures one message's send/recv
// lifecycle. Only populated when config.enablePerMessageTracking = true.
struct PerMessageResult {
  // Define fields constituting unique keys
  1: uint64 workloadId = 0;
  2: uint64 srcRank = 0;
  3: uint64 dstRank = 0;
  4: uint64 messageIndex = 0;
  5: uint64 iteration = 0;

  // Record send and recv times
  51: uint64 sendTimeNs = 0;
  // Always set: a send with no matching receive is not emitted.
  52: i64 recvTimeNs = 0;

  // Define fields to track metadata of messages
  101: uint64 sizeBytes = 0;
}

// Workloads
struct WorkloadResult {
  1: string name = "";
  3: uint32 workloadId = 0;
  4: workload.Workload workCfg;
  10: uint64 startTimeNs = 0;
  11: uint64 endTimeNs = 0;
  12: uint64 runTimeNs = 0;
  13: map<uint32, PerRankWorkloadResult> perRank;
  // Not populated by the simulator: always empty. Each rank's device id is in
  // `workCfg.rankToId`.
  20: RankToHostMap rankToHost;
  // A workload type's own result, if it reports one; otherwise `defaultRes`.
  24: WorkloadSpecificResult specificResult = {"defaultRes": DefaultRes{}};
  // Map child workloadId to child workload result. Not populated by the
  // simulator: always empty. A batch is reported only as a whole, under its
  // own name.
  31: map<i32, WorkloadResult> childrenResults;
  // Not populated by the simulator: every field keeps its default.
  41: BatchWorkloadResult batchWorkloadResult;
  // Original workload name from input config.
  50: string inputName = "";
  51: uint32 iteration = 0;

  // Per-message result records. Only populated when
  // config.enablePerMessageTracking = true.
  60: list<PerMessageResult> perMessageResults;
}

// Use for repeated collectives results
struct BatchWorkloadResult {
  1: i64 maxCollectiveRuntimeNs = 0;
  2: i64 minCollectiveRuntimeNs = 0;
  // max collective runtime - min collective runtime
  3: double collectivesUncertainty = 0.0;
  // (max collective runtime - min collective runtime) / min collective runtime
  4: double collectivesRelativeUncertainty = 0.0;
  // first collective runtime / rest collectives avg runtime
  5: double firstToSubsequentRunsRatio = 0.0;
  6: list<i64> collectivesRuntimesNs;
}

@thrift.ReserveIds{ids = [11]} // @oss-enable
struct NicStats {
  1: FlexDeviceId flexId;
  5: list<transport_stats.IntfPktStats> perCosStats;
  6: list<transport_stats.IntfPktStatsEntry> perDestStats;
  10: transport_stats.TransportStats transport;
  // @oss-disable
  // @oss-disable
}

struct MacStats {
  1: list<switches.InterfaceStats> cosStats;
}

// @oss-disable
// @oss-disable
// @oss-disable
// @oss-disable
@thrift.ReserveIds{ids = [0, 1, 2]} // @oss-enable
enum WiringType {
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
}

// Data bytes placed on the wire for one (srcNicPort, dstNicPort) pair on a NIC,
// accumulated at TX-complete. Both ports are 0-based. Only pairs with nonzero
// bytes are emitted (sparse).
struct PortPairBytes {
  1: uint16 srcNicPort = 0;
  2: uint16 dstNicPort = 0;
  3: uint64 bytes = 0;
}

// The (srcNicPort, dstNicPort) byte pairs sharing one wiring type, so in-rack
// (planar) and cross-rack (meshed) traffic can be separated downstream.
struct WiringPortPairBytes {
  1: WiringType wiringType;
  2: list<PortPairBytes> entries;
}

// End-of-sim stats for a multi-port NIC (>1 egress port). Emitted once per NIC
// (not per snapshot) and only for multi-port NICs -- an extensible home for
// future per-port / per-plane metrics.
struct MultiPortNicStats {
  // Cross-rack port-pair load-balance heatmap, grouped by wiring type. Counts
  // every data-bearing transmit, so retransmitted bytes are included.
  1: list<WiringPortPairBytes> portPairBytesByWiring;
}

struct NicSummary {
  1: FlexDeviceId flexId;
  2: string name = "";
  3: NicStats nicStats;
  // Per-COS MAC stats per port; the inner list is per COS. The outer list has
  // one entry per port of this endpoint, in ascending port number: the 0-based
  // NIC egress port (the port field of the port's address minus 1, as in
  // PortPairBytes) when the endpoint has a port field (EndpointConfig.portIdx
  // is not 0), otherwise the port's index on its node. With ports numbered
  // 0..n-1, entry k is port k.
  4: list<MacStats> macStats;
}

// Simulation
struct Snapshot {
  1: uint64 timeNs = 0;
  3: list<switches.Switch> switchStats;
  8: list<NicSummary> nicStats;
}

// Information about a QP that was a "straggler" (slow to complete)
struct QpStragglerInfo {
  // Source FlexDeviceId
  1: list<uint32> srcId;
  // Destination FlexDeviceId
  2: list<uint32> dstId;
  // Source Queue Pair Number
  3: uint32 srcQPN = 0;
  // Destination Queue Pair Number
  4: uint32 dstQPN = 0;
  // Completion time (simulation nanoseconds)
  5: uint64 completionTimeNs = 0;
  // Format: "src-{srcId}_dst-{dstId}_QP-{srcQPN}"
  6: string qpIdentifier = "";
}

struct SimulationStats {
  1: uint64 desEvents = 0;
  // Not populated by the simulator: always 0.
  2: uint64 workloadEvents = 0;
  3: uint64 totalBytesDelivered = 0;
  4: uint64 endTimeNs = 0;
  5: uint64 maxRssBytes = 0;
  // Last QP to complete (the straggler). Set only by a transport that
  // overrides Transport::getLastStraggler(); RoCEv2 does not, so a RoCEv2 run
  // leaves it unset.
  6: optional QpStragglerInfo lastStraggler;
}

struct RequestStats {
  7: i64 totalReqs = 0;
  8: i64 totalBytes = 0;
}

struct RealWorldTiming {
  1: double start = 0.0;
  2: double end = 0.0;
  3: double runtime = 0.0;
}

struct TraceSample {
  1: uint64 timeNs = 0;
  2: double value = 0.0;
}

// @oss-disable
@thrift.ReserveIds{ids = [25, 27, 28, 29, 30]} // @oss-enable
struct SimulationResult {
  1: config.Simulation config;
  2: SimulationStats stats;
  3: WorkloadResultMap workloads;
  11: SwitchStatsByName switchStats;
  5: list<Snapshot> snapshots;
  // Not populated by the simulator: `totalReqs` and `totalBytes` stay 0.
  6: RequestStats requestStats;
  7: RealWorldTiming realWorldTiming;
  8: NicSummaryByName nics;
  10: uint64 randomSeed = 0;
  20: TraceSamplesByName traces;
  21: string chromeTrace = "";
  // Commit hash of the simulation build. Set by builds that carry revision
  // metadata; empty otherwise.
  22: string buildRevision = "";
  // Unique identifier for this simulation run (UUID-based).
  23: string runId = "";
  // Blob-store path the simulation result was written to; empty if the run
  // wrote to none. The field name predates the blob-store abstraction and is
  // kept for wire compatibility.
  24: string manifoldPath = "";
  // @oss-disable
  // @oss-disable
  // Per-multi-port-NIC stats (e.g. the port-pair load-balance heatmap), keyed
  // by NIC name. Populated once at end-of-sim by transports that implement
  // `Transport::getMultiPortNicStats()`; empty otherwise.
  26: MultiPortNicStatsByName multiPortNicStats;
  // @oss-disable
  // @oss-disable
}

@cpp.Type{template = "folly::F14FastMap"}
typedef map<i32, i32> RankToHostMap
@cpp.Type{
  template = "folly::F14FastMap",
}
typedef map<string, MultiPortNicStats> MultiPortNicStatsByName

@cpp.Type{
  template = "folly::F14FastMap",
}
typedef map<string, NicSummary> NicSummaryByName
@cpp.Type{
  template = "folly::F14FastMap",
}
typedef map<string, switches.Switch> SwitchStatsByName
@cpp.Type{
  template = "folly::F14FastMap",
}
typedef map<string, WorkloadResult> WorkloadResultMap
@cpp.Type{
  template = "folly::F14FastMap",
}
typedef map<string, list<TraceSample>> TraceSamplesByName
