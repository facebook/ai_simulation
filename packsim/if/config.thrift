/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace cpp2 packsim.thrift
namespace py3 packsim

cpp_include "folly/container/F14Map.h"

// @oss-disable
include "packsim/if/nic_config.thrift"
include "packsim/if/qp_pair_config.thrift"
// @oss-disable
include "packsim/if/switch_config.thrift"
include "packsim/if/topology.thrift"
include "packsim/if/workload.thrift"
// @oss-disable
include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

// @oss-disable
@thrift.AllowLegacyMissingUris // @oss-enable
package; // @oss-enable

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

// Workload

// Not read by the simulator. How `assignNewWorkloadSchedule`
// (util/builders.h) gives each rank of a workload its own accelerator:
// SUPERNODE hands them out one at a time; every other value, UNKNOWN
// included, hands them out in pairs adjacent in device id order, rank 2k
// taking the first of a pair and rank 2k+1 the second, so each workload's
// `numEndpoints` must be even.
enum SchedulingUnit {
  UNKNOWN = 0,
  SUPERNODE = 1,
  RACK = 2,
  ENDPOINT = 3,
}

// Not read by the simulator. The order in which `assignNewWorkloadSchedule`
// hands out accelerators, or pairs of them for a unit other than SUPERNODE:
// RANDOM shuffles them, SEQUENTIAL and UNKNOWN keep device id order, and
// OVERLAP_RACKS makes it throw.
enum SchedulingMethod {
  UNKNOWN = 0,
  SEQUENTIAL = 1,
  RANDOM = 2,
  OVERLAP_RACKS = 3,
}

// Not read by the simulator: no config or result field has this type.
// `getWorkCfg` (util/builders.h) builds a workload of the type it is given,
// and throws for a type it does not build, such as UNKNOWN, BATCH or
// NEW_WORKLOAD.
@thrift.ReserveIds{ids = [12, 15]} // @oss-enable
enum WorkloadType {
  UNKNOWN = 0,
  ALL_TO_ALL = 1,
  ALL_REDUCE_DBT = 2,
  ALL_REDUCE_HALVING_DOUBLING = 3,
  BATCH = 4,
  VECTORIZED_ALL_TO_ALL = 5,
  INCAST = 6,
  PERMUTATION = 7,
  ALL_REDUCE_RING = 8,
  NEW_WORKLOAD = 9,
  REDUCE_SCATTER_RING = 10,
  ALL_GATHER_RING = 11,
  // @oss-disable
  REDUCE_SCATTER_HALVING = 13,
  ALL_GATHER_DOUBLING = 14,
  // @oss-disable
}

struct OriginalWorkloadInfo {
  1: uint32 internalWorkloadId;
  2: string originalWorkloadName;
}

struct WorkloadNewSchedule {
  // Not read by the simulator, which places each rank on the device
  // `Workload.rankToId` maps it to. `assignNewWorkloadSchedule`
  // (util/builders.h) reads this and `method` to fill `rankToId`.
  1: SchedulingUnit unit;
  // Not read by the simulator; see `unit`.
  2: SchedulingMethod method;
  3: list<workload.Workload> workloads;

  // Populate optional value to enable DAG based scheduling.
  // NOTE: Each workload must be part of exactly one DAG.
  4: optional list<workload.WorkloadDag> workloadDags;

  // Run the DAGs above in a loop until every DAG has completed at
  // least N iterations. A DAG that gets there first keeps repeating
  // until the last one does, and the run ends once each DAG has
  // finished the iteration it is in. Iteration k > 1 of a workload
  // is reported under its own `SimulationResult.workloads` key: the
  // workload's iteration-1 key followed by `_ITER_<k>`. This value
  // is only relevant if DAGs are provided above.
  5: optional uint32 minIterationsPerDag;

  // Internally generated map from workload names used within
  // PackSim to internal workload IDs and input names. For example,
  // we generate sub-workloads with different names for
  // AllReduce rails.
  8: map<
    string /* generated/internal workload name */,
    OriginalWorkloadInfo
  > workloadNameInfos;
}

// Simulation

// Job metadata set by whatever submits the run, carried through into the
// result.
// @oss-disable
// @oss-disable
// The simulator reads only `resultPath`, to warn that it is // @oss-enable
// ignored: `--blob_output` names the result's blob path. // @oss-enable
@thrift.ReserveIds{ids = [30]} // @oss-enable
struct JobInfo {
  // Blob-store path where config is stored
  1: string configPath;
  // Blob-store path to write results to; see JobInfo.
  2: string resultPath;
  // Username of the person who launched the job
  10: string launchUser;
  // Launch time of job by user, in seconds since epoch
  11: double launchTime;
  // Name of the sweep this job belongs to
  20: string sweepId;
  // Set of variables that were set specifically for this sweep item
  // Should be a serialized JSON map
  21: string sweepParametersJson;
  // @oss-disable
  // @oss-disable
}

struct RandomSettings {
  1: uint64 seed = 0;
  // If true, simulation will use seed above
  // If false, simulation will reseed with random device
  2: bool useDeterministicSeed = false;
}

// Configs that are applied at network level.
struct NetworkLevelFeatureConfig {
  // Enable per-message result tracking. When true, records send/recv
  // timestamps for every message and includes them in WorkloadResult.
  // Latency is measured transport-to-transport (source transport send
  // to destination transport receive), not end-to-end.
  // Adds memory overhead proportional to message count.
  1: bool enablePerMessageTracking = false;
}

// @oss-disable
@thrift.ReserveIds{ids = [128, 131, 132, 133, 201, 202]} // @oss-enable
struct Simulation {
  // The snapshot interval in nano seconds. If zero or less then snapshotting is disabled. The system snapshots
  // all the nodes specified in 'snapshotNodes' and if the list empty then snapshot all the switches.
  3: i64 snapshotIntervalNs = -1;

  // Conditional-trace interval in ns per trace category (1000 traces the
  // category every 1 us); each must be at least 1. The flag
  // all_conditional_traces_interval_us, when set, caps every interval and also
  // traces the categories not listed here. Neither this map nor the flag
  // traces anything when no conditional trace is registered.
  4: ConditionalTraceIntervalsMap conditionalTraceIntervalsNs;

  // The list of nodes to collect the snapshot for and if empty the snapshot is collected for all the switches.
  9: list<string> snapshotNodes;

  // Random number generator setting.
  7: RandomSettings random;

  // Map from role to switch config.
  21: map<string, switch_config.SwitchConfig> switchConfigMap;

  // Map of nic configs. The endpoint picks the nic config based on it's nicConfig value (see WorkloadNode::WorkloadNode)
  22: map<string, nic_config.NicConfig> nicConfigMap;

  // Legacy single-NIC config. Endpoints take their NIC from `nicConfigMap`;
  // when that map is empty, this is copied into it as "default". Some fields
  // are still read from here, whatever `nicConfigMap` says:
  // `netDeviceSnapshotIntervalNs` sets the periodic-stats interval of every
  // link, switch-to-switch links included; `outofOrderPktSupport` feeds a
  // topology-build check that only logs "NIC does not support out of order
  // packet arrival" when this is false and the entry an accelerator's own
  // node-level `lpmSpec` uses for that accelerator's address has a tiebreak
  // that reorders packets, such as `spray`; and `checkSimCfg` rejects a
  // `pktDropCfg` drop type other than `NO_DROP` when `outofOrderPktSupport` is
  // set, which is the only effect `pktDropCfg` has (the simulator never
  // injects the drops `pktDropCfg` configures). The transport's own
  // out-of-order handling uses the endpoint's `nicConfigMap` entry.
  30: nic_config.NicConfig nic;
  70: topology.FlexTopology flex_topology;
  80: JobInfo jobInfo;
  100: string name;
  127: WorkloadNewSchedule newWorkloadSchedule;
  // @oss-disable
  // False to omit config from result struct to reduce size
  129: bool includeConfigInResults = true;
  // If true, allow workloads to complete with missing or extra bytes without throwing
  130: bool allowIncompleteWorkloads = false;

  // @oss-disable
  // @oss-disable

  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable

  // @oss-disable
  // @oss-disable

  // Configs that are applied at network level.
  200: optional NetworkLevelFeatureConfig networkLevelFeatureConfig;

  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
}

@cpp.Type{template = "folly::F14FastMap"}
typedef map<string, uint64> ConditionalTraceIntervalsMap
