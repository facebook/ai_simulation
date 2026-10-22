/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// @oss-disable
// @oss-disable
include "packsim/if/qp_pair_config.thrift"
include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

cpp_include "folly/container/F14Map.h"

// @oss-disable
@thrift.AllowLegacyMissingUris // @oss-enable
package; // @oss-enable

namespace cpp2 packsim.workload.thrift
namespace py3 packsim.workload

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

// Workload mode for AllToAll operations
enum WorkloadMode {
  UNKNOWN = 0,
  SENDRCV = 1, // Default two-sided SEND/RECV
  // WRITE (requester pushes data). The RoCEv2 transport throws when a NIC with
  // outofOrderPktSupport on receives a WRITE packet; see
  // NicConfig.outofOrderPktSupport.
  PUSH = 2,
  // READ (requester pulls data). The RoCEv2 transport does not model RDMA READ
  // and throws at the first READ a PULL workload posts. A PULL workload with
  // channelSettings, or inside a batch that sets them, is rejected when its
  // ranks are built.
  PULL = 3,
}

// @oss-disable
@thrift.ReserveIds{ids = [7, 9]} // @oss-enable
struct AllToAll {
  1: uint64 sizeBytes;
  2: uint32 numChunks = 1;
  3: uint32 maxChunksInFlight = 1;
  // Vectorized AllToAll: entry [i][j] is the bytes rank i sends rank j. Empty
  // means a uniform AllToAll of sizeBytes; otherwise a numRanks x numRanks
  // matrix whose diagonal is 0 (a rank sends nothing to itself).
  4: list<list<uint32>> vectorizedSizesBytes;
  // msgDelaysNs, is optional, and if provided, must be an array of equal
  // size and shape as the vectorizedSizesBytes array (i.e., this feature
  // only works for A2AV workloads for the moment). The msg delays introduce
  // waiting times (given in nanonseconds) that the sender will wait before
  // sending the corresponding messages defined in vectorizedSizesBytes.
  // The delay is happening early, at the WQE posting time.
  //
  // If numChunks > 1, then only the first chunk is delayed and the others
  // follow.
  //
  // If msgDelaysNs is not provided, no message delays are assumed (i.e.,
  // the msg delays are all set to zero).
  5: list<list<uint64>> msgDelaysNs;

  // Whether to use PXN for rail-based topologies.
  // https://developer.nvidia.com/blog/doubling-all2all-performance-with-nvidia-collective-communication-library-2-12/
  6: bool usePxn = false;
  // Number of GPUs per NV domain (GPUs joined by NVLink). Must be > 0, and
  // changes traffic only when usePxn is true. Ranks r and s are then on the
  // same rail when r % numGpusPerNvDomain == s % numGpusPerNvDomain, a rank
  // exchanges messages only with the other ranks on its rail, and each
  // message carries numGpusPerNvDomain times the data it would without PXN.
  // E.g. with usePxn, 16 ranks and numGpusPerNvDomain = 8, rank 0 exchanges
  // messages only with rank 8. With or without PXN, a rank also exchanges
  // nothing with the peers that Workload.rankToNvDomain puts in its NV domain.
  8: uint32 numGpusPerNvDomain = 1;
  // @oss-disable
  // @oss-disable

  // Workload mode: SENDRCV (default), PUSH (WRITE; see WorkloadMode.PUSH),
  // PULL (READ; see WorkloadMode.PULL). When set to PUSH or PULL, routes to
  // AllToAllPushPullRank.
  10: WorkloadMode workloadMode = WorkloadMode.SENDRCV;
}

enum AllReduceAlgorithm {
  UNKNOWN = 0,
  RING = 1,
  DOUBLE_BINARY_TREE = 2,
  // Optimize number of RDMA messages posted
  RECURSIVE_MSG_OPT = 3,
  // Optimize for sending less data farther away at the cost of more messages posted
  RECURSIVE_DIST_OPT = 4,
}

// NIC selection for ring collective chunks.
enum RingNicSelectionMode {
  // Send every chunk through the first compatible NIC pair.
  SINGLE_NIC = 0,
  // Distribute chunks round-robin across one compatible path per NIC.
  ALL_AVAILABLE_NICS = 1,
}

// Values for `ShardingOptions.shardingAlgorithm`, which the simulator does not
// read.
enum ShardingAlgorithm {
  UNKNOWN = 0,
  SEQUENTIAL = 1,
  ROUND_ROBIN = 2,
}

// Not read by the simulator: a collective is split across rails by its own
// `numRails` field.
// @oss-disable
// @oss-disable
struct ShardingOptions {
  // Number of sub-workloads.
  1: uint64 numRails = 8;

  // Sharding algorithm for data or model
  2: ShardingAlgorithm shardingAlgorithm = ShardingAlgorithm.UNKNOWN;
}

struct AllReduce {
  1: uint64 sizeBytes;
  2: AllReduceAlgorithm algorithm;
  3: uint32 numChunks = 1;
  4: uint32 maxChunksInFlight = 1;
  // Rails the collective is split across. Each rail runs as its own workload,
  // `<name>-rail<r>`, moving 1/numRails of `sizeBytes` among ranks r,
  // r + numRails, r + 2 * numRails, ... Must be at least 1, divide both
  // `sizeBytes` and `Workload.numEndpoints`, and leave each rail at least 2
  // ranks, so the default of 8 needs at least 16 endpoints. With the `RING`
  // algorithm a rail may keep 1 rank when that rank has an entry in
  // `Workload.rankToNvDomain`: it then sends nothing and completes at once. A
  // rail keeps the qpTemplates, nicQpnPairMap and qpnPairMap entries between
  // its own ranks, renumbered, and the qpnPortConfigs of their local QPNs.
  // @oss-disable
  5: uint32 numRails = 8;
  // Not read by the simulator, which splits the collective by `numRails`.
  // @oss-disable
  6: ShardingOptions shardingOptions;
  // Select how a ring algorithm distributes chunks across compatible NICs.
  7: RingNicSelectionMode nicSelectionMode = RingNicSelectionMode.SINGLE_NIC;
}

struct ReduceScatter {
  1: uint64 sizeBytes;
  2: AllReduceAlgorithm algorithm;
  3: uint32 numChunks = 32;
  4: uint32 maxChunksInFlight = 1;
  // Rails the collective is split across; see `AllReduce.numRails`.
  5: uint32 numRails = 8;
  // Not read by the simulator, which splits the collective by `numRails`.
  // @oss-disable
  6: ShardingOptions shardingOptions;
}

struct AllGather {
  1: uint64 sizeBytes;
  2: AllReduceAlgorithm algorithm;
  3: uint32 numChunks = 32;
  4: uint32 maxChunksInFlight = 1;
  // Rails the collective is split across; see `AllReduce.numRails`.
  5: uint32 numRails = 8;
  // Not read by the simulator, which splits the collective by `numRails`.
  // @oss-disable
  6: ShardingOptions shardingOptions;
  // Select how ring chunks are distributed across compatible NICs.
  7: RingNicSelectionMode nicSelectionMode = RingNicSelectionMode.SINGLE_NIC;
}

@thrift.ReserveIds{ids = [4]} // @oss-enable
struct Incast {
  1: uint64 sizeBytes;
  // Ranks 1..numSources each send sizeBytes to rank 0; ranks above numSources
  // take no part. 0 (the default) means every rank other than rank 0.
  // Otherwise numSources must be below numEndpoints, and an incast needs at
  // least 2 endpoints.
  2: uint32 numSources = 0;
  3: uint32 numChunks = 1;
  // @oss-disable
  // Workload mode: SENDRCV (default two-sided), PUSH (one-sided WRITE; see
  // WorkloadMode.PUSH). When set to PUSH, senders use RDMA WRITEs and no
  // receives are posted.
  5: WorkloadMode workloadMode = WorkloadMode.SENDRCV;
}

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
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
// @oss-disable

@thrift.ReserveIds{ids = [4]} // @oss-enable
struct Permutation {
  1: uint64 sizeBytes;
  // Data flows from rank i to rank dstRanks[i]. Must be a permutation of
  // 0..numEndpoints-1 in which no rank maps to itself; left empty, a random
  // single cycle is generated.
  2: list<uint32> dstRanks;
  3: uint32 numChunks = 1;
  // @oss-disable
  // @oss-disable
  // Workload mode: SENDRCV (default), PUSH (WRITE; see WorkloadMode.PUSH),
  // PULL (READ; see WorkloadMode.PULL). When set to PUSH or PULL, routes to
  // PermutationPushPullRank.
  5: WorkloadMode workloadMode = WorkloadMode.SENDRCV;
}

struct Batch {
  1: list<Workload> children;
}

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

struct Compute {
  1: uint64 computeTimeNs;
  2: optional uint64 computeTimeJitterNs;

  // Present so every workload arm carries a `sizeBytes` field. A compute
  // workload moves no data, so 0 is the only meaningful value.
  3: uint64 sizeBytes = 0;
}

// Ids reserved below are never reused for a different type. // @oss-enable
@thrift.ReserveIds{ids = [7, 20, 21, 30]} // @oss-enable
union WorkloadSpecificConfig {
  1: AllToAll allToAll;
  2: AllReduce allReduce;
  3: Incast incast;
  4: Permutation permutation;
  5: ReduceScatter reduceScatter;
  6: AllGather allGather;
  // @oss-disable
  10: Batch batch;
  // @oss-disable
  // @oss-disable
  // @oss-disable
  40: Compute compute;
}

struct QPNPair {
  1: uint32 local;
  2: uint32 remote;
}

@thrift.ReserveIds{ids = [57]} // @oss-enable
struct Workload {
  // globally unique workload identifier
  1: string name;
  3: uint32 numEndpoints = 8;
  // Not read by the simulator.
  4: bool sortRanks = true;
  20: uint64 startTimeNs = 0;
  21: uint64 maxStaggeredStartTimeNs = 0;
  // per-rank staggered time
  23: list<uint64> explicitStaggeredStartTimeNs;
  30: WorkloadSpecificConfig config;
  40: map<uint32, list<uint32>> rankToId;
  42: map<uint32, list<list<uint32>>> rankToIdList;
  // Explicit queue pair templates. AllReduce, AllGather and ReduceScatter
  // workloads are first split into rails (see AllReduce.numRails), and each
  // rail keeps the templates between its own ranks. Then, for a top-level
  // workload whose nicQpnPairMap is empty, these are expanded into
  // nicQpnPairMap and qpnPortConfigs and the list is cleared.
  // @oss-disable
  // @oss-disable
  // A batch child's templates are never expanded and have no effect. RoCEv2
  // does not restrict its port choice to a template's `ports`; see
  // QpnPortConfig.
  43: list<qp_pair_config.QueuePairTemplate> qpTemplates;

  // --- runtime data below ---

  // The metadata is an optional arbitrary key/value map that can be
  // used to provide hints that are associated with the workload, and
  // must not be used in a way that affect the simulation results. If
  // you find yourself trying to use the metadata to pass parameters
  // to the simulator, then you are using the metadata wrong. The metadata
  // can be useful at the stage where we process and visualize the results,
  // to provide additional context about a specific workload in an experiment.
  // Examples of information we may want carry with the metadata:
  // * Is the workload coming from real traces or it's a synthetic workload?
  // * An identifier of the experiment the workload is associated with
  // * The blob-store path where the data we used to build the workload live
  41: map<string, string> metadata;
  52: QPNPairMapMap qpnPairMap;
  // NIC-aware QPN mapping for multi-NIC configurations
  // {srcRank, dstRank} -> map{ {srcNicIdx, dstNicIdx} -> list<QPNPair>}
  55: NicQPNPairMapMap nicQpnPairMap;
  // Port configurations for each QPN, populated from qpTemplates
  // Maps localQPN -> QpnPortConfig (srcPorts and dstPorts)
  // RoCEv2's port choice does not depend on them; see QpnPortConfig.
  56: QpnPortConfigMap qpnPortConfigs;

  // channelSettings can be used to divide and send message over multiple QPs,
  // by default this struct is not set, to enable this feature this needs to be
  // set. A PULL workload cannot set it, nor can a batch with a PULL child:
  // building the ranks throws (see WorkloadMode.PULL). Channel QPs come from
  // qpnPairMap, numbered apart from qpTemplates, so a workload with
  // channelSettings cannot set qpTemplates, nor supply a nicQpnPairMap without
  // its qpnPairMap: building the simulation throws, and --validate reports it.
  // @oss-disable
  // @oss-disable
  53: optional ChannelSettings channelSettings;

  // Rank -> NV domain index, e.g. {0: 0, 1: 0, 2: 1, 3: 1} puts ranks 0 and 1
  // in domain 0 and ranks 2 and 3 in domain 1. AllToAll, and collectives
  // whose algorithm is RING, send nothing over the network between ranks of
  // one domain. This map is the only source of NV-domain membership: when it
  // is unset or empty, no two ranks share a domain, whatever
  // AllToAll.numGpusPerNvDomain is. When set, it must give every rank a
  // domain: an AllToAll or RING rank throws for a rank it leaves out.
  @cpp.Type{template = "folly::F14FastMap"}
  54: optional map<uint32, uint32> rankToNvDomain;
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable
  // @oss-disable

  // This is an internally assigned workload ID from PackSim. Please
  // do not set this as part of the input and do not rely on its value
  // when preparing the input config.
  2: uint32 workloadId = 0;
}

struct ChannelSettings {
  1: uint32 numChannels = 1;
  2: uint32 numQPsPerChannel = 1;
  // 1 MB
  3: uint32 channelMessageSizeBytes = 1048576;
}

struct WorkloadDag {
  // Dependencies between workloads in the DAG. Each workload can only
  // belong to one DAG globally. DAG must not contain any loops.
  1: map<
    string /* workload name */,
    set<string/* preceding workload name */ >
  > workloadDependencies;
}

// The following were automatically generated and may benefit from renaming.
@cpp.Type{
  template = "folly::F14FastMap",
}
typedef map<uint32, list<QPNPair>> QPNPairMap

// Map of srcRank -> map of dstRank -> list of {srcQPN, dstQPN} pairs
@cpp.Type{
  template = "folly::F14FastMap",
}
typedef map<uint32, QPNPairMap> QPNPairMapMap

// Map of {srcNicIdx, dstNicIdx} -> list of QPNPairs
// Used for mapping NIC pair to a list of queue pairs
@thrift.AllowUnsafeNonSealedKeyType
typedef map<qp_pair_config.NicPair, list<QPNPair>> NicPairToQPNList

// Map of {srcRank, dstRank} -> {srcNicIdx, dstNicIdx} -> list of QPNPairs
// Maps a pair of communicating ranks to a map of NIC pairs to their queue pairs
@thrift.AllowUnsafeNonSealedKeyType
typedef map<qp_pair_config.RankPair, NicPairToQPNList> NicQPNPairMapMap

// Map of localQPN -> QpnPortConfig
// Stores port configurations for each QPN (srcPorts and dstPorts)
@cpp.Type{template = "folly::F14FastMap"}
typedef map<uint32, qp_pair_config.QpnPortConfig> QpnPortConfigMap
