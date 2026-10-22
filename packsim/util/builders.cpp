/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/builders.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <folly/logging/LogConfigParser.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/xlog.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/topology_constants.h"
#include "packsim/util/config-builders.h"
// @oss-disable

// NOTE: This is capitalized as it might be set as an environment variable
DEFINE_string(FOLLY_LOG_LEVEL, "INFO", "Folly log level");

namespace packsim {

namespace {

// Indexed by PackSimSimConfig::ingAlpha.
constexpr std::array<float, 11> kAlphaByIngAlpha = {
    0.0078125,
    0.015625,
    0.03125,
    0.0625,
    0.125,
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0};

const folly::F14FastMap<uint8_t, uint8_t> kDscpToCosMapDefault =
    {{56, 7}, {48, 6}, {40, 5}, {32, 4}, {24, 3}, {16, 2}, {8, 1}, {0, 0}};

TransportSelection resolveTransportSelection(
    const TransportKind transport,
    const RoceCongestionControl congestionControl,
    const RoceLossRecovery lossRecovery) {
  TransportSelection selection{transport, congestionControl, lossRecovery};
  // RoCEv2 accepts every congestion-control and loss-recovery combination.
  // @oss-disable
  return selection;
}

packsim::thrift::Simulation cfgToSim(
    const packsim::PackSimSimConfig& cfg,
    const uint8_t numQueues = 1) {
  if (numQueues == 0) {
    throw std::invalid_argument("numQueues must be greater than 0");
  }
  packsim::thrift::Simulation sim;
  sim.snapshotIntervalNs() = cfg.snapshotIntervalNs;

  // Link settings
  packsim::ClosLinks links;
  links.accelToRtsw.speed = cfg.gpuRtswLinkSpeed;
  links.accelToRtsw.delayNs = cfg.n2SwDlyNs;
  links.accelToRtsw.numLinks = 1;

  links.rtswToCtsw.speed = cfg.rtswCtswLinkSpeed;
  links.rtswToCtsw.delayNs = cfg.sw2SwDlyNs;
  links.rtswToCtsw.numLinks = cfg.numRtswCtswLinks;

  links.ctswToAtsw.speed = cfg.ctswAtswLinkSpeed;
  links.ctswToAtsw.delayNs = cfg.sw2SwDlyNs;
  links.ctswToAtsw.numLinks = cfg.numCtswAtswLinks;

  // Topology settings
  packsim::ClosTopologySpec topo;
  topo.accelPerHost = cfg.numAccelPerHost;
  topo.accelPerRack = cfg.numHostsPerRack * cfg.numAccelPerHost;
  topo.accelPerZone = cfg.numAccelPerZone;

  topo.zones = kFourZoneClosZones;
  topo.numCtsws = cfg.numCtsws;
  topo.numAtsws = cfg.numAtsws;
  topo.nominalRacksPerZone = cfg.nominalRacksPerZone;

  const packsim::ClosSpinePorts wiredPorts =
      packsim::wiredSpinePorts(links, topo, kFourZoneClosPlanes);

  // Switch settings
  auto& switchCfgs = *sim.switchConfigMap();
  packsim::thrift::SwitchParams rtswCfg;
  rtswCfg.numBuffs() = 2U;
  rtswCfg.numQueues() = numQueues;
  rtswCfg.numUplinks() = cfg.numCtsws * cfg.numRtswCtswLinks;
  rtswCfg.numDownlinks() = cfg.numAccelPerHost * cfg.numHostsPerRack;
  rtswCfg.cellSizeBytes() = cfg.switchBuffCellSize;
  rtswCfg.bufSizeBytes() = cfg.switchBuffSizeinMB * 1024 * 1024;
  rtswCfg.rsvdBytesPerPort() = cfg.rswRsvdSzPerPort;
  rtswCfg.xonHystBytes() = cfg.rswXonHystBytes;
  rtswCfg.hdrmBytesPerPortUl() = cfg.rswHdrmSzPerPortUl;
  rtswCfg.hdrmBytesPerPortDl() = cfg.rswHdrmSzPerPortDl;
  rtswCfg.dynShareEn() = cfg.rswDynShareEn;
  rtswCfg.alpha() = kAlphaByIngAlpha.at(cfg.ingAlpha);
  rtswCfg.isLosslessPerPg() = std::vector<bool>(numQueues, true);

  switchCfgs["RTSW"] = packsim::switchParamsToConfig(rtswCfg);
  packsim::thrift::SwitchParams ctswCfg;
  ctswCfg.numBuffs() = 1U;
  ctswCfg.numQueues() = numQueues;
  const packsim::SwitchPortCounts ctswPorts =
      cfg.spinePorts.value_or(wiredPorts.ctsw);
  ctswCfg.numUplinks() = ctswPorts.uplinks;
  ctswCfg.numDownlinks() = ctswPorts.downlinks;
  ctswCfg.cellSizeBytes() = cfg.switchBuffCellSize;
  ctswCfg.bufSizeBytes() = cfg.bswSwitchBuffSizeinMB * 1024 * 1024;
  ctswCfg.rsvdBytesPerPort() = cfg.bswSwitchRsvdSzPerPort;
  ctswCfg.xonHystBytes() = cfg.bswXonHystBytes;
  ctswCfg.hdrmBytesPerPortUl() = cfg.bswSwitchHdrmSzPerPort;
  ctswCfg.hdrmBytesPerPortDl() = cfg.bswSwitchHdrmSzPerPort;
  // bswHdrmPoolPerBuff is given per half of the switch buffer; this model uses
  // one buffer, so it gets twice the value.
  ctswCfg.hdrmPoolPerBuff() = cfg.bswHdrmPoolPerBuff * 2;
  ctswCfg.dynShareEn() = cfg.bswDynShareEn;
  ctswCfg.alpha() = kAlphaByIngAlpha.at(cfg.ingAlpha);
  ctswCfg.isLosslessPerPg() = std::vector<bool>(numQueues, true);

  switchCfgs["CTSW"] = packsim::switchParamsToConfig(ctswCfg);

  packsim::thrift::SwitchParams atswCfg = ctswCfg;
  const packsim::SwitchPortCounts atswPorts =
      cfg.spinePorts.value_or(wiredPorts.atsw);
  atswCfg.numUplinks() = atswPorts.uplinks;
  atswCfg.numDownlinks() = atswPorts.downlinks;
  atswCfg.bufSizeBytes() = cfg.atswSwitchBuffSizeinMB * 1024 * 1024;
  switchCfgs["ATSW"] = packsim::switchParamsToConfig(atswCfg);

  // NIC settings
  auto& nicCfg = *sim.nic();
  nicCfg.writeEff() = cfg.nicEfficiency;
  nicCfg.readGbps() = cfg.gpuMemBWGbps;
  const uint32_t buffSzPerQueue = cfg.nicBufferSize / numQueues;
  const uint32_t xoffPerQueue =
      (numQueues == 1) ? cfg.nicXoff : (buffSzPerQueue * 0.95);
  const uint32_t xonPerQueue =
      (numQueues == 1) ? cfg.nicXon : (xoffPerQueue - cfg.mtub);
  const int32_t perCosBufferSize = folly::to<int32_t>(buffSzPerQueue);
  const int32_t perCosXon = folly::to<int32_t>(xonPerQueue);
  const int32_t perCosXoff = folly::to<int32_t>(xoffPerQueue);
  const packsim::thrift::PortQueueConfig nicQueueCfg;
  for (uint8_t queue = 0; queue < numQueues; queue++) {
    nicCfg.perCosBufferSize()->emplace_back(perCosBufferSize);
    nicCfg.perCosXon()->emplace_back(perCosXon);
    nicCfg.perCosXoff()->emplace_back(perCosXoff);
    nicCfg.queueCfg()->emplace_back(nicQueueCfg);
  }
  nicCfg.speed() = cfg.gpuRtswLinkSpeed;
  nicCfg.mtu() = folly::to<int32_t>(cfg.mtub);
  nicCfg.ackCos() = cfg.nicAckCos;
  nicCfg.dataCos() = cfg.nicDataCos;
  nicCfg.queueFilterCfg()->fieldToQueue() = kDscpToCosMapDefault;

  nicCfg.transport() = cfg.transportConfig;

  sim.nicConfigMap()[packsim::thrift::topology_constants::kDefaultNicConfig()] =
      nicCfg;

  return packsim::addFlexTopology(
      sim, links, topo, cfg.strRouting, kFourZoneClosPlanes);
}

std::vector<std::vector<uint32_t>> randomA2AVec(
    const uint32_t numAccel,
    const uint32_t sizeBytes,
    Random& random) {
  std::vector<std::vector<uint32_t>> vectorizedMsgSize;
  // fill in zeros for the 2D vectorizedMsgSiz array
  vectorizedMsgSize.reserve(numAccel);
  for (uint32_t i = 0; i < numAccel; i++) {
    vectorizedMsgSize.emplace_back(numAccel, 0U);
  }

  // fill in numbers drawn from normal distribution, approx. 1/Nth each
  const double meanBytes = static_cast<double>(sizeBytes) / numAccel;
  const double stdBytes = std::sqrt(meanBytes);
  for (uint32_t i = 0; i < numAccel; i++) {
    for (uint32_t j = 0; j < numAccel; j++) {
      if (i != j) {
        const int32_t sample =
            std::round(random.sampleNormal(meanBytes, stdBytes));
        vectorizedMsgSize.at(i).at(j) =
            sample >= 0 ? folly::to<uint32_t>(sample) : 0U;
      }
    }
  }
  return vectorizedMsgSize;
}

} // namespace

TransportKind parseTransportKind(const std::string& transportType) {
  std::string normalized = transportType;
  folly::toLowerAscii(normalized);
  if (normalized == "roce") {
    return TransportKind::ROCE;
  }
  // @oss-disable
    // @oss-disable
  // @oss-disable
  // Report the caller's original spelling, not the lowercased form, so
  // misspellings are recognizable in the error.
  throw std::invalid_argument(
      fmt::format("Unrecognized transport type: {}", transportType));
}

packsim::PackSimSimConfig buildFourZoneClosCliConfig(
    const uint32_t numAccel,
    const uint8_t numQueues) {
  packsim::PackSimSimConfig packsimSimCfg;
  packsimSimCfg.numAccelPerZone = numAccel / kFourZoneClosZones;
  packsimSimCfg.numQueues = numQueues;
  return packsimSimCfg;
}

packsim::thrift::Simulation buildFourZoneClosConfig(
    const FourZoneClosConfigOptions& options) {
  const auto selection = resolveTransportSelection(
      options.transportType, options.congestionControl, options.lossRecovery);

  auto cliCfg = options.buildFabricFn(options.numAccel, options.numQueues);
  const auto makeCongestionControl = [](const RoceCongestionControl algorithm) {
    packsim::thrift::RoceV2CongestionControlConfig congestionControlConfig;
    switch (algorithm) {
      case RoceCongestionControl::NONE:
        congestionControlConfig.dummy() =
            packsim::thrift::DummyRoceV2StrategyConfig{};
        return congestionControlConfig;
      case RoceCongestionControl::DCQCN:
        congestionControlConfig.dcqcn() = packsim::thrift::DcqcnCcConfig{};
        return congestionControlConfig;
    }
    throw std::logic_error("Unsupported RoCE congestion control");
  };
  const auto makeLossRecovery = [](const RoceLossRecovery algorithm) {
    packsim::thrift::RoceV2LossRecoveryConfig lossRecoveryConfig;
    switch (algorithm) {
      case RoceLossRecovery::NONE:
        lossRecoveryConfig.dummy() =
            packsim::thrift::DummyRoceV2StrategyConfig{};
        return lossRecoveryConfig;
      case RoceLossRecovery::GO_BACK_N:
        lossRecoveryConfig.goBackN() = packsim::thrift::GoBackNLrConfig{};
        return lossRecoveryConfig;
    }
    throw std::logic_error("Unsupported RoCE loss recovery");
  };

  switch (selection.transport) {
    case TransportKind::ROCE: {
      packsim::thrift::RoceV2TransportConfig roceConfig;
      roceConfig.congestionControl() =
          makeCongestionControl(selection.congestionControl);
      roceConfig.lossRecovery() = makeLossRecovery(selection.lossRecovery);
      cliCfg.transportConfig.roceV2() = std::move(roceConfig);
      break;
    }
    // @oss-disable
      // @oss-disable
          // @oss-disable
      // @oss-disable
  }
  return cfgToSim(cliCfg, options.numQueues);
}

packsim::thrift::Simulation buildFourZoneClosConfig(
    const uint32_t numAccel,
    const uint8_t numQueues) {
  return buildFourZoneClosConfig({
      .numAccel = numAccel,
      .numQueues = numQueues,
  });
}

packsim::workload::thrift::Workload getWorkCfg(
    Random& random,
    const packsim::thrift::WorkloadType type,
    const uint32_t numAccel,
    const uint32_t sizeBytes,
    const uint32_t numChunks) {
  packsim::workload::thrift::Workload workCfg;
  workCfg.name() = "TestWorkload";
  workCfg.workloadId() = 0;
  workCfg.numEndpoints() = numAccel;
  workCfg.maxStaggeredStartTimeNs() = 10'000;
  auto& w = *workCfg.config();

  switch (type) {
    case packsim::thrift::WorkloadType::ALL_TO_ALL:
      w.allToAll() = packsim::workload::thrift::AllToAll();
      w.allToAll()->sizeBytes() = sizeBytes;
      w.allToAll()->numChunks() = numChunks;
      w.allToAll()->maxChunksInFlight() = numChunks;
      return workCfg;
    case packsim::thrift::WorkloadType::VECTORIZED_ALL_TO_ALL:
      w.allToAll() = packsim::workload::thrift::AllToAll();
      w.allToAll()->vectorizedSizesBytes() =
          randomA2AVec(numAccel, sizeBytes, random);
      w.allToAll()->numChunks() = 1;
      w.allToAll()->maxChunksInFlight() = 1;
      return workCfg;
    case packsim::thrift::WorkloadType::ALL_REDUCE_DBT:
      w.allReduce() = packsim::workload::thrift::AllReduce();
      w.allReduce()->algorithm() =
          packsim::workload::thrift::AllReduceAlgorithm::DOUBLE_BINARY_TREE;
      w.allReduce()->sizeBytes() = sizeBytes;
      w.allReduce()->numChunks() = numChunks;
      w.allReduce()->maxChunksInFlight() = numChunks;
      return workCfg;
    case packsim::thrift::WorkloadType::ALL_REDUCE_HALVING_DOUBLING:
      w.allReduce() = packsim::workload::thrift::AllReduce();
      w.allReduce()->algorithm() =
          packsim::workload::thrift::AllReduceAlgorithm::RECURSIVE_DIST_OPT;
      w.allReduce()->sizeBytes() = sizeBytes;
      w.allReduce()->numChunks() = numChunks;
      w.allReduce()->maxChunksInFlight() = numChunks;
      return workCfg;
    case packsim::thrift::WorkloadType::ALL_REDUCE_RING:
      w.allReduce() = packsim::workload::thrift::AllReduce();
      w.allReduce()->algorithm() =
          packsim::workload::thrift::AllReduceAlgorithm::RING;
      w.allReduce()->sizeBytes() = sizeBytes;
      w.allReduce()->numChunks() = numChunks;
      w.allReduce()->maxChunksInFlight() = numChunks;
      return workCfg;
    case packsim::thrift::WorkloadType::INCAST:
      w.incast() = packsim::workload::thrift::Incast();
      w.incast()->sizeBytes() = sizeBytes;
      w.incast()->numChunks() = numChunks;
      w.incast()->numSources() = numAccel - 1;
      return workCfg;
    case packsim::thrift::WorkloadType::PERMUTATION: {
      w.permutation() = packsim::workload::thrift::Permutation();
      w.permutation()->sizeBytes() = sizeBytes;
      return workCfg;
    }
    case packsim::thrift::WorkloadType::REDUCE_SCATTER_RING:
      w.reduceScatter() = packsim::workload::thrift::ReduceScatter();
      w.reduceScatter()->sizeBytes() = sizeBytes;
      w.reduceScatter()->algorithm() =
          packsim::workload::thrift::AllReduceAlgorithm::RING;
      return workCfg;
    case packsim::thrift::WorkloadType::ALL_GATHER_RING:
      w.allGather() = packsim::workload::thrift::AllGather();
      w.allGather()->sizeBytes() = sizeBytes;
      w.allGather()->algorithm() =
          packsim::workload::thrift::AllReduceAlgorithm::RING;
      return workCfg;
    // @oss-disable
      // @oss-disable
      // @oss-disable
    case packsim::thrift::WorkloadType::ALL_GATHER_DOUBLING:
      w.allGather() = packsim::workload::thrift::AllGather();
      w.allGather()->algorithm() =
          packsim::workload::thrift::AllReduceAlgorithm::RECURSIVE_MSG_OPT;
      w.allGather()->sizeBytes() = sizeBytes;
      return workCfg;
    case packsim::thrift::WorkloadType::REDUCE_SCATTER_HALVING:
      w.reduceScatter() = packsim::workload::thrift::ReduceScatter();
      w.reduceScatter()->algorithm() =
          packsim::workload::thrift::AllReduceAlgorithm::RECURSIVE_DIST_OPT;
      w.reduceScatter()->sizeBytes() = sizeBytes;
      w.reduceScatter()->numChunks() = numChunks;
      w.reduceScatter()->maxChunksInFlight() = numChunks;
      w.reduceScatter()->numRails() = 8;
      return workCfg;
    case packsim::thrift::WorkloadType::UNKNOWN:
    case packsim::thrift::WorkloadType::BATCH:
    case packsim::thrift::WorkloadType::NEW_WORKLOAD:
    // @oss-disable
      break;
  }
  // Also reached by a value outside the enum, which a thrift enum can hold.
  throw std::runtime_error("Unsupported Type");
}

packsim::workload::thrift::Workload getArDbtCfg(
    Random& random,
    const uint32_t numAccel,
    const uint32_t sizeBytes,
    const uint32_t numChunks) {
  return getWorkCfg(
      random,
      packsim::thrift::WorkloadType::ALL_REDUCE_DBT,
      numAccel,
      sizeBytes,
      numChunks);
}

packsim::workload::thrift::Workload getArRhdCfg(
    Random& random,
    const uint32_t numAccel,
    const uint32_t sizeBytes,
    const uint32_t numChunks) {
  return getWorkCfg(
      random,
      packsim::thrift::WorkloadType::ALL_REDUCE_HALVING_DOUBLING,
      numAccel,
      sizeBytes,
      numChunks);
}

void assignNewWorkloadSchedule(
    const packsim::thrift::FlexTopology& topology,
    packsim::thrift::WorkloadNewSchedule& sched,
    Random& random) {
  if (sched.method() == packsim::thrift::SchedulingMethod::OVERLAP_RACKS) {
    throw std::runtime_error("OVERLAP_RACKS no longer supported");
  }
  std::vector<packsim::thrift::Node> nodeList;
  nodeList.reserve(topology.nodes()->size());
  std::vector<uint32_t> rackList;
  for (const auto& pair : *topology.nodes()) {
    if (*pair.second.type() == packsim::thrift::NodeType::ACCEL) {
      nodeList.emplace_back(pair.second);
    }
  }
  rackList.reserve(nodeList.size() / 2);
  for (uint32_t i = 0; i < nodeList.size() / 2; i++) {
    rackList.emplace_back(i);
  }

  XLOGF(DBG1, "{} nodes in nodeList", nodeList.size());

  // Sort list of nodes by FlexId
  std::sort(
      nodeList.begin(),
      nodeList.end(),
      [](const packsim::thrift::Node& a, const packsim::thrift::Node& b) {
        return *a.id() < *b.id();
      });

  // If random, shuffle the corresponding list
  if (sched.method() == packsim::thrift::SchedulingMethod::RANDOM) {
    if (sched.unit() == packsim::thrift::SchedulingUnit::SUPERNODE) {
      random.shuffle(nodeList.begin(), nodeList.end());
    } else {
      random.shuffle(rackList.begin(), rackList.end());
    }
  }

  // Only support random and sequential
  uint32_t listIdx{0};
  uint32_t numRanks{0};
  uint32_t workloadId{0};
  const size_t numNodes = nodeList.size();
  for (auto& w : *sched.workloads()) {
    w.workloadId() = workloadId++;
    numRanks += *w.numEndpoints();

    // Make sure we don't overflow total # of nodes
    if (numRanks > numNodes) {
      throw std::runtime_error(
          fmt::format(
              "Workload definition requires {} hosts, but numAccelerators is only {}",
              numRanks,
              numNodes));
    }

    if (sched.unit() == packsim::thrift::SchedulingUnit::SUPERNODE) {
      for (uint32_t r = 0; r < *w.numEndpoints(); r++) {
        w.rankToId()[r] = *nodeList.at(listIdx++).id();
      }
    } else {
      if (*w.numEndpoints() % 2 != 0) {
        throw std::invalid_argument(
            fmt::format(
                "Workload {} places its ranks in pairs, so numEndpoints must be "
                "even unless scheduling by SUPERNODE; got {}",
                *w.workloadId(),
                *w.numEndpoints()));
      }
      for (uint32_t r = 0; r < *w.numEndpoints() / 2; r++) {
        const auto rack = rackList.at(listIdx++);
        w.rankToId()[2 * r] = *nodeList.at(2 * rack).id();
        w.rankToId()[2 * r + 1] = *nodeList.at(2 * rack + 1).id();
      }
    }
  }
}

void updateLogLevel() {
  const bool isFollyLogLevelDefault =
      gflags::GetCommandLineFlagInfoOrDie("FOLLY_LOG_LEVEL").is_default;
  if (!isFollyLogLevelDefault) {
    folly::LoggerDB::get().updateConfig(
        folly::parseLogConfig(FLAGS_FOLLY_LOG_LEVEL));
  }
  // FOLLY_LOG_LEVEL in the environment is applied after the flag, so it wins
  // for any log category both set.
  if (const char* follyLogLevelEnv = std::getenv("FOLLY_LOG_LEVEL")) {
    folly::LoggerDB::get().updateConfig(
        folly::parseLogConfig(follyLogLevelEnv));
  }

  const auto& logConfig = folly::LoggerDB::get().getConfig();
  const auto& rootConfig = logConfig.getCategoryConfigs().find("");
  const auto logLevel = rootConfig != logConfig.getCategoryConfigs().end()
      ? rootConfig->second.level
      : folly::LogLevel::INFO;
  XLOGF(INFO, "Log level is currently {}", folly::logLevelToString(logLevel));
}

packsim::thrift::Simulation buildFourZoneClosWorkloadSim(
    Random& random,
    const FourZoneClosWorkloadOptions& options) {
  if (options.numAccel == 0) {
    throw std::invalid_argument("numAccel must be greater than 0");
  }
  if (options.sizeBytes == 0) {
    throw std::invalid_argument("sizeBytes must be greater than 0");
  }
  if (options.numCopies == 0) {
    throw std::invalid_argument("numCopies must be greater than 0");
  }
  if (!(options.numAccel % options.numCopies == 0)) {
    throw std::invalid_argument("numAccel must be a multiple of numCopies");
  }

  auto sim = buildFourZoneClosConfig({
      .numAccel = options.numAccel,
      .numQueues = 1,
      .transportType = options.transportType,
      .congestionControl = options.congestionControl,
      .lossRecovery = options.lossRecovery,
      .buildFabricFn = options.buildFabricFn,
  });
  auto& workSched = *sim.newWorkloadSchedule();
  workSched.unit() = options.unit;
  workSched.method() = options.method;

  const uint32_t numAccelPerWorkload = options.numAccel / options.numCopies;
  auto& workloads = *workSched.workloads();
  workloads.reserve(options.numCopies);
  for (uint32_t i = 0; i < options.numCopies; i++) {
    auto workCfg = getWorkCfg(
        random,
        options.workloadType,
        numAccelPerWorkload,
        options.sizeBytes,
        options.numChunks);
    workCfg.name() = fmt::format("{}_{}", *workCfg.name(), i);
    workloads.emplace_back(std::move(workCfg));
  }
  assignNewWorkloadSchedule(*sim.flex_topology(), workSched, random);
  return sim;
}

} // namespace packsim
