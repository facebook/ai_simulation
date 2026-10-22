/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/config-builders.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/Conv.h>
#include <folly/Math.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/ceil-divide.h"
#include "packsim/kernel/prefix-id-utils.h"

namespace packsim {

namespace {

constexpr uint32_t kAccelLayer = 1;
constexpr uint32_t kRtswLayer = 2;
constexpr uint32_t kCtswLayer = 3;
constexpr uint32_t kAtswLayer = 4;

// An accelerator id is its layer followed by its zone, rack, host and index.
constexpr uint32_t kAccelIdLength = 5;

const folly::F14FastMap<uint8_t, uint8_t> dscpToCosMapDefault =
    {{56, 7}, {48, 6}, {40, 5}, {32, 4}, {24, 3}, {16, 2}, {8, 1}, {0, 0}};

// The shared-cell limit written for a port or queue that has no limit of its
// own: the largest int32_t, 2^31 - 1.
constexpr uint32_t kUnlimitedCells = INT32_MAX;

// Largest drop or mark percentage.
constexpr uint32_t kMaxPercentage = 100;

// Converts `percentage`, the SwitchParams drop or mark percentage `fieldName`,
// to the whole percentage WredEcnConfig holds. Throws std::invalid_argument
// unless it is a whole number from 0 to 100.
uint32_t convertToWholePercentage(
    const double percentage,
    const std::string& fieldName) {
  // NaN fails both comparisons, so it never reaches the cast.
  if (!(percentage >= 0 && percentage <= kMaxPercentage) ||
      std::trunc(percentage) != percentage) {
    throw std::invalid_argument(
        fmt::format(
            "SwitchParams.{} must be a whole percentage from 0 to {}, got {}",
            fieldName,
            kMaxPercentage,
            percentage));
  }
  return static_cast<uint32_t>(percentage);
}

// Node ids and route prefixes are thrift `PrefixId`s, whose quartets are `i32`
// on the wire.
packsim::thrift::PrefixId makePrefixId(
    const std::initializer_list<uint32_t> quartets) {
  packsim::thrift::PrefixId id;
  id.reserve(quartets.size());
  for (const uint32_t quartet : quartets) {
    id.emplace_back(folly::to<int32_t>(quartet));
  }
  return id;
}

std::string nameAccel(
    const uint32_t zone,
    const uint32_t rack,
    const uint32_t host,
    const uint32_t accel) {
  return fmt::format("Accel_z{}_r{}_h{}_a{}", zone, rack, host, accel);
}

std::string nameRtsw(const uint32_t zone, const uint32_t rack) {
  return fmt::format("RTSW_z{}_r{}", zone, rack);
}

std::string
nameCtsw(const uint32_t zone, const uint32_t plane, const uint32_t ctsw) {
  return fmt::format("CTSW_z{}_p{}_c{}", zone, plane, ctsw);
}

std::string nameAtsw(const uint32_t plane, const uint32_t atsw) {
  return fmt::format("ATSW_p{}_a{}", plane, atsw);
}

std::unordered_map<std::string, packsim::thrift::Node> buildAccelerators(
    const uint32_t zones,
    const uint32_t racksPerZone,
    const uint32_t hostsPerRack,
    const uint32_t accelPerHost) {
  std::unordered_map<std::string, packsim::thrift::Node> accels;
  for (uint32_t z = 0; z < zones; z++) {
    for (uint32_t r = 0; r < racksPerZone; r++) {
      for (uint32_t h = 0; h < hostsPerRack; h++) {
        for (uint32_t a = 0; a < accelPerHost; a++) {
          packsim::thrift::Node node;
          const auto name = nameAccel(z, r, h, a);
          node.name() = name;
          node.id() = makePrefixId({kAccelLayer, z, r, h, a});
          node.role() = "ACCEL";
          node.type() = packsim::thrift::NodeType::ACCEL;
          packsim::thrift::EndpointConfig epCfg;
          node.endpointCfgs()->emplace_back(epCfg);
          accels[name] = node;
        }
      }
    }
  }
  return accels;
}

std::unordered_map<std::string, packsim::thrift::Node> buildRtsws(
    const uint32_t zones,
    const uint32_t racksPerZone) {
  std::unordered_map<std::string, packsim::thrift::Node> rtsws;
  for (uint32_t z = 0; z < zones; z++) {
    for (uint32_t r = 0; r < racksPerZone; r++) {
      packsim::thrift::Node node;
      const auto name = nameRtsw(z, r);
      node.name() = name;
      node.id() = makePrefixId({kRtswLayer, z, r, 0, 1});
      node.role() = "RTSW";
      node.type() = packsim::thrift::NodeType::SWITCH;
      rtsws[name] = node;
    }
  }
  return rtsws;
}

std::unordered_map<std::string, packsim::thrift::Node> buildCtsws(
    const uint32_t zones,
    const uint32_t planes,
    const uint32_t ctswsPerPlanePerZone) {
  std::unordered_map<std::string, packsim::thrift::Node> ctsws;
  for (uint32_t z = 0; z < zones; z++) {
    for (uint32_t p = 0; p < planes; p++) {
      for (uint32_t c = 0; c < ctswsPerPlanePerZone; c++) {
        packsim::thrift::Node node;
        const auto name = nameCtsw(z, p, c);
        node.name() = name;
        node.id() = makePrefixId({kCtswLayer, z, p, c, 1});
        node.role() = "CTSW";
        node.type() = packsim::thrift::NodeType::SWITCH;
        ctsws[name] = node;
      }
    }
  }
  return ctsws;
}

std::unordered_map<std::string, packsim::thrift::Node> buildAtsws(
    const uint32_t planes,
    const uint32_t atswsPerPlane) {
  std::unordered_map<std::string, packsim::thrift::Node> atsws;
  for (uint32_t p = 0; p < planes; p++) {
    for (uint32_t a = 0; a < atswsPerPlane; a++) {
      packsim::thrift::Node node;
      const auto name = nameAtsw(p, a);
      node.name() = name;
      node.id() = makePrefixId({kAtswLayer, p, a, 0, 1});
      node.role() = "ATSW";
      node.type() = packsim::thrift::NodeType::SWITCH;
      atsws[name] = node;
    }
  }
  return atsws;
}

packsim::thrift::TiebreakSpec getRtswUplinkTiebreak(
    const std::string& routingStr,
    const uint32_t accelPerHost) {
  packsim::thrift::TiebreakSpec rtswUplinkTiebreak;
  packsim::thrift::EcmpTiebreakSpec rtswEcmp;
  packsim::thrift::DprTiebreakSpec rtswDpr;
  rtswDpr.dotProduct() = {0, 0, 0, accelPerHost, 1};
  rtswDpr.divScale() = 1;

  // Matched case-insensitively; the throw below still names the caller's
  // spelling.
  std::string routing = routingStr;
  folly::toUpperAscii(routing);
  if ((routing == "DPR") || (routing == "MIXED")) {
    rtswUplinkTiebreak.dpr() = rtswDpr;
  } else if (routing == "ECMP") {
    rtswUplinkTiebreak.ecmp() = rtswEcmp;
  } else {
    throw std::runtime_error(
        fmt::format(
            "Routing mode {} not supported by flex topology", routingStr));
  }
  return rtswUplinkTiebreak;
}

packsim::thrift::TiebreakSpec getCtswUplinkTiebreak(
    const std::string& routingStr,
    const uint32_t oversubRatio) {
  packsim::thrift::TiebreakSpec ctswUplinkTiebreak;
  packsim::thrift::EcmpTiebreakSpec ctswEcmp;
  packsim::thrift::DprTiebreakSpec ctswDpr;
  ctswDpr.dotProduct() = {0, 0, 1};
  ctswDpr.divScale() = oversubRatio;

  // Matched case-insensitively; the throw below still names the caller's
  // spelling.
  std::string routing = routingStr;
  folly::toUpperAscii(routing);
  if ((routing == "ECMP") || (routing == "MIXED")) {
    ctswUplinkTiebreak.ecmp() = ctswEcmp;
  } else if (routing == "DPR") {
    ctswUplinkTiebreak.dpr() = ctswDpr;
  } else {
    throw std::runtime_error(
        fmt::format(
            "Routing mode {} not supported by flex topology", routingStr));
  }
  return ctswUplinkTiebreak;
}

packsim::thrift::TiebreakSpec getAtswDownlinkTiebreak(
    const std::string& routingStr) {
  packsim::thrift::TiebreakSpec atswUplinkTiebreak;
  packsim::thrift::EcmpTiebreakSpec atswEcmp;
  packsim::thrift::ReflectionTiebreakSpec atswDpr;
  // Matched case-insensitively; the throw below still names the caller's
  // spelling.
  std::string routing = routingStr;
  folly::toUpperAscii(routing);
  if ((routing == "ECMP") || (routing == "MIXED")) {
    atswUplinkTiebreak.ecmp() = atswEcmp;
  } else if (routing == "DPR") {
    atswUplinkTiebreak.reflect() = atswDpr;
  } else {
    throw std::runtime_error(
        fmt::format(
            "Routing mode {} not supported by flex topology", routingStr));
  }
  return atswUplinkTiebreak;
}

void validateClosTopology(const ClosTopologySpec& topo, const uint32_t planes) {
  if (topo.accelPerHost == 0 || topo.accelPerRack < topo.accelPerHost ||
      planes == 0) {
    throw std::invalid_argument(
        fmt::format(
            "A Clos topology needs accelPerHost > 0, accelPerRack >= "
            "accelPerHost and planes > 0; got accelPerHost {}, accelPerRack {}, "
            "planes {}",
            topo.accelPerHost,
            topo.accelPerRack,
            planes));
  }
}

} // namespace

packsim::thrift::SwitchConfig switchParamsToConfig(
    const packsim::thrift::SwitchParams& cfg) {
  packsim::thrift::SwitchConfig switchCfg;
  uint64_t hdrmPoolPerBuff;
  uint32_t downlinkHdrmCellsPerPort;
  uint32_t uplinkHdrmCellsPerPort;

  if (cfg.isWedge100().value()) {
    return (wedge100CfgToThrift(cfg));
  }
  if (cfg.numBuffs().value() == 0) {
    throw std::invalid_argument("numBuffs must be greater than 0");
  }
  if (cfg.numUplinks().value() == 0 && cfg.numDownlinks().value() == 0) {
    throw std::invalid_argument(
        "numUplinks + numDownlinks must be greater than 0");
  }
  if (cfg.cellSizeBytes().value() == 0) {
    throw std::invalid_argument("cellSizeBytes must be greater than 0");
  }

  const uint8_t numBuffs = cfg.numBuffs().value();
  const uint32_t uplinkPorts = cfg.numUplinks().value();
  const uint32_t downlinkPorts = cfg.numDownlinks().value();
  // The ports are numbered 0 to totalPorts - 1, and a port number is uint16_t.
  // The sum is taken in 64 bits so that it cannot wrap before the check.
  const uint16_t totalPorts =
      folly::to<uint16_t>(uint64_t{uplinkPorts} + downlinkPorts);
  // Every buffer serves the same number of uplinks and of downlinks, so a
  // remainder in either group would be left without a buffer.
  if (uplinkPorts % numBuffs != 0 || downlinkPorts % numBuffs != 0) {
    throw std::invalid_argument(
        fmt::format(
            "numUplinks ({}) and numDownlinks ({}) must each be divisible by numBuffs ({})",
            uplinkPorts,
            downlinkPorts,
            numBuffs));
  }
  const uint64_t portsPerBuff = totalPorts / numBuffs;
  const uint32_t uplinkPortsPerBuff = uplinkPorts / numBuffs;
  const uint32_t downlinkPortsPerBuff = downlinkPorts / numBuffs;
  const uint32_t cellSize = cfg.cellSizeBytes().value();

  const uint64_t bytesPerBuff = cfg.bufSizeBytes().value() / numBuffs;
  const uint32_t cellsPerBuff = ceilDivide32(bytesPerBuff, cellSize);

  const uint32_t portRsvdCells =
      ceilDivide32(cfg.rsvdBytesPerPort().value(), cellSize);
  const uint32_t xonHystCells =
      ceilDivide32(cfg.xonHystBytes().value(), cellSize);
  const uint8_t numQueues = std::max<uint8_t>(cfg.numQueues().value(), 1);
  if (cfg.isLosslessPerPg().value().size() != numQueues) {
    throw std::invalid_argument(
        "Size of the array isLosslessPerPg should be equal to numQueues");
  }
  if (std::all_of(
          cfg.isLosslessPerPg().value().begin(),
          cfg.isLosslessPerPg().value().end(),
          [](bool i) { return !i; })) {
    // Pfc is disabled on all pgs
    if (cfg.hdrmPoolPerBuff().value() != 0) {
      throw std::runtime_error("Headroom Pool can't be > 0 for lossy mode");
    }
    hdrmPoolPerBuff = 0;
    switchCfg.egressAdmissionCheckEnable() = true;
    switchCfg.enableFatalOnDrop() = false;
  } else {
    downlinkHdrmCellsPerPort =
        ceilDivide32(cfg.hdrmBytesPerPortDl().value(), cellSize);
    uplinkHdrmCellsPerPort =
        ceilDivide32(cfg.hdrmBytesPerPortUl().value(), cellSize);

    hdrmPoolPerBuff = folly::divCeil(cfg.hdrmPoolPerBuff().value(), cellSize);
    if (hdrmPoolPerBuff == 0) {
      hdrmPoolPerBuff =
          uint64_t{downlinkHdrmCellsPerPort} * downlinkPortsPerBuff +
          uint64_t{uplinkHdrmCellsPerPort} * uplinkPortsPerBuff;
    }

    if (std::any_of(
            cfg.isLosslessPerPg().value().begin(),
            cfg.isLosslessPerPg().value().end(),
            [](bool i) { return !i; })) {
      // Disable fatal drop if lossless is disabled on any of the pg
      switchCfg.egressAdmissionCheckEnable() = true;
      switchCfg.enableFatalOnDrop() = false;
    }
  }

  if (hdrmPoolPerBuff + portRsvdCells * portsPerBuff > cellsPerBuff) {
    throw std::invalid_argument(
        fmt::format(
            "Headroom pool ({} cells) plus reserved cells ({} per port x {} ports) exceed the {} cells of each buffer",
            hdrmPoolPerBuff,
            portRsvdCells,
            portsPerBuff,
            cellsPerBuff));
  }
  const uint32_t hdrmPoolCells = folly::to<uint32_t>(hdrmPoolPerBuff);
  uint64_t sharedCells = cellsPerBuff;
  sharedCells -= portsPerBuff * portRsvdCells;
  sharedCells -= hdrmPoolPerBuff;
  // If dynamic sharing is disabled, statically carve the shared buffer by port
  // Per-PG limit isn't affected by this; the per-port limit will be the only
  // one in effect
  const uint32_t portSharedLimit = cfg.dynShareEn().value()
      ? kUnlimitedCells
      : static_cast<uint32_t>(sharedCells / portsPerBuff);
  const uint32_t minQSizeWred = cfg.minQSizeWRED().value();
  const uint32_t maxQSizeWred = cfg.maxQSizeWRED().value();
  const uint32_t dropPercentageWred =
      convertToWholePercentage(cfg.dropProbWRED().value(), "dropProbWRED");
  const uint32_t minQSizeEcn = cfg.minQSizeECN().value();
  const uint32_t maxQSizeEcn = cfg.maxQSizeECN().value();
  const uint32_t markPercentageEcn =
      convertToWholePercentage(cfg.markProbECN().value(), "markProbECN");

  auto& bufs = *switchCfg.bufs();
  bufs.resize(numBuffs);
  uint16_t portNum = 0;
  for (uint8_t buff = 0; buff < numBuffs; buff++) {
    auto& bufCfg = bufs.at(buff);
    bufCfg.cellSizeBytes() = cellSize;
    bufCfg.totalCells() = cellsPerBuff;
    bufCfg.headroomPoolCells() = hdrmPoolCells;

    for (uint32_t buffPort = 0; buffPort < portsPerBuff;
         buffPort++, portNum++) {
      // Each buffer's first downlinkPortsPerBuff ports are downlinks, e.g.
      // 0-7 and 16-23 for two buffers of 16 ports with 8 downlinks each.
      const bool downlink = buffPort < downlinkPortsPerBuff;
      const std::string group = downlink ? "downlinks" : "uplinks";
      switchCfg.portGroups()[group].emplace_back(portNum);
      switchCfg.portToBufMap()->singleMap()[portNum] = buff;

      auto& portCfg = bufCfg.ports()[portNum];
      portCfg.port() = portNum;
      portCfg.portRsvdCells() = portRsvdCells;
      portCfg.portMaxSharedCells() = portSharedLimit;
      portCfg.xonHystCells() = xonHystCells;

      portCfg.pgs()->resize(numQueues);
      portCfg.queueCfg()->resize(numQueues);
      for (uint8_t q = 0; q < numQueues; q++) {
        auto& pg = portCfg.pgs()->at(q);
        pg.priority() = q;
        pg.rsvdCells() = 0U; // Allocated reserved cells to the port
        pg.canUsePortRsvd() = true;
        pg.dynamicSharing() = cfg.dynShareEn().value();
        pg.alpha() = cfg.alpha().value();
        pg.maxSharedCells() = kUnlimitedCells;
        if (cfg.isLosslessPerPg().value()[q]) {
          pg.maxHdrmCells() =
              downlink ? downlinkHdrmCellsPerPort : uplinkHdrmCellsPerPort;
          pg.lossless() = true;
          pg.pfc() = true;
        } else {
          pg.maxHdrmCells() = 0U;
          pg.lossless() = false;
          pg.pfc() = false;
        }

        pg.xonHystCells() = xonHystCells;
        auto& queueCfg = portCfg.queueCfg()->at(q);
        queueCfg.wredEcnCfg()->queueProfileCfg() =
            cfg.queueProfileType().value();
        queueCfg.wredEcnCfg()->minQSizeWRED() = minQSizeWred;
        queueCfg.wredEcnCfg()->maxQSizeWRED() = maxQSizeWred;
        queueCfg.wredEcnCfg()->dropRateWRED() = dropPercentageWred;
        queueCfg.wredEcnCfg()->minQSizeECN() = minQSizeEcn;
        queueCfg.wredEcnCfg()->maxQSizeECN() = maxQSizeEcn;
        queueCfg.wredEcnCfg()->markRateECN() = markPercentageEcn;
        queueCfg.wredEcnCfg()->exp() = cfg.exponentialfactorQAvg().value();
        queueCfg.wredEcnCfg()->averagingPeriod() =
            cfg.avgRefreshPeriodUs().value();
      }

      bufCfg.ports()[portNum].queueScheduleCfg() =
          cfg.queueSchedulerType().value();

      // Setup DCSP(i) to Cos(i)
      bufCfg.ports()[portNum].queueFilterCfg()->fieldToQueue() =
          dscpToCosMapDefault;
    }

    if (std::any_of(
            cfg.isLosslessPerPg().value().begin(),
            cfg.isLosslessPerPg().value().end(),
            [](bool i) { return !i; })) {
      for (uint16_t egrPortNum = 0; egrPortNum < totalPorts; egrPortNum++) {
        auto isEgrOnlyPortConfig = false;
        if (!bufCfg.ports()->contains(egrPortNum)) {
          isEgrOnlyPortConfig = true;
        }
        auto& portCfg = bufCfg.ports()[egrPortNum];
        portCfg.ucQueues()->resize(numQueues);
        portCfg.isEgrPort() = true;
        if (isEgrOnlyPortConfig) {
          portCfg.port() = egrPortNum;
          portCfg.isIngrPort() = false;
        }
        for (uint8_t q = 0; q < numQueues; q++) {
          // egress side
          auto& egrCfg = portCfg.ucQueues()->at(q);
          egrCfg.cos() = q;
          egrCfg.rsvdCells() =
              ceilDivide32(cfg.rsvdBytesPerEgrQueue().value(), cellSize);
          egrCfg.dynamicSharing() = cfg.egrDynShareEn().value();
          egrCfg.alpha() = cfg.egrAlpha().value();
          egrCfg.maxSharedCells() = kUnlimitedCells;
          egrCfg.resumeHystCells() =
              ceilDivide32(cfg.egrResumeHystBytes().value(), cellSize);
        }
      }
    }
  }
  switchCfg.pipelineDelayNs() = cfg.pipelineDelayNs().value();
  switchCfg.expDelayNs() = cfg.expDelayNs().value();
  return switchCfg;
}

packsim::thrift::SwitchConfig wedge100CfgToThrift(
    const packsim::thrift::SwitchParams& cfg) {
  packsim::thrift::SwitchConfig switchCfg;

  if (std::any_of(
          cfg.isLosslessPerPg().value().begin(),
          cfg.isLosslessPerPg().value().end(),
          [](bool i) { return i; })) {
    throw std::runtime_error("Wedge100 lossless mode not supported yet!!");
  }

  // The port map below splits ingress ports two ways and egress ports two
  // ways, one buffer per combination.
  if (cfg.numBuffs().value() != 4) {
    throw std::invalid_argument("wedge100 requires numBuffs == 4");
  }
  if (cfg.cellSizeBytes().value() == 0) {
    throw std::invalid_argument("cellSizeBytes must be greater than 0");
  }

  const uint8_t numBuffs = cfg.numBuffs().value();
  const uint32_t uplinkPorts = cfg.numUplinks().value();
  const uint32_t downlinkPorts = cfg.numDownlinks().value();
  // The sum is taken in 64 bits, as in switchParamsToConfig, so that it cannot
  // wrap.
  const uint64_t totalPorts = uint64_t{uplinkPorts} + downlinkPorts;
  // Per ingress half, not per buffer: the ingress ports split two ways, each
  // half feeding two of the four buffers (see the port map below).
  const uint64_t portsPerBuff = totalPorts / 2;
  const uint32_t uplinkPortsPerBuff = uplinkPorts / 2;
  const uint32_t downlinkPortsPerBuff = downlinkPorts / 2;
  if (uplinkPortsPerBuff + downlinkPortsPerBuff != portsPerBuff) {
    throw std::runtime_error(
        "Some port count does not split evenly between the two ingress halves");
  }
  const uint32_t cellSize = cfg.cellSizeBytes().value();

  const uint64_t bytesPerBuff = cfg.bufSizeBytes().value() / numBuffs;
  const uint32_t cellsPerBuff = ceilDivide32(bytesPerBuff, cellSize);

  const uint32_t pgRsvdCells =
      ceilDivide32(cfg.rsvdBytesPerPortPg().value(), cellSize);

  auto& bufs = *switchCfg.bufs();
  bufs.resize(numBuffs);

  const uint8_t numQueues = cfg.numQueues().value();
  const uint32_t minQSizeWred = cfg.minQSizeWRED().value();
  const uint32_t maxQSizeWred = cfg.maxQSizeWRED().value();
  const uint32_t dropPercentageWred =
      convertToWholePercentage(cfg.dropProbWRED().value(), "dropProbWRED");
  const uint32_t minQSizeEcn = cfg.minQSizeECN().value();
  const uint32_t maxQSizeEcn = cfg.maxQSizeECN().value();
  const uint32_t markPercentageEcn =
      convertToWholePercentage(cfg.markProbECN().value(), "markProbECN");

  // generate port vector for each buffer
  std::map<uint8_t, std::vector<uint16_t>> ingrPortPerBuff;
  // Egress port numbers keep the uint32_t type of the thrift port fields.
  std::map<uint8_t, std::vector<uint32_t>> egrPortPerBuff;
  std::vector<uint16_t> uplinkPortNums = {
      8, 9, 10, 11, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31};

  switchCfg.useEgress() = true;
  switchCfg.useIngress() = true;
  switchCfg.egressAdmissionCheckEnable() = true;
  switchCfg.enableFatalOnDrop() = false;

  for (uint8_t buff = 0; buff < numBuffs; buff++) {
    if (buff < 2) {
      auto egrPortStart = buff * 64U;
      auto egrPortEnd = egrPortStart + 64;
      for (uint16_t i = 0; i < 32; i++) {
        ingrPortPerBuff[buff].emplace_back(i);
        for (uint32_t j = egrPortStart; j < egrPortEnd; j++) {
          auto portPairKey = fmt::format("{}.{}", i, j);
          switchCfg.portToBufMap()->pairMap()[portPairKey] = buff;
        }
      }
      for (uint16_t i = 96; i < 128; i++) {
        ingrPortPerBuff[buff].emplace_back(i);
        for (uint32_t j = egrPortStart; j < egrPortEnd; j++) {
          auto portPairKey = fmt::format("{}.{}", i, j);
          switchCfg.portToBufMap()->pairMap()[portPairKey] = buff;
        }
      }
      for (uint32_t j = egrPortStart; j < egrPortEnd; j++) {
        egrPortPerBuff[buff].emplace_back(j);
      }
    } else {
      auto egrPortStart = (buff - 2U) * 64U;
      auto egrPortEnd = egrPortStart + 64;
      for (uint16_t i = 32; i < 96; i++) {
        ingrPortPerBuff[buff].emplace_back(i);
        for (uint32_t j = egrPortStart; j < egrPortEnd; j++) {
          auto portPairKey = fmt::format("{}.{}", i, j);
          switchCfg.portToBufMap()->pairMap()[portPairKey] = buff;
        }
      }
      for (uint32_t j = egrPortStart; j < egrPortEnd; j++) {
        egrPortPerBuff[buff].emplace_back(j);
      }
    }
  }

  for (uint16_t p = 0; p < 32; p++) {
    if (std::count(uplinkPortNums.begin(), uplinkPortNums.end(), p)) {
      switchCfg.portGroups()["uplinks"].emplace_back(p);
    } else {
      switchCfg.portGroups()["downlinks"].emplace_back(p);
    }
  }

  for (uint8_t buff = 0; buff < 4; buff++) {
    auto& bufCfg = bufs.at(buff);
    bufCfg.cellSizeBytes() = cellSize;
    bufCfg.totalCells() = cellsPerBuff;
    bufCfg.headroomPoolCells() = 0U; // lossy setup

    for (size_t idx = 0; idx < ingrPortPerBuff[buff].size(); idx++) {
      const uint16_t portNum = ingrPortPerBuff[buff].at(idx);

      packsim::thrift::PortBufferConfig pbc;
      pbc.port() = portNum;
      pbc.portRsvdCells() = 0U;
      pbc.portMaxSharedCells() = kUnlimitedCells;
      pbc.xonHystCells() = 0U;
      pbc.isIngrPort() = true;
      pbc.isEgrPort() = false;

      if (!(numQueues > 0 &&
            numQueues == cfg.isLosslessPerPg().value().size())) {
        throw std::runtime_error("Bad check");
      }
      pbc.pgs()->resize(numQueues);
      pbc.queueCfg()->resize(numQueues);

      for (uint8_t q = 0; q < numQueues; q++) {
        auto& pg = pbc.pgs()->at(q);
        pg.priority() = q;
        // Allocated reserved cells to the port,pg
        pg.rsvdCells() = pgRsvdCells;
        pg.canUsePortRsvd() = false;
        pg.dynamicSharing() = cfg.dynShareEn().value();
        pg.alpha() = cfg.alpha().value();
        pg.maxSharedCells() = kUnlimitedCells;
        pg.maxHdrmCells() = 0U;
        pg.xonHystCells() = 0U;
        pg.lossless() = cfg.isLosslessPerPg().value().at(q);
        pg.pfc() = false;
        auto& queueCfg = pbc.queueCfg()->at(q);
        queueCfg.wredEcnCfg()->queueProfileCfg() =
            cfg.queueProfileType().value();
        queueCfg.wredEcnCfg()->minQSizeWRED() = minQSizeWred;
        queueCfg.wredEcnCfg()->maxQSizeWRED() = maxQSizeWred;
        queueCfg.wredEcnCfg()->dropRateWRED() = dropPercentageWred;
        queueCfg.wredEcnCfg()->minQSizeECN() = minQSizeEcn;
        queueCfg.wredEcnCfg()->maxQSizeECN() = maxQSizeEcn;
        queueCfg.wredEcnCfg()->markRateECN() = markPercentageEcn;
        queueCfg.wredEcnCfg()->exp() = cfg.exponentialfactorQAvg().value();
        queueCfg.wredEcnCfg()->averagingPeriod() =
            cfg.avgRefreshPeriodUs().value();
      }

      bufCfg.ports()[portNum].queueScheduleCfg() =
          cfg.queueSchedulerType().value();

      // Setup DCSP(i) to Cos(i)
      bufCfg.ports()[portNum].queueFilterCfg()->fieldToQueue() =
          dscpToCosMapDefault;
      bufCfg.ports()[portNum] = pbc;
    }

    for (size_t idx = 0; idx < egrPortPerBuff[buff].size(); idx++) {
      packsim::thrift::PortBufferConfig pbcEgr;
      auto portNum = egrPortPerBuff[buff].at(idx);
      pbcEgr.queueCfg()->resize(numQueues);
      for (uint8_t que = 0; que < numQueues; que++) {
        packsim::thrift::EgressQueueBufferConfig egrCfg;
        // egress side
        egrCfg.cos() = que;
        egrCfg.rsvdCells() =
            ceilDivide32(cfg.rsvdBytesPerEgrQueue().value(), cellSize);
        egrCfg.dynamicSharing() = cfg.egrDynShareEn().value();
        egrCfg.alpha() = cfg.egrAlpha().value();
        egrCfg.maxSharedCells() = kUnlimitedCells;
        egrCfg.resumeHystCells() =
            ceilDivide32(cfg.egrResumeHystBytes().value(), cellSize);
        pbcEgr.ucQueues()->emplace_back(std::move(egrCfg));
        auto& queueCfg = pbcEgr.queueCfg()->at(que);
        queueCfg.wredEcnCfg()->queueProfileCfg() =
            cfg.queueProfileType().value();
        queueCfg.wredEcnCfg()->minQSizeWRED() = minQSizeWred;
        queueCfg.wredEcnCfg()->maxQSizeWRED() = maxQSizeWred;
        queueCfg.wredEcnCfg()->dropRateWRED() = dropPercentageWred;
        queueCfg.wredEcnCfg()->minQSizeECN() = minQSizeEcn;
        queueCfg.wredEcnCfg()->maxQSizeECN() = maxQSizeEcn;
        queueCfg.wredEcnCfg()->markRateECN() = markPercentageEcn;
        queueCfg.wredEcnCfg()->exp() = cfg.exponentialfactorQAvg().value();
        queueCfg.wredEcnCfg()->averagingPeriod() =
            cfg.avgRefreshPeriodUs().value();
      }

      if (!bufCfg.ports()->contains(portNum)) {
        pbcEgr.port() = portNum;
        pbcEgr.isEgrPort() = true;
        pbcEgr.isIngrPort() = false;
        bufCfg.ports()[portNum] = std::move(pbcEgr);
      } else {
        auto& pbc = bufCfg.ports()[portNum];
        pbc.isEgrPort() = true;
        pbc.ucQueues() = *pbcEgr.ucQueues();
      }
      bufCfg.ports()[portNum].queueScheduleCfg() =
          cfg.queueSchedulerType().value();

      // Setup DCSP(i) to Cos(i)
      bufCfg.ports()[portNum].queueFilterCfg()->fieldToQueue() =
          dscpToCosMapDefault;
    }
  }
  return switchCfg;
}

packsim::thrift::Simulation addFlexTopology(
    const packsim::thrift::Simulation& oldSim,
    const ClosLinks& links,
    const ClosTopologySpec& topo,
    const std::string& routingStr,
    const uint32_t planes) {
  validateClosTopology(topo, planes);
  packsim::thrift::Simulation sim = oldSim;
  sim.flex_topology() = packsim::thrift::FlexTopology();

  const uint32_t zones = topo.zones;
  const uint32_t hostsPerRack = topo.accelPerRack / topo.accelPerHost;
  const uint32_t accelPerHost = topo.accelPerHost;
  const uint32_t racksPerZone = topo.accelPerZone / hostsPerRack / accelPerHost;

  const uint32_t atswsPerPlane = topo.numAtsws / planes;
  const uint32_t ctswsPerPlanePerZone = topo.numCtsws / planes;

  const uint32_t rtswCtswLinks = links.rtswToCtsw.numLinks;
  const uint32_t ctswAtswLinks = links.ctswToAtsw.numLinks;

  // `FlexLinkDelayConfig.delayNs` is `i64` on the wire; `folly::to` rejects a
  // delay above `INT64_MAX`.
  const uint64_t accelToRtswDelayNs =
      static_cast<uint64_t>(folly::to<int64_t>(links.accelToRtsw.delayNs));
  const uint64_t rtswToCtswDelayNs =
      static_cast<uint64_t>(folly::to<int64_t>(links.rtswToCtsw.delayNs));
  const uint64_t ctswToAtswDelayNs =
      static_cast<uint64_t>(folly::to<int64_t>(links.ctswToAtsw.delayNs));

  auto accels =
      buildAccelerators(zones, racksPerZone, hostsPerRack, accelPerHost);
  auto rtsws = buildRtsws(zones, racksPerZone);
  auto ctsws = buildCtsws(zones, planes, ctswsPerPlanePerZone);
  auto atsws = buildAtsws(planes, atswsPerPlane);

  // Global link ID counter
  uint32_t linkId = 0;
  // Connect accelerators to RTSWs
  for (uint32_t z = 0; z < zones; z++) {
    for (uint32_t r = 0; r < racksPerZone; r++) {
      for (uint32_t h = 0; h < hostsPerRack; h++) {
        for (uint32_t a = 0; a < accelPerHost; a++) {
          { // Have the scale-out link in its own block to prevent accidental
            // variable reuse
            auto name1 = nameAccel(z, r, h, a);
            auto name2 = nameRtsw(z, r);

            auto& accel = accels.at(name1);
            auto& rtsw = rtsws.at(name2);

            packsim::thrift::FlexLink link;
            link.linkId() = linkId;
            link.node1() = name1;
            link.node2() = name2;
            link.node1PortGroup() = "uplinks";
            link.node2PortGroup() = "downlinks";
            link.speed() = links.accelToRtsw.speed;
            auto linkDelayConfig = packsim::thrift::FlexLinkDelayConfig();
            *linkDelayConfig.delayNs() = accelToRtswDelayNs;
            *link.delayConfig() = linkDelayConfig;

            accel.lpmSpec()->defaultRoute()->links()->emplace_back(linkId);
            accel.lpmSpec()->defaultRoute()->tiebreak()->ecmp() =
                packsim::thrift::EcmpTiebreakSpec();

            auto& epCfg = accel.endpointCfgs()->at(0);
            epCfg.linkIds()->emplace_back(linkId);
            epCfg.nicIds()->emplace_back(*accel.id());
            epCfg.lpmSpec()->defaultRoute()->links()->emplace_back(linkId);
            epCfg.lpmSpec()->defaultRoute()->tiebreak()->ecmp() =
                packsim::thrift::EcmpTiebreakSpec();

            auto& table = *rtsw.lpmSpec()->prefixTable();
            auto route = packsim::makePrefixKey(*accel.id());
            table[route].links()->emplace_back(linkId);
            table[route].tiebreak()->ecmp() =
                packsim::thrift::EcmpTiebreakSpec();

            rtsw.lpmSpec()->validPrefixLengths() = {kAccelIdLength};

            sim.flex_topology()->links()->emplace(linkId, link);

            linkId++;
          }

          for (uint32_t dst = a + 1; dst < accelPerHost; dst++) {
            auto name1 = nameAccel(z, r, h, a);
            auto name2 = nameAccel(z, r, h, dst);
            auto& accel1 = accels.at(name1);
            auto& accel2 = accels.at(name2);

            packsim::thrift::FlexLink link;
            link.linkId() = linkId;
            link.node1() = name1;
            link.node2() = name2;
            link.node1PortGroup() = "uplinks";
            link.node2PortGroup() = "uplinks";
            link.speed() = links.accelToRtsw.speed;
            auto linkDelayConfig = packsim::thrift::FlexLinkDelayConfig();
            *linkDelayConfig.delayNs() = accelToRtswDelayNs;
            *link.delayConfig() = linkDelayConfig;

            accel1.endpointCfgs()->at(0).nicIds()->emplace_back(*accel1.id());
            accel1.endpointCfgs()->at(0).linkIds()->emplace_back(linkId);
            accel2.endpointCfgs()->at(0).nicIds()->emplace_back(*accel2.id());
            accel2.endpointCfgs()->at(0).linkIds()->emplace_back(linkId);

            auto& table1 = *accel1.lpmSpec()->prefixTable();
            auto& table2 = *accel2.lpmSpec()->prefixTable();

            table1[packsim::makePrefixKey(*accel2.id())].links()->emplace_back(
                linkId);
            table1[packsim::makePrefixKey(*accel2.id())].tiebreak()->ecmp() =
                packsim::thrift::EcmpTiebreakSpec();
            table2[packsim::makePrefixKey(*accel1.id())].links()->emplace_back(
                linkId);
            table2[packsim::makePrefixKey(*accel1.id())].tiebreak()->ecmp() =
                packsim::thrift::EcmpTiebreakSpec();

            accel1.lpmSpec()->validPrefixLengths() = {kAccelIdLength};
            accel2.lpmSpec()->validPrefixLengths() = {kAccelIdLength};

            sim.flex_topology()->links()->emplace(linkId, link);

            linkId++;
          }
        }
      }
    }
  }

  // Connect RTSWs to CTSWs
  const auto rtswUplinkTiebreak =
      getRtswUplinkTiebreak(routingStr, accelPerHost);
  for (uint32_t z = 0; z < zones; z++) {
    for (uint32_t p = 0; p < planes; p++) {
      for (uint32_t c = 0; c < ctswsPerPlanePerZone; c++) {
        for (uint32_t r = 0; r < racksPerZone; r++) {
          for (uint32_t l = 0; l < rtswCtswLinks; l++) {
            auto name1 = nameRtsw(z, r);
            auto name2 = nameCtsw(z, p, c);

            auto& rtsw = rtsws.at(name1);
            auto& ctsw = ctsws.at(name2);

            packsim::thrift::FlexLink link;
            link.linkId() = linkId;
            link.node1() = name1;
            link.node2() = name2;
            link.node1PortGroup() = "uplinks";
            link.node2PortGroup() = "downlinks";
            link.speed() = links.rtswToCtsw.speed;
            auto linkDelayConfig = packsim::thrift::FlexLinkDelayConfig();
            *linkDelayConfig.delayNs() = rtswToCtswDelayNs;
            *link.delayConfig() = linkDelayConfig;

            rtsw.lpmSpec()->defaultRoute()->links()->emplace_back(linkId);
            rtsw.lpmSpec()->defaultRoute()->tiebreak() = rtswUplinkTiebreak;

            auto route = packsim::makePrefixKey(makePrefixId({1, z, r}));
            auto& table = *ctsw.lpmSpec()->prefixTable();
            table[route].links()->emplace_back(linkId);
            table[route].tiebreak()->ecmp() =
                packsim::thrift::EcmpTiebreakSpec();
            ctsw.lpmSpec()->validPrefixLengths() = {3};

            sim.flex_topology()->links()->emplace(linkId, link);
            linkId++;
          }
        }
      }
    }
  }

  // Connect CTSWs to ATSWs
  const uint32_t totalCtswUplinks = ctswAtswLinks * atswsPerPlane;
  // `DprTiebreakSpec.divScale` is `i32` on the wire; `folly::to` rejects a
  // ratio above `INT32_MAX`.
  const uint32_t oversubRatio = totalCtswUplinks > 0
      ? static_cast<uint32_t>(std::max(
            1, folly::to<int32_t>(topo.nominalRacksPerZone / totalCtswUplinks)))
      : std::numeric_limits<uint32_t>::max();
  const auto ctswUplinkTiebreak =
      getCtswUplinkTiebreak(routingStr, oversubRatio);
  const auto atswDownlinkTiebreak = getAtswDownlinkTiebreak(routingStr);
  for (uint32_t p = 0; p < planes; p++) {
    for (uint32_t a = 0; a < atswsPerPlane; a++) {
      for (uint32_t z = 0; z < zones; z++) {
        for (uint32_t c = 0; c < ctswsPerPlanePerZone; c++) {
          for (uint32_t l = 0; l < ctswAtswLinks; l++) {
            auto name1 = nameCtsw(z, p, c);
            auto name2 = nameAtsw(p, a);

            auto& ctsw = ctsws.at(name1);
            auto& atsw = atsws.at(name2);

            packsim::thrift::FlexLink link;
            link.linkId() = linkId;
            link.node1() = name1;
            link.node2() = name2;
            link.node1PortGroup() = "uplinks";
            link.node2PortGroup() = "downlinks";
            link.speed() = links.ctswToAtsw.speed;
            auto linkDelayConfig = packsim::thrift::FlexLinkDelayConfig();
            *linkDelayConfig.delayNs() = ctswToAtswDelayNs;
            *link.delayConfig() = linkDelayConfig;

            ctsw.lpmSpec()->defaultRoute()->links()->emplace_back(linkId);
            ctsw.lpmSpec()->defaultRoute()->tiebreak() = ctswUplinkTiebreak;

            auto route = packsim::makePrefixKey(makePrefixId({1, z}));
            auto& table = *atsw.lpmSpec()->prefixTable();
            table[route].links()->emplace_back(linkId);
            table[route].tiebreak() = atswDownlinkTiebreak;

            atsw.lpmSpec()->validPrefixLengths() = {2};

            sim.flex_topology()->links()->emplace(linkId, link);
            linkId++;
          }
        }
      }
    }
  }

  for (const auto& pair : accels) {
    sim.flex_topology()->nodes()->emplace(pair);
  }

  for (const auto& pair : rtsws) {
    sim.flex_topology()->nodes()->emplace(pair);
  }

  for (const auto& pair : ctsws) {
    sim.flex_topology()->nodes()->emplace(pair);
  }

  for (const auto& pair : atsws) {
    sim.flex_topology()->nodes()->emplace(pair);
  }

  const auto nodeCount = sim.flex_topology()->nodes()->size();
  const auto linkCount = sim.flex_topology()->links()->size();

  XLOGF(
      DBG0,
      "Constructed flex topology with {} nodes and {} links",
      nodeCount,
      linkCount);

  return sim;
}

ClosSpinePorts wiredSpinePorts(
    const ClosLinks& links,
    const ClosTopologySpec& topo,
    const uint32_t planes) {
  validateClosTopology(topo, planes);
  // The same integer arithmetic `addFlexTopology` sizes its loops with.
  const uint32_t hostsPerRack = topo.accelPerRack / topo.accelPerHost;
  const uint32_t racksPerZone =
      topo.accelPerZone / hostsPerRack / topo.accelPerHost;
  const uint32_t atswsPerPlane = topo.numAtsws / planes;
  const uint32_t ctswsPerPlanePerZone = topo.numCtsws / planes;
  return ClosSpinePorts{
      .ctsw =
          {.uplinks = atswsPerPlane * links.ctswToAtsw.numLinks,
           .downlinks = racksPerZone * links.rtswToCtsw.numLinks},
      .atsw =
          {.uplinks = 0,
           .downlinks =
               topo.zones * ctswsPerPlanePerZone * links.ctswToAtsw.numLinks},
  };
}

} // namespace packsim
