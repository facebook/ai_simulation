/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "packsim/if/gen-cpp2/config_types.h"

namespace packsim {

/** Uplink and downlink port counts of one switch model. */
struct SwitchPortCounts {
  uint32_t uplinks{};
  uint32_t downlinks{};
};

/**
 * The fabric, switch-buffer and NIC parameters `cfgToSim` turns into a
 * four-zone Clos simulation. The defaults are illustrative round numbers
 * chosen to satisfy the builders' structural constraints (the rack switch's
 * uplinks and downlinks fit its two buffers, and the spine rails map one to one
 * onto a rack's accelerators); they describe no deployed fabric.
 *
 * At the defaults a rack holds 4 accelerators per host x 2 hosts, each plane
 * has 4 CTSWs per zone and 2 ATSWs, and the 8-rack nominal zone spreads over
 * the spine's 4 uplinks at 2 racks each. Example: 128 accelerators build 4
 * racks per zone.
 */
struct PackSimSimConfig {
  /* General Simulation Parameters */
  int64_t snapshotIntervalNs = -1;
  packsim::thrift::TransportConfig transportConfig;

  /* Network Backend Params */
  double bswSwitchBuffSizeinMB = 128;
  double atswSwitchBuffSizeinMB = 128;
  uint64_t bswSwitchHdrmSzPerPort = 64 * 1024;
  uint64_t bswSwitchRsvdSzPerPort = 4200;
  double switchBuffSizeinMB = 32;
  // One-way propagation delay for accelerator links: both accelerator-to-RTSW
  // and intra-host accelerator-to-accelerator links.
  uint64_t n2SwDlyNs = 500;
  // One-way propagation delay for switch-to-switch links (RTSW-to-CTSW and
  // CTSW-to-ATSW).
  uint64_t sw2SwDlyNs = 1000;
  // Large enough to hold a 4096B message
  uint32_t mtub = 4200;
  // Units are bytes
  uint32_t switchBuffCellSize = 254;
  // ingAlpha=5 -> Alpha is actually set to 0.25
  uint8_t ingAlpha = 5;
  std::string strRouting = "DPR";
  double nicEfficiency = 90;
  uint32_t nicXon = 128 * 1024;
  uint32_t nicXoff = 192 * 1024;
  uint32_t nicBufferSize = 256 * 1024;
  uint64_t rswHdrmSzPerPortUl = 64 * 1024;
  uint64_t rswHdrmSzPerPortDl = 32 * 1024;
  uint64_t rswRsvdSzPerPort = 4200;
  uint64_t bswHdrmPoolPerBuff = 0;
  uint64_t rswXonHystBytes = 8 * 1024;
  uint64_t bswXonHystBytes = 8 * 1024;
  uint32_t numCtsws{8};
  uint32_t numAtsws{4};
  uint32_t numRtswCtswLinks{1};
  uint32_t numCtswAtswLinks{2};
  std::string gpuRtswLinkSpeed{"100Gbps"};
  std::string rtswCtswLinkSpeed{"100Gbps"};
  std::string ctswAtswLinkSpeed{"200Gbps"};
  uint32_t numAccelPerZone{64};
  uint32_t numAccelPerHost{4};
  uint32_t numHostsPerRack{2};
  // Zone width, in racks, that the spine (CTSW) DPR tiebreak spreads over the
  // CTSW-to-ATSW uplinks; see `ClosTopologySpec::nominalRacksPerZone`.
  uint32_t nominalRacksPerZone{8};
  // Port counts of both the CTSW and the ATSW model. Unset, each tier gets
  // exactly the ports the topology wires to it (`wiredSpinePorts`).
  std::optional<SwitchPortCounts> spinePorts;
  bool rswDynShareEn = true;
  bool bswDynShareEn = false;
  uint8_t numQueues = 1;
  uint8_t nicDataCos = 0;
  uint8_t nicAckCos = 0;

  /* Shared workload params */
  double gpuMemBWGbps = 400.0;
};

/**
 * Build the buffer, port and queue config of a switch from its parameters.
 * Each of the `numBuffs` buffers serves `numDownlinks / numBuffs` downlinks
 * and `numUplinks / numBuffs` uplinks. Example: 8 uplinks and 8 downlinks
 * over 2 buffers put 4 of each on every buffer.
 *
 * @param cfg  Switch parameters; with `isWedge100` set, the config comes from
 *             `wedge100CfgToThrift` instead.
 *
 * @return     The switch config.
 *
 * @throw std::invalid_argument On parameters it cannot lay out, e.g. a
 *        `numUplinks` or `numDownlinks` that is not a multiple of `numBuffs`.
 */
packsim::thrift::SwitchConfig switchParamsToConfig(
    const packsim::thrift::SwitchParams& cfg);

packsim::thrift::SwitchConfig wedge100CfgToThrift(
    const packsim::thrift::SwitchParams& cfg);

struct ClosLinkSpec {
  std::string speed;
  uint64_t delayNs{};
  uint32_t numLinks{1};
};

struct ClosLinks {
  ClosLinkSpec accelToRtsw;
  ClosLinkSpec rtswToCtsw;
  ClosLinkSpec ctswToAtsw;
};

struct ClosTopologySpec {
  uint32_t accelPerHost{4};
  uint32_t accelPerRack{8};
  uint32_t accelPerZone{64};
  uint32_t zones{4};
  uint32_t numCtsws{8};
  uint32_t numAtsws{4};
  // Zone width, in racks, that the spine (CTSW) DPR tiebreak spreads over the
  // CTSW-to-ATSW uplinks: each uplink carries
  // nominalRacksPerZone / (ctswToAtsw.numLinks * ATSWs per plane) consecutive
  // racks, and at least one. It may exceed the populated rack count, so a
  // partly built zone keeps the full zone's rack-to-uplink mapping. Example: 8
  // racks over 4 uplinks puts racks {0, 1} on uplink 0.
  uint32_t nominalRacksPerZone{8};
};

packsim::thrift::Simulation addFlexTopology(
    const packsim::thrift::Simulation& sim,
    const ClosLinks& links,
    const ClosTopologySpec& topo,
    const std::string& routingStr = "MIXED",
    uint32_t planes = 2);

/** The ports `addFlexTopology` wires to each CTSW and to each ATSW. */
struct ClosSpinePorts {
  SwitchPortCounts ctsw;
  // The top tier, so it has no uplinks.
  SwitchPortCounts atsw;
};

/**
 * Count the ports `addFlexTopology(sim, links, topo, routingStr, planes)`
 * wires to each CTSW and ATSW. Example: the `ClosTopologySpec` defaults over
 * 2 planes, with 1 RTSW-to-CTSW and 2 CTSW-to-ATSW links, give each CTSW 4
 * uplinks and 8 downlinks and each ATSW 32 downlinks.
 *
 * @param links   Per-tier link counts.
 * @param topo    Accelerators and switches per tier.
 * @param planes  Planes the CTSW and ATSW tiers are split across.
 *
 * @return        Uplinks and downlinks of one CTSW and of one ATSW.
 *
 * @throw std::invalid_argument On the topologies `addFlexTopology` rejects.
 */
ClosSpinePorts wiredSpinePorts(
    const ClosLinks& links,
    const ClosTopologySpec& topo,
    uint32_t planes);

} // namespace packsim
