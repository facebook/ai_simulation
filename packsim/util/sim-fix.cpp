/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/sim-fix.h"

#include <iterator>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/logging/xlog.h>

#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/if/gen-cpp2/topology_constants.h"
#include "packsim/if/gen-cpp2/workload_types.h"
// @oss-disable
#include "packsim/workloads/builder.h"

using packsim::workload::thrift::Workload;
using packsim::workload::thrift::WorkloadSpecificConfig;

namespace packsim {

namespace {

// Build a RankPair from a source and destination rank.
packsim::thrift::RankPair makeRankPair(
    const uint32_t srcRank,
    const uint32_t dstRank) {
  packsim::thrift::RankPair rankPair;
  rankPair.srcRank() = srcRank;
  rankPair.dstRank() = dstRank;
  return rankPair;
}

// Build a NicPair from a source and destination NIC index.
packsim::thrift::NicPair makeNicPair(
    const uint32_t srcNicIdx,
    const uint32_t dstNicIdx) {
  packsim::thrift::NicPair nicPair;
  nicPair.srcNicIdx() = srcNicIdx;
  nicPair.dstNicIdx() = dstNicIdx;
  return nicPair;
}

template <typename T>
T makeRailConfigFromConfig(const T& conf, const std::string& workloadName) {
  const uint64_t sizeBytes = *conf.sizeBytes();
  const uint32_t numRails = *conf.numRails();
  if (numRails == 0) {
    throw std::runtime_error(
        fmt::format(
            "Workload {}: numRails must be at least 1, got 0", workloadName));
  }
  if (sizeBytes % numRails != 0) {
    throw std::runtime_error(
        fmt::format(
            "Workload {}: cannot spread {} bytes evenly over {} rails",
            workloadName,
            sizeBytes,
            numRails));
  }

  T ret = conf;
  ret.numRails() = 1;
  ret.sizeBytes() = sizeBytes / numRails;
  return ret;
}

/**
 * Split a workload's collective config across its rails.
 *
 * @param w  The workload.
 *
 * @return   The rail count and the per-rail config of a rail-splittable
 *           workload (AllGather, ReduceScatter or AllReduce), or std::nullopt
 *           for a workload type that is not split across rails.
 * @throw    std::runtime_error if numRails is 0 or the size does not divide
 *           evenly over the rails.
 */
std::optional<std::pair<uint32_t, WorkloadSpecificConfig>>
railConfigForWorkload(const Workload& w) {
  WorkloadSpecificConfig railConfig;
  switch (w.config()->getType()) {
    case WorkloadSpecificConfig::Type::allGather:
      railConfig.allGather() =
          makeRailConfigFromConfig(*w.config()->allGather(), *w.name());
      return std::make_pair(
          *w.config()->allGather()->numRails(), std::move(railConfig));
    case WorkloadSpecificConfig::Type::reduceScatter:
      railConfig.reduceScatter() =
          makeRailConfigFromConfig(*w.config()->reduceScatter(), *w.name());
      return std::make_pair(
          *w.config()->reduceScatter()->numRails(), std::move(railConfig));
    case WorkloadSpecificConfig::Type::allReduce:
      railConfig.allReduce() =
          makeRailConfigFromConfig(*w.config()->allReduce(), *w.name());
      return std::make_pair(
          *w.config()->allReduce()->numRails(), std::move(railConfig));
    case WorkloadSpecificConfig::Type::__EMPTY__:
    case WorkloadSpecificConfig::Type::allToAll:
    case WorkloadSpecificConfig::Type::incast:
    case WorkloadSpecificConfig::Type::permutation:
    // @oss-disable
    case WorkloadSpecificConfig::Type::batch:
    // @oss-disable
    // @oss-disable
    // @oss-disable
    case WorkloadSpecificConfig::Type::compute:
      break;
  }
  return std::nullopt;
}

// Parameters describing how to build one rail's sub-workload.
struct RailSplitPlan {
  // Total number of rails the source workload is split into.
  uint32_t numRails;
  // Number of ranks in each rail (numEndpoints / numRails).
  uint32_t numRailRanks;
  // Per-rail workload config (1/numRails of the original transfer size).
  WorkloadSpecificConfig railConfig;
};

// Collect the queue-pair templates that belong to one rail, renumbered to
// rail-local ranks.
//
// Templates are keyed by global rank, but a rail sub-workload's rankToId holds
// only its own ranks, so copying the list wholesale would trip the rank
// presence check in buildNicQPNMapFromQpTemplates(). The rail split assigns
// globalRank = railRank * numRails + rail, so a global rank sits on this rail
// iff rank % numRails == rail, and its rail-local index is rank / numRails.
//
// Both endpoints must be on this rail: a template spanning two rails describes
// a connection the split does not produce.
std::vector<packsim::thrift::QueuePairTemplate> buildRailQpTemplates(
    const Workload& w,
    const RailSplitPlan& plan,
    const uint32_t rail) {
  const uint32_t numRails = plan.numRails;
  std::vector<packsim::thrift::QueuePairTemplate> railTemplates;
  for (const auto& qpTemplate : *w.qpTemplates()) {
    const uint32_t srcRank = *qpTemplate.ranks()->srcRank();
    const uint32_t dstRank = *qpTemplate.ranks()->dstRank();
    if (srcRank % numRails != rail || dstRank % numRails != rail) {
      continue;
    }
    auto& railTemplate = railTemplates.emplace_back(qpTemplate);
    railTemplate.ranks()->srcRank() = srcRank / numRails;
    railTemplate.ranks()->dstRank() = dstRank / numRails;
  }
  return railTemplates;
}

// Copy the nicQpnPairMap entries between two ranks on this rail into railW,
// renumbered as in buildRailQpTemplates.
void copyRailNicQpnPairMap(
    const Workload& w,
    const RailSplitPlan& plan,
    const uint32_t rail,
    Workload& railW) {
  const uint32_t numRails = plan.numRails;
  for (const auto& [rankPair, nicPairs] : *w.nicQpnPairMap()) {
    const uint32_t srcRank = *rankPair.srcRank();
    const uint32_t dstRank = *rankPair.dstRank();
    if (srcRank % numRails != rail || dstRank % numRails != rail) {
      continue;
    }
    railW.nicQpnPairMap()->emplace(
        makeRankPair(srcRank / numRails, dstRank / numRails), nicPairs);
  }
}

// Copy the legacy qpnPairMap entries between two ranks on this rail into
// railW, renumbered as in buildRailQpTemplates.
void copyRailQpnPairMap(
    const Workload& w,
    const RailSplitPlan& plan,
    const uint32_t rail,
    Workload& railW) {
  const uint32_t numRails = plan.numRails;
  for (const auto& [srcRank, peers] : *w.qpnPairMap()) {
    if (srcRank % numRails != rail) {
      continue;
    }
    for (const auto& [dstRank, qpnPairs] : peers) {
      if (dstRank % numRails != rail) {
        continue;
      }
      (*railW.qpnPairMap())[srcRank / numRails][dstRank / numRails] = qpnPairs;
    }
  }
}

// The local QPNs of every QPN pair in w's nicQpnPairMap and qpnPairMap.
folly::F14FastSet<uint32_t> collectLocalQpns(const Workload& w) {
  folly::F14FastSet<uint32_t> localQpns;
  for (const auto& [rankPair, nicPairs] : *w.nicQpnPairMap()) {
    for (const auto& [nicPair, qpnPairs] : nicPairs) {
      for (const auto& qpnPair : qpnPairs) {
        localQpns.insert(*qpnPair.local());
      }
    }
  }
  for (const auto& [srcRank, peers] : *w.qpnPairMap()) {
    for (const auto& [dstRank, qpnPairs] : peers) {
      for (const auto& qpnPair : qpnPairs) {
        localQpns.insert(*qpnPair.local());
      }
    }
  }
  return localQpns;
}

// Copy a supplied nicQpnPairMap and qpnPairMap into one rail, keeping the
// entries between its own ranks and the qpnPortConfigs of their local QPNs.
// An entry spanning two rails describes a connection the split does not
// produce.
void copyRailQpnMaps(
    const Workload& w,
    const RailSplitPlan& plan,
    const uint32_t rail,
    Workload& railW) {
  copyRailNicQpnPairMap(w, plan, rail, railW);
  copyRailQpnPairMap(w, plan, rail, railW);
  const auto localQpns = collectLocalQpns(railW);
  for (const auto& [qpn, portConfig] : *w.qpnPortConfigs()) {
    if (localQpns.contains(qpn)) {
      railW.qpnPortConfigs()->emplace(qpn, portConfig);
    }
  }
}

// Build the sub-workload for a single rail, remapping global ranks to
// rail-local ranks via globalRank = railRank * numRails + rail.
Workload buildRailWorkload(
    const Workload& w,
    const RailSplitPlan& plan,
    const uint32_t rail) {
  Workload railW;
  railW.name() = fmt::format("{}-rail{}", *w.name(), rail);
  railW.numEndpoints() = plan.numRailRanks;
  railW.sortRanks() = *w.sortRanks();
  railW.startTimeNs() = *w.startTimeNs();
  railW.maxStaggeredStartTimeNs() = *w.maxStaggeredStartTimeNs();
  railW.config() = plan.railConfig;
  railW.metadata() = *w.metadata();
  railW.qpTemplates() = buildRailQpTemplates(w, plan, rail);
  copyRailQpnMaps(w, plan, rail, railW);
  if (w.channelSettings().has_value()) {
    railW.channelSettings() = w.channelSettings().value();
  }

  const bool hasExplicitStaggeredStartTimeNs =
      w.explicitStaggeredStartTimeNs()->size() > 0;
  for (uint32_t railRank = 0; railRank < plan.numRailRanks; railRank++) {
    const uint32_t globalRank = railRank * plan.numRails + rail;
    railW.rankToId()->try_emplace(railRank, w.rankToId()->at(globalRank));
    // rankToId is accel-level, but a caller may supply rankToIdList with the
    // per-NIC device ids. Dropping it lets fixRankToIdList() regenerate the
    // list from rankToId, which silently loses the NIC component and leaves
    // every destination unreachable.
    const auto rankToIdListIt = w.rankToIdList()->find(globalRank);
    if (rankToIdListIt != w.rankToIdList()->end()) {
      railW.rankToIdList()->try_emplace(railRank, rankToIdListIt->second);
    }
    if (hasExplicitStaggeredStartTimeNs) {
      railW.explicitStaggeredStartTimeNs()->emplace_back(
          w.explicitStaggeredStartTimeNs()->at(globalRank));
    }
    if (w.rankToNvDomain().has_value()) {
      const auto it = w.rankToNvDomain()->find(globalRank);
      if (it != w.rankToNvDomain()->end()) {
        railW.rankToNvDomain().ensure()[railRank] = it->second;
      }
    }
  }
  return railW;
}

// One direction of a queue-pair template's mapping: the rank/NIC endpoints,
// the local/remote QPNs, and the port config to store under the local QPN.
struct DirectionalQpnEntry {
  packsim::thrift::RankPair rankPair;
  packsim::thrift::NicPair nicPair;
  uint32_t localQpn;
  uint32_t remoteQpn;
  packsim::thrift::QpnPortConfig portConfig;
};

// Insert one directional QPN mapping into the workload's nicQpnPairMap and
// qpnPortConfigs (the latter keyed by the local QPN).
void insertDirectionalQpnEntry(
    packsim::workload::thrift::Workload& workload,
    DirectionalQpnEntry entry) {
  packsim::workload::thrift::QPNPair qpnPair;
  qpnPair.local() = entry.localQpn;
  qpnPair.remote() = entry.remoteQpn;
  (*workload.nicQpnPairMap())[entry.rankPair][entry.nicPair].emplace_back(
      std::move(qpnPair));
  (*workload.qpnPortConfigs())[entry.localQpn] = std::move(entry.portConfig);
}

// Throw if either endpoint rank of a template is absent from rankToId.
void validateTemplateRanksPresent(
    const packsim::workload::thrift::Workload& workload,
    const uint32_t srcRank,
    const uint32_t dstRank) {
  const auto& rankToId = *workload.rankToId();
  if (!rankToId.contains(srcRank)) {
    throw std::runtime_error(
        fmt::format(
            "Failed to find source rank {} in workload {}",
            srcRank,
            *workload.name()));
  } else if (!rankToId.contains(dstRank)) {
    throw std::runtime_error(
        fmt::format(
            "Failed to find destination rank {} in workload {}",
            dstRank,
            *workload.name()));
  }
}

// Assign a globally-unique forward+reverse QPN pair for one queue-pair template
// and record both directions in the workload's NIC-QPN maps.
void addQpTemplateToNicMaps(
    packsim::workload::thrift::Workload& workload,
    const packsim::thrift::QueuePairTemplate& qpTemplate,
    uint32_t& nextQpn) {
  const uint32_t srcRank = *qpTemplate.ranks()->srcRank();
  const uint32_t dstRank = *qpTemplate.ranks()->dstRank();
  const uint32_t srcNicIdx = *qpTemplate.nics()->srcNicIdx();
  const uint32_t dstNicIdx = *qpTemplate.nics()->dstNicIdx();

  validateTemplateRanksPresent(workload, srcRank, dstRank);

  // QPNs must be unique across all templates to avoid port-config collisions in
  // qpnPortConfigs, which is keyed only by QPN number.
  const uint32_t srcQpn = ++nextQpn;
  const uint32_t dstQpn = ++nextQpn;

  const auto& srcPorts = *qpTemplate.ports()->srcPorts();
  const auto& dstPorts = *qpTemplate.ports()->dstPorts();

  XLOGF(
      DBG,
      "nicQpnMap: QP template srcRank={} dstRank={} srcNicIdx={} dstNicIdx={} -> srcQpn={} dstQpn={} srcPorts=[{}] dstPorts=[{}]",
      srcRank,
      dstRank,
      srcNicIdx,
      dstNicIdx,
      srcQpn,
      dstQpn,
      fmt::join(srcPorts, ","),
      fmt::join(dstPorts, ","));

  // Forward src -> dst: the local (src) QPN exposes src ports as srcPorts.
  packsim::thrift::QpnPortConfig forwardPortConfig;
  forwardPortConfig.srcPorts() = srcPorts;
  forwardPortConfig.dstPorts() = dstPorts;
  insertDirectionalQpnEntry(
      workload,
      {.rankPair = makeRankPair(srcRank, dstRank),
       .nicPair = makeNicPair(srcNicIdx, dstNicIdx),
       .localQpn = srcQpn,
       .remoteQpn = dstQpn,
       .portConfig = std::move(forwardPortConfig)});

  // Reverse dst -> src: from the destination's view, src/dst ports are swapped.
  packsim::thrift::QpnPortConfig reversePortConfig;
  reversePortConfig.srcPorts() = dstPorts;
  reversePortConfig.dstPorts() = srcPorts;
  insertDirectionalQpnEntry(
      workload,
      {.rankPair = makeRankPair(dstRank, srcRank),
       .nicPair = makeNicPair(dstNicIdx, srcNicIdx),
       .localQpn = dstQpn,
       .remoteQpn = srcQpn,
       .portConfig = std::move(reversePortConfig)});
}

// Log a summary of the nicQpnPairMap built for a workload (rank/NIC/QPN pair
// counts and port-config count).
void logNicQpnMapSummary(
    const packsim::workload::thrift::Workload& workload,
    const uint32_t nextQpn) {
  const size_t numRankPairs = workload.nicQpnPairMap()->size();
  size_t totalNicPairs = 0;
  size_t totalQpnPairs = 0;
  for (const auto& [rankPair, nicMap] : *workload.nicQpnPairMap()) {
    totalNicPairs += nicMap.size();
    for (const auto& [nicPair, qpnList] : nicMap) {
      totalQpnPairs += qpnList.size();
    }
  }

  XLOGF(
      INFO,
      "Built nicQpnPairMap for workload {}: {} qpTemplates -> nextQpn={}, "
      "{} rank pairs, {} NIC pairs, {} QPN pairs, {} port configs",
      workload.name().value(),
      workload.qpTemplates()->size(),
      nextQpn,
      numRankPairs,
      totalNicPairs,
      totalQpnPairs,
      workload.qpnPortConfigs()->size());
}

// A config may leave `nicConfigMap` empty and set only `nic`; an empty map gets
// `nic` as its "default" entry.
void fixNicCfg(packsim::thrift::Simulation& simCfg) {
  if (simCfg.nicConfigMap()->size() == 0) {
    simCfg.nicConfigMap()
        [packsim::thrift::topology_constants::kDefaultNicConfig()] =
        *simCfg.nic();
  }
}

/**
 * Check that every rank key in `w`'s rankToId and rankToIdList is below its
 * numEndpoints.
 *
 * Ranks index per-rank arrays sized by numEndpoints, such as the rows and
 * columns of AllToAll.vectorizedSizesBytes, so a rank at or above numEndpoints
 * reads past their end.
 *
 * @param w  Workload to check.
 *
 * @throw std::invalid_argument  If a rankToId or rankToIdList key of `w` is not
 *                               below its numEndpoints.
 */
void validateRanksBelowNumEndpoints(const Workload& w) {
  const uint32_t numEndpoints = *w.numEndpoints();
  const auto validateRanks = [&](const auto& rankMap,
                                 const std::string& fieldName) {
    for (const auto& [rank, _] : rankMap) {
      if (rank >= numEndpoints) {
        throw std::invalid_argument(
            fmt::format(
                "Workload {}: {} has rank {}, but numEndpoints is {}; every "
                "rank must be below numEndpoints",
                *w.name(),
                fieldName,
                rank,
                numEndpoints));
      }
    }
  };
  validateRanks(*w.rankToId(), "rankToId");
  validateRanks(*w.rankToIdList(), "rankToIdList");
}

void fixRankToIdList(packsim::thrift::Simulation& simCfg) {
  for (auto& w : *simCfg.newWorkloadSchedule()->workloads()) {
    if (w.rankToIdList()->empty()) {
      for (auto& [rank, id] : *w.rankToId()) {
        w.rankToIdList()->insert({rank, {id}});
      }
    }
  }
}

/**
 * Populate a permutation workload's dstRanks with a random single cycle.
 *
 * The rank-based permutation requires a populated dstRanks array, which a
 * config may omit; a workload that already carries one is left alone.
 *
 * Degenerate endpoint counts are not derangements: a count of 1 yields the
 * self-mapping `{0}`, and a count of 0 yields an empty list that the
 * already-populated check reads as unpopulated on any later fixup pass.
 *
 * @param w       Permutation workload to fix up, modified in place.
 * @param random  Source of the shuffle.
 */
void fixOnePermutationWorkload(Workload& w, Random& random) {
  auto& perm = *w.config()->permutation();
  const uint32_t numAccel = *w.numEndpoints();
  if (perm.dstRanks()->size() != 0) {
    // Don't overwrite existing dstRanks
    return;
  }

  std::vector<uint32_t> ranks(numAccel);
  std::iota(ranks.begin(), ranks.end(), 0U);
  random.shuffle(ranks.begin(), ranks.end());

  auto dstRanks = perm.dstRanks();
  dstRanks->resize(numAccel);
  // `ranks` is a permutation of 0..numAccel-1, so the loop assigns every slot.
  for (uint32_t i = 0; i < numAccel; i++) {
    dstRanks[ranks[i]] = ranks[(i + 1) % numAccel];
  }
}

void fixOneBatchWorkload(Workload& w, Random& random) {
  auto& batch = *w.config()->batch();
  std::vector<Workload> newChildren;
  for (auto& child : *batch.children()) {
    // Children of a batch workload are expected to use the parent's rankToId
    // map; copy it into any child that leaves it empty.
    if (child.rankToId()->size() == 0) {
      child.rankToId() = *w.rankToId();
    }
    // Recursively make sure all children are fixed as well
    for (auto& c : fixOneWorkload(child, random)) {
      newChildren.emplace_back(std::move(c));
    }
  }

  uint32_t childId = 1;
  for (auto& c : newChildren) {
    c.workloadId() = childId++;
  }
  batch.children() = newChildren;
}

// Split a single workload configured for N rails into N workloads of 1/Nth the
// size on N different rails. We do this by rebuilding the rankToId map for each
// workload. Currently AllGather, ReduceScatter, and AllReduce are supported.
std::vector<Workload> splitWorkloadToRails(const Workload& w) {
  auto railCfg = railConfigForWorkload(w);
  if (!railCfg.has_value()) {
    // Workload type is not rail-split; return it unchanged.
    return {w};
  }
  auto [numRails, railConfig] = std::move(*railCfg);

  const auto numRanks = *w.numEndpoints();
  if ((numRanks % numRails) != 0) {
    throw std::runtime_error(
        fmt::format(
            "Workload {}: cannot split {} ranks evenly on {} rails",
            *w.name(),
            numRanks,
            numRails));
  }
  // @oss-disable

  const RailSplitPlan plan{
      .numRails = numRails,
      .numRailRanks = numRanks / numRails,
      .railConfig = std::move(railConfig)};

  std::vector<Workload> railWorkloads;
  railWorkloads.reserve(numRails);
  for (uint32_t rail = 0; rail < numRails; rail++) {
    railWorkloads.emplace_back(buildRailWorkload(w, plan, rail));
  }

  return railWorkloads;
}

void checkSimCfg(const packsim::thrift::Simulation& simCfg) {
  if (*(simCfg.nic()->outofOrderPktSupport())) {
    if ((*simCfg.nic()->pktDropCfg()->dropPktType()) !=
        packsim::thrift::NicEgrPktDropType::NO_DROP) {
      throw std::runtime_error(
          "Error in Simulation Config. outofOrderPktSupport is set and Nic is configured for drops");
    }
    // Switch-side enableFatalOnDrop is deliberately left unchecked:
    // switchParamsToConfig() in config-builders.cpp clears it for any switch
    // carrying a lossy PG, so requiring it here would reject those topologies.
    // Recovering a switch drop is then the transport's job, and with OOO on
    // RoCEv2 repairs a PSN gap only when
    // GoBackNLrConfig.selectiveGapRepair is set: otherwise
    // GoBackNRecovery::checkPsn accepts every PSN without a gap NACK and no
    // retransmit timer runs, so neither resends the dropped packet. A lost data
    // packet leaves its message incomplete at the receiver, and a lost last
    // packet or ACK leaves it incomplete at the sender.
  }
}

/**
 * Check that a workload with channelSettings takes its notify QPs from the same
 * numbering as its channel QPs. Channel QPs come from qpnPairMap, while
 * notifies use nicQpnPairMap, which qpTemplates or a supplied map number
 * separately, so the two can name the same QP. Runs before buildQPNMap, while
 * an empty qpnPairMap still means none was supplied.
 *
 * @param simCfg  The simulation config to check.
 *
 * @throw std::invalid_argument  If a workload with channelSettings sets
 *                               qpTemplates, or supplies a nicQpnPairMap
 *                               without a qpnPairMap.
 */
void validateChannelQpSources(const packsim::thrift::Simulation& simCfg) {
  for (const auto& wkld : *simCfg.newWorkloadSchedule()->workloads()) {
    if (!wkld.channelSettings().has_value()) {
      continue;
    }
    const bool suppliesNicMapWithoutQpnMap =
        !wkld.nicQpnPairMap()->empty() && wkld.qpnPairMap()->empty();
    if (!wkld.qpTemplates()->empty() || suppliesNicMapWithoutQpnMap) {
      throw std::invalid_argument(
          fmt::format(
              "Workload {}: qpTemplates or a nicQpnPairMap supplied without its qpnPairMap cannot be combined with channel settings: channel QPs come from qpnPairMap, numbered separately, so they can be the notify QPs",
              *wkld.name()));
    }
  }
}

/**
 * Check that every rank pair of every workload keeps a QP for notifies alone.
 * A notify takes the last QP of the rank pair's first NIC pair, and data uses
 * that QP too when it is the only one there.
 *
 * @param workloads  The workloads, with nicQpnPairMap built.
 *
 * @throw std::invalid_argument  If a rank pair has fewer than two QPs on its
 *                               first NIC pair.
 */
void validateNotifyQpsAreSeparate(
    const std::vector<packsim::workload::thrift::Workload>& workloads) {
  for (const auto& wkld : workloads) {
    for (const auto& [rankPair, qpnsByNicPair] : *wkld.nicQpnPairMap()) {
      if (qpnsByNicPair.empty()) {
        continue;
      }
      // NicPairToQPNList is a std::map, so begin() is the lowest
      // (srcNicIdx, dstNicIdx): the entry WorkloadRank::getNotifyQPNs takes the
      // notify QP from.
      const auto& [nicPair, qpnPairs] = *qpnsByNicPair.begin();
      if (qpnPairs.size() < 2) {
        throw std::invalid_argument(
            fmt::format(
                "Workload {}: rank pair ({}, {}) has {} QP(s) on its first NIC pair ({}, {}), where notifies need a QP of their own: add another qpTemplate for this pair, for example the reverse direction",
                *wkld.name(),
                *rankPair.srcRank(),
                *rankPair.dstRank(),
                qpnPairs.size(),
                *nicPair.srcNicIdx(),
                *nicPair.dstNicIdx()));
      }
    }
  }
}

} // namespace

// Separate function to fix a single workload function so we can call it
// recursively when fixing batch workloads
std::vector<Workload> fixOneWorkload(Workload& w, Random& random) {
  validateRanksBelowNumEndpoints(w);
  switch (w.config()->getType()) {
    case WorkloadSpecificConfig::Type::permutation:
      fixOnePermutationWorkload(w, random);
      break;
    case WorkloadSpecificConfig::Type::batch:
      fixOneBatchWorkload(w, random);
      break;
    case WorkloadSpecificConfig::Type::__EMPTY__:
    case WorkloadSpecificConfig::Type::allToAll:
    case WorkloadSpecificConfig::Type::allReduce:
    case WorkloadSpecificConfig::Type::incast:
    case WorkloadSpecificConfig::Type::reduceScatter:
    case WorkloadSpecificConfig::Type::allGather:
    // @oss-disable
    // @oss-disable
    // @oss-disable
    // @oss-disable
    case WorkloadSpecificConfig::Type::compute:
      break;
  }
  return splitWorkloadToRails(w);
}

void validateAndFixWorkloads(
    packsim::thrift::Simulation& simCfg,
    Random& random) {
  auto& workloadNameInfos = *simCfg.newWorkloadSchedule()->workloadNameInfos();
  folly::F14FastSet<std::string> originalWkldNames;
  originalWkldNames.reserve(simCfg.newWorkloadSchedule()->workloads()->size());
  std::vector<Workload> newWorkloads;
  uint32_t workloadId{0};
  for (auto& w : *simCfg.newWorkloadSchedule()->workloads()) {
    if (w.name()->empty()) {
      throw std::runtime_error("Workload name cannot be empty.");
    }
    if (originalWkldNames.contains(*w.name())) {
      throw std::runtime_error(
          fmt::format("Duplicate input workload name: {}", *w.name()));
    }
    originalWkldNames.emplace(*w.name());
    for (auto& newW : fixOneWorkload(w, random)) {
      newW.workloadId() = workloadId++;
      auto& newWkldInfos = workloadNameInfos[*newW.name()];
      newWkldInfos.internalWorkloadId() = *newW.workloadId();
      newWkldInfos.originalWorkloadName() = *w.name();
      newWorkloads.emplace_back(std::move(newW));
    }
  }
  simCfg.newWorkloadSchedule()->workloads() = newWorkloads;
}

uint32_t getNumQPs(const packsim::workload::thrift::Workload& workload) {
  // default 2 (1 for data, 1 for notify)
  uint32_t numQPs = 2;
  if (workload.channelSettings().has_value()) {
    numQPs = workload.channelSettings()->numChannels().value() *
            workload.channelSettings()->numQPsPerChannel().value() +
        1; // + 1 for notify QP
  }
  return numQPs;
}

// Returns map of wkldId -> map of srcRank -> map of dstRank -> vector of
// {srcQP, dstQP} pairs
void buildQPNMap(std::vector<packsim::workload::thrift::Workload>& workloads) {
  folly::F14FastMap<std::vector<uint32_t>, uint32_t> nextQpId;
  for (size_t wkldIdx = 0; wkldIdx < workloads.size(); wkldIdx++) {
    auto& wkld = workloads.at(wkldIdx);
    if (!wkld.qpnPairMap()->empty()) {
      // Don't overwrite existing QPN map
      XLOGF(
          INFO, "qpnPairMap already set for workload {}", wkld.name().value());
      continue;
    }
    if (wkldIdx != *wkld.workloadId()) {
      throw std::runtime_error("WorkloadID mismatch");
    }
    // @oss-disable
        // @oss-disable
      // @oss-disable
      // @oss-disable
    // @oss-disable

    const uint32_t numQPs = getNumQPs(wkld);
    // Other XPUs this XPU connects
    const auto rankPeers = getRankPeers(wkld);
    std::unordered_set<std::tuple<uint32_t, uint32_t>> genPairs;
    for (uint32_t rank1 = 0; rank1 < rankPeers.size(); rank1++) {
      for (const auto rank2 : rankPeers[rank1]) {
        // Ordered pair
        const auto pair = rank1 < rank2 ? std::make_tuple(rank1, rank2)
                                        : std::make_tuple(rank2, rank1);
        if (genPairs.contains(pair)) {
          // Don't generate more than 1 set of QPs per pair of ranks for this
          // workload
          continue;
        }
        genPairs.emplace(pair);
        const auto& id1Vals = wkld.rankToId()->at(rank1);
        const auto& id2Vals = wkld.rankToId()->at(rank2);
        for (uint32_t _i = 0; _i < numQPs; _i++) {
          packsim::workload::thrift::QPNPair pair1, pair2;
          // Prefix increment to start from 1 on first visit to ID
          const uint32_t qpn1 = ++nextQpId[id1Vals];
          const uint32_t qpn2 = ++nextQpId[id2Vals];
          pair1.local() = qpn1;
          pair1.remote() = qpn2;
          pair2.local() = qpn2;
          pair2.remote() = qpn1;
          wkld.qpnPairMap()[rank1][rank2].emplace_back(std::move(pair1));
          wkld.qpnPairMap()[rank2][rank1].emplace_back(std::move(pair2));
        }
      }
    }
  }
}

void buildNicQPNMapFromQpTemplates(
    packsim::workload::thrift::Workload& workload,
    uint32_t& nextQpn) {
  if (workload.qpTemplates()->empty()) {
    return;
  }

  XLOGF(
      DBG,
      "Building nicQpnPairMap from {} qpTemplates for workload {}",
      workload.qpTemplates()->size(),
      workload.name().value());

  // Clear existing maps
  workload.nicQpnPairMap()->clear();
  workload.qpnPortConfigs()->clear();

  for (const auto& qpTemplate : *workload.qpTemplates()) {
    addQpTemplateToNicMaps(workload, qpTemplate, nextQpn);
  }

  logNicQpnMapSummary(workload, nextQpn);
}

void buildNicQPNMapFromLegacy(packsim::workload::thrift::Workload& workload) {
  if (workload.qpnPairMap()->empty()) {
    return;
  }

  // Clear existing nicQpnPairMap
  workload.nicQpnPairMap()->clear();

  // Convert legacy qpnPairMap to nicQpnPairMap
  // All queue pairs are assumed to be on NIC 0 for both src and dst
  constexpr uint32_t kDefaultNicIdx = 0;

  for (const auto& [srcRank, peerMap] : *workload.qpnPairMap()) {
    for (const auto& [dstRank, qpnPairs] : peerMap) {
      for (const auto& qpnPair : qpnPairs) {
        packsim::workload::thrift::QPNPair newPair;
        newPair.local() = *qpnPair.local();
        newPair.remote() = *qpnPair.remote();

        // {srcRank, dstRank} -> {srcNicIdx, dstNicIdx} -> list<QPNPair>
        packsim::thrift::RankPair rankPair;
        rankPair.srcRank() = srcRank;
        rankPair.dstRank() = dstRank;

        packsim::thrift::NicPair nicPair;
        nicPair.srcNicIdx() = kDefaultNicIdx;
        nicPair.dstNicIdx() = kDefaultNicIdx;

        (*workload.nicQpnPairMap())[rankPair][nicPair].emplace_back(
            std::move(newPair));
      }
    }
  }

  XLOGF(
      INFO,
      "Built nicQpnPairMap from legacy qpnPairMap for workload {}",
      workload.name().value());
}

void buildNicQPNMap(
    std::vector<packsim::workload::thrift::Workload>& workloads,
    uint32_t& nextQpn) {
  for (auto& wkld : workloads) {
    // @oss-disable
    // @oss-disable
      // @oss-disable
    // @oss-disable

    // Skip if nicQpnPairMap is already populated
    if (!wkld.nicQpnPairMap()->empty()) {
      XLOGF(
          INFO,
          "nicQpnPairMap already set for workload {}",
          wkld.name().value());
      continue;
    }

    // Prefer qpTemplates when the workload config supplies them
    if (!wkld.qpTemplates()->empty()) {
      buildNicQPNMapFromQpTemplates(wkld, nextQpn);
      // Clear qpTemplates after building the maps to reduce result size.
      // The templates are no longer needed after nicQpnPairMap and
      // qpnPortConfigs are built.
      wkld.qpTemplates()->clear();
    } else {
      // Fall back to legacy qpnPairMap
      // Legacy data structure qpnPairMap already has QPNs assigned, so
      // we will build the nicQpnPairMap with these already provided QPNs
      buildNicQPNMapFromLegacy(wkld);
    }
  }
}

void fixupSimCfg(packsim::thrift::Simulation& simCfg, Random& random) {
  fixNicCfg(simCfg);
  validateAndFixWorkloads(simCfg, random);
  validateChannelQpSources(simCfg);
  // @oss-disable
  buildQPNMap(*simCfg.newWorkloadSchedule()->workloads());
  uint32_t nextQpn = 0;
  // @oss-disable
  // @oss-disable
  // @oss-disable
      // @oss-disable
  buildNicQPNMap(*simCfg.newWorkloadSchedule()->workloads(), nextQpn);
  validateNotifyQpsAreSeparate(*simCfg.newWorkloadSchedule()->workloads());
  fixRankToIdList(simCfg);
  // @oss-disable
      // @oss-disable
  // @oss-disable
      // @oss-disable
  checkSimCfg(simCfg);
}

} // namespace packsim
