/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc-config.h"

#include <algorithm>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

#include <fmt/format.h>

#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/cbfc/cbfc-types.h"

namespace packsim::cbfc {
namespace {

// Reject a VC id outside 0..numVcs-1. `field` names the offending config field
// so every field carrying VC ids reports the same message.
void requireVcInRange(
    const uint32_t vc,
    const uint32_t numVcs,
    const std::string& field) {
  if (vc >= numVcs) {
    throw std::invalid_argument(
        fmt::format("{} has VC id {} outside 0..{}.", field, vc, numVcs - 1));
  }
}

// A port has at most `kCbfcLimits.maxVcs` VCs. Every validator below
// length-checks or indexes against `numVcs`, so this one has to run first.
void validateVcCount(const packsim::thrift::CbfcConfig& config) {
  const uint32_t numVcs = *config.numVcs();
  if (numVcs < 1 || numVcs > kCbfcLimits.maxVcs) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.numVcs ({}) is outside the supported range 1..{}.",
            numVcs,
            int{kCbfcLimits.maxVcs}));
  }
}

void validateLosslessVcs(const packsim::thrift::CbfcConfig& config) {
  const uint32_t numVcs = *config.numVcs();
  if (config.isLosslessPerVc()->size() != numVcs) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.isLosslessPerVc has {} entries but numVcs is {}; it is "
            "indexed by VC id, so it needs exactly one flag per VC.",
            config.isLosslessPerVc()->size(),
            numVcs));
  }
  if (std::find(
          config.isLosslessPerVc()->begin(),
          config.isLosslessPerVc()->end(),
          true) == config.isLosslessPerVc()->end()) {
    throw std::invalid_argument(
        "CbfcConfig.isLosslessPerVc marks no VC lossless. UEC 1.0.3 Tbl 5-27 "
        "requires 1 <= (number of lossless VCs) <= NumVCs; CBFC with none "
        "gates nothing and makes the lossless invariant vacuous.");
  }
}

void validateVcMap(const packsim::thrift::CbfcConfig& config) {
  const uint32_t numVcs = *config.numVcs();
  for (const auto& entry : *config.vcMap()) {
    requireVcInRange(*entry.vcId(), numVcs, "CbfcConfig.vcMap");
    // `mask` and `value` are matched against a 6-bit DSCP or 4-bit PCP/DEI
    // field. The width check rejects either one when it is wider than the
    // selected field.
    const uint32_t fieldMax =
        *entry.selector() == packsim::thrift::VcMapSelector::DSCP ? 0x3F : 0xF;
    for (const auto& [what, v] :
         {std::pair{"mask", *entry.mask()},
          std::pair{"value", *entry.value()}}) {
      if (v > fieldMax) {
        throw std::invalid_argument(
            fmt::format(
                "CbfcConfig.vcMap {} {} is outside 0..{} for selector {}.",
                what,
                v,
                fieldMax,
                *entry.selector() == packsim::thrift::VcMapSelector::DSCP
                    ? "DSCP"
                    : "PCP_DEI"));
      }
    }
  }
}

void validateVcMinCredits(const packsim::thrift::CbfcConfig& config) {
  const uint32_t numVcs = *config.numVcs();
  if (!config.vcMinCredits()->empty() &&
      config.vcMinCredits()->size() != numVcs) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.vcMinCredits has {} entries but numVcs is {}; it is "
            "indexed by VC id, so it is either empty or one entry per VC.",
            config.vcMinCredits()->size(),
            numVcs));
  }
}

void validateCreditSizing(const packsim::thrift::CbfcConfig& config) {
  // Range only, deliberately not set membership: UEC §5.2.10.1's
  // {32,64,...,2048} is a conformance floor on what a sender must support, not
  // a whitelist; receivers in this tree use cell sizes outside it (the switch
  // default is 254 B). The ceiling is the 12-bit S_CreditSize wire field.
  if (*config.expectedCreditSizeBytes() > kMaxCreditSizeBytes) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.expectedCreditSizeBytes ({}) is outside 0..{} (0 = derive).",
            *config.expectedCreditSizeBytes(),
            kMaxCreditSizeBytes));
  }
  if (!kCbfcLimits.isPktOvhdSupported(*config.pktOvhdBytes())) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.pktOvhdBytes ({}) is not supported: accepted values "
            "are {}..{} in steps of {} B.",
            *config.pktOvhdBytes(),
            kCbfcLimits.minPktOvhdBytes,
            kCbfcLimits.maxPktOvhdBytes,
            kCbfcLimits.pktOvhdStepBytes));
  }
}

// Once groups exist they must cover every lossless VC: an uncovered one keeps
// unrestricted access to the shared pool, defeating the isolation the groups
// provide. `grouped` is every VC id claimed by some VC group.
void requireEveryLosslessVcGrouped(
    const packsim::thrift::CbfcConfig& config,
    const std::set<uint32_t>& grouped) {
  for (uint32_t vc = 0; vc < config.isLosslessPerVc()->size(); ++vc) {
    if (config.isLosslessPerVc()->at(vc) && !grouped.contains(vc)) {
      throw std::invalid_argument(
          fmt::format(
              "CbfcConfig.isLosslessPerVc marks VC {} lossless but no VC group "
              "claims it. Once any group is configured every lossless VC must "
              "belong to exactly one.",
              vc));
    }
  }
}

void validateVcGroups(const packsim::thrift::CbfcConfig& config) {
  const uint32_t numVcs = *config.numVcs();
  // An empty vcGroups is the no-tier default and is legal.
  if (!config.vcGroups()->empty()) {
    if (config.vcGroups()->size() > kCbfcLimits.maxVcGroups) {
      throw std::invalid_argument(
          fmt::format(
              "CbfcConfig.vcGroups has {} groups; at most {} are supported.",
              config.vcGroups()->size(),
              int{kCbfcLimits.maxVcGroups}));
    }
    std::set<uint32_t> grouped;
    for (const auto& entry : *config.vcGroups()) {
      if (entry.vcIds()->size() > kCbfcLimits.maxVcsPerGroup) {
        throw std::invalid_argument(
            fmt::format(
                "CbfcConfig.vcGroups has a group with {} VCs; at most {} VCs "
                "per group are supported.",
                entry.vcIds()->size(),
                int{kCbfcLimits.maxVcsPerGroup}));
      }
      for (const uint32_t vc : *entry.vcIds()) {
        requireVcInRange(vc, numVcs, "CbfcConfig.vcGroups");
        if (!grouped.emplace(vc).second) {
          throw std::invalid_argument(
              fmt::format(
                  "CbfcConfig.vcGroups lists VC {} in more than one group.",
                  vc));
        }
      }
    }
    requireEveryLosslessVcGrouped(config, grouped);
  }
}

// Check the CF_min, CF_max and CC_msg timers against the cbfc-types.h bounds.
void validateTimers(const packsim::thrift::CbfcConfig& config) {
  if (*config.cfMinTimerBytes() < kCfMinTimerMinBytes) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.cfMinTimerBytes ({}) is below PackSim's floor of "
            "{} B of wire time.",
            *config.cfMinTimerBytes(),
            kCfMinTimerMinBytes));
  }
  // Tbl 5-27 ranges the two timers independently and does not require this,
  // but a minimum spacing above the maximum is not a sensible configuration.
  if (*config.cfMinTimerBytes() > *config.cfMaxTimerBytes()) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.cfMinTimerBytes ({}) exceeds cfMaxTimerBytes ({}). "
            "CF_min is the minimum spacing between CF_Updates and CF_max the "
            "maximum, so CF_min must not be larger.",
            *config.cfMinTimerBytes(),
            *config.cfMaxTimerBytes()));
  }

  // A zero CF_max or CC_msg spacing wedges the simulation: the timer re-arms
  // with the same delay, so the event lands on the current timestamp, the PDES
  // worker loop never reaches its next-frame boundary, and the clock freezes
  // with no diagnostic.
  if (*config.cfMaxTimerBytes() < kCfMaxTimerMinBytes ||
      *config.cfMaxTimerBytes() > kCfMaxTimerMaxBytes) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.cfMaxTimerBytes ({}) is outside the UEC 1.0.3 "
            "Tbl 5-27 range {}..{} B of wire time.",
            *config.cfMaxTimerBytes(),
            kCfMaxTimerMinBytes,
            kCfMaxTimerMaxBytes));
  }
  // Tbl 5-27 states the range with a minimum granularity of 16 kB, so a value
  // inside the range still has to land on a step of it.
  if (*config.cfMaxTimerBytes() % kCfMaxTimerMinBytes != 0) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.cfMaxTimerBytes ({}) is not a multiple of the UEC "
            "1.0.3 Tbl 5-27 minimum granularity of {} B.",
            *config.cfMaxTimerBytes(),
            kCfMaxTimerMinBytes));
  }
  if (*config.ccMsgTimerNs() < kCcMsgTimerMinNs ||
      *config.ccMsgTimerNs() > kCcMsgTimerMaxNs) {
    throw std::invalid_argument(
        fmt::format(
            "CbfcConfig.ccMsgTimerNs ({}) is outside the range {} ns (UEC "
            "1.0.3 Tbl 5-27 floor) to {} ns (PackSim ceiling).",
            *config.ccMsgTimerNs(),
            kCcMsgTimerMinNs,
            kCcMsgTimerMaxNs));
  }
}

// Reject a CBFC link config before any of it is translated. `validateVcCount`
// must stay first: the rest are written against a `numVcs` already known to be
// in range.
void validateCbfcConfig(const packsim::thrift::CbfcConfig& config) {
  validateVcCount(config);
  validateLosslessVcs(config);
  validateVcMap(config);
  validateVcMinCredits(config);
  validateCreditSizing(config);
  validateVcGroups(config);
  validateTimers(config);
}

} // namespace

CbfcConfig makeCbfcConfig(
    const packsim::thrift::CbfcConfig& config,
    const uint64_t linkBps) {
  validateCbfcConfig(config);
  CbfcConfig cfg;
  cfg.numVcs = *config.numVcs();
  cfg.losslessVc = *config.isLosslessPerVc();
  cfg.vcMap.reserve(config.vcMap()->size());
  for (const auto& entry : *config.vcMap()) {
    VcMapRule rule;
    rule.selector = (*entry.selector() == packsim::thrift::VcMapSelector::DSCP)
        ? VcSelector::DSCP
        : VcSelector::PCP_DEI;
    rule.mask = *entry.mask();
    rule.value = *entry.value();
    rule.vcId = *entry.vcId();
    cfg.vcMap.emplace_back(std::move(rule));
  }
  // 0 is the thrift "derive from the receiver" sentinel; resolve it to the
  // default credit size so the runtime config never holds 0.
  // `bootstrapCbfcCredits` in `topology/flex-topology.cpp` replaces it at sim
  // init with the receivers' declared cell size, when either end of the link
  // declares one.
  cfg.creditSizeBytes = *config.expectedCreditSizeBytes() == 0
      ? kCbfcLimits.defaultCreditSizeBytes
      : *config.expectedCreditSizeBytes();
  // `validateCreditSizing` admits only the PktOvhd values `kCbfcLimits`
  // supports, and none of them is negative.
  static_assert(kCbfcLimits.minPktOvhdBytes >= 0);
  cfg.pktOvhdBytes = static_cast<uint32_t>(*config.pktOvhdBytes());
  cfg.cfMinTimerBytes = *config.cfMinTimerBytes();
  cfg.cfMaxTimerBytes = *config.cfMaxTimerBytes();
  cfg.ccMsgTimer = Time::fromNs(*config.ccMsgTimerNs());
  cfg.ccUpdateLlrEligible = *config.ccUpdateLlrEligible();
  cfg.cfCreditThresholdCredits = *config.cfCreditThresholdCredits();
  cfg.vcMinCredits = *config.vcMinCredits();
  cfg.overshootReserveCredits = *config.overshootReserveCredits();
  // Group ids are already range-checked above.
  cfg.vcGroups.reserve(config.vcGroups()->size());
  for (const auto& entry : *config.vcGroups()) {
    VcGrpRule rule;
    rule.limitCredits = *entry.limitCredits();
    rule.vcIds = *entry.vcIds();
    cfg.vcGroups.emplace_back(std::move(rule));
  }
  cfg.linkBps = linkBps;
  return cfg;
}

} // namespace packsim::cbfc
