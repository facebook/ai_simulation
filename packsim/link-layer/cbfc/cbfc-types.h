/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <vector>

#include "packsim/kernel/time.h"
#include "packsim/link-layer/cbfc/cbfc-limits.h"
// @oss-disable

namespace packsim::cbfc {

/**
 * UEC (Ultra Ethernet Consortium) 1.0.3 §5.2 value types for the CBFC
 * (Credit-Based Flow Control) module: the packet->VC mapping enum, the runtime
 * config, and the stats, as plain C++ types independent of the thrift
 * `CbfcConfig` that `makeCbfcConfig` (cbfc-config.h) translates into
 * `cbfc::CbfcConfig`. The on-wire message-type values (`CbfcCcMsgType`, the
 * CF_Update header) live with the packet definitions in `packet/base.h`.
 */

/**
 * Header field a `VcMapRule` matches on (UEC §5.2.5.2 / R_PktID_Sel
 * §5.3.2.2.15.4). Independent of the thrift `VcMapSelector`.
 */
enum class VcSelector : uint8_t {
  PCP_DEI = 0, // Match (pcpDei & mask) == value.
  DSCP = 1, // Match (dscp & mask) == value.
};

/**
 * One packet->VC mapping rule (UEC §5.2.5.2). Rules are evaluated in order;
 * the first match assigns the VC. A packet matching no rule is best-effort.
 */
struct VcMapRule {
  // The header field this rule matches on. `PCP_DEI` is 0, so a rule left at
  // its defaults is a live PCP/DEI rule rather than an unset one.
  VcSelector selector = VcSelector::PCP_DEI;
  // Mask applied to the field before comparison (4-bit PCP/DEI, 6-bit DSCP).
  // 0 is a CATCH-ALL, not "unset": `vcFor` tests `(field & mask) == value`
  // first-match-wins, so a rule left at its defaults matches every packet and
  // shadows every rule after it.
  uint32_t mask = 0;
  // Value compared against `(field & mask)`. A bit set here but not in
  // `mask` makes the rule dead: it can never match.
  uint32_t value = 0;
  // VC index assigned on match (0..numVcs-1).
  uint8_t vcId = 0;
};

/**
 * One VC group: the VCs that share a burst-absorption pool, and that pool's
 * limit in credits. The group id is this entry's index in
 * `CbfcConfig::vcGroups`. `limitCredits == 0` means the group may not draw on
 * the shared pool at all — its members are confined to their own per-VC
 * reserve (`vcMinCredits`).
 */
struct VcGrpRule {
  // VC ids in this group: at most `kCbfcLimits.maxVcsPerGroup` of them, each
  // below `CbfcConfig::numVcs`, and no VC in more than one group. The `Cbfc`
  // constructor rejects all three violations.
  std::vector<uint8_t> vcIds;
  // Ceiling, in credits, on what this group may draw from the shared pool.
  uint64_t limitCredits = 0;
};

// Default `CbfcConfig::linkBps`, used to convert the byte-denominated CF
// timers to time. `makeCbfcConfig` always overwrites it with the port's real
// rate, so this default stands only for a `CbfcConfig` built directly inside
// this module. Same value as `kLlrDefaultLinkBps800Gbps`.
inline constexpr uint64_t kCbfcDefaultLinkBps800Gbps =
    800ull * 1'000ull * 1'000ull * 1'000ull;

// UEC §5.2.5.2's ceiling on VCs per port, 1 <= NumVCs <= 32. Sizes and
// bounds-checks per-VC state, which is therefore valid for any link config;
// what a link may actually configure is checked against `kCbfcLimits` when
// its config is validated.
inline constexpr uint8_t kUecMaxVcs = 32;

// Capacity for per-group sender state. A VC is in at most one group, so no
// link can usefully have more groups than VCs.
inline constexpr uint8_t kLedgerMaxVcGroups = kUecMaxVcs;

// Validation bounds for the CBFC timers. UEC 1.0.3 Tbl 5-27 gives CF_max's
// 16 kB .. 1 MB range and 16 kB granularity, and CC_msg's 1 us floor. The
// 800 B CF_min floor and the 250 ms CC_msg ceiling are PackSim's own bounds.
inline constexpr uint64_t kCfMinTimerMinBytes = 800; // PackSim floor
inline constexpr uint64_t kCfMaxTimerMinBytes = 16ull * 1024; // "16 kB to 1 MB"
// CF_min default: the PackSim floor itself. One 8 B CF_Update CtlOS
// (Control Ordered Set) per 800 B of wire time is 8/808 ~= 1% of link
// bandwidth: inside §5.2.11.2's 2% hard cap (>= 400 B apart) but above its
// 0.5% guidance (~1600 B apart).
inline constexpr uint64_t kDefaultCfMinTimerBytes = kCfMinTimerMinBytes;
inline constexpr uint64_t kCfMaxTimerMaxBytes = 1024ull * 1024;
inline constexpr uint64_t kCcMsgTimerMinNs = 1'000; // Tbl 5-27 floor
inline constexpr uint64_t kCcMsgTimerMaxNs = 250'000'000; // PackSim ceiling

// Largest credit size expressible on the wire: the LLDP `S_CreditSize`
// field is 12 bits (UEC 1.0.3 §5.3.2.2.7).
inline constexpr uint32_t kMaxCreditSizeBytes = 4095;

// The CBFC limits link configs are validated against; see `CbfcLimits`.
// @oss-disable
inline constexpr CbfcLimits kCbfcLimits = CbfcLimits::uec(); // @oss-enable
static_assert(kCbfcLimits.maxVcs >= 1 && kCbfcLimits.maxVcs <= kUecMaxVcs);
static_assert(kCbfcLimits.maxVcGroups <= kLedgerMaxVcGroups);
static_assert(
    kCbfcLimits.maxVcGroups * kCbfcLimits.maxVcsPerGroup >= kCbfcLimits.maxVcs,
    "once any group exists every lossless VC must be groupable");
// Tbl 5-27's extracted PDF text drops a trailing digit and shows this ceiling
// as 12. The UEC prose gives -16 to +127, so do not "correct" it to 12.
static_assert(
    kCbfcLimits.pktOvhdStepBytes >= 1 && kCbfcLimits.maxPktOvhdBytes <= 127,
    "UEC 1.0.3 §5.2.10.2 caps PktOvhd at +127");
static_assert(
    kCbfcLimits.minPktOvhdBytes >= 0,
    "a negative PktOvhd charges fewer credits than PackSim's whole-frame "
    "buffers occupy");
// `makeCbfcConfig` substitutes the default for the thrift "derive" sentinel,
// and the substituted value is what reaches `S_CreditSize`, so it has to fit
// the field on its own.
static_assert(
    kCbfcLimits.defaultCreditSizeBytes >= 1 &&
        kCbfcLimits.defaultCreditSizeBytes <= kMaxCreditSizeBytes,
    "the default credit size must fit the 12-bit S_CreditSize wire field");

// The credit-size-dependent bounds live with `kMaxLostCredits` in
// cbfc-credit-tracker.h: they are a property of the R_VC_CF counter width.

/**
 * CBFC runtime configuration (UEC 1.0.3 §5.2). Translated from
 * `thrift::CbfcConfig` by `makeCbfcConfig` (cbfc-config.h). Durations use
 * PackSim's `Time` type.
 *
 * Receiver-derived parameters (TotalCredits, and CreditSize itself) are NOT
 * held here — they are read from the receiver buffer at init; see the
 * "Initialization" section of `cbfc.h` and `resolveLinkCreditSizeBytes` in
 * `topology/flex-topology.cpp`.
 */
struct CbfcConfig {
  // Number of virtual channels on the link, 1..`kCbfcLimits.maxVcs`.
  // `makeCbfcConfig` range-checks the thrift value before narrowing it into
  // this field.
  uint8_t numVcs = 1;

  // Per-VC lossless disposition, indexed by VC id. A lossless VC is
  // credit-gated; a best-effort VC bypasses the gate.
  //
  // Size must be exactly `numVcs`; the `VcMapper` constructor rejects any other
  // size, because `VcMapper::isLossless` reads false past the end and a short
  // vector would leave the VCs it omits ungated with no diagnostic.
  std::vector<bool> losslessVc{false};

  // Packet->VC mapping rules (§5.2.5.2), evaluated in order.
  std::vector<VcMapRule> vcMap;

  // Bytes per credit (§5.2.5.2 CreditSize). Never 0 here: the "derive from
  // the receiver" sentinel belongs to `thrift::CbfcConfig` and is resolved by
  // `makeCbfcConfig` before this struct is built.
  uint32_t creditSizeBytes = kCbfcLimits.defaultCreditSizeBytes;

  // Per-packet buffer overhead added before the credit roundup
  // (§5.2.5.2 PktOvhd). Only values `kCbfcLimits` accepts reach
  // here, none negative: PackSim's receive buffers store the whole frame, CRC
  // included, so the negative PktOvhd §5.2.5.2 permits for a CRC-stripping
  // receiver is not modelled.
  uint32_t pktOvhdBytes = 0;

  // CF_min_timer: minimum spacing between CF_Update CtlOS, in bytes of wire
  // time (§5.2.5.7.3). Lower bound `kCfMinTimerMinBytes`.
  uint64_t cfMinTimerBytes = kDefaultCfMinTimerBytes;

  // CF_max_timer: maximum spacing between CF_Update CtlOS, in bytes of wire
  // time (Tbl 5-27: 16 kB .. 1 MB).
  uint64_t cfMaxTimerBytes = kCfMaxTimerMinBytes;

  // CC_msg_timer: spacing between CC_Update packets (§5.2.5.6.4). Range
  // 1 us (Tbl 5-27) .. 250 ms (PackSim).
  Time ccMsgTimer = Time::fromUs(100);

  // Whether CC_Update takes an LLR sequence number and a replay-buffer entry,
  // per UEC §5.2.9's "CC_Update packets must be LLR-eligible and carry a valid
  // sequence number". False makes it LLR-ineligible, taking neither; see the
  // thrift field for why there is no middle setting.
  bool ccUpdateLlrEligible = true;

  // Optional batching gate on CF_Update generation, a PackSim extension
  // outside UEC §5.2.5.7.3. A drain only makes its VC eligible once the
  // credits freed since that VC's last report reach this many. 0, the default,
  // disables the gate, so any drain that frees credit on a lossless VC makes
  // that VC eligible immediately.
  //
  // It is a gate, not an extra trigger: it buys CtlOS bandwidth by lengthening
  // the credit loop by up to `cfCreditThresholdCredits * creditSizeBytes` of
  // un-returned buffer per VC, so keep it low for a deterministic RTT. CF_max
  // remains the unconditional backstop that reports whatever sub-threshold
  // residue a VC is holding.
  //
  // One per-link value, applied to each VC independently.
  uint64_t cfCreditThresholdCredits = 0;

  // Credits dedicated to each VC out of the pooled limit, indexed by VC id.
  // Either empty -- the default, meaning no VC has a reserve -- or one entry
  // per VC, 0 where that VC has none. A VC can always transmit into its own
  // reserve no matter how much of the shared pool its peers hold, so a backlog
  // on one lossless VC cannot stall another (§5.2.5.2).
  std::vector<uint64_t> vcMinCredits;

  // Credits withheld from the shared pool to absorb per-packet encapsulation
  // growth added after the credit gate.
  uint64_t overshootReserveCredits = 0;

  // VC group membership and per-group limits. Empty — the default — disables
  // the group tier: every lossless VC draws directly on the global shared pool.
  // Group limits are not required to fit inside the shared pool: the global
  // limit is checked as well, so they stay out of the init feasibility check
  // (`CbfcSenderLedger::reservationsFit`: Σ vcMinCredits + overshoot reserve
  // <= pool).
  std::vector<VcGrpRule> vcGroups;

  // Effective link rate (bits/sec), used to convert the byte-denominated CF
  // timers into `Time`. Overridden by the port with the real link rate.
  uint64_t linkBps = kCbfcDefaultLinkBps800Gbps;
};

/**
 * CBFC counters (UEC 1.0.3 §5.2). The owning `Cbfc` exposes a const reference
 * via `stats()`; surfaced to thrift `CbfcStats` by `Cbfc::exportThriftStats`.
 */
struct Stats {
  // Message counters.
  // CF_Update CtlOS sent (receiver role).
  uint64_t cfUpdatesSent = 0;
  // CF_Update CtlOS received (sender role).
  uint64_t cfUpdatesRcvd = 0;
  // CC_Update packets sent (sender role).
  uint64_t ccUpdatesSent = 0;
  // CC_Update packets received (receiver role).
  uint64_t ccUpdatesRcvd = 0;
  // CC_Update entries discarded for LostCredits above `kMaxLostCredits`
  // (§5.2.5.7.4's serious error). Counted per VC entry, not per packet — one
  // CC_Update carries 16 entries and can trip several.
  uint64_t ccUpdateLostCreditsOutOfRangeCount = 0;

  // Credit counters (in credits).
  // S_VC_CC increments (on transmit).
  uint64_t creditsConsumed = 0;
  // R_VC_CF increments. A managed switch queue returns the uncredited share at
  // enqueue and only the headroom at drain/drop; the NIC receive buffer returns
  // the whole cost at drain. CC_Update reclaim is counted separately, in
  // `leakedCreditsReclaimed`.
  uint64_t creditsFreed = 0;
  // Credits returned to the sender via CF_Update.
  uint64_t creditsReturned = 0;
  // Credits reclaimed by the CC_Update reconcile.
  uint64_t leakedCreditsReclaimed = 0;
  // Lossless-VC TX-gate skips for insufficient credit.
  uint64_t gateBlockedCount = 0;

  // Correctness invariant: BUFFER_FULL drops on lossless VCs. MUST stay 0.
  uint64_t losslessVcBufferFullDrops = 0;
};

} // namespace packsim::cbfc
