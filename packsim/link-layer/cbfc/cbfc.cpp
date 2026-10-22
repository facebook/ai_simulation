/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/cbfc/cbfc.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include <folly/logging/xlog.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/link-layer/cbfc/credit-counter.h"

namespace packsim::cbfc {

namespace {
// Bit masks of the CF_Update CF count fields (15 bits, UEC Tbl 5-21) and the
// CC_Update S_VC_CC fields (20 bits, Tbl 5-22). The counters written into
// these fields have the same widths, so masking never changes a value; it lets
// the compiler prove that the value fits the packet field.
constexpr uint64_t kCfCountMask = (uint64_t{1} << kFreedWidthBits) - 1;
constexpr uint64_t kSVcCcMask = (uint64_t{1} << kCreditWidthBits) - 1;
// Number of R_VC_CF reports carried in one CF_Update CtlOS (UEC Tbl 5-21).
// `CbfcCfUpdatePayload` carries exactly that many: vc1/cf1 and vc2/cf2.
constexpr int kCfReportsPerUpdate = 2;
// Every VC a link can have falls in some CC_Update range.
static_assert(
    kUecMaxVcs <= CcRangeScheduler::kMaxRanges * kCcUpdateVcsPerMsg,
    "a CC_Update range per 16 VCs must cover kUecMaxVcs");

// CC_Update ranges a link with `numVcs` VCs needs: one per 16 VCs.
uint8_t ccRangeCount(const uint8_t numVcs) {
  return (numVcs + kCcUpdateVcsPerMsg - 1) / kCcUpdateVcsPerMsg;
}

// Reject a `CbfcConfig` field that is out of range.
//
// Throws rather than `XCHECK`-aborting: an out-of-range field is a recoverable
// input error the caller can report against the offending link, and aborting
// would take the whole simulation down for one bad link. Same policy as
// `CbfcValidate::checkedNumVcs`.
[[noreturn]] void rejectConfig(const std::string& msg) {
  throw std::invalid_argument(msg);
}

// Convert a byte-denominated CF timer to wire time at the link rate.
//
// Checks `linkBps` here rather than in `validateCbfcConfig`, which never
// sees it -- `linkBps` is a constructor argument, not a thrift field. The
// constructor derives both CF spacings in its member initializer list, which
// runs before the body, and `DataRate` divides by the rate.
//
// @param config  Supplies `linkBps`.
// @param bytes   The byte count to convert.
// @return        The time `bytes` occupy on the wire at `config.linkBps`.
// @throw std::invalid_argument if `config.linkBps` is 0.
Time bytesToWireTime(const CbfcConfig& config, uint64_t bytes) {
  if (config.linkBps == 0) {
    rejectConfig("CbfcConfig.linkBps is zero");
  }
  return DataRate(config.linkBps).bytesToTime(bytes);
}

} // namespace

Cbfc::Cbfc(
    folly::not_null<SimContext*> simCtx,
    const packsim::thrift::CbfcConfig& config,
    const uint64_t linkBps)
    : Cbfc(simCtx, makeCbfcConfig(config, linkBps)) {}

Cbfc::Cbfc(folly::not_null<SimContext*> simCtx, const CbfcConfig& config)
    : simCtx_(simCtx),
      config_(config),
      mapper_(config),
      ledger_(config.numVcs),
      tracker_(config.numVcs),
      cfDirty_(config.numVcs, false),
      cfEverFreed_(config.numVcs, false),
      cfAccum_(config.numVcs, 0),
      // Neither derived spacing needs its own check: the byte floors
      // `makeCbfcConfig` enforces, `kCfMinTimerMinBytes` (PackSim) and
      // `kCfMaxTimerMinBytes` (Tbl 5-27), keep both positive at femtosecond
      // resolution for every link rate this models.
      cfMinSpacing_(bytesToWireTime(config, config.cfMinTimerBytes)),
      cfMaxSpacing_(bytesToWireTime(config, config.cfMaxTimerBytes)),
      ccRanges_(ccRangeCount(config.numVcs)),
      ccMsgTimer_(simCtx, config.ccMsgTimer),
      cfMaxTimer_(simCtx, cfMaxSpacing_) {}

Cbfc::~Cbfc() {
  shutdown();
}

void Cbfc::start() {
  if (running_) {
    return;
  }
  running_ = true;
  stopped_ = false;
  ccMsgLoop_ = ccMsgLoop();
  cfMaxLoop_ = cfMaxLoop();
}

void Cbfc::shutdown() {
  running_ = false;
  stopped_ = true;
  // A mark left standing here would be emitted on the first pull after the
  // next start(), ahead of the restarted timers. No counts are lost with it:
  // `popCcUpdate` reads S_VC_CC from the ledger at pop time, so the mark is a
  // request for a report rather than a snapshot.
  ccRanges_.clear();
  // Same for the CF_Update side. `cfEverFreed_` is deliberately NOT cleared:
  // `cfMaxLoop` keys the §5.2.5.7.3 lost-update refresh off it, so clearing
  // it would strand credit freed before the shutdown but never reported, which
  // is the one way this path can break the lossless invariant.
  std::fill(cfDirty_.begin(), cfDirty_.end(), false);
  std::fill(cfAccum_.begin(), cfAccum_.end(), 0);
  // Reset the tasks BEFORE cancelling the timers, so no frame can be resumed
  // by a timer mid-teardown.
  ccMsgLoop_.reset();
  cfMaxLoop_.reset();
  // The unwind clears each waiter but leaves the scheduled resumes, which
  // live in the timer rather than in any frame. Cancel both or the
  // `SimStopwatch` destructors trip their `!scheduledResume_.isActive()`
  // XCHECK.
  ccMsgTimer_.cancel();
  cfMaxTimer_.cancel();
}

void Cbfc::setCreditSize(uint32_t creditSizeBytes) {
  // Same rejection as the constructor; see `Cbfc::setCreditSize` in cbfc.h for
  // why an unresolved sentinel must not reach `pktCreditsFor`.
  if (creditSizeBytes == 0) {
    rejectConfig("CBFC credit size is zero");
  }
  config_.creditSizeBytes = creditSizeBytes;
}

void Cbfc::initSenderPortLimit(uint64_t totalCredits) {
  ledger_.setPortCreditLimit(totalCredits);
}

bool Cbfc::initSenderReservations() {
  // Bounded by construction: `validateVcMinCredits` in `cbfc-config.cpp`
  // rejects a `vcMinCredits` whose size is neither 0 nor `numVcs`, and it
  // cannot change afterwards.
  for (uint8_t vc = 0; vc < config_.vcMinCredits.size(); ++vc) {
    ledger_.setVcMinCredits(vc, config_.vcMinCredits[vc]);
  }
  ledger_.setOvershootReserve(config_.overshootReserveCredits);
  // An empty vcGroups leaves every VC ungrouped, which is the no-tier default.
  // Likewise bounded by construction: `validateVcGroups` in
  // `cbfc-config.cpp` rejects more than `kCbfcLimits.maxVcGroups` groups.
  for (uint8_t grp = 0; grp < config_.vcGroups.size(); ++grp) {
    const auto& entry = config_.vcGroups[grp];
    ledger_.setVcGrpLimit(grp, entry.limitCredits);
    for (const auto vc : entry.vcIds) {
      ledger_.assignVcGroup(vc, grp);
    }
  }
  return ledger_.reservationsFit();
}

std::optional<uint8_t> Cbfc::losslessVcOf(const Packet& pkt) const {
  // Only IP packets carry a DSCP; non-IP frames (control/raw) are best-effort.
  if (pkt.pktType < PacketType::IPV6) {
    return std::nullopt;
  }
  const auto& ip = static_cast<const Ipv6Packet&>(pkt);
  const uint8_t dscp = ip.getDscpVal();
  // PackSim has no explicit VLAN PCP/DEI field; the DSCP class selector
  // (dscp >> 3) is the PCP proxy (DSCP CSx == x << 3).
  const uint8_t pcpDei = dscp >> 3;
  const auto vc = mapper_.vcFor(dscp, pcpDei);
  if (vc && mapper_.isLossless(*vc)) {
    return vc;
  }
  return std::nullopt;
}

uint32_t Cbfc::pktCreditsFor(uint32_t bytes) const {
  const uint32_t effectiveBytes = bytes + config_.pktOvhdBytes;
  // `makeCbfcConfig` resolves the 0 sentinel and `setCreditSize` rejects zero;
  // the max is defensive.
  const uint32_t creditSize = std::max(config_.creditSizeBytes, 1u);
  // Ceiling division (UEC §5.2.5.6.1 roundup).
  return (effectiveBytes + creditSize - 1) / creditSize;
}

void Cbfc::onPacketStored(const Packet& pkt) {
  const auto vc = losslessVcOf(pkt);
  if (!vc) {
    return;
  }
  tracker_.onStored(*vc, pktCreditsFor(pkt.getSize()));
}

void Cbfc::onPacketDrained(const Packet& pkt) {
  // Whole-packet wrapper: the receiver held every credit until drain.
  //
  // EXCLUSIVE with `onCreditsHeld` for a given packet, which already returned
  // the uncredited share at enqueue; using both would free that share twice
  // and drive R_VC_CF past R_VC_CC. A buffer picks one path for every packet
  // it admits: the switch uses `onCreditsHeld` + `onCreditsFreed`, the NIC
  // buffer uses this.
  onCreditsFreed(pkt, pktCreditsFor(pkt.getSize()));
}

void Cbfc::onCreditsHeld(const Packet& pkt, const uint32_t heldCredits) {
  const uint32_t whole = pktCreditsFor(pkt.getSize());
  // Clamped, not asserted: an over-large held count would wrap the subtraction
  // and free credits for a packet the buffer is still holding.
  const uint32_t held = std::min(heldCredits, whole);
  onCreditsFreed(pkt, whole - held);
}

void Cbfc::onCreditsFreed(const Packet& pkt, const uint32_t credits) {
  if (credits == 0) {
    return;
  }
  const auto vc = losslessVcOf(pkt);
  if (!vc) {
    return;
  }
  tracker_.onDrained(*vc, credits);
  stats_.creditsFreed += credits;
  // Set BEFORE the accumulation gate below: a VC that never reaches the
  // threshold must still be eligible for the CF_max lost-update refresh, or it
  // holds un-returned credit forever and the gate breaks the lossless
  // invariant.
  cfEverFreed_.at(*vc) = true;
  // The optional accumulation gate; at the default threshold of 0 it
  // suppresses nothing (`unsigned < 0`). It must stay on this producer side:
  // gating inside `popCfUpdate` would also swallow `cfMaxLoop`'s re-dirty,
  // which §5.2.5.7.3 makes mandatory.
  cfAccum_.at(*vc) += credits;
  if (cfAccum_.at(*vc) < config_.cfCreditThresholdCredits) {
    return;
  }
  // A shut-down module must not dirty a VC or raise the edge: `popCfUpdate`
  // does not consult the run state, so the port could otherwise still pull a
  // report from a module whose timers are cancelled. The drain is already in
  // the tracker and `cfEverFreed_` is set, so a restart's CF_max refresh
  // reports these credits rather than losing them.
  //
  // `stopped_`, not `!running_`: a module that has never been started must
  // still accumulate and report, and `running_` does not distinguish the two.
  if (stopped_) {
    return;
  }
  cfDirty_.at(*vc) = true;
  onCtlosReady(); // let the port pull a CF_Update returning the freed credits
}

bool Cbfc::canTransmit(const Packet& pkt) const {
  // UEC 1.0.3 §5.2.9: PFC belongs to a best-effort VC, and a CC_Update is
  // configured best-effort and "does not use credits". Both are terminated by
  // the peer's MAC and never occupy an ingress buffer, so there is nothing for
  // credit to account for. Stated here rather than left to `losslessVcOf`,
  // which would also return nullopt for them -- but only as a side effect of
  // their sitting below `PacketType::IPV6`, which reads as an accident.
  if (packetIs<MacControlFrame>(pkt)) {
    return true;
  }
  const auto vc = losslessVcOf(pkt);
  if (!vc) {
    return true; // best-effort bypasses the credit gate
  }
  return ledger_.canTransmit(*vc, pktCreditsFor(pkt.getSize()));
}

void Cbfc::onTransmit(const Packet& pkt) {
  const auto vc = losslessVcOf(pkt);
  if (!vc) {
    return;
  }
  const uint32_t credits = pktCreditsFor(pkt.getSize());
  ledger_.onTransmit(*vc, credits);
  stats_.creditsConsumed += credits;
}

std::optional<CbfcCfUpdatePayload> Cbfc::packDirtyReports() {
  CbfcCfUpdatePayload payload;
  const uint8_t numVcs = tracker_.numVcs();
  int slot = 0;
  uint8_t lastPlacedVc = cfRrCursor_;
  for (uint8_t i = 0; i < numVcs && slot < kCfReportsPerUpdate; ++i) {
    const uint8_t vc = (cfRrCursor_ + i) % numVcs;
    if (!cfDirty_.at(vc)) {
      continue;
    }
    const uint16_t cf = tracker_.freed(vc) & kCfCountMask;
    if (slot == 0) {
      payload.vc1 = vc;
      payload.cf1 = cf;
    } else {
      payload.vc2 = vc;
      payload.cf2 = cf;
    }
    cfDirty_.at(vc) = false;
    cfAccum_.at(vc) = 0;
    lastPlacedVc = vc;
    ++slot;
  }
  if (slot == 0) {
    return std::nullopt;
  }
  cfRrCursor_ = (lastPlacedVc + 1) % numVcs;
  if (slot < kCfReportsPerUpdate) {
    // Tbl 5-21 gives CF2 no "unused" encoding — both reports are read
    // unconditionally — so repeat report 1. The repeat is inert: the receiver
    // applies it second, at a forward distance of 0. Leaving {vc=0, cf=0} here
    // would NOT be inert; the sender reads a report as the forward distance
    // from its own S_VC_CF over the counter's full range (see
    // `CbfcSenderLedger::onCfUpdate`), so a literal 0 against a non-zero
    // S_VC_CF[0] is a large advance and would hand that VC credits it never
    // freed.
    payload.vc2 = payload.vc1;
    payload.cf2 = payload.cf1;
  }
  return payload;
}

bool Cbfc::hasPendingCtlos() const {
  return hasDirtyVc() && simCtx_->now() - lastCfSentTime_ >= cfMinSpacing_;
}

std::optional<PacketPtr> Cbfc::popCtlos() {
  auto payload = popCfUpdate();
  if (!payload) {
    return std::nullopt;
  }
  return buildCfUpdatePacket(*payload);
}

std::optional<CbfcCfUpdatePayload> Cbfc::popCfUpdate() {
  if (!hasPendingCtlos()) {
    return std::nullopt;
  }
  auto payload = packDirtyReports();
  if (!payload) {
    return std::nullopt; // unreachable given hasDirtyVc(); defensive
  }
  lastCfSentTime_ = simCtx_->now();
  stats_.cfUpdatesSent++;
  return payload;
}

PacketPtr Cbfc::buildCfUpdatePacket(const CbfcCfUpdatePayload& payload) {
  PacketPtr pkt = createPacket<PacketType::CTLOS_CF_UPDATE>(0);
  pkt.as<CfUpdateFrame>()->cfUpdate = payload;
  return pkt;
}

std::optional<PacketPtr> Cbfc::popCcUpdate() {
  const std::optional<uint8_t> range = ccRanges_.pop();
  if (!range.has_value()) {
    return std::nullopt;
  }

  PacketPtr pkt = createPacket<PacketType::ETHERNET_CC>(0);
  auto* cc = pkt.as<EthernetCcPacket>();
  cc->msgType =
      (*range == 0) ? CbfcCcMsgType::VC_0_15 : CbfcCcMsgType::VC_16_31;
  const uint8_t base = *range * kCcUpdateVcsPerMsg;
  for (uint8_t i = 0; i < kCcUpdateVcsPerMsg; ++i) {
    const uint8_t vc = base + i;
    cc->sVcCc.at(i) =
        (vc < ledger_.numVcs()) ? ledger_.consumed(vc) & kSVcCcMask : 0;
  }
  stats_.ccUpdatesSent++;
  return pkt;
}

void Cbfc::rxCfUpdate(const CbfcCfUpdatePayload& payload) {
  stats_.cfUpdatesRcvd++;
  uint64_t returned = 0;
  const auto apply = [&](const uint8_t vc, const uint16_t rVcCf) {
    // A peer naming a VC this port does not have is a misconfiguration: the
    // credits it reports can never be returned. Log it once, or the link is
    // indistinguishable from an idle one.
    if (vc >= ledger_.numVcs()) {
      if (!cfUnknownVcLogged_) {
        cfUnknownVcLogged_ = true;
        XLOGF(
            ERR,
            "CBFC received a CF_Update naming VC {}, outside this port's "
            "0..{}. The credits it reports cannot be returned. The two ends "
            "disagree on numVcs.",
            int{vc},
            int{ledger_.numVcs()} - 1);
      }
      return;
    }
    const auto result = ledger_.onCfUpdate(vc, rVcCf);
    returned += result.creditsReturned;
  };
  apply(payload.vc1, payload.cf1);
  // Slot 2 is a copy of slot 1 when only one VC was dirty (see
  // `packDirtyReports`). Applying the copy is inert: the first application
  // already advanced `S_VC_CF` to that value, leaving a forward distance of
  // zero.
  apply(payload.vc2, payload.cf2);
  // Counts the receiver-reported delta, not what this side's tiers released.
  // On an over-return the two differ, and that gap is the receiver correcting
  // our bookkeeping, which is what this counter is for.
  stats_.creditsReturned += returned;
  if (returned > 0) {
    onWake(); // a credit-starved VC may now resume
  }
}

void Cbfc::rxCcUpdate(const EthernetCcPacket& pkt) {
  stats_.ccUpdatesRcvd++;
  const uint8_t base =
      (pkt.msgType == CbfcCcMsgType::VC_16_31) ? kCcUpdateVcsPerMsg : 0;
  bool reclaimedAny = false;
  for (uint8_t i = 0; i < kCcUpdateVcsPerMsg; ++i) {
    const uint8_t vc = base + i;
    if (vc >= tracker_.numVcs()) {
      // Symmetric with `rxCfUpdate`'s unknown-VC log. For a VC_16_31 message
      // `base` is already >= numVcs, so the FIRST iteration breaks and the
      // whole range is discarded, leaving §5.2.5.6.4 leak recovery never
      // running for this link — indistinguishable from an idle link without a
      // diagnostic.
      if (i == 0 && !ccRangeUnknownLogged_) {
        ccRangeUnknownLogged_ = true;
        XLOGF(
            ERR,
            "CBFC received a CC_Update covering VCs {}..{}, entirely outside "
            "this port's 0..{}. No credits can be reconciled from it, so leak "
            "recovery will not run. The two ends disagree on numVcs.",
            int{base},
            int{base} + int{kCcUpdateVcsPerMsg} - 1,
            int{tracker_.numVcs()} - 1);
      }
      break;
    }
    const CcUpdateResult reconcile = tracker_.onCcUpdate(vc, pkt.sVcCc.at(i));
    if (reconcile.lostCreditsOutOfRange) {
      // Nothing moved, so nothing new to report — do not dirty the VC or we
      // would spend CtlOS bandwidth on an unchanged R_VC_CF.
      ++stats_.ccUpdateLostCreditsOutOfRangeCount;
    } else if (reconcile.lostCredits > 0) {
      stats_.leakedCreditsReclaimed += reconcile.lostCredits;
      // A §5.2.5.7.4 reclaim advances R_VC_CF by the same mechanism as a
      // drain, so the accumulator must track it or the delta drifts. Dirtying
      // stays unconditional below: reclaim exists to unstick a stalled credit
      // loop and must not be batched behind the accumulation gate.
      cfAccum_.at(vc) += reconcile.lostCredits;
      // A VC that only ever became active through leak recovery must still get
      // the CF_max refresh.
      cfEverFreed_.at(vc) = true;
      // Same shut-down rule as `onCreditsFreed`: a CC_Update still in flight
      // when the link tears CBFC down must not dirty a VC or raise the edge.
      // The reclaim is already in the tracker and in `cfEverFreed_`, so a
      // restart's CF_max refresh reports it.
      if (stopped_) {
        continue;
      }
      cfDirty_.at(vc) = true;
      reclaimedAny = true;
    }
  }
  if (reclaimedAny) {
    onCtlosReady(); // reclaimed credits made a CF_Update reportable
  }
}

coro::SimScopedTask Cbfc::ccMsgLoop() {
  while (true) {
    ccMsgTimer_.reset();
    co_await ccMsgTimer_.expiration();
    // Re-marking an already-marked range does not queue a second report.
    ccRanges_.queueAll();
    // Pulse every tick, not only on the tick that queued the range. The
    // driver waits on a PULSE `SimEvent`, which wakes only waiters already
    // parked, so a skipped tick would leave the queued CC_Update unannounced
    // for the rest of the run and stop leak recovery.
    onWake();
  }
}

coro::SimScopedTask Cbfc::cfMaxLoop() {
  while (true) {
    cfMaxTimer_.reset();
    co_await cfMaxTimer_.expiration();
    // The predicate must stay the sticky `cfEverFreed_` flag, not a value
    // test; see its declaration in cbfc.h.
    bool dirtiedAny = false;
    for (uint8_t vc = 0; vc < tracker_.numVcs(); ++vc) {
      if (cfEverFreed_.at(vc)) {
        cfDirty_.at(vc) = true;
        dirtiedAny = true;
      }
    }
    // A link carrying only best-effort traffic has nothing to refresh.
    if (dirtiedAny) {
      onCtlosReady();
    }
  }
}

bool Cbfc::hasDirtyVc() const noexcept {
  return std::find(cfDirty_.begin(), cfDirty_.end(), true) != cfDirty_.end();
}

void Cbfc::onWake() {
  XCHECK(onWake_) << "no wake subscriber registered on CBFC";
  onWake_();
}

void Cbfc::onCtlosReady() {
  XCHECK(onCtlosReady_) << "no ctlos-ready subscriber registered on CBFC";
  onCtlosReady_();
}

packsim::switches::thrift::CbfcStats Cbfc::exportThriftStats() const {
  const auto& s = stats_;
  packsim::switches::thrift::CbfcStats out;
  out.cfUpdatesSent() = s.cfUpdatesSent;
  out.cfUpdatesRcvd() = s.cfUpdatesRcvd;
  out.ccUpdatesSent() = s.ccUpdatesSent;
  out.ccUpdatesRcvd() = s.ccUpdatesRcvd;
  out.ccUpdateLostCreditsOutOfRangeCount() =
      s.ccUpdateLostCreditsOutOfRangeCount;
  out.creditsConsumed() = s.creditsConsumed;
  out.creditsFreed() = s.creditsFreed;
  out.creditsReturned() = s.creditsReturned;
  out.leakedCreditsReclaimed() = s.leakedCreditsReclaimed;
  out.gateBlockedCount() = s.gateBlockedCount;
  out.losslessVcBufferFullDrops() = s.losslessVcBufferFullDrops;
  return out;
}

} // namespace packsim::cbfc
