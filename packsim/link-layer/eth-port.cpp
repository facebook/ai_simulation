/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/eth-port.h"

#include <algorithm>
#include <cmath>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <folly/Utility.h>
#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/kernel/data-rate.h"
#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/link-layer/cbfc/nic-rx-credit-bridge.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/link-layer-scheduler.h"
#include "packsim/link-layer/llr/llr-config-translation.h"
#include "packsim/link-layer/llr/llr.h"
#include "packsim/link-layer/wire-trace-flag.h"
#include "packsim/packet/pfc.h"
#include "packsim/queues/queues.h"

namespace packsim {

namespace {

// PackSim convention: bit 7 of `Ipv6Packet::hopLimit` is the "TTL-marked"
// flag that the per-port `rxTtldPkts` / `txTtldPkts` counters count. RoCE
// sends a retransmission with hop limit 192 (bit set) and an original with
// 64 (see `transport/roce-base.cpp`); a frame whose sender keeps the default
// hop limit of 255 carries the bit too. Read here, never written.
constexpr uint8_t kHopLimitTtldBit = 0x80;

// Nanoseconds per microsecond, for the link-flap log line.
constexpr uint64_t kNsPerMicrosecond = 1000;

// Mirror of `emitTraceTx` in `eth-channel.cpp`. Shared LLR-header
// extraction lives in `wire-trace-flag.h`; the TX/RX-specific tag and
// role label differ enough that the XLOG lines stay separate.
void emitTraceRx(const PacketPtr& pkt, const EthernetPort& dst) noexcept {
  if (!FLAGS_packsim_trace_wire_events) {
    return;
  }
  const auto llr = LlrTraceFields::extractFrom(pkt);
  XLOGF(
      INFO,
      "TRACE-RX T={:014} dst={}/{} pkt={} llr={} seq={} size={}",
      dst.getSimCtx()->nowNs(),
      dst.getFlexDeviceId().toString(),
      dst.getPortNum(),
      folly::to_underlying(pkt->pktType),
      llr.type,
      llr.seq,
      pkt->getSize());
}

} // namespace

// Spec references in this file are to UEC (Ultra Ethernet Consortium)
// v1.0 §5.1 — the Link Level Retry specification ("UEC" expanded here
// once for the file; subsequent citations use the abbreviation).
// File-scope named constants live in `llr-types.h` so the LLR module
// and its callers share the same source of truth.

EthernetPort::EthernetPort(SimContext* simCtx, const std::string& dataRate)
    : EthernetPort(simCtx, dataRate, FlexDeviceId{}) {}

EthernetPort::EthernetPort(
    SimContext* simCtx,
    const std::string& dataRate,
    const FlexDeviceId& id)
    : flexId_(id),
      pfc_(*this),
      txArbiter_(*this),
      dataRate_(dataRate),
      simCtx_(simCtx) {
  reinitializeQueues();
}

EthernetPort::~EthernetPort() {
  // teardownLlr() drives llr_->shutdown() so the SimEvent destructors
  // observe a clean (no-suspended-waiter) state — without this, the
  // unique_ptr<llr::Llr> destructor would XCHECK on the first SimEvent
  // it tries to destroy. Not disableLlr(), which would start a new wire
  // driver on a CBFC port mid-destruction. Idempotent: no-op if LLR was
  // never enabled or already disabled.
  teardownLlr();
  // Stop the CBFC module's periodic timers before members go away.
  // Idempotent — no-op if CBFC was never enabled.
  disableCbfc();
  // On a port without LLR, teardownLlr() short-circuits and leaves the wire
  // driver in place, whichever of enableCbfc(), disableLlr() or the first wake
  // built it. Explicit teardown here drains the txDriver coroutine before port
  // members go away. Idempotent — no-op if scheduler is null or already torn
  // down by teardownLlr above.
  teardownWireDriver();
}

void EthernetPort::enablePeriodicStats(
    const std::string& name,
    const uint64_t intervalNs) {
  name_ = name;
  if (intervalNs == 0) {
    // `updatePeriodicStats` reschedules itself at this interval, so running it
    // at zero would be an unbounded zero-delay event loop.
    return;
  }
  statsIntervalNs_ = intervalNs;
  updatePeriodicStats();
}

void EthernetPort::updatePeriodicStats() {
  uint64_t txBytes = 0;
  uint64_t rxBytes = 0;
  for (const auto& cosStats : portStats_) {
    txBytes += cosStats.txBytes;
    rxBytes += cosStats.rxBytes;
  }
  if (prevTxBytes_ != txBytes) {
    simCtx_->tracer.trace(
        fmt::format("TXBytes_{}_Port_{}", name_, switchPortNum_),
        txBytes - prevTxBytes_);
    prevTxBytes_ = txBytes;
  }
  if (prevRxBytes_ != rxBytes) {
    simCtx_->tracer.trace(
        fmt::format("RXBytes_{}_Port_{}", name_, switchPortNum_),
        rxBytes - prevRxBytes_);
    prevRxBytes_ = rxBytes;
  }
  simCtx_->schedule(
      Time::fromNs(statsIntervalNs_), &EthernetPort::updatePeriodicStats, this);
}

/**
 * Receive a packet through this port with optional receive latency.
 *
 * @param pkt The packet to receive. The port takes this handle over.
 */
void EthernetPort::receive(PacketPtr pkt) {
  if (rxLatencyNs_ > 0) {
    simCtx_->schedule(
        Time::fromNs(rxLatencyNs_),
        &EthernetPort::doReceive,
        this,
        std::move(pkt));
  } else {
    doReceive(std::move(pkt));
  }
}

void EthernetPort::doReceive(PacketPtr pkt) {
  emitTraceRx(pkt, *this);

  if (dropIfBitError(pkt)) {
    return;
  }
  if (dropIfLinkDown(pkt)) {
    return;
  }
  if (dispatchCtlosFrame(pkt)) {
    return;
  }
  if (dispatchMacControlFrame(pkt)) {
    return;
  }
  deliverData(std::move(pkt));
}

std::string EthernetPort::getPacketRepr(const PacketPtr& pkt) const {
  // Every field is always present and in a fixed order, so a drop log can be
  // grepped and parsed across frame kinds. A field that does not apply to this
  // kind keeps its "N/A".
  std::string subtype{"N/A"};
  std::string cos{"N/A"};
  std::string llrSeq{"N/A"};

  if (pkt->pktType == PacketType::CTLOS_LLR) {
    // `as<>` is a static_cast, safe because the enum already pinned the type.
    const auto* ctlos = pkt.as<LlrCtlosFrame>();
    subtype = llr::LlrEnumNames::toString(ctlos->payload.type);
    llrSeq = fmt::to_string(ctlos->payload.seq);
  } else if (!pkt.is<CtlosFrame>()) {
    // Only a data frame is charged to a CoS. PFC and CC_Update are accounted
    // to queue 0 as a catch-all, which is not a class of service.
    if (pkt.is<Ipv6Packet>()) {
      cos = fmt::to_string(getCosFor(pkt));
    }
    // Reached for data frames and for CC_Update, a MAC control frame that can
    // still be LLR-eligible.
    const auto* eth = pkt.tryAs<EthernetPacket>();
    if (eth != nullptr && eth->llrHeader.enabled) {
      llrSeq = fmt::to_string(eth->llrHeader.seqNum);
    }
  }

  // `pktType` is logged numerically because the enum has no name table.
  return fmt::format(
      "type={} subtype={} port={} cos={} llr_seq={} size={}B time={}ns",
      folly::to_underlying(pkt->pktType),
      subtype,
      switchPortNum_,
      cos,
      llrSeq,
      pkt->getSize(),
      simCtx_->nowNs());
}

bool EthernetPort::dropIfBitError(const PacketPtr& pkt) {
  static constexpr double kMaxBer = 1.0;
  const double ber = rxBerOverride_.has_value()
      ? *rxBerOverride_
      : folly::not_null<const EthernetChannel*>(channel_)->getBer();
  if (ber >= kMaxBer) {
    return false;
  }
  // (1 - BER)^n: the probability that a packet of n bits crosses a link with
  // this bit error rate untouched. This draw must stay the first RNG
  // operation performed on an arriving frame, or seeded runs stop reproducing.
  static constexpr double kByteToBit{8.0};
  const double pGood = std::pow(1 - ber, pkt->getSize() * kByteToBit);
  if (simCtx_->random.sampleBernoulli(pGood)) {
    return false;
  }

  XLOGF(DBG2, "BER drop on {}: {}", flexId_.toString(), getPacketRepr(pkt));
  if (pkt.is<CtlosFrame>()) {
    // Nothing to account: an ordered set has no CoS to charge and no observer
    // expecting it.
  } else {
    recordReceiveDrop(
        pkt,
        getCosFor(pkt),
        packsim::switches::thrift::PacketDropReason::BER_ERROR);
  }
  return true;
}

bool EthernetPort::dropIfLinkDown(const PacketPtr& pkt) {
  if (channelState_ != ChannelState::DOWN) {
    return false;
  }
  XLOGF(
      DBG,
      "LINK_DOWN_DROP(doReceive): src {} dst {}: {}",
      getPeerFlexDeviceId().toString(),
      flexId_.toString(),
      getPacketRepr(pkt));
  if (pkt.is<CtlosFrame>()) {
    // Nothing to account: an ordered set has no CoS to charge and no observer
    // expecting it.
  } else {
    recordReceiveDrop(
        pkt,
        getCosFor(pkt),
        packsim::switches::thrift::PacketDropReason::LINK_ERROR);
  }
  return true;
}

bool EthernetPort::dispatchCtlosFrame(const PacketPtr& pkt) {
  if (pkt->pktType == PacketType::CTLOS_CF_UPDATE) {
    if (cbfc_) {
      cbfc_->rxCfUpdate(pkt.as<CfUpdateFrame>()->cfUpdate);
    }
    return true;
  }

  if (pkt->pktType == PacketType::CTLOS_LLR) {
    // `rxCtlos` fires the `onSchedulerWake_` callback installed in
    // `setupLinkLayerScheduler()`, so an LLR_ACK that just freed buffer space
    // (or an LLR_NACK that triggered REPLAY) re-pumps the wire without waiting
    // for the next tx-complete.
    if (llr_) {
      const auto* ctlos = pkt.as<LlrCtlosFrame>();
      llr_->rxCtlos(
          ctlos->payload.type, ctlos->payload.seq, ctlos->payload.initData);
    }
    return true;
  }
  return false;
}

bool EthernetPort::dispatchMacControlFrame(const PacketPtr& pkt) {
  if (pkt->pktType == PacketType::ETHERNET_PFC) {
    pfc_.receiveFrame(pkt);
    return true;
  }
  // CC_Update is deliberately absent. It shares `TxKind::MAC_CONTROL` with PFC
  // on the transmit side and is never credit-gated, but on receive it is
  // data-ordered: when LLR is enabled it is LLR-eligible and carries a
  // sequence number, so it must clear LLR's gap filter before its cumulative
  // S_VC_CC is believed. Consuming it here would let a CC_Update that arrived
  // behind a dropped frame be applied before the retransmission, and
  // `CbfcCreditTracker::onCcUpdate` would book the not-yet-replayed frame as a
  // leaked credit and free it a second time when the replay drains. It is
  // consumed at the delivery point instead -- see `consumeIfCcUpdate`, UEC
  // 1.0.3 5.2.7.3 and 5.2.9 rule 3.
  return false;
}

void EthernetPort::deliverData(PacketPtr pkt) {
  // Restore socket-level QPConnection for port-level addressed packets. This
  // must happen BEFORE delivery so the upper layer's transport routing finds
  // the right socket.
  if (pkt.is<Ipv6Packet>() && usingPortLevelAddresses(pkt)) {
    auto* ipPkt = pkt.as<Ipv6Packet>();
    ipPkt->conn = ipPkt->socketLevelConn;
  }

  // With LLR enabled the module computes expected/duplicate/missing and drives
  // the ACK/NACK SM, then says whether this frame reaches the MAC client. It
  // is a filter in front of the delivery below, not a second delivery path.
  if (llr_) {
    // `as<>` is a static_cast and never returns null, so the type must be
    // tested first: a non-Ethernet frame has no `llrHeader` to read.
    const auto* eth = pkt.tryAs<EthernetPacket>();
    const llr::FrameType type = (eth && eth->llrHeader.enabled)
        ? llr::FrameType::LLR_ELIGIBLE
        : llr::FrameType::LLR_INELIGIBLE;
    const uint32_t seq = eth ? eth->llrHeader.seqNum : 0u;
    // PackSim doesn't model FCS errors at the wire level; always GOOD.
    if (llr_->rxFrame(pkt, type, llr::FcsStatus::GOOD, seq) ==
        llr::Llr::RxDisposition::DROP) {
      return;
    }
  }

  bumpRxStats(pkt);
  // UEC 1.0.3 §5.2.7.3: a CC_Update is delivered on MA_DATA.indication "in
  // strict packet order relative to all other received data packets". This is
  // that point. With LLR enabled its sequence-gap filter has already run, so a
  // CC_Update following discarded LLR-eligible frames never reaches here --
  // §5.2.9 rule 3. Without LLR there is no discard window to apply.
  if (consumeIfCcUpdate(pkt)) {
    return;
  }
  deliverToMacClient(std::move(pkt));
}

/**
 * Send a packet through this port with optional transmit latency.
 *
 * @param pkt The packet to transmit. Must be non-null and properly formatted.
 *            The port takes this handle over.
 */
void EthernetPort::send(PacketPtr pkt) {
  if (txLatencyNs_ > 0) {
    simCtx_->schedule(
        Time::fromNs(txLatencyNs_),
        &EthernetPort::doSend,
        this,
        std::move(pkt));
  } else {
    doSend(std::move(pkt));
  }
}

void EthernetPort::doSend(PacketPtr pkt) {
  if (channel_ == nullptr) {
    throw std::runtime_error("EthernetPort::send: no channel");
  }
  if (channelState_ == ChannelState::DOWN) {
    XLOGF(
        DBG,
        "LINK_DOWN_DROP(doSend): src {} dst {} pktType {} size {} time={}ns",
        flexId_.toString(),
        getPeerFlexDeviceId().toString(),
        folly::to_underlying(pkt->pktType),
        pkt->getSize(),
        simCtx_->nowNs());
    recordSendDrop(
        pkt,
        getCosFor(pkt),
        packsim::switches::thrift::PacketDropReason::LINK_ERROR);
    return;
  }

  // The queue the filter selected, and whether the packet was enqueued there
  // rather than dropped. An enqueued packet moves into its queue; a dropped
  // packet stays in `pkt`.
  const auto [outQueue, enqueue] = queues_->pushPacketByMove(pkt);
  if (enqueue) {
    // `pushPacket` returns the filter's CoS for every packet, accepted or
    // not, so `outQueue` is always a real queue here.
    updateTxQueueAvailability(outQueue);
    onDataEnqueued();
  } else {
    recordSendDrop(
        pkt,
        outQueue,
        packsim::switches::thrift::PacketDropReason::BUFFER_FULL);
  }
}

PacketPtr EthernetPort::peekNextAppPacket() const {
  // `peekQueue()`, not `nextQueue()`: advancing the scheduler cursor here would
  // make the following `getTxPacket()` land on a different queue.
  const auto cosId = queues_->peekQueue();
  if (!cosId.has_value()) {
    return PacketPtr{};
  }
  PacketPtr peek;
  queues_->peekPacketInQueue(*cosId, &peek);
  return peek;
}

void EthernetPort::getTxPacket(PacketPtr& packet) {
  bool marked = false;
  const std::optional<uint8_t> outQueue = queues_->popPacket(packet, &marked);
  if (!outQueue.has_value()) {
    // Nothing eligible: either no queue has work, or the scheduler skipped
    // every occupied one. `marked` cannot be set without a popped packet.
    return;
  }
  updateTxQueueAvailability(*outQueue);
  if (isVoqEcnMarkingEnabled_) {
    queues_->markVoqEcnPacket(packet, *outQueue, &marked);
  }
  if (marked) {
    onPacketMarked(
        packet,
        *outQueue,
        packsim::switches::thrift::PacketDropReason::UNKNOWN);
    ++portStats_.at(*outQueue).ecnCeMarkedPkts;
  }
}

uint8_t EthernetPort::getPktCos(const PacketPtr& p) const {
  return queues_->filter(p);
}

uint32_t EthernetPort::getQueueLength(const uint8_t cos) const {
  return queues_->getQueueLengthInBytes(cos);
}

uint32_t EthernetPort::getTotalQueueLength() const noexcept {
  return queues_->getTotalQueueLengthInBytes();
}

uint8_t EthernetPort::getNumQueues() const noexcept {
  return queues_->getNumQueues();
}

bool EthernetPort::isTxQueueAvailable(const uint8_t cos) const {
  return queues_->isTxQueueAvailable(cos);
}

void EthernetPort::onTxQueueSpaceAvailable(const uint8_t cos) {
  XCHECK(onTxQueueSpaceAvailable_) << "no tx-queue-space subscriber registered";
  onTxQueueSpaceAvailable_(cos);
}

void EthernetPort::updateTxQueueAvailability(const uint8_t cos) {
  // Only endpoint ports subscribe. Switch ports never read the flag, so their
  // queues skip the check and are never marked full.
  if (hasTxQueueSpaceSubscriber_) {
    queues_->sendPendingTxQueueBytes(cos);
  }
}

EthernetChannel* EthernetPort::getChannel() const {
  return channel_;
}

void EthernetPort::attach(EthernetChannel* ch) {
  channel_ = ch;
  channel_->attach(this);
}

void EthernetPort::setOnLinkUpCallback(const OnLinkUpCallback& cb) {
  onLinkUp_ = cb;
}

void EthernetPort::setOnPacketMarkedCallback(const OnPacketMarkedCallback& cb) {
  onPacketMarked_ = cb;
}

void EthernetPort::setPortNum(const uint16_t index) {
  switchPortNum_ = index;
}

uint16_t EthernetPort::getPortNum() const {
  return switchPortNum_;
}

void EthernetPort::reinitializeQueues() {
  if (portQueueCfg_.empty()) {
    portQueueCfg_.resize(1);
  }
  queues_ = std::make_unique<Queues>(
      simCtx_, queueFilterCfg_, queueSchedulingAlgoCfg_, portQueueCfg_);
  portStats_.clear();
  portStats_.resize(getNumQueues());
  // A lambda over the raiser, not a copy of the member, so a subscriber set
  // after this still reaches the new queues.
  queues_->setOnTxQueueSpaceAvailableCallback(
      [this](uint8_t cos) { onTxQueueSpaceAvailable(cos); });
  // `queues_` was just replaced, so the previous gate went with it.
  // FlexTopology enables CBFC in `connectNodes` before `addSwitchPort`
  // reconfigures queues, so the gate must be reinstalled here as well as in
  // `enableCbfc`.
  installCbfcQueueGate();
  pfc_.resize(getNumQueues());
}

FlexDeviceId EthernetPort::getPeerFlexDeviceId() const {
  return getPeer()->getFlexDeviceId();
}

uint16_t EthernetPort::getPeerPortNum() const {
  return getPeer()->getPortNum();
}

PortStats EthernetPort::getPortStats(const uint8_t cos) const {
  return portStats_.at(cos);
}

std::vector<PortStats> EthernetPort::getAllPortStats() const {
  std::vector<PortStats> out{portStats_.begin(), portStats_.end()};
  if (llr_) {
    const auto stats = llr_->exportThriftStats();
    // LLR is per-port, not per-CoS — stamp the same snapshot onto
    // every port-CoS entry.
    for (auto& portStat : out) {
      portStat.llrStats = stats;
    }
  }
  if (cbfc_) {
    const auto cbfcStats = cbfc_->exportThriftStats();
    for (auto& portStat : out) {
      portStat.cbfcStats = cbfcStats;
    }
  }
  return out;
}

void EthernetPort::setRxPfcState(const uint8_t qId, const PfcState state) {
  pfc_.setRxState(qId, state);
}

EthernetPort* FOLLY_NONNULL EthernetPort::getPeer() const {
  if (peer_ == nullptr) {
    peer_ = channel_->getPeer(*this);
  }
  return peer_;
}

void EthernetPort::setRxLog10BerOverride(
    const std::optional<double> log10Ber) noexcept {
  rxBerOverride_ = log10Ber.has_value()
      ? std::optional<double>{std::pow(10, *log10Ber)}
      : std::nullopt;
}

void EthernetPort::setChannelState(const ChannelState state) {
  const auto prevState = channelState_;
  channelState_ = state;
  if (state == ChannelState::DOWN) {
    pfc_.onLinkDown();

    // Drop all packets in the queue for this port. The CBFC gate must come off
    // first or the drain under-runs: `popPacket` skips credit-starved queues,
    // so the loop would exit with packets still resident and the drop counters
    // and LINK_ERROR callback would miss them. Same reasoning as the TX PFC
    // reset above.
    queues_->clearTxGate();
    PacketPtr pkt;
    while (queues_->popPacket(pkt).has_value()) {
      recordSendDrop(
          pkt,
          getCosFor(pkt),
          packsim::switches::thrift::PacketDropReason::LINK_ERROR);
    }
    // Scoped to the drain, not to the link-down: no other path reinstalls the
    // gate on a flap, so leaving it cleared would disable credit gating for the
    // rest of the run.
    installCbfcQueueGate();
    // The drain empties every queue, so give each the space check the normal
    // dequeue path runs; otherwise a queue marked full stays full, and its
    // subscriber never hears that it has room. After the loop, not per pop, so
    // the subscriber does not re-enter the port mid-drain.
    for (uint8_t cos = 0; cos < getNumQueues(); ++cos) {
      updateTxQueueAvailability(cos);
    }
    XLOGF(
        INFO,
        "Flapped link src {} dst {} at time {} microsecs",
        flexId_.toString(),
        getPeerFlexDeviceId().toString(),
        simCtx_->now().getNs() / kNsPerMicrosecond);
  } else if (state == ChannelState::UP && prevState == ChannelState::DOWN) {
    onLinkUp();
  }
}

// ============================================================================
// LLR (Link Level Retry) Methods
// ============================================================================

void packsim::EthernetPort::enableLlr(
    const packsim::thrift::LlrConfig& config,
    const bool enableGlobalTimer) {
  // llr_ owns the spec-compliant INIT handshake and the
  // SEND_ACKS / SEND_NACK SM. The thrift `initCtlosSpacingNs`
  // field is not plumbed (see `topology.thrift`) — `ackByteThreshold` is
  // mapped to UEC `ctlos_target_spacing` and the LLR_INIT period is
  // computed as `4 × ctlos_target_spacing` per UEC §5.1.9.1.1.
  const llr::LlrConfig llrConfig = llr::LlrConfigTranslator::translate(
      config, dataRate_.getBps(), enableGlobalTimer);

  llr_ = std::make_unique<llr::Llr>(simCtx_, llrConfig);

  // Wire-up before `start()`:
  //   - `setTxPacketSource(this)` so `Llr::pullNextNewFrame` can call
  //     `peekNextAppPacket()` / `getTxPacket()` through the
  //     `TxPacketSource` interface this port implements.
  //   - `setOnPostCallback` on `ctlosSlots` so every `post*` routes
  //     through `onCtlosPosted()`, which signals the scheduler.
  //   - `setupLinkLayerScheduler()` builds the wire driver if none exists
  //     (CBFC enabled first, or LLR re-enabled on a CBFC port, leaves one),
  //     and `wireLlrWake()` points `llr_`'s scheduler-wake callback at it,
  //     so a future LLR_ACK that frees buffer space can wake a parked
  //     admission. MUST run before `start()` so the callback is in place
  //     before any TX activity.
  llr_->setTxPacketSource(this);
  llr_->ctlosSlots().setOnPostCallback([this]() { onCtlosPosted(); });
  setupLinkLayerScheduler();
  wireLlrWake();
  llr_->start();
  // `mode_local` + `mode_remote` are both implied by
  // `skipInitNegotiation=true` (`Llr::start()` pre-configures them); for
  // the standard handshake path, set them explicitly after `start()` so
  // the order is robust:
  //   1. Doing both AFTER start() means a throw from either setter cannot
  //      leave the Llr in a "constructed but never started" state. The
  //      destructor is safe for "never started" (no waiters on any
  //      SimEvent), but moving the mode toggles after `start()` removes
  //      the need to rely on that invariant.
  //   2. `setLlrModeRemote(true)` broadcasts `llrModeRemoteChanged_`,
  //      which only wakes coroutines that already exist — i.e. only
  //      post-start. Calling it before start would silently drop the
  //      transition.
  // `setLlrModeLocal` does NOT broadcast (its only signal-emitting branch
  // is the true→false edge, see `Llr::setLlrModeLocal`), so its placement
  // is order-insensitive — keep it grouped with `setLlrModeRemote` for
  // readability.
  const bool needsHandshake = !llrConfig.skipInitNegotiation;
  if (needsHandshake) {
    llr_->setLlrModeLocal(true);
    llr_->setLlrModeRemote(true);
  }
}

void packsim::EthernetPort::disableLlr() {
  if (!llr_) {
    return;
  }
  teardownLlr();
  // CBFC's wake callbacks still point at the driver just destroyed, and its
  // CC_msg timer fires them every tick. They cannot be cleared instead:
  // `Cbfc` requires a subscriber. Rebuilding the driver rewires them.
  if (cbfc_) {
    setupLinkLayerScheduler();
  }
}

void packsim::EthernetPort::teardownLlr() {
  // Idempotent: no-op when LLR was never enabled (or already torn down).
  if (!llr_) {
    return;
  }

  llr_->logCounterSummary(flexId_.toString(), switchPortNum_);
  // Tear down LinkLayerScheduler + consumers BEFORE destroying llr_. The
  // txDriver may be parked mid-frame, and on resume it reaches llr_ through
  // txArbiter_; destroying the coroutine here means it can never resume
  // against a torn-down llr_. The explicit teardown here runs BEFORE
  // `llr_.reset()` below — destruction order via member declaration would
  // otherwise run the LinkLayerScheduler dtor AFTER this returns, which is
  // too late.
  teardownWireDriver();
  llr_->shutdown();
  llr_.reset();
}

void packsim::EthernetPort::enableCbfc(
    const packsim::thrift::CbfcConfig& config) {
  // Validation lives in the `cbfc::Cbfc` constructor, which checks the link
  // config as written before translating it.
  cbfc_ = std::make_unique<cbfc::Cbfc>(simCtx_, config, dataRate_.getBps());
  // `setupLinkLayerScheduler()` returns early when the scheduler already
  // exists (LLR enabled first), so the wakes must be installed here too.
  setupLinkLayerScheduler();
  if (linkLayerScheduler_) {
    wireCbfcWakes();
  }
  // An accel endpoint's port never reconfigures its queues after
  // construction, so `reinitializeQueues` will not run again and this is the
  // only install point for those.
  installCbfcQueueGate();
  // Revive the existing bridge rather than replace it: a `NicRxBuffer` from a
  // previous enable still holds a raw pointer to it, so constructing a new one
  // would free the object that buffer drains through.
  if (rxCreditBridge_ == nullptr) {
    rxCreditBridge_ = std::make_unique<cbfc::NicRxCreditBridge>(cbfc_.get());
  } else {
    rxCreditBridge_->reattach(cbfc_.get());
  }
  cbfc_->start();
}

void packsim::EthernetPort::disableCbfc() {
  if (!cbfc_) {
    return;
  }
  // Before `cbfc_` goes away: the gate captures `this` and reads `cbfc_`, so a
  // surviving gate would dereference a destroyed module on the next pop.
  if (queues_) {
    queues_->clearTxGate();
  }
  cbfc_->shutdown();
  // Detach rather than destroy: transport-owned buffers hold a raw pointer to
  // the bridge, so it outlives any single enable/disable cycle.
  if (rxCreditBridge_) {
    rxCreditBridge_->detach();
  }
  cbfc_.reset();
}

bool EthernetPort::consumeIfCcUpdate(const PacketPtr& pkt) {
  if (pkt->pktType != PacketType::ETHERNET_CC) {
    return false;
  }
  // Drop silently when CBFC is not enabled here — a §5.2.6.2 frame with no
  // consumer must never reach the MAC client, or switch routing sees a non-IP
  // packet it cannot classify.
  if (cbfc_) {
    cbfc_->rxCcUpdate(*pkt.as<EthernetCcPacket>());
  }
  return true;
}

void EthernetPort::onDataEnqueued() {
  // Lazy scheduler init for ports with neither LLR nor CBFC. enableLlr() and
  // enableCbfc() init eagerly, and disableLlr() rebuilds the driver on a CBFC
  // port; for the rest the first wake (data Send, PFC enqueue, etc.) is the
  // natural setup point — by then the port's LLR configuration is stable
  // (port setup completes before any traffic).
  if (!linkLayerScheduler_) {
    setupLinkLayerScheduler();
  }
  linkLayerScheduler_->signalDataAvailable();
}

void EthernetPort::onCtlosPosted() {
  // CtlOS wakes only fire from LLR's LlrCtlosSlots::onPost, which is
  // installed in enableLlr() — so by the time we get here,
  // setupLinkLayerScheduler() has already run (eager LLR init). The
  // lazy-init guard mirrors onDataEnqueued for symmetry/safety.
  if (!linkLayerScheduler_) {
    setupLinkLayerScheduler();
  }
  linkLayerScheduler_->signalCtlosAvailable();
}

void EthernetPort::setCtlosInsertionRules(
    const packsim::thrift::CtlosInsertionConfig& config) {
  if (*config.minSpacingBytes() > kMaxFrameBytes) {
    throw std::invalid_argument(
        fmt::format(
            "CtlosInsertionConfig.minSpacingBytes ({}) is outside 0..{}. UEC "
            "1.0.3 §5.2.11.2 sets a floor of 400 B and no ceiling, but a "
            "spacing beyond one maximum-size frame makes CtlOS rarer than one "
            "per frame, which starves LLR ACK generation and wedges the TX "
            "buffer.",
            *config.minSpacingBytes(),
            kMaxFrameBytes));
  }
  if (*config.noPreemptPrefixBytes() >= kMaxFrameBytes) {
    throw std::invalid_argument(
        fmt::format(
            "CtlosInsertionConfig.noPreemptPrefixBytes ({}) is outside 0..{}. "
            "A prefix that covers a whole maximum-size frame leaves no frame "
            "a CtlOS can preempt, contradicting §5.2.7.2's requirement that "
            "CtlOS be able to preempt an in-progress packet.",
            *config.noPreemptPrefixBytes(),
            kMaxFrameBytes - 1));
  }
  ctlosMinSpacingBytes_ = *config.minSpacingBytes();
  ctlosNoPreemptPrefixBytes_ = *config.noPreemptPrefixBytes();
  // Call before the port starts transmitting: `LinkLayerScheduler::
  // setCtlosInsertionRules` has no defined behaviour on a running driver. The
  // scheduler is created lazily, so `setupLinkLayerScheduler` applies these
  // values to whichever one it later constructs.
  if (linkLayerScheduler_) {
    linkLayerScheduler_->setCtlosInsertionRules(
        ctlosMinSpacingBytes_, ctlosNoPreemptPrefixBytes_);
  }
}

void EthernetPort::setupLinkLayerScheduler() {
  if (linkLayerScheduler_) {
    // Already built, lazily by a first wake or by an earlier enableLlr() or
    // enableCbfc(). The enable call that lands here wires its own wake
    // callbacks after this returns.
    return;
  }

  XCHECK(channel_ != nullptr)
      << "setupLinkLayerScheduler called before attach()";
  linkLayerScheduler_ = std::make_unique<wire::LinkLayerScheduler>(
      simCtx_, channel_, this, &txArbiter_, dataRate_);
  // Must precede `start()` below: the rules have no defined behaviour on a
  // port that is already transmitting.
  linkLayerScheduler_->setCtlosInsertionRules(
      ctlosMinSpacingBytes_, ctlosNoPreemptPrefixBytes_);

  if (cbfc_) {
    wireCbfcWakes();
  }

  linkLayerScheduler_->start();
}

void packsim::EthernetPort::installCbfcQueueGate() {
  if (!cbfc_ || !queues_) {
    return;
  }
  // Let the scheduler skip a queue whose head packet has no credit instead of
  // stalling the port. Best-effort and non-IP frames always pass, so this only
  // ever holds back a credit-gated VC (UEC §5.2.8.2: "The sender's
  // implementation must prevent blocking between VCs").
  queues_->setTxGate(
      [this](const PacketPtr& pkt) { return cbfc_->canTransmit(*pkt); });
}

void packsim::EthernetPort::wireLlrWake() {
  // Lets Llr pulse the scheduler on REPLAY entry (the arbiter's
  // `nextOutboundFrame` begins draining the txBuffer snapshot) and on every
  // ACK/NACK that advances `acked_seq` (so any blocked admission retries).
  // Callback (vs typed pointer) keeps `Llr` decoupled from
  // `wire::LinkLayerScheduler`.
  llr_->setOnSchedulerWakeCallback(
      [s = linkLayerScheduler_.get()]() { s->signalDataAvailable(); });
}

void packsim::EthernetPort::wireCbfcWakes() {
  // Returned credits and a queued CC_Update are data-path events: they let the
  // credit gate re-run and put an ordinary Ethernet packet in the outbound
  // queue.
  cbfc_->setOnWakeCallback(
      [s = linkLayerScheduler_.get()]() { s->signalDataAvailable(); });
  // A ready CF_Update is a CtlOS, and `ctlosAvailable_` is the only signal the
  // scheduler's in-flight splice path awaits. On the data signal it would wait
  // for the current frame to drain and could never preempt, which UEC 1.0.3
  // §5.2.7.2 requires of CtlOS.
  cbfc_->setOnCtlosReadyCallback(
      [s = linkLayerScheduler_.get()]() { s->signalCtlosAvailable(); });
}

void EthernetPort::teardownWireDriver() {
  if (!linkLayerScheduler_) {
    return;
  }
  linkLayerScheduler_->shutdown();
  linkLayerScheduler_.reset();
}

uint8_t EthernetPort::getCosFor(const PacketPtr& pkt) const {
  return pkt.is<Ipv6Packet>() ? getPktCos(pkt) : uint8_t{0};
}

PortStats& EthernetPort::getStatsFor(const PacketPtr& pkt) {
  // `.at()`, not `operator[]`: `getPktCos` runs a configurable queue filter,
  // so a misconfigured filter should surface here rather than corrupt the
  // counters of an adjacent CoS.
  return portStats_.at(getCosFor(pkt));
}

void EthernetPort::bumpDropStats(const PacketPtr& pkt, const uint8_t cos) {
  PortStats& stats = portStats_.at(cos);
  ++stats.dropPkts;
  stats.dropBytes += pkt->getSize();
}

void EthernetPort::recordReceiveDrop(
    const PacketPtr& pkt,
    const uint8_t cos,
    const packsim::switches::thrift::PacketDropReason reason) {
  bumpDropStats(pkt, cos);
  if (observer_ != nullptr) {
    observer_->onPacketDroppedOnReceive(*this, pkt, cos, reason);
  }
}

void EthernetPort::recordSendDrop(
    const PacketPtr& pkt,
    const uint8_t cos,
    const packsim::switches::thrift::PacketDropReason reason) {
  bumpDropStats(pkt, cos);
  if (observer_ != nullptr) {
    observer_->onPacketDroppedOnSend(*this, pkt, cos, reason);
  }
}

void EthernetPort::bumpRxStats(const PacketPtr& pkt) {
  PortStats& stats = getStatsFor(pkt);
  if (pkt.is<Ipv6Packet>()) {
    const auto* ip = pkt.as<Ipv6Packet>();
    if (ip->ecn == Ipv6Packet::EcnType::ECN_CE) {
      ++stats.rxEcnCePkts;
    }
    if (ip->hopLimit & kHopLimitTtldBit) {
      ++stats.rxTtldPkts;
    }
  }
  ++stats.rxPkts;
  stats.rxBytes += pkt->getSize();
}

void EthernetPort::bumpTxStats(const PacketPtr& pkt) {
  PortStats& stats = getStatsFor(pkt);
  if (pkt.is<Ipv6Packet>()) {
    const auto* ip = pkt.as<Ipv6Packet>();
    if (ip->ecn == Ipv6Packet::EcnType::ECN_CE) {
      ++stats.txEcnCePkts;
    }
    if (ip->hopLimit & kHopLimitTtldBit) {
      ++stats.txTtldPkts;
    }
  }
  ++stats.txPkts;
  stats.txBytes += pkt->getSize();
}

void EthernetPort::deliverToMacClient(PacketPtr pkt) {
  XCHECK(observer_ != nullptr) << fmt::format(
      "device {} port {} received a frame with no observer installed; "
      "call setObserver() during port setup",
      flexId_.toString(),
      getPortNum());
  observer_->onPacketRcvdByMove(*this, std::move(pkt));
}

void EthernetPort::onLinkUp() {
  // Optional: `Endpoint` is the only subscriber, and it installs the handler
  // on its own port. A switch's ports come from `FbSwitch::adoptPort`,
  // which does not, so a link flap on a switch port raises this with nobody
  // listening.
  if (onLinkUp_) {
    onLinkUp_(this);
  }
}

void EthernetPort::onPacketMarked(
    const PacketPtr& pkt,
    const uint8_t queue,
    const packsim::switches::thrift::PacketDropReason reason) {
  // Optional: `FbSwitch::adoptPort` is the only subscriber, and `popPacket`
  // sets `marked` from the queue's ECN or WRED_ECN profile whoever owns the
  // port. An accelerator port configured with one of those therefore reaches
  // here unsubscribed. VOQ_ECN marking needs `enableVoqEcnMarking()`, which
  // only `FbSwitch` calls.
  if (onPacketMarked_) {
    onPacketMarked_(this, pkt, queue, reason);
  }
}

} // namespace packsim
