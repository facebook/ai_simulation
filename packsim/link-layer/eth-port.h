/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <bitset>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <folly/CppAttributes.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/switches_types.h"
#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/link-layer/eth-channel.h"
#include "packsim/link-layer/pfc-controller.h"
#include "packsim/link-layer/port-observer.h"
#include "packsim/link-layer/port-stats.h"
#include "packsim/link-layer/tx-arbiter.h"
#include "packsim/link-layer/tx-packet-source.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/ptr/PacketPtr.h"

namespace packsim {

class FlexNode;
class Queues;

namespace wire {
class LinkLayerScheduler;
} // namespace wire

// Held only by `unique_ptr` and handed out as raw pointers, so the port
// header does not need either module's definition. Anything that actually
// calls into them includes the corresponding header itself.
namespace llr {
class Llr;
struct LlrConfig;
} // namespace llr

namespace cbfc {
class Cbfc;
class NicRxCreditBridge;
} // namespace cbfc

/**
 * One end of an Ethernet link: the MAC-layer port a node sends and receives
 * packets through. Owns the per-CoS transmit queues and the PFC controller,
 * plus the LLR and CBFC modules when enabled; serializes frames onto its
 * attached `EthernetChannel` at `dataRate_`; and reports sends, arrivals and
 * drops to its `IPortObserver`.
 *
 * Example usage:
 *
 *      auto port = std::make_unique<EthernetPort>(simCtx, "400Gbps", id);
 *      port->setObserver(&observer);  // required before it can receive
 *      port->attach(channel);         // one of the link's two ends
 *      port->send(pkt);               // queued; the wire driver transmits it
 *
 * NOTE: Not thread-safe. Used only from its own `SimContext`; frames from the
 * peer arrive as events scheduled there.
 */
class EthernetPort : public TxPacketSource {
 public:
  // `PfcController` is this class, split out: it drives `queues_`, bumps
  // `portStats_` and wakes the scheduler, all of which stay here because
  // `queues_` is replaced whenever the port is reconfigured.
  friend class PfcController;
  friend class TxArbiter;

  // Upper bound for the UEC 1.0.3 §5.2.11.2 CtlOS insertion-rule validators in
  // `setCtlosInsertionRules()`: past one maximum-size frame each rule defeats
  // its own purpose. Not a limit applied to outgoing frames.
  static constexpr uint32_t kMaxFrameBytes = 9216;
  // The port's Priority Flow Control state machine and timers.
  PfcController& pfc() noexcept {
    return pfc_;
  }

  // Decides what this port transmits next; driven by the wire scheduler.
  TxArbiter& txArbiter() noexcept {
    return txArbiter_;
  }

  // ===== Standard port API =====

  explicit EthernetPort(SimContext* simCtx, const std::string& dataRate);
  explicit EthernetPort(
      SimContext* simCtx,
      const std::string& dataRate,
      const FlexDeviceId& id);

  ~EthernetPort() override;

  virtual void receive(PacketPtr p);

  void send(PacketPtr packet);

  void getTxPacket(PacketPtr& pkt) override;

  /**
   * Peek the packet `getTxPacket()` will return next, without popping it: a
   * latched CBFC CC_Update when one is pending, otherwise the head of the
   * next-eligible CoS queue. Callers check admission
   * (`Llr::pullNextNewFrame`) or credits (`cbfcGateNextAppPacket`) before the
   * packet leaves its queue.
   *
   * @return  The next packet to transmit, or a null `PacketPtr` when nothing
   *          is queued.
   */
  [[nodiscard]] PacketPtr peekNextAppPacket() const override;

  void setNode(FlexNode* node) {
    node_ = node;
  }

  FlexNode* getNode() const noexcept {
    return node_;
  }

  SimContext* getSimCtx() const noexcept {
    return simCtx_;
  }

  EthernetChannel* getChannel() const;

  /**
   * Install the layer above this port. Required before the port can
   * receive: `IPortObserver::onPacketRcvd` has no default.
   *
   * @param observer  Must outlive this port.
   */
  void setObserver(IPortObserver* observer) {
    observer_ = observer;
  }

  using OnLinkUpCallback = std::function<void(EthernetPort*)>;

  void setOnLinkUpCallback(const OnLinkUpCallback& cb);

  using OnPacketMarkedCallback = std::function<void(
      EthernetPort*,
      const PacketPtr&,
      uint8_t,
      packsim::switches::thrift::PacketDropReason)>;

  // ECN marking has a single consumer (`FbSwitch`), so it stays a callback
  // rather than joining `IPortObserver` and adding a no-op override to
  // every other implementer.
  void setOnPacketMarkedCallback(const OnPacketMarkedCallback& cb);

  /**
   * `getPortNum()` before `setPortNum()` has run. A port is constructed and
   * configured before it is attached, so this is the value throughout topology
   * construction and for the lifetime of any port that never joins a node.
   * It is the largest `uint16_t`, so that value is not a usable port number.
   */
  static constexpr uint16_t kUnattachedPort =
      std::numeric_limits<uint16_t>::max();

  void setPortNum(uint16_t index);

  /**
   * This port's index on its node, or `kUnattachedPort` before
   * `setPortNum()` has run. Unsigned because a port index is, and because
   * every consumer uses it as an index or a map key.
   */
  uint16_t getPortNum() const;

  virtual void setRxPfcState(uint8_t queue, PfcState state);

  void attach(EthernetChannel* ch);

  /**
   * The FlexDeviceId of the peer: the port on the other end of this port's
   * channel.
   *
   * @return  The peer's FlexDeviceId.
   *
   * @throw std::runtime_error if this port is not on its channel (its
   *        `attach()` threw) or nothing other than this port is attached to
   *        that channel.
   */
  FlexDeviceId getPeerFlexDeviceId() const;

  /**
   * The port number the peer occupies on its own node.
   *
   * @return  That port number, or `kUnattachedPort` before the peer's
   *          `setPortNum()` has run.
   *
   * @throw std::runtime_error if this port is not on its channel (its
   *        `attach()` threw) or nothing other than this port is attached to
   *        that channel.
   */
  uint16_t getPeerPortNum() const;

  PortStats getPortStats(uint8_t cos) const;
  std::vector<PortStats> getAllPortStats() const;

  /**
   * The peer: the port on the other end of this port's channel, resolved on
   * first use.
   *
   * @return  The peer. Non-const despite the `const` qualifier: this port
   *          does not own its peer -- the topology does -- so this port's
   *          constness says nothing about whether the peer may be modified.
   *          Same rule as `std::shared_ptr<T>::get() const` returning `T*`.
   *          Never null.
   *
   * @throw std::runtime_error if this port is not on its channel (its
   *        `attach()` threw) or nothing other than this port is attached to
   *        that channel.
   */
  [[nodiscard]] EthernetPort* FOLLY_NONNULL getPeer() const;

  void setFlexDeviceId(const FlexDeviceId& id) noexcept {
    flexId_ = id;
  }

  const FlexDeviceId& getFlexDeviceId() const noexcept {
    return flexId_;
  }

  void setQueueSchedulerConfig(
      const packsim::thrift::SchedulingAlgorithmType& queueSchedulingAlgoCfg) {
    queueSchedulingAlgoCfg_ = queueSchedulingAlgoCfg;
    reinitializeQueues();
  }

  void setQueueFilterConfig(
      const packsim::thrift::QueueFilterConfig& queueFilterCfg) {
    queueFilterCfg_ = queueFilterCfg;
    reinitializeQueues();
  }

  void setPortQueueConfig(
      const std::vector<packsim::thrift::PortQueueConfig>& portQueueCfg) {
    portQueueCfg_ = portQueueCfg;
    reinitializeQueues();
  }

  /**
   * Turn on VOQ-based ECN (Explicit Congestion Notification) marking: a packet
   * this port transmits from a queue whose profile is VOQ_ECN is then marked
   * by the bytes still on that queue (`Queues::markVoqEcnPacket`). `FbSwitch`
   * turns it on for every port it adds, where that queue is the virtual output
   * queue for {egress port, queue}. It stays off on every other port, so a
   * VOQ_ECN profile on an endpoint's queue marks nothing. Survives queue
   * reconfiguration.
   */
  void enableVoqEcnMarking() noexcept {
    isVoqEcnMarkingEnabled_ = true;
  }

  void setTxLatencyNs(const uint32_t txLatencyNs) {
    txLatencyNs_ = txLatencyNs;
  }

  void setRxLatencyNs(const uint32_t rxLatencyNs) {
    rxLatencyNs_ = rxLatencyNs;
  }

  uint8_t getNumQueues() const noexcept;

  uint8_t getPktCos(const PacketPtr& p) const;

  // Line rate of this port.
  DataRate getDataRate() const noexcept {
    return dataRate_;
  }

  uint32_t getQueueLength(uint8_t cos) const;

  uint32_t getTotalQueueLength() const noexcept;

  bool isTxQueueAvailable(uint8_t cos) const;

  using OnTxQueueSpaceAvailableCallback = std::function<void(uint8_t)>;
  void setOnTxQueueSpaceAvailableCallback(
      const OnTxQueueSpaceAvailableCallback& cb) {
    onTxQueueSpaceAvailable_ = cb;
    hasTxQueueSpaceSubscriber_ = static_cast<bool>(cb);
  }

  // Start the periodic byte-count trace, which counts the frames of every
  // class of service. An `intervalNs` of 0 leaves it off.
  void enablePeriodicStats(const std::string& name, uint64_t intervalNs);

  void setChannelState(ChannelState state);

  ChannelState getChannelState() const noexcept {
    return channelState_;
  }

  /**
   * Apply `log10Ber` to the frames this port receives instead of its channel's
   * bit error rate, or return to the channel's with std::nullopt. Only this
   * port reads the value, so call it from this port's own context or while the
   * engine is not running.
   *
   * @param log10Ber  log10 of the bit error rate (e.g. -6.0 for 1e-6); 0 or
   *                  more disables bit errors. std::nullopt restores the
   *                  channel's.
   */
  void setRxLog10BerOverride(std::optional<double> log10Ber) noexcept;

  void setPortIndex(std::optional<int> portIndex) {
    portIndex_ = portIndex;
  }

  bool usingPortLevelAddresses(const PacketPtr& pkt) const noexcept {
    return portIndex_.has_value() && usesPortLevelAddressing(pkt->pktType);
  }

  // LLR (Link Level Retry) methods — see UEC 1.0.3 §5.1 for the spec.

  /**
   * Enable LLR on this port. `config` is translated by
   * `llr::LlrConfigTranslator::translate`.
   *
   * @throws std::invalid_argument on out-of-spec-range inputs:
   *   - `replayCtMax >= 255` (collides with the "no maximum"
   *     sentinel). Use `0` (or omit) for "no limit"; values `1..254`
   *     are honored.
   *   - `ackByteThreshold > kCtlosTargetSpacingMaxBytes` (16384,
   *     UEC §5.1.4 Tbl 5-9). Values `<= 0` map to the spec minimum
   *     (`kCtlosTargetSpacingMinBytes`, 400); positive values up to
   *     the spec maximum are honored.
   */
  void enableLlr(
      const packsim::thrift::LlrConfig& config,
      bool enableGlobalTimer = true);

  /**
   * Tear LLR down on this port. A port that keeps CBFC gets a fresh wire
   * driver without LLR, so CBFC's credit updates keep flowing. Idempotent — a
   * no-op on a port where LLR is not enabled.
   */
  void disableLlr();

  bool isLlrEnabled() const noexcept {
    return llr_ != nullptr;
  }

  /**
   * Read-only view of the LLR module, or nullptr when LLR is disabled.
   * A test seam: no production code reads the module through the port.
   */
  const llr::Llr* llr() const noexcept {
    return llr_.get();
  }

  /**
   * Non-const overload — tests use this to drive the AN/TX state
   * machines directly (e.g., `llr()->rxCtlos(LLR_ACK, seq, 0)`)
   * without standing up a full peer port. Production code does not
   * mutate `llr::Llr` from outside; the const overload above is the
   * expected production access path.
   */
  llr::Llr* llr() noexcept {
    return llr_.get();
  }

  /**
   * Apply UEC 1.0.3 §5.2.11.2's CtlOS insertion rules to this port. The rules
   * span the port's COMBINED CtlOS stream, so this is neither an LLR nor a
   * CBFC setting even though both feed it. Safe to call before the wire driver
   * exists; see `ctlosMinSpacingBytes_` for the staging rule.
   *
   * @throws std::invalid_argument if either value is beyond the bound at
   *         which the rule would starve CtlOS entirely.
   */
  void setCtlosInsertionRules(
      const packsim::thrift::CtlosInsertionConfig& config);

  // CBFC (Credit-Based Flow Control) methods — see UEC 1.0.3 §5.2.

  /**
   * Enable CBFC on this port from a per-link thrift config: constructs the
   * `cbfc::Cbfc` module, wires it into the wire driver, `queues_` and the NIC
   * receive path, and starts its timers. Sender credit limits are NOT set
   * here — `bootstrapCbfcCredits` in `topology/flex-topology.cpp` derives them
   * from the partner's receive buffer.
   *
   * @param config  Per-link CBFC settings: VC count and lossless VC set, the
   *                VC map, credit sizing, VC groups, and the CF/CC timers.
   *                Validated in full before any of it is translated, so a
   *                rejected config leaves the port untouched.
   *
   * @throws std::invalid_argument on out-of-range inputs: `numVcs`
   *         outside `1..cbfc::kCbfcLimits.maxVcs`, an empty lossless VC set
   *         or a VC id outside `0..numVcs-1` (UEC 1.0.3 Tbl 5-27), credit
   *         sizing outside its supported bounds, malformed VC groups, or a
   *         CF/CC timer outside the bounds in `cbfc/cbfc-types.h`.
   */
  void enableCbfc(const packsim::thrift::CbfcConfig& config);

  /**
   * Tear CBFC down on this port: shuts the module down (cancelling its
   * timers and discarding any queued CC_Update range), clears the credit
   * predicate on `queues_`, drops any latched CC_Update, detaches the NIC
   * receive credit bridge, and destroys the module. Idempotent — a no-op on a
   * port where CBFC was never enabled.
   */
  void disableCbfc();

  bool isCbfcEnabled() const noexcept {
    return cbfc_ != nullptr;
  }

  // Read-only view of the CBFC module (nullptr when CBFC is disabled).
  const cbfc::Cbfc* cbfc() const noexcept {
    return cbfc_.get();
  }

  cbfc::Cbfc* cbfc() noexcept {
    return cbfc_.get();
  }

  /**
   * The NIC receive-buffer credit observer, installed on a `NicRxBuffer` by
   * the transport that creates one. nullptr until CBFC has been enabled at
   * least once, and non-null but inert after a disable, so this is not a
   * "CBFC is enabled" test — ask `cbfc()` for that.
   */
  [[nodiscard]] cbfc::NicRxCreditBridge* rxCreditBridge() noexcept {
    return rxCreditBridge_.get();
  }

  EthernetPort(const EthernetPort&) = delete;
  EthernetPort& operator=(const EthernetPort&) = delete;
  EthernetPort(EthernetPort&&) = delete;
  EthernetPort& operator=(EthernetPort&&) = delete;

 protected:
  // The only two members a subclass overrides. `port-latency-test` derives
  // from this class to intercept them; everything else below is private.
  virtual void doSend(PacketPtr packet);
  virtual void doReceive(PacketPtr p);

 private:
  /**
   * One line of packet metadata for the drop logs: always the packet type,
   * port, size and timestamp, plus whatever else identifies this kind of
   * frame -- the ordered-set kind and sequence for an LLR CtlOS, the charged
   * CoS for a data frame, and the LLR sequence number for anything
   * LLR-eligible.
   *
   * Called only on a drop, so the `is<>` it performs is off the per-frame
   * path.
   *
   * @param pkt  The frame to describe.
   *
   * @return     The formatted description.
   */
  [[nodiscard]] std::string getPacketRepr(const PacketPtr& pkt) const;

  /**
   * Apply the link's bit error rate to an arriving frame.
   *
   * Draws from the shared simulation RNG exactly once per call, and only when
   * the link has a BER configured. That draw is part of the run's
   * reproducibility: drops are decided by draw *order*, so this must stay the
   * first thing `doReceive` does to a frame.
   *
   * A control ordered set of either producer is dropped silently -- no
   * counters, no observer -- because it has no CoS to charge.
   *
   * @param pkt  The arriving frame.
   *
   * @return     True if the frame was dropped and must not be processed
   *             further.
   */
  [[nodiscard]] bool dropIfBitError(const PacketPtr& pkt);

  /**
   * Drop an arriving frame if the link went down after the receive was
   * scheduled. Silent for a control ordered set, as above.
   *
   * @param pkt  The arriving frame.
   *
   * @return     True if the frame was dropped and must not be processed
   *             further.
   */
  [[nodiscard]] bool dropIfLinkDown(const PacketPtr& pkt);

  /**
   * Hand a control ordered set to the module that owns it: a CBFC CF_Update
   * to `Cbfc`, an LLR ACK/NACK/INIT/INIT_ECHO to `Llr`. Dropped silently when
   * that module is disabled, because a peer may keep sending after a local
   * teardown and an ordered set carries no IP connection the switch routing
   * layer could classify.
   *
   * @param pkt  The arriving frame.
   *
   * @return     True if the frame was a control ordered set and was consumed
   *             here.
   */
  [[nodiscard]] bool dispatchCtlosFrame(const PacketPtr& pkt);

  /**
   * Hand a MAC control frame to the module that owns it. Today that is PFC
   * only: CC_Update is a MAC control frame for transmit scheduling but is
   * data-ordered on receive, so it is consumed at the delivery point instead.
   * The reason is in the implementation.
   *
   * @param pkt  The arriving frame.
   *
   * @return     True if the frame was consumed here.
   */
  [[nodiscard]] bool dispatchMacControlFrame(const PacketPtr& pkt);

  // Deliver an ordinary data frame upward, through LLR when it is enabled and
  // straight to the MAC client when it is not.
  void deliverData(PacketPtr pkt);

  /**
   * If `pkt` is an ETHERNET_CC frame, apply it to `cbfc_`, or drop it when
   * CBFC is disabled.
   *
   * @return  True if `pkt` was consumed here, so the RX path must not deliver
   *          it upward.
   */
  [[nodiscard]] bool consumeIfCcUpdate(const PacketPtr& pkt);

  /**
   * Point LLR's scheduler-wake callback at the wire driver. Requires `llr_`
   * and `linkLayerScheduler_` to be live, and `llr_` not yet started.
   */
  void wireLlrWake();

  /**
   * Install both CBFC wake callbacks on the wire driver. Requires `cbfc_` and
   * `linkLayerScheduler_` to be live.
   */
  void wireCbfcWakes();

  /**
   * Install the CBFC credit predicate on `queues_` so the scheduler skips a
   * starved VC rather than stalling the port. Idempotent; no-op without CBFC.
   */
  void installCbfcQueueGate();

  // Minimum wire distance in bytes between two CtlOS on this port's combined
  // LLR + CBFC stream (UEC 1.0.3 §5.2.11.2). 0 leaves the rule unenforced and
  // is the default; see `thrift::CtlosInsertionConfig` for why. Staged on the
  // port because the wire driver is created lazily, and forwarded to it on
  // construction and on any later change.
  uint32_t ctlosMinSpacingBytes_{0};
  // Bytes at the head of a packet during which a CtlOS may not preempt
  // (UEC 1.0.3 §5.2.11.2). 0 leaves the rule unenforced and is the default;
  // staged and forwarded like `ctlosMinSpacingBytes_` above.
  uint32_t ctlosNoPreemptPrefixBytes_{0};

  // Resolved lazily on first use and never changed afterwards, so the peer
  // accessors stay `const`.
  mutable EthernetPort* peer_{nullptr};
  FlexDeviceId flexId_;

  // Raise the port's own events. Each member is read nowhere but its raiser.
  // Both are optional: a port is exercised standalone across the link-layer
  // suite, with no owner subscribing.
  void onLinkUp();
  void onPacketMarked(
      const PacketPtr& pkt,
      uint8_t queue,
      packsim::switches::thrift::PacketDropReason reason);

  // Link-up recovery notification
  OnLinkUpCallback onLinkUp_;
  // Mark callback
  OnPacketMarkedCallback onPacketMarked_;

  // The layer above. Non-owning; must outlive this port.
  IPortObserver* observer_{nullptr};

  EthernetChannel* channel_{nullptr};

  // The bit error rate applied to received frames in place of the channel's
  // (see `setRxLog10BerOverride`); unset while the channel's applies.
  std::optional<double> rxBerOverride_;

  PfcController pfc_;
  TxArbiter txArbiter_;
  std::unique_ptr<Queues> queues_;
  uint16_t switchPortNum_{kUnattachedPort};

  std::vector<PortStats> portStats_;

  packsim::thrift::SchedulingAlgorithmType queueSchedulingAlgoCfg_{
      packsim::thrift::SchedulingAlgorithmType::RR};
  packsim::thrift::QueueFilterConfig queueFilterCfg_;
  std::vector<packsim::thrift::PortQueueConfig> portQueueCfg_;
  // Set by `enableVoqEcnMarking()`. Kept on the port rather than in `queues_`,
  // which every queue reconfiguration replaces.
  bool isVoqEcnMarkingEnabled_{false};

  // Line rate this port serializes frames at.
  const DataRate dataRate_;

  OnTxQueueSpaceAvailableCallback onTxQueueSpaceAvailable_;
  // Whether onTxQueueSpaceAvailable_ holds a subscriber, for
  // updateTxQueueAvailability(); only the setter and the raiser touch the
  // member itself.
  bool hasTxQueueSpaceSubscriber_{false};

  void updatePeriodicStats();
  uint64_t statsIntervalNs_{0};
  std::string name_;
  // Bytes sent, summed over every class of service, at the last trace sample.
  uint64_t prevTxBytes_{0};
  // Bytes received, summed over every class of service, at the last trace
  // sample.
  uint64_t prevRxBytes_{0};

  FlexNode* node_{};
  SimContext* const simCtx_{nullptr};

  ChannelState channelState_{ChannelState::UP};

  uint32_t txLatencyNs_{0};
  uint32_t rxLatencyNs_{0};
  std::optional<int> portIndex_;

  // The `llr::Llr` module owns the entire LLR layer:
  //   - TX state machine (TX SM) per UEC 1.0.3 §5.1.5 Fig 5-3
  //   - ACK/NACK transmit state machine (AN-SM) per UEC §5.1.6 Fig 5-4
  //   - The three spec timers (replay_timer, data_age_timer,
  //     pcs_lost_status_timer) per UEC §5.1.5 Tbl 5-10
  //   - Periodic LLR_ACK / LLR_INIT Control Ordered Set (CtlOS) emission
  //   - The replay buffer (holds LLR-eligible frames pending ACK).
  // nullptr when LLR is not enabled on this port.
  std::unique_ptr<llr::Llr> llr_;

  // The `cbfc::Cbfc` module owns the CBFC layer (UEC 1.0.3 §5.2): the per-VC
  // credit ledger (sender) + tracker (receiver), the TX credit gate, and
  // CF_Update / CC_Update generation and reception. nullptr when CBFC is not
  // enabled on this port. Independent of `llr_`; the two coexist.
  std::unique_ptr<cbfc::Cbfc> cbfc_;
  // NOT tied to `cbfc_`: created on the first `enableCbfc`, detached but kept
  // alive on `disableCbfc` because transport-owned buffers hold a raw pointer
  // to it, and destroyed only with this port. Ask `cbfc_`, not this, to know
  // whether CBFC is on. Hands NIC buffer store/drain events to the module on
  // endpoint ports.
  std::unique_ptr<cbfc::NicRxCreditBridge> rxCreditBridge_;

  // ===== Coroutine wire driver =====
  //
  // Every port's TX path goes through the `LinkLayerScheduler`. The
  // scheduler pulls frames via `TxArbiter::nextOutboundFrame()` /
  // `TxArbiter::nextCtlosFrame()` and reports completion via
  // `TxArbiter::onFrameTransmitted()`. `enableLlr()` (before
  // `llr_->start()`) and `enableCbfc()` init it eagerly, and `disableLlr()`
  // rebuilds it on a port that keeps CBFC; other ports init lazily on the
  // first wake via `onDataEnqueued()` / `onCtlosPosted()`.
  std::unique_ptr<wire::LinkLayerScheduler> linkLayerScheduler_;

  /**
   * Construct the wire driver, apply the staged CtlOS insertion rules, point
   * CBFC's wake callbacks at it when CBFC is enabled, and start the
   * `txDriver`. LLR's wake callback is left to `enableLlr()`, which calls
   * `wireLlrWake()` whether this built the driver or found one. The driver
   * itself does not depend on the LLR state.
   *
   * Called from:
   *   - `enableLlr()` after `llr_` is constructed (eager init,
   *     LLR mode).
   *   - `enableCbfc()` (eager init, so CBFC's wakes have a target).
   *   - `disableLlr()` on a CBFC port, after `teardownLlr()` destroyed
   *     the LLR-mode driver.
   *   - `onDataEnqueued()` / `onCtlosPosted()` lazily on first wake
   *     when no scheduler exists yet (non-LLR mode).
   *
   * Idempotent: returns immediately if a scheduler already exists.
   */
  void setupLinkLayerScheduler();

  /**
   * Drain the txDriver coroutine and release the scheduler. Idempotent —
   * no-op if no scheduler is set up. Called from `teardownLlr()` (LLR
   * teardown) and from `~EthernetPort()` (non-LLR teardown, since
   * `teardownLlr` does nothing on a non-LLR port).
   */
  void teardownWireDriver();

  /**
   * Shut `llr_` down and destroy it, tearing the wire driver down first
   * because a txDriver parked mid-frame reaches `llr_` on resume. Never
   * rebuilds the driver, so the destructor calls this rather than
   * `disableLlr()`. Idempotent — no-op if LLR is not enabled.
   */
  void teardownLlr();

  /**
   * Wake the scheduler's data signal. For non-LLR ports, lazily
   * initializes the scheduler on the first wake. Producers that
   * enqueue a non-CtlOS frame should use this so callers don't
   * repeat the branch.
   */
  void onDataEnqueued();

  /**
   * CtlOS variant of `onDataEnqueued`. Pulses `ctlosAvailable_` so an
   * in-flight PCS-splice race aborts and the new CtlOS gets spliced in
   * ahead of the data frame.
   */
  void onCtlosPosted();

  /**
   * Rebuild `queues_`, `portStats_` and the PFC event vectors from the current
   * queue configuration. Discards anything still queued, so it runs from the
   * constructor and from the `set*Config` setters only.
   */
  void reinitializeQueues();

  // Raises the tx-queue-space event for `cos`.
  void onTxQueueSpaceAvailable(uint8_t cos);

  /**
   * Re-runs the queue's space check for `cos` after it gained or lost a
   * frame: marks the queue full past the filter's maxQSizeThreshold, and free
   * again, raising the tx-queue-space event, at its minQSizeThreshold.
   *
   * @param cos  The class of service whose queue changed.
   */
  void updateTxQueueAvailability(uint8_t cos);

  /**
   * The per-CoS counters a frame belongs to.
   *
   * @param pkt  The frame to attribute.
   *
   * @return     The counters for `pkt`'s CoS queue if it is an IP frame;
   *             otherwise those for queue 0, since CtlOS, PFC and CC_Update
   *             carry no CoS.
   *
   * @throws     `std::out_of_range` if the configurable queue filter behind
   *             `getPktCos()` returns a CoS with no `portStats_` entry. The
   *             bounds check is deliberate: a misconfigured filter must
   *             surface here rather than corrupt an adjacent CoS's counters.
   */
  [[nodiscard]] PortStats& getStatsFor(const PacketPtr& pkt);

  /**
   * Count a dropped frame against `cos` and notify that direction's drop
   * observer. The observer is optional; the counters are not.
   *
   * @param pkt     The frame being dropped.
   * @param cos     The queue to charge. Passed in rather than derived, because
   *                the buffer-full path knows the queue the push actually
   *                selected, which for a non-IP frame is not the queue
   *                `getStatsFor()` would pick.
   * @param reason  Passed through to the observer.
   */
  void recordReceiveDrop(
      const PacketPtr& pkt,
      uint8_t cos,
      packsim::switches::thrift::PacketDropReason reason);
  void recordSendDrop(
      const PacketPtr& pkt,
      uint8_t cos,
      packsim::switches::thrift::PacketDropReason reason);

  // Drop counters shared by both directions; the direction decides only which
  // observer is told.
  void bumpDropStats(const PacketPtr& pkt, uint8_t cos);

  // The CoS queue a frame is accounted to: its own for IP frames, queue 0 for
  // everything else.
  [[nodiscard]] uint8_t getCosFor(const PacketPtr& pkt) const;

  // Count a received frame against its CoS, with its ECN and TTL markings.
  void bumpRxStats(const PacketPtr& pkt);

  // Count a transmitted frame against its CoS, with its ECN and TTL markings.
  void bumpTxStats(const PacketPtr& pkt);

  /**
   * Hand a received frame up to the MAC client. Unlike the drop
   * notifications, `IPortObserver::onPacketRcvd` carries the data path, so a
   * port with no observer is a wiring error rather than a missing diagnostic
   * and fails loudly (XCHECK) instead of swallowing the frame.
   */
  void deliverToMacClient(PacketPtr pkt);
};

} // namespace packsim
