/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>
#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/data-rate.h"
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/nic/simple-memory.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/ptr/PacketPtr.h"
// @oss-disable

namespace packsim {

class EthernetPort;

struct Message { // @oss-enable
// @oss-disable
  uint32_t srcRank{};
  uint32_t dstRank{};
  uint32_t sizeBytes{};
  FlexDeviceId srcIdx{};
  FlexDeviceId dstIdx{};

  // If we have multiple NICs, specify which to use
  uint32_t endpointId{0};
  uint32_t dstEndpointId{0};
  // On this transport and connection, unique within one iteration of the
  // workload, in the order the workload creates its data messages; notify
  // messages all carry kNotifyConnectionMessageId. Assigned at creation,
  // normally by the workload. Used, for example, for chunk IDs.
  uint32_t connectionMessageId{};
  // On RoceSocketBase sockets, the position of this message in its socket's
  // posting order: RoceSocketBase assigns it when the message is posted,
  // counting sends and receives separately (see RoceSocketBase::send and
  // RoceSocketBase::recv), matches incoming data and ACKs to posted messages
  // by it, and completes messages in this order, as a queue pair completes its
  // work requests. connectionMessageId cannot serve: it is fixed before the
  // message is posted, so it need not follow the order the socket takes
  // messages in. Other transports may leave sequenceNumber unset or carry
  // their own sequence numbers in it.
  uint32_t sequenceNumber{0};

  uint32_t channelID{0};

  uint32_t localQPN{UINT32_MAX};
  uint32_t remoteQPN{UINT32_MAX};
  uint32_t workloadId{UINT32_MAX};
  uint32_t iteration{1};

  // Pointer to the full workload configuration
  // Set by WorkloadRank when creating the message
  const packsim::workload::thrift::Workload* workload{nullptr};

  uint64_t sendDelayNs{0};

  bool send{true};
  bool recv{true};
  bool isWrite{false};
  bool isWriteImm{false};
  bool isRead{false};
  uint32_t writeImmData{0};
  uint64_t dataPtr{0};
  uint32_t sendOrWriteData{0};

  std::string toString() const {
    auto bool2int = [](bool b) { return b ? 1 : 0; };
    return fmt::format(
        "cmi:{}_srcRank:{}_dstRank:{}_msn:{}_send:{}_recv:{}_size:{}b_"
        "delay:{}ns_src:{}_localQPN:{}_dst:{}_remoteQPN:{}_workloadId:{}",
        connectionMessageId,
        srcRank,
        dstRank,
        sequenceNumber,
        bool2int(send),
        bool2int(recv),
        sizeBytes,
        sendDelayNs,
        srcIdx.toString(),
        localQPN,
        dstIdx.toString(),
        remoteQPN,
        workloadId);
  }
};

class Transport;

class Socket {
 public:
  Socket(
      Transport* transport,
      const FlexDeviceId& src,
      const FlexDeviceId& dst,
      uint16_t srcPort,
      uint32_t localQPN,
      uint32_t remoteQPN,
      const packsim::workload::thrift::Workload* workload = nullptr);

  /* Application interface */
  virtual void send(Message* msg) = 0;
  virtual void recv(Message* msg) = 0;
  virtual ~Socket() = default;

  /* Port interface */
  virtual void onPacketSent(const PacketPtr& pkt) = 0;
  virtual void onPacketRcvd(const PacketPtr& pkt) = 0;
  virtual void onPacketWrittenToMemory(const PacketPtr& pkt) = 0;

  uint16_t getSrcPort() const noexcept {
    return srcPort_;
  }

  uint16_t getDstPort() const noexcept {
    return dstPort_;
  }

  uint32_t getLocalQPN() const noexcept {
    return conn_->srcQPN;
  }

  uint32_t getRemoteQPN() const noexcept {
    return conn_->dstQPN;
  }

  const FlexDeviceId& getDstId() const noexcept {
    return conn_->dstId;
  }

  const FlexDeviceId& getSrcId() const noexcept {
    return conn_->srcId;
  }

  const packsim::thrift::IntfPktStats& getStats() const noexcept {
    return stats_;
  }

  const packsim::workload::thrift::Workload* getWorkload() const noexcept {
    return workload_;
  }

  const QPConnection* getConnection() const noexcept {
    return conn_.get();
  }

  using OnMessageReceivedCallback = std::function<void(Message*)>;
  using OnMessageAckedCallback = std::function<void(Message*)>;
  void setOnMessageReceivedCallback(const OnMessageReceivedCallback& cb) {
    onMessageReceived_ = cb;
  }

  void setOnMessageAckedCallback(const OnMessageAckedCallback& cb) {
    onMessageAcked_ = cb;
  }

 protected:
  SimContext* const simCtx_;
  Transport* const transport_;

  uint32_t txSequenceNumber_{0};
  uint32_t rxSequenceNumber_{0};

  const uint16_t srcPort_{0};
  uint16_t dstPort_{0};

  const std::unique_ptr<const QPConnection> conn_;

  // Raise a completion event. Nothing invokes the members directly -- these
  // are the only readers, and they assert the subscriber is wired. An
  // unsubscribed socket is a wiring bug, not a case to swallow.
  void onMessageReceived(Message* msg) {
    XCHECK(onMessageReceived_) << "no message-received subscriber registered";
    onMessageReceived_(msg);
  }

  void onMessageAcked(Message* msg) {
    XCHECK(onMessageAcked_) << "no message-acked subscriber registered";
    onMessageAcked_(msg);
  }

  OnMessageReceivedCallback onMessageReceived_;
  OnMessageAckedCallback onMessageAcked_;

  packsim::thrift::IntfPktStats stats_;

  // Workload that created this QP
  // Set at construction (proactive) or captured from first Message (reactive)
  const packsim::workload::thrift::Workload* workload_{nullptr};
};

/**
 * Everything a Transport needs at construction, gathered from the endpoint
 * that will own it.
 *
 * Holds references into the caller and is passed by const reference to
 * makeTransport(), so it must not outlive that call.
 */
struct TransportParams {
  SimContext* simCtx;
  const FlexDeviceId& flexId;
  const packsim::thrift::NicConfig& nicConfig;
  // Owning endpoint's name, for construction-time logging. A reference like
  // the two fields above: it must outlive the makeTransport() call.
  const std::string& endpointName;
};

class Transport {
 public:
  explicit Transport(
      SimContext* simCtx,
      const FlexDeviceId& id,
      const packsim::thrift::NicConfig* nicConfig)
      : simCtx_(simCtx), id_(id), nicConfig_(nicConfig) {
    memory_ = std::make_unique<SimpleMemory>(
        *nicConfig_->readGbps() / 8.0, *nicConfig_->readLatencyNs());
    nicBps_ = DataRate(*nicConfig_->speed()).getBps();
  }
  virtual ~Transport() = default;
  virtual Socket* connect(
      const FlexDeviceId& dst,
      uint16_t port,
      uint32_t localQPN,
      uint32_t remoteQPN,
      uint8_t priority = 0,
      const packsim::workload::thrift::Workload* workload = nullptr) = 0;

  using SendPacketFn = std::function<bool(const PacketPtr&)>;
  virtual void setSendPacketFn(const SendPacketFn& cb) {
    sendPacketFn_ = cb;
  }

  /**
   * Register a physical NIC port for per-port packet routing. Called by
   * Endpoint during topology wiring, before connect(). Transports that
   * need per-port routing override this; the default no-op is correct for
   * transports that use single-port ECMP routing.
   *
   * @param egressPort        0-based port index from the topology.
   * @param ethPort           The EthernetPort for queue-depth queries.
   * @param portSendPacketFn  Function that sends a packet out this port.
   */
  virtual void addPort(
      uint16_t /*egressPort*/,
      EthernetPort* /*ethPort*/,
      const SendPacketFn& /*portSendPacketFn*/) {}

  /**
   * Register a NIC port the topology has numbered, so a transport that selects
   * an egress plane per packet knows which planes exist.
   *
   * Distinct from addPort(), which wires the per-port send function and is also
   * called for a single-port NIC -- one with no plane to choose between. This
   * is called only where the topology numbers the ports.
   *
   * @param egressNicPort  0-based port index from the topology.
   */
  virtual void addAvailableNicPort(uint16_t /*egressNicPort*/) {}

  /**
   * The owning endpoint's view of its own port topology, for a transport that
   * chooses an egress plane per packet.
   */
  struct PortTopology {
    // 0-based destination NIC port numbers at `dst` reachable from local egress
    // port `srcPort`, traced through the fabric's forwarding.
    std::function<std::set<uint16_t>(uint16_t srcPort, const FlexDeviceId& dst)>
        reachableDstPorts;
    // Whether 0-based egress NIC port `port` currently has its link up.
    std::function<bool(uint16_t port)> isEgressPortLive;
  };

  /**
   * Hand the transport the endpoint's port topology. Called once per endpoint
   * during topology wiring, before connect().
   *
   * The default is a no-op: a transport that pins each QP pair to one port
   * needs neither function.
   */
  virtual void setPortTopology(const PortTopology& /*topology*/) {}

  using OnPfcStateChangedCallback = std::function<void(uint8_t, PfcState)>;
  // Set the subscriber to PFC changes of the receive buffer that holds packets
  // no port delivered. Only a transport driven without an endpoint stores
  // packets there; a port's own receive buffer pauses that port's peer.
  virtual void setOnPfcStateChangedCallback(
      const OnPfcStateChangedCallback& cb) {
    onPfcStateChanged_ = cb;
  }

  virtual bool onPacketSent(const PacketPtr& pkt, EthernetPort* ethPort) = 0;
  virtual bool onPacketRcvd(const PacketPtr& pkt, EthernetPort* ethPort) = 0;

  /**
   * Called when a packet is dropped (e.g., due to BER, queue overflow)
   * complete per stage. Default implementation does nothing; transports
   * can override for retransmission logic
   *
   * @param  pkt       Packet to be dropped.
   *
   */
  virtual void onPacketDropped(const PacketPtr& /* pkt */) {
    // Default: no action on packet drop
    // Subclasses can override to handle retransmission
  }

  /**
   * Called when a link transitions from DOWN to UP, signaling that
   * the link is available for traffic again.
   * Transports should override this to resume arbitration/sending
   * on stalled QPs.
   */
  virtual void onLinkUp(EthernetPort* /* ethPort */) {
    // Default: no action
  }

  // A QP that has finished transmitting, with its completion time and a debug
  // hint for re-running it.
  struct CompletedQp {
    FlexDeviceId srcId;
    FlexDeviceId dstId;
    uint32_t srcQPN{};
    uint32_t dstQPN{};
    uint64_t completionTimeNs{};
    // Human-readable QP trace prefix (src-..._dst-..._QP-...).
    std::string qpIdentifier;
    // CLI flag (including the leading --) to re-run with this QP traced. A
    // view onto the producing transport's static flag literal: it is constant
    // per transport, so it is not copied into each per-QP record.
    std::string_view debugFlag;
  };

  /**
   * Get the last QP to complete (the straggler), reported as
   * SimulationStats.lastStraggler. A transport that does not override this
   * leaves that field unset.
   *
   * @return  The last QP to complete, or std::nullopt if none is tracked.
   */
  [[nodiscard]] virtual std::optional<CompletedQp> getLastStraggler() const {
    return std::nullopt;
  }

  virtual packsim::thrift::NicSummary getNicStatsSummaryResults() const {
    return {};
  }

  // End-of-sim stats specific to multi-port NICs (the port-pair heatmap), or
  // nullopt for transports/NICs without multi-port data.
  [[nodiscard]] virtual std::optional<packsim::thrift::MultiPortNicStats>
  getMultiPortNicStats() const {
    return std::nullopt;
  }

  using OnMessageReceivedCallback = std::function<void(Message*)>;
  using OnMessageAckedCallback = std::function<void(Message*)>;
  virtual void setOnMessageReceivedCallback(
      const OnMessageReceivedCallback& cb) {
    onMessageReceived_ = cb;
  }

  virtual void setOnMessageAckedCallback(const OnMessageAckedCallback& cb) {
    onMessageAcked_ = cb;
  }

  // Fixed latency in ns of this NIC reading `sizeBytes` of payload at `addr`
  // in its own memory. `sizeBytes` is never zero.
  using GetPayloadReadLatencyNsFn =
      std::function<uint64_t(uint64_t addr, size_t sizeBytes)>;

  /**
   * Charge each payload read whose address the transport knows with this
   * function's latency instead of NicConfig.readLatencyNs. Read bandwidth is
   * unchanged. Only a transport that passes the address to
   * readBytesFromMemory() consults it.
   *
   * @param fn  Fixed latency of one read, by address and size.
   */
  void setGetPayloadReadLatencyNsFn(const GetPayloadReadLatencyNsFn& fn) {
    getPayloadReadLatencyNsFn_ = fn;
  }

  /**
   * Reserve this NIC's memory for one payload read.
   *
   * @param bytes      Bytes read.
   * @param localAddr  Where the payload lives in this NIC's memory, when the
   *                   caller knows it.
   *
   * @return           Delay in nanoseconds until the read completes. A read
   *                   of at least one byte with an address takes its fixed
   *                   latency from the function set by
   *                   setGetPayloadReadLatencyNsFn(), when there is one; every
   *                   other read takes NicConfig.readLatencyNs.
   */
  [[nodiscard]] uint64_t readBytesFromMemory(
      size_t bytes,
      std::optional<uint64_t> localAddr = std::nullopt) {
    auto* mem = sharedMemory_ ? sharedMemory_ : memory_.get();
    // Having no function installed is the default configuration, not a wiring
    // bug: a transport charges the flat latency unless its owner installs one.
    if (bytes == 0 || !localAddr.has_value() || !getPayloadReadLatencyNsFn_) {
      return mem->getReadDelayNs(bytes, simCtx_->nowNs());
    }
    return mem->getReadDelayNs(
        bytes, simCtx_->nowNs(), getPayloadReadLatencyNsFn(*localAddr, bytes));
  }

  // Fixed latency in ns of this NIC writing `sizeBytes` of received payload at
  // `addr` in its own memory. `sizeBytes` is never zero.
  using GetPayloadWriteLatencyNsFn =
      std::function<uint64_t(uint64_t addr, size_t sizeBytes)>;

  /**
   * Charge the memory write of every received payload, so a transport that
   * asks writeBytesToMemory() can hold a packet until its payload write
   * completes. A write of at least one byte whose address the transport knows
   * takes this function's latency, a write of zero bytes takes no fixed
   * latency, and any other write takes NicConfig.readLatencyNs, which also
   * sets the write latency.
   *
   * @param fn  Fixed latency of one write, by address and size.
   */
  void setGetPayloadWriteLatencyNsFn(const GetPayloadWriteLatencyNsFn& fn) {
    getPayloadWriteLatencyNsFn_ = fn;
  }

  /**
   * Reserve this NIC's memory for one received payload write.
   *
   * @param bytes      Bytes written. A write of zero bytes takes no fixed
   *                   latency, so it completes just after the writes already
   *                   reserved.
   * @param localAddr  Where the payload is written in this NIC's memory, when
   *                   the caller knows it.
   *
   * @return           Delay in nanoseconds until the write completes, or
   *                   std::nullopt when no write latency function is set:
   *                   then nothing is reserved and the payload counts as
   *                   written on arrival. Writes complete in the order they are
   *                   reserved.
   */
  [[nodiscard]] std::optional<uint64_t> writeBytesToMemory(
      size_t bytes,
      std::optional<uint64_t> localAddr) {
    // Receive-side writes are charged only on request; without a function a
    // transport treats every payload as written when it arrives.
    if (!getPayloadWriteLatencyNsFn_) {
      return std::nullopt;
    }
    auto* mem = sharedMemory_ ? sharedMemory_ : memory_.get();
    if (bytes == 0) {
      return mem->getWriteDelayNs(0, simCtx_->nowNs(), /* latencyNs= */ 0.0);
    }
    if (!localAddr.has_value()) {
      return mem->getWriteDelayNs(bytes, simCtx_->nowNs());
    }
    return mem->getWriteDelayNs(
        bytes, simCtx_->nowNs(), getPayloadWriteLatencyNsFn(*localAddr, bytes));
  }

  // Use an externally-owned SimpleMemory instead of the built-in one.
  // The caller must ensure the shared memory outlives this transport.
  void setSharedMemory(SimpleMemory* shared) {
    sharedMemory_ = shared;
  }

  Socket* findOrMakeSocket(
      const FlexDeviceId& partnerId,
      uint32_t localQPN,
      uint32_t remoteQPN,
      const packsim::workload::thrift::Workload* workload = nullptr) {
    std::lock_guard<std::mutex> lock(socketMutex_);
    Socket* socket;
    auto it = idToSocket_.find({partnerId, localQPN});
    if (it == idToSocket_.end()) {
      socket = connect(
          partnerId, ephemeralPortCounter_++, localQPN, remoteQPN, 0, workload);
    } else {
      socket = it->second;
    }
    return socket;
  }

  // For iterative workloads, we need to create fresh new sockets with each
  // iteration. This approach does not delete the sockets but it will make it so
  // that they cannot be found by above function. This will force the system to
  // create new and fresh sockets for each iteration while letting the stats for
  // previous socket iterations remain for inspection.
  void unregisterSocket(const FlexDeviceId& partnerId, uint32_t localQPN) {
    std::lock_guard<std::mutex> lock(socketMutex_);
    idToSocket_.erase({partnerId, localQPN});
    qpnToSocket_.erase(localQPN);
  }

  std::string name_;

  virtual void sendPendingData(uint32_t /* cos */) {}

  // Subscriber notified to initialize endpoint port configs when a workload is
  // first seen. This is called when a socket is created with a workload that
  // has port configs, allowing the endpoint to proactively initialize all port
  // configs before any packets are routed.
  using OnWorkloadPortConfigsCallback =
      std::function<void(const packsim::workload::thrift::Workload*)>;
  // Virtual so a transport that owns inner transports can forward the
  // subscriber to them: each inner transport raises this event for itself when
  // a socket is created with a workload that carries port configs.
  virtual void setOnWorkloadPortConfigsCallback(
      const OnWorkloadPortConfigsCallback& cb) {
    onWorkloadPortConfigs_ = cb;
  }

  /**
   * Log the QP with the most outstanding work: the per-QP diagnostic that
   * --packsim_detect_stall prints when it stops a stalled run. A transport
   * that does not override this prints nothing.
   */
  virtual void logWorstIncompleteQP() const {}

  // Verify that there are no outstanding messages.
  virtual bool verifyComplete() {
    XLOGF(WARN, "Transport::verifyComplete() not implemented");
    return true;
  }

  SimContext* getSimCtx() const {
    return simCtx_;
  }

  // Read-only access to all sockets, for inspection by tests and debug tools.
  // NOT synchronized: this does not acquire socketMutex_, so it must only be
  // called when the simulation is paused (engine not running). While the sim
  // runs, connect()/findOrMakeSocket() push into sockets_ under socketMutex_,
  // so a concurrent call here would race and a reallocation could invalidate
  // the returned reference. Pausing parks all PDES worker threads at the
  // barrier, so no socket creation is in flight and sockets_ (and each Socket's
  // state) forms a consistent snapshot.
  const std::vector<std::unique_ptr<Socket>>& getSockets() const {
    return sockets_;
  }

 protected:
  virtual Socket* getSocket(size_t idx) {
    return sockets_.at(idx).get();
  }

  virtual Socket* getSocketByQPN(uint32_t localQPN) {
    return qpnToSocket_.at(localQPN);
  }

 protected:
  SimContext* const simCtx_;
  // Mutex protecting sockets_, qpnToSocket_, and idToSocket_ from concurrent
  // access during socket creation/lookup.
  mutable std::mutex socketMutex_;
  std::vector<std::unique_ptr<Socket>> sockets_;
  std::unordered_map<uint32_t, Socket*> qpnToSocket_;
  folly::F14FastMap<std::tuple<FlexDeviceId, uint32_t>, packsim::Socket*>
      idToSocket_;

  FlexDeviceId id_;
  const packsim::thrift::NicConfig* nicConfig_;
  std::unique_ptr<SimpleMemory> memory_{nullptr};
  SimpleMemory* sharedMemory_{nullptr};
  uint64_t nicBps_;

  // Fixed latency of one payload read from the function the owner installed.
  // readBytesFromMemory() is its only caller and calls it only after one is
  // installed.
  uint64_t getPayloadReadLatencyNsFn(
      const uint64_t addr,
      const size_t sizeBytes) const {
    XCHECK(getPayloadReadLatencyNsFn_)
        << "no payload-read latency function registered";
    return getPayloadReadLatencyNsFn_(addr, sizeBytes);
  }

  GetPayloadReadLatencyNsFn getPayloadReadLatencyNsFn_;

  // Fixed latency of one received payload write from the function the owner
  // installed. writeBytesToMemory() is its only caller and calls it only after
  // one is installed.
  uint64_t getPayloadWriteLatencyNsFn(
      const uint64_t addr,
      const size_t sizeBytes) const {
    XCHECK(getPayloadWriteLatencyNsFn_)
        << "no payload-write latency function registered";
    return getPayloadWriteLatencyNsFn_(addr, sizeBytes);
  }

  GetPayloadWriteLatencyNsFn getPayloadWriteLatencyNsFn_;

  // Send a packet through the function the owner registered. Nothing invokes
  // sendPacketFn_ directly; this is its only reader and it asserts the owner
  // wired one -- a transport with no way to reach the wire is a wiring bug.
  bool sendPacketFn(const PacketPtr& pkt) const {
    XCHECK(sendPacketFn_) << "no send-packet function registered";
    return sendPacketFn_(pkt);
  }

  SendPacketFn sendPacketFn_;

  // Raise the PFC event of the receive buffer no port owns. Nothing invokes
  // onPfcStateChanged_ directly; this is its only reader.
  void onPfcStateChanged(uint8_t cos, PfcState state) {
    XCHECK(onPfcStateChanged_) << "no pfc-state-changed subscriber registered";
    onPfcStateChanged_(cos, state);
  }

  OnPfcStateChangedCallback onPfcStateChanged_;

  // Raise a completion event. Nothing invokes the members directly -- these
  // are the only readers, and they assert the subscriber is wired. An
  // unsubscribed transport is a wiring bug, not a case to swallow.
  void onMessageReceived(Message* msg) {
    XCHECK(onMessageReceived_) << "no message-received subscriber registered";
    onMessageReceived_(msg);
  }

  void onMessageAcked(Message* msg) {
    XCHECK(onMessageAcked_) << "no message-acked subscriber registered";
    onMessageAcked_(msg);
  }

  OnMessageReceivedCallback onMessageReceived_;
  OnMessageAckedCallback onMessageAcked_;

  // Raise the workload-port-configs event. Unlike a completion, this one is
  // optional: it primes an endpoint's port configs ahead of the first packet,
  // so a transport driven with no endpoint has nothing to prime and loses
  // nothing by skipping it. Every endpoint subscribes in its constructor.
  void onWorkloadPortConfigs(
      const packsim::workload::thrift::Workload* workload) {
    XCHECK(onWorkloadPortConfigs_)
        << "no workload-port-configs subscriber registered";
    onWorkloadPortConfigs_(workload);
  }

  OnWorkloadPortConfigsCallback onWorkloadPortConfigs_;

  std::vector<packsim::thrift::IntfPktStats> cosStats_;

  uint16_t ephemeralPortCounter_{1024};
};

} // namespace packsim
