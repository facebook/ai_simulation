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
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <tuple>
#include <utility>

#include <folly/CppAttributes.h>
#include <folly/container/F14Map.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/nic/nic-rx-buffer.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/transport/congestion-control.h"
#include "packsim/transport/error-recovery.h"
#include "packsim/transport/roce-constants.h"
#include "packsim/transport/roce-send-info.h"
#include "packsim/transport/transport.h"

namespace packsim {

class RoceTransportBase;
class EthernetPort;
struct RocePacket;

// Configuration for constructing a RoceTransportBase with the desired
// error recovery and congestion control behavior.
struct RoceTransportConfig {
  enum class ErrorRecovery { NONE, GO_BACK_N };
  enum class CongestionControl { NONE, DCQCN };

  ErrorRecovery errorRecovery = ErrorRecovery::NONE;
  CongestionControl congestionControl = CongestionControl::NONE;

  // Go-Back-N config (only used when errorRecovery == GO_BACK_N).
  uint8_t rnrTimeIdx = 1;
  bool selectiveGapRepair = false;

  /**
   * Build the config from the `roceV2` thrift transport variant. Single source
   * of the RoCE-family mapping, used by `makeTransport()`
   * (`transport/transport-factory.h`).
   *
   * @param transport  The `roceV2` TransportConfig union arm.
   *
   * @return           The equivalent RoceTransportConfig.
   *
   * @throw std::invalid_argument  If `transport` is unset, is a non-RoCE
   *                               variant, or has an unset sub-union arm.
   */
  static RoceTransportConfig fromThrift(
      const packsim::thrift::TransportConfig& transport);
};

class RoceSocketBase : public Socket,
                       public ErrorRecoverySocket,
                       public CongestionControlSocket {
 public:
  // Type alias for backwards compatibility with code that uses SendInfo.
  using SendInfo = RoceSendInfo;

  // Factory invoked inside the constructor with `*this` and `*simCtx_` so
  // strategies can take non-null references to the owning socket and
  // SimContext at construction time (no separate attach step).
  using ErrorRecoveryFactory = std::function<
      std::unique_ptr<ErrorRecovery>(RoceSocketBase&, SimContext&)>;
  using CongestionControlFactory = std::function<std::unique_ptr<
      CongestionControl>(CongestionControlSocket&, SimContext&)>;

  RoceSocketBase(
      RoceTransportBase* transport,
      const FlexDeviceId& src,
      const FlexDeviceId& dst,
      uint16_t srcPort,
      uint32_t localQPN,
      uint32_t remoteQPN,
      int32_t smtu = 4200,
      const packsim::workload::thrift::Workload* workload = nullptr,
      const ErrorRecoveryFactory& errorRecoveryFactory = nullptr,
      const CongestionControlFactory& congestionControlFactory = nullptr);
  virtual ~RoceSocketBase() override = default;
  RoceSocketBase(const RoceSocketBase&) = delete;
  RoceSocketBase& operator=(const RoceSocketBase&) = delete;
  RoceSocketBase(RoceSocketBase&&) = delete;
  RoceSocketBase& operator=(RoceSocketBase&&) = delete;

  void send(Message* msg) override;
  /**
   * Post a receive for `msg`.
   *
   * @param msg  The message to receive.
   *
   * @throw std::invalid_argument if `msg->isRead`: RoCEv2 does not model RDMA
   *        READ.
   * @throw std::runtime_error if the receive queue already holds maxQpDepth
   *        messages.
   */
  void recv(Message* msg) override;

  virtual void onPacketSent(const PacketPtr& pkt) override;
  virtual void onPacketRcvd(const PacketPtr& pkt) override;
  void onPacketWrittenToMemory(const PacketPtr& pkt) override;

  void sendPkt(
      const PacketPtr& pkt,
      bool callbackOnSend = false,
      bool isAck = false,
      bool isNack = false,
      bool isCnp = false);
  virtual void fetchDataAndSend(
      uint32_t bytes,
      uint32_t sequenceNum,
      bool endOfMsg,
      bool endOfQuotaChunk,
      bool isWrite = false,
      bool isWriteImm = false,
      uint32_t writeImmData = 0,
      uint32_t sendOrWriteData = 0);

  virtual std::tuple<bool, uint32_t> sendBytes(uint32_t quota);

  size_t queuedMsgs() const noexcept {
    return queuedMsgs_.size();
  }

  size_t expectedMsgs() const noexcept {
    return expectedMsgs_.size();
  }

  bool hasWork() const {
    return !isQpFailed() &&
        (!queuedMsgs_.empty() ||
         (errorRecovery_ && errorRecovery_->hasRetransmitPending()));
  }

  bool outOfOrderPktSupport() const noexcept {
    return outOfOrderPktSupport_;
  }

  // Access error recovery component (may be null).
  ErrorRecovery* FOLLY_NULLABLE getErrorRecovery() const noexcept {
    return errorRecovery_.get();
  }

  // Access congestion control component (may be null).
  const CongestionControl* FOLLY_NULLABLE
  getCongestionControl() const noexcept {
    return congestionControl_.get();
  }

  // Helpers exposed for ErrorRecovery strategies to call back into the
  // socket. The strategy owns timer scheduling; the socket only handles
  // packet emission and wakeup signalling.

  /**
   * Emit a NACK packet for the given gap.
   *
   * @param seqNum         Sequence number of the message containing the gap.
   * @param psn            Expected PSN to include in the NACK.
   * @param rnrBackoffIdx  RNR back-off-table index, or nullopt for none.
   */
  void emitNackPacket(
      uint32_t seqNum,
      uint32_t psn,
      std::optional<uint8_t> rnrBackoffIdx) override;

  // Mark this socket as having work to send and notify the transport. This is
  // the single final overrider for both ErrorRecoverySocket::wakeupForSend and
  // CongestionControlSocket::wakeupForSend (same signature).
  void wakeupForSend() override;

  // Emit one CNP toward the sender (CongestionControlSocket interface).
  void emitCongestionNotification() override;

 protected:
  // True if the attached recovery strategy reports that the QP is in ERR.
  bool isQpFailed() const noexcept {
    return errorRecovery_ && errorRecovery_->isFailed();
  }

  virtual void scheduleSend(uint64_t delayNs, const SendInfo& info);
  virtual void ackRcvd(uint32_t seqNum, uint32_t psn);
  virtual void nackRcvd(uint32_t expectedPsn, uint8_t nackTime);
  virtual void cnpRcvd();
  // Count, in this socket's own stats, a packet it sent that left the NIC
  // carrying the ECN-CE (congestion experienced) mark. The transport counts the
  // per-CoS stat.
  void countEcnSent() noexcept;
  /**
   * Count a received packet carrying the ECN-CE mark, per socket and under
   * the CoS this NIC uses for its packet type.
   *
   * @param pkt  The received, CE-marked packet.
   */
  void countEcnRcvd(const RocePacket& pkt);
  /**
   * Check a received PSN and apply the resulting receive-path side effects
   * (deferred-ACK release, duplicate ACK, NACK request).
   *
   * @param seqNum        Sequence number of the received packet.
   * @param psn           PSN of the received packet.
   * @param ackRequested  In/out: the packet's ACK request. CLEARED when the
   *                      ACK is deferred instead of sent now, so the caller
   *                      must not assume it survives the call.
   *
   * @return              True if the caller should keep processing the packet.
   */
  virtual bool isPsnOk(uint32_t seqNum, uint32_t psn, bool& ackRequested);
  virtual void sendRnr(uint32_t psn);
  virtual void createAndSendPkt(const SendInfo& info);
  virtual RoceTransportBase* transport() const;
  void emitAckPacket(uint32_t seqNum, uint32_t psn);
  void roceEncap(const PacketPtr& pkt, const SendInfo& info, uint32_t psn)
      const;
  void udpEncap(const PacketPtr& pkt) const;
  void ipv6Encap(const PacketPtr& pkt, const SendInfo& info) const;
  void writeImmEncap(const PacketPtr& pkt, const SendInfo& info) const;

  // Execute a retransmit batch from error recovery.
  uint64_t executeRetransmit(uint64_t quota);

 protected:
  friend class RoceTransportBase;

  uint32_t msgBytesSent_{0};
  uint32_t txPsn_{1};
  uint32_t expectedPsn_{1};
  uint8_t dataCos_{0};
  uint8_t ackCos_{0};
  uint8_t cnpCos_{0};
  bool outOfOrderPktSupport_{false};
  // Symmetric device-pair key cached for the per-packet port lookup.
  const uint64_t egressPortKey_;
  std::list<Message*> expectedMsgs_;
  std::list<Message*> queuedMsgs_;
  std::list<Message*> sentMsgs_;
  uint32_t rxWritesReceived_{0};
  folly::F14FastMap<PacketPtr, Message*> finalPacketToMsg_;
  uint32_t nextSendCqeSequenceNum_{0};
  uint32_t nextRecvCqeSequenceNum_{0};
  folly::F14FastMap<uint32_t, Message*> completedSendMsgs_;
  folly::F14FastMap<uint32_t, Message*> completedRecvMsgs_;
  folly::F14FastMap<size_t, uint32_t> bytesReceivedPerMsg_;

  Message* removeSentMessage(uint32_t seqNum);
  void completeAckedMessages(uint32_t seqNum, const AckResult& result);

  // Cancel congestion control timers when all send and receive work is done.
  void cancelTimersIfComplete();

  // Strategy components (null = disabled).
  std::unique_ptr<ErrorRecovery> errorRecovery_;
  std::unique_ptr<CongestionControl> congestionControl_;
};

class RoceTransportBase : public Transport {
 public:
  /**
   * Build a RoCE transport for one device.
   *
   * @param simCtx     The simulation context.
   * @param id         The device this transport belongs to.
   * @param nicConfig  The NIC config.
   * @param roceCfg    The error recovery and congestion control selection.
   *
   * @throw std::invalid_argument  If `roceCfg` selects Go-Back-N with an
   *                               `rnrTimeIdx` outside the RNR timer table.
   * @throw std::invalid_argument or std::runtime_error  If `roceCfg` selects
   *        DCQCN and DcqcnCongestionControl::validateConfig rejects the NIC
   *        config's `dcqcnCfg`.
   */
  explicit RoceTransportBase(
      SimContext* simCtx,
      const FlexDeviceId& id,
      const packsim::thrift::NicConfig* FOLLY_NONNULL nicConfig,
      const RoceTransportConfig& roceCfg = {});
  virtual ~RoceTransportBase() override = default;
  RoceTransportBase(const RoceTransportBase&) = delete;
  RoceTransportBase& operator=(const RoceTransportBase&) = delete;
  RoceTransportBase(RoceTransportBase&&) = delete;
  RoceTransportBase& operator=(RoceTransportBase&&) = delete;

  Socket* connect(
      const FlexDeviceId& dst,
      uint16_t port,
      uint32_t localQPN,
      uint32_t remoteQPN,
      uint8_t priority = 0,
      const packsim::workload::thrift::Workload* workload = nullptr) override;

  /**
   * Register an egress port this transport may send on.
   *
   * `Endpoint::addPort` calls this for every port of an endpoint with a port
   * index (`EndpointConfig.portIdx > 0`), and only for the first port of one
   * without. Owning several ports does not make a single connection faster,
   * though: see `sendPacket` and the multi-port section of
   * `transport/DESIGN.md`.
   *
   * @param egressPort        0-based egress NIC port number.
   * @param ethPort           Unused; the send path goes through
   *                          @p portSendPacketFn.
   * @param portSendPacketFn  Function that transmits a packet on
   *                          @p egressPort.
   */
  void addPort(
      uint16_t egressPort,
      EthernetPort* ethPort,
      const SendPacketFn& portSendPacketFn) override;

  void socketReady(RoceSocketBase* socket);

  /**
   * Transmit a packet, choosing an egress port when more than one is
   * registered.
   *
   * The port slot is keyed on the source and destination device IDs, so every
   * QP between the same device pair is pinned to one port. When both endpoints
   * register the same ordered port set, both directions share that port. That
   * keeps a QP's packets on a single path and makes selection independent of
   * the QPN allocator. Aggregate port use therefore comes from communicating
   * with several device peers, not from opening several QPs to one peer. See
   * the multi-port section of `transport/DESIGN.md`, and
   * `MultiPortRoceV2Test.SingleQpPairDoesNotSpreadAcrossPorts`, which fails if
   * it changes.
   *
   * With no ports registered the packet goes to the single `sendPacketFn_`.
   *
   * @param socket  Socket the packet belongs to; supplies the device IDs.
   * @param pkt     Packet to send. A `RocePacket` is stamped with the chosen
   *                egress and destination NIC port.
   *
   * @return        Whatever the selected send callback returns.
   */
  bool sendPacket(RoceSocketBase* socket, const PacketPtr& pkt);

  /**
   * Pick the egress port slot for a device pair, symmetrically.
   *
   * Both directions of a pair must select the same slot when the endpoints
   * have the same port count: the destination address is rewritten from
   * dstNicPort, and the port-to-switch wiring is planar, so a packet leaving
   * src port N can only be delivered to dst port N.
   *
   * Keyed on device identity rather than queue-pair numbers, whose allocation
   * stride would otherwise leave ports unreachable. The component sum assumes
   * the supported topology IDs enumerate peers densely; it is not a
   * collision-free hash for arbitrary sparse or strided ID sets.
   *
   * @param srcId       Source NIC device ID.
   * @param dstId       Destination NIC device ID.
   * @param numPorts    Number of egress ports available at each endpoint; must
   *                    be non-zero and equal in both directions.
   *
   * @return            Slot index in [0, numPorts).
   */
  static size_t egressPortSlot(
      const FlexDeviceId& srcId,
      const FlexDeviceId& dstId,
      size_t numPorts) noexcept;

  bool sendPacketWithCallback(RoceSocketBase* socket, const PacketPtr& pkt);
  bool writePacketToMemory(RoceSocketBase* socket, const PacketPtr& pkt);
  void onPacketWrittenToMemory(const PacketPtr& pkt);

  bool onPacketSent(const PacketPtr& pkt, EthernetPort* ethPort) override;
  void onPacketDropped(const PacketPtr& pkt) override;
  bool onPacketRcvd(const PacketPtr& pkt, EthernetPort* ethPort) override;

  packsim::thrift::NicSummary getNicStatsSummaryResults() const override;

  /**
   * Always reports complete: it does not check its sockets' message lists. A
   * check would have to ignore the notify receives that stay posted after a
   * completed run. An unfinished run is reported by the workload-rank and
   * byte-count checks in PackSimSimulation::verifySimulationMetrics().
   *
   * @return  true.
   */
  bool verifyComplete() override {
    return true;
  }

 protected:
  friend class RoceSocketBase;

  /**
   * The CoS this NIC sends a packet of `pkt`'s type on, as `sendPkt` and
   * `ipv6Encap` choose it.
   *
   * @param pkt  The packet whose type decides the CoS.
   *
   * @return     ackCos_ for an ACK or NACK, cnpCos_ for a CNP, else dataCos_.
   */
  uint8_t getCosForPacketType(const RocePacket& pkt) const noexcept;

  /**
   * Count a RoCE packet that left this NIC carrying the ECN-CE (congestion
   * experienced) mark: always under the per-CoS stat of its type, and in its
   * sending socket's stats only while that socket is still registered under
   * the packet's own connection.
   *
   * @param pkt  The sent, CE-marked packet.
   */
  void countEcnSent(const RocePacket& pkt);

  virtual RoceSocketBase* makeSocket(
      const FlexDeviceId& dst,
      uint32_t localQPN,
      uint32_t remoteQPN,
      uint16_t port,
      const packsim::workload::thrift::Workload* workload = nullptr);

  virtual void sendNext();
  bool trySendOnSocket(RoceSocketBase* socket);
  static size_t egressPortSlot(
      uint64_t devicePairKey,
      size_t numPorts) noexcept;

  NicRxBuffer* queueToMemory(EthernetPort* ethPort);
  NicRxBuffer* queueToMemory(uint16_t portNum);
  std::pair<int, bool> queuePacketToMemory(const PacketPtr& pkt);

  folly::F14FastMap<uint16_t, std::unique_ptr<NicRxBuffer>>
      queueToMemoryPerPort_;
  NicRxBuffer defaultQueueToMemory_;

  folly::F14FastMap<PacketPtr, RoceSocketBase*> packetTxCbSocket_;
  uint32_t nextDst_{0};
  uint32_t activeSends_{0};
  uint32_t maxActiveSends_{8};
  uint32_t byteQuota_{65536};
  // The send and receive queue limit of each socket. It is empty when the
  // queues are unlimited.
  std::optional<size_t> maxQpDepth_;

  // The CoS values from the NIC config. The constructor checks that each one
  // fits uint8_t.
  const uint8_t dataCos_;
  const uint8_t ackCos_;
  const uint8_t cnpCos_;

  // Configuration for socket creation.
  RoceTransportConfig roceCfg_;

  // Encapsulation overhead in bytes (computed in constructor).
  uint32_t encapBytes_{0};

  // Active socket tracking for round-robin scheduling.
  folly::F14FastMap<RoceSocketBase*, uint32_t> socketIndices_;
  std::map<uint32_t, RoceSocketBase*> activeRoceSockets_;
  std::optional<uint32_t> nextSocketIndex_;

  // Sorted by physical port number so QP-to-port assignment is deterministic.
  std::map<uint16_t, SendPacketFn> portSendPacketFns_;
};

inline std::ostream& operator<<(std::ostream& os, const RoceSendInfo& info) {
  os << "sendInfo " << "psn: " << info.psn
     << ", sequenceNum: " << info.sequenceNum << ", isAck: " << std::boolalpha
     << info.isAck;
  return os;
}

} // namespace packsim
