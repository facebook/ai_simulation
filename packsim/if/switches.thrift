/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// @oss-disable
include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

@thrift.AllowLegacyMissingUris
package;

namespace cpp2 packsim.switches.thrift
namespace py3 packsim.switches

// Unsigned integer types. Each keeps the signed wire type of its width and
// generates the unsigned C++ type of that width, so a field changed to one
// still reads data written before the change.
@cpp.Type{name = "::std::uint8_t"}
typedef byte uint8
@cpp.Type{name = "::std::uint16_t"}
typedef i16 uint16
@cpp.Type{name = "::std::uint32_t"}
typedef i32 uint32
@cpp.Type{name = "::std::uint64_t"}
typedef i64 uint64

typedef list<uint32> FlexDeviceId

enum PfcState {
  XOFF = 0,
  XON = 1,
}

struct BufferStats {
  1: uint64 cellSizeBytes = 1;

  2: uint64 bufPkts = 0;
  3: uint64 watermarkBufPkts = 0;

  4: uint64 availCells = 0;
  5: uint64 usedCells = 0;
  6: uint64 watermarkCells = 0;

  7: uint64 availQueueRsvdCells = 0;
  8: uint64 usedQueueRsvdCells = 0;
  9: uint64 watermarkQueueRsvdCells = 0;

  10: uint64 availPortRsvdCells = 0;
  11: uint64 usedPortRsvdCells = 0;
  12: uint64 watermarkPortRsvdCells = 0;

  13: uint64 availSharedCells = 0;
  14: uint64 usedSharedCells = 0;
  15: uint64 watermarkSharedCells = 0;

  16: uint64 availHdrmCells = 0;
  17: uint64 usedHdrmCells = 0;
  18: uint64 watermarkHdrmCells = 0;

  21: uint64 admittedPkts = 0;
  22: uint64 admittedBytes = 0;

  19: uint64 droppedPkts = 0;
  20: uint64 droppedBytes = 0;

  27: uint64 wredDroppedPkts = 0;
  28: uint64 wredDroppedBytes = 0;

  29: uint64 ecnMarkedPkts = 0;

  // Nanoseconds this queue sat empty after its first packet arrived, summed
  // over every idle period that a later packet ended. The idle period still
  // open when the simulation stops is deliberately not counted: nothing ever
  // closes it, and charging it would report a queue as idle for the interval
  // in which it simply had no more work.
  23: uint64 totalBubbleTimeNs = 0;
  // When this queue first held a packet, and so where the window the two
  // fields below are measured over begins. 0 if no packet ever arrived, in
  // which case the window is empty and all four of these fields are 0.
  24: uint64 firstDataTimeNs = 0;
  // Where that window ends. While packets are still buffered it is the
  // instant the result was serialized, since the queue has been busy since
  // its last refill; once the queue has drained it is that final drain, which
  // is what keeps the trailing idle period out of the window.
  25: uint64 lastDataTimeNs = 0;
  // `totalBubbleTimeNs` over the window, so 0 means the queue was never idle
  // and 1 would mean it was never busy. 0 when the window is empty.
  26: double bubbleFrac = 0;
}

struct Queue {
  1: i32 portNum = -1;
  2: i32 queueNum = -1;
  3: double alpha = 0;
  4: optional uint64 xonHystCells;
  5: bool canUsePortRsvd = true;
  6: bool dynamicSharing = true;
  7: bool pfc = true;
  8: bool lossless = true;
  9: PfcState pfcState = XON;
  10: optional uint64 resumeOffsetCells;

  20: BufferStats bufStats;
}

struct Port {
  1: i32 portNum = -1;
  10: BufferStats bufStats;
  30: list<Queue> queues;
}

struct Buffer {
  1: BufferStats bufStats;
  2: list<Port> ports;
  3: uint32 bufferIdx;
}

// LLR (UEC 1.0.3 §5.1) statistics. Field IDs are stable for on-disk
// backward compatibility; see per-field comments for current semantics.
//
// Id 3 held a retry-exhaustion counter that was removed; it is reserved so
// the stability promise above holds for it too.
@thrift.ReserveIds{ids = [3]}
struct LlrStats {
  // Retry attempts — count of REPLAY-state entries (one per batch).
  // The per-frame replay count (`stats.txReplay`) is finer-grained and
  // not exposed through this field.
  1: uint64 retries = 0;
  // Count of LLR-eligible frames transmitted with a good FCS
  // (`stats.txOk`). This is a total successful-LLR-transmission count,
  // not a post-retry-only count.
  2: uint64 successes = 0;
  // ACKs sent by receiver
  4: uint64 acksSent = 0;
  // ACKs received by sender
  5: uint64 acksRcvd = 0;
  // NACKs sent by receiver
  6: uint64 nacksSent = 0;
  // NACKs received by sender
  7: uint64 nacksRcvd = 0;
  // Frames dropped due to TX buffer full. The LLR module never drops
  // admitted frames — backpressure leaves them in the upstream queue —
  // so this field stays at 0 in current traces.
  @thrift.Deprecated{
    message = "LLR module never drops admitted frames; backpressure leaves frames in the upstream queue. Stays 0 in current traces.",
  }
  8: uint64 txBufferDrops = 0;
  // Current TX buffer occupancy (packets)
  9: uint64 txBufferSize = 0;
  // Current TX buffer occupancy (bytes)
  10: uint64 txBufferBytes = 0;
  // Peak TX buffer occupancy (packets)
  11: uint64 txBufferPeakPackets = 0;
  // Peak TX buffer occupancy (bytes)
  12: uint64 txBufferPeakBytes = 0;
  // Frames discarded per the INIT/FLUSH DISCARD behavior
  // (`stats.txDiscard`).
  13: uint64 txDiscard = 0;
  // INIT/FLUSH state-entry counters (`stats.txInitEntries` /
  // `stats.txFlushEntries`).
  // Number of TX SM entries into INIT
  14: uint64 initCount = 0;
  // Number of TX SM entries into FLUSH
  15: uint64 flushCount = 0;
  // LLR_INIT ordered sets sent
  16: uint64 initsSent = 0;
  // LLR_INIT ordered sets received
  17: uint64 initsRcvd = 0;
  // LLR_INIT_ECHO ordered sets sent
  18: uint64 initEchosSent = 0;
  // LLR_INIT_ECHO ordered sets received
  19: uint64 initEchosRcvd = 0;
  // Underlying value of `llr::TxState` (defined in `llr-types.h` with
  // explicit IDs):
  // 0=UNKNOWN, 1=LLR_OFF, 2=INIT, 3=ADVANCE, 4=REPLAY, 5=FLUSH.
  20: uint32 txState = 0;
  // Underlying value of `llr::AckNackState` (defined in `llr-types.h`
  // with explicit IDs):
  // 0=UNKNOWN, 1=OFF, 2=SEND_ACKS, 4=NACK_SENT (3, the spec's transient
  // SEND_NACK, is never stored).
  21: uint32 ackNackState = 0;
  // Bytes retransmitted on replay (`txReplayBytes`)
  22: uint64 retryBytes = 0;
}

// CBFC (Credit-Based Flow Control, UEC 1.0.3 §5.2) statistics.
//
// Every counter here is per-port, not per-queue.
// `EthernetPort::getAllPortStats` stamps one per-port snapshot onto
// every (port, queue) row, as it does for `LlrStats`, so consumers must
// aggregate with `max` or take one row per port; summing across queues
// multiplies the counts.
//
// `@thrift.TerseWrite` must stay at struct scope: codegen makes
// `__fbthrift_is_empty()` return false for any struct holding an unqualified
// field, so the field-level annotation on `InterfaceStats.cbfcStats` alone
// would never skip this struct.
@thrift.TerseWrite
struct CbfcStats {
  // ----- Message counters -----
  // CF_Update CtlOS sent by this port in its receiver role.
  1: uint64 cfUpdatesSent = 0;
  // CF_Update CtlOS received by this port in its sender role.
  2: uint64 cfUpdatesRcvd = 0;
  // CC_Update packets sent by this port in its sender role.
  3: uint64 ccUpdatesSent = 0;
  // CC_Update packets received by this port in its receiver role.
  4: uint64 ccUpdatesRcvd = 0;
  // S_VC_CC entries rejected because LostCredits exceeded 2^14-1, a serious
  // error per UEC §5.2.5.7.4. One increment per VC entry, not per CC_Update
  // packet.
  5: uint64 ccUpdateLostCreditsOutOfRangeCount = 0;

  // ----- Credit counters (in credits) -----
  // Credits this port charges to S_VC_CC as it transmits.
  10: uint64 creditsConsumed = 0;
  // Credits this port releases into R_VC_CF as the receiver. A CBFC-managed
  // switch queue releases the uncredited share at ENQUEUE and the headroom
  // only at drain or drop; the NIC receive buffer releases the whole cost at
  // drain. Excludes CC_Update reconcile reclaim, counted in
  // `leakedCreditsReclaimed`.
  11: uint64 creditsFreed = 0;
  // Credits the link partner's CF_Update CtlOS returns to this port acting as
  // the sender: the S_VC_CF advance of UEC §5.2.5.6.3. Credits this port gets
  // back, not credits it gives away.
  12: uint64 creditsReturned = 0;
  // Credits recovered by the UEC §5.2.5.7.4 CC_Update reconcile, which adds
  // LostCredits to R_VC_CF and resyncs R_VC_CC to the sender's S_VC_CC. Not
  // included in `creditsFreed`.
  13: uint64 leakedCreditsReclaimed = 0;

  // ----- Correctness invariants -----
  // BUFFER_FULL drops on lossless VCs. MUST stay 0 when CBFC operates
  // correctly.
  20: uint64 losslessVcBufferFullDrops = 0;

  // ----- Gate events (counts of occurrences, NOT credits) -----
  // Times a lossless VC was skipped at the TX gate for insufficient credits.
  30: uint64 gateBlockedCount = 0;
}

struct InterfaceStats {
  1: uint64 txPkts = 0;
  2: uint64 txBytes = 0;
  3: uint64 rxPkts = 0;
  4: uint64 rxBytes = 0;

  10: uint64 txXon = 0;
  11: uint64 txXoff = 0;
  12: uint64 txPauseDurationNs = 0;
  13: uint64 rxXon = 0;
  14: uint64 rxXoff = 0;
  15: uint64 rxPauseDurationNs = 0;

  16: uint64 ecnCeMarkedPkts = 0;
  17: uint64 rxEcnCePkts = 0;
  18: uint64 txEcnCePkts = 0;
  // Packets received with bit 7 of the IPv6 hop limit set. RoCE sets it on
  // retransmissions only, so on RoCE traffic this counts retransmitted
  // packets; an IPv6 packet whose sender keeps the default hop limit of 255
  // counts too.
  19: uint64 rxTtldPkts = 0;
  // As rxTtldPkts, for packets sent.
  20: uint64 txTtldPkts = 0;

  21: uint64 dropPkts = 0;
  22: uint64 dropBytes = 0;

  // LLR statistics (if LLR enabled)
  30: optional LlrStats llrStats;
  // Not `optional`: a port with CBFC disabled reports zeros rather than a
  // missing value, so "off" is indistinguishable from "ran and counted
  // nothing". Terse write keeps the all-zero case off the wire.
  @thrift.TerseWrite
  31: CbfcStats cbfcStats;
}

// Reasons why a packet is dropped
enum PacketDropReason {
  UNKNOWN = 0,
  NO_ROUTE = 1, // No routing path found
  BUFFER_FULL = 2, // Buffer admission failure
  WRED = 3, // WRED-based drop
  TTL_EXPIRED = 4, // Time-to-live expired
  LINK_ERROR = 5, // Link-level error (Link down)
  BER_ERROR = 6, // Uncorrectable packet data corruption, detected by CRC and uncorrectable through FEC
  OUT_OF_SEQUENCE_ERROR = 7, // Sequence number error
  DUPLICATE_SEQUENCE_ERROR = 8, // Duplicate sequence number
}

// Tracks drops by reason, packet type, and time
struct PacketDropEvent {
  1: PacketDropReason dropReason;
  2: string packetType = "";
  3: uint64 droppedPkts = 0;
  4: uint64 droppedBytes = 0;
  5: uint64 eventTimeNs = 0;
  // Port number where the drop occurred
  6: uint32 portNum;
  // ID of the incident that caused this drop (0 = not incident-related)
  7: uint32 incidentId;
}

@thrift.ReserveIds{ids = [40]} // @oss-enable
struct Switch {
  1: string name;
  2: FlexDeviceId flexId;
  // Map key is port #, list is one per COS
  10: map<i32, list<InterfaceStats>> intfStats;
  // Map key is port #, value is remote switch FlexId
  11: map<i32, FlexDeviceId> portPartners;
  // Map key is port #, value is remote switch port number
  12: map<i32, i32> portPartnerPortNums;
  20: list<Buffer> ingressBuffers;
  21: list<Buffer> egressBuffers;
  // Not populated by the simulator: always empty. `egressBuffers`, filled
  // when `SwitchConfig.useEgress` is set, holds each egress buffer's queues,
  // per port.
  22: list<Queue> egressQueues;
  30: list<PacketDropEvent> packetDropEvents;
  // @oss-disable
}
