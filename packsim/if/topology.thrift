/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// @oss-disable
include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

cpp_include "folly/container/F14Map.h"

// @oss-disable
@thrift.AllowLegacyMissingUris // @oss-enable
package; // @oss-enable

namespace cpp2 packsim.thrift
namespace py3 packsim

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

// Flexible Topology
typedef uint64 LinkId // Straightforward counter

@cpp.Type{template = "std::vector"}
typedef list<uint32> PrefixId // Ipv6 prefix quartets, ish

// Index of a CBFC virtual channel. Valid range is 0..`CbfcConfig.numVcs - 1`,
// enforced for every `CbfcConfig` field by `validateCbfcConfig` in
// `link-layer/cbfc/cbfc-config.cpp` when the topology builds the link.
typedef uint8 VcId

// Raised while routing a packet through a FlexNode when it has no usable link
// toward the packet's destination: the LPM lookup returns no links, every
// matching link is down, or the `fly` tiebreak takes the shortest path and no
// link's far end is in the destination's pod.
// @oss-disable
// Purely a function of the simulated topology and the packet, so the same
// input reproduces it. FbSwitch catches this and converts it into a NO_ROUTE
// packet drop for retransmitting transports; other packet types let it
// propagate.
// @oss-disable
// @oss-disable
// @oss-disable
// @oss-disable
// @oss-disable
safe exception PackSimRoutingException {
  @thrift.ExceptionMessage
  1: string message;
  2: optional string nodeName;
  3: optional string destinationId;
}

struct EcmpTiebreakSpec {}

// Picks a random link for each packet, so packets of one QP can arrive out of
// order: with the RoCEv2 transport, the receiving NICs need
// NicConfig.outofOrderPktSupport.
struct SprayTiebreakSpec {}

struct PathSprayTiebreakSpec {}

struct DprTiebreakSpec {
  1: list<uint32> dotProduct;
  // Must be positive.
  2: uint32 divScale = 1;
}

struct ReflectionTiebreakSpec {}

struct DragonFlyPlusTiebreakSpec {
  1: uint32 maxAdditionalHops = 0;
  2: string secondTieBreak = "ECMP";
}

@thrift.ReserveIds{ids = [3]} // @oss-enable
union TiebreakSpec {
  1: EcmpTiebreakSpec ecmp;
  2: DprTiebreakSpec dpr;
  // @oss-disable
  4: ReflectionTiebreakSpec reflect;
  5: SprayTiebreakSpec spray;
  6: DragonFlyPlusTiebreakSpec fly;
  7: PathSprayTiebreakSpec pathspray;
}

struct LpmTableEntry {
  1: list<LinkId> links;
  2: TiebreakSpec tiebreak;
}

struct LpmTableSpec {
  // Map prefix to list of LinkIds
  // Node this LpmTableSpec is on should be one of the two nodes in every link
  1: LpmTableEntryByPrefix prefixTable;
  // List of valid map key lengths
  2: list<uint32> validPrefixLengths;
  3: LpmTableEntry defaultRoute;
}

enum NodeType {
  ACCEL = 0,
  SWITCH = 4,
}

// The `Simulation.nicConfigMap` key of the NIC config that an endpoint uses
// unless its `nicConfig` names another, and that an ACCEL node without
// `endpointCfgs` uses. When `nicConfigMap` is empty, building the simulation
// copies `Simulation.nic` into it under this key.
const string kDefaultNicConfig = "default";

// TODO: Modify FlexNode::routePacket to accept a list of allowed outputs to tiebreak across
// This will allow us to use the routePacket functionality in the endpoint to choose one of
// the allowed
struct EndpointConfig {
  1: string nicConfig = kDefaultNicConfig;
  2: list<uint32> linkIds;
  // Each entry is the endpoint's NIC address, e.g. [1, 0, 1, 1, 1]. Every
  // entry must name that same address; builders repeat it once per link.
  3: list<PrefixId> nicIds;
  4: LpmTableSpec lpmSpec;
  // Not read by the simulator.
  // @oss-disable
  5: list<PrefixId> portIds;
  // Index of port field in the FlexDeviceId
  6: uint8 portIdx;
}

struct Node {
  // Name should be globally unique and spec for the node (accel or switch)
  // should be elswhere in the larger Simulation object
  1: string name;
  2: PrefixId id;
  3: LpmTableSpec lpmSpec;
  4: NodeType type;
  5: string role;
  // Not read by the simulator.
  10: i32 systemId = 0;
  // If more than one of these is provided, workload must select the correct "NIC"
  11: list<EndpointConfig> endpointCfgs;
  // Not read by the simulator; workloads take NV-domain membership from
  // `Workload.rankToNvDomain`.
  // @oss-disable
  // @oss-disable
  12: i32 nvDomain;
}

enum LinkStatus {
  DOWN = 0, // link not working
  UP = 1, // link working
}

struct FlexLinkDelayConfig {
  1: uint64 delayNs;
  2: double expDelayNs = 0;
}

@thrift.ReserveIds{ids = [4]}
struct LlrConfig {
  1: uint32 ackByteThreshold = 8192;
  // 512KB
  2: uint64 txBufferMaxBytes = 524288;
  // 100 microseconds
  3: uint64 retryTimeoutNs = 100000;
  // Maximum consecutive replay windows without forward progress before FLUSH
  // 0 = unlimited (never enter FLUSH based on replay count)
  5: i64 replayCtMax = 0;
  // Start LLR on both ends of the link in its post-handshake state instead of
  // exchanging LLR_INIT / LLR_INIT_ECHO at startup; not part of the UEC spec.
  // Defaults to true, so a link with an llrConfig skips the handshake unless
  // this is set to false.
  6: bool skipInitNegotiation = true;
  // What the TX state machine does after FLUSH discards the replay buffer.
  // true: re-enter INIT, renegotiate with the peer and resume. false: stay
  // in FLUSH until `Llr::managementReset()`, which nothing in the simulator
  // calls, so the port sends no further data frames for the rest of the run.
  // FLUSH is reachable only when replayCtMax > 0 or dataAgeTimerMaxNs > 0.
  7: bool reInitOnDiscard = true;
  // Data age timer maximum in nanoseconds (0 = disabled)
  8: uint64 dataAgeTimerMaxNs = 0;
  // PCS lost status timer maximum in nanoseconds (0 = disabled)
  9: uint64 pcsLostStatusTimerMaxNs = 0;
  // Ignored; see the deprecation message.
  @thrift.Deprecated{
    message = "Not consumed by the LLR module. ackByteThreshold (field 1, bytes) maps to UEC ctlos_target_spacing; the LLR module derives the LLR_INIT period internally as 4 * ctlos_target_spacing / link_rate per UEC 1.0.3 sec 5.1.9.1.1.",
  }
  10: uint64 initCtlosSpacingNs = 10000;
}

// How a packet's header fields select its virtual channel (VC).
// UEC 1.0.3 §5.2.5.2 and §5.3.2.2.15.4 (R_PktID_Sel).
enum VcMapSelector {
  // Match on L2 PCP/DEI bits: member iff (pcpDei & mask) == value. PackSim
  // carries no VLAN PCP/DEI field, so `Cbfc::losslessVcOf` substitutes the
  // DSCP class selector (dscp >> 3): the field is only ever 0..7, and a rule
  // requiring the 0x8 bit can never match. mask=0x7, value=0x5 selects CS5.
  PCP_DEI = 0,
  // Match on L3 DSCP bits: member iff (dscp & mask) == value.
  DSCP = 1,
}

// One packet->VC mapping rule (UEC §5.2.5.2). Rules are evaluated in order;
// the first match assigns the VC. A packet matching no rule is treated as
// best-effort (not credit-gated).
struct VcMapEntry {
  1: VcMapSelector selector = PCP_DEI;
  // Mask applied to the selected field before comparison. PCP_DEI: 4-bit
  // mask (0..0xF); DSCP: 6-bit mask (0..0x3F).
  2: uint32 mask = 0;
  // Value compared against (field & mask). A default-constructed entry
  // (mask = 0) matches every packet: it is a match-all rule assigning vcId,
  // not a no-op, and being first-match it swallows all traffic.
  3: uint32 value = 0;
  // VC index assigned on match. See `VcId` for the range.
  4: VcId vcId = 0;
}

// One VC group: the VCs sharing a burst-absorption pool, plus that pool's limit
// in credits. The group id is this entry's index in `CbfcConfig.vcGroups`. VC
// groups are a PackSim extension; UEC 1.0.3 §5.2.5.2 leaves how the sender
// divides its pool to the implementation.
struct VcGrpEntry {
  // VC ids in this group: at most `CbfcLimits::maxVcsPerGroup`
  // (link-layer/cbfc/cbfc-limits.h), and no id may appear in a second group.
  // Once any group exists, every lossless VC must be in one --
  // `requireEveryLosslessVcGrouped` rejects a config that leaves one out. A
  // best-effort VC listed in a group is accepted and never reaches the gate.
  1: list<VcId> vcIds;
  // Credits this group may draw from the shared pool. A limit of 0 confines
  // its members to their own `vcMinCredits`.
  2: uint64 limitCredits = 0;
}

// Credit-Based Flow Control configuration (UEC 1.0.3 §5.2). Per-link; attach
// to FlexLink. Receiver-derived parameters (TotalCredits, and CreditSize
// itself) come from each side's own receiver buffer at init, not from this
// struct -- see the "Initialization" section of link-layer/cbfc/cbfc.h and
// `resolveLinkCreditSizeBytes` in topology/flex-topology.cpp.
// `isLosslessPerVc` must mark at least one VC lossless, so attaching a
// default-constructed `CbfcConfig` to a link aborts topology construction
// rather than installing a no-op gate. `vcMap` may legally be empty, in which
// case the credit gate is installed but every packet bypasses it.
struct CbfcConfig {
  // Number of virtual channels on the link, 1..`CbfcLimits::maxVcs`
  // (link-layer/cbfc/cbfc-limits.h); §5.2.5.2 allows 1 <= NumVCs <= 32.
  // Tbl 5-27's 2..32 is what an implementation must be able to SUPPORT, not a
  // floor on any one link, so the single-VC default is conformant.
  1: uint8 numVcs = 1;
  // Lossless (credit-gated) disposition per VC, indexed by VC id; a false
  // entry is best-effort and bypasses the credit gate. Exactly `numVcs`
  // entries, and at least one must be true -- Tbl 5-27 requires
  // 1 <= (lossless VCs) <= NumVCs, and a link with none gates nothing. Dense
  // rather than a list of ids so it needs no translation, matching
  // `SwitchConfig.isLosslessPerPg`.
  2: list<bool> isLosslessPerVc;
  // Packet->VC mapping rules; see `VcMapEntry` for match semantics. Must be
  // configured identically on both link partners.
  3: list<VcMapEntry> vcMap;
  // Bytes represented by one credit (UEC §5.2.5.2 CreditSize). A property of
  // the LINK: both ends resolve to one value, because each uses it to charge
  // what it sends AND to count what it receives.
  //
  // 0 derives it from the receiver's cell size, falling back to
  // `CbfcLimits::defaultCreditSizeBytes` on links where neither end
  // quantizes. A non-zero value that disagrees with that cell size is
  // rejected, not overridden, by `resolveLinkCreditSizeBytes` in
  // `flex-topology.cpp`.
  10: uint32 expectedCreditSizeBytes = 0;
  // Per-packet buffer overhead in bytes, added before the credit roundup
  // (UEC §5.2.5.2 PktOvhd). Accepted values are
  // `CbfcLimits::minPktOvhdBytes`..`maxPktOvhdBytes` in steps of
  // `pktOvhdStepBytes`, a conservative subset of §5.2.10.2's -16..+127
  // (PackSim buffers store the whole frame, so a negative overhead is not
  // modelled).
  11: i32 pktOvhdBytes = 0;
  // CF_min_timer: minimum spacing between CF_Update CtlOS, in bytes of wire
  // time (UEC §5.2.5.7.3). The floor, 800 B, is a PackSim bound and also the
  // default.
  20: uint64 cfMinTimerBytes = 800;
  // CF_max_timer: maximum spacing between CF_Update CtlOS, in bytes of wire
  // time (Tbl 5-27: 16 kB .. 1 MB).
  21: uint64 cfMaxTimerBytes = 16384;
  // CC_msg_timer: spacing between CC_Update packets, in nanoseconds
  // (UEC §5.2.5.6.4). Range 1 us (Tbl 5-27) .. 250 ms (PackSim).
  22: uint64 ccMsgTimerNs = 100000;
  // Optional CF_Update batching, a PackSim extension with no UEC §5.2.5.7.3
  // counterpart: a drain makes its VC eligible only once this many credits
  // have been freed since that VC's last report. It BATCHES credit return
  // rather than hastening it. 0 = disabled, reproducing UEC trigger 1 exactly
  // (report on any R_VC_CF update, subject to CF_min).
  // Must be non-negative; there is no upper bound, and a large threshold merely
  // makes the trigger rare, leaving CF_max to carry the reports.
  // `CbfcSenderLedger::setPortCreditLimit` is what keeps a CF_Update's advance
  // inside the 15-bit R_VC_CF.
  23: uint64 cfCreditThresholdCredits = 0;
  // Whether CC_Update participates in LLR. UEC 1.0.3 §5.2.9 requires it:
  // "CC_Update packets must be LLR-eligible and carry a valid sequence
  // number", which is why the default is true. Set false to take CC_Update out
  // of LLR entirely (outside §5.2.9); it then carries neither a sequence
  // number nor a replay-buffer entry. There is deliberately no middle
  // setting -- a sequence number without a replay entry would leave a hole in
  // the peer's sequence space that nothing can ever fill.
  24: bool ccUpdateLlrEligible = true;

  // --- Sender credit reservations (ids 40+). `vcMinCredits` and
  // `overshootReserveCredits` are carve-outs from the pooled limit; the shared
  // pool is whatever remains after both. UEC §5.2.5.2 leaves the distribution
  // to the implementation, so these are configured rather than derived.

  // Credits dedicated to each VC, indexed by VC id. A VC can always transmit
  // into its own reserve however much of the shared pool its peers hold, which
  // is what keeps lossless VCs non-blocking with respect to each other. Either
  // empty (the default -- the whole pool stays shared, reproducing single-pool
  // gating) or exactly `numVcs` entries; a zero entry means that VC gets no
  // reserve. Dense rather than keyed by id so it needs no translation.
  40: list<uint64> vcMinCredits;

  // Credits withheld from the shared pool to absorb per-packet encapsulation
  // growth added after the credit gate.
  41: uint64 overshootReserveCredits = 0;

  // VC group membership and per-group limits: at most
  // `CbfcLimits::maxVcGroups` groups, each a burst-absorption pool with its
  // own limit. Empty -- the default -- disables the group tier entirely and
  // every lossless VC draws directly on the global shared pool. Group limits
  // are not required to fit inside the global shared pool, which is why the
  // global limit is still checked and why these do not enter the init
  // feasibility check.
  42: list<VcGrpEntry> vcGroups;
}

// UEC 1.0.3 §5.2.11.2 CtlOS insertion rules. The budget spans the COMBINED
// CtlOS stream on a port -- LLR ACK/NACK/INIT and CBFC CF_Update share it --
// so this is a link property rather than part of either LlrConfig or
// CbfcConfig. Both rules default to 0 = unenforced.
struct CtlosInsertionConfig {
  // Minimum wire distance between two CtlOS, in bytes. §5.2.11.2 keeps CtlOS
  // under 2% of link bandwidth by spacing them at least 400 B apart, and the
  // rule holds "even if there are no packets in transit and the CtlOS would be
  // separated only by IDLE characters" -- so this is wire time, not intervening
  // data bytes. Spec value 400; 0 disables.
  1: uint32 minSpacingBytes = 0;
  // Head-of-packet bytes a CtlOS may not preempt inside (§5.2.11.2: "CtlOS are
  // allowed to preempt a packet except: within the first 256 bytes of a
  // packet"). Spec value 256; 0 disables.
  2: uint32 noPreemptPrefixBytes = 0;
}

struct FlexLink {
  1: LinkId linkId;
  2: string node1;
  3: string node2;
  // Providing a value for sourceAddress sets the address on the EthernetPort this link is attaching to
  4: PrefixId sourceAddress;
  // Providing a value for destinationAddress sets the address on the EthernetPort this link is attaching to
  5: PrefixId destinationAddress;
  6: string node1PortGroup;
  7: string node2PortGroup;
  10: string speed;
  12: LinkStatus status = UP;
  // If this is >= 0, i.e. BER >= 1, bit errors are disabled
  20: double log10Ber = 1;
  30: i64 linkFlapDelayNs = 0;
  31: FlexLinkDelayConfig delayConfig;
  32: optional LlrConfig llrConfig;
  33: optional CbfcConfig cbfcConfig;
  34: optional CtlosInsertionConfig ctlosConfig;
}

// Switch Type
//
// Id 2 held DSF, which never had a switch implementation: the branch that
// would have built one was commented out, so a DSF topology produced a null
// switch. Reserved rather than reusable, because a stored config may still
// carry the value.
@thrift.ReserveIds{ids = [2]}
enum SwitchType {
  ETHERNET_IPV6 = 0, // Rdma over Converged Ethernet switches
}

struct FlexTopology {
  1: map<string, Node> nodes;
  2: map<LinkId, FlexLink> links;
  // Not read by the simulator, and the C++ builders under `util/` leave it at
  // 0. Intended as the number of quartets in a fully-qualified device id: 5 in
  // the four-zone Clos that `util/builders.h` constructs.
  10: i32 fullPrefixLength;
  20: string name = "disagg_clos";
  // Not read by the simulator.
  45: SwitchType switchType = ETHERNET_IPV6;
}

// The following were automatically generated and may benefit from renaming.
@cpp.Type{
  template = "folly::F14FastMap",
}
typedef map<string, LpmTableEntry> LpmTableEntryByPrefix
