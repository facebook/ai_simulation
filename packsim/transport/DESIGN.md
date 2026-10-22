# PackSim RoCE Transport Design

PackSim's transport layer uses **composition over inheritance** to model
network transport protocols. Instead of creating subclasses for each
combination of behaviors, a single `RoceTransportBase` class takes a
`RoceTransportConfig` that selects a strategy component for error recovery and
one for congestion control, and each of its sockets builds and owns its own
instances.

This guide explains the architecture and shows how to extend it. Paths in it are
relative to `packsim/`, as in the PackSim README, except `CMakeLists.txt`, which
is at the repository root.

## Architecture Overview

```
Transport (base interface)
└── RoceTransportBase    (one per endpoint; holds a RoceTransportConfig)
      owns a RoceSocketBase per QP, each of which owns
        ErrorRecovery?      (error recovery — GoBackNRecovery today)
        CongestionControl?  (congestion control — DcqcnCongestionControl today)
```

### Why composition instead of subclasses

Transport behaviors vary along **orthogonal axes** — error recovery and
congestion control are independent choices. With inheritance, supporting N
error recovery modes × M congestion control modes requires N×M subclasses.
With composition, it requires N + M components that can be freely combined.

This means:
- Combining Go-Back-N with DCQCN is just two config settings — no new class
  needed
- A new error recovery algorithm is one new component (a header and a source),
  not a new subclass tree
- Components can be tested in isolation without instantiating a full transport

### Key files

| File | Purpose |
|------|---------|
| `transport/transport.h/.cpp` | `Transport` and `Socket`, the base classes; `TransportParams` |
| `transport/roce-base.h` | `RoceSocketBase`, `RoceTransportBase`, `RoceTransportConfig` |
| `transport/roce-base.cpp` | Unified implementation that delegates to strategy components; also `RoceTransportConfig::fromThrift()` |
| `transport/roce-send-info.h` | `RoceSendInfo` — per-packet send metadata; a leaf header shared by the socket and the error-recovery component |
| `transport/error-recovery.h/.cpp` | `ErrorRecovery` ABC; `GoBackNRecovery` — Go-Back-N ARQ error recovery |
| `transport/congestion-control.h/.cpp` | `CongestionControl` ABC; `DcqcnCongestionControl` — DCQCN rate-based congestion control, including the rate-control state machine itself |
| `transport/transport-factory.h/.cpp` | `makeTransport()` — maps a `TransportConfig` union arm to a transport implementation; takes the `TransportParams` declared in `transport/transport.h` |
| `if/transport_config.thrift` | `union TransportConfig` — the plug-point new transports are added at |
| `if/roce_v2_config.thrift` | `RoceV2LossRecoveryConfig` / `RoceV2CongestionControlConfig` sub-unions, and their arms |
| `if/nic_config.thrift` | `NicConfig` — per-NIC tuning shared with the strategies |

## How it works

### RoceTransportConfig

The transport's behavior is determined at construction time by a config struct,
which `fromThrift()` builds from the `roceV2` arm of the NIC's
`TransportConfig`:

```cpp
struct RoceTransportConfig {
  enum class ErrorRecovery { NONE, GO_BACK_N };
  enum class CongestionControl { NONE, DCQCN };

  ErrorRecovery errorRecovery = ErrorRecovery::NONE;
  CongestionControl congestionControl = CongestionControl::NONE;

  // Go-Back-N config (only used when errorRecovery == GO_BACK_N).
  uint8_t rnrTimeIdx = 1;
  bool selectiveGapRepair = false;

  static RoceTransportConfig fromThrift(
      const packsim::thrift::TransportConfig& transport);
};
```

With `NONE` for both, you get optimistic RoCE (no loss recovery, no rate
limiting). `RoceTransportBase::makeSocket()` turns the config into a factory for
each strategy it selects, and the `RoceSocketBase` constructor calls them, so
every socket builds and owns its own components.

### Strategy component interaction

The socket delegates to its strategy components at key points:

| Socket method | Error recovery hook | Congestion control hook |
|---------------|-------------------|----------------------|
| `isPsnOk()` | `checkPsn()` — classifies the PSN; on a gap the socket then calls `onMissingMessage()`, and the strategy sends the NACK | — |
| `scheduleSend()` | `onPacketScheduled()` — records for retransmit | — |
| `ackRcvd()` | `onAck()` — frees acknowledged packets | — |
| `nackRcvd()` | `onNack()` — resets retransmit pointer | — |
| `executeRetransmit()` | `getRetransmitBatch()` — picks the packets to resend | — |
| `sendBytes()` | — | `adjustQuota()` — limits by available tokens |
| `sendBytes()` (after) | — | `onBytesSent()` — consumes tokens |
| `cnpRcvd()` | — | `onCongestionSignal()` — triggers rate reduction |
| `onPacketRcvd()` (every packet, before dispatch) | — | `onPacketReceived()` — DCQCN emits a CNP for an ECN-marked packet |
| `RoceTransportBase::trySendOnSocket()` | `isSendBlocked()` | `isSendBlocked()` |
| `ackRcvd()` and `onPacketWrittenToMemory()`, once the QP has no send or receive outstanding | — | `cancelTimers()` |

### Transport-level scheduling

`RoceTransportBase::sendNext()` is the unified send scheduler. It
round-robins across active sockets, with these priorities:

1. Skip sockets either strategy blocks (`isSendBlocked()` on the error
   recovery, e.g. RNR back-off, or on the congestion control)
2. Retransmit pending packets first (error recovery)
3. Send new data from the queue
4. Remove socket from active set when nothing remains

### Multi-port behaviour

An endpoint with a port index (`EndpointConfig.portIdx > 0`) registers each of
its egress ports with `RoceTransportBase::addPort()`, which fills
`portSendPacketFns_`; an endpoint without one registers only its first port, so
the choice below has one candidate. `sendPacket()` picks the port for a packet
from the two endpoint device IDs:

```cpp
auto portIt = portSendPacketFns_.begin();
std::advance(
    portIt,
    egressPortSlot(socket->egressPortKey_, portSendPacketFns_.size()));
```

The key is cached when the socket is created. Although `Endpoint` temporarily
stamps port-level addresses while routing a packet, `EthernetPort` restores
the socket-level connection before transport delivery. Reactive receive sockets
therefore derive their key from the original endpoint IDs.

Two properties follow, and both are deliberate.

**A device pair is pinned to one port.** The device-ID sum is symmetric under
the source/destination swap, so both directions land on the same physical port
when both endpoints register the same ordered port set. Every QP between those
devices therefore keeps a single path and stays in order. The mapping
deliberately excludes QPNs because their allocation stride can share a factor
with the port count and leave ports unreachable. `QueuePairTemplate.ports` and
`Workload.qpnPortConfigs` do not change the choice. On a port-indexed endpoint
(`EndpointConfig.portIdx > 0`), the per-port send function that
`Endpoint::addPort` registers routes the packet by the stamped port
(`Endpoint::routePacketByEgressNicPort`) without consulting them, and
`Endpoint::routePacket` applies the same bypass to a port-level packet that
carries a transport-chosen port. Without a port index, the endpoint routes by
device address through its LPM table and never reads them either.

**Aggregate bandwidth across ports comes from communicating with several
device peers, not from opening several QPs to one peer.** A point-to-point or
few-peer workload can therefore use fewer ports than a dense collective. The
component sum is tailored to the hierarchical device IDs used by the supported
topologies; it does not promise collision-free placement for arbitrary sparse
sets of IDs.

The consequence is measurable. Between two accelerators with one QP pair for
data, four 100G ports, each through its own switch, complete an all-to-all in
54,221ns against 21,693ns for a single 400G port through one switch — 2.5x
slower at identical aggregate bandwidth.
`MultiPortRoceV2Test.SingleQpPairDoesNotSpreadAcrossPorts` in
`simulator/oss_tests/` pins the behaviour — it fails if the four ports come
within 1.5x of the single one — so a change that starts striping a QP has to be
a deliberate one.

Spreading a QP across ports needs more than `addPort()`, which RoCEv2 already
has: it needs reachability- and liveness-aware per-packet plane selection at
the point where ports are registered. No transport in this build implements
that.

## Adding a New Error Recovery Strategy

To add a new error recovery (e.g., Selective ACK), follow these steps:

### 1. Create the component

Create `transport/selective-ack-recovery.h` and `.cpp`. The component should
manage its own state (retransmit buffers, timers) and expose decision methods
that the socket calls. Study `GoBackNRecovery` as the reference
implementation.

Subclass `ErrorRecovery`, implement its required methods, and override optional
hooks as needed. See `error-recovery.h` for the full interface. Key methods:

```cpp
class SelectiveAckRecovery final : public ErrorRecovery {
 public:
  SelectiveAckRecovery(ErrorRecoverySocket& socket, SimContext& simCtx, ...);

  // Required.
  PsnAction checkPsn(
      uint32_t& expectedPsn, uint32_t receivedPsn, bool oooSupport) override;
  void onPacketScheduled(const RoceSendInfo& info) override;
  AckResult onAck(uint32_t psn) override;
  void onNack(
      uint32_t expectedPsn,
      std::optional<uint8_t> rnrBackoffIdx) override;
  RetransmitBatch getRetransmitBatch(uint64_t quota) override;
  bool hasRetransmitPending() const noexcept override;

  // Optional hooks, each with a default in error-recovery.h. A receiver that
  // buffers out-of-order packets, as selective repeat does, needs most of them.
  bool isSendBlocked() const override;
  uint32_t adjustAckPsn(
      uint32_t requestedAckPsn, uint32_t expectedPsn) const noexcept override;
  bool useByteCountCompletion() const noexcept override;
  void onMissingMessage(uint32_t seqNum, uint32_t psn) override;
  void deferAck(uint32_t seqNum, uint32_t psn) override;
  std::vector<std::pair<uint32_t, uint32_t>> takeReadyDeferredAcks(
      uint32_t expectedPsn) override;
  void onSendRnrRequested(uint32_t psn) override;
  RetransmitStats getRetransmitStats() const noexcept override;
  bool isFailed() const noexcept override;
};
```

The strategy reaches back into the socket via `emitNackPacket` and
`wakeupForSend` for I/O, but does not create packets directly.

### 2. Add to RoceTransportConfig

```cpp
enum class ErrorRecovery { NONE, GO_BACK_N, SELECTIVE_ACK };
```

Add a field for each parameter of the strategy too, as `rnrTimeIdx` and
`selectiveGapRepair` are for Go-Back-N: this struct is the only route from the
`lossRecovery` arm to the component.

### 3. Wire into RoceTransportBase::makeSocket()

In `RoceTransportBase::makeSocket()` (`transport/roce-base.cpp`), set the local
`errorRecoveryFactory` to a lambda that constructs the component when
`roceCfg_.errorRecovery` selects it, capturing its parameters from `roceCfg_`;
the `RoceSocketBase` constructor calls it with the socket and the `SimContext`.
Socket methods such as `isPsnOk` and `ackRcvd` already delegate to
`errorRecovery_`. Also widen the `GO_BACK_N` check in
`RoceTransportBase::getNicStatsSummaryResults()`, which otherwise leaves the
strategy's retransmit counters (`getRetransmitStats()`) out of the result.

If the new recovery mechanism needs another decision point, add a virtual hook
with a behavior-preserving default to `ErrorRecovery` and call it from the
appropriate socket path.

### 4. Add thrift config

In `if/roce_v2_config.thrift`, add a config struct for any parameters your
recovery needs (see `GoBackNLrConfig`) and add an arm for it to
`union RoceV2LossRecoveryConfig`. Parameters that belong to the NIC rather than
to the strategy live on `NicConfig` in `if/nic_config.thrift` instead — on the
congestion-control side, `DcqcnConfig dcqcnCfg` is there for that reason.

### 5. Map the thrift arm onto the enum

In `RoceTransportConfig::fromThrift()` (`transport/roce-base.cpp`), add a case
for the new arm to the `switch (roceThriftConfig.lossRecovery()->getType())`
that sets `config.errorRecovery` and copies the arm's parameters into the fields
from step 2.

That switch is exhaustive and has no `default`, so skipping this step shows up
only as the `-Wswitch` unhandled-enum diagnostic (a warning, or an error under
`-Werror`), and only where that warning is on: Clang enables it by default,
GCC only under `-Wall`, and this repository's `CMakeLists.txt` turns on no
warnings. To make it an error while you add an arm, configure the plain CMake
build with `-DCMAKE_CXX_FLAGS=-Werror=switch` (getdeps would pass it to every
dependency as well). Suppress or miss the warning and control falls straight
past the switch to `return config;`, leaving `errorRecovery` at its default —
the transport is built silently with no loss recovery rather than reporting
anything. The `std::invalid_argument("unsupported RoCE loss recovery config")`
throw a few lines down is for the unset (`__EMPTY__`) arm only and will not
catch this.

### 6. Add tests

Add a test fixture under `transport/oss_tests/` that constructs
`RoceTransportBase` with your new config. Test both normal and lossy scenarios.
Also add a case to `transport/oss_tests/roce-transport-config-test.cpp` that
maps the new thrift arm and its parameters through
`RoceTransportConfig::fromThrift()`: the fixtures build `RoceTransportConfig` by
hand, so that test is the one that catches a missed step 5.

### 7. Build

A new `transport/*.cpp` needs no edit to the CMake build in this repository:
`CMakeLists.txt` globs its sources. The few things it does enumerate are
exceptions to the glob, and each one fails at configure time if it drifts,
naming what drifted — a new `.thrift` in `if/` must be added to
`PACKSIM_THRIFT_FILES`; a file renamed or removed from
`PACKSIM_MAIN_SOURCES`, `PACKSIM_TEST_SUPPORT_SOURCES` or
`PACKSIM_HEAVY_TEST_SOURCES` must be updated there (the last two are checked
when tests are built, the default); a new source that defines `main()` must be
listed in `PACKSIM_MAIN_SOURCES` (the check reads the text, so it catches
`int main(` or `auto main(` at the start of a line, after any indentation, not
every spelling); and a test source whose basename collides with another test or
with one of the build's own target names must be renamed.

A build that lists its sources explicitly rather than globbing does need the
new `.cpp` added to the target that owns this directory.

## Adding a New Congestion Control Algorithm

To add a new CC (e.g., CUBIC), the pattern mirrors error recovery:

### 1. Create the component

Create `transport/cubic-controller.h` and `.cpp`. Subclass `CongestionControl`,
implement the required send-gating methods, and override only the feedback hooks
the algorithm uses. Constructed via inversion of control: the ctor takes the
owning `CongestionControlSocket` and `SimContext` by reference, so it wires its
own send-budget wakeup (`socket.wakeupForSend()`) and registers any tracer
snapshots at build time. Study `DcqcnCongestionControl` as the reference.

```cpp
class CubicController final : public CongestionControl {
 public:
  CubicController(CongestionControlSocket& socket, SimContext& simCtx, ...);

  // Send gating (required).
  size_t adjustQuota(size_t requestedQuota) const override;
  void onBytesSent(uint32_t dataBytes) override;

  // Congestion feedback — override only what the algorithm uses.
  void onCongestionSignal() override;         // the sender received a CNP
  void onPacketReceived(bool ecnCe) override; // every packet the socket receives

  // Optional: hold sends back entirely, and stop timers once the QP is idle.
  bool isSendBlocked() const noexcept override;
  void cancelTimers() override;
};
```

`CongestionControl` has no ACK or loss hook: `nackRcvd()` never reaches it,
`ackRcvd()` reaches it only through `cancelTimers()` once the QP has nothing
outstanding, and `onPacketReceived()` sees ACKs and NACKs but only their ECN
mark. Retransmissions bypass its budget: `executeRetransmit()` calls neither
`adjustQuota()` nor `onBytesSent()`, so retransmitted bytes are neither limited
by nor charged to it, and only `isSendBlocked()` holds them back. An algorithm
that reacts to ACKs or loss, as CUBIC does, needs a new hook with a
behavior-preserving default, called from those socket methods, as step 3 above
describes for error recovery.

### 2-7. Same pattern as error recovery

Add the enum; wire it into `RoceTransportBase::makeSocket()` through its
`congestionControlFactory`, and extend the DCQCN-only `maxActiveSends_` check
in the `RoceTransportBase` constructor if your controller rate-limits sockets,
as DCQCN does; add thrift config (the congestion-control side is
`union RoceV2CongestionControlConfig`); map the arm onto the enum in
`fromThrift()` — the congestion-control switch above it has the same
no-`default` shape — add tests, and build.

## Adding a New Transport Protocol (e.g., TCP)

If the new protocol shares RoCE's fundamental packet flow (send → ACK →
completion), it can build on `RoceTransportBase` with new strategy
components. The key question is: **does the new protocol share the base
packet encapsulation and send/receive pipeline?**

### If yes (similar packet flow, different recovery/CC)

Add new error recovery and/or congestion control components as described
above. The existing `RoceSocketBase` pipeline (segmentation, encap, ACK
handling, CQE ordering) handles the rest.

### If no (fundamentally different protocol semantics)

Create a new `Transport` subclass, add an arm for its config to
`union TransportConfig` in `if/transport_config.thrift`, and construct the
subclass for that arm in `makeTransport()` (`transport/transport-factory.cpp`).
`RoceTransportConfig::fromThrift()` switches over that union's arms with no
`default`, so give the new arm a case there that throws `std::invalid_argument`,
as the code after the switch does. This is appropriate when:
- The packet format is completely different
- The send/receive pipeline has different stages
- The connection model differs (e.g., stream-based vs. message-based)

Even in this case, consider whether parts of the protocol can be decomposed
into reusable components rather than baking everything into the subclass.

## Combining Error Recovery + Congestion Control

One of the key benefits of composition: behaviors combine freely. To use
Go-Back-N with DCQCN:

```cpp
RoceTransportConfig cfg;
cfg.errorRecovery = RoceTransportConfig::ErrorRecovery::GO_BACK_N;
cfg.congestionControl = RoceTransportConfig::CongestionControl::DCQCN;
cfg.rnrTimeIdx = 1;
auto transport =
    std::make_unique<RoceTransportBase>(&simCtx, id, &nicConfig, cfg);
```

The socket will create both a `GoBackNRecovery` and a `DcqcnCongestionControl`.
In `sendNext()`, retransmission takes priority (error recovery), and
`sendBytes()` respects the DCQCN token budget (congestion control), which
retransmissions bypass. The two components don't interact directly — they each
hook into different points of the socket pipeline.

## Testing Conventions

- Tests go under `transport/oss_tests/`
- Each transport configuration gets its own fixture class in
  `send-msg-tests.cpp`, subclassing `TransportTest` from
  `send-msg-test-fixture.h`, which provides the simulation harness
- Cover normal delivery (as `SendMessage` does) for each configuration that
  delivers reliably, and a drop (as `SendMessageLossy` does) for each one whose
  recovery repairs it. Error recovery alone is not enough: Go-Back-N with
  `outofOrderPktSupport` on and `selectiveGapRepair` off never repairs a gap, so
  assert the loss instead, as
  `GoBackNOooRoceTest.DroppedMiddlePacketIsNeverRetransmitted` does. Without
  error recovery a drop is never repaired either: an in-order receiver aborts
  the run at the QP's next packet, and otherwise the message never completes
- Quota-specific tests go in `transport/oss_tests/roce-base-quota-test.cpp`
- Component-level tests (e.g., testing `GoBackNRecovery` in isolation) go in
  their own file, alongside `error-recovery-test.cpp` and
  `congestion-control-test.cpp`

## Common Pitfalls

- **Don't create a new Transport subclass** when a strategy component would
  suffice. Ask: "Is this a new protocol, or a new behavior within RoCE?"
- **Don't put I/O in strategy components.** Components manage state and
  return decisions; each schedules only its own timers. The socket handles
  packet creation and transmission.
- **Don't forget the active socket tracking.** When error recovery triggers
  (e.g., NACK received), the socket must be in the active set for
  `sendNext()` to service it. `socketReady()` puts it there, but nothing calls
  it for you on a NACK: the strategy has to call `wakeupForSend()`, at once or
  when its back-off expires, as `GoBackNRecovery::onNack()` does.
- **Test lossy scenarios.** Error recovery is only exercised when packets are
  dropped. The test harness supports this via the `lossy_` flag.
