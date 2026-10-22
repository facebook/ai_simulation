/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

#include "packsim/kernel/time.h"
#include "packsim/link-layer/llr/llr-seq.h"
#include "packsim/packet/llr-ctlos-types.h"

namespace packsim::llr {

/**
 * UEC 1.0.3 §5.1 enumerations and value types for the LLR module.
 *
 * Each enum declared in this file reserves value 0 as `UNKNOWN`, a
 * default-constructed sentinel that never appears at runtime, and every
 * `switch` over one includes an `UNKNOWN` case that XCHECKs. A forgotten
 * initializer or a bad cast then fails loudly instead of silently reading as
 * the first real state. `LlrCtlosType` does not follow this convention; it is
 * an alias, and the type it names documents why it has no sentinel.
 */

/**
 * TX state machine states (UEC §5.1.5, Fig 5-3 p. 470).
 *
 * Underlying integer values are explicit so they stay stable when this
 * enum is reordered or extended — the values are exposed to downstream
 * readers through the `LlrStats.txState` thrift field, where silent
 * renumbering would shift the schema.
 */
enum class TxState : uint8_t {
  UNKNOWN = 0, // Default-constructed sentinel — never valid at runtime.
  LLR_OFF = 1, // Mode disabled; pass-through.
  INIT = 2, // Handshake in progress; periodic LLR_INIT being sent.
  ADVANCE = 3, // Normal forward transmission.
  REPLAY = 4, // Retransmitting buffered frames.
  FLUSH = 5, // Replay limit / timer expired; discarding buffer.
};

/**
 * ACK/NACK TX state machine states (UEC §5.1.6, Fig 5-4 p. 475).
 *
 * Underlying integer values are explicit for the same reason as
 * `TxState` — exposed via `LlrStats.ackNackState`.
 */
enum class AckNackState : uint8_t {
  UNKNOWN = 0, // Default-constructed sentinel — never valid at runtime.
  OFF = 1, // Mode disabled or pre-INIT.
  SEND_ACKS = 2, // Periodic LLR_ACK being sent.
  // 3 is the spec's transient SEND_NACK, which `Llr::anSmOnGapDetected` passes
  // through in one step, so it is never stored.
  NACK_SENT = 4, // NACK sent; suppress further ACKs/NACKs until next expected
                 // frame.
};

/**
 * Behavior of TX LLR for LLR-desired frames in non-ADVANCE/REPLAY states
 * (UEC §5.1.5 p. 473): how to handle a frame the MAC client wants
 * LLR-protected when the SM is in INIT or FLUSH.
 */
enum class FrameBehavior : uint8_t {
  UNKNOWN = 0, // Default-constructed sentinel — never valid at runtime.
  DISCARD, // Drop and count in tx_discards.
  BLOCK, // Refuse to accept new LLR-desired frames.
  BEST_EFFORT, // Pass through as LLR_INELIGIBLE (no replay buffer, no seq).
};

/** Frame type carried on the wire (UEC §5.1.2 Tbl 5-3 / Tbl 5-5). */
enum class FrameType : uint8_t {
  UNKNOWN = 0, // Default-constructed sentinel — never valid at runtime.
  LLR_ELIGIBLE, // Frame carries an LLR sequence number; LLR-protected.
  LLR_INELIGIBLE, // Standard 802.3 frame; not LLR-protected.
};

/**
 * Outcome of `Llr::admissionDecision(bytes)` — what should happen to a
 * candidate LLR-eligible frame given the current TX state and config.
 * `Llr::pullNextNewFrame` dispatches on this for frames in the CoS
 * queues, as described per value below. `TxArbiter` also consults it for
 * an LLR-eligible CBFC CC_Update frame it is about to send, and holds such
 * a frame on `DROP` instead of discarding it.
 */
enum class AdmissionResult : uint8_t {
  UNKNOWN = 0, // Default-constructed sentinel — never valid at runtime.
  // ADVANCE + capacity OK. Caller consumes the packet, admits it to
  // the replay buffer (assigns `tx_seq`, stores), stamps `LLR_ELIGIBLE
  // + seq`, and transmits. This is the spec's happy path.
  ADMIT_AND_TX,
  // LLR_OFF state, or INIT/FLUSH with `BEST_EFFORT`. Caller consumes
  // the packet but does NOT admit to the replay buffer; stamps
  // `LLR_INELIGIBLE` and transmits as a pass-through frame.
  PASSTHROUGH_TX,
  // INIT/FLUSH with `DISCARD`. Caller consumes the packet and drops
  // it; bumps `stats_.txDiscard`.
  DROP,
  // INIT/FLUSH with `BLOCK`, REPLAY (refuses new admissions), or
  // ADVANCE with capacity exceeded. Caller leaves the packet in the
  // upstream queue; retry later (after state transition or buffer-
  // free wake).
  BLOCK,
};

/** Receive-side FCS status (UEC §5.1.2 Tbl 5-5 p. 466). */
enum class FcsStatus : uint8_t {
  UNKNOWN = 0, // Default-constructed sentinel — never valid at runtime.
  GOOD, // FCS check passed.
  POISONED, // FCS deliberately corrupted (XOR 0xAAAAAAAA) by sender to
            // signal "data is bad but don't NACK" — see §5.1.10 p. 478.
  BAD, // FCS check failed (real corruption).
};

/**
 * Human-readable names for each LLR enum — used by dump / log call sites
 * that surface state to engineers.
 *
 * `LlrEnumNames` is a stateless namespace-class: all members are
 * `static` and the class is never instantiated. Call as
 * `LlrEnumNames::toString(state)`.
 */
class LlrEnumNames {
 public:
  [[nodiscard]] static const char* toString(TxState s) noexcept {
    switch (s) {
      case TxState::UNKNOWN:
        return "UNKNOWN";
      case TxState::LLR_OFF:
        return "LLR_OFF";
      case TxState::INIT:
        return "INIT";
      case TxState::ADVANCE:
        return "ADVANCE";
      case TxState::REPLAY:
        return "REPLAY";
      case TxState::FLUSH:
        return "FLUSH";
    }
    return "?";
  }

  [[nodiscard]] static const char* toString(AckNackState s) noexcept {
    switch (s) {
      case AckNackState::UNKNOWN:
        return "UNKNOWN";
      case AckNackState::OFF:
        return "OFF";
      case AckNackState::SEND_ACKS:
        return "SEND_ACKS";
      case AckNackState::NACK_SENT:
        return "NACK_SENT";
    }
    return "?";
  }

  [[nodiscard]] static const char* toString(FrameBehavior b) noexcept {
    switch (b) {
      case FrameBehavior::UNKNOWN:
        return "UNKNOWN";
      case FrameBehavior::DISCARD:
        return "DISCARD";
      case FrameBehavior::BLOCK:
        return "BLOCK";
      case FrameBehavior::BEST_EFFORT:
        return "BEST_EFFORT";
    }
    return "?";
  }

  [[nodiscard]] static const char* toString(FrameType t) noexcept {
    switch (t) {
      case FrameType::UNKNOWN:
        return "UNKNOWN";
      case FrameType::LLR_ELIGIBLE:
        return "LLR_ELIGIBLE";
      case FrameType::LLR_INELIGIBLE:
        return "LLR_INELIGIBLE";
    }
    return "?";
  }

  [[nodiscard]] static const char* toString(FcsStatus s) noexcept {
    switch (s) {
      case FcsStatus::UNKNOWN:
        return "UNKNOWN";
      case FcsStatus::GOOD:
        return "GOOD";
      case FcsStatus::POISONED:
        return "POISONED";
      case FcsStatus::BAD:
        return "BAD";
    }
    return "?";
  }

  [[nodiscard]] static const char* toString(LlrCtlosType t) noexcept {
    switch (t) {
      case LlrCtlosType::LLR_ACK:
        return "LLR_ACK";
      case LlrCtlosType::LLR_NACK:
        return "LLR_NACK";
      case LlrCtlosType::LLR_INIT:
        return "LLR_INIT";
      case LlrCtlosType::LLR_INIT_ECHO:
        return "LLR_INIT_ECHO";
    }
    return "?";
  }

  [[nodiscard]] static const char* toString(AdmissionResult r) noexcept {
    switch (r) {
      case AdmissionResult::UNKNOWN:
        return "UNKNOWN";
      case AdmissionResult::ADMIT_AND_TX:
        return "ADMIT_AND_TX";
      case AdmissionResult::PASSTHROUGH_TX:
        return "PASSTHROUGH_TX";
      case AdmissionResult::DROP:
        return "DROP";
      case AdmissionResult::BLOCK:
        return "BLOCK";
    }
    return "?";
  }

  LlrEnumNames() = delete;
};

// Sentinel value for `LlrConfig::replayCtMax`: the spec-defined "no
// maximum" sentinel (UEC §5.1.4 Tbl 5-9 p. 468). When `replayCtMax`
// equals this value, `Llr::replayCtIsMax()` always returns false and
// REPLAY never escalates to FLUSH on the replay-counter rule.
inline constexpr uint8_t kReplayCtMaxNoLimit = 0xFF;

// Spec-mandated minimum for `LlrConfig::ctlosTargetSpacingBytes`
// (UEC §5.1.4 Tbl 5-9). `LlrConfigTranslator::translate` uses it as the
// fallback for a thrift `ackByteThreshold` of 0: the CtlOS loop periods are
// proportional to the spacing, so 0 would schedule zero-duration loops.
inline constexpr uint32_t kCtlosTargetSpacingMinBytes = 400;

// Spec-mandated maximum for `LlrConfig::ctlosTargetSpacingBytes`
// (UEC §5.1.4 Tbl 5-9). `LlrConfigTranslator::validate` rejects a thrift
// `ackByteThreshold` above this with `std::invalid_argument`.
inline constexpr uint32_t kCtlosTargetSpacingMaxBytes = 16384;

// Sentinel duration treated as "timer effectively disabled" for the
// `dataAgeTimerMax` / `pcsLostStatusTimerMax` fields.
// `LlrConfigTranslator::translate` maps the thrift sentinel `0` to it.
//
// Sized to be safely beyond any realistic packsim runtime (tens of
// seconds of simulated time for high-fidelity sweeps) while staying
// well under the int64_t headroom the scheduler uses for relative
// delays: `Time` stores femtoseconds, so the ~9.2 × 10^18 fs int64_t
// ceiling corresponds to ~2.5 h — 1 h leaves a comfortable margin.
inline const Time kEffectivelyDisabledLlrTimer = Time::fromSec(60 * 60);

// Default `LlrConfig::linkBps`. `LlrConfigTranslator::translate` overwrites it
// with the port's configured rate; the default only gives a directly built
// `LlrConfig` and unit tests a fallback. 800 Gbps is the fastest Ethernet port
// rate IEEE 802.3 defines (802.3df).
inline constexpr uint64_t kLlrDefaultLinkBps800Gbps =
    800ull * 1'000ull * 1'000ull * 1'000ull;

/**
 * LLR configuration values (UEC §5.1.4 Tbl 5-9 p. 468).
 * All durations use PackSim's `Time` (femtosecond-resolution) type.
 */
struct LlrConfig {
  // `outstanding_seq_max` (§5.1.4 Tbl 5-9). Spec range 0..524288.
  // The 524288 absolute max = 2^19 = `LlrSeq::kSeqHalf` — required
  // to keep the modular comparisons in `ack_nack_seq_ok` (§5.1.8) and
  // `expected/duplicate/missing_frame` (§5.1.7 Tbl 5-11) unambiguous.
  // Implementation may set lower based on sublayer delays.
  uint32_t outstandingSeqMax = LlrSeq::kSeqHalf;

  // Maximum unacknowledged data bytes permitted (Tbl 5-9). SHOULD be set
  // to the bandwidth-delay product to keep PFC working correctly.
  uint64_t outstandingDataMax = 1u << 20;

  // `replay_timer_max` (Tbl 5-9). Spec range 0..65535ns with ≤10ns res.
  Time replayTimerMax;

  // `replay_ct_max` (Tbl 5-9). Spec range 0..255. **Special value
  // `kReplayCtMaxNoLimit` (0xFF) means "no maximum"** —
  // `replay_ct_is_max` is never set TRUE. See `Llr::replayCtIsMax()`
  // for the derivation.
  uint8_t replayCtMax = kReplayCtMaxNoLimit;

  // `pcs_lost_status_timer_max` (Tbl 5-9). Spec range 0..4.29s with ≤100ns
  // res. Zero causes immediate expiration. Support for non-zero is OPTIONAL
  // per spec; implementation supports it.
  Time pcsLostStatusTimerMax;

  // `data_age_timer_max` (Tbl 5-9). Spec range 0..4.29s with ≤100ns res.
  Time dataAgeTimerMax;

  // `llr_init_behavior` (Tbl 5-9). Behavior of TX LLR for LLR-desired
  // frames while TX SM is in INIT.
  FrameBehavior initBehavior = FrameBehavior::BLOCK;

  // `llr_flush_behavior` (Tbl 5-9). Behavior while TX SM is in FLUSH.
  FrameBehavior flushBehavior = FrameBehavior::BLOCK;

  // `re_init_on_discard` (Tbl 5-9). TRUE: FLUSH → INIT (auto-recover).
  // FALSE: FLUSH → wait for management intervention via
  // `Llr::managementReset()`. Nothing in the simulator calls it, so the TX
  // SM stays in FLUSH for the rest of the run and `flushBehavior` applies to
  // every later LLR-desired frame.
  bool reInitOnDiscard = true;

  // NOT spec-defined: skip the LLR_INIT / LLR_INIT_ECHO handshake at
  // startup. When TRUE, `Llr::start()` pre-configures both SMs to their
  // post-handshake state (TX SM = ADVANCE, AN-SM = SEND_ACKS,
  // `next_rx_seq_vld = TRUE`, `next_rx_seq = 0`) and implies
  // `setLlrModeLocal(true)` / `setLlrModeRemote(true)`. The thrift
  // `LlrConfig.skipInitNegotiation` defaults to TRUE and
  // `LlrConfigTranslator` copies it, so a link configured with LLR skips the
  // handshake unless its config turns this off; the FALSE default here
  // applies only to an `LlrConfig` built directly.
  bool skipInitNegotiation = false;

  // `ctlos_target_spacing` (Tbl 5-9). Target bytes between successive
  // LLR_ACK/LLR_NACK CtlOS. Spec range 400..16384 bytes; 8192 is an
  // illustrative default inside that range.
  uint32_t ctlosTargetSpacingBytes = 8192;

  // Gate for periodic LLR_ACK emission in `ackCtlosLoop`. Set FALSE in
  // tests that drive ACK emission by direct API calls and need to assert
  // observable state without background ACKs stepping on the assertion.
  // Defaults to TRUE, the spec behavior.
  bool enableGlobalTimer = true;

  // Maximum replay-buffer size in bytes. Implementation-defined; sized
  // to at least the bandwidth-delay product (spec p. 463).
  uint64_t txBufferMaxBytes = 1u << 20;

  // Effective link rate in bits-per-second. Used to derive the
  // `ctlos_target_spacing`-based periods for `ackCtlosLoop` and
  // `initCtlosLoop`.
  uint64_t linkBps = kLlrDefaultLinkBps800Gbps;
};

/**
 * LLR counters (UEC §5.1.11 Tbl 5-13 p. 479). All 22 spec counters
 * enumerated. The owning `Llr` exposes a const reference via `stats()`.
 */
struct Stats {
  // TX-side CtlOS counters
  uint64_t txInitCtlos = 0;
  uint64_t txInitEchoCtlos = 0;
  uint64_t txAckCtlos = 0;
  uint64_t txNackCtlos = 0;

  // TX-side frame counters
  // Frames discarded per init/flush DISCARD behavior.
  uint64_t txDiscard = 0;
  // LLR-eligible frames TX'd with good FCS.
  uint64_t txOk = 0;
  // Frames transmitted as LLR_INELIGIBLE because the TX SM was in LLR_OFF,
  // or in INIT/FLUSH with BEST_EFFORT. A sustained nonzero rate here signals
  // a link that's spending real traffic time outside ADVANCE.
  uint64_t txPassthrough = 0;
  // LLR-eligible frames TX'd with poisoned FCS.
  uint64_t txPoisoned = 0;
  // Replay frames transmitted (per-frame, NOT batches).
  uint64_t txReplay = 0;
  // Bytes retransmitted on replay (sum of replay frame sizes).
  uint64_t txReplayBytes = 0;
  // REPLAY-state exits (one per batch).
  uint64_t txReplayBatches = 0;
  // REPLAY entries triggered by NACK.
  uint64_t txReplayViaNack = 0;
  // REPLAY entries triggered by replay_timer.
  uint64_t txReplayViaTimer = 0;
  // ACKs from byte-on-receive trigger.
  uint64_t txAckByteTrigger = 0;
  // ACKs from ackCtlosLoop periodic timer.
  uint64_t txAckPeriodic = 0;

  // TX SM state-entry counters (not spec-mandated; exported as
  // `LlrStats.initCount` / `flushCount`).
  // Count of TX SM entries into INIT.
  uint64_t txInitEntries = 0;
  // Count of TX SM entries into FLUSH.
  uint64_t txFlushEntries = 0;

  // RX-side CtlOS counters
  uint64_t rxInitCtlos = 0;
  uint64_t rxInitEchoCtlos = 0;
  uint64_t rxAckCtlos = 0;
  uint64_t rxNackCtlos = 0;
  // ack_nack_seq_ok was FALSE.
  uint64_t rxAckNackSeqError = 0;

  // RX-side frame counters (by FCS status)
  uint64_t rxOk = 0;
  uint64_t rxPoisoned = 0;
  uint64_t rxBad = 0;

  // RX-side frame counters (by sequence-number relationship)
  uint64_t rxExpectedSeqGood = 0;
  uint64_t rxExpectedSeqPoisoned = 0;
  uint64_t rxExpectedSeqBad = 0;
  uint64_t rxMissingSeq = 0;
  uint64_t rxDuplicateSeq = 0;
  // Times the receiver detected the start of a replay.
  uint64_t rxReplay = 0;
};

} // namespace packsim::llr
