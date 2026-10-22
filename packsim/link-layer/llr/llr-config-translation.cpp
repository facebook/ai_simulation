/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/llr/llr-config-translation.h"

#include <stdexcept>

#include <fmt/format.h>

#include "packsim/kernel/time.h"

namespace packsim::llr {

void LlrConfigTranslator::validate(const packsim::thrift::LlrConfig& config) {
  // `kReplayCtMaxNoLimit` (0xFF, spec Tbl 5-9) is how `LlrConfig` spells "no
  // maximum". Reject rather than clamp, so a caller asking for 255 or 300 is
  // told instead of silently getting "no limit".
  const int64_t replayCtMax = *config.replayCtMax();
  if (replayCtMax >= kReplayCtMaxNoLimit) {
    throw std::invalid_argument(
        fmt::format(
            "replayCtMax ({}) collides with the 'no maximum' sentinel ({}). "
            "Use 0 (or omit) to mean 'no limit'; values 1..{} are honored.",
            replayCtMax,
            kReplayCtMaxNoLimit,
            kReplayCtMaxNoLimit - 1));
  }

  const uint32_t ackByteThreshold = *config.ackByteThreshold();
  if (ackByteThreshold > kCtlosTargetSpacingMaxBytes) {
    throw std::invalid_argument(
        fmt::format(
            "ackByteThreshold ({}) exceeds the UEC spec maximum ({}). "
            "Allowed range: 1..{} (positive values), or <= 0 for the "
            "spec minimum default ({}).",
            ackByteThreshold,
            kCtlosTargetSpacingMaxBytes,
            kCtlosTargetSpacingMaxBytes,
            kCtlosTargetSpacingMinBytes));
  }
}

LlrConfig LlrConfigTranslator::translate(
    const packsim::thrift::LlrConfig& config,
    const uint64_t linkBps,
    const bool enableGlobalTimer) {
  validate(config);

  const int64_t replayCtMax = *config.replayCtMax();
  const uint32_t ackByteThreshold = *config.ackByteThreshold();
  const uint64_t dataAgeTimerNs = *config.dataAgeTimerMaxNs();
  const uint64_t pcsLostStatusNs = *config.pcsLostStatusTimerMaxNs();

  LlrConfig cfg;
  // UEC 1.0.3 §5.1.4 Tbl 5-9: `outstanding_seq_max` maxes at 524288. The
  // thrift `txBufferMaxBytes` is a *byte* count and is not a meaningful
  // bound on a *seq* count — clamp directly to the spec ceiling.
  cfg.outstandingSeqMax = LlrSeq::kSeqHalf;
  cfg.outstandingDataMax = *config.txBufferMaxBytes();
  cfg.txBufferMaxBytes = *config.txBufferMaxBytes();
  cfg.replayTimerMax = Time::fromNs(*config.retryTimeoutNs());
  // Thrift `0` (or any non-positive value) means "no max"; without
  // translation it would mean "FLUSH on first replay".
  cfg.replayCtMax = replayCtMax <= 0 ? kReplayCtMaxNoLimit
                                     : static_cast<uint8_t>(replayCtMax);
  // Spec §5.1.4 Tbl 5-9 / §5.1.3.1.1: `ackByteThreshold` corresponds to
  // UEC `ctlos_target_spacing` (bytes). Drives the periodic LLR_ACK rate
  // directly and the LLR_INIT rate derivatively (= 4× per §5.1.9.1.1).
  // 0 maps to the spec minimum — the loop periods are proportional to this
  // value, so 0 would schedule zero-duration loops.
  cfg.ctlosTargetSpacingBytes =
      ackByteThreshold != 0 ? ackByteThreshold : kCtlosTargetSpacingMinBytes;
  // Thrift `0` means "disabled" (timer never fires), but `LlrConfig` follows
  // the spec where `Time{}` = 0 = immediate expiration. Map the sentinel to a
  // duration well beyond any realistic sim runtime so the timer is effectively
  // disabled.
  cfg.dataAgeTimerMax = dataAgeTimerNs > 0 ? Time::fromNs(dataAgeTimerNs)
                                           : kEffectivelyDisabledLlrTimer;
  cfg.pcsLostStatusTimerMax = pcsLostStatusNs > 0
      ? Time::fromNs(pcsLostStatusNs)
      : kEffectivelyDisabledLlrTimer;
  cfg.initBehavior = FrameBehavior::BLOCK;
  cfg.flushBehavior = FrameBehavior::BLOCK;
  cfg.reInitOnDiscard = *config.reInitOnDiscard();
  cfg.skipInitNegotiation = *config.skipInitNegotiation();
  // Test-only gate: suppresses the periodic LLR_ACK loop in tests that
  // drive the SMs by direct API calls. The INIT loop runs either way.
  cfg.enableGlobalTimer = enableGlobalTimer;
  // The port's actual rate, so the byte → time conversion of the CtlOS
  // spacings in `Llr::Llr` is correct. `LlrConfig` defaults to 800 Gbps,
  // which over-estimates a slower link and would make the ACK period too
  // short.
  cfg.linkBps = linkBps;
  return cfg;
}

} // namespace packsim::llr
