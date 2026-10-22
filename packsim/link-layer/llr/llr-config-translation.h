/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/link-layer/llr/llr-types.h"

namespace packsim::llr {

/**
 * The single point where the caller-facing `thrift::LlrConfig` becomes the
 * `llr::LlrConfig` the LLR module runs on.
 *
 * The two schemas disagree in three ways, and this is where the disagreement
 * is resolved:
 *
 *  - **Sentinels.** Thrift uses a non-positive value to mean "no maximum"
 *    (`replayCtMax`), "use the spec default" (`ackByteThreshold`) and
 *    "disabled" (the two timers). `LlrConfig` follows UEC, where those same
 *    values mean "FLUSH on first replay", "zero-duration loop period" and
 *    "expire immediately" respectively. Translating is not optional.
 *  - **Units and ranges.** `outstanding_seq_max` is a sequence count, not the
 *    byte count thrift supplies, and several fields have UEC range limits the
 *    thrift schema cannot express.
 *  - **Link rate.** `LlrConfig::linkBps` drives the byte-to-nanosecond
 *    conversion for the periodic CtlOS loops and has no thrift counterpart;
 *    the caller supplies the port's actual rate.
 *
 * Example usage:
 *
 *      const auto cfg = LlrConfigTranslator::translate(
 *          thriftConfig, dataRate_.getBps(), enableGlobalTimer);
 *
 * NOTE: Pure -- no simulation state is read or written, so this is callable
 * before a port, channel or scheduler exists.
 */
class LlrConfigTranslator {
 public:
  /**
   * Validate `config` against the UEC ranges, then translate it.
   *
   * @param config             Caller-facing LLR settings.
   * @param linkBps            The port's line rate in bits per second, used
   *                           for `LlrConfig::linkBps`.
   * @param enableGlobalTimer  False suppresses the periodic LLR_ACK loop, for
   *                           tests that drive the state machines by direct
   *                           API calls. The INIT loop runs either way.
   *
   * @return                   The translated module config.
   *
   * @throw std::invalid_argument  On an out-of-spec-range input; see
   *                               `validate()`. Validation runs in full before
   *                               anything is translated, so a rejected config
   *                               produces no partial result.
   */
  [[nodiscard]] static LlrConfig translate(
      const packsim::thrift::LlrConfig& config,
      uint64_t linkBps,
      bool enableGlobalTimer);

 private:
  /**
   * Reject the inputs UEC 1.0.3 §5.1.4 Tbl 5-9 has no representation for.
   *
   * @param config  Caller-facing LLR settings.
   *
   * @throw std::invalid_argument  If `replayCtMax` collides with the "no
   *                               maximum" sentinel, or `ackByteThreshold`
   *                               exceeds the spec maximum.
   */
  static void validate(const packsim::thrift::LlrConfig& config);
};

} // namespace packsim::llr
