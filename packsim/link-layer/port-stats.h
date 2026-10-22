/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>

#include "packsim/if/gen-cpp2/switches_types.h"

namespace packsim {

/**
 * Cumulative counters for one CoS on one port.
 *
 * A port keeps one instance per CoS queue. `llrStats` and `cbfcStats` are
 * per-port rather than per-CoS; the port stamps the same snapshot onto
 * every entry when it reports.
 */
struct PortStats {
  uint64_t rxEcnCePkts{0};
  uint64_t rxTtldPkts{0};
  uint64_t rxPkts{0};
  uint64_t rxBytes{0};
  uint64_t txEcnCePkts{0};
  uint64_t txTtldPkts{0};
  uint64_t txPkts{0};
  uint64_t txBytes{0};
  uint32_t txXon{0};
  uint32_t txXoff{0};
  uint32_t rxXon{0};
  uint32_t rxXoff{0};
  uint64_t txPauseDurationNs{0};
  uint64_t rxPauseDurationNs{0};
  uint64_t dropPkts{0};
  uint64_t dropBytes{0};
  uint64_t ecnCeMarkedPkts{0};

  // LLR statistics (populated if LLR is enabled)
  std::optional<packsim::switches::thrift::LlrStats> llrStats;

  // CBFC statistics (populated if CBFC is enabled)
  std::optional<packsim::switches::thrift::CbfcStats> cbfcStats;

  /**
   * Project onto the thrift schema for the simulation result snapshot.
   *
   * @return  The thrift view of these counters. `llrStats` and `cbfcStats`
   *          are left unset when the corresponding module is disabled.
   */
  [[nodiscard]] packsim::switches::thrift::InterfaceStats toThrift() const;
};

} // namespace packsim
