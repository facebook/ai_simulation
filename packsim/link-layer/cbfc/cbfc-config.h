/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/link-layer/cbfc/cbfc-types.h"

namespace packsim::cbfc {

/**
 * Validate a link's configured CBFC settings and translate them into the
 * runtime `CbfcConfig`.
 *
 * The returned config indexes the per-VC settings densely by VC id so the hot
 * path can subscript them, and has the `creditSizeBytes == 0` "derive from the
 * receiver" sentinel already resolved, so it never holds 0.
 *
 * @param config   The link's CBFC settings as configured.
 * @param linkBps  The port's bit rate. Required because the byte-denominated
 *                 CF_min and CF_max timers cannot be converted to wire time
 *                 without it.
 *
 * @return         The runtime config, ready to construct a `Cbfc`.
 *
 * @throw std::invalid_argument if any field is out of range, naming the
 *        offending thrift field and the configured value.
 */
[[nodiscard]] CbfcConfig makeCbfcConfig(
    const packsim::thrift::CbfcConfig& config,
    uint64_t linkBps);

} // namespace packsim::cbfc
