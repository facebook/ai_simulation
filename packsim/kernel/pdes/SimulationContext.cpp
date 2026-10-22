/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/pdes/SimulationContext.h"

#include <cstdint>
#include <string>

#include <fmt/format.h>

namespace packsim::pdes {

uint64_t SimulationContext::idCtr_ = 0;

std::string SimulationContext::nameWithPtr() const {
  return fmt::format("{}({})", name, static_cast<const void*>(this));
}

} // namespace packsim::pdes
