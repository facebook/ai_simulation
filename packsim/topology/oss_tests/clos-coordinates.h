/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>

#include <fmt/core.h>
#include <fmt/format.h>

#include "packsim/kernel/device-id.h"

namespace packsim {

/**
 * A device's coordinates in the four-zone Clos fabric the test builders
 * produce. Which fields are populated depends on the device's tier; the rest
 * stay 0.
 */
struct ClosCoordinates {
  uint16_t atsw{0};
  uint16_t rack{0};
  uint16_t plane{0};
  uint16_t ctsw{0};
  uint16_t accel{0};
  uint16_t host{0};
  uint16_t zone{0};
  uint16_t linkNum{0};

  /**
   * Decode a flex device id. Its first value selects the fabric tier
   * (accel/rtsw/ctsw/atsw), which decides the coordinates the rest encode.
   *
   * @param flexId    The device id to decode.
   *
   * @return          The device's coordinates.
   *
   * @throw std::runtime_error if the first value names no known tier.
   */
  static ClosCoordinates fromFlexId(const FlexDeviceId& flexId) {
    constexpr int kAccelLayer = 1;
    constexpr int kRtswLayer = 2;
    constexpr int kCtswLayer = 3;
    constexpr int kAtswLayer = 4;

    ClosCoordinates coords;
    const auto& vals = flexId.values;
    switch (vals.at(0)) {
      case kAccelLayer:
        coords.zone = vals.at(1);
        coords.rack = vals.at(2);
        coords.host = vals.at(3);
        coords.accel = vals.at(4);
        break;
      case kRtswLayer:
        coords.zone = vals.at(1);
        coords.rack = vals.at(2);
        break;
      case kCtswLayer:
        coords.zone = vals.at(1);
        coords.plane = vals.at(2);
        coords.ctsw = vals.at(3);
        break;
      case kAtswLayer:
        coords.plane = vals.at(1);
        coords.atsw = vals.at(2);
        break;
      default:
        throw std::runtime_error(
            fmt::format("Unrecognized layer #: {}", vals.at(0)));
    }
    return coords;
  }
};

} // namespace packsim
