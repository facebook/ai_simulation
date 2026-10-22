/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/util/memory-usage.h"

#include <sys/resource.h>

namespace packsim {

size_t getPeakRssBytes() {
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) != 0) {
    return 0;
  }
  // Linux reports ru_maxrss in kibibytes, not bytes.
  return static_cast<size_t>(usage.ru_maxrss) * 1024;
}

} // namespace packsim
