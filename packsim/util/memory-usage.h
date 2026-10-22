/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>

namespace packsim {

/**
 * Peak resident set size reached by this process so far, in bytes.
 *
 * The value is a high-water mark: it never decreases over the life of the
 * process, so it is safe to sample once at the end of a run.
 *
 * @return  Peak RSS in bytes, or 0 if the kernel refuses the query.
 */
size_t getPeakRssBytes();

} // namespace packsim
