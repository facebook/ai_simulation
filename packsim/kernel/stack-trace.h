/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

namespace packsim {

/**
 * Render a stack trace for the exception currently being handled.
 *
 * Call this from inside a catch block. Outside one there is no exception in
 * flight and the result is an empty string. During a nested throw every active
 * exception contributes its own frame list, in order.
 *
 * The trace points at the correct source location in opt builds, but inlining
 * means the frames need not correspond one-to-one with the source.
 *
 * @return  Formatted frame lists for the in-flight exceptions, concatenated.
 */
std::string getCurrentExceptionStackTrace();

} // namespace packsim
