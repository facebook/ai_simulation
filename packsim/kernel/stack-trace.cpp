/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/kernel/stack-trace.h"

#include <sstream>

#include <folly/debugging/exception_tracer/ExceptionTracer.h>

namespace packsim {

std::string getCurrentExceptionStackTrace() {
  std::ostringstream out;
  for (const auto& exceptionInfo :
       folly::exception_tracer::getCurrentExceptions()) {
    out << exceptionInfo;
  }
  return out.str();
}

} // namespace packsim
