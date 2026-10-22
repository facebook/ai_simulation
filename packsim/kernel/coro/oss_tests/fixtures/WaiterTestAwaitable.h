/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <coroutine>

#include "packsim/kernel/coro/WaiterState.h"

namespace packsim::coro::tests {

/**
 * Test-only awaitable that bridges a SimTask coroutine to a WaiterState:
 * suspends the coroutine, installs its handle into the given waiter, and
 * records resumption via a flag. Mirrors what SimSignal/SimFuture/SimQueue
 * do internally — exercising WaiterState's contract directly without
 * going through an owning primitive.
 */
struct WaiterTestAwaitable {
  // Single-waiter slot the suspended coroutine's handle is installed into.
  detail::WaiterState& waiter;
  // Set to true when the coroutine resumes past the co_await.
  bool& resumed;

  bool await_ready() const noexcept {
    return false;
  }

  void await_suspend(std::coroutine_handle<> h) const noexcept {
    waiter.install(h);
  }

  void await_resume() const noexcept {
    resumed = true;
  }
};

} // namespace packsim::coro::tests
