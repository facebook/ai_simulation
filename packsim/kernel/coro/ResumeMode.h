/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace packsim::coro {

/**
 * How the SimSignal/SimFuture/SimQueue primitives (and the
 * `detail::WaiterState` machinery they own) resume the suspended waiter
 * on notify():
 *   - Direct:   resume synchronously inside notify() — same call stack,
 *               same PDES tick, no scheduleNow allocation.
 *   - Deferred: post the resume through scheduleNow — bounds stack depth
 *               on long producer chains by giving the dispatcher a chance
 *               to unwind between steps.
 *
 * Lives in its own header (not in `WaiterState.h`) because `ResumeMode`
 * is part of the public constructor surface of every primitive that owns
 * a `detail::WaiterState`, while `WaiterState.h` is a detail header that
 * only the coroutine primitives are meant to include. Splitting the enum
 * out lets application code reach the public type without dragging in
 * (and transitively depending on) the detail implementation header.
 */
enum class ResumeMode {
  DIRECT,
  DEFERRED,
};

} // namespace packsim::coro
