/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/packet/allocator/GlobalManager.h"

#include <folly/SingletonThreadLocal.h>
#include <folly/detail/Futex.h>
#include <folly/detail/StaticSingletonManager.h>

namespace packsim {

// static
detail::PacketAllocator* GlobalManager::getPoolManager() {
  return &folly::SingletonThreadLocal<detail::PacketAllocator>::get();
}

} // namespace packsim
