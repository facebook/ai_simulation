/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Conv.h>

namespace packsim {

// Thrift adapter that stores an integer field as T in C++ while the wire type
// stays the wider signed Wire. A wider wire type lets JSON and Python write
// every value of T; their signed range of a same-width wire type stops at half
// of it (32767 for a 16-bit field). Reading a value that does not fit T, such
// as a negative one, throws folly::ConversionError.
template <typename T, typename Wire>
struct NarrowIntAdapter {
  static T fromThrift(const Wire value) {
    return folly::to<T>(value);
  }

  static Wire toThrift(const T value) {
    return value;
  }
};

} // namespace packsim
