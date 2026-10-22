/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/validations/register-validations.h"

#include "packsim/validations/channel-workload-should-not-mix-qp-sources.h"
// @oss-disable
#include "packsim/validations/link-node-should-match.h"
#include "packsim/validations/nic-config-should-exist.h"
#include "packsim/validations/no-cycles-in-workload-dag.h"
#include "packsim/validations/node-name-and-key-should-match.h"

namespace packsim {

namespace {
template <typename T>
void reg() {
  Validation::registerValidation(std::make_unique<T>());
}
} // namespace

void registerValidations() {
  static bool registered = false;
  if (registered) {
    return;
  }
  registered = true;

  reg<details::NodeNameAndKeyShouldMatch>();
  reg<details::NicConfigShouldExist>();
  reg<details::NoCyclesInWorkloadDag>();
  reg<details::LinkNodeShouldMatch>();
  // @oss-disable
  reg<details::ChannelWorkloadShouldNotMixQpSources>();
}
} // namespace packsim
