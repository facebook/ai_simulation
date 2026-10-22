/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include "packsim/transport/transport.h"

namespace packsim {

/**
 * Construct the Transport that the NIC config's transport union selects.
 *
 * The union is the plug-point new transports are added at, so the mapping from
 * arm to implementation belongs here rather than in the endpoint that owns the
 * result: an endpoint has no reason to know every transport that exists.
 *
 * @param params  The owning endpoint's simulation context, address, NIC config
 *                and name.
 *
 * @return        The selected transport, never null.
 *
 * @throw std::invalid_argument if the transport union is unset, or selects an
 *        unsupported transport.
 */
std::unique_ptr<Transport> makeTransport(const TransportParams& params);

} // namespace packsim
