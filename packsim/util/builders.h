/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <string>

#include <gflags/gflags.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/random.h"
#include "packsim/util/config-builders.h"
#include "packsim/util/transport-kinds.h"

// NOTE: This is capitalized as it might be set as an environment variable
DECLARE_string(FOLLY_LOG_LEVEL);

namespace packsim {

/**
 * Parse a case-insensitive transport name at a string-based API boundary.
 *
 * @param transportType  Transport name to parse.
 *
 * @return               The typed transport selection.
 *
 * @throw std::invalid_argument If the transport name is unrecognized.
 */
TransportKind parseTransportKind(const std::string& transportType);

// Zones in the four-zone Clos fabric the builders below produce.
inline constexpr int32_t kFourZoneClosZones = 4;
// Planes the spine (CTSW) and aggregation (ATSW) tiers are split across.
inline constexpr int32_t kFourZoneClosPlanes = 2;

/**
 * Build the fabric, switch-buffer and NIC parameters of the four-zone Clos
 * preset: `PackSimSimConfig`'s illustrative defaults, with the accelerators
 * spread evenly over the four zones. At 128 accelerators that is 4 racks of 8
 * per zone, 4 CTSWs per zone in each of 2 planes, and 2 ATSWs per plane.
 *
 * @param numAccel     Total accelerators, spread evenly over the zones. Only
 *                     multiples of 32 (4 zones x 8 accelerators per rack)
 *                     build whole racks; the remainder is dropped, and fewer
 *                     than 32 builds a switch-only fabric.
 * @param numQueues    NIC and switch queues per port.
 *
 * @return             The preset's parameters.
 */
packsim::PackSimSimConfig buildFourZoneClosCliConfig(
    uint32_t numAccel,
    uint8_t numQueues = 1);

// Builds the fabric, switch-buffer and NIC parameters of a four-zone Clos for
// `numAccel` accelerators with `numQueues` queues per port. Example:
// `&buildFourZoneClosCliConfig`, the preset the builders use by default.
using BuildFabricFn =
    std::function<PackSimSimConfig(uint32_t numAccel, uint8_t numQueues)>;

/** Options for constructing a four-zone Clos simulation configuration. */
struct FourZoneClosConfigOptions {
  // Total accelerators in the simulation.
  uint32_t numAccel{128};

  // Number of NIC and switch queues.
  uint8_t numQueues{1};

  // Transport implementation.
  TransportKind transportType{TransportKind::ROCE};

  // RoCE congestion-control algorithm.
  RoceCongestionControl congestionControl{RoceCongestionControl::NONE};

  // RoCE loss-recovery algorithm.
  RoceLossRecovery lossRecovery{RoceLossRecovery::NONE};

  // Builds the fabric's parameters.
  BuildFabricFn buildFabricFn{&buildFourZoneClosCliConfig};
};

/**
 * Build a four-zone Clos simulation configuration.
 *
 * @param options                Topology, queue, transport, and algorithm
 *                               options.
 *
 * @return                       The generated simulation configuration.
 *
 * @throw std::invalid_argument If an option is unrecognized, or if the
 *                              selected transport is incompatible with the
 *                              selected algorithm.
 */
packsim::thrift::Simulation buildFourZoneClosConfig(
    const FourZoneClosConfigOptions& options);

/**
 * Build a four-zone Clos config with default RoCE transport settings.
 *
 * The defaults are TransportKind::ROCE with RoceCongestionControl::NONE and
 * RoceLossRecovery::NONE, which is the split-option spelling of the transport
 * this overload previously selected via the "OptimisticRoce" string. Same
 * generated config, named in the new three-knob vocabulary.
 */
packsim::thrift::Simulation buildFourZoneClosConfig(
    uint32_t numAccel,
    uint8_t numQueues = 1);

/**
 * Build the workload config for a workload type.
 *
 * @param random     Random source for the vectorized all-to-all sizes.
 * @param type       The workload type to build.
 * @param numAccel   Number of accelerators the workload spans.
 * @param sizeBytes  Collective size in bytes.
 * @param numChunks  Number of chunks the collective is split into.
 *
 * @return           The workload config.
 * @throw            std::runtime_error for a type it does not build, such as
 *                   UNKNOWN, BATCH or NEW_WORKLOAD, or for a value outside
 *                   the enum.
 */
packsim::workload::thrift::Workload getWorkCfg(
    Random& random,
    packsim::thrift::WorkloadType type,
    uint32_t numAccel,
    uint32_t sizeBytes,
    uint32_t numChunks = 16);
packsim::workload::thrift::Workload getArDbtCfg(
    Random& random,
    uint32_t numAccel,
    uint32_t sizeBytes,
    uint32_t numChunks = 16);
packsim::workload::thrift::Workload getArRhdCfg(
    Random& random,
    uint32_t numAccel,
    uint32_t sizeBytes,
    uint32_t numChunks = 16);

void assignNewWorkloadSchedule(
    const packsim::thrift::FlexTopology& topology,
    packsim::thrift::WorkloadNewSchedule& sched,
    Random& random);

void updateLogLevel();

/** Options for constructing a four-zone Clos workload simulation. */
struct FourZoneClosWorkloadOptions {
  // Workload collective.
  packsim::thrift::WorkloadType workloadType{
      packsim::thrift::WorkloadType::ALL_TO_ALL};

  // Total accelerators in the simulation.
  uint32_t numAccel{128};

  // Bytes in each workload instance.
  uint32_t sizeBytes{1};

  // Number of workload copies.
  uint32_t numCopies{1};

  // Unit used to schedule workloads.
  packsim::thrift::SchedulingUnit unit{
      packsim::thrift::SchedulingUnit::ENDPOINT};

  // Method used to schedule workloads.
  packsim::thrift::SchedulingMethod method{
      packsim::thrift::SchedulingMethod::SEQUENTIAL};

  // Transport implementation.
  TransportKind transportType{TransportKind::ROCE};

  // RoCE congestion-control algorithm.
  RoceCongestionControl congestionControl{RoceCongestionControl::NONE};

  // RoCE loss-recovery algorithm.
  RoceLossRecovery lossRecovery{RoceLossRecovery::NONE};

  // Chunks in each collective.
  uint32_t numChunks{16};

  // Builds the fabric's parameters.
  BuildFabricFn buildFabricFn{&buildFourZoneClosCliConfig};
};

packsim::thrift::Simulation buildFourZoneClosWorkloadSim(
    Random& random,
    const FourZoneClosWorkloadOptions& options);
} // namespace packsim
