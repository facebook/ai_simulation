/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <vector>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/random.h"

namespace packsim {

/**
 * Apply the per-workload-type fixups a raw config may be missing.
 *
 * @param w       Workload to fix up, modified in place.
 * @param random  Source of randomness for the fixups that need one.
 *
 * @return        The workloads to use in place of `w`: `w` itself for most
 *                types, or the per-rail split for a rail-split workload.
 *
 * @throw std::invalid_argument  If a rankToId or rankToIdList key of `w`, or of
 *                               a batch child once it inherits w's rankToId,
 *                               is not below that workload's numEndpoints.
 * @throw std::runtime_error     If a rail-split workload (AllGather,
 *                               ReduceScatter or AllReduce, batch children
 *                               included) sets numRails to 0, or cannot be
 *                               divided evenly across its rails: neither its
 *                               rank count nor its `sizeBytes` may leave a
 *                               remainder.
 * @throw std::out_of_range      If a rail-split workload's rankToId lacks a
 *                               rank below numEndpoints, or its non-empty
 *                               explicitStaggeredStartTimeNs has fewer than
 *                               numEndpoints entries.
 */
std::vector<packsim::workload::thrift::Workload> fixOneWorkload(
    packsim::workload::thrift::Workload& w,
    Random& random);

/**
 * Returns the number of Queue Pairs (QPs) for a given workload.
 *
 * By default, returns 2 QPs (1 for data, 1 for notify). If the workload has
 * channelSettings configured, calculates the total as:
 *   (numChannels * numQPsPerChannel) + 1 (for the notify QP)
 */
uint32_t getNumQPs(const packsim::workload::thrift::Workload& workload);

/**
 * Builds the qpnPairMap for each workload in the provided list.
 *
 * This function assigns unique Queue Pair Numbers (QPNs) across all workloads
 * by maintaining a shared counter (nextQpId) that persists across the entire
 * workload list. This ensures that when multiple workloads run concurrently,
 * each workload uses a distinct range of QPNs, preventing message queue
 * collisions on shared endpoints.
 *
 * For example, if workload 0 uses QPNs 1-32 on an endpoint, workload 1 will
 * start from QPN 33 on that same endpoint.
 *
 * If a workload already has a non-empty qpnPairMap (e.g., supplied in the
 * input config), it is skipped and not overwritten.
 */
void buildQPNMap(std::vector<packsim::workload::thrift::Workload>& workloads);

/**
 * Builds the nicQpnPairMap for a workload from explicit QP templates.
 *
 * This function populates the NIC-aware QPN mapping from the qpTemplates list,
 * which contains explicit queue pair definitions with NIC indices.
 * This is the preferred path when QP-template connectivity data is available.
 *
 * @param workload The workload to populate nicQpnPairMap for
 * @param nextQpn  Global QPN counter shared across all templates and workloads
 *                 to ensure globally unique QPNs
 */
void buildNicQPNMapFromQpTemplates(
    packsim::workload::thrift::Workload& workload,
    uint32_t& nextQpn);

/**
 * Builds the nicQpnPairMap for a workload from the legacy qpnPairMap.
 *
 * This function creates a NIC-aware mapping from the legacy flat QPN map
 * by assuming all queue pairs belong to NIC 0. This provides backward
 * compatibility for configurations that don't specify NIC information.
 *
 * @param workload The workload to populate nicQpnPairMap for
 */
void buildNicQPNMapFromLegacy(packsim::workload::thrift::Workload& workload);

/**
 * Main entry point for building the NIC-aware QPN map.
 *
 * This function determines the appropriate source for QPN data and
 * populates the nicQpnPairMap accordingly:
 * 1. If qpTemplates is non-empty, uses buildNicQPNMapFromQpTemplates
 * 2. Otherwise, falls back to buildNicQPNMapFromLegacy
 *
 * Skips workloads whose nicQpnPairMap is already non-empty (e.g., supplied in
 * the input config); they keep their qpTemplates unexpanded.
 *
 * @param workloads The workloads to populate nicQpnPairMap for
 * @param nextQpn   Global QPN counter shared across all workloads
 */
void buildNicQPNMap(
    std::vector<packsim::workload::thrift::Workload>& workloads,
    uint32_t& nextQpn);

/**
 * Iterates over the full workload schedule fixing all of the workloads
 * one-by-one. Also, validates that all workloads have unique non-empty names.
 * Populates a mapping between the new workload names and original names.
 * Assigns unique workload IDs to all workloads.
 */
void validateAndFixWorkloads(
    packsim::thrift::Simulation& simCfg,
    Random& random);

void fixupSimCfg(packsim::thrift::Simulation& simCfg, Random& random);

} // namespace packsim
