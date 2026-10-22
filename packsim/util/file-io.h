/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>

#include <folly/CppAttributes.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/util/blob-store.h"

namespace packsim::io {

// The blob store is passed in rather than constructed here so that this target
// does not depend on any particular object store. It is the first parameter
// throughout, so the dependency reads the same way at every call site.
//
// It is a reference where a store is required and a FOLLY_NULLABLE pointer
// where it is optional; null means "no blob store is available", and a request
// for a blob path is then an error rather than a silent no-op.

void writeOutputFile(
    IBlobStore* FOLLY_NULLABLE blobStore,
    const std::string& outputFileName,
    const std::string& blobOutput,
    const packsim::thrift::SimulationResult& result,
    bool addSimplifiedOutput,
    bool printTiming = true);

/**
 * Checks the result destinations before a run, so a bad one fails before the
 * simulation rather than after it. Either name may be empty, meaning that
 * destination is not wanted. Missing parent directories of `outputFileName`
 * are created; an existing file there is left in place, to be overwritten by
 * `writeOutputFile`.
 *
 * @throw std::invalid_argument if either name has an extension `FileType`
 *   does not accept, `outputFileName` starts with "~", or `outputFileName`
 *   is a directory or a symlink to one.
 * @throw std::runtime_error if the parent directory of `outputFileName`
 *   cannot be created.
 */
void prepareOutputDestinations(
    const std::string& outputFileName,
    const std::string& blobOutput);

bool checkOutputPathAndCreateDir(
    const std::string& outputFileName,
    bool overwrite);

void writeConfigToBlobStore(
    IBlobStore& blobStore,
    const std::string& path,
    packsim::thrift::Simulation const& simCfg);

bool readConfig(
    IBlobStore* FOLLY_NULLABLE blobStore,
    const std::string& filePath,
    const std::string& blobPath,
    packsim::thrift::Simulation& cfg);

} // namespace packsim::io
