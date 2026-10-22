/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdlib>
#include <exception>
#include <string>

#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/Utility.h>
#include <folly/init/Init.h>
#include <folly/logging/LogStreamProcessor.h>
#include <folly/logging/xlog.h>
#include <gflags/gflags.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/simulator/packsim-simulation.h"
#include "packsim/util/builders.h"
// clang-format off
// @oss-disable
#include "packsim/util/filesystem-blob-store.h" // @oss-enable
// clang-format on
#include "packsim/util/file-io.h"

DEFINE_string(run_id, "", "The run id used to store the simulation config");
DEFINE_string(
    output_file,
    "",
    "Output filename, <run-id>-result.json if empty; the extension picks the format: .json, .cf or .bin, optionally followed by .gz or .zstd");

namespace {
// Blob-store path prefix under which simulation snapshots are stored. It has to
// match the one `packsim.cpp` writes to.
// @oss-disable
constexpr auto kSnapshotPathPrefix = "_packsim_runs/snapshots"; // @oss-enable
} // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage(
      "Re-runs a stored PackSim run.\n"
      "Usage: packsim-replay --run_id=<run-id> [--output_file=<result>]");
  folly::Init init(&argc, &argv);
  packsim::updateLogLevel();

  if (FLAGS_run_id.empty()) {
    XLOGF(ERR, "Run id not specified");
    return EXIT_FAILURE;
  }

  const auto path =
      fmt::format("{}/{}.bin.gz", kSnapshotPathPrefix, FLAGS_run_id);

  packsim::thrift::Simulation cfg;
  // @oss-disable
  packsim::io::FilesystemBlobStore blobStore; // @oss-enable

  bool configLoaded = false;
  try {
    configLoaded = packsim::io::readConfig(&blobStore, "", path, cfg);
  } catch (const std::exception& e) {
    XLOGF(ERR, "Failed to load replay snapshot {}: {}", path, e.what());
    return EXIT_FAILURE;
  }

  if (!configLoaded) {
    XLOGF(ERR, "No config provided; exiting...");
    return EXIT_FAILURE;
  }

  const auto filename = FLAGS_output_file.empty()
      ? fmt::format("{0}-result.json", FLAGS_run_id)
      : folly::copy(FLAGS_output_file);
  try {
    packsim::io::prepareOutputDestinations(filename, /*blobOutput=*/"");
  } catch (const std::exception& e) {
    XLOGF(ERR, "Invalid output destination: {}", e.what());
    return EXIT_FAILURE;
  }

  packsim::PackSimSimulation sim(&cfg);

  sim.run();

  if (sim.shouldOutputResults()) {
    auto result = sim.getThriftResult();
    result.runId() = FLAGS_run_id;

    packsim::io::writeOutputFile(
        &blobStore, filename, {}, result, /*addSimplifiedOutput=*/false);
  }

  return EXIT_SUCCESS;
}
