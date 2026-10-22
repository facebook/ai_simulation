/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <cstdlib>
#include <exception>
#include <random>
#include <string>
#include <vector>

#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/Random.h>
#include <folly/container/F14Map.h>
#include <folly/init/Init.h>
#include <folly/logging/LogStreamProcessor.h>
#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>
#include <gflags/gflags.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
// @oss-disable
#include "packsim/simulator/packsim-simulation.h"
#include "packsim/util/builders.h"
#include "packsim/util/event-sink.h"
// clang-format off
// @oss-disable
#include "packsim/util/filesystem-blob-store.h" // @oss-enable
// @oss-disable
#include "packsim/util/json-lines-event-sink.h" // @oss-enable
// @oss-disable
// @oss-disable
// clang-format on
#include "packsim/util/file-io.h"
#include "packsim/util/packsim-logger.h"
#include "packsim/validations/validation.h"
#include "packsim/validations/validator.h"

DEFINE_string(
    json_cfg,
    "",
    "Path to the simulation config, a serialized Thrift Simulation; the extension picks the format: .json, .cf or .bin, optionally followed by .gz or .zstd");
DEFINE_string(
    output_file,
    "packsim_result.json",
    "Output filename; the extension picks the format: .json, .cf or .bin, optionally followed by .gz or .zstd");
DEFINE_string(
    blob_json_cfg,
    "",
    "Blob-store path of the simulation config; the extension picks the format, as for --json_cfg");
DEFINE_string(blob_cfg, "", "Blob-store path of simulation config");
DEFINE_string(
    blob_output,
    "",
    "Blob-store path to write output to [WILL OVERWRITE]");
DEFINE_string(
    run_id,
    "",
    "Id that names this run's replay snapshot and is recorded in its events and result; a random one if empty");
DEFINE_bool(
    allow_no_output,
    false,
    "If true, will still run simulation if output_file and blob_output are both empty");
DEFINE_bool(
    no_real_world_timing,
    false,
    "If true, discards the real world timing part of the result struct; with a set seed, runId (unless --run_id is given) and stats.maxRssBytes still differ between runs");
DEFINE_uint64(
    random_seed,
    0,
    "If provided, use the argument as the seed for the random generator");
DEFINE_bool(
    add_simplified_output,
    false,
    "If true, add simplified result to output (local file and blob store)");
DEFINE_bool(
    is_test,
    false,
    "If true, skip the replay snapshot (packsim-replay cannot re-run this run) and the run's event log");
DEFINE_bool(
    validate,
    false,
    "If true, run all config validations before simulating and abort on failure");
DEFINE_bool(
    validate_ignore_warnings,
    true,
    "When validating (--validate), treat validations that report only warnings as passing");
DEFINE_bool(
    validate_stop_on_error,
    false,
    "When validating (--validate), stop at the first validation that reports an error");
// @oss-disable
    // @oss-disable
    // @oss-disable
    // @oss-disable

namespace {
// Where a run's replay snapshot is stored, as a blob-store path.
// `packsim-replay` reads back from the same one and the two have to agree.
// @oss-disable
constexpr auto kSnapshotPathPrefix = "_packsim_runs/snapshots"; // @oss-enable
// log2(bytes per GiB): right-shift a byte count by this to get GiB.
constexpr uint64_t kBytesToGiBShift = 30;
} // namespace

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage(
      "Runs a PackSim simulation.\n"
      "Usage: packsim --json_cfg=<config> [--output_file=<result>] [flags]");
  folly::Init init(&argc, &argv);
  packsim::updateLogLevel();

  std::string runId = FLAGS_run_id;
  if (runId.empty()) {
    runId = fmt::format("PACKSIM_RUN_{:016x}", folly::Random::secureRand64());
  }

  packsim::thrift::Simulation cfg;

  // @oss-disable
  // Read the blob-store flags into locals once. // @oss-enable
  std::string blobJsonCfg = FLAGS_blob_json_cfg;
  std::string blobCfg = FLAGS_blob_cfg;
  std::string blobOutputFlag = FLAGS_blob_output;
  // @oss-disable
      // @oss-disable

  if (!blobJsonCfg.empty() && !blobCfg.empty()) {
    XLOGF(ERR, "Both flags blob_json_cfg and blob_cfg can't be specified");
    return EXIT_FAILURE;
  }

  // @oss-disable
  packsim::io::FilesystemBlobStore blobStore; // @oss-enable
  // @oss-disable
  packsim::JsonLinesEventSink eventSink; // @oss-enable

  bool configLoaded = false;
  try {
    configLoaded = blobCfg.empty()
        ? packsim::io::readConfig(&blobStore, FLAGS_json_cfg, blobJsonCfg, cfg)
        : packsim::io::readConfig(&blobStore, FLAGS_json_cfg, blobCfg, cfg);
  } catch (const std::exception& e) {
    XLOGF(ERR, "Failed to load simulation config: {}", e.what());
    return EXIT_FAILURE;
  }

  const auto seedInfo = gflags::GetCommandLineFlagInfoOrDie("random_seed");
  if (!seedInfo.is_default) {
    cfg.random()->seed() = FLAGS_random_seed;
    cfg.random()->useDeterministicSeed() = true;
  }

  if (!configLoaded) {
    XLOGF(ERR, "No config provided; exiting...");
    return EXIT_FAILURE;
  }

  // Draw an unfixed seed here rather than in Random, so the replay snapshot
  // written below records it and packsim-replay reproduces this run.
  if (!*cfg.random()->useDeterministicSeed()) {
    cfg.random()->seed() = std::random_device{}();
    cfg.random()->useDeterministicSeed() = true;
    XLOGF(INFO, "Using randomly generated seed of {}", *cfg.random()->seed());
  }

  if (FLAGS_validate) {
    const auto validationResults = packsim::runValidations(
        cfg,
        packsim::ValidatorOptions{
            .ignoreWarnings = FLAGS_validate_ignore_warnings,
            .stopOnError = FLAGS_validate_stop_on_error});
    packsim::Validation::print(validationResults);
    if (!validationResults.empty()) {
      XLOGF(
          ERR,
          "Config validation failed with {} failing validation(s); exiting.",
          validationResults.size());
      return EXIT_FAILURE;
    }
    XLOGF(INFO, "All config validations passed.");
  }

  std::string blobOutput = blobOutputFlag;
  const std::string& configResultPath = *cfg.jobInfo()->resultPath();
  // @oss-disable
    // @oss-disable
  // @oss-disable
  if (!configResultPath.empty() && configResultPath != blobOutput) {
    XLOGF(
        WARN,
        "Ignoring jobInfo.resultPath {} from the config: --blob_output names the result's blob path",
        configResultPath);
  }

  // @oss-disable
      // @oss-disable
  const bool isTest = FLAGS_is_test; // @oss-enable

  // No second destination is defaulted here: `--output_file` already writes a
  // local file, and the blob store is rooted at the working directory, so a
  // defaulted blob path would write the same result a second time into a
  // directory beside the sources. Pass `--blob_output` to ask for one.
  // @oss-disable
    // @oss-disable
        // @oss-disable
  // @oss-disable

  if (FLAGS_output_file.size() == 0 && blobOutput.size() == 0) {
    if (!FLAGS_allow_no_output) {
      XLOGF(
          ERR,
          "No output specified -- cannot run without -allow_no_output flag");
      return EXIT_FAILURE;
    }
  }

  try {
    packsim::io::prepareOutputDestinations(FLAGS_output_file, blobOutput);
  } catch (const std::exception& e) {
    XLOGF(ERR, "Invalid output destination: {}", e.what());
    return EXIT_FAILURE;
  }

  const auto snapshotPath =
      fmt::format("{0}/{1}.bin.gz", kSnapshotPathPrefix, runId);
  if (!isTest) {
    packsim::PacksimLogger::initialize(runId, eventSink);

    try {
      packsim::io::writeConfigToBlobStore(blobStore, snapshotPath, cfg);
      // Names the binary and the flag rather than a build-system invocation:
      // this line prints on every run, and the reader is as likely to have
      // installed the binary as to have a source checkout of any kind.
      XLOGF(INFO, "Replay this run with: packsim-replay --run_id={}", runId);
    } catch (std::exception const& e) {
      XLOGF(
          ERR,
          "Unable to store the simulation config for replay {}: {}",
          snapshotPath,
          e.what());
      packsim::PacksimLogger::log(e);
    }
  }

  try {
    packsim::PacksimLogger::log(packsim::LogDataType::BEGIN);
    packsim::PackSimSimulation sim(&cfg);

    // @oss-disable
      // @oss-disable
      // @oss-disable
      // @oss-disable
    // @oss-disable
      // @oss-disable
    // @oss-disable
    sim.run(); // @oss-enable

    if (sim.shouldOutputResults()) {
      auto result = sim.getThriftResult();
      result.runId() = runId;
      if (!blobOutput.empty()) {
        // @oss-disable
            // @oss-disable
        result.manifoldPath() = blobOutput; // @oss-enable
      }

      const auto echoMetric = [](char const* name, uint64_t value) {
        XLOGF(INFO, "metric({})={}", name, value);
      };

      // Echo basic metrics.
      echoMetric("endTimeNs", result.stats()->endTimeNs().value());
      echoMetric(
          "totalBytesDelivered", result.stats()->totalBytesDelivered().value());
      echoMetric("num_workloads", result.workloads()->size());
      echoMetric(
          "maxRssGBytes",
          result.stats()->maxRssBytes().value() >> kBytesToGiBShift);

      if (FLAGS_no_real_world_timing) {
        result.realWorldTiming() = packsim::thrift::RealWorldTiming();
      }

      packsim::io::writeOutputFile(
          &blobStore,
          FLAGS_output_file,
          blobOutput,
          result,
          FLAGS_add_simplified_output);
    }
    packsim::PacksimLogger::log(packsim::LogDataType::END);
  } catch (std::exception const& e) {
    packsim::PacksimLogger::log(e);
    throw; // rethrow the same exception.
  }

  return EXIT_SUCCESS;
}
