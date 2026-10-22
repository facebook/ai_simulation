/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/simulator/packsim-simulation.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/ExceptionString.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <folly/json/dynamic.h>
#include <folly/json/json.h>
#include <folly/logging/LogStreamProcessor.h>
#include <folly/logging/xlog.h>
#include <gflags/gflags.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/endpoint/runtime/dependency-info-builder.h"
#include "packsim/endpoint/runtime/dependency-tracker.h"
// @oss-disable
#include "packsim/endpoint/runtime/global-dag-coordinator.h"
#include "packsim/endpoint/runtime/message-tracker.h"
#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
// @oss-disable
#include "packsim/kernel/device-id.h"
#include "packsim/kernel/random.h"
// @oss-disable
// @oss-disable
#include "packsim/switch/fb-switch.h"
#include "packsim/topology/flex-topology.h"
#include "packsim/transport/transport.h"
// @oss-disable
#include "packsim/util/memory-usage.h"
#include "packsim/util/packsim-logger.h"
#include "packsim/util/sim-fix.h"
// @oss-disable
// @oss-disable

DEFINE_uint64(
    packsim_report_interval_sec,
    5,
    "How often to report progress in simulation, in seconds (must be positive)");
DEFINE_validator(
    packsim_report_interval_sec,
    +[](const char* /* flagName */, const uint64_t intervalSec) {
      return intervalSec > 0;
    });
DEFINE_uint64(
    packsim_log_progress_sec,
    300,
    "How often to log simulation progress to the event sink");
DEFINE_bool(
    packsim_detect_stall,
    false,
    "Stop the simulation when no bytes are delivered for several consecutive "
    "report intervals, and log each transport's worst incomplete QP where the "
    "transport reports one (RoCEv2 does not). Off by default so no workload "
    "can be killed unless it opts in.");
DEFINE_uint64(
    packsim_max_wall_time_sec,
    0,
    "Stop the simulation and fail it once its event loop has run this many "
    "seconds of wall-clock time, checked every --packsim_report_interval_sec. "
    "0, the default, means no limit.");

namespace packsim {

// Defined here (not in header) because the forward-declared unique_ptr
// members need their complete types for destruction.
PackSimSimulation::~PackSimSimulation() {
  // Runs before any member is destroyed. The stats read contexts owned by
  // members declared after scheduler_, which are destroyed before it.
  scheduler_.printExitStats();
}

namespace {
// Share of the expected bytes delivered, 1 when none are expected.
double completionFractionOf(
    const uint64_t bytesRcvd,
    const uint64_t bytesRcvdExpected) noexcept {
  return bytesRcvdExpected > 0
      ? static_cast<double>(bytesRcvd) / bytesRcvdExpected
      : 1;
}

// Why --packsim_max_wall_time_sec stopped a run, e.g. "Simulation exceeded its
// wall-clock limit of 600 s (--packsim_max_wall_time_sec) at 41.2000%
// complete".
std::string describeWallTimeLimit(
    const std::chrono::seconds maxWallTime,
    const double completionFraction) {
  return fmt::format(
      "Simulation exceeded its wall-clock limit of {} s "
      "(--packsim_max_wall_time_sec) at {:.4f}% complete",
      maxWallTime.count(),
      100 * completionFraction);
}

std::string getWorkloadNameFromRankResult(
    packsim::thrift::PerRankWorkloadResult const& result) {
  XCHECK_GT(*result.iteration(), 0U)
      << "Iteration for per rank result cannot be 0";
  if (*result.iteration() > 1) {
    return fmt::format("{}_ITER_{}", *result.name(), *result.iteration());
  }
  return *result.name();
}

// Whether the DAG coordinator can restart a DAG. It restarts one that completes
// an iteration while any DAG has completed fewer than `minIterationsPerDag`.
bool canRestartDag(const DependencyInfos* depInfos) noexcept {
  if (depInfos == nullptr || !depInfos->minIterationsPerDag.has_value()) {
    return false;
  }
  const uint32_t minIters = *depInfos->minIterationsPerDag;
  return minIters >= 2 || (minIters == 1 && depInfos->dagList.size() > 1);
}

} // namespace

static constexpr bool kShouldOutputResults = true;

PackSimSimulation::PackSimSimulation(packsim::thrift::Simulation* simCfg)
    : // fixupSimCfg is a wrapper over various things to deal with evolution of
      // the simulation config, etc. See util/sim-fix.cpp for more details.
      //
      // Because it modifies simCfg (and sets the random seed for the
      // simulations), it has to be called before constructing any of the
      // objects like flexTopo_ or depInfos_.
      //
      // To make this happen, the initialization argument to simCfg_ uses the
      // comma operator: https://en.wikipedia.org/wiki/Comma_operator
      //
      // In short, (a(), b) evaluates a(), discards the result, and returns b
      // That's exactly what happens here: We first call fixupSimCfg, discard
      // the return value (it's void anyway), and then return simCfg, which gets
      // used to initialize simCfg_.
      random_(*simCfg),
      simCfg_((fixupSimCfg(*simCfg, random_), simCfg)),
      flexTopo_(&scheduler_, &random_, *simCfg),
      depInfos_(DependencyInfoBuilder::build(*simCfg_)) {
  // @oss-disable

  if (depInfos_) {
    globalDagCoordinator_ = std::make_unique<GlobalDagCoordinator>(
        &scheduler_,
        depInfos_->dagList.size(),
        depInfos_->minIterationsPerDag.value_or(0));
    globalDagCoordinator_->setOnAllDagsFinishedCallback([this] {
      {
        std::unique_lock lk(workloadRankMutex_);
        XLOGF(DBG0, "Workloads complete at {}ns", lastRankComplete_.getNs());
      }
      scheduler_.stop();
    });
  }

  buildWorkloadRanks();

  // @oss-disable
      // @oss-disable

  uint64_t minLinkDelayNs = INT64_MAX;
  for (const auto& [linkName, link] : *simCfg->flex_topology()->links()) {
    const uint64_t delayNs = *link.delayConfig()->delayNs();
    // The smallest link delay is the simulator's lookahead, which must be
    // positive.
    if (delayNs == 0) {
      throw std::invalid_argument(
          fmt::format(
              "FlexLink '{}' has delayConfig.delayNs 0; every link needs a delay of at least 1 ns",
              linkName));
    }
    minLinkDelayNs = std::min(minLinkDelayNs, delayNs);
  }
  XLOGF(INFO, "Simulator lookahead time: {}ns", minLinkDelayNs);
  scheduler_.setLookaheadTimeFs(Time::fromNs(minLinkDelayNs).getFs());
}

void PackSimSimulation::buildWorkloadRanks() {
  uint32_t builtRanks = 0;
  folly::F14FastSet<std::string> wkldNames;
  folly::F14FastSet<uint32_t> wkldIds;

  // @oss-disable
      // @oss-disable
  // @oss-disable

  for (const auto& wCfg : *simCfg_->newWorkloadSchedule()->workloads()) {
    if (wCfg.rankToId().value().empty()) {
      // No rank to id mapping
      throw std::runtime_error(
          fmt::format(
              "Workload {} is not scheduled: its rankToId map is empty. "
              "Populate Workload.rankToId (field 40 in if/workload.thrift), "
              "which maps each rank to its device id, before running -- for "
              "example by calling assignNewWorkloadSchedule() from "
              "util/builders.h.",
              wCfg.name().value()));
    }
    if (wkldNames.contains(wCfg.name().value())) {
      // Duplicate workload names
      throw std::runtime_error(
          fmt::format(
              "Duplicate workload name {} found in config",
              wCfg.name().value()));
    }
    if (wkldIds.contains(wCfg.workloadId().value())) {
      // Duplicate workload ids
      throw std::runtime_error(
          fmt::format(
              "Duplicate workload id {} found in config",
              wCfg.workloadId().value()));
    }
    wkldNames.emplace(wCfg.name().value());
    wkldIds.emplace(wCfg.workloadId().value());

    for (const auto& [rank, idVals] : *wCfg.rankToId()) {
      std::unique_lock lk(workloadRankMutex_);
      const FlexDeviceId id{idVals};

      auto endpoint = flexTopo_.getWorkloadNode(id);
      endpoint->buildWorkloadRank(wCfg, rank);
      endpoint->setOnAllWorkloadsCompleteCallback(
          [this](const FlexDeviceId& id) { workloadRankComplete(id); });
      if (depInfos_) {
        globalDagCoordinator_->addDagNode(
            depInfos_->workloadToDagId.at(*wCfg.workloadId()),
            endpoint->getSimCtx());
      }
      incompleteWorkloadRanks_.emplace(endpoint->getFlexId());
      ++builtRanks;
    }
  }
  if (depInfos_) {
    for (const auto& wCfg : *simCfg_->newWorkloadSchedule()->workloads()) {
      XCHECK(!wCfg.rankToId()->empty()) << fmt::format(
          "Workload {} has no rank to ID map populated.", *wCfg.workloadId());

      auto const leaderRank = wCfg.rankToId()->begin()->first;
      auto const leaderId = FlexDeviceId(wCfg.rankToId()->begin()->second);
      auto* const leaderNode = flexTopo_.getWorkloadNode(leaderId);
      auto* const leaderContext = leaderNode->getSimCtx();
      auto leaderBarrier = std::make_unique<CollectiveBarrier>(
          leaderContext, leaderContext, leaderRank, &wCfg);
      auto const leaderBarrierPtr = leaderBarrier.get();

      for (const auto& [rank, idVals] : *wCfg.rankToId()) {
        std::unique_lock lk(workloadRankMutex_);
        const FlexDeviceId id{idVals};

        auto endpoint = flexTopo_.getWorkloadNode(id);
        if (rank == leaderRank) {
          endpoint->setCollectiveBarrier(
              *wCfg.workloadId(), std::move(leaderBarrier));
        } else {
          endpoint->setCollectiveBarrier(
              *wCfg.workloadId(),
              std::make_unique<CollectiveBarrier>(
                  leaderContext,
                  endpoint->getSimCtx(),
                  rank,
                  &wCfg,
                  leaderBarrierPtr));
        }
      }
    }
  }
  for (auto& endpoint : flexTopo_.getAllWorkloadNodes()) {
    if (depInfos_) {
      endpoint->setDependencyInfos(
          depInfos_.get(), globalDagCoordinator_.get());
    }
    endpoint->startWorkloads();
  }
  XLOGF(
      INFO,
      "Expected bytes: {} ({} rank workloads)",
      totalBytesRcvdExpected(),
      builtRanks);
}

bool PackSimSimulation::isWorkloadComplete() const noexcept {
  return incompleteWorkloadRanks_.empty();
}

void PackSimSimulation::workloadRankComplete(const FlexDeviceId& id) {
  std::unique_lock lk(workloadRankMutex_);
  incompleteWorkloadRanks_.erase(id);
  const auto remainingRanks = incompleteWorkloadRanks_.size();
  // This function may be called in non-deterministic order; keep track of the
  // maximum time seen
  lastRankComplete_ = std::max(lastRankComplete_, scheduler_.timeNow());

  XLOGF(
      DBG,
      "WorkloadRank complete: {} ({} remain)",
      id.toString(),
      remainingRanks);
  // When a DAG can restart, a node that has run out of workloads may be rebuilt
  // for its next iteration, so the DAG coordinator ends the run instead. A
  // coordinator with no DAG nodes never raises that event.
  const bool coordinatorEndsRun = canRestartDag(depInfos_.get()) &&
      globalDagCoordinator_ != nullptr && globalDagCoordinator_->hasDagNodes();
  if (remainingRanks == 0 && !coordinatorEndsRun) {
    XLOGF(DBG0, "Workloads complete at {}ns", lastRankComplete_.getNs());
    scheduler_.stop();
  }
}

void PackSimSimulation::run() {
  {
    std::unique_lock lk(workloadRankMutex_);
    lastRankComplete_ = Time::fromNs(0);
  }
  startTime_ = std::chrono::system_clock::now();
  progressThread_ = std::jthread(
      [this](const std::stop_token& stopToken) { logProgress(stopToken); });

  SCOPE_EXIT {
    complete_ = true;
    endTime_ = std::chrono::system_clock::now();
    // Joined here rather than by the destructor: the thread reads the topology
    // and the workload counters, and a caller that writes results before
    // destroying the simulation would otherwise wait out the rest of a report
    // interval.
    progressThread_.request_stop();
    progressThread_.join();
  };
  try {
    scheduler_.run();
  } catch (const std::exception& e) {
    XLOGF(ERR, "Simulation failed: {}", folly::exceptionStr(e));
    throw;
  } catch (...) {
    XLOGF(
        ERR,
        "Simulation failed: {}",
        folly::exceptionStr(std::current_exception()));
    throw;
  }

  // Stall detection (on the progress thread) defers the diagnostic dump to
  // here: the event loop has now returned, so the simulation thread can read
  // per-QP transport state without racing the scheduler.
  if (stalled_) {
    dumpIncompleteState();
  }
  // Thrown rather than left to verifySimulationMetrics(), so the run fails
  // even with allowIncompleteWorkloads and its error names the limit.
  if (timedOut_) {
    throw std::runtime_error(describeWallTimeLimit(
        std::chrono::seconds{FLAGS_packsim_max_wall_time_sec},
        completionFractionOf(totalBytesRcvd(), totalBytesRcvdExpected())));
  }
  verifySimulationMetrics();
}

void PackSimSimulation::verifySimulationMetrics() {
  // Verify that all endpoints have completed. Transports whose
  // verifyComplete() does not check outstanding messages (the base default,
  // which logs a WARN, and RoCEv2) report complete unconditionally.
  bool complete = true;
  for (const auto& endpoint : flexTopo_.getAllEndpoints()) {
    const bool c = endpoint->getTransport()->verifyComplete();
    complete = complete && c;
  }
  if (!complete) {
    XLOGF(WARN, "Some transports report non-completion");
  }

  if (!this->isWorkloadComplete()) {
    auto msg = fmt::format(
        "{} workload ranks didn't complete", incompleteWorkloadRanks_.size());
    if (!*simCfg_->allowIncompleteWorkloads()) {
      XLOGF(ERR, "{}", msg);
      throw std::runtime_error(msg);
    }
    XLOGF(WARN, "{}", msg);
  }

  desEvents_ = scheduler_.getTotalEvents();
  XLOGF(DBG0, "Total DES events: {}", desEvents_);

  const uint64_t totalBytes = totalBytesRcvd();
  const uint64_t totalBytesExpected = totalBytesRcvdExpected();
  XLOGF(
      DBG0,
      "Total bytes received: {}, expected total bytes: {}",
      totalBytes,
      totalBytesExpected);

  if (totalBytesExpected != totalBytes) {
    auto msg = totalBytesExpected > totalBytes
        ? fmt::format("Missing bytes: {}", totalBytesExpected - totalBytes)
        : fmt::format("Extra bytes: {}", totalBytes - totalBytesExpected);
    msg += fmt::format(
        " (expected: {}, actual: {})", totalBytesExpected, totalBytes);
    if (!*simCfg_->allowIncompleteWorkloads()) {
      XLOGF(ERR, "{}", msg);
      throw std::runtime_error(msg);
    }
    XLOGF(WARN, "{}", msg);
  }
}

uint64_t PackSimSimulation::totalBytesRcvd() const {
  uint64_t byts{0};
  for (const auto& ctx : flexTopo_.simCtxs_) {
    byts += ctx->dataRcvd;
  }
  return byts;
}

uint64_t PackSimSimulation::totalBytesRcvdExpected() const {
  uint64_t byts{0};
  for (const auto& ctx : flexTopo_.simCtxs_) {
    byts += ctx->dataRcvdExpected;
  }
  return byts;
}

void PackSimSimulation::logProgress(const std::stop_token& stopToken) {
  const auto startTime = std::chrono::steady_clock::now();
  const std::chrono::seconds reportInterval{FLAGS_packsim_report_interval_sec};

  // Log progress every logProgressMultiple th loop.
  const auto logProgressMultiple = std::max<uint64_t>(
      1, FLAGS_packsim_log_progress_sec / FLAGS_packsim_report_interval_sec);
  uint64_t loopIndex = 0;

  // The thread sleeps by waiting on this condition variable, which returns at
  // the next report time or as soon as run() requests a stop. Nothing notifies
  // it otherwise.
  std::mutex wakeMutex;
  std::condition_variable_any wake;
  std::unique_lock wakeLock(wakeMutex);

  while (true) {
    // Reports land on multiples of the interval since the start.
    const auto sinceStart = std::chrono::steady_clock::now() - startTime;
    const auto nextReport =
        startTime + (sinceStart / reportInterval + 1) * reportInterval;
    wake.wait_until(wakeLock, stopToken, nextReport, [] { return false; });
    if (stopToken.stop_requested()) {
      return;
    }

    double elapsedSeconds = std::chrono::duration<double>(
                                std::chrono::steady_clock::now() - startTime)
                                .count();
    // Progress-thread read of counters the sim thread updates; intentionally
    // unsynchronized, as it only feeds logging and the stall heuristic.
    const uint64_t bytesRcvd = totalBytesRcvd();

    if (bytesRcvd == 0) {
      XLOGF(
          INFO,
          "No data received in first {} seconds",
          std::round(elapsedSeconds));
    }
    const uint64_t bytesRcvdExpected = totalBytesRcvdExpected();
    const double completionFraction =
        completionFractionOf(bytesRcvd, bytesRcvdExpected);
    const double estRemain =
        elapsedSeconds / completionFraction - elapsedSeconds;
    uint64_t maxSimTimeNs = 0;
    for (const auto& ctx : flexTopo_.simCtxs_) {
      maxSimTimeNs = std::max(maxSimTimeNs, ctx->nowNs());
    }
    XLOGF(
        INFO,
        "{:0.01f}% complete in {:0.01f} seconds! Estimated time to completion: {:0.01f} seconds. Bytes received: {}. Sim time: {}ns",
        100 * completionFraction,
        elapsedSeconds,
        estRemain,
        bytesRcvd,
        maxSimTimeNs);

    // Report progress to the event sink.
    if ((++loopIndex % logProgressMultiple) == 0) {
      PacksimLogger::logProgress(100 * completionFraction, bytesRcvd);
    }

    if (FLAGS_packsim_detect_stall &&
        checkForStall(bytesRcvd, bytesRcvdExpected, completionFraction)) {
      return;
    }
    if (checkWallTimeLimit(
            std::chrono::duration<double>{elapsedSeconds},
            completionFraction)) {
      return;
    }
  }
}

bool PackSimSimulation::checkWallTimeLimit(
    const std::chrono::duration<double> elapsed,
    const double completionFraction) {
  const std::chrono::seconds maxWallTime{FLAGS_packsim_max_wall_time_sec};
  if (maxWallTime.count() == 0 || elapsed < maxWallTime) {
    return false;
  }
  XLOGF(
      ERR,
      "{}. Stopping.",
      describeWallTimeLimit(maxWallTime, completionFraction));
  timedOut_ = true;
  scheduler_.stop();
  return true;
}

bool PackSimSimulation::checkForStall(
    const uint64_t bytesRcvd,
    const uint64_t bytesRcvdExpected,
    const double completionFraction) {
  constexpr uint32_t kStallIntervals = 3;
  // A run that never delivers a byte is stalled too, but its first byte
  // legitimately lags startup (QPs may take several intervals to begin
  // transmitting), so allow a longer grace before flagging zero delivery.
  constexpr uint32_t kNoDeliveryStallIntervals = 12;
  // Clamp the unsigned delta: bytesRcvd is read off the simulation thread
  // without synchronization, so a stale read below prevBytesRcvd_ would
  // otherwise wrap to a huge value and be misread as progress, masking a stall.
  const uint64_t bytesThisInterval =
      bytesRcvd > prevBytesRcvd_ ? bytesRcvd - prevBytesRcvd_ : 0;
  prevBytesRcvd_ = bytesRcvd;

  // bytesRcvdExpected > 0: skip simulations with no expected data (e.g.,
  //   receive-only endpoints or empty workloads).
  // bytesThisInterval == 0: no new bytes delivered in this reporting period.
  if (bytesRcvdExpected > 0 && bytesThisInterval == 0) {
    ++stallCount_;
    // Strict threshold once delivery has started; the longer grace applies
    // only while still waiting for the first byte.
    const uint32_t stallThreshold =
        bytesRcvd > 0 ? kStallIntervals : kNoDeliveryStallIntervals;
    if (stallCount_ >= stallThreshold) {
      XLOGF(
          ERR,
          "Simulation stalled at {:.4f}% ({} bytes remaining). "
          "No progress for {} consecutive intervals. "
          "Stopping; each transport that reports one (RoCEv2 does not) logs "
          "its worst incomplete QP.",
          100 * completionFraction,
          bytesRcvdExpected - bytesRcvd,
          stallThreshold);
      // The state dump walks live transport structures the simulation thread
      // owns, so it cannot run here on the progress thread. Flag the stall and
      // stop the scheduler; the dump runs on the simulation thread once
      // scheduler_.run() returns.
      stalled_ = true;
      scheduler_.stop();
      return true;
    }
  } else {
    stallCount_ = 0;
  }
  return false;
}

void PackSimSimulation::dumpIncompleteState() {
  for (const auto& endpoint : flexTopo_.getAllEndpoints()) {
    endpoint->getTransport()->logWorstIncompleteQP();
  }
}

WorkloadNode* PackSimSimulation::getEndpoint(const FlexDeviceId& idx) {
  return flexTopo_.getWorkloadNode(idx);
}

std::string PackSimSimulation::getTraceEventsJson() const {
  folly::dynamic events = folly::dynamic::array;
  for (const auto& ctx : flexTopo_.simCtxs_) {
    for (auto& e : ctx->tracer.getTraceEvents()) {
      events.push_back(std::move(e));
    }
  }
  folly::dynamic trace = folly::dynamic::object;
  trace["traceEvents"] = std::move(events);
  trace["displayTimeUnit"] = "ns";
  return folly::toJson(trace);
}

packsim::thrift::SimulationResult PackSimSimulation::getThriftResult() {
  if (thriftResultConsumed_) {
    throw std::logic_error(
        "getThriftResult() called more than once; result data has already been moved");
  }
  thriftResultConsumed_ = true;

  packsim::thrift::SimulationResult res;
  if (*simCfg_->includeConfigInResults()) {
    res.config() = *simCfg_;
  }
  res.randomSeed() = random_.getSeed();
  res.chromeTrace() = getTraceEventsJson();
  // @oss-disable

  auto& stats = *res.stats();
  stats.desEvents() = desEvents_;
  stats.totalBytesDelivered() = totalBytesRcvd();
  stats.maxRssBytes() = getPeakRssBytes();

  {
    std::unique_lock lk(workloadRankMutex_);
    stats.endTimeNs() = lastRankComplete_.getNs();
  }

  auto& realWorldTiming = *res.realWorldTiming();
  const std::chrono::duration<double> start = startTime_.time_since_epoch();
  const std::chrono::duration<double> end = endTime_.time_since_epoch();
  realWorldTiming.start() = start.count();
  realWorldTiming.end() = end.count();
  realWorldTiming.runtime() = (end - start).count();
  XLOGF(
      DBG0,
      "Start, end, dur: {}, {}, {}",
      *realWorldTiming.start(),
      *realWorldTiming.end(),
      *realWorldTiming.runtime());

  for (const auto& switch_ : flexTopo_.getAllSwitches()) {
    auto switchSum = switch_->getSummary();
    auto name = switchSum.name().value();
    res.switchStats()->try_emplace(std::move(name), std::move(switchSum));
  }

  getWorkloadResults(res);

  res.snapshots() = flexTopo_.serializeSnapshots();
  res.requestStats() = std::move(reqStats_);

  // @oss-disable
    // @oss-disable
  // @oss-disable

  for (const auto& nic_ : flexTopo_.getAllEndpoints()) {
    auto nicSum = nic_->getNicStatsSummaryResults();
    res.nics()->try_emplace(nic_->getName(), std::move(nicSum));
    if (auto multiPort = nic_->getTransport()->getMultiPortNicStats()) {
      res.multiPortNicStats()->try_emplace(
          nic_->getName(), std::move(*multiPort));
    }
  }

  // The last QP to complete across the whole simulation: the max over each
  // endpoint's own last-to-complete QP.
  std::optional<Transport::CompletedQp> simulationStraggler;
  for (const auto& endpoint : flexTopo_.getAllEndpoints()) {
    const auto endpointStraggler = endpoint->getTransport()->getLastStraggler();
    if (endpointStraggler.has_value()) {
      if (!simulationStraggler.has_value() ||
          endpointStraggler->completionTimeNs >
              simulationStraggler->completionTimeNs) {
        simulationStraggler = endpointStraggler;
      }
    }
  }

  // Set the last straggler in stats
  if (simulationStraggler.has_value()) {
    packsim::thrift::QpStragglerInfo resultStraggler;
    resultStraggler.srcId() = simulationStraggler->srcId.asVec();
    resultStraggler.dstId() = simulationStraggler->dstId.asVec();
    resultStraggler.srcQPN() = simulationStraggler->srcQPN;
    resultStraggler.dstQPN() = simulationStraggler->dstQPN;
    resultStraggler.completionTimeNs() = simulationStraggler->completionTimeNs;
    resultStraggler.qpIdentifier() = simulationStraggler->qpIdentifier;
    stats.lastStraggler() = resultStraggler;

    XLOGF(
        INFO,
        "Last QP to complete: {} at {}ns."
        " Re-run with {}=\"{}\"",
        simulationStraggler->qpIdentifier,
        simulationStraggler->completionTimeNs,
        simulationStraggler->debugFlag,
        simulationStraggler->qpIdentifier);
  }

  for (const auto& ctx : flexTopo_.simCtxs_) {
    for (const auto& [name, samples] : ctx->tracer.traces()) {
      auto& dest = res.traces()[name];
      dest.reserve(dest.size() + samples.size());
      for (const auto& sample : samples) {
        packsim::thrift::TraceSample ts;
        ts.timeNs() = sample.timeNs;
        ts.value() = sample.value;
        dest.emplace_back(std::move(ts));
      }
    }
  }

  XLOGF(
      INFO,
      "Ending simulation time: {} ns; {} events",
      *res.stats()->endTimeNs(),
      *res.stats()->desEvents());

  // @oss-disable
      // @oss-disable

  auto humanBytes = folly::prettyPrint(
      *res.stats()->maxRssBytes(),
      folly::PRETTY_BYTES,
      /* addSpace= */ false);

  XLOGF(
      INFO,
      "Simulation running time: {:0.03f} seconds (peak RSS: {})",
      *res.realWorldTiming()->runtime(),
      humanBytes);

  return res;
}

void PackSimSimulation::getWorkloadResults(
    packsim::thrift::SimulationResult& simRes) const {
  auto& workloads = *simRes.workloads();

  // Iterate across all WorkloadNodes and collect per-rank stats
  for (const auto& e : flexTopo_.getAllWorkloadNodes()) {
    for (auto const& wkldRankResult : e->getWorkloadRankResults()) {
      const auto name = getWorkloadNameFromRankResult(wkldRankResult);
      const auto rank = *wkldRankResult.rank();
      auto it = workloads.find(name);
      if (it == workloads.end()) {
        auto& res = workloads[name];
        res.name() = name;
        res.workloadId() = *wkldRankResult.workloadId();
        res.startTimeNs() = *wkldRankResult.startTimeNs();
        res.endTimeNs() = *wkldRankResult.endTimeNs();
        res.runTimeNs() = 0;
        res.inputName() = *wkldRankResult.name();
        res.iteration() = *wkldRankResult.iteration();
        it = workloads.find(name);
      }
      if (*it->second.workloadId() != *wkldRankResult.workloadId()) {
        throw std::runtime_error(
            fmt::format(
                "Per-rank results from different workload IDs found: {} != {}",
                *it->second.workloadId(),
                *wkldRankResult.workloadId()));
      }
      it->second.perRank()->try_emplace(rank, wkldRankResult);
    }
  }

  // Aggregate per-rank stats into overall workload completion stats
  for (auto& [name, wkldResult] : workloads) {
    if (wkldResult.perRank()->empty()) {
      continue;
    }
    auto& [_, firstRankResult] = *wkldResult.perRank()->begin();
    uint64_t startTimeNs = *firstRankResult.startTimeNs();
    uint64_t endTimeNs = *firstRankResult.endTimeNs();
    for (const auto& [_, rankResult] : *wkldResult.perRank()) {
      // Start time is when the last rank joins, to avoid measuring the impact
      // of start-time-jitter. This may occasionally result in
      // slightly-above-100%-of-roofline measurements.
      startTimeNs = std::max(startTimeNs, *rankResult.startTimeNs());
      // End time is when the last rank completes
      endTimeNs = std::max(endTimeNs, *rankResult.endTimeNs());
    }
    wkldResult.startTimeNs() = startTimeNs;
    wkldResult.endTimeNs() = endTimeNs;
    wkldResult.runTimeNs() = endTimeNs - startTimeNs;
  }

  for (const auto& workCfg : *simCfg_->newWorkloadSchedule()->workloads()) {
    workloads[*workCfg.name()].workCfg() = workCfg;
  }

  // Use results from first iteration to populate configs for other iterations.
  for (auto& [_, wkldResult] : workloads) {
    if (*wkldResult.iteration() > 1) {
      wkldResult.workCfg() = *workloads.at(*wkldResult.inputName()).workCfg();
    }
  }

  // Collect per-message results if tracking is enabled.
  if (simCfg_->networkLevelFeatureConfig().has_value() &&
      *simCfg_->networkLevelFeatureConfig()->enablePerMessageTracking()) {
    // Gather trackers from all nodes
    std::vector<const MessageTracker*> trackers;
    trackers.reserve(flexTopo_.getAllWorkloadNodes().size());
    for (const auto& node : flexTopo_.getAllWorkloadNodes()) {
      if (const auto* tracker = node->messageTracker()) {
        trackers.emplace_back(tracker);
      }
    }

    auto grouped = collectPerMessageResults(trackers);

    // A workload repeated by DAG iterations has one WorkloadResult per
    // iteration, all with the same workloadId.
    folly::F14FastMap<
        std::pair<uint64_t, uint64_t>,
        packsim::thrift::WorkloadResult*>
        workloadByIdAndIteration;
    for (auto& [name, wkldResult] : workloads) {
      workloadByIdAndIteration[{
          *wkldResult.workloadId(), *wkldResult.iteration()}] = &wkldResult;
    }

    for (auto& [wId, records] : grouped) {
      for (auto& record : records) {
        auto it = workloadByIdAndIteration.find({wId, *record.iteration()});
        if (it == workloadByIdAndIteration.end()) {
          XLOGF(
              WARN,
              "Per-message record for workloadId {} iteration {} has no "
              "matching workload",
              wId,
              *record.iteration());
          continue;
        }
        it->second->perMessageResults()->emplace_back(std::move(record));
      }
    }
  }

  // @oss-disable
}

bool PackSimSimulation::shouldOutputResults() const noexcept {
  return kShouldOutputResults;
}

} // namespace packsim
