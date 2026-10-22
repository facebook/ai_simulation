/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * Examples demonstrating each PackSim coroutine primitive on the real
 * PDES (Parallel Discrete Event Simulation) engine. Each example shows a
 * specific pattern/scenario where the primitive shines, with annotated
 * output and a `// BEFORE (callback):` comment block showing the
 * equivalent callback-based code being replaced.
 *
 * Built as the `coro_demo` target, and run straight out of the build
 * directory. Prefer a debug build: the XDCHECK consistency checks are
 * compiled out of an optimised one, and they are half of what the examples
 * demonstrate. The XCHECK lifetime traps fire under any build mode.
 *
 * GROWTH GUIDANCE: at ~900 lines, this file is already long. Add a new
 * example only if it illustrates a distinct primitive-usage pattern not
 * already covered by the existing 14 (signal/future/queue/delay/schedule-on/
 * barrier patterns are well-represented). If a 15th example would be marginal
 * coverage of a pattern already shown, extend the existing example's
 * commentary instead. If a new pattern truly needs a fresh example, consider
 * splitting this file into multiple grouped demos (e.g., coro_demo_signal.cpp,
 * coro_demo_future.cpp) rather than letting this file grow without bound.
 */

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>
#include <folly/memory/not_null.h>
#include <gflags/gflags.h>

#include "packsim/kernel/coro/SimBarrier.h"
#include "packsim/kernel/coro/SimCancellableDelay.h"
#include "packsim/kernel/coro/SimDelay.h"
#include "packsim/kernel/coro/SimFuture.h"
#include "packsim/kernel/coro/SimQueue.h"
#include "packsim/kernel/coro/SimScheduleOn.h"
#include "packsim/kernel/coro/SimSignal.h"
#include "packsim/kernel/coro/SimTask.h"

using packsim::Scheduler;
using packsim::SimContext;
using packsim::Time;

using packsim::coro::SimBarrier;
using packsim::coro::SimCancellableDelay;
using packsim::coro::SimFuture;
using packsim::coro::SimQueue;
using packsim::coro::SimSignal;
using packsim::coro::SimTask;

using packsim::coro::simDelay;
using packsim::coro::simScheduleOn;
using packsim::coro::simYield;

namespace {

// Line rate = 800 Gbps = 100 bytes/ns (Example 9). Giga and nano are
// reciprocals (10^9 vs 10^-9), so Gbps/8 yields bytes/ns directly.
constexpr uint64_t kLineRateGbps = 800;
// Integer division — must be exact, otherwise the demo's expected
// timeline silently truncates. Pin the multiple-of-8 invariant so a
// future edit to kLineRateGbps that breaks the math fails to build.
static_assert(
    kLineRateGbps % 8 == 0,
    "kLineRateGbps must be a multiple of 8 to keep kBytesPerNsAtLineRate exact");
constexpr uint64_t kBytesPerNsAtLineRate = kLineRateGbps / 8;

// (Example 9 uses SimQueue<std::optional<uint32_t>>: a populated optional
// carries the packet size in bytes; std::nullopt is the shutdown signal.
// This avoids signed/unsigned mixing and removes a magic-number sentinel.)

// 1 microsecond expressed in femtoseconds; the PDES lookahead of Examples 12
// and 14.
constexpr uint64_t kOneMicrosecondFs = 1'000'000'000;

// Number of yields in Example 2's same-timestamp batch — chosen large
// enough to make the "no time advance" point visually obvious.
constexpr int kExample2YieldIterations = 1000;

// Sliding-window depth for Example 11 — must match the number of ACKs
// the AckPath coroutine generates so the demo terminates cleanly.
constexpr int kExample11NumPackets = 5;

// Example 14 barrier participants and iteration count. 4 participants
// across 4 contexts is small enough to keep the demo's annotated stdout
// readable; 2 iterations exercises the dispatcher's arrived_-reset
// path between iterations.
constexpr uint32_t kExample14NumParticipants = 4;
constexpr uint32_t kExample14NumIterations = 2;
// Per-participant work duration: participant N takes 5*(N+1) µs of local
// work before arriving. Staggered to make "slowest participant gates the
// barrier" visible.
constexpr uint64_t kExample14PerParticipantWorkUs = 5;

// ============================================================
// Helper: run a section with header/footer
// ============================================================

struct Section {
  // PDES scheduler driving this section's events.
  Scheduler scheduler;
  // Simulation context bound to `scheduler` and used by the example coroutines.
  SimContext ctx;

  /**
   * @param name         Human-readable section name. Printed as the banner
   *                     and used as the SimContext's name.
   * @param lookaheadFs  Engine lookahead (femtoseconds). Mutable until
   *                     `scheduler.run()` starts; engine-wide (read live
   *                     by scheduleRemote, not captured at registration).
   *                     Default 0 = use scheduler's default.
   */
  explicit Section(const std::string& name, const uint64_t lookaheadFs = 0)
      : scheduler{}, ctx{&scheduler, name} {
    if (lookaheadFs != 0) {
      scheduler.setLookaheadTimeFs(lookaheadFs);
    }
    fmt::print("\n{:=^70}\n", fmt::format(" {} ", name));
  }

  /**
   * Drive the simulation to completion, then print the final sim time
   * footer that closes each section's output block.
   */
  void run() {
    scheduler.run();
    fmt::print("  Final sim time: {}ns\n", ctx.nowNs());
  }
};

// ============================================================
// Example 1: SimTask + simDelay
// Pattern: Multi-step pipeline
//
// BEFORE (callbacks):
//   void step1() { doA(); ctx->schedule(50ns, &step2); }
//   void step2() { doB(); ctx->schedule(100ns, &step3); }
//   void step3() { doC(); ctx->schedule(200ns, &done); }
// ============================================================

SimTask example1Pipeline(folly::not_null<SimContext*> ctx) {
  fmt::print(
      "  t={:>5}ns: [pipeline] Start - processing 3 stages\n", ctx->nowNs());

  co_await simDelay(ctx, Time::fromNs(50));
  fmt::print(
      "  t={:>5}ns: [pipeline] Stage 1 complete (decode)\n", ctx->nowNs());

  co_await simDelay(ctx, Time::fromNs(100));
  fmt::print(
      "  t={:>5}ns: [pipeline] Stage 2 complete (execute)\n", ctx->nowNs());

  co_await simDelay(ctx, Time::fromNs(200));
  fmt::print(
      "  t={:>5}ns: [pipeline] Stage 3 complete (writeback)\n", ctx->nowNs());
}

// ============================================================
// Example 2: simYield
// Pattern: Processing a batch without advancing simulation time
//
// BEFORE (callbacks):
//   void processBatch(int remaining) {
//     if (remaining == 0) return;
//     processOne();
//     ctx->scheduleNow([this, remaining] { processBatch(remaining - 1); });
//   }
// ============================================================

SimTask example2Yield(folly::not_null<SimContext*> ctx) {
  int processed = 0;
  fmt::print(
      "  t={:>5}ns: [yield] Processing {} items at same timestamp\n",
      ctx->nowNs(),
      kExample2YieldIterations);

  for (int i = 0; i < kExample2YieldIterations; ++i) {
    processed++;
    co_await simYield(ctx);
  }

  fmt::print(
      "  t={:>5}ns: [yield] Done. Processed {} items, time unchanged\n",
      ctx->nowNs(),
      processed);
}

// ============================================================
// Example 3: Concurrent coroutines interleaving
//
// BEFORE (callbacks): two independent self-rescheduling chains run
// concurrently — each function holds its own state and schedules its
// continuation onto the same context, interleaving naturally with the
// other.
// ============================================================

SimTask example3Switch(folly::not_null<SimContext*> ctx) {
  for (int hop = 0; hop < 3; ++hop) {
    fmt::print(
        "  t={:>5}ns: [switch] Hop {} - pipeline processing\n",
        ctx->nowNs(),
        hop);
    co_await simDelay(ctx, Time::fromNs(30));
  }
  fmt::print(
      "  t={:>5}ns: [switch] Packet routed through 3 hops\n", ctx->nowNs());
}

SimTask example3Nic(folly::not_null<SimContext*> ctx) {
  for (int pkt = 0; pkt < 4; ++pkt) {
    fmt::print("  t={:>5}ns: [nic]    TX packet {}\n", ctx->nowNs(), pkt);
    co_await simDelay(ctx, Time::fromNs(20));
  }
  fmt::print("  t={:>5}ns: [nic]    All packets transmitted\n", ctx->nowNs());
}

// ============================================================
// Example 4: SimSignal - reactive loop
// Pattern: Bridge push callbacks to coroutine waits.
//
// BEFORE (callbacks):
//   onAck(Msg* m) { handleAck(m); checkCompletionAndContinue(); }
//   sendNext() { transport->send(next, &onAck); }
// ============================================================

SimTask example4Transport(
    folly::not_null<SimContext*> ctx,
    SimSignal& ackSignal) {
  for (int i = 0; i < 3; ++i) {
    co_await simDelay(ctx, Time::fromNs(100));
    fmt::print(
        "  t={:>5}ns: [transport] ACK {} arrived -> signal()\n",
        ctx->nowNs(),
        i);
    ackSignal.signal();
  }
}

SimTask example4Workload(
    folly::not_null<SimContext*> ctx,
    SimSignal& ackSignal) {
  for (int i = 0; i < 3; ++i) {
    fmt::print(
        "  t={:>5}ns: [workload] Sent message {}, waiting for ACK...\n",
        ctx->nowNs(),
        i);
    co_await ackSignal.wait();
    fmt::print("  t={:>5}ns: [workload] Message {} ACKed!\n", ctx->nowNs(), i);
  }
}

// ============================================================
// Example 5: SimSignal - signal accumulation
//
// BEFORE (callbacks):
//   onPacket() { ++received; if (received == expected) wakeReceiver(); }
//   sender ran ahead, wakeReceiver coalesced all "extra" notifications.
// ============================================================

SimTask example5BurstSender(
    folly::not_null<SimContext*> ctx,
    SimSignal& recvSignal) {
  for (int i = 0; i < 5; ++i) {
    co_await simDelay(ctx, Time::fromNs(10));
    recvSignal.signal();
  }
  fmt::print("  t={:>5}ns: [sender] Burst of 5 packets sent\n", ctx->nowNs());
}

SimTask example5SlowReceiver(
    folly::not_null<SimContext*> ctx,
    SimSignal& recvSignal) {
  co_await simDelay(ctx, Time::fromNs(200));
  fmt::print(
      "  t={:>5}ns: [receiver] Woke up, {} signals pending\n",
      ctx->nowNs(),
      recvSignal.pendingSignals());

  for (int i = 0; i < 5; ++i) {
    co_await recvSignal.wait();
    fmt::print(
        "  t={:>5}ns: [receiver] Consumed signal {} (no suspend!)\n",
        ctx->nowNs(),
        i);
  }
}

// ============================================================
// Example 6: SimSignal - cancellable timer (generation counter)
//
// NOTE: this is the GENERIC cancellation pattern. The shared `currentGen`
// variable is mutated and read by coroutines on the SAME SimContext — PDES
// guarantees single-thread execution per context, so no lock is needed.
// In a cross-context scenario, the coroutine would need to migrate via
// simScheduleOn first. For PDES `Event.cancel()`-equivalent cancellation,
// see Example 13 (which uses SimCancellableDelay's stored event handle
// approach — the right tool for a periodic timer such as DCQCN's).
// ============================================================

SimTask example6PfcRefresh(
    folly::not_null<SimContext*> ctx,
    const uint32_t myGen,
    uint32_t& currentGen) {
  int frame = 0;
  while (currentGen == myGen) {
    fmt::print(
        "  t={:>5}ns: [pfc] Sending refresh frame {} (gen={})\n",
        ctx->nowNs(),
        frame++,
        myGen);
    co_await simDelay(ctx, Time::fromNs(50));
  }
  fmt::print(
      "  t={:>5}ns: [pfc] Gen {} cancelled, exiting\n", ctx->nowNs(), myGen);
}

SimTask example6Controller(folly::not_null<SimContext*> ctx, uint32_t& pfcGen) {
  pfcGen = 1;
  fmt::print(
      "  t={:>5}ns: [ctrl] Starting PFC refresh (gen=1)\n", ctx->nowNs());
  example6PfcRefresh(ctx, 1, pfcGen);

  co_await simDelay(ctx, Time::fromNs(120));
  pfcGen = 2;
  fmt::print(
      "  t={:>5}ns: [ctrl] PFC state changed -> gen bumped to 2\n",
      ctx->nowNs());

  // The demo ends after the single generation bump; a production caller
  // would launch a new example6PfcRefresh(ctx, 2, pfcGen) here to start
  // the gen=2 refresh loop.
  co_await simDelay(ctx, Time::fromNs(100));
  fmt::print("  t={:>5}ns: [ctrl] Done\n", ctx->nowNs());
}

// ============================================================
// Example 7: SimFuture<void> - send and wait for ACK
//
// BEFORE (callbacks):
//   transport->send(msg, [this] { onAck(); });
//   void onAck() { ++ackedCount; if (more()) sendNext(); }
// ============================================================

SimTask example7AckHandler(
    folly::not_null<SimContext*> ctx,
    SimFuture<void>& ackFuture) {
  co_await simDelay(ctx, Time::fromNs(150));
  fmt::print(
      "  t={:>5}ns: [network] ACK received -> resolve()\n", ctx->nowNs());
  ackFuture.resolve();
}

SimTask example7Sender(folly::not_null<SimContext*> ctx) {
  for (int msg = 0; msg < 2; ++msg) {
    SimFuture<void> ackFuture(ctx);
    fmt::print(
        "  t={:>5}ns: [sender] Sending message {}, awaiting ACK...\n",
        ctx->nowNs(),
        msg);
    example7AckHandler(ctx, ackFuture);
    co_await ackFuture;
    fmt::print(
        "  t={:>5}ns: [sender] Message {} complete\n", ctx->nowNs(), msg);
  }
}

// ============================================================
// Example 8: SimFuture<T> - future carrying a value
//
// BEFORE (callbacks):
//   remote.compute(input, [this](int result) { onResult(result); });
//   void onResult(int r) { handle(r); }
// ============================================================

SimTask example8RemoteCompute(
    folly::not_null<SimContext*> ctx,
    SimFuture<int>& result,
    const int input) {
  co_await simDelay(ctx, Time::fromNs(80));
  const int computed = input * input + 1;
  fmt::print(
      "  t={:>5}ns: [compute] Result = {} -> resolve()\n",
      ctx->nowNs(),
      computed);
  result.resolve(computed);
}

SimTask example8Requester(folly::not_null<SimContext*> ctx) {
  SimFuture<int> result(ctx);
  fmt::print(
      "  t={:>5}ns: [requester] Requesting compute(7)...\n", ctx->nowNs());
  example8RemoteCompute(ctx, result, 7);

  const int val = co_await result;
  fmt::print("  t={:>5}ns: [requester] Got result: {}\n", ctx->nowNs(), val);
}

// ============================================================
// Example 9: SimQueue<T> - TX pump pattern
//
// BEFORE (callbacks):
//   void sendPacket() {
//     auto pkt = dequeue();
//     transmit(pkt, txTime, [this]() { sendPacket(); }); // recursive
//   }
// ============================================================

SimTask example9TxPump(
    folly::not_null<SimContext*> ctx,
    SimQueue<std::optional<uint32_t>>& queue) {
  while (true) {
    const auto work = co_await queue.pop();
    if (!work) {
      fmt::print(
          "  t={:>5}ns: [pump] Shutdown signal -> stopping\n", ctx->nowNs());
      break;
    }
    const uint64_t txTimeNs = *work / kBytesPerNsAtLineRate;
    fmt::print(
        "  t={:>5}ns: [pump] TX {}B ({}ns serialization)\n",
        ctx->nowNs(),
        *work,
        txTimeNs);
    co_await simDelay(ctx, Time::fromNs(txTimeNs));
    fmt::print("  t={:>5}ns: [pump] TX complete\n", ctx->nowNs());
  }
}

SimTask example9Producer(
    folly::not_null<SimContext*> ctx,
    SimQueue<std::optional<uint32_t>>& queue) {
  const std::vector<uint32_t> sizes = {1000, 4096, 2000, 500};
  for (const uint32_t sizeBytes : sizes) {
    fmt::print(
        "  t={:>5}ns: [producer] Enqueue {}B packet\n",
        ctx->nowNs(),
        sizeBytes);
    queue.push(sizeBytes);
  }
  co_await simDelay(ctx, Time::fromNs(500));
  queue.push(std::nullopt); // shutdown signal
}

// ============================================================
// Example 10: SimQueue with move-only types
//
// BEFORE (callbacks): the producer would have invoked a callback per
// message with the move-only payload by-value, requiring per-callsite
// std::move dance. The SimQueue<unique_ptr<T>> shape moves the dance
// inside the queue's API.
// ============================================================

struct Example10Message {
  // Sequence number assigned by the producer; used in the consumer's log line.
  size_t id{};
  // Move-only payload demonstrated by std::unique_ptr<Example10Message> in
  // the example flow — owns its storage end-to-end without copies.
  std::string payload;
};

SimTask example10Consumer(
    folly::not_null<SimContext*> ctx,
    SimQueue<std::unique_ptr<Example10Message>>& queue) {
  for (int i = 0; i < 3; ++i) {
    auto msg = co_await queue.pop();
    fmt::print(
        "  t={:>5}ns: [consumer] Got msg {}: \"{}\"\n",
        ctx->nowNs(),
        msg->id,
        msg->payload);
  }
}

SimTask example10Producer(
    folly::not_null<SimContext*> ctx,
    SimQueue<std::unique_ptr<Example10Message>>& queue) {
  const std::vector<std::string> payloads = {"hello", "world", "done"};
  for (size_t i = 0; i < payloads.size(); ++i) {
    co_await simDelay(ctx, Time::fromNs(25));
    // C++20 parenthesized aggregate initialization (P0960R3): heap-
    // allocates the message in place without the temporary-then-move
    // dance of `make_unique<T>(T{...})`.
    auto msg = std::make_unique<Example10Message>(i, payloads[i]);
    fmt::print(
        "  t={:>5}ns: [producer] Pushing msg {}: \"{}\"\n",
        ctx->nowNs(),
        i,
        payloads[i]);
    queue.push(std::move(msg));
  }
}

// ============================================================
// Example 11: Sliding window with SimSignal
// Pattern: Congestion-controlled sender (cwnd).
//
// BEFORE (callbacks):
//   sendIfRoom() { if (inflight < cwnd) { send(); ++inflight; } }
//   onAck() { --inflight; sendIfRoom(); } // recursive resume
// ============================================================

SimTask example11Sender(
    folly::not_null<SimContext*> ctx,
    SimSignal& windowOpen,
    int& inflight,
    const int cwndPackets) {
  for (int pkt = 0; pkt < kExample11NumPackets; ++pkt) {
    while (inflight >= cwndPackets) {
      fmt::print(
          "  t={:>5}ns: [sender] Window full ({}/{}), waiting...\n",
          ctx->nowNs(),
          inflight,
          cwndPackets);
      co_await windowOpen.wait();
    }
    ++inflight;
    fmt::print(
        "  t={:>5}ns: [sender] Sent pkt {} (inflight={}/{})\n",
        ctx->nowNs(),
        pkt,
        inflight,
        cwndPackets);
    co_await simDelay(ctx, Time::fromNs(10));
  }
  fmt::print("  t={:>5}ns: [sender] All packets sent\n", ctx->nowNs());
}

SimTask example11AckPath(
    folly::not_null<SimContext*> ctx,
    SimSignal& windowOpen,
    int& inflight) {
  for (int i = 0; i < kExample11NumPackets; ++i) {
    co_await simDelay(ctx, Time::fromNs(50));
    --inflight;
    fmt::print(
        "  t={:>5}ns: [ack] ACK {} -> inflight={} -> signal()\n",
        ctx->nowNs(),
        i,
        inflight);
    windowOpen.signal();
  }
}

// ============================================================
// Example 12: Cross-context scheduling via simScheduleOn
//
// BEFORE (callbacks): callers used `targetCtx->scheduleRemote([] { ... })`
// directly with a callback closure. simScheduleOn wraps it as a co_await,
// so control flow stays linear across the context-migration boundary.
// ============================================================

SimTask example12CrossContext(
    folly::not_null<SimContext*> nicCtx,
    folly::not_null<SimContext*> swCtx) {
  fmt::print("  t={:>5}ns: [nic_ctx] Packet arrived at NIC\n", nicCtx->nowNs());

  co_await simDelay(nicCtx, Time::fromNs(30));
  fmt::print(
      "  t={:>5}ns: [nic_ctx] NIC processing done, migrating...\n",
      nicCtx->nowNs());

  co_await simScheduleOn(swCtx);
  fmt::print("  t={:>5}ns: [sw_ctx] Now on switch context!\n", swCtx->nowNs());

  co_await simDelay(swCtx, Time::fromNs(20));
  fmt::print(
      "  t={:>5}ns: [sw_ctx] Switch pipeline complete\n", swCtx->nowNs());
}

// ============================================================
// Example 13: Periodic timer with PDES-equivalent cancellation
// Pattern: self-rescheduling timer with synchronous cancel() semantics.
//
// This is how the DCQCN alpha and rate-increase timers in
// transport/congestion-control.cpp would be expressed as coroutines. The
// SimCancellableDelay stores both the scheduled event handle and the suspended
// coroutine handle, so cancel()
// matches the existing `Scheduler::EventHandle.cancel()` semantic exactly:
// the resume callback never fires, AND the coroutine frame is freed.
//
// BEFORE (callbacks; example uses a periodic-ACK shape for clarity):
//   void onPeriodicAckTimer() {
//     if (!enabled_) return;
//     sendAck();
//     periodicAckTimerEvent_ = ctx->schedule(interval, &onPeriodicAckTimer,
//     this);
//   }
//   void cancelPeriodicAckTimer() { periodicAckTimerEvent_.cancel(); }
//
// AFTER (coroutine):
//   See example13PeriodicAckLoop below.
// ============================================================

SimTask example13PeriodicAckLoop(
    folly::not_null<SimContext*> ctx,
    SimCancellableDelay& handle) {
  int tick = 0;
  while (true) {
    co_await handle.awaitDelay(ctx, Time::fromNs(50));
    fmt::print(
        "  t={:>5}ns: [periodic-ack] Tick {} - sendAck()\n",
        ctx->nowNs(),
        tick++);
  }
}

SimTask example13Controller(
    folly::not_null<SimContext*> ctx,
    SimCancellableDelay& handle) {
  fmt::print(
      "  t={:>5}ns: [ctrl] Starting periodic ACK loop (interval=50ns)\n",
      ctx->nowNs());

  // Run for 175ns (3 ticks should fire at 50, 100, 150).
  co_await simDelay(ctx, Time::fromNs(175));

  // Synchronous cancellation: the pending resume at t=200 is removed from
  // the PDES queue (semantically equivalent to Scheduler::EventHandle::cancel;
  // implementation in SimCancellableDelay::cancel) AND the coroutine frame
  // is destroyed. Verified indirectly via the Final sim time printed below
  // — the loop halts at the cancel rather than running to its next tick.
  fmt::print(
      "  t={:>5}ns: [ctrl] Cancelling periodic-ACK timer (PDES-equivalent)\n",
      ctx->nowNs());
  handle.cancel();

  fmt::print(
      R"(  t={:>5}ns: [ctrl] Cancelled — see Final sim time below; the loop halted at the cancel call rather than running to its next tick.
)",
      ctx->nowNs());
}

// ============================================================
// Example 14: SimBarrier (lock-free N-way collective barrier)
// Pattern: iteration boundary across N participants on N contexts.
//
// BEFORE (callbacks):
//   void participantWork() {
//     doLocalWork();
//     barrier.arrive([this] { onBarrierRelease(); });
//   }
//   void onBarrierRelease() { startNextIteration(); }
// SimBarrier wraps this as `co_await barrier->arrive(myCtx)`; control
// flow stays linear across the cross-context release dispatch
// (1 × lookahead — last-to-arrive participant directly schedules
// resumes back to all peers, no coord round-trip). The same SimBarrier
// instance handles every iteration — the dispatcher resets the arrival
// counter after each release.
// ============================================================

SimTask example14Participant(
    folly::not_null<SimContext*> myCtx,
    std::shared_ptr<SimBarrier> barrier,
    const uint32_t participantId) {
  for (uint32_t iter = 0; iter < kExample14NumIterations; ++iter) {
    co_await simDelay(
        myCtx,
        Time::fromUs(kExample14PerParticipantWorkUs * (participantId + 1)));
    fmt::print(
        "  t={:>5}ns: [p{}] iter {} work done -> arrive at barrier\n",
        myCtx->nowNs(),
        participantId,
        iter);
    co_await barrier->arrive(myCtx);
    fmt::print(
        "  t={:>5}ns: [p{}] iter {} barrier released - synchronized\n",
        myCtx->nowNs(),
        participantId,
        iter);
  }
}

// ============================================================
// Per-example runners. Each wires the example's coroutines into a fresh
// Section and runs the scheduler. Kept as separate functions (rather than
// inline blocks in main()) to keep each function short and to make
// individual examples greppable by name (`runExample5`, etc.).
// ============================================================

void runExample1() {
  Section section("Example 1: SimTask + simDelay (multi-step pipeline)");
  section.ctx.scheduleNow([&]() { example1Pipeline(&section.ctx); });
  section.run();
}

void runExample2() {
  Section section("Example 2: simYield (batch without advancing time)");
  section.ctx.scheduleNow([&]() { example2Yield(&section.ctx); });
  section.run();
}

void runExample3() {
  Section section("Example 3: Concurrent coroutines interleaving");
  section.ctx.scheduleNow([&]() { example3Switch(&section.ctx); });
  section.ctx.scheduleNow([&]() { example3Nic(&section.ctx); });
  section.run();
}

void runExample4() {
  Section section("Example 4: SimSignal (reactive loop - send/wait for ACK)");
  SimSignal ackSignal(&section.ctx);
  section.ctx.scheduleNow([&]() {
    example4Workload(&section.ctx, ackSignal);
    example4Transport(&section.ctx, ackSignal);
  });
  section.run();
}

void runExample5() {
  Section section("Example 5: SimSignal (signal accumulation)");
  SimSignal recvSignal(&section.ctx);
  section.ctx.scheduleNow([&]() {
    example5BurstSender(&section.ctx, recvSignal);
    example5SlowReceiver(&section.ctx, recvSignal);
  });
  section.run();
}

void runExample6() {
  Section section(
      "Example 6: SimSignal (cancellable timer - generation counter)");
  uint32_t pfcGen = 0;
  section.ctx.scheduleNow([&]() { example6Controller(&section.ctx, pfcGen); });
  section.run();
}

void runExample7() {
  Section section("Example 7: SimFuture<void> (send and wait for ACK)");
  section.ctx.scheduleNow([&]() { example7Sender(&section.ctx); });
  section.run();
}

void runExample8() {
  Section section("Example 8: SimFuture<int> (future carrying a value)");
  section.ctx.scheduleNow([&]() { example8Requester(&section.ctx); });
  section.run();
}

void runExample9() {
  Section section("Example 9: SimQueue<optional<uint32_t>> (TX pump pattern)");
  SimQueue<std::optional<uint32_t>> queue(&section.ctx);
  section.ctx.scheduleNow([&]() {
    example9TxPump(&section.ctx, queue);
    example9Producer(&section.ctx, queue);
  });
  section.run();
}

void runExample10() {
  Section section("Example 10: SimQueue<unique_ptr> (move-only types)");
  SimQueue<std::unique_ptr<Example10Message>> queue(&section.ctx);
  section.ctx.scheduleNow([&]() {
    example10Consumer(&section.ctx, queue);
    example10Producer(&section.ctx, queue);
  });
  section.run();
}

void runExample11() {
  Section section("Example 11: Sliding window (cwnd with SimSignal)");
  SimSignal windowOpen(&section.ctx);
  // `inflight` and `kCwndPackets` live on this function's stack; the
  // example11Sender / example11AckPath coroutines capture `inflight` by
  // reference into their frames. They MUST outlive the coroutine
  // frames. Blocking on section.run() below guarantees this in this
  // demo context — run() runs to completion (all coroutines naturally
  // finish or cancel) before returning. For long-running coroutines
  // that outlive their launch site, move such state into a struct that
  // outlives scheduler.run(), or capture by value / move ownership
  // into the coroutine.
  int inflight = 0;
  constexpr int kCwndPackets = 2;
  section.ctx.scheduleNow([&]() {
    example11Sender(&section.ctx, windowOpen, inflight, kCwndPackets);
    example11AckPath(&section.ctx, windowOpen, inflight);
  });
  section.run();
}

void runExample12() {
  Section section(
      "Example 12: simScheduleOn (cross-context migration)",
      /*lookaheadFs=*/kOneMicrosecondFs);
  SimContext swCtx(&section.scheduler, "switch");
  section.ctx.scheduleNow(
      [&]() { example12CrossContext(&section.ctx, &swCtx); });
  section.run();
}

void runExample13() {
  Section section("Example 13: Periodic timer w/ PDES-equiv cancellation");
  SimCancellableDelay handle;
  section.ctx.scheduleNow([&]() {
    example13PeriodicAckLoop(&section.ctx, handle);
    example13Controller(&section.ctx, handle);
  });
  section.run();
}

void runExample14() {
  Section section(
      "Example 14: SimBarrier (lock-free N-way collective barrier)",
      /*lookaheadFs=*/kOneMicrosecondFs);

  // N participant contexts. In a real workload, these would be
  // per-endpoint or per-host. Allocated on the heap so pointers remain
  // stable across the capture-by-pointer in the launcher lambda.
  std::vector<std::unique_ptr<SimContext>> participantCtxs;
  participantCtxs.reserve(kExample14NumParticipants);
  for (uint32_t i = 0; i < kExample14NumParticipants; ++i) {
    participantCtxs.emplace_back(
        std::make_unique<SimContext>(
            &section.scheduler, fmt::format("p{}", i)));
  }

  // Single barrier instance shared by all participants. The dispatcher
  // resets the arrival counter after each release, so the same instance
  // handles every iteration without reconstruction.
  //
  // Lifetime: `barrier` is a shared_ptr held by the local variable here
  // and copied into each participant coroutine frame (pass-by-value to
  // example14Participant). After section.run() returns, all participants
  // have finished and dropped their frame-held references; the local
  // `barrier` drops at function exit; refcount → 0 and the barrier
  // destructs automatically.
  auto barrier = std::make_shared<SimBarrier>(kExample14NumParticipants);

  for (uint32_t i = 0; i < kExample14NumParticipants; ++i) {
    // auto* const (not const auto*): the pointer itself doesn't get
    // reassigned, but scheduleNow is a non-const method so the pointee
    // must remain mutable.
    auto* const participantCtx = participantCtxs.at(i).get();
    // Capture barrier by reference — section.run() completes before the
    // enclosing runExample14() returns, so the outer shared_ptr outlives
    // every scheduled lambda. Pass-by-value to example14Participant then
    // does exactly ONE copy (into the coroutine frame), vs the 2 copies
    // (capture + function arg) a by-value capture would make.
    // REQUIRES: section.run() must complete before runExample14()
    // returns. A future refactor that splits setup from run (e.g.,
    // returns the section) would dangle this `&barrier` reference —
    // switch to capture-by-value if that ever changes.
    participantCtx->scheduleNow([participantCtx, &barrier, i]() {
      example14Participant(participantCtx, barrier, i);
    });
  }
  section.run();
}

} // namespace

// ============================================================
// Main
// ============================================================

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv);

  // Force single-threaded for deterministic output regardless of any
  // --pdes_threads CLI override the user may have passed (the demo's
  // annotated stdout depends on a deterministic dispatch order).
  XCHECK(!gflags::SetCommandLineOption("pdes_threads", "1").empty())
      << "Failed to override --pdes_threads — flag not registered?";

  fmt::print("PackSim Coroutine Primitives - Usage Examples\n");

  runExample1();
  runExample2();
  runExample3();
  runExample4();
  runExample5();
  runExample6();
  runExample7();
  runExample8();
  runExample9();
  runExample10();
  runExample11();
  runExample12();
  runExample13();
  runExample14();

  fmt::print("\n{:=^70}\n", " All examples complete ");
  return 0;
}
