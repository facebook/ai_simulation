/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <optional>

#include <gtest/gtest.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/kernel/time.h"
#include "packsim/kernel/trace.h"
#include "packsim/packet/pfc.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/transport/oss_tests/roce-transport-sim-fixture.h"
#include "packsim/transport/roce-base.h"
#include "packsim/transport/transport.h"

namespace packsim {

namespace {
// Lookahead of the loopback simulations, 1 us, as in RoceTransportSimTest.
constexpr uint64_t kLookaheadFs = 1'000'000'000;
// One-way wire delay of the loopback.
constexpr uint64_t kWireDelayNs = 100;
// A message of 65 full packets, long enough that a slowly drained RX buffer
// still holds most of it well after its first packets have arrived.
constexpr uint32_t kMessageBytes = 4096 * 65;
// Local QP number of the receiving socket.
constexpr uint32_t kReceiverQpn = 0;
// NIC write efficiency, in percent, at which the receiver drains its RX buffer
// at a tenth of the 400 Gbps wire.
constexpr double kTenthOfLineRateWriteEffPercent = 10;

// How sendOneMessage sends its message.
struct RunOptions {
  // Send the message as an RDMA WRITE, which the receiver posts nothing for,
  // rather than as a SEND.
  bool isWrite{false};
  // When to unregister the receiving socket, as a workload rank does when it
  // completes; never if unset.
  std::optional<uint64_t> unregisterReceiverAtNs;
};

// What happened to one message at each end of the loopback.
struct MessageRun {
  // When the sender saw the message acked, in ns; 0 if it never was.
  uint64_t ackedNs{};
};
} // namespace

class TransportMemoryTest : public RoceTransportSimTest {
 protected:
  /**
   * Send one kMessageBytes message between two RoCEv2 transports built from
   * `nicCfg`, over a loopback wire, in a simulation of its own.
   *
   * @param nicCfg   The NIC config of both ends.
   * @param options  How to send the message.
   *
   * @return  What happened to the message.
   */
  MessageRun sendOneMessage(
      const packsim::thrift::NicConfig& nicCfg,
      const RunOptions& options = {}) {
    Tracer::ConditionalMap conditionalMap;
    Scheduler scheduler;
    scheduler.setLookaheadTimeFs(kLookaheadFs);
    SimContext simCtx{&scheduler, "SimContext", 0, &conditionalMap};
    RoceTransportBase src(&simCtx, srcId_, &nicCfg);
    RoceTransportBase dst(&simCtx, dstId_, &nicCfg);
    const auto wire = [&simCtx](RoceTransportBase& from, Transport& to) {
      return [&simCtx, &from, &to](const PacketPtr& pkt) {
        from.onPacketSent(pkt, nullptr);
        simCtx.schedule(
            Time::fromNs(kWireDelayNs),
            &Transport::onPacketRcvd,
            &to,
            pkt,
            nullptr);
        return true;
      };
    };
    src.setSendPacketFn(wire(src, dst));
    dst.setSendPacketFn(wire(dst, src));
    MessageRun run;
    src.setOnPfcStateChangedCallback([](uint8_t, PfcState) {});
    dst.setOnPfcStateChangedCallback([](uint8_t, PfcState) {});
    src.setOnMessageAckedCallback(
        [&run, &simCtx](Message*) { run.ackedNs = simCtx.nowNs(); });
    dst.setOnMessageReceivedCallback([](Message*) {});

    Socket* const srcSocket = src.connect(dstId_, 1234, 0, 0);
    Socket* const dstSocket = dst.connect(srcId_, 2345, kReceiverQpn, 0);
    if (srcSocket == nullptr || dstSocket == nullptr) {
      ADD_FAILURE() << "connect() returned no socket";
      return run;
    }
    Socket& srcSock = *srcSocket;
    Socket& dstSock = *dstSocket;
    Message msg{
        .sizeBytes = kMessageBytes,
        .srcIdx = srcId_,
        .dstIdx = dstId_,
        .connectionMessageId = 0,
        .isWrite = options.isWrite};
    simCtx.scheduleNow([&srcSock, &dstSock, &msg]() {
      srcSock.send(&msg);
      if (!msg.isWrite) {
        dstSock.recv(&msg);
      }
    });
    if (options.unregisterReceiverAtNs.has_value()) {
      simCtx.schedule(
          Time::fromNs(*options.unregisterReceiverAtNs),
          [this, &dst]() { dst.unregisterSocket(srcId_, kReceiverQpn); });
    }
    scheduler.run();
    return run;
  }
};

TEST_F(
    TransportMemoryTest,
    UnregisteringTheTargetWhileItsWritesWaitIsHarmless) {
  // The message arrives in about 6 us, and its last packets leave the slowly
  // drained RX buffer some 50 us later.
  nicCfg_.writeEff() = kTenthOfLineRateWriteEffPercent;
  // Unregister the receiver, as the rank of a WRITE's target can complete,
  // while most of the message still waits in its RX buffer.
  constexpr uint64_t kUnregisterAtNs = 10'000;
  MessageRun run;
  EXPECT_NO_THROW(
      run = sendOneMessage(
          nicCfg_,
          {.isWrite = true, .unregisterReceiverAtNs = kUnregisterAtNs}));
  EXPECT_NE(run.ackedNs, 0u) << "the sender's WRITE never completed";
}

} // namespace packsim
