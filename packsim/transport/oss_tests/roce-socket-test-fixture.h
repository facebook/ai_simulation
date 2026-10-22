/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/kernel/device-id.h"
#include "packsim/packet/roce.h"
#include "packsim/transport/error-recovery.h"
#include "packsim/transport/roce-base.h"

namespace packsim {

// Transport whose sendNext() only counts: these sockets are built directly
// rather than through the transport, so the real round-robin scheduler has no
// socket registered and would throw.
class TestRoceTransport final : public RoceTransportBase {
 public:
  using RoceTransportBase::RoceTransportBase;

  void sendNext() override {
    ++sendNextCalls_;
  }

  int sendNextCalls() const noexcept {
    return sendNextCalls_;
  }

 private:
  int sendNextCalls_{0};
};

/**
 * Scaffolding for driving a RoceSocketBase against a stubbed ErrorRecovery: a
 * simulation context, a NIC config, an inert transport that captures every
 * emitted packet, and one copy of the RoceSocketBase constructor call.
 * Subclasses supply the ErrorRecoveryFactory their scenario needs.
 *
 * `roce-psn-action-test` and `roce-socket-stats-test` include this.
 * `error-recovery-stub-test` and `congestion-control-stub-test` still declare
 * their own `TestRoceTransport` and `makeSocket()`; migrating them is
 * follow-up work, and until then those definitions must not diverge from this
 * one.
 */
class RoceSocketTestBase : public ::testing::Test {
 public:
  void SetUp() override {
    nicCfg_.writeEff() = 100;
    nicCfg_.readGbps() = 400;
    nicCfg_.readLatencyNs() = 0;
    nicCfg_.perCosBufferSize() = {1024 * 1024, 1024 * 1024};
    nicCfg_.perCosXoff() = {1024 * 1024, 1024 * 1024};
    nicCfg_.perCosXon() = {1024 * 1024, 1024 * 1024};
    nicCfg_.speed() = "400Gbps";

    transport_ =
        std::make_unique<TestRoceTransport>(&simCtx_, srcId_, &nicCfg_);
    transport_->setSendPacketFn([this](const PacketPtr& pkt) {
      sentPackets_.emplace_back(pkt);
      return true;
    });
    transport_->setOnMessageAckedCallback([](Message*) {});
    transport_->setOnMessageReceivedCallback([](Message*) {});
  }

  std::unique_ptr<RoceSocketBase> makeSocket(
      const RoceSocketBase::ErrorRecoveryFactory& factory) {
    auto socket = std::make_unique<RoceSocketBase>(
        transport_.get(),
        srcId_,
        dstId_,
        /*srcPort=*/1234,
        /*localQPN=*/0,
        /*remoteQPN=*/0,
        /*smtu=*/4200,
        /*workload=*/nullptr,
        factory,
        /*congestionControlFactory=*/nullptr);
    // transport->connect normally wires these up, but we bypass it here.
    socket->setOnMessageAckedCallback([this](Message* msg) {
      ackedSequenceNums_.emplace_back(msg->sequenceNumber);
    });
    socket->setOnMessageReceivedCallback([](Message*) {});
    return socket;
  }

 protected:
  Scheduler scheduler_;
  Tracer::ConditionalMap conditionalMap_;
  SimContext simCtx_{&scheduler_, "SimContext", 0, &conditionalMap_};
  FlexDeviceId srcId_{{1, 0, 0, 1}};
  FlexDeviceId dstId_{{1, 0, 0, 2}};
  packsim::thrift::NicConfig nicCfg_;
  std::unique_ptr<TestRoceTransport> transport_;
  std::vector<PacketPtr> sentPackets_;
  std::vector<uint32_t> ackedSequenceNums_;
};

} // namespace packsim
