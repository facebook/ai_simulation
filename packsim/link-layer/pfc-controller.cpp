/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "packsim/link-layer/pfc-controller.h"

// @oss-disable
#include "packsim/link-layer/eth-port.h"
#include "packsim/queues/queues.h"

namespace packsim {

namespace {
// A PFC pause "quantum" is 512 bit-times on the wire (IEEE 802.1Qbb) —
// multiply a quanta count by this to get total bit-times before converting to
// a pause duration via `DataRate::bitsToTime`.
constexpr uint32_t kBitsPerQuantum = 512;
} // namespace

PfcController::PfcController(EthernetPort& port) : port_(port) {}

void PfcController::resize(const uint8_t numQueues) {
  resumeEvents_.resize(numQueues);
  refreshEvents_.resize(numQueues);
}

PfcState PfcController::getRxState(const uint8_t qId) const {
  return port_.queues_->getRxPfcState(qId);
}

PfcState PfcController::getTxState(const uint8_t qId) const {
  return port_.queues_->getTxPfcState(qId);
}

void PfcController::receiveFrame(const PacketPtr& pkt) {
  // @oss-disable
  const auto* pfc = pkt.as<EthernetPfcPacket>();
  for (uint8_t id = 0; id < port_.getNumQueues(); ++id) {
    if (!pfc->pfcClassEnable[id]) {
      continue;
    }
    const auto quanta = pfc->pfcClassPauseQuanta[id];
    if (quanta == 0) {
      ++port_.portStats_.at(id).rxXon;
    } else {
      ++port_.portStats_.at(id).rxXoff;
    }
    updateTxStateFromPeer(id, quanta);
  }
}

void PfcController::updateTxStateFromPeer(
    const uint8_t qId,
    const uint16_t quanta) {
  const PfcState newState = (quanta == 0) ? PfcState::XON : PfcState::XOFF;
  const PfcState qState = getTxState(qId);
  if ((qState == PfcState::XON) && (newState == PfcState::XON)) {
    // A resume for a class that is not paused is legal on the wire: IEEE
    // 802.1Qbb does not require a pause to precede one, and a sender that
    // coalesces its outbound state emits them routinely -- a class paused and
    // unpaused before its frame reached the wire arrives here as a bare XON.
    // Nothing to do: no pause interval to close and no timer to cancel.
    return;
  }

  if (qState == PfcState::XOFF) {
    resumeEvents_.at(qId).cancel();
  }

  auto& queues = *port_.queues_;
  switch (newState) {
    case PfcState::XON: {
      port_.portStats_.at(qId).txPauseDurationNs +=
          port_.getSimCtx()->nowNs() - queues.getTxPauseStartTime(qId);
      break;
    }
    case PfcState::XOFF: {
      queues.setTxPauseStartTime(qId);
      const auto pauseTime =
          port_.getDataRate().bitsToTime(kBitsPerQuantum * quanta);
      resumeEvents_.at(qId) = port_.getSimCtx()->schedule(
          pauseTime, &PfcController::resumeTx, this, qId);
      break;
    }
  }

  queues.updateTxPfcState(qId, newState);

  if (newState == PfcState::XON) {
    // A previously-paused CoS is now eligible.
    port_.onDataEnqueued();
  }
}

void PfcController::resumeTx(const uint8_t qId) {
  if (getTxState(qId) == PfcState::XON) {
    return;
  }
  // `updateTxStateFromPeer` wakes the scheduler when a previously-paused CoS
  // becomes eligible again.
  updateTxStateFromPeer(qId, 0);
}

void PfcController::setRxState(const uint8_t qId, const PfcState state) {
  auto& queues = *port_.queues_;
  if (queues.getRxPfcState(qId) == state) {
    return;
  }
  auto& portStats = port_.portStats_.at(qId);
  switch (state) {
    case PfcState::XON:
      ++portStats.rxXon;
      queues.updateRxPfcState(qId, PfcState::XON);
      portStats.rxPauseDurationNs +=
          port_.getSimCtx()->nowNs() - queues.getRxPauseStartTime(qId);
      break;
    case PfcState::XOFF:
      ++portStats.rxXoff;
      queues.updateRxPfcState(qId, PfcState::XOFF);
      queues.setRxPauseStartTime(qId);
      break;
  }

  markState(qId, state);
}

void PfcController::markState(const uint8_t qId, const PfcState state) {
  switch (state) {
    case PfcState::XON: {
      pending_.quanta.at(qId) = 0;
      refreshEvents_.at(qId).cancel();
      break;
    }
    case PfcState::XOFF: {
      pending_.quanta.at(qId) = txQuanta_;
      // The peer's pause expires after `txQuanta_`, so the refresh must be
      // scheduled sooner than that or the peer resumes while this port is
      // still congested. It re-marks the same class, which is why the register
      // must still emit a frame for an unchanged state.
      const auto pauseRefTime =
          port_.getDataRate().bitsToTime(kBitsPerQuantum * refreshQuanta_);
      refreshEvents_.at(qId) = port_.getSimCtx()->schedule(
          pauseRefTime, &PfcController::refreshRxPause, this, qId);
      break;
    }
  }
  // Overwrite in place: if this class already had a pending state that has not
  // reached the wire, the newer one supersedes it outright.
  pending_.classEnable.set(qId);
  pending_.dirty = true;
  // Wake the scheduler (lazy-initing for non-LLR ports) so the PFC tier is
  // re-polled. Every PFC path routes through here.
  port_.onDataEnqueued();
}

void PfcController::refreshRxPause(const uint8_t qId) {
  markState(qId, PfcState::XOFF);
}

PacketPtr PfcController::buildPendingFrame() {
  auto pkt = createPacket<PacketType::ETHERNET_PFC>(0);
  auto* pfcPkt = pkt.as<EthernetPfcPacket>();
  for (size_t id = 0; id < kNumPfcClasses; ++id) {
    if (!pending_.classEnable.test(id)) {
      continue;
    }
    pfcPkt->pfcClassEnable[id] = 1;
    pfcPkt->pfcClassPauseQuanta[id] = pending_.quanta.at(id);
    // Counted here rather than at mark time: a class marked twice before the
    // wire drained only ever put its latest state on the wire.
    auto& ps = port_.portStats_.at(id);
    if (pending_.quanta.at(id) == 0) {
      ++ps.txXon;
    } else {
      ++ps.txXoff;
    }
  }
  pending_ = Pending{};
  return pkt;
}

void PfcController::onLinkDown() {
  pending_ = Pending{};
  for (auto& event : resumeEvents_) {
    event.cancel();
  }
  for (auto& event : refreshEvents_) {
    event.cancel();
  }
  // Reset all TX PFC states to XON. The link-down breaks the PFC contract, so
  // any outstanding XOFF must be cleared. Callers drain the queues after this,
  // and `popPacket` skips XOFF'd queues.
  auto& queues = *port_.queues_;
  for (uint8_t qId = 0; qId < port_.getNumQueues(); ++qId) {
    if (queues.getTxPfcState(qId) == PfcState::XOFF) {
      queues.updateTxPfcState(qId, PfcState::XON);
    }
  }
}

} // namespace packsim
