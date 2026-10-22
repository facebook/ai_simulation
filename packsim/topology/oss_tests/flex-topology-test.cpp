/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <stddef.h>
#include <algorithm>
#include <array>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/container/detail/F14Policy.h>
#include <folly/lang/Hint.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/nic_config_types.h"
#include "packsim/if/gen-cpp2/roce_v2_config_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/if/gen-cpp2/transport_config_types.h"
#include "packsim/kernel/scheduler.h"
#include "packsim/kernel/sim-context.h"
#include "packsim/link-layer/cbfc/cbfc-sender-ledger.h"
#include "packsim/link-layer/cbfc/cbfc-types.h"
#include "packsim/link-layer/cbfc/cbfc.h"
#include "packsim/link-layer/eth-port.h"
#include "packsim/packet/base.h"
#include "packsim/packet/ptr/PacketPtr.h"
#include "packsim/topology/flex-topology.h"
#include "packsim/util/builders.h"

namespace packsim {

// Target bytes between successive LLR ACKs (LLR-enabled test config).
constexpr uint32_t kLlrAckByteThresholdBytes = 4096;
// LLR transmit buffer capacity (1 MiB) used by the LLR-enabled test config.
constexpr uint64_t kLlrTxBufferMaxBytes = 1024 * 1024;

class FlexTopologyLlrTest : public ::testing::Test {
 protected:
  void SetUp() override {
    simCfg_ = packsim::buildFourZoneClosConfig(2);
  }

  packsim::Scheduler scheduler_;
  packsim::SimContext simCtx_{&scheduler_, 0};
  packsim::thrift::Simulation simCfg_;
};

TEST_F(FlexTopologyLlrTest, LlrEnabledWhenConfigured) {
  // Enable LLR on all links
  packsim::thrift::LlrConfig llrCfg;
  llrCfg.ackByteThreshold() = kLlrAckByteThresholdBytes;
  llrCfg.txBufferMaxBytes() = kLlrTxBufferMaxBytes;

  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.llrConfig() = llrCfg;
  }

  auto topo = std::make_unique<packsim::FlexTopology>(
      &scheduler_, &simCtx_.random, simCfg_);

  // Verify LLR is enabled on both ports of each link
  for (const auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    auto [port1, port2] = topo->getLinkPorts(linkId);
    EXPECT_TRUE(port1->isLlrEnabled())
        << "LLR not enabled on port1 for link " << linkId;
    EXPECT_TRUE(port2->isLlrEnabled())
        << "LLR not enabled on port2 for link " << linkId;
  }
}

TEST_F(FlexTopologyLlrTest, LlrDisabledWhenNotConfigured) {
  // No LLR config set (default)
  auto topo = std::make_unique<packsim::FlexTopology>(
      &scheduler_, &simCtx_.random, simCfg_);

  // Verify LLR is disabled on both ports of each link
  for (const auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    auto [port1, port2] = topo->getLinkPorts(linkId);
    EXPECT_FALSE(port1->isLlrEnabled())
        << "LLR unexpectedly enabled on port1 for link " << linkId;
    EXPECT_FALSE(port2->isLlrEnabled())
        << "LLR unexpectedly enabled on port2 for link " << linkId;
  }
}

class FlexTopologyCbfcTest : public ::testing::Test {
 protected:
  void SetUp() override {
    simCfg_ = packsim::buildFourZoneClosConfig(2);
  }

  static packsim::thrift::CbfcConfig makeCbfcConfig() {
    packsim::thrift::CbfcConfig cfg;
    cfg.numVcs() = 2;
    cfg.isLosslessPerVc() = {false, true};
    packsim::thrift::VcMapEntry entry;
    entry.selector() = packsim::thrift::VcMapSelector::DSCP;
    entry.mask() = 0x3F;
    entry.value() = 0x18; // DSCP CS3 -> VC1
    entry.vcId() = 1;
    cfg.vcMap() = {entry};
    // 0 derives it from the receiver's cell size.
    cfg.expectedCreditSizeBytes() = 0;
    return cfg;
  }

  // With `expectedCreditSizeBytes` left at the 0 sentinel, this is the value
  // the link's credit size resolves to.
  static void setSwitchCellSize(
      packsim::thrift::Simulation& simCfg,
      uint32_t cellSizeBytes) {
    for (auto& [role, swCfg] : *simCfg.switchConfigMap()) {
      for (auto& buf : *swCfg.bufs()) {
        buf.cellSizeBytes() = cellSizeBytes;
      }
    }
  }

  // Sets `maxHdrmCells` on every PG of every switch port and, when
  // `poolCells` is given, `headroomPoolCells` on every switch buffer.
  static void setPgHeadroomCells(
      packsim::thrift::Simulation& simCfg,
      uint32_t pgCells,
      std::optional<uint32_t> poolCells) {
    for (auto& [role, swCfg] : *simCfg.switchConfigMap()) {
      for (auto& buf : *swCfg.bufs()) {
        if (poolCells.has_value()) {
          buf.headroomPoolCells() = *poolCells;
        }
        for (auto& [port, portCfg] : *buf.ports()) {
          for (auto& pg : *portCfg.pgs()) {
            pg.maxHdrmCells() = pgCells;
          }
        }
      }
    }
  }

  packsim::Scheduler scheduler_;
  packsim::SimContext simCtx_{&scheduler_, 0};
  packsim::thrift::Simulation simCfg_;
};

TEST_F(FlexTopologyCbfcTest, CbfcEnabledAndBootstrappedWhenConfigured) {
  const auto cbfcCfg = makeCbfcConfig();
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.cbfcConfig() = cbfcCfg;
  }

  auto topo = std::make_unique<packsim::FlexTopology>(
      &scheduler_, &simCtx_.random, simCfg_);

  // This frame maps to lossless VC1 (DSCP CS3), and a sender with no credits
  // gates a lossless frame, so `canTransmit` succeeding means the bootstrapped
  // pool is non-zero. `canTransmit` is a pure const query, so one packet can
  // probe every port.
  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(0);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3);

  for (const auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    auto [port1, port2] = topo->getLinkPorts(linkId);
    for (auto* ethPort : {port1, port2}) {
      ASSERT_TRUE(ethPort->isCbfcEnabled())
          << "CBFC not enabled for link " << linkId;
      EXPECT_EQ(2, ethPort->cbfc()->numVcs());
      EXPECT_TRUE(ethPort->cbfc()->canTransmit(*losslessPkt))
          << "lossless VC has no bootstrapped credit for link " << linkId;
    }
  }
}

// The CtlOS insertion rules (UEC §5.2.11.2) that `connectNodes` installs have
// no getter, so this pins the plumbing through the rejection path:
// `setCtlosInsertionRules` throws on an out-of-range value, which can only
// surface from topology construction if the config reached a port. Their
// runtime behaviour is covered in `link-layer-scheduler-test.cpp`.
TEST_F(FlexTopologyCbfcTest, CtlosInsertionRulesReachThePorts) {
  packsim::thrift::CtlosInsertionConfig ctlosCfg;
  ctlosCfg.minSpacingBytes() = packsim::EthernetPort::kMaxFrameBytes + 1;
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.ctlosConfig() = ctlosCfg;
  }

  EXPECT_THAT(
      [&] { packsim::FlexTopology(&scheduler_, &simCtx_.random, simCfg_); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("minSpacingBytes")));
}

// The spec's own numbers (UEC §5.2.11.2): 400 B minimum spacing between two
// CtlOS, and a 256 B window at the head of a packet that a CtlOS may not
// preempt into. With the rejection above, this pins that `ctlosConfig` is
// neither dropped nor mangled between the link config and the port.
TEST_F(FlexTopologyCbfcTest, AcceptsTheSpecCtlosInsertionRules) {
  packsim::thrift::CtlosInsertionConfig ctlosCfg;
  ctlosCfg.minSpacingBytes() = 400;
  ctlosCfg.noPreemptPrefixBytes() = 256;
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.ctlosConfig() = ctlosCfg;
  }

  EXPECT_NO_THROW(packsim::FlexTopology(&scheduler_, &simCtx_.random, simCfg_));
}

TEST_F(FlexTopologyCbfcTest, CbfcDisabledWhenNotConfigured) {
  auto topo = std::make_unique<packsim::FlexTopology>(
      &scheduler_, &simCtx_.random, simCfg_);

  for (const auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    auto [port1, port2] = topo->getLinkPorts(linkId);
    EXPECT_FALSE(port1->isCbfcEnabled())
        << "CBFC unexpectedly enabled on port1 for link " << linkId;
    EXPECT_FALSE(port2->isCbfcEnabled())
        << "CBFC unexpectedly enabled on port2 for link " << linkId;
  }
}

// One credit must be one receive cell, or the pool derived from headroom
// bytes stops bounding real occupancy: a frame smaller than a cell still
// occupies a whole one. `expectedCreditSizeBytes = 0` asks for exactly that,
// on both ends of the link at once.
TEST_F(FlexTopologyCbfcTest, CreditSizeComesFromTheReceiveCell) {
  auto cbfcCfg = makeCbfcConfig();
  cbfcCfg.expectedCreditSizeBytes() = 0;
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.cbfcConfig() = cbfcCfg;
  }

  auto topo = std::make_unique<packsim::FlexTopology>(
      &scheduler_, &simCtx_.random, simCfg_);

  const auto& nodes = *simCfg_.flex_topology()->nodes();
  size_t numCheckedLinks = 0;
  for (const auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    auto [port1, port2] = topo->getLinkPorts(linkId);
    // Only a switch quantizes its receive buffer, so read the cell size from
    // this link's switch end. A switch-to-switch link has two such ends, and
    // both must agree for the expectation below to mean anything.
    std::optional<uint64_t> cellSizeBytes;
    for (const auto* name : {&*link.node1(), &*link.node2()}) {
      const auto& node = nodes.at(*name);
      if (*node.type() != packsim::thrift::NodeType::SWITCH) {
        continue;
      }
      const auto& swCfg = simCfg_.switchConfigMap()->at(*node.role());
      const uint64_t thisEndCellSizeBytes =
          *swCfg.bufs()->front().cellSizeBytes();
      if (cellSizeBytes.has_value()) {
        ASSERT_EQ(*cellSizeBytes, thisEndCellSizeBytes)
            << "link " << linkId << "'s two switch ends quantize differently; "
            << "bootstrap should have rejected it before reaching here";
      }
      cellSizeBytes = thisEndCellSizeBytes;
    }
    // A link with no switch end has nothing to compare against, and failing
    // the size assertion below would blame the wrong thing.
    ASSERT_TRUE(cellSizeBytes.has_value())
        << "link " << linkId << " has no switch end to read a cell size from";
    ASSERT_GT(*cellSizeBytes, 0u);
    ++numCheckedLinks;
    // Both ends, because CreditSize is per link rather than per direction.
    EXPECT_EQ(*cellSizeBytes, port1->cbfc()->creditSizeBytes());
    EXPECT_EQ(*cellSizeBytes, port2->cbfc()->creditSizeBytes());
  }
  EXPECT_GT(numCheckedLinks, 0u);
}

// `validateCbfcConfig` only sees the configured 0 standing in for the default;
// the receiver's cell decides the real size. A resolved size the
// 12-bit S_CreditSize field cannot carry could never be advertised to the link
// partner, so bootstrap rejects it even though the configured value passed.
TEST_F(
    FlexTopologyCbfcTest,
    RejectsAResolvedCreditSizeTheWireFieldCannotCarry) {
  setSwitchCellSize(simCfg_, packsim::cbfc::kMaxCreditSizeBytes + 1);
  auto cbfcCfg = makeCbfcConfig();
  cbfcCfg.expectedCreditSizeBytes() = 0;
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.cbfcConfig() = cbfcCfg;
  }

  EXPECT_THAT(
      [&] { packsim::FlexTopology(&scheduler_, &simCtx_.random, simCfg_); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("credit size")));
}

// UEC §5.2.5.8 permits CreditSize < CellSize; PackSim does not model it. See
// `resolveLinkCreditSizeBytes` in flex-topology.cpp.
TEST_F(FlexTopologyCbfcTest, RejectsACreditSizeThatIsNotTheReceiveCell) {
  auto cbfcCfg = makeCbfcConfig();
  // A real value, but not the receiver's cell.
  cbfcCfg.expectedCreditSizeBytes() = 8;
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.cbfcConfig() = cbfcCfg;
  }

  EXPECT_THAT(
      [&] {
        packsim::FlexTopology topo(&scheduler_, &simCtx_.random, simCfg_);
      },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("requires CreditSize == CellSize")));
}

// CreditSize is per link, so two receivers that quantize differently have no
// single value that equals both. Rejecting is the only safe answer: whichever
// end lost would have a pool that no longer bounds its real occupancy.
TEST_F(FlexTopologyCbfcTest, RejectsALinkWhoseEndsQuantizeDifferently) {
  const auto cbfcCfg = makeCbfcConfig();
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.cbfcConfig() = cbfcCfg;
  }
  // The throw needs a link whose two ends are switches of DIFFERENT roles.
  // Without one nothing throws, and the failure would point at the mismatch
  // check instead of at the fixture that stopped providing the case.
  size_t numCrossRoleSwitchLinks = 0;
  std::string oddRole;
  const auto& nodes = *simCfg_.flex_topology()->nodes();
  for (const auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    const auto n1 = nodes.find(*link.node1());
    const auto n2 = nodes.find(*link.node2());
    if (n1 == nodes.end() || n2 == nodes.end()) {
      continue;
    }
    if (*n1->second.type() == packsim::thrift::NodeType::SWITCH &&
        *n2->second.type() == packsim::thrift::NodeType::SWITCH &&
        *n1->second.role() != *n2->second.role()) {
      ++numCrossRoleSwitchLinks;
      if (oddRole.empty()) {
        oddRole = *n1->second.role();
      }
    }
  }
  ASSERT_GT(numCrossRoleSwitchLinks, 0u)
      << "fixture must contain a switch-to-switch link spanning two roles";

  // Both sizes must stay under `cbfc::kMaxCreditSizeBytes` however many roles
  // the fixture grows to, or the credit-size-range error fires before the
  // mismatch this test is about.
  constexpr uint32_t kBaseCellSizeBytes =
      packsim::cbfc::kCbfcLimits.defaultCreditSizeBytes;
  constexpr uint32_t kOddCellSizeBytes = 2 * kBaseCellSizeBytes;
  static_assert(kOddCellSizeBytes < packsim::cbfc::kMaxCreditSizeBytes);

  // Every role shares the base size, so the only mismatching links are the
  // ones touching `oddRole`; which link throws is then deterministic.
  for (auto& [role, switchCfg] : *simCfg_.switchConfigMap()) {
    for (auto& buf : *switchCfg.bufs()) {
      buf.cellSizeBytes() = kBaseCellSizeBytes;
    }
  }
  for (auto& buf : *simCfg_.switchConfigMap()->at(oddRole).bufs()) {
    buf.cellSizeBytes() = kOddCellSizeBytes;
  }

  EXPECT_THAT(
      [&] {
        std::make_unique<packsim::FlexTopology>(
            &scheduler_, &simCtx_.random, simCfg_);
      },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("different units")));
}

// A CBFC link's two ends almost never agree on a port number: a switch takes
// its from its port groups, an endpoint counts its ports. Starving
// switch port 0 down to a single cell makes that disagreement observable --
// only senders whose partner really is on port 0 should be credit-gated, so a
// bootstrap indexing the partner's buffer by the LOCAL port number gates
// exactly the wrong set of ports.
TEST_F(FlexTopologyCbfcTest, CreditsComeFromThePartnersOwnPortNumber) {
  const auto cbfcCfg = makeCbfcConfig();
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.cbfcConfig() = cbfcCfg;
  }
  // One cell, not zero: bootstrap rejects a partner advertising less than one
  // credit, and that rejection is a different test.
  for (auto& [role, switchCfg] : *simCfg_.switchConfigMap()) {
    for (auto& buf : *switchCfg.bufs()) {
      auto it = buf.ports()->find(0);
      if (it == buf.ports()->end()) {
        continue;
      }
      for (auto& pg : *it->second.pgs()) {
        pg.maxHdrmCells() = 1;
      }
    }
  }

  auto topo = std::make_unique<packsim::FlexTopology>(
      &scheduler_, &simCtx_.random, simCfg_);

  // A max-size lossless frame needs far more credits than one cell of headroom
  // can back, whatever the configured cell size.
  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(
      packsim::EthernetPort::kMaxFrameBytes);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3);

  const auto& nodes = *simCfg_.flex_topology()->nodes();
  size_t numStarvedPartners = 0;
  for (const auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    auto [port1, port2] = topo->getLinkPorts(linkId);
    const std::array<
        std::tuple<
            packsim::EthernetPort*,
            packsim::EthernetPort*,
            const std::string*>,
        2>
        ends{{{port1, port2, &*link.node2()}, {port2, port1, &*link.node1()}}};
    for (const auto& [ethPort, partnerPort, partnerName] : ends) {
      // Only a switch varies its credited headroom by port; a WorkloadNode
      // advertises its whole NIC receive buffer whatever port it is asked
      // about, so it can never starve its partner here.
      const bool partnerIsSwitch =
          *nodes.at(*partnerName).type() == packsim::thrift::NodeType::SWITCH;
      const bool partnerStarved =
          partnerIsSwitch && partnerPort->getPortNum() == 0;
      numStarvedPartners += partnerStarved ? 1 : 0;
      EXPECT_EQ(!partnerStarved, ethPort->cbfc()->canTransmit(*losslessPkt))
          << "link " << linkId << ": port " << ethPort->getPortNum()
          << " has a partner on port " << partnerPort->getPortNum();
    }
  }
  // Guard against the assertions above passing vacuously.
  EXPECT_GT(numStarvedPartners, 0u);
}

TEST_F(FlexTopologyCbfcTest, RejectsALinkWhosePartnerAdvertisesNoHeadroom) {
  const auto cbfcCfg = makeCbfcConfig();
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.cbfcConfig() = cbfcCfg;
  }
  setPgHeadroomCells(simCfg_, /*pgCells=*/0, /*poolCells=*/std::nullopt);

  // Match the message, not just the type: `bootstrapCbfcCredits` also throws
  // `std::invalid_argument` from the credit-size resolution and from
  // `CbfcCreditTracker::validateCreditSize`, so a bare type check would pass
  // on the wrong failure.
  EXPECT_THAT(
      [&] {
        std::make_unique<packsim::FlexTopology>(
            &scheduler_, &simCtx_.random, simCfg_);
      },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("credited lossless receive buffer")));
}

// UEC Tbl 5-27 lets a receiver advertise up to 2^19-1 credits, but the sender
// ledger holds at most `kMaxPortCreditLimit`. A partner advertising more must
// leave its sender capped at that limit rather than abort the build.
TEST_F(FlexTopologyCbfcTest, CapsAPartnerPoolAboveTheLedgerLimit) {
  const auto cbfcCfg = makeCbfcConfig();
  for (auto& [linkId, link] : *simCfg_.flex_topology()->links()) {
    link.cbfcConfig() = cbfcCfg;
  }
  // A port advertises its lossless headroom at one credit per cell. Give every
  // port one cell more than the ledger can hold, and a headroom pool that
  // backs all of it.
  constexpr uint32_t kCellSizeBytes = 254;
  constexpr uint64_t kMaxCredits =
      packsim::cbfc::CbfcSenderLedger::kMaxPortCreditLimit;
  constexpr uint32_t kOverLimitCells{kMaxCredits + 1};
  setSwitchCellSize(simCfg_, kCellSizeBytes);
  setPgHeadroomCells(
      simCfg_, /*pgCells=*/kOverLimitCells, /*poolCells=*/kOverLimitCells);

  std::unique_ptr<packsim::FlexTopology> topo;
  ASSERT_NO_THROW(
      topo = std::make_unique<packsim::FlexTopology>(
          &scheduler_, &simCtx_.random, simCfg_));

  // The smallest lossless frame fits in one cell, so it costs one credit and
  // the number the gate admits back to back is the sender's whole pool.
  auto losslessPkt = packsim::createPacket<packsim::PacketType::UDP>(0);
  losslessPkt.as<packsim::UdpPacket>()->setDscpFromCos(3);
  ASSERT_LE(losslessPkt->getSize(), kCellSizeBytes);

  // Both ends of a switch-to-switch link advertise the raised headroom, so its
  // two directions stand for the rest.
  const auto& nodes = *simCfg_.flex_topology()->nodes();
  const auto isSwitch = [&nodes](const std::string& name) {
    return *nodes.at(name).type() == packsim::thrift::NodeType::SWITCH;
  };
  const auto& links = *simCfg_.flex_topology()->links();
  const auto linkIt =
      std::find_if(links.begin(), links.end(), [&](const auto& entry) {
        return isSwitch(*entry.second.node1()) &&
            isSwitch(*entry.second.node2());
      });
  ASSERT_NE(linkIt, links.end())
      << "fixture must contain a switch-to-switch link";
  auto [port1, port2] = topo->getLinkPorts(linkIt->first);
  for (auto* ethPort : {port1, port2}) {
    auto* sender = ethPort->cbfc();
    ASSERT_NE(sender, nullptr)
        << "CBFC not enabled on port " << ethPort->getPortNum();
    uint64_t numAdmitted = 0;
    // Stops one past the limit, so an uncapped pool fails instead of spinning.
    while (numAdmitted <= kMaxCredits && sender->canTransmit(*losslessPkt)) {
      sender->onTransmit(*losslessPkt);
      ++numAdmitted;
    }
    EXPECT_EQ(kMaxCredits, numAdmitted) << "port " << ethPort->getPortNum();
  }
}

class FlexTopologyGoBackNAckTest : public ::testing::Test {
 protected:
  // A NIC config running RoCEv2 with Go-Back-N loss recovery.
  static packsim::thrift::NicConfig makeGoBackNNic(
      const bool outOfOrder,
      const bool selectiveGapRepair) {
    packsim::thrift::NicConfig nic;
    nic.outofOrderPktSupport() = outOfOrder;
    packsim::thrift::GoBackNLrConfig goBackN;
    goBackN.selectiveGapRepair() = selectiveGapRepair;
    packsim::thrift::RoceV2TransportConfig roce;
    roce.congestionControl()->dummy() =
        packsim::thrift::DummyRoceV2StrategyConfig{};
    roce.lossRecovery()->goBackN() = std::move(goBackN);
    nic.transport()->roceV2() = std::move(roce);
    return nic;
  }

  // Add an accelerator with one endpoint, using the NIC config `nicName`.
  void addAccel(const std::string& name, const std::string& nicName) {
    auto& node = (*simCfg_.flex_topology()->nodes())[name];
    node.name() = name;
    node.type() = packsim::thrift::NodeType::ACCEL;
    node.endpointCfgs()->emplace_back().nicConfig() = nicName;
  }

  packsim::thrift::Simulation simCfg_;
};

TEST_F(
    FlexTopologyGoBackNAckTest,
    NamesAConfigThatAcceptsEveryPsnBesideACumulativeOne) {
  (*simCfg_.nicConfigMap())["inOrder"] =
      makeGoBackNNic(/*outOfOrder=*/false, /*selectiveGapRepair=*/false);
  (*simCfg_.nicConfigMap())["ooo"] =
      makeGoBackNNic(/*outOfOrder=*/true, /*selectiveGapRepair=*/false);
  addAccel("accel0", "inOrder");
  addAccel("accel1", "ooo");

  const auto mismatch = FlexTopology::describeGoBackNAckMismatch(simCfg_);

  ASSERT_TRUE(mismatch.has_value());
  EXPECT_THAT(
      *mismatch,
      ::testing::HasSubstr(
          "NicConfig 'ooo' runs RoCEv2 goBackN with "
          "outofOrderPktSupport on and selectiveGapRepair off"));
  EXPECT_THAT(
      *mismatch,
      ::testing::HasSubstr(
          "NicConfig 'inOrder' runs goBackN with cumulative ACKs"));
}

TEST_F(FlexTopologyGoBackNAckTest, AcceptsSelectiveGapRepairOnTheOooConfig) {
  (*simCfg_.nicConfigMap())["inOrder"] =
      makeGoBackNNic(/*outOfOrder=*/false, /*selectiveGapRepair=*/false);
  (*simCfg_.nicConfigMap())["ooo"] =
      makeGoBackNNic(/*outOfOrder=*/true, /*selectiveGapRepair=*/true);
  addAccel("accel0", "inOrder");
  addAccel("accel1", "ooo");

  EXPECT_EQ(FlexTopology::describeGoBackNAckMismatch(simCfg_), std::nullopt);
}

TEST_F(FlexTopologyGoBackNAckTest, IgnoresAConfigNoAcceleratorUses) {
  (*simCfg_.nicConfigMap())["inOrder"] =
      makeGoBackNNic(/*outOfOrder=*/false, /*selectiveGapRepair=*/false);
  (*simCfg_.nicConfigMap())["ooo"] =
      makeGoBackNNic(/*outOfOrder=*/true, /*selectiveGapRepair=*/false);
  addAccel("accel0", "inOrder");
  addAccel("accel1", "inOrder");

  EXPECT_EQ(FlexTopology::describeGoBackNAckMismatch(simCfg_), std::nullopt);
}

TEST_F(FlexTopologyGoBackNAckTest, RejectsEndpointsWithDifferentPortIndices) {
  // One reachability tracer serves every endpoint, over a single port field.
  addAccel("accel0", "nic");
  addAccel("accel1", "nic");
  (*simCfg_.flex_topology()->nodes())["accel0"]
      .endpointCfgs()
      ->at(0)
      .portIdx() = 1;
  (*simCfg_.flex_topology()->nodes())["accel1"]
      .endpointCfgs()
      ->at(0)
      .portIdx() = 2;
  packsim::Scheduler scheduler;
  packsim::SimContext simCtx{&scheduler, 0};

  EXPECT_THAT(
      [&] { packsim::FlexTopology(&scheduler, &simCtx.random, simCfg_); },
      ::testing::ThrowsMessage<std::invalid_argument>(
          ::testing::HasSubstr("must share one port index")));
}

} // namespace packsim
