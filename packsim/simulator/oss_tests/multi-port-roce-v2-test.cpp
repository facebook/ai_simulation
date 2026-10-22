/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdint>
#include <iterator>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/container/F14Map.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/FieldRef.h>

#include "packsim/if/gen-cpp2/config_types.h"
#include "packsim/if/gen-cpp2/nic_config_types.h"
#include "packsim/if/gen-cpp2/qp_pair_config_types.h"
#include "packsim/if/gen-cpp2/results_types.h"
#include "packsim/if/gen-cpp2/switch_config_types.h"
#include "packsim/if/gen-cpp2/topology_types.h"
#include "packsim/if/gen-cpp2/workload_types.h"
#include "packsim/kernel/prefix-id-utils.h"
#include "packsim/simulator/packsim-simulation.h"
#include "packsim/transport/oss_tests/roce-v2-test-config.h"
#include "packsim/util/config-builders.h"
// @oss-disable

namespace packsim {

namespace {

// Port number is stored at this index in the FlexDeviceId.values array.
constexpr uint8_t kPortIdx = 5;
// Full prefix length for our topology (5-element PrefixId).
constexpr int kFullPrefixLength = 5;
// Ports in the multi-port topology, each 1/kNumPorts of the single-port speed.
constexpr uint32_t kNumPorts = 4;

constexpr int32_t kMessageSizeBytes = 1048576; // 1 MB

// Ports per NIC in the two-port incast topology.
constexpr uint32_t kTwoPorts = 2;
// Enough data to back the receiver's buffer up past its XOFF threshold
// (RoceV2TestConfig: 448 KiB) many times over.
constexpr uint64_t kIncastSizeBytes = 4 * 1024 * 1024;
// NIC write efficiency, in percent, at which the receiver drains its buffer at
// half the link rate.
constexpr uint32_t kHalfRateWriteEffPercent = 50;

// The `SwitchBufferConfig.cellSizeBytes` default.
constexpr uint32_t kCellSizeBytes = 254;

packsim::thrift::SwitchConfig makeSimpleSwitchConfig(uint32_t numDownlinks) {
  packsim::thrift::SwitchParams sp;
  sp.numBuffs() = 1;
  sp.numQueues() = 1;
  sp.numUplinks() = 0;
  sp.numDownlinks() = numDownlinks;
  sp.cellSizeBytes() = kCellSizeBytes;
  sp.bufSizeBytes() = 64 * 1024 * 1024; // 64 MB
  sp.rsvdBytesPerPort() = 0;
  sp.xonHystBytes() = 0;
  sp.hdrmBytesPerPortUl() = 10000;
  sp.hdrmBytesPerPortDl() = 10000;
  sp.dynShareEn() = true;
  sp.alpha() = 8.0;
  sp.isLosslessPerPg() = {true};
  sp.pipelineDelayNs() = 1250U;
  return packsim::switchParamsToConfig(sp);
}

// A RoCEv2 NIC at the given speed. `readGbps` is the aggregate DMA read
// bandwidth across all ports.
packsim::thrift::NicConfig makeRoceV2NicConfig(
    const std::string& speed,
    double readGbps) {
  packsim::thrift::NicConfig nic;
  // Sets the transport arm and the perCos vectors NicRxBuffer requires, and
  // overwrites any existing perCos values, so it has to come first.
  RoceV2TestConfig::configureRoceV2Nic(nic);
  nic.speed() = speed;
  nic.mtu() = 4200;
  nic.writeEff() = 96;
  nic.readGbps() = readGbps;
  return nic;
}

packsim::thrift::FlexLink makeLink(
    packsim::thrift::LinkId linkId,
    const std::string& node1Name,
    const std::string& node2Name,
    const std::string& speed,
    const std::vector<uint32_t>& sourceAddress,
    const std::vector<uint32_t>& destinationAddress) {
  packsim::thrift::FlexLink link;
  link.linkId() = linkId;
  link.node1() = node1Name;
  link.node2() = node2Name;
  link.node1PortGroup() = "uplinks";
  link.node2PortGroup() = "downlinks";
  link.speed() = speed;
  link.sourceAddress() = sourceAddress;
  link.destinationAddress() = destinationAddress;
  link.delayConfig()->delayNs() = 1000;
  return link;
}

packsim::thrift::Node makeSwitchNode(
    const std::string& name,
    const std::vector<uint32_t>& id,
    const std::vector<
        std::pair<std::vector<uint32_t>, packsim::thrift::LinkId>>& routes) {
  packsim::thrift::Node swNode;
  swNode.name() = name;
  swNode.id() = id;
  swNode.type() = packsim::thrift::NodeType::SWITCH;
  swNode.role() = "SW";
  auto& swTable = *swNode.lpmSpec()->prefixTable();
  for (const auto& [accelId, linkId] : routes) {
    auto key = packsim::makePrefixKey(accelId);
    swTable[key].links()->emplace_back(linkId);
    swTable[key].tiebreak()->ecmp() = packsim::thrift::EcmpTiebreakSpec();
  }
  swNode.lpmSpec()->validPrefixLengths() = {kFullPrefixLength};
  return swNode;
}

// A set portIdx enables multi-port addressing at that index in the
// FlexDeviceId; an unset one leaves the node single-port.
packsim::thrift::Node makeAccelNode(
    const std::string& name,
    const std::vector<uint32_t>& id,
    const std::vector<uint32_t>& linkIds,
    std::optional<uint8_t> portIdx = std::nullopt) {
  packsim::thrift::Node node;
  node.name() = name;
  node.id() = id;
  node.type() = packsim::thrift::NodeType::ACCEL;
  node.role() = "ACCEL";

  packsim::thrift::EndpointConfig epCfg;
  epCfg.nicConfig() = "default";
  if (portIdx.has_value()) {
    epCfg.portIdx() = *portIdx;
  }
  for (auto lid : linkIds) {
    epCfg.linkIds()->emplace_back(lid);
    epCfg.nicIds()->emplace_back(id);
    epCfg.lpmSpec()->defaultRoute()->links()->emplace_back(lid);
    node.lpmSpec()->defaultRoute()->links()->emplace_back(lid);
  }
  epCfg.lpmSpec()->defaultRoute()->tiebreak()->ecmp() =
      packsim::thrift::EcmpTiebreakSpec();
  node.endpointCfgs() = {epCfg};
  node.lpmSpec()->defaultRoute()->tiebreak()->ecmp() =
      packsim::thrift::EcmpTiebreakSpec();

  return node;
}

packsim::thrift::Simulation buildBaseSim(
    const std::string& nicSpeed,
    double readGbps,
    const std::vector<uint32_t>& accel0Id,
    const std::vector<uint32_t>& accel1Id) {
  packsim::thrift::Simulation sim;
  sim.snapshotIntervalNs() = -1;

  auto nicCfg = makeRoceV2NicConfig(nicSpeed, readGbps);
  *sim.nic() = nicCfg;
  sim.nicConfigMap()["default"] = nicCfg;
  sim.switchConfigMap()["SW"] = makeSimpleSwitchConfig(2);

  sim.flex_topology()->fullPrefixLength() = kFullPrefixLength;

  auto& workSched = *sim.newWorkloadSchedule();
  workSched.unit() = packsim::thrift::SchedulingUnit::ENDPOINT;
  workSched.method() = packsim::thrift::SchedulingMethod::SEQUENTIAL;

  packsim::workload::thrift::Workload workCfg;
  workCfg.name() = "A2A";
  workCfg.workloadId() = 0;
  workCfg.numEndpoints() = 2;
  auto& w = *workCfg.config();
  w.allToAll() = packsim::workload::thrift::AllToAll();
  w.allToAll()->sizeBytes() = kMessageSizeBytes;
  w.allToAll()->numChunks() = 16;
  w.allToAll()->maxChunksInFlight() = 16;

  workCfg.rankToId()[0] = accel0Id;
  workCfg.rankToId()[1] = accel1Id;

  workSched.workloads()->emplace_back(workCfg);

  return sim;
}

// Two nodes, one switch, one port each:
//
//   Accel_0 ---- Switch_0 ---- Accel_1
//           400G          400G
//
packsim::thrift::Simulation buildSinglePort400GConfig() {
  std::vector<uint32_t> accel0Id = {1, 0, 0, 0, 0};
  std::vector<uint32_t> accel1Id = {1, 0, 0, 0, 1};
  std::vector<uint32_t> switchId = {2, 0, 0, 0, 1};

  auto sim = buildBaseSim("400Gbps", 400, accel0Id, accel1Id);
  auto& topo = *sim.flex_topology();

  topo.nodes()["Switch_0"] =
      makeSwitchNode("Switch_0", switchId, {{accel0Id, 0}, {accel1Id, 1}});
  topo.nodes()["Accel_0"] = makeAccelNode("Accel_0", accel0Id, {0});
  topo.nodes()["Accel_1"] = makeAccelNode("Accel_1", accel1Id, {1});

  topo.links()[0] = makeLink(0, "Accel_0", "Switch_0", "400Gbps", {}, {});
  topo.links()[1] = makeLink(1, "Accel_1", "Switch_0", "400Gbps", {}, {});

  return sim;
}

// Two nodes, four rail-aligned switches, four ports each at 100G -- the same
// 400G aggregate as buildSinglePort400GConfig.
//
//   Accel_0 port0 ---- Switch_0 ---- port0 Accel_1
//   Accel_0 port1 ---- Switch_1 ---- port1 Accel_1
//   Accel_0 port2 ---- Switch_2 ---- port2 Accel_1
//   Accel_0 port3 ---- Switch_3 ---- port3 Accel_1
//
packsim::thrift::Simulation buildFourPort100GConfig() {
  std::vector<uint32_t> accel0Id = {1, 0, 0, 0, 0};
  std::vector<uint32_t> accel1Id = {1, 0, 0, 0, 1};

  auto sim = buildBaseSim("100Gbps", kNumPorts * 100, accel0Id, accel1Id);
  auto& topo = *sim.flex_topology();

  for (uint32_t port = 0; port < kNumPorts; port++) {
    const packsim::thrift::LinkId linkAccel0 = port;
    const packsim::thrift::LinkId linkAccel1 = port + kNumPorts;

    std::string switchName = fmt::format("Switch_{}", port);
    std::vector<uint32_t> switchId = {2, 0, port, 0, 1};

    // Port-level addresses: accel base ID with 1-based port at kPortIdx.
    std::vector<uint32_t> accel0PortAddr = {1, 0, 0, 0, 0, port + 1, 0, 0};
    std::vector<uint32_t> accel1PortAddr = {1, 0, 0, 0, 1, port + 1, 0, 0};
    std::vector<uint32_t> switchAddr = {2, 0, port, 0, 1, 0, 0, 0};

    topo.links()[linkAccel0] = makeLink(
        linkAccel0,
        "Accel_0",
        switchName,
        "100Gbps",
        accel0PortAddr,
        switchAddr);
    topo.links()[linkAccel1] = makeLink(
        linkAccel1,
        "Accel_1",
        switchName,
        "100Gbps",
        accel1PortAddr,
        switchAddr);

    topo.nodes()[switchName] = makeSwitchNode(
        switchName, switchId, {{accel0Id, linkAccel0}, {accel1Id, linkAccel1}});
  }

  std::vector<uint32_t> accel0Links;
  std::vector<uint32_t> accel1Links;
  accel0Links.reserve(kNumPorts);
  accel1Links.reserve(kNumPorts);
  for (uint32_t p = 0; p < kNumPorts; p++) {
    accel0Links.emplace_back(p);
    accel1Links.emplace_back(p + kNumPorts);
  }

  topo.nodes()["Accel_0"] =
      makeAccelNode("Accel_0", accel0Id, accel0Links, kPortIdx);
  topo.nodes()["Accel_1"] =
      makeAccelNode("Accel_1", accel1Id, accel1Links, kPortIdx);

  return sim;
}

// Two nodes, two rail-aligned switches, two ports each at 100G. Accel_1 sends
// an incast to Accel_0, whose NIC drains received data at half the link rate,
// so it pauses the port the data arrives on. Accel_0's link through
// Switch_<firstWiredPort> has the lower link id, so that port is wired first.
//
//   Accel_0 port0 ---- Switch_0 ---- port0 Accel_1
//   Accel_0 port1 ---- Switch_1 ---- port1 Accel_1
//
packsim::thrift::Simulation buildTwoPortIncastConfig(
    const uint32_t firstWiredPort) {
  const std::vector<uint32_t> accel0Id = {1, 0, 0, 0, 0};
  const std::vector<uint32_t> accel1Id = {1, 0, 0, 0, 1};

  auto sim = buildBaseSim("100Gbps", kTwoPorts * 100, accel0Id, accel1Id);
  sim.nic()->writeEff() = kHalfRateWriteEffPercent;
  sim.nicConfigMap()["default"].writeEff() = kHalfRateWriteEffPercent;
  // Incast ranks 1..numSources send to rank 0, which is Accel_0.
  packsim::workload::thrift::Incast incast;
  incast.sizeBytes() = kIncastSizeBytes;
  incast.numSources() = 1;
  sim.newWorkloadSchedule()->workloads()->at(0).config()->incast() =
      std::move(incast);

  auto& topo = *sim.flex_topology();
  std::vector<uint32_t> accel0Links;
  std::vector<uint32_t> accel1Links;
  for (uint32_t port = 0; port < kTwoPorts; port++) {
    const packsim::thrift::LinkId linkAccel0 = port == firstWiredPort ? 0 : 1;
    const packsim::thrift::LinkId linkAccel1 = port + kTwoPorts;
    accel0Links.emplace_back(linkAccel0);
    accel1Links.emplace_back(linkAccel1);

    const std::string switchName = fmt::format("Switch_{}", port);
    const std::vector<uint32_t> switchAddr = {2, 0, port, 0, 1, 0, 0, 0};
    topo.links()[linkAccel0] = makeLink(
        linkAccel0,
        "Accel_0",
        switchName,
        "100Gbps",
        {1, 0, 0, 0, 0, port + 1, 0, 0},
        switchAddr);
    topo.links()[linkAccel1] = makeLink(
        linkAccel1,
        "Accel_1",
        switchName,
        "100Gbps",
        {1, 0, 0, 0, 1, port + 1, 0, 0},
        switchAddr);
    topo.nodes()[switchName] = makeSwitchNode(
        switchName,
        {2, 0, port, 0, 1},
        {{accel0Id, linkAccel0}, {accel1Id, linkAccel1}});
  }
  topo.nodes()["Accel_0"] =
      makeAccelNode("Accel_0", accel0Id, accel0Links, kPortIdx);
  topo.nodes()["Accel_1"] =
      makeAccelNode("Accel_1", accel1Id, accel1Links, kPortIdx);

  return sim;
}

// What one switch sent and received over a run, summed over its ports and CoS
// queues.
struct SwitchTraffic {
  // Bytes the switch sent.
  uint64_t txBytes{0};
  // PFC XOFF frames the switch received.
  uint64_t rxXoff{0};
};

// Run a simulation and write each switch's traffic, keyed by switch name, to
// *outTraffic. Uses ASSERT, so callers wrap with ASSERT_NO_FATAL_FAILURE.
void runAndGetSwitchTraffic(
    packsim::thrift::Simulation& simCfg,
    std::map<std::string, SwitchTraffic>* outTraffic) {
  PackSimSimulation sim(&simCfg);
  sim.run();
  ASSERT_TRUE(sim.isWorkloadComplete()) << "Workload should complete";

  const auto result = sim.getThriftResult();
  for (const auto& [name, switchStats] : *result.switchStats()) {
    SwitchTraffic traffic;
    for (const auto& [port, perCosStats] : *switchStats.intfStats()) {
      for (const auto& stats : perCosStats) {
        traffic.txBytes += *stats.txBytes();
        traffic.rxXoff += *stats.rxXoff();
      }
    }
    (*outTraffic)[name] = traffic;
  }
}

// Run a simulation and write the all-to-all workload runtime in nanoseconds to
// *outNs. Uses ASSERT, so callers wrap with ASSERT_NO_FATAL_FAILURE.
void runAndGetRuntimeNs(packsim::thrift::Simulation& simCfg, uint64_t* outNs) {
  PackSimSimulation sim(&simCfg);
  sim.run();
  ASSERT_TRUE(sim.isWorkloadComplete()) << "Workload should complete";

  auto result = sim.getThriftResult();
  auto& workloads = *result.workloads();
  ASSERT_EQ(workloads.size(), 1u) << "Should have exactly one workload";

  *outNs = *workloads.begin()->second.runTimeNs();
}

// Allow every queue pair between the workload's two ranks only `port` (0-based)
// on each NIC.
void restrictQpTemplatesToPort(
    packsim::thrift::Simulation& simCfg,
    uint16_t port) {
  auto& workload = simCfg.newWorkloadSchedule()->workloads()->at(0);
  for (const auto& [srcRank, dstRank] :
       {std::pair<uint32_t, uint32_t>{0, 1}, {1, 0}}) {
    packsim::thrift::QueuePairTemplate qpTemplate;
    qpTemplate.ranks()->srcRank() = srcRank;
    qpTemplate.ranks()->dstRank() = dstRank;
    qpTemplate.nics()->srcNicIdx() = 0;
    qpTemplate.nics()->dstNicIdx() = 0;
    qpTemplate.ports()->srcPorts() = {port};
    qpTemplate.ports()->dstPorts() = {port};
    workload.qpTemplates()->emplace_back(std::move(qpTemplate));
  }
}

// Run a simulation and write the bytes each switch received, keyed by switch
// name, to *outBytes. Uses ASSERT, so callers wrap with
// ASSERT_NO_FATAL_FAILURE.
void runAndGetSwitchRxBytes(
    packsim::thrift::Simulation& simCfg,
    std::map<std::string, uint64_t>* outBytes) {
  PackSimSimulation sim(&simCfg);
  sim.run();
  ASSERT_TRUE(sim.isWorkloadComplete()) << "Workload should complete";

  const auto result = sim.getThriftResult();
  for (const auto& [name, switchStats] : *result.switchStats()) {
    uint64_t rxBytes = 0;
    for (const auto& [port, perCosStats] : *switchStats.intfStats()) {
      for (const auto& stats : perCosStats) {
        rxBytes += *stats.rxBytes();
      }
    }
    (*outBytes)[name] = rxBytes;
  }
}

} // namespace

// Pins the RoCEv2 multi-port model documented in `transport/DESIGN.md`:
// `RoceTransportBase::sendPacket` maps a device pair onto one egress port, so a
// workload between one device pair does not reach the aggregate bandwidth of
// the ports it owns.
//
// A transport that stripes a QP pair across ports would meet the opposite
// bound instead. Rather than omit the case for RoCEv2, this asserts the
// behaviour RoCEv2 actually specifies -- so a change that
// starts striping a QP across ports fails here and has to be a deliberate
// decision rather than a silent semantic change.
TEST(MultiPortRoceV2Test, SingleQpPairDoesNotSpreadAcrossPorts) {
  uint64_t singlePortNs = 0;
  auto singlePortCfg = buildSinglePort400GConfig();
  ASSERT_NO_FATAL_FAILURE(runAndGetRuntimeNs(singlePortCfg, &singlePortNs));
  ASSERT_GT(singlePortNs, 0U) << "1x400G runtime should be positive";

  uint64_t fourPortNs = 0;
  auto fourPortCfg = buildFourPort100GConfig();
  ASSERT_NO_FATAL_FAILURE(runAndGetRuntimeNs(fourPortCfg, &fourPortNs));

  const double ratio = static_cast<double>(fourPortNs) / singlePortNs;

  // 4x100G being *faster* than 1x400G would mean aggregate bandwidth is
  // being over-counted, which is a bug under any transport.
  EXPECT_GE(ratio, 0.95) << "4x100G (" << fourPortNs << " ns) should not beat "
                         << "1x400G (" << singlePortNs << " ns) - ratio "
                         << ratio << " - bandwidth is being over-counted";

  // The documented limitation. The bound is deliberately loose: the point is
  // that one device pair is confined to a subset of the ports, not the precise
  // penalty, which depends on chunking and on the fixed link and pipeline
  // delays that do not scale with port count.
  EXPECT_GT(ratio, 1.5)
      << "4x100G (" << fourPortNs << " ns) came within 50% of 1x400G ("
      << singlePortNs << " ns) - ratio " << ratio
      << " - a single device pair now spreads across ports. If that is "
      << "intended, "
      << "update the multi-port section of transport/DESIGN.md and the "
      << "RoceTransportBase::sendPacket docstring, which both specify "
      << "device-pair port pinning.";
}

// Pins the QpnPortConfig doc in `if/qp_pair_config.thrift`: on a port-indexed
// endpoint RoCEv2 picks the egress port from the device pair, so queue-pair
// templates that allow only another port move no traffic off the pinned one.
TEST(MultiPortRoceV2Test, QpTemplatePortsDoNotMoveTrafficOffThePinnedPort) {
  // Arrange: find the port the device pair is pinned to without templates.
  // Switch_<p> links port p of both NICs (buildFourPort100GConfig).
  auto unrestrictedCfg = buildFourPort100GConfig();
  std::map<std::string, uint64_t> unrestrictedBytes;
  ASSERT_NO_FATAL_FAILURE(
      runAndGetSwitchRxBytes(unrestrictedCfg, &unrestrictedBytes));
  std::optional<uint16_t> pinnedPort;
  for (uint16_t port = 0; port < kNumPorts; ++port) {
    if (unrestrictedBytes.at(fmt::format("Switch_{}", port)) > 0) {
      ASSERT_FALSE(pinnedPort.has_value())
          << "one device pair should use one port";
      pinnedPort = port;
    }
  }
  ASSERT_TRUE(pinnedPort.has_value()) << "no switch carried traffic";
  const uint16_t otherPort = (*pinnedPort + 1) % kNumPorts;

  // Act
  auto restrictedCfg = buildFourPort100GConfig();
  restrictQpTemplatesToPort(restrictedCfg, otherPort);
  std::map<std::string, uint64_t> restrictedBytes;
  ASSERT_NO_FATAL_FAILURE(
      runAndGetSwitchRxBytes(restrictedCfg, &restrictedBytes));

  // Assert
  EXPECT_GT(restrictedBytes.at(fmt::format("Switch_{}", *pinnedPort)), 0U)
      << "traffic left the pinned port " << *pinnedPort;
  EXPECT_EQ(restrictedBytes.at(fmt::format("Switch_{}", otherPort)), 0U)
      << "templates allowing only port " << otherPort << " moved traffic "
      << "onto it; if RoCEv2 now honours QPN port configs, update the "
      << "QpnPortConfig doc in if/qp_pair_config.thrift, the qpTemplates and "
      << "qpnPortConfigs docs in if/workload.thrift, README.md and "
      << "transport/DESIGN.md";
}

// Pins NIC-generated PFC to the port whose receive buffer filled: each NIC port
// has a receive buffer of its own, and only pausing that port's peer stops the
// data. The data port is wired first, so it is not the last port wired.
TEST(MultiPortRoceV2Test, NicPauseLeavesThroughThePortItsDataArrivesOn) {
  // Arrange: find the port the data arrives on. Wiring order does not move it:
  // RoCEv2 pins the device pair to an egress port by port number.
  auto probeCfg = buildTwoPortIncastConfig(/*firstWiredPort=*/0);
  std::map<std::string, SwitchTraffic> probe;
  ASSERT_NO_FATAL_FAILURE(runAndGetSwitchTraffic(probeCfg, &probe));
  const uint64_t switch0TxBytes = probe.at("Switch_0").txBytes;
  const uint64_t switch1TxBytes = probe.at("Switch_1").txBytes;
  ASSERT_NE(switch0TxBytes, switch1TxBytes)
      << "the probe cannot tell which switch carried the data: both sent "
      << switch0TxBytes << " bytes";
  const uint32_t dataPort = switch0TxBytes > switch1TxBytes ? 0 : 1;

  // Act: the probe wired port 0 first, so it already is this run when the data
  // arrives on port 0.
  std::map<std::string, SwitchTraffic> traffic = std::move(probe);
  if (dataPort != 0) {
    auto simCfg = buildTwoPortIncastConfig(dataPort);
    ASSERT_NO_FATAL_FAILURE(runAndGetSwitchTraffic(simCfg, &traffic));
  }

  // Assert
  const std::string dataSwitch = fmt::format("Switch_{}", dataPort);
  const std::string idleSwitch = fmt::format("Switch_{}", 1 - dataPort);
  ASSERT_GT(traffic.at(dataSwitch).txBytes, traffic.at(idleSwitch).txBytes)
      << "the data no longer arrives on port " << dataPort
      << " once that port is wired first";
  EXPECT_GT(traffic.at(dataSwitch).rxXoff, 0U)
      << "Accel_0 never paused the switch that sent it the data";
  EXPECT_EQ(traffic.at(idleSwitch).rxXoff, 0U)
      << "Accel_0 paused a switch that sent it no data";
}

// RoceTransportBase does not override Transport::getLastStraggler(), so even a
// RoCEv2 run in which every QP completes leaves SimulationStats.lastStraggler
// unset. README.md and if/results.thrift state this.
TEST(RoceV2SimulationTest, CompletedRunLeavesLastStragglerUnset) {
  auto simCfg = buildSinglePort400GConfig();
  PackSimSimulation sim(&simCfg);
  sim.run();
  ASSERT_TRUE(sim.isWorkloadComplete()) << "Workload should complete";

  const auto result = sim.getThriftResult();
  EXPECT_FALSE(result.stats()->lastStraggler().has_value())
      << "a completed RoCEv2 run now reports a straggler; update the straggler "
         "bullet in README.md and the lastStraggler comment in "
         "if/results.thrift";
}

} // namespace packsim
