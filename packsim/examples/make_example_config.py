# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

"""Write `incast-2host.json`, the example config beside this file.

The example is two accelerators, each linked to one switch at 200 Gbps, and an
incast in which one of them sends 4 KiB to the other over RoCEv2. The config is
a `Simulation` (`if/config.thrift`) in Thrift's Simple JSON protocol, built here
from plain dicts and lists, so the script needs nothing beyond Python's standard
library. From the repository root, this rewrites the example and runs it:

    python3 packsim/examples/make_example_config.py
    ./_build/packsim --json_cfg=packsim/examples/incast-2host.json \\
        --output_file=result.json

To write a config of your own, edit the constants and builders in this script,
and pass `--output` so the example stays as it is. `packsim/README.md`
describes the format under "Writing your own config".
"""

from __future__ import annotations

import argparse
import itertools
import json
from collections.abc import Sequence
from pathlib import Path

# A JSON object, as `json` reads and writes one.
JsonObject = dict[str, object]

# What `main` writes when given no `--output`, in this script's directory.
EXAMPLE_FILENAME = "incast-2host.json"
# The largest value a Thrift `i32` holds.
I32_MAX = 2_147_483_647

# The experiment: ranks 1 to `NUM_SOURCES` each send `MESSAGE_BYTES` to rank 0
# (`Incast` in `if/workload.thrift`). Rank `r` runs on accelerator `r + 1`.
NUM_ACCELERATORS = 2
NUM_SOURCES = 1
MESSAGE_BYTES = 4_096
SEED = 1234

# Eight traffic classes, each a priority group and a queue on every port, and
# DSCP 8 * n selects class n. RoCEv2 sends its data in class 1 and its ACKs in
# class 7. On the switch every class but class 0 is lossless.
NUM_CLASSES = 8
DSCP_PER_CLASS = 8
LOSSY_CLASS = 0
DATA_CLASS = 1
ACK_CLASS = 7

# The hardware. It models no particular switch or NIC: each value is a schema
# default, a round number, or derived from those.
LINK_SPEED = "200Gbps"
# About 20 m of fiber at 5 ns/m.
LINK_DELAY_NS = 100
MTU_BYTES = 4_200

# Switch buffers count in cells of `SwitchBufferConfig.cellSizeBytes`'s default.
CELL_SIZE_BYTES = 254
TOTAL_CELLS = 100_000
# The PFC headroom of each lossless priority group. The example's one sender
# never congests the switch, so nothing draws on it.
MAX_HDRM_CELLS = 100
# One MTU in cells, rounded up: the XON hysteresis of every port and priority
# group, and the resume hysteresis of every egress queue.
HYSTERESIS_CELLS = (MTU_BYTES + CELL_SIZE_BYTES - 1) // CELL_SIZE_BYTES
# The dynamic-threshold sharing factor of every priority group and egress queue.
ALPHA = 0.25
# The most shared cells a port, a priority group or an egress queue may hold:
# more than any buffer here has, so none is capped.
UNCAPPED_CELLS = I32_MAX
PIPELINE_DELAY_NS = 500

# Twice the link's rate, so reading from memory never holds the NIC back.
NIC_READ_GBPS = 400
NIC_LATENCY_NS = 100
# `NicConfig.writeEff` is a percentage: 100 drains the receive buffer at the
# link's rate.
NIC_WRITE_EFF_PERCENT = 100
# The NIC's receive buffer for each class, 256 KiB. The NIC pauses the switch at
# 95% of that, rounded down as `util/builders.cpp` rounds it, and resumes it one
# MTU lower.
NIC_RX_BUFFER_BYTES = 262_144
NIC_XOFF_BYTES = NIC_RX_BUFFER_BYTES * 95 // 100
NIC_XON_BYTES = NIC_XOFF_BYTES - MTU_BYTES

# Queue lengths across which the ECN marking probability rises from 0 to
# `ECN_MARK_PERCENT`. Every queue uses the VOQ_ECN profile, which reads no WRED
# threshold, so the WRED thresholds repeat these.
ECN_MIN_BYTES = 5_000
ECN_MAX_BYTES = 200_000
ECN_MARK_PERCENT = 1

# Enum values, which Simple JSON writes as integers, named as in the `.thrift`
# files under `if/`.
NODE_TYPE_ACCEL = 0  # NodeType.ACCEL
NODE_TYPE_SWITCH = 4  # NodeType.SWITCH
LINK_STATUS_UP = 1  # LinkStatus.UP
SWITCH_TYPE_ETHERNET_IPV6 = 0  # SwitchType.ETHERNET_IPV6
QUEUE_FILTER_DSCP = 1  # QueueFilterField.DSCP
QUEUE_PROFILE_VOQ_ECN = 3  # QueueProfileType.VOQ_ECN
SCHEDULER_ROUND_ROBIN = 0  # SchedulingAlgorithmType.RR
SCHEDULER_STRICT_PRIORITY = 3  # SchedulingAlgorithmType.SP
PACKET_DROP_NONE = 0  # NicEgrPktDropType.NO_DROP
NIC_SIZE_FULL = 1  # NicSize.FULL
SCHEDULING_UNIT_ENDPOINT = 3  # SchedulingUnit.ENDPOINT
SCHEDULING_METHOD_RANDOM = 2  # SchedulingMethod.RANDOM
WORKLOAD_MODE_SENDRCV = 1  # WorkloadMode.SENDRCV

# Device addresses are lists of integers. For accelerator `a`, counting from 1:
#
#   the accelerator         [1, 0, 1, a, 0, 0]
#   its NIC                 [1, 0, 1, a, 1, 0]
#   that NIC's one port     [1, 0, 1, a, 1, 1]
#   the switch              [2, 0, 1, 1]
#
# Field 5 of a NIC's address numbers its ports from 1 (`EndpointConfig.portIdx`).
# Packets carry port addresses, so the switch routes on all six fields.
PORT_FIELD = 5
PORT_ADDRESS_FIELDS = 6

SWITCH_NAME = "rtsw001.r001"
# `switchConfigMap` is keyed by a switch's role.
SWITCH_ROLE = "RTSW"
# Not read by the simulator, which keys nothing by an accelerator's role.
ACCEL_ROLE = "ACCEL"
# The `nicConfigMap` key every endpoint uses (`kDefaultNicConfig`).
NIC_PROFILE = "default"
# A link names a port group at each end, and a switch gives a group's links the
# group's ports in order of link id: switch port `a - 1` ends link `a - 1`, from
# accelerator `a`.
UPLINKS = "uplinks"
DOWNLINKS = "downlinks"


def build_simulation() -> JsonObject:
    """The example `Simulation`."""
    nic = build_nic_config()
    return {
        # Zero or less turns snapshots off.
        "snapshotIntervalNs": -1,
        "conditionalTraceIntervalsNs": {},
        "random": {"seed": SEED, "useDeterministicSeed": True},
        "snapshotNodes": [],
        "switchConfigMap": {SWITCH_ROLE: build_switch_config()},
        "nicConfigMap": {NIC_PROFILE: nic},
        "nic": nic,
        "flex_topology": build_topology(),
        "name": "",
        "newWorkloadSchedule": build_workload_schedule(),
        "includeConfigInResults": True,
        "allowIncompleteWorkloads": False,
        "networkLevelFeatureConfig": {"enablePerMessageTracking": True},
    }


def build_switch_config() -> JsonObject:
    """The switch's `SwitchConfig`: one ingress buffer, holding every port."""
    ports = [get_link_id(accelerator) for accelerator in get_accelerators()]
    return {
        "bufs": [build_switch_buffer(ports)],
        "portGroups": {DOWNLINKS: ports},
        "egressBufs": [],
        "pipelineDelayNs": PIPELINE_DELAY_NS,
        "expDelayNs": 0,
        "portToBufMap": {
            "singleMap": {str(port): 0 for port in ports},
            "pairMap": {},
        },
        "useIngress": True,
        "useEgress": False,
        "egressAdmissionCheckEnable": True,
        "enableFatalOnDrop": False,
    }


def build_switch_buffer(ports: Sequence[int]) -> JsonObject:
    """A `SwitchBufferConfig` holding `ports`, whose headroom pool has room for
    every lossless priority group on them to fill its headroom at once."""
    lossless_classes = [c for c in range(NUM_CLASSES) if is_lossless_class(c)]
    return {
        "cellSizeBytes": CELL_SIZE_BYTES,
        "totalCells": TOTAL_CELLS,
        "headroomPoolCells": len(ports) * len(lossless_classes) * MAX_HDRM_CELLS,
        "ports": {str(port): build_switch_port(port) for port in ports},
        # Not read by the simulator.
        "_calcSharedBufferCells": 0,
        "_calcSharedBufferCellsEgr": 0,
    }


def build_switch_port(port: int) -> JsonObject:
    """Switch port `port`'s `PortBufferConfig`: a priority group, a queue and an
    egress queue for each traffic class."""
    classes = range(NUM_CLASSES)
    return {
        "port": port,
        "portRsvdCells": 0,
        "portMaxSharedCells": UNCAPPED_CELLS,
        "xonHystCells": HYSTERESIS_CELLS,
        "pgs": [build_priority_group(c) for c in classes],
        "queueCfg": [
            build_queue_config(drop_rate_percent=0, averaging_period_ns=0)
            for _ in classes
        ],
        "queueScheduleCfg": SCHEDULER_ROUND_ROBIN,
        "queueFilterCfg": build_queue_filter(),
        "ucQueues": [build_egress_queue(c) for c in classes],
        "isIngrPort": True,
        "isEgrPort": True,
    }


def build_priority_group(priority: int) -> JsonObject:
    """The `PriorityGroupBufferConfig` of traffic class `priority`."""
    is_lossless = is_lossless_class(priority)
    return {
        "priority": priority,
        "rsvdCells": 0,
        "canUsePortRsvd": True,
        "dynamicSharing": True,
        "alpha": ALPHA,
        "maxSharedCells": UNCAPPED_CELLS,
        "xonHystCells": HYSTERESIS_CELLS,
        "maxHdrmCells": MAX_HDRM_CELLS if is_lossless else 0,
        "lossless": is_lossless,
        "pfc": is_lossless,
    }


def build_egress_queue(cos: int) -> JsonObject:
    """The `EgressQueueBufferConfig` of traffic class `cos`."""
    return {
        "cos": cos,
        "rsvdCells": 0,
        "dynamicSharing": True,
        "alpha": ALPHA,
        "maxSharedCells": UNCAPPED_CELLS,
        "resumeHystCells": HYSTERESIS_CELLS,
    }


def build_queue_config(drop_rate_percent: int, averaging_period_ns: int) -> JsonObject:
    """A queue's `PortQueueConfig`, marking ECN between `ECN_MIN_BYTES` and
    `ECN_MAX_BYTES`.

    Args:
        drop_rate_percent: `WredEcnConfig.dropRateWRED`.
        averaging_period_ns: `WredEcnConfig.averagingPeriod`.
    """
    return {
        "wredEcnCfg": {
            "queueProfileCfg": QUEUE_PROFILE_VOQ_ECN,
            "minQSizeWRED": ECN_MIN_BYTES,
            "maxQSizeWRED": ECN_MAX_BYTES,
            "dropRateWRED": drop_rate_percent,
            "exp": 0,
            "averagingPeriod": averaging_period_ns,
            "minQSizeECN": ECN_MIN_BYTES,
            "maxQSizeECN": ECN_MAX_BYTES,
            "markRateECN": ECN_MARK_PERCENT,
        },
        "coloredWredEcnCfgs": {},
    }


def build_queue_filter() -> JsonObject:
    """The `QueueFilterConfig` that puts DSCP 8 * n in queue n."""
    return {
        "field": QUEUE_FILTER_DSCP,
        "fieldToQueue": {str(DSCP_PER_CLASS * c): c for c in range(NUM_CLASSES)},
        "fieldToColor": {},
        "minQSizeThreshold": 30_000,
        "maxQSizeThreshold": 60_000,
    }


def build_nic_config() -> JsonObject:
    """The `NicConfig` every endpoint uses."""
    classes = range(NUM_CLASSES)
    return {
        "writeEff": NIC_WRITE_EFF_PERCENT,
        "readGbps": NIC_READ_GBPS,
        "readLatencyNs": NIC_LATENCY_NS,
        "maxQpDepth": -1,
        "allowReachableRoutesOnly": True,
        "speed": LINK_SPEED,
        "mtu": MTU_BYTES,
        "ackCoalescingFactor": I32_MAX,
        "queueScheduleCfg": SCHEDULER_STRICT_PRIORITY,
        "txLatencyNs": NIC_LATENCY_NS,
        "rxLatencyNs": NIC_LATENCY_NS,
        "queueFilterCfg": build_queue_filter(),
        # On a NIC queue the VOQ_ECN profile marks nothing.
        "queueCfg": [
            build_queue_config(drop_rate_percent=100, averaging_period_ns=7)
            for _ in classes
        ],
        "dcqcnCfg": build_dcqcn_config(),
        # The schema's defaults. Drop injection is not modelled.
        "pktDropCfg": {
            "dropPktType": PACKET_DROP_NONE,
            "numBurstDrops": 10,
            "numBurstDropIntrvl": 1_048_576,
            "randDropRange": 100,
        },
        "outofOrderPktSupport": False,
        "perCosBufferSize": [NIC_RX_BUFFER_BYTES for _ in classes],
        "perCosXon": [NIC_XON_BYTES for _ in classes],
        "perCosXoff": [NIC_XOFF_BYTES for _ in classes],
        "ackCos": ACK_CLASS,
        "dataCos": DATA_CLASS,
        "transport": build_transport(),
        "netDeviceSnapshotIntervalNs": -1,
        "enableChromeTracing": False,
        "nicSize": NIC_SIZE_FULL,
    }


def build_dcqcn_config() -> JsonObject:
    """The schema's default `DcqcnConfig`. The congestion control that
    `build_transport` selects, `dummy`, does not read it."""
    return {
        "npCnpPrio": 1,
        "rpDceTcpG": 1019,
        "rpDceTcpRtt": 1,
        "rpInitialAlpha": 1023,
        "rpMinDecFac": 50,
        "rpMinRate": 1.5,
        "rpGd": 11,
        "rpRateReduceMonitorPeriod": 4,
        "rpClampTgtRate": False,
        "rpClampTgtRateAti": True,
        "rpTimerReset": 300,
        "rpByteReset": 32767,
        "rpThreshold": 1,
        "rpAiRate": 1,
        "rpHaiRate": 50,
        "dcqcnEnable": False,
        "byteCntEnable": False,
        "rateLimiterBucketSize": 65_536,
        "rateLimiterRefreshPeriodNs": 1_000,
    }


def build_transport() -> JsonObject:
    """RoCEv2, with `dummy` for both congestion control and loss recovery, which
    turns each off (`if/roce_v2_config.thrift`)."""
    return {
        "roceV2": {
            "congestionControl": {"dummy": {}},
            "lossRecovery": {"dummy": {}},
        }
    }


def build_topology() -> JsonObject:
    """The `FlexTopology`: the switch, the accelerators and a link from each
    accelerator to the switch."""
    nodes: JsonObject = {SWITCH_NAME: build_switch_node()}
    links: JsonObject = {}
    for accelerator in get_accelerators():
        nodes[get_accelerator_name(accelerator)] = build_accelerator_node(accelerator)
        links[str(get_link_id(accelerator))] = build_link(accelerator)
    return {
        "nodes": nodes,
        "links": links,
        "fullPrefixLength": 0,
        "name": "single_hop",
        "switchType": SWITCH_TYPE_ETHERNET_IPV6,
    }


def build_switch_node() -> JsonObject:
    """The switch's `Node`, which routes each accelerator's port address over
    that accelerator's link."""
    routes: JsonObject = {
        get_prefix_key(get_port_address(accelerator)): build_route(
            [get_link_id(accelerator)]
        )
        for accelerator in get_accelerators()
    }
    return {
        "name": SWITCH_NAME,
        "id": get_switch_address(),
        "lpmSpec": build_lpm_table(routes, [PORT_ADDRESS_FIELDS]),
        "type": NODE_TYPE_SWITCH,
        "role": SWITCH_ROLE,
        "systemId": 0,
        "endpointCfgs": [],
        "nvDomain": 0,
    }


def build_accelerator_node(accelerator: int) -> JsonObject:
    """Accelerator `accelerator`'s `Node`, with one NIC of one port."""
    return {
        "name": get_accelerator_name(accelerator),
        "id": get_accelerator_address(accelerator),
        "lpmSpec": build_lpm_table({}, []),
        "type": NODE_TYPE_ACCEL,
        "role": ACCEL_ROLE,
        "systemId": 0,
        "endpointCfgs": [
            {
                "nicConfig": NIC_PROFILE,
                "linkIds": [get_link_id(accelerator)],
                "nicIds": [get_nic_address(accelerator)],
                "lpmSpec": build_lpm_table({}, []),
                "portIds": [],
                "portIdx": PORT_FIELD,
            }
        ],
        # Not read by the simulator: the workload's `rankToNvDomain` places
        # each rank in an NV domain.
        "nvDomain": get_nv_domain(accelerator),
    }


def build_link(accelerator: int) -> JsonObject:
    """The `FlexLink` from accelerator `accelerator`'s port to the switch."""
    return {
        "linkId": get_link_id(accelerator),
        "node1": get_accelerator_name(accelerator),
        "node2": SWITCH_NAME,
        "sourceAddress": get_port_address(accelerator),
        "destinationAddress": get_switch_address(),
        "node1PortGroup": UPLINKS,
        "node2PortGroup": DOWNLINKS,
        "speed": LINK_SPEED,
        "status": LINK_STATUS_UP,
        # Zero or more turns bit errors off.
        "log10Ber": 1,
        "linkFlapDelayNs": 0,
        "delayConfig": {"delayNs": LINK_DELAY_NS, "expDelayNs": 0},
    }


def build_lpm_table(routes: JsonObject, prefix_lengths: list[int]) -> JsonObject:
    """An `LpmTableSpec` with no default route.

    Args:
        routes: the route to each prefix, keyed as `get_prefix_key` keys it.
        prefix_lengths: the prefixes' lengths in fields, longest first.
    """
    return {
        "prefixTable": routes,
        "validPrefixLengths": prefix_lengths,
        "defaultRoute": build_route([]),
    }


def build_route(links: list[int]) -> JsonObject:
    """A route over `links`, spreading flows across them by ECMP."""
    return {"links": links, "tiebreak": {"ecmp": {}}}


def get_prefix_key(address: list[int]) -> str:
    """The `prefixTable` key of `address`: its fields joined with dots, e.g.
    "1.0.1.2.1.1"."""
    return ".".join(str(field) for field in address)


def build_workload_schedule() -> JsonObject:
    """The `WorkloadNewSchedule`, which holds the incast."""
    return {
        "unit": SCHEDULING_UNIT_ENDPOINT,
        "method": SCHEDULING_METHOD_RANDOM,
        "workloads": [build_incast()],
        "workloadNameInfos": {},
    }


def build_incast() -> JsonObject:
    """The incast's `Workload`."""
    ranks = range(NUM_ACCELERATORS)
    return {
        "name": "INCAST_0",
        "workloadId": 0,
        "numEndpoints": NUM_ACCELERATORS,
        "sortRanks": True,
        "startTimeNs": 0,
        "maxStaggeredStartTimeNs": 0,
        "explicitStaggeredStartTimeNs": [],
        "config": {
            "incast": {
                "sizeBytes": MESSAGE_BYTES,
                "numSources": NUM_SOURCES,
                "numChunks": 1,
                "workloadMode": WORKLOAD_MODE_SENDRCV,
            }
        },
        "rankToId": {str(r): get_accelerator_address(r + 1) for r in ranks},
        "metadata": {},
        "rankToIdList": {str(r): [get_nic_address(r + 1)] for r in ranks},
        # Two queue pairs between every two ranks, one each way: notifies need
        # a queue pair to themselves (`QueuePairTemplate`).
        "qpTemplates": [
            build_queue_pair_template(src, dst)
            for src, dst in itertools.permutations(ranks, 2)
        ],
        "qpnPairMap": {},
        "rankToNvDomain": {str(r): get_nv_domain(r + 1) for r in ranks},
        "nicQpnPairMap": {},
        "qpnPortConfigs": {},
    }


def build_queue_pair_template(src_rank: int, dst_rank: int) -> JsonObject:
    """A `QueuePairTemplate` from `src_rank`'s NIC port to `dst_rank`'s."""
    return {
        "ranks": {"srcRank": src_rank, "dstRank": dst_rank},
        "nics": {"srcNicIdx": 0, "dstNicIdx": 0},
        "ports": {"srcPorts": [0], "dstPorts": [0]},
    }


def is_lossless_class(priority: int) -> bool:
    return priority != LOSSY_CLASS


def get_accelerators() -> range:
    """The accelerators' numbers, counting from 1."""
    return range(1, NUM_ACCELERATORS + 1)


def get_accelerator_name(accelerator: int) -> str:
    return f"accel{accelerator:03d}.r001"


def get_accelerator_address(accelerator: int) -> list[int]:
    return [1, 0, 1, accelerator, 0, 0]


def get_nic_address(accelerator: int) -> list[int]:
    return [1, 0, 1, accelerator, 1, 0]


def get_port_address(accelerator: int) -> list[int]:
    return [1, 0, 1, accelerator, 1, 1]


def get_switch_address() -> list[int]:
    return [2, 0, 1, 1]


def get_link_id(accelerator: int) -> int:
    return accelerator - 1


def get_nv_domain(accelerator: int) -> int:
    """Each accelerator alone in an NV domain, numbered as the accelerator is."""
    return accelerator


def render(simulation: JsonObject) -> bytes:
    """`simulation` as the example file holds it: indented, keys in the order
    the builders give them, and a final newline."""
    return json.dumps(simulation, indent=2).encode("utf-8") + b"\n"


def get_default_output() -> Path:
    """The example config, beside this script."""
    return Path(__file__).with_name(EXAMPLE_FILENAME)


def main(argv: Sequence[str] | None = None) -> None:
    """Write the config to `--output`, by default the example beside this script.

    Args:
        argv: the arguments after the program's name; `sys.argv[1:]` if None.

    Raises:
        SystemExit: if the file cannot be written.
    """
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=get_default_output(),
        help=f"the file to write (default: the {EXAMPLE_FILENAME} beside this script)",
    )
    output: Path = parser.parse_args(argv).output
    try:
        output.write_bytes(render(build_simulation()))
    except OSError as exc:
        raise SystemExit(f"could not write {output}: {exc}") from exc


if __name__ == "__main__":
    main()
