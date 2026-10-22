# PackSim

PackSim is a packet-level, discrete-event simulator for AI training fabrics. It
models an RDMA network end to end — endpoints and their NICs, the queue pairs
and transport running on them, the switches and their buffers, and the links
between — and reports how long a collective takes and where the time went.

It is built for asking *what if* about a fabric that does not exist yet: change
the topology, the buffer sizing, the congestion-control parameters or the
collective algorithm, re-run, and compare. A run is deterministic once its seed
is fixed (`--random_seed`, or `random.useDeterministicSeed` in the config), so
two configurations can be compared directly.

> Paths in this document are relative to this directory (`packsim/`), except
> those that start at the repository root: `CMakeLists.txt`, `CONTRIBUTING.md`
> and the `build/` and `.github/` directories. The build system lives at that
> root, one level up, and every shell command runs from there: the directory
> `git clone` creates.

## What this directory contains

The simulator core, its Thrift schemas, and its unit tests:

| Directory | Contents |
|---|---|
| `kernel/` | The parallel discrete-event simulation (PDES) engine and scheduler, and the random number generator |
| `endpoint/` | Endpoints and their runtime |
| `transport/` | RoCEv2 transport, Go-Back-N error recovery, DCQCN congestion control |
| `nic/` | NIC receive buffers and a memory model |
| `switch/` | Switch model, buffer accounting, port and queue trackers |
| `queues/` | Queue models and scheduling disciplines |
| `link-layer/` | Wire model, link-layer retry, credit-based flow control (`cbfc/`) |
| `packet/` | Packet types and the pooled packet allocator |
| `topology/` | Topology construction |
| `workloads/` | Collective algorithms and traffic patterns |
| `node/`, `simulator/` | Node model and the top-level simulation driver |
| `validations/` | Config validators |
| `if/` | Thrift IDL for the config and result schemas |
| `util/` | Shared helpers, config builders, I/O |
| `examples/` | A runnable simulation config to point the binary at |

Unit tests live in `oss_tests/` directories beside the code they cover. Three
end-to-end tests, which run `examples/incast-2host.json` and replay it, are
defined in `CMakeLists.txt`.

## What this release does not contain

PackSim is developed inside Meta and this repository is a subset of that tree.
The omissions are deliberate, not accidental:

- **RoCEv2 is the only transport.** The transport layer is designed to be
  extended — see *Extending the transport* below.
- **No multi-port NIC statistics.** `SimulationResult.multiPortNicStats` and
  the `MultiPortNicStats` / `WiringPortPairBytes` / `PortPairBytes` /
  `WiringType` types ship because `if/results.thrift` defines them, but
  `WiringType` has no values here, and no transport in this repository
  overrides `Transport::getMultiPortNicStats()`, whose base returns `nullopt`.
  The map stays empty on every run, including multi-port ones.
- **No straggler report, per-QP stall dump or transport completion check.**
  `SimulationStats.lastStraggler` (`if/results.thrift`) is filled from
  `Transport::getLastStraggler()`, and `--packsim_detect_stall` prints per-QP
  state through `Transport::logWorstIncompleteQP()`. No transport in this
  repository overrides either, so `lastStraggler` is never set and a detected
  stall stops the run without naming a QP.
  `RoceTransportBase::verifyComplete()` always returns true, so an unfinished
  run is caught only by the workload-rank and byte-count checks, which fail it
  unless `Simulation.allowIncompleteWorkloads` is set.
- **No RDMA READ.** RoCEv2 models SEND/RECV and WRITE. A `PULL` workload
  (`WorkloadMode.PULL` on an all-to-all or permutation workload) posts READs,
  and the transport throws at the first one; with `channelSettings` set on it
  or on a batch that contains it, building its ranks throws instead. Use
  `SENDRCV`, or `PUSH` where the receiving NICs leave `outofOrderPktSupport`
  off (with it on, a received WRITE throws; see
  `NicConfig.outofOrderPktSupport`).
- **No incident injection.** There is no incident schema, and nothing calls
  `FbSwitch::setIncidentLookup`, so `Simulation` has no incident scenario to
  set, `SimulationResult` no incident results, and every
  `PacketDropEvent.incidentId` is 0.
- **No conditional traces.** No component of the simulator in this repository
  registers a conditional trace, so `Simulation.conditionalTraceIntervalsNs`
  (`if/config.thrift`) and `--all_conditional_traces_interval_us` trace
  nothing. A zero interval in the map is still rejected when the simulation is
  built.
- **No per-packet port selection.** RoCEv2 pins each device pair to one egress
  port (see *Known limitations*), so no endpoint builds the per-queue-pair port
  table that `NicConfig.allowReachableRoutesOnly` filters, and the field has no
  effect, although `examples/incast-2host.json` sets it. The port-reachability
  tracer the topology builds for port-addressed endpoints, which is why they
  must all use one `portIdx`, is never queried.
- **No data in some result fields.** `WorkloadResult.rankToHost`,
  `childrenResults` and `batchWorkloadResult`, `SimulationStats.workloadEvents`,
  `SimulationResult.requestStats` and `Switch.egressQueues`
  (`if/results.thrift`, `if/switches.thrift`) stay empty or zero on every run,
  and `WorkloadResult.specificResult` is always `defaultRes`. Each rank's
  device id is in `WorkloadResult.workCfg.rankToId`, a batch is reported only
  as a whole, under its own name, and when `SwitchConfig.useEgress` is set
  `Switch.egressBuffers` holds the egress buffers' queues.
- **No Python layer.** Result analysis, notebooks and the plotting library are
  not included. Results written in Thrift's Compact (`.cf`) or Binary (`.bin`)
  protocol can be read by any binding fbthrift generates from
  `if/results.thrift`; the schema uses fbthrift's structured annotations, which
  other Thrift compilers do not accept.
  JSON results are standard JSON in the shape of Thrift's Simple JSON protocol,
  except that a map keyed by structs or lists, which Simple JSON writes with
  objects or arrays as keys, is a list of `{"key", "value"}` objects.
- **No interactive debug shell and no benchmark harness.**

You will also see two kinds of marker comment left behind by the export tooling.

An at-sign followed by `oss-disable` marks a line that exists only in the
internal tree: the tooling strips the line's content and leaves the bare marker.
Most stand alone or in short runs; where a whole internal-only struct or
function was removed they form longer blocks, the longest of them in
`if/workload.thrift`. They are not missing code you need to write, and nothing
reads them at build time.

An at-sign followed by `oss-enable` trails a line that is live **here** and
commented out internally. Those lines are working code, not residue.

Please leave both alone; `CONTRIBUTING.md` explains why.

## Building

PackSim links seven third-party libraries directly, all permissively licensed:

[Boost](https://www.boost.org/) (headers only) ·
[fbthrift](https://github.com/facebook/fbthrift) ·
[fmt](https://github.com/fmtlib/fmt) ·
[folly](https://github.com/facebook/folly) ·
[gflags](https://github.com/gflags/gflags) ·
[googletest](https://github.com/google/googletest) ·
[magic_enum](https://github.com/Neargye/magic_enum)

fbthrift is used for code generation and serialization only — PackSim declares
no Thrift services — so its RPC stack is not required.

The easiest way to get all seven is `getdeps`. It ships in this repository
under `build/fbcode_builder/`, so it runs on a fresh clone. It builds folly and
fbthrift from source at the revisions pinned in `build/deps/github_hashes/`,
and the other five at the versions its manifests in
`build/fbcode_builder/manifests/` name. With `--allow-system-packages`, as
below, it uses a distribution package instead wherever a dependency's manifest
names one and it is installed.

You need Linux, Python 3.10 or newer, git, CMake 3.24 or newer (fbthrift's
minimum), and GCC 11 or newer or Clang 16 or newer; getdeps provides the rest.

These are the install, build and test commands CI runs on Ubuntu, after a
`sudo apt-get update`:

```sh
# Install the distribution packages getdeps may use instead of building them.
sudo python3 build/fbcode_builder/getdeps.py --allow-system-packages \
  install-system-deps --recursive packsim
python3 build/fbcode_builder/getdeps.py --allow-system-packages \
  build --src-dir=. packsim
python3 build/fbcode_builder/getdeps.py --allow-system-packages \
  test --src-dir=. packsim
```

On Debian and Ubuntu, `install-system-deps` installs the packages with
`apt-get` and then, as a last and separate step, runs `pip install pex`, which
PackSim's build does not use. That step fails where pip is not installed, with a
Python traceback, and where the distribution marks its Python as externally
managed (PEP 668, as Debian 12 and Ubuntu 23.04 and later do), with
`!! Failed`. Either way the packages are installed by then: carry on with
`build`.

CI also packages the binaries: it runs `install-system-deps` for patchelf as
well, passes `--project-install-prefix packsim:/usr/local` (and
`--build-type RelWithDebInfo`, the default) to both `build` and `test`, and runs
`fixup-dyn-deps` between them.

Pass the same flags to every command: getdeps derives the build directory from
them, so a `test` with different flags looks for a build that does not exist.
Leaving out `--allow-system-packages` makes getdeps build more of the
dependencies from source, which is slower, but not all of them: on Linux it
always uses the system OpenSSL (`build/fbcode_builder/manifests/openssl`), so
the `install-system-deps` step is still needed. A successful `build` ends
quietly after the install step; when a build command fails, it ends with
`!! Failed`.

getdeps keeps everything it downloads and builds — about 13 GB for this build
and its tests — under `$TMPDIR`, or `/tmp` when that is unset. Where `/tmp` is
a RAM-backed tmpfs capped at half of memory, as on Fedora, Arch, Debian 13 and
Ubuntu 25.10 and later, a machine with less than 32 GB of RAM runs out of space
partway through. Put it on disk instead: export `TMPDIR` before the first
command, or add the same `--scratch-path <dir>` to every command except the
`sudo` one, which would create `<dir>` owned by root.

getdeps sizes its parallelism from the host's memory and cores. Inside a
container with a lower memory limit, add `--num-jobs N` to the same commands,
with about one job for every 3 GB the container may use.

The installed `packsim` and `packsim-replay` are in the bin subdirectory of the
directory this prints:

```sh
python3 build/fbcode_builder/getdeps.py --allow-system-packages \
  show-inst-dir --src-dir=. packsim
```

Note that `getdeps` resolves the full transitive set, not just the seven above
— it builds folly and fbthrift from source, so it pulls their dependencies too.
Some of those are copyleft, among them `libdwarf`, `libaio` and `libiberty`,
which folly uses and PackSim's own code does not. getdeps builds folly as a
static library, so those three are also on the link line of the PackSim
executables it produces. It takes them from the distribution's packages where
`--allow-system-packages` finds them, and otherwise builds them, `libiberty`
from the GNU binutils tarball. None of these libraries is part of this
repository, and each keeps its own licence. The CMake path below does not fetch
or build them, though a folly you already have installed may still have been
built against them. List the whole set before you build either way:

```sh
python3 build/fbcode_builder/getdeps.py list-deps --src-dir=. packsim
```

If you would rather use dependencies you already have installed, this is an
ordinary CMake project. It requires C++20 and builds out of tree. folly and
fbthrift change quickly and PackSim follows them closely: getdeps builds it at
the revisions pinned in `build/deps/github_hashes/`, which move forward daily,
and an installed folly or fbthrift much older than those may not build it.
`CMakeLists.txt` is at the **repository root**, not in this directory, so run
these from there:

```sh
cmake -S . -B _build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build _build -j 8
ctest --test-dir _build
```

Pick the job count by memory, not cores: a compile job needs up to about
1.5 GB. A bare `-j` with CMake's default Makefiles generator starts every
compile at once, and the build then needs about 90 GB.

Build into `_build`, not `build` — `build/` is part of the repository and holds
`fbcode_builder` and the dependency pins, so a `rm -rf build` on a CMake output
directory placed there would take `getdeps.py` with it.

`find_package` for gflags and googletest runs in module mode against the
find-modules under `build/fbcode_builder/CMake`, which `CMakeLists.txt` adds to
`CMAKE_MODULE_PATH` for you — no flag needed.

`-DBUILD_TESTS=OFF` skips the unit-test executables, which are most of the
build. `-DBUILD_DEMOS=OFF` skips `coro_demo`, a small standalone example of the
coroutine primitives in `kernel/coro/`.

With GCC or Clang on Linux, the unit-test executables are built without debug
info outside a `Debug` build, which keeps a `RelWithDebInfo` build tree to a few
gigabytes instead of close to twenty. Other toolchains, the library and every
other binary keep theirs. To step through a failing test in a debugger,
configure with `-DPACKSIM_TEST_DEBUG_INFO=ON` or build `Debug`.

Four parallel-QP workload tests are built but not run by default, so ctest
lists them as `Not Run (Disabled)`. Each runs on about one core; together they
take about 6 minutes on a recent x86-64 server, over half of it
`all-reduce-parallel-qp-test`, and none peaks above 2 GiB. To run them,
configure with
`-DPACKSIM_HEAVY_TESTS=ON`; `ctest --test-dir _build -L heavy` then runs only
those four and `ctest --test-dir _build -LE heavy` everything else. With
getdeps, pass
`--extra-cmake-defines '{"PACKSIM_HEAVY_TESTS": "ON"}'` to both `build` and
`test`: getdeps derives the build directory from its flags, so a `test` without
the define looks for a build that does not exist. getdeps also folds the define
into every dependency's build hash, and passes it to each one it builds with
CMake, so it builds them all again, folly and fbthrift included.

### Continuous integration

`.github/workflows/getdeps_linux.yml` builds the getdeps path above on
`ubuntu-24.04` and runs the tests, on every push and pull request against
`main`. That is the whole matrix, and Linux-only is a decision rather than an
omission: every angled include under `packsim/` resolves to one of the seven
dependencies, to the C++ standard library, or to
`<fcntl.h>`/`<unistd.h>`/`<sys/resource.h>`, so a job on another platform would
mostly compile the same code again. It would not be a supported build either:
`util/memory-usage.cpp`, for one, reads the peak RSS from `getrusage` as
kibibytes, the unit Linux uses.

The workflow file is generated — it says so in its header — so an edit to it
will be overwritten. Regenerate it instead, from the repository root:

```sh
python3 build/fbcode_builder/getdeps.py generate-github-actions \
  --allow-system-packages --os-type linux \
  --free-up-disk --free-up-disk-before-build \
  --output-dir .github/workflows packsim
```

The generator needs the `jinja2` Python package. Without it the command fails,
and only after it has emptied the workflow file.

Both `--free-up-disk` flags are required for PackSim, even though the generator
treats them as optional and defaults them to off. Without them it emits a
workflow with its disk-reclaiming steps missing. The build needs the space those
steps reclaim, so a regeneration that drops them produces a job that fails on a
full disk with nothing in the diff to explain why.

Settings that have to survive the next regeneration belong in
`build/fbcode_builder/manifests/packsim`, under its `[github.actions]` section,
not in the workflow. Only the job timeout (`timeout_minutes`) is set there.
Tests and sccache run because the generator turns both on unless that section
sets `run_tests = off` or `sccache = off`. The build type is
the exception: the generator's own `--build-type`, which defaults to
`RelWithDebInfo`, takes precedence over a `build_type` set there.

## Running

Besides the unit tests, a default build produces three executables: `packsim`,
`packsim-replay`, and `coro_demo` (the last only with `BUILD_DEMOS`, which is on
by default).

`packsim` takes a simulation config, a serialized `Simulation` struct (defined
in `if/config.thrift`), and writes a `SimulationResult` (`if/results.thrift`).
A getdeps build installs `packsim` and `packsim-replay` in the bin subdirectory
of the directory `show-inst-dir` prints. A plain CMake build leaves the
executables at the top of its build directory, so from the repository root:

```sh
./_build/packsim \
  --json_cfg=my-fabric.json \
  --output_file=result.json
```

Useful flags:

| Flag | Effect |
|---|---|
| `--json_cfg` | Path to the simulation config. The extension picks its format, the same way as for `--output_file` |
| `--output_file` | Where to write the result, packsim_result.json by default. The extension picks the format — `.json`, `.cf` (Thrift compact) or `.bin` — and an optional `.gz` or `.zstd` after it compresses the file, so `.cf.zstd` is a compressed compact result |
| `--validate` | Run every config validator before simulating, and abort on failure |
| `--random_seed` | Override the seed, forcing a deterministic run |
| `--no_real_world_timing` | Zero the wall-clock timings in the result (`realWorldTiming`). With a fixed seed, `runId` (random unless `--run_id` sets it) and `stats.maxRssBytes` (peak memory) still differ from run to run |
| `--is_test` | Skip both the replay snapshot (so `packsim-replay` cannot re-run the run) and the JSON Lines run events (begin, progress, end, exception) otherwise printed to standard output |
| `--pdes_threads` | Worker threads for the simulation engine (see below) |

`packsim --help` lists the rest. The `--blob_*` flags name paths in a blob
store; in this build that store is the local filesystem rooted at the working
directory, and a blob path has to stay under it. An absolute path, or one whose
`..` components climb out of the working directory, is refused: a config read
from such a path fails the run, while a result written to one is not written,
with an error in the log, and the run still succeeds. `--run_id`, which names
the replay snapshot below, becomes part of the snapshot's path, so a `/` in it
makes subdirectories under the snapshot directory and a `..` climbs out of it;
only an id that climbs out of the working directory is refused, and the run
then logs an error and goes on without a snapshot. A config's
`jobInfo.resultPath` is ignored, with a warning: only `--blob_output` writes the
result to a blob path, so a config you download cannot pick a file for the run
to overwrite. `--json_cfg` and `--output_file` are ordinary paths, not confined
to the working directory.

Every run without `--is_test` also writes a replay snapshot of its config to
`_packsim_runs/snapshots/<run-id>.bin.gz`, relative to the working directory.
Once it is written, and before the simulation starts, `packsim` logs the
command that re-runs it with `packsim-replay`:

```sh
./_build/packsim-replay --run_id=<run-id>
```

That writes `<run-id>-result.json` unless you pass `--output_file`. Both
commands resolve the snapshot directory against the working directory, so run
them from the same place. The snapshot records the seed the run used, so a
replay reproduces a run whose config did not fix one.

The simulator echoes a few headline metrics to the log as it finishes —
end time, bytes delivered, workload count, peak RSS — with the full detail in
the result file.

`examples/incast-2host.json` is a complete, runnable config to start from: two
accelerators and one switch, carrying a single incast workload with a fixed
seed. Its switch buffer, delays, ECN thresholds and NIC timing are schema
defaults, round numbers or values derived from them, not a model of any
particular switch or NIC. The command below runs from the repository root, like
the ones above it, so the path it passes carries the `packsim/` prefix:

```sh
./_build/packsim --json_cfg=packsim/examples/incast-2host.json --output_file=result.json
```

When it loads a JSON config, `packsim` ignores every key the schema does not
define, such as a misspelled field name, and logs a warning that names each one
by its path, up to 20 of them. It does not check a compact or binary config,
which identifies fields by id, a JSON config over 8 MiB, or a JSON config that
has a map keyed by structs and contains whitespace, which is neither standard
JSON nor Simple JSON as `packsim` writes it; the log notes when a JSON config
goes unchecked.

### Writing your own config

**Edit a copy of the example.** `examples/incast-2host.json` is the only config
this repository ships. It is a `Simulation` struct (`if/config.thrift`) in
Thrift's Simple JSON protocol: field names are the keys, enum values are the
integers the `.thrift` files assign them (`"type": 4` is a switch), and map keys
are strings, integer and enum keys included, except in a map keyed by a struct
(`nicQpnPairMap`), where the key is the struct's object; `packsim` reads that,
but standard JSON tools reject it. A field left out takes its default: the
value its `.thrift` file gives it, or zero or empty where the file gives none.
An `optional` field left out is unset, which `packsim` can treat differently
from an explicit empty value: `"channelSettings": {}` turns channels on. A
workload with channels cannot also list `qpTemplates`, so delete the example's
before turning them on. Settings such as the workload
(`newWorkloadSchedule.workloads`) or the switch buffers (`switchConfigMap`,
keyed by switch role) can be edited in place. Adding a device cannot. It needs:

- a node in `flex_topology.nodes`, with `linkIds` and `nicIds` in its
  `endpointCfgs`;
- a link in `flex_topology.links` for each of its ports, with the device as
  `node1` and `sourceAddress` set to that port's address: the NIC id with the
  field at `portIdx` (5 in the example) set to the 1-based port number, so
  `[1,0,1,3,1,1]` for NIC id `[1,0,1,3,1,0]`;
- a route keyed by each new port address (`"1.0.1.3.1.1"`, not the NIC id) in
  every switch's `lpmSpec`;
- for each new link, a port on the switch it reaches, in that switch role's
  `switchConfigMap` entry: in the port group the link names (`portGroups`), in
  `portToBufMap.singleMap`, and in the matching buffer's `bufs[n].ports`;
- in each workload that should use the device, a higher `numEndpoints`, a rank
  in `rankToId`, and entries in `rankToIdList` (NIC ids) and `rankToNvDomain`
  when the workload fills those maps, as the example does; also raise any field
  that limits which ranks take part, such as `Incast.numSources`, which the
  example sets to 1 (ranks above it send nothing), and keep `numEndpoints` a
  count the workload accepts (a collective's `numRails` must divide it and
  leave each rail at least 2 ranks, or 1 for a `RING` collective whose ranks
  have NV domains; see `AllReduce.numRails`);
- entries for its queue pairs in the `qpTemplates` of each workload that
  lists them, as the example does. List two templates between each pair of
  ranks, such as one in each direction, since notifies need a queue pair to
  themselves (see `QueuePairTemplate`). A workload with an empty `qpTemplates`
  gets queue pairs generated for every rank pair it connects, and needs no
  entries. The last two rules assume the workload's `nicQpnPairMap` and
  `qpnPairMap` are empty, as they are in the example.

Run an edited config with `--validate`, which checks it before the simulation
starts. It reports a link that a node's `linkIds` or a route names but
`flex_topology.links` lacks, or that does not name that node. Every other
omission in the list passes it and shows up only when the simulation runs: as
an exception at startup, when traffic first needs the missing piece, or at the
end when a rank has not completed; or as a device that sends nothing.

**Generate configs from C++.** For a larger fabric, write a program against
`util/builders.h` and `util/config-builders.h`, which several of the unit tests
under `*/oss_tests/` use to make their configs. `buildFourZoneClosWorkloadSim`
returns a complete four-zone Clos `Simulation` running a collective (it takes a
`packsim::Random` from `kernel/random.h`). `addFlexTopology` returns a copy of
a `Simulation` whose `flex_topology` is replaced by a Clos fabric described by
`ClosLinks` and `ClosTopologySpec`; the copy runs only once `switchConfigMap`
holds `RTSW`, `CTSW` and `ATSW` entries sized for the ports it wires, as
`buildFourZoneClosConfig` builds them, and once its workloads use the new
devices. The program includes these headers from the repository root, as
`#include "packsim/util/builders.h"`. Serialize the result with
`apache::thrift::SimpleJSONSerializer` from
`<thrift/lib/cpp2/protocol/Serializer.h>` and write it to a `.json` file.
`buildFourZoneClosWorkloadSim` fixes no seed, so pass `--random_seed` when
running its config for a repeatable run.

The build installs only `packsim` and `packsim-replay`, with no headers or
library, so such a program has to be built inside this source tree, with an
executable next to `packsim`'s in the root `CMakeLists.txt`. A source in this
directory or below it, outside any `oss_tests/` directory, also needs its path
in `PACKSIM_MAIN_SOURCES` there. For a source saved as make-config.cpp in this
directory, add `"${CMAKE_CURRENT_SOURCE_DIR}/packsim/make-config.cpp"` to that
list and:

```cmake
add_executable(make-config "${CMAKE_CURRENT_SOURCE_DIR}/packsim/make-config.cpp")
target_link_libraries(make-config PRIVATE packsim_core)
```

Without the `PACKSIM_MAIN_SOURCES` entry, configure stops with an error naming
the file: every other source under this directory outside `oss_tests/` is
compiled into the `packsim_core` library, and configure rejects a `main()`
there. It looks for one by text, so it catches `int main(` or `auto main(` at
the start of a line, after any indentation, but not, for example, a return type
on a line of its own. A source outside this directory needs only those two
lines, naming its own path.

## Known limitations

**A device pair is pinned to one egress port.** When an endpoint with a port
index (`EndpointConfig.portIdx > 0`) owns several ports, the transport selects
one from the source and destination device IDs: the sum of both IDs' fields,
modulo the number of ports. The sum is symmetric under the source/destination
swap, so when both endpoints register the same ordered port set, both
directions and every queue pair between the devices use the same physical port.
This keeps QP packets on one path and makes selection independent of QPN
allocation stride. `QueuePairTemplate.ports` (`if/qp_pair_config.thrift`) and
`Workload.qpnPortConfigs` do not change that selection. An endpoint without a
port index registers only its first port with the transport, and routes every
packet by device address through its LPM table, whose tiebreak picks the link.

The consequence is that aggregate bandwidth across ports comes from
communicating with *several device peers*, not from opening several QPs to one
peer. A point-to-point workload is confined to the port its device pair selects,
and adding ports does not make it faster — between two accelerators with one QP
pair for data, four 100G ports, each through its own switch, complete an
all-to-all in 54,221ns against 21,693ns for a single 400G port through one
switch, 2.5x slower at identical aggregate bandwidth.
`MultiPortRoceV2Test.SingleQpPairDoesNotSpreadAcrossPorts`
(`simulator/oss_tests/multi-port-roce-v2-test.cpp`) pins this behaviour: it
fails if the four ports come within 1.5x of the single one, so a change that
starts striping a QP has to be a deliberate one.

**The Ethernet inter-frame gap is modelled as zero.**
`EthernetPacket::getInterFrameGapSize()` (`packet/base.h`) returns 0 rather than
the IEEE 802.3 12-byte gap, and the link-layer scheduler feeds that straight
into transmit time — so every frame occupies the wire for slightly less time
than the standard allows. The error is bounded by the gap's share of the time a
frame should occupy the wire: 12 bytes of the 84 a minimum-size frame takes
(64-byte frame, 8-byte preamble, 12-byte gap) is 14.3%, and for a full data
frame, with the 4,096-byte payload RoCEv2 sends at most, it is about 0.3%, so
it matters for small-packet traffic and is close to noise for bulk transfer.
This is not an approximation chosen for speed. The spec-correct gap shifts
transmit completion by around a nanosecond at 100Gbps, which is enough to pull
two events into the same PDES lookahead window on different worker threads and
expose a pre-existing race in switch buffer accounting; the override documents
the race and the fix it calls for. Returning 0 avoids it.

**`--pdes_threads` defaults to 4 workers.** Its default value, 0, means 4
rather than one per core, because one-per-core is pathological on large hosts —
a DCQCN test that takes 2m47s at 4 threads exceeded 10 minutes at 192 — while
on the more modest machines typical of open-source CI the difference is small.
Pass `--pdes_threads=N` to use more.

**Packet spraying over a fabric that can drop packets needs
`selectiveGapRepair`.** The `spray` tiebreak picks a random link for each
packet, so the receiving NICs need `outofOrderPktSupport`. With it on, RoCEv2
`goBackN` loss recovery repairs a PSN gap only when
`GoBackNLrConfig.selectiveGapRepair` is true. With it false, the receiver
accepts every PSN without sending a gap NACK, and no retransmit timer runs, so
no gap NACK or timer resends a dropped packet. A dropped data packet leaves its
message incomplete at the receiver, even though the sender completes the
message once its last packet is acknowledged. A dropped last packet or ACK
leaves the message incomplete at the sender, and later send completions on that
QP wait behind it. If no periodic timers are left, the run then ends with
`N workload ranks didn't complete` (with `allowIncompleteWorkloads`, a warning
and missing bytes). While a periodic timer keeps running, the run does not end
on its own until `--packsim_detect_stall` or `--packsim_max_wall_time_sec`
(both off by default) stops it. CBFC and
LLR timers, and the port byte trace that
`Simulation.nic.netDeviceSnapshotIntervalNs` turns on, for example, keep
running. With the `dcqcn` congestion-control arm, so do a QP's two DCQCN timers,
alpha update and rate increase, until an ACK or a receive completion leaves that
QP with nothing outstanding; after that, only a CNP that cuts the rate restarts
one, the rate-increase timer. Both start at QP creation when
`DcqcnConfig.dcqcnEnable` is set; otherwise the rate-increase timer starts at
the first CNP that QP receives. Packets are dropped by a switch buffer that
cannot admit them (on a lossy priority group, or on a lossless one whose
headroom is used up), by a switch or NIC egress queue whose `WRED` or
`WRED_ECN` profile drops them (RoCEv2 sends packets that are not ECN-capable,
and `WRED_ECN` drops rather than marks those unless an earlier `ECN` or
`VOQ_ECN` queue has marked them), by a switch with no route for them, and by a
link that is down or has a negative `log10Ber` without LLR. While
`SwitchConfig.enableFatalOnDrop` is true, its default, a packet a switch buffer
cannot admit throws and ends the run instead.

## Extending the transport

The transport layer uses composition rather than inheritance: a single
`RoceTransportBase` takes a config that selects its error-recovery and
congestion-control strategies, and each socket builds its own, so N recovery
modes and M congestion-control modes cost N+M components instead of N×M
subclasses.

To add a strategy, subclass `ErrorRecovery` (`transport/error-recovery.h`) or
`CongestionControl` (`transport/congestion-control.h`), add the enum value to
`RoceTransportConfig` in `transport/roce-base.h`, wire it into
`RoceTransportConfig::fromThrift()` and `RoceTransportBase::makeSocket()` in
`transport/roce-base.cpp`, and add an arm for it, with any parameters, to the
matching union in `if/roce_v2_config.thrift`. `GoBackNRecovery` and
`DcqcnCongestionControl` are the reference implementations, and
[transport/DESIGN.md](transport/DESIGN.md) walks through each step.

Strategy components hold state and return decisions; the socket owns packet
creation and transmission, and a component schedules only its own timers.
Keeping I/O out of the components is what lets them be tested in isolation.

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) and our
[Code of Conduct](../CODE_OF_CONDUCT.md).

## License

PackSim is licensed under the BSD 3-Clause License, as found in the
[LICENSE](../LICENSE) file.
