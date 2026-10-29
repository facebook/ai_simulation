# Changelog

Notable changes to this repository are recorded here, newest release first, in
the format of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The
repository is published continuously from Meta's internal tree and has no
numbered releases yet, so changes collect under Unreleased; the commit history
on `main` has the detail.

## [Unreleased]

### Added

- `NicConfig.writeLatencyNs` in `packsim/if/nic_config.thrift`: the fixed
  latency of a NIC writing a received payload into its memory, for a transport
  that holds each received packet until its payload is written. Unset, nothing
  changes. RoCEv2 never charges it: a valid value has no effect with it, and a
  negative or non-finite one is rejected when the transport is built.
- PackSim, a packet-level, discrete-event simulator for AI training fabrics. It
  models endpoints and their NICs, RoCEv2 transport with Go-Back-N loss
  recovery and DCQCN congestion control, switches and their buffers, and links
  with link-layer retry and credit-based flow control. See
  `packsim/README.md`.
- A CMake build for Linux with a getdeps manifest, and a GitHub Actions
  workflow that builds PackSim and runs its unit tests on every push and pull
  request against `main`.
- A documentation site, rendered from `packsim/README.md` and
  `packsim/transport/DESIGN.md` and published with GitHub Pages.
- The BSD 3-Clause License, in `LICENSE`.
- `Duration`, a span of simulated time, in `packsim/kernel/duration.h`. It and
  `Time` do not convert to each other, so a function that takes a `Duration`
  rejects a `Time`, and the reverse. `Scheduler::scheduleAfter` and
  `SimContext::schedule` also accept a `Duration` delay; their `Time` overloads
  stay.
- `Scheduler::runUntil(Time)`, which runs the simulation up to a point in time.
  `Scheduler::run(Time)` runs for a span, and its zero runs until no events are
  left; `runUntil` at the current time runs nothing.
- `packsim/examples/make_example_config.py`, the Python script that writes
  `packsim/examples/incast-2host.json`, using nothing beyond the standard
  library.

[Unreleased]: https://github.com/facebook/ai_simulation/commits/main
