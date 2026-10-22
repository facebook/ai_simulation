# Changelog

Notable changes to this repository are recorded here, newest release first, in
the format of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The
repository is published continuously from Meta's internal tree and has no
numbered releases yet, so changes collect under Unreleased; the commit history
on `main` has the detail.

## [Unreleased]

### Added

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

[Unreleased]: https://github.com/facebook/ai_simulation/commits/main
