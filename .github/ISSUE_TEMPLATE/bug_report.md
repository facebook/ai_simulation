---
name: Bug report
about: Something in this repository does not behave as documented
labels: bug
---

<!-- Security bugs do not belong here. Meta runs a bounty program for their safe
     disclosure: https://bugbounty.meta.com — please go through that process and
     do not file a public issue. -->

## What happened, and what you expected instead

## How to reproduce it

<!-- The most useful thing you can give us is a config.
     `packsim/examples/incast-2host.json` is a small one to start from; a diff
     against it is easier to read than a whole file. Include the command line
     you ran. -->

## Environment

- PackSim commit:
- OS and compiler:
- Built with: <!-- getdeps, or CMake against your own dependencies -->

## Anything you already ruled out

<!-- packsim/README.md has a "What this release does not contain" section and a
     "Known limitations" section. Several things that look like bugs are listed
     there — result fields that stay empty, a device pair pinned to one egress
     port, a zero inter-frame gap. Worth a look first; if the behaviour you hit
     is described there but the description is wrong or unclear, that is still
     worth an issue. -->
