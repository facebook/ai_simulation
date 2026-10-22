# Contributing to AI Simulation

Thank you for your interest in contributing. Please read this first — how
changes reach this repository is different from most projects, including from
most of Meta's other open-source repositories.

## How this repository works

This repository is **generated** from a directory inside Meta's internal
monorepo, which is the source of truth. Changes flow one way, from there to
here; nothing here is merged back automatically.

Several exported files have internal-only lines removed on the way out. A patch
written against the exported form of one of those files does not apply cleanly
to the internal original. So the automated GitHub-to-internal import that some
Meta repositories offer **does not work for this repository and is not used**,
whether or not the button is visible. We would rather tell you that up front
than have you find out after opening a pull request.

## Please leave the export markers alone

Where a line was removed on the way out, a comment consisting only of a marker —
an at-sign followed by `oss-disable` — is left behind. They look like dead
comments; they are not. Each one marks the position of a line that exists only
in the internal tree.

There is a second marker, an at-sign followed by `oss-enable`, and it is the
opposite: it trails a line that is live **here** and commented out internally.
Those lines are working code. `packsim/README.md` describes both.

**Do not delete either, and do not tidy them up.** A change that removes an
`oss-disable` marker is a change we cannot apply internally without also
publishing the internal-only line it was holding back. A change that removes an
`oss-enable` marker cannot be applied either: the marker is what keeps its line
live here while the internal build keeps it commented out. If a marker is
genuinely in the way of what you want to do, say so in an issue and we will deal
with it on our side.

## Pull requests

You are welcome to open one, and it is the clearest way to show us exactly what
you mean. Be aware of what happens next: we read it, and if we take the change,
a Meta engineer applies it internally, where it goes through the same review as
any other change. It reaches GitHub on the following export rather than by being
merged here, and the commit that lands is authored by that engineer — we credit
you in its message.

Before opening one:

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests in the `oss_tests/`
   directory beside the code they cover.
3. If you've changed APIs, update the surrounding comments and any `DESIGN.md`
   that describes them.
4. Build and run the tests as described under *Building* in
   `packsim/README.md`: the getdeps `test` command, or
   `ctest --test-dir _build` after a plain CMake build.
5. If you've changed `packsim/README.md` or `packsim/transport/DESIGN.md`,
   preview the documentation site they are also published as: in `website/`,
   run `yarn install --frozen-lockfile` and then `yarn start`. That
   needs Node.js 22 or newer and Yarn 1 (`npm install --global yarn`).
6. If you haven't already, complete the Contributor License Agreement ("CLA").

For anything larger than a bug fix, please open an issue first. It is cheaper
for both of us to agree on the approach before you write it, given the
round-trip above.

## Contributor License Agreement ("CLA")

In order to accept your pull request, we need you to submit a CLA. You only
need to do this once to work on any of Meta's open source projects.

Complete your CLA here: <https://code.facebook.com/cla>

## Issues

We use GitHub issues to track public bugs, and to discuss anything larger than
a bug fix before it is written. For a bug, please ensure your description is
clear and has sufficient instructions to be able to reproduce the issue.

Meta has a [bounty program](https://bugbounty.meta.com) for the safe disclosure
of security bugs. In those cases, please go through the process outlined on that
page and do not file a public issue.

## Code of Conduct

This project has adopted a Code of Conduct that we expect participants to
adhere to. Please read [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) so that you can
understand what actions will and will not be tolerated.

## License

By contributing to this project, you agree that your contributions will be
licensed under the LICENSE file in the root directory of this source tree.
