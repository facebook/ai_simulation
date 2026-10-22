Thanks for taking the time to open this. One thing about this repository is easy
to miss and worth knowing before you go further: it is **generated** from a
directory inside Meta's internal monorepo, and changes flow one way. A pull
request here is never merged. If we take the change, a Meta engineer applies it
internally, where it goes through the same review as any other change, and it
reaches GitHub on the following export — with you credited in the commit
message. [CONTRIBUTING.md](https://github.com/facebook/ai_simulation/blob/main/CONTRIBUTING.md)
explains why.

That round trip is also why, for anything larger than a bug fix, we would rather
you opened an issue first: agreeing on the approach before you write it is
cheaper for both of us.

## What this changes, and why

<!-- Link an issue if there is one. -->

## How you tested it

<!-- Building and testing are described under *Building* in packsim/README.md:
     the getdeps `test` command, or `ctest --test-dir _build` after a plain
     CMake build. -->

## Before you submit

- [ ] Tests, if the change needs them, are in the `oss_tests/` directory beside
      the code they cover.
- [ ] I have not deleted or tidied up any marker comment — an at-sign followed
      by `oss-disable` or by `oss-enable`. They look like dead comments and are
      not; `CONTRIBUTING.md` says what they are.
- [ ] I have completed the [Contributor License Agreement](https://code.facebook.com/cla).
      This is needed once, for any of Meta's open source projects.
