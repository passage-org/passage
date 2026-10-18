# Building this on Linux, in a Codespace

Everything in this repository has only ever been built on macOS, on arm64.
This container is the other case: Ubuntu 24.04, x86_64, no Homebrew, nothing
preinstalled. It exists to find out what that breaks.

## What it does

`devcontainer.json` asks for a **4-core, 16 GB, 64 GB** machine and runs
`install-deps.sh` on creation. That script installs what QLever needs, taken
from the fork's own CI rather than guessed, and rewrites the submodule URL so
that the pinned QLever comes from the GitHub mirror instead of the SSH GitLab
remote a Codespace cannot reach.

Then, by hand, because it is tens of minutes and does not belong in a
container-creation hook:

```bash
.devcontainer/build-and-test.sh
```

It initialises the submodule, builds `libqleverjni.so` and `qlever-index`,
builds the six-triple index of Figure 2 of the Passage paper, runs the twelve
tests, and finishes by printing one continuation from `passage-cli`.

Expect `Tests run: 8` then `Tests run: 4`.

## The two things this is actually testing

**That a clean build works at all.** On the development machine the shared
library is whatever was last built from a working tree, and the Maven build
never has to produce it from the pinned commit. A Codespace starts from the
pin. That alone caught a stale submodule pointer: it named a QLever from
before the snapshot natives existed, so a clean build would have produced a
library without them.

**Whether jemalloc is what crashes the JVM.** `install-deps.sh` deliberately
leaves out `libjemalloc-dev`. QLever links jemalloc when it finds it and warns
and continues when it does not. On macOS the JVM dies with `SIGSEGV` in
jemalloc's per-thread cleanup *after* a run has printed its results — exit 132
rather than 0, documented in `passage-qlever/README.md`. If the last step here
exits 0, that diagnosis is right and the fix is a build option rather than a
patch. `WITH_JEMALLOC=1 .devcontainer/install-deps.sh` puts it back for
comparison.

## What is likely to break, and what to do

| | |
| --- | --- |
| The configure step fails on `Boost::url` | the base image is older than 24.04 and its Boost predates 1.81; `install-deps.sh` checks and says so |
| The compile is killed | not enough memory. `hostRequirements` asks for 16 GB, but a Codespace can be created on a smaller machine and that is not an error, only a slower, failing build |
| No space left | the build tree alone is ~2.7 GB; QLever's CI frees 6 GB before building |
| `UnsatisfiedLinkError` on a native method | the pinned QLever is older than the Java side that calls it |
| The tests say `Tests run: 0` | they aborted rather than ran; the `SKIPPING <class>` line above says why, usually an index that was not named |
