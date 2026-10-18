#!/usr/bin/env bash
#
# The whole chain, from a fresh container to passing tests. This is
# `passage-qlever/README.md`'s "from a clone to passing tests, in one paste",
# with the macOS parts removed and the timings of a Codespace instead.
#
# Not an `onCreateCommand`: the C++ build is tens of minutes and a prebuild
# that long is worse than running this by hand, once, when you want it.
set -euo pipefail

cd "$(dirname "$0")/.."
START=$(date +%s)
step() { echo; echo "===== $* ($(( $(date +%s) - START ))s)"; }

step "the pinned QLever"
git submodule update --init passage-qlever/qlever
echo "pinned at $(git -C passage-qlever/qlever rev-parse --short HEAD)"

step "the JNI bridge and qlever-index"
# `build-native.sh` needs no macOS flags here: on Linux its extra-flags list
# stays empty and `nproc` sets the parallelism.
mvn -pl passage-qlever install -Pnative -DskipTests

step "the library that came out"
find ~/.cache/passage-qlever passage-qlever/target/classes/native \
     -name 'libqleverjni.*' -exec ls -la {} \; 2>/dev/null || true

step "an index: the six triples of Figure 2 of the Passage paper"
mkdir -p "$HOME/qlever-fig2"
cat > "$HOME/qlever-fig2/fig2.ttl" <<'TTL'
<http://ex.org/H1> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H2> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H3> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O1> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O2> .
<http://ex.org/H3> <http://ex.org/occupation> <http://ex.org/O3> .
TTL
passage-qlever/build-index.sh "$HOME/qlever-fig2/fig2" "$HOME/qlever-fig2/fig2.ttl"

step "the tests"
# Expect `Tests run: 8` then `Tests run: 4`. A `Tests run: 0` means they
# aborted rather than ran, and the `SKIPPING <class>` line above says why.
mvn -pl passage-qlever,passage-volcano test \
    -Dtest='QLeverBackendTest,QLeverContinuationTest' -DfailIfNoTests=false \
    -Dqlever.test.index="$HOME/qlever-fig2/fig2"

step "a continuation from the command line"
mvn -q -pl passage-cli -am install -DskipTests
# The exit status is the point of this step, so it must not end the script:
# 132 is the SIGSEGV in jemalloc's thread cleanup, 0 means it is not there.
set +e
java -jar passage-cli/target/passage.jar \
    -d "$HOME/qlever-fig2/fig2" \
    -q 'SELECT * WHERE { ?human <http://ex.org/type> <http://ex.org/Human> .
                         ?human <http://ex.org/occupation> ?occupation }' \
    -l 1 -r
RC=$?
set -e
echo "exit $RC — 0 means no jemalloc crash, 132 means it got linked after all"

step "done"
