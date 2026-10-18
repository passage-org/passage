#!/usr/bin/env bash
#
# Everything the QLever build needs on Ubuntu. The dependency list comes from
# the QLever fork's own CI (`.github/workflows/install-dependencies-ubuntu` and
# `install-compiler-ubuntu`) rather than from guesswork, so that what breaks
# here breaks there too.
#
# Runs as `onCreateCommand`, so it is part of a Codespaces prebuild if one is
# configured, and it has to stay idempotent.
#
# git is installed before the long installs because `build-and-test.sh` needs
# it for the submodule, and a bare `ubuntu:24.04` has none even though the
# devcontainer image does.
set -euo pipefail

SUDO=""
[ "$(id -u)" -eq 0 ] || SUDO=sudo
export DEBIAN_FRONTEND=noninteractive

# shellcheck disable=SC1091
. /etc/os-release 2>/dev/null || true
echo "== ${PRETTY_NAME:-unknown OS}, $(uname -m), user $(id -un)"

if ! command -v apt-get >/dev/null 2>&1; then
    cat >&2 <<'MSG'
!! There is no apt-get here, so this is not the Ubuntu container that
!! .devcontainer/devcontainer.json asks for.
!!
!! In a Codespace that almost always means the container failed to build and
!! you are in the recovery container: the workspace is mounted, so these
!! scripts are all present, but the image is not the one requested and nothing
!! in it is what they expect. The reason is in the creation log:
!!
!!     tail -40 /workspaces/.codespaces/.persistedshare/creation.log
!!
!! Fix what it names, then rebuild the container (command palette:
!! "Codespaces: Rebuild Container").
MSG
    exit 1
fi

echo "== apt-get update"
$SUDO apt-get update -qq

echo "== git"
$SUDO apt-get install -y --no-install-recommends git ca-certificates

echo "== build tools"
$SUDO apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config mold

# From apt rather than from a devcontainer feature, so that there is nothing
# left to resolve when the container is built: a feature version that does not
# exist fails the whole build and lands you in the recovery container.
#
# `default-jdk` rather than `openjdk-21-jdk`, although on Ubuntu 24.04 it pulls
# exactly that: it also brings the `/usr/lib/jvm/default-java` symlink, which
# is one of the four places QLever's CMake looks for `jni.h`. With only
# `openjdk-21-jdk` the JDK sits in a versioned directory, that symlink does not
# exist, and without JAVA_HOME in the environment CMake finds no headers, skips
# the JNI target, and the build fails much later with
# `No rule to make target 'qleverjni'`.
echo "== JDK 21 and Maven"
$SUDO apt-get install -y --no-install-recommends default-jdk maven
JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")
echo "export JAVA_HOME=$JAVA_HOME" | $SUDO tee /etc/profile.d/java-home.sh >/dev/null
export JAVA_HOME

# Ubuntu 24.04 ships Boost 1.83 and QLever wants >= 1.81 for `Boost::url`, so
# the `ppa:mhier/libboost-latest` that the CI uses for older runners is not
# needed here. Checked below rather than assumed, because an older base image
# passes apt and then fails the CMake configure with a message about
# `Boost::url`.
echo "== third-party libraries"
$SUDO apt-get install -y --no-install-recommends \
    libicu-dev libzstd-dev libssl-dev tzdata libboost-all-dev

# Deliberately absent: `libjemalloc-dev`. QLever links jemalloc when it finds
# it and warns and continues when it does not. Loaded into a JVM that already
# has an allocator, it crashes the process with SIGSEGV in its per-thread
# cleanup *after* the work is done — exit 132 instead of 0, which
# `passage-qlever/README.md` documents at length. Leaving it out is the one
# place this container deliberately differs from the CI. It does not settle
# that diagnosis by itself: the crash needs a workload big enough for QLever to
# start a worker thread, which nothing in `build-and-test.sh` provides.
if [ "${WITH_JEMALLOC:-0}" = 1 ]; then
    echo "== jemalloc, as the CI has it"
    $SUDO apt-get install -y --no-install-recommends libjemalloc-dev
fi

BOOST=$(dpkg-query -W -f='${Version}' libboost-dev 2>/dev/null || echo none)
case "$BOOST" in
    none)         echo "!! no libboost-dev" >&2; exit 1 ;;
    1.7*|1.80*)   echo "!! Boost $BOOST has no Boost::url; QLever needs >= 1.81" >&2; exit 1 ;;
esac

echo "== boost $BOOST, cmake $(cmake --version | head -1 | cut -d' ' -f3), $(c++ --version | head -1)"
echo "== $(mvn -v 2>/dev/null | head -1)"
echo "== JAVA_HOME=$JAVA_HOME"
echo "== done; now run .devcontainer/build-and-test.sh"
