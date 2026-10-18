#!/usr/bin/env bash
#
# Everything the QLever build needs on Ubuntu. Taken from the fork's own CI
# (`.github/workflows/install-dependencies-ubuntu` and `install-compiler-ubuntu`)
# rather than guessed, so that what breaks here breaks there too.
#
# Deliberately NOT installed: `libjemalloc-dev`. QLever links jemalloc when it
# finds it and warns-and-continues when it does not. Loaded into a JVM that
# already has an allocator, it crashes the process with SIGSEGV in jemalloc's
# per-thread cleanup *after* the work is done — exit 132 instead of 0, which
# `passage-qlever/README.md` documents at length. Leaving it out is the one
# place this container deliberately differs from the CI, and it is how we find
# out whether that diagnosis is right. `WITH_JEMALLOC=1` puts it back.
#
# Runs as `onCreateCommand`, so it is part of a Codespaces prebuild if one is
# configured, and it must stay idempotent.
set -euo pipefail

SUDO=""
[ "$(id -u)" -eq 0 ] || SUDO=sudo
export DEBIAN_FRONTEND=noninteractive

# First, because it costs nothing and the rest of this script can fail: this
# used to be the last step, and then an apt hiccup left the submodule pointing
# at a host the container cannot reach.
#
# The submodule is recorded with its SSH GitLab URL. Fetch it from the GitHub
# mirror instead, without touching `.gitmodules` — the two remotes hold the
# same commits.
echo "== submodule URL rewrite"
git config --global \
    url."https://github.com/momo54/qlever.git".insteadOf \
    "git@gitlab.univ-nantes.fr:molli-p/qlever.git"

echo "== apt-get update"
$SUDO apt-get update -qq

echo "== build tools"
$SUDO apt-get install -y --no-install-recommends \
    build-essential cmake git pkg-config ca-certificates \
    mold

# Ubuntu 24.04 ships Boost 1.83 and QLever wants >= 1.81 for `Boost::url`, so
# the `ppa:mhier/libboost-latest` the CI uses for older runners is not needed.
# Checked rather than assumed, because an older base image would pass apt and
# fail at the CMake configure step with a message about `Boost::url`.
echo "== third-party libraries"
$SUDO apt-get install -y --no-install-recommends \
    libicu-dev libzstd-dev libssl-dev tzdata \
    libboost-all-dev

if [ "${WITH_JEMALLOC:-0}" = 1 ]; then
    echo "== jemalloc, as the CI has it"
    $SUDO apt-get install -y --no-install-recommends libjemalloc-dev
fi

BOOST=$(dpkg-query -W -f='${Version}' libboost-dev 2>/dev/null || echo none)
echo "== boost $BOOST, cmake $(cmake --version | head -1 | cut -d' ' -f3), $(c++ --version | head -1)"
case "$BOOST" in
    none) echo "!! no libboost-dev" >&2; exit 1 ;;
    1.7*|1.80*) echo "!! Boost $BOOST has no Boost::url; QLever needs >= 1.81" >&2; exit 1 ;;
esac

echo "== done; now run .devcontainer/build-and-test.sh"
