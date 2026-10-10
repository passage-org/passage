#!/usr/bin/env bash
#
# Build the QLever JNI bridge from the `qlever` submodule and put the resulting
# shared library where `QLeverNative.load()` can find it on the classpath, i.e.
# `<output>/native/<os>-<arch>/`.
#
# Called by the `native` Maven profile; see NATIVE.md. It is not part of the
# default build, because it needs a C++ toolchain and takes considerably longer
# than the rest of this module.
#
#   ./build-native.sh <output directory>
#
set -euo pipefail

OUT=${1:?usage: build-native.sh <output directory>}
HERE=$(cd "$(dirname "$0")" && pwd)
SRC=$HERE/qlever
BUILD=$SRC/build

[ -f "$SRC/CMakeLists.txt" ] ||
  { echo "the qlever submodule is empty; run: git submodule update --init" >&2; exit 1; }

case $(uname -s) in
  Darwin) OS=darwin; EXT=dylib ;;
  Linux)  OS=linux;  EXT=so ;;
  *)      echo "unsupported platform: $(uname -s)" >&2; exit 1 ;;
esac
ARCH=$(uname -m)
JOBS=$( (command -v nproc >/dev/null 2>&1 && nproc) ||
        (command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu) || echo 4 )

# The flags QLever's own macOS CI uses, plus the `range-v3` guard a recent
# Apple Clang needs; see the fork's PASSAGE-README.md, which also explains why
# a linked Homebrew `abseil` has to be unlinked first.
EXTRA=()
if [ "$OS" = darwin ] && command -v brew >/dev/null 2>&1; then
  EXTRA=(-DCMAKE_OSX_DEPLOYMENT_TARGET=11.0
         -DUSE_PARALLEL=false
         -D_NO_TIMING_TESTS=ON
         -DCOMPILER_SUPPORTS_MARCH_NATIVE=FALSE
         -DICU_ROOT="$(brew --prefix icu4c)"
         -DOPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"
         -DCMAKE_CXX_FLAGS=-DMETA_NO_STD_FORWARD_DECLARATIONS)
  if [ -e "$(brew --prefix)/include/absl" ]; then
    echo "a linked Homebrew abseil would shadow the one QLever pins;" >&2
    echo "run 'brew unlink abseil' (reversible with 'brew link abseil')" >&2
    exit 1
  fi
fi

echo "== configuring QLever in $BUILD"
cmake -S "$SRC" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release "${EXTRA[@]}"
# `qlever-index` comes along because an index is needed to use this module at
# all, and it links what `qleverjni` already built, so it costs a link rather
# than a compile.
echo "== building the qleverjni and qlever-index targets (-j $JOBS)"
cmake --build "$BUILD" --target qleverjni qlever-index -j "$JOBS"

LIB=$BUILD/lib/libqleverjni.$EXT
[ -f "$LIB" ] || { echo "no $LIB after the build" >&2; exit 1; }
[ -x "$BUILD/qlever-index" ] || { echo "no $BUILD/qlever-index" >&2; exit 1; }

DEST=$OUT/native/$OS-$ARCH
mkdir -p "$DEST"
cp "$LIB" "$DEST/"
echo "== $DEST/$(basename "$LIB")"
# Not copied anywhere: it is a 60 MB tool, not something to ship in a jar.
echo "== $BUILD/qlever-index, for building indexes"
