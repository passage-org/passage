#!/usr/bin/env bash
#
# Build the QLever JNI bridge from the `qlever` submodule and put the resulting
# shared library where `QLeverNative.load()` can find it on the classpath, i.e.
# `<output>/native/<os>-<arch>/`.
#
# Called by the `native` Maven profile; see README.md. It is not part of the
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
# `JOBS` in the environment overrides it. QLever has translation units that
# take a couple of gigabytes each, so one job per core can exhaust the memory
# of a small machine; the compiler is then killed by the OOM killer and says
# so as `c++: fatal error: Killed signal terminated program cc1plus`, which
# reads like a compiler bug and is not one. `JOBS=2` is the way out.
JOBS=${JOBS:-$( (command -v nproc >/dev/null 2>&1 && nproc) ||
        (command -v sysctl >/dev/null 2>&1 && sysctl -n hw.ncpu) || echo 4 )}

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

# CMake creates the `qleverjni` target only when it finds `jni.h`, looking in
# `$JAVA_HOME/include`, `$JAVA_HOME/Contents/Home/include`,
# `/opt/homebrew/opt/openjdk/include` and `/usr/lib/jvm/default-java/include`.
# A Linux box with a versioned JDK directory, no `default-java` symlink and no
# JAVA_HOME matches none of them: the target is quietly not created and the
# build fails much later with `No rule to make target 'qleverjni'`. Resolve it
# here and say so now instead.
if [ -z "${JAVA_HOME:-}" ] && command -v javac >/dev/null 2>&1; then
  # Only if it really holds the headers: `javac` is often a shim, and
  # exporting its directory would hand CMake a JAVA_HOME that means nothing.
  if JH=$(dirname "$(dirname "$(readlink -f "$(command -v javac)" 2>/dev/null)")") &&
     [ -f "$JH/include/jni.h" ]; then
    JAVA_HOME=$JH
    export JAVA_HOME
    echo "== JAVA_HOME=$JAVA_HOME, derived from javac"
  fi
fi
JNI=""
for d in "${JAVA_HOME:-}/include" "${JAVA_HOME:-}/Contents/Home/include" \
         /opt/homebrew/opt/openjdk/include /usr/lib/jvm/default-java/include; do
  if [ -f "$d/jni.h" ]; then JNI=$d; break; fi
done
if [ -z "$JNI" ]; then
  echo "no jni.h in any of the four places QLever's CMake looks, so the JNI" >&2
  echo "target would not be created and the build would fail later with" >&2
  echo "\"No rule to make target 'qleverjni'\". Set JAVA_HOME to a JDK, or" >&2
  echo "install one: on Ubuntu 'apt install default-jdk' also provides" >&2
  echo "/usr/lib/jvm/default-java, which CMake looks for." >&2
  exit 1
fi
echo "== jni.h from $JNI"

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

# Also outside `target`, because a later `mvn install` without this profile
# rewrites the jar without the library and leaves nothing behind, after which
# every test here skips for want of it. `QLeverNative.load()` looks here too.
CACHE=${XDG_CACHE_HOME:-$HOME/.cache}/passage-qlever/$OS-$ARCH
mkdir -p "$CACHE"
cp "$LIB" "$CACHE/"
echo "== $CACHE/$(basename "$LIB")"
# Not copied anywhere: it is a 60 MB tool, not something to ship in a jar.
echo "== $BUILD/qlever-index, for building indexes"
