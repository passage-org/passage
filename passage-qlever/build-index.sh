#!/usr/bin/env bash
#
# Build a QLever index suited to Passage, which is not what QLever's own
# defaults are tuned for.
#
# What it does differently from calling `qlever-index` yourself is little: it
# pipes a `.gz` file in, which `qlever-index` parses as compressed bytes rather
# than decompressing, and it guesses the format from the name.
#
#   ./build-index.sh <basename> <data file> [format]
#
# `format` is QLever's `-F`: nt, ttl, nq or trig.
#
# The block size is QLever's default, which is the right one *provided*
# `QLeverBackend` keeps its cache of decompressed blocks. Without that cache a
# block of 250 kB per column is decompressed once per lookup, and smaller blocks
# are a worthwhile workaround — ROWS_PER_BLOCK=1000 made the join sixteen times
# faster. With the cache the opposite holds, since one decompressed block then
# serves thousands of lookups: large blocks reach 1.23M solutions/s against
# 1.04M for 1,000-row ones, on a smaller index. See README.md.
#
set -euo pipefail

BASENAME=${1:?usage: build-index.sh <basename> <data file> [format]}
DATA=${2:?usage: build-index.sh <basename> <data file> [format]}
ROWS=${ROWS_PER_BLOCK:-}

HERE=$(cd "$(dirname "$0")" && pwd)
INDEXER=$HERE/qlever/build/qlever-index
[ -x "$INDEXER" ] ||
  { echo "no $INDEXER; run: mvn -pl passage-qlever install -Pnative" >&2; exit 1; }
[ -f "$DATA" ] || { echo "no such file: $DATA" >&2; exit 1; }

if [ $# -ge 3 ]; then
  FORMAT=$3
else
  case ${DATA%.gz} in
    *.nt) FORMAT=nt ;; *.ttl) FORMAT=ttl ;;
    *.nq) FORMAT=nq ;; *.trig) FORMAT=trig ;;
    *) echo "cannot guess the format of $DATA; pass it as the third argument" >&2
       exit 1 ;;
  esac
fi

BLOCK_SIZE=()
[ -z "$ROWS" ] || BLOCK_SIZE=(--index-rows-per-block "$ROWS")

mkdir -p "$(dirname "$BASENAME")"
echo "== $BASENAME from $DATA ($FORMAT), ${ROWS:-default} rows per block"
if [ "${DATA##*.}" = gz ]; then
  # `qlever-index` parses the compressed bytes rather than decompressing, so
  # the data goes in over stdin.
  gzip -dc "$DATA" |
    "$INDEXER" -i "$BASENAME" -f - -F "$FORMAT" -p true "${BLOCK_SIZE[@]}"
else
  "$INDEXER" -i "$BASENAME" -f "$DATA" -F "$FORMAT" -p true "${BLOCK_SIZE[@]}"
fi
echo "== $BASENAME"
