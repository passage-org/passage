#!/usr/bin/env bash
#
# Build a QLever index suited to Passage, which is not what QLever's own
# defaults are tuned for.
#
# A block holds 250 kB per column, so reading a single triple decompresses up
# to a megabyte. That is the right trade for QLever's engine, which scans many
# rows at a time, and the wrong one for Passage, whose nested-loop joins open a
# scan per outer solution and read a handful of rows from each. Measured on
# WatDiv 10M, 1,000 rows per block instead of the default 31,250 makes opening
# a scan 19 times cheaper — 0.048 ms against 0.915 — and the join sixteen times
# faster, for five percent more disk. See README.md.
#
#   ./build-index.sh <basename> <data file> [format]
#
# `format` is QLever's `-F`: nt, ttl, nq or trig, guessed from the name when
# left out. A `.gz` file is decompressed on the way in, which `qlever-index`
# does not do for itself.
#
# ROWS_PER_BLOCK=n overrides the block size. Smaller is faster per scan and
# larger on disk: 100 rows gives 0.016 ms per scan and doubles the index.
#
set -euo pipefail

BASENAME=${1:?usage: build-index.sh <basename> <data file> [format]}
DATA=${2:?usage: build-index.sh <basename> <data file> [format]}
ROWS=${ROWS_PER_BLOCK:-1000}

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

mkdir -p "$(dirname "$BASENAME")"
echo "== $BASENAME from $DATA ($FORMAT), $ROWS rows per block"
if [ "${DATA##*.}" = gz ]; then
  # `qlever-index` parses the compressed bytes rather than decompressing, so
  # the data goes in over stdin.
  gzip -dc "$DATA" |
    "$INDEXER" -i "$BASENAME" -f - -F "$FORMAT" -p true \
               --index-rows-per-block "$ROWS"
else
  "$INDEXER" -i "$BASENAME" -f "$DATA" -F "$FORMAT" -p true \
             --index-rows-per-block "$ROWS"
fi
echo "== $BASENAME"
