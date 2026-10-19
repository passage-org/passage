#!/usr/bin/env bash
#
# QLever's own engine against this backend, on the same join and the same data.
#
#   ./bench-engine.sh <index basename>        # e.g. $HOME/bench-watdiv/qlever/watdiv10m
#
# Why this exists: the comparison in README.md pits Passage-on-QLever against
# Passage-on-Blazegraph, and never against QLever's own engine. That last one
# is what says how much the continuation machinery costs, and it is a different
# question from how fast the storage is.
#
# Three things this script is careful about, each of which gave a wrong answer
# first:
#
#   * A FRESH SERVER PER RUN. QLever caches query results, so three runs
#     against one server measured 62, 19 and 22 ms: the last two were the
#     cache answering.
#
#   * `Accept: application/octet-stream`, the binary export of the `IdTable`.
#     Not TSV: TSV resolves three IRIs per row through the vocabulary, and the
#     benchmark it is compared against never does — `QLeverVsBlazegraphTest`
#     counts `BackendBindings`, which resolve a string only when asked, and it
#     never asks. Measured in TSV, the same query reads six times SLOWER than
#     Passage, and what it measures is a text formatter.
#
#   * NOT `COUNT(*)`. A count produces no bindings, so it is not what Passage
#     does. Counted, this join takes 70 ms; with its rows actually produced,
#     438 ms. The difference is the whole point.
#
# `qlever-server` is not built by `build-native.sh`, which builds only the
# bridge and the indexer:
#
#   cmake --build qlever/build --target qlever-server -j $(nproc)
#
set -euo pipefail

IDX=${1:?usage: bench-engine.sh <index basename>}
HERE=$(cd "$(dirname "$0")" && pwd)
SRV=${QLEVER_SERVER:-$HERE/qlever/build/qlever-server}
TYPE='<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
EXPECTED_TRIPLES=${EXPECTED_TRIPLES:-10916457}

[ -x "$SRV" ] || { echo "no $SRV; build the qlever-server target" >&2; exit 1; }
[ -f "$IDX.index.pso" ] || { echo "no QLever index at $IDX" >&2; exit 1; }

JOIN="SELECT * WHERE { ?s $TYPE ?c . ?s ?p ?o }"

# A server of its own, killed before returning, so that nothing is cached
# between runs and nothing is left listening afterwards.
with_server() {  # $1 = a function to call with the port
  local port=$((7800 + RANDOM % 120))
  if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null 2>&1; then
    with_server "$@"   # that one is taken, draw another
    return
  fi
  "$SRV" -i "$IDX" -p "$port" -j 4 -m 4G > "/tmp/qlever-bench-$port.log" 2>&1 &
  local pid=$!
  local waited=0
  until curl -s -m 2 "http://localhost:$port/" -X POST --data-urlencode \
        'query=SELECT * WHERE { ?s ?p ?o } LIMIT 1' >/dev/null 2>&1; do
    sleep 1
    waited=$((waited + 1))
    [ $waited -lt 60 ] || { echo "server did not come up; see /tmp/qlever-bench-$port.log" >&2
                            kill $pid 2>/dev/null || true; exit 1; }
  done
  "$1" "$port"
  kill $pid 2>/dev/null || true
  wait $pid 2>/dev/null || true
}

ask() {  # $1 = port, $2 = query, $3 = accept
  curl -s -X POST -H "Accept: $3" --data-urlencode "query=$2" "http://localhost:$1/"
}

# First, because a server answering from the wrong index looks exactly like one
# answering from the right index. This is not hypothetical: three measurements
# were once taken against a container holding the port, on a tenth of the data.
check_index() {
  local n
  n=$(ask "$1" 'SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }' \
          'text/tab-separated-values' | tail -1 | tr -d '"')
  echo "== $n triples in the index (expected $EXPECTED_TRIPLES)"
  [ "$n" = "$EXPECTED_TRIPLES" ] || { echo "WRONG INDEX, stopping" >&2; exit 1; }
}

show_plan() {
  echo "== the query, and the plan the server chose for it"
  echo "   $JOIN"
  ask "$1" "$JOIN LIMIT 1" 'application/qlever-results+json' |
    python3 -I -c '
import json, sys
tree = json.load(sys.stdin)["runtimeInformation"]["query_execution_tree"]
def walk(node, depth=0):
    print("   " + "  " * depth + node.get("description", "?").split(",")[0])
    for child in node.get("children", []):
        walk(child, depth + 1)
walk(tree)'
}

# `rows = bytes / 8 / columns` is also the integrity check: four variables at
# eight bytes each, so the byte count has to divide exactly by 32.
timed() {  # $1 = port, $2 = query, $3 = columns, $4 = label
  local start bytes end ms rows
  start=$(date +%s%N)
  bytes=$(ask "$1" "$2" 'application/octet-stream' | wc -c | tr -d ' ')
  end=$(date +%s%N)
  ms=$(( (end - start) / 1000000 ))
  rows=$(( bytes / 8 / $3 ))
  printf "   %-16s %10d rows  %6d ms  %12d /s\n" "$4" "$rows" "$ms" \
         "$(( rows * 1000 / (ms > 0 ? ms : 1) ))"
}

with_server check_index
with_server show_plan

echo "== the join, rows actually produced, raw Ids, a fresh server per run"
for i in 1 2 3; do
  run() { timed "$1" "$JOIN" 4 "whole join $i"; }
  with_server run
done
for i in 1 2 3; do
  run() { timed "$1" "$JOIN LIMIT 200000" 4 "first 200k $i"; }
  with_server run
done

cat <<'NOTE'

== to compare against, from README.md, same join and same data
   Passage on QLever     10,332,352 solutions   4,363 ms   (a JVM of its own)
   Passage on Blazegraph 10,332,352 solutions   3,198 ms   (a JVM of its own)

   The figures above carry the HTTP transfer of 330 MB on loopback, which
   Passage does not pay, so they flatter Passage rather than QLever.
NOTE
