# Journal

A running log of the Passage-on-QLever work: what was done, what it measured,
and what turned out to be wrong. Newest first. Dates are the day the work
landed, not the day it started.

The companion documents are `README.md` (how to build, how to measure, what the
storage looks like), `feasibility/README.md` (the FILTER study), and
`REPORT-filter-vs-offset.md` in the student-subject repository next to the
slides. This file records the decisions those documents assume.

Two repositories move together: this one (`passage`, branch
`filter-continuation`) and the QLever fork (`qlever-1`, branch `scan-distinct`).
Entries name the commit in each where it matters.

---

## 2026-10-18 — A continuation can tell that the graph moved under it

`QLeverBackend.snapshotId()` returns the id of the snapshot the backend reads;
`snapshotHasMoved()` says whether the index has since advanced past it.
(`passage` c172d744, `qlever-1` effa8239.)

QLever already had what was needed. `LocatedTriplesState::index_` is a
monotonic counter raised by every `insertTriples`, `deleteTriples` and
`addFromSnapshotDiff`, and it is already part of the query cache key, so the
engine was relying on it to be exactly the thing we wanted to read. Three
natives expose it: `snapshotId` (the pinned state a backend holds),
`currentSnapshotId` (what the delta-triples manager now publishes), and
`moveSnapshotForTesting`, which inserts one triple so a test can move the graph
on purpose.

Reading the pinned id costs 100 ns, the current one 164 ns — cheap enough to
check on every continuation rather than once per sequence.

This **detects, it does not repair.** A continuation that resumes on a moved
snapshot can be reported, refused, or restarted; it cannot be made to finish
against the state it began on. That needs the snapshot to be pinned for the
whole sequence, which is the MVCC question, deliberately left out of scope.

`aGraphThatMovesUnderAContinuationIsNoticed` in `QLeverBackendTest` opens its
own backend, because it mutates the graph. The first version of the hook
reported "not detected" and the mechanism was blamed before the plumbing was:
stderr instrumentation printed `pinned=0 current=1` on the C++ side, so the
counter had been right all along and a stale dylib copy was reading it. The
finished test was checked by neutralising `snapshotHasMoved()` to `return
false` and confirming it fails.

Still open: the same id on the HTTP path — a `meta` field, which means
`libqlever/Qlever.h` as well as the JNI bridge.

## 2026-10-17 — FILTER instead of OFFSET: tried, measured, dropped

The question was whether a triple pattern could be continued with
`FILTER (?key > last)` — keyset pagination — and so without forking QLever at
all, since the block prefilter for relational and logical expressions is
upstream already.

It works. All eight shapes of triple pattern, enumerated to exhaustion, give
exactly the OFFSET enumeration, and the cost per slice does not grow with
depth. The key has to be a lexicographic nest, not a conjunction —
`?o > O || (?o = O && (?p > P || (?p = P && ?s > S)))` — because everything
with a larger `?o` belongs to the remainder whatever its `?p`. The shape is an
L, not a rectangle. One column short of the full key loses solutions, which is
what `feasibility/filter_tp.py` measures.

It was dropped for a reason that is a property of the data, not of the method.
WatDiv objects are IRIs and plain strings — one datatype tag — and inside one
tag the Id bits order by value. QLever enumerates by the bits of the Id, and
those group **by datatype tag first**: `Bool < Int < Double < …strings and
IRIs… < Date`. On an index holding every type, interrupted after the integer
42, 18 rows remain and `FILTER(?o > 42)` returns one of them. Even within the
integers the scan yields `0, 3, 17, 42, 1000, −7` — negatives last, two's
complement — so −7 is lost. The probe reports that no `>` expression describes
a suffix of that scan.

The typed repair — a disjunction over the remaining datatype classes — is
correct and 4,000× slower: 15,400 ms against 7 ms. A conjunction is pruned by
any one conjunct, so adding `isIRI(?o)` to a `>` costs nothing; a disjunction
needs *every* branch to prune, and `DATATYPE(?x) = <t>` prunes nothing, so one
such branch turns the slice into a full scan. A key that crosses datatype
classes is necessarily a disjunction, so it necessarily pays that.

Final comparison, `?s friendOf ?o` in 4,492 slices of a thousand:

| | OFFSET + patch | FILTER |
|---|---|---|
| correctness | unconditional | homogeneous column only |
| 4,492 slices | 21,324 ms | 26,630 ms |
| mechanisms to maintain | one | two — OFFSET stays the fallback |

Slower *and* narrower. Said out loud in the slides so nobody spends a month
rediscovering it.

## 2026-10-17 — Stock versus patched OFFSET, which changed the conclusion

The detour's useful by-product. Measuring the alternative forced the comparison
against stock QLever that had been deferred three times — done by compiling the
`firstBlockAfterOffset` short-circuit behind an environment variable, so the
two runs differ in exactly one function.

| OFFSET into `?s ?p ?o` | patched | stock |
|---|---|---|
| 1,000 | 3 ms | 5 ms |
| 1,000,000 | 4 ms | 7 ms |
| 5,000,000 | 5 ms | 20 ms |
| 10,000,000 | 4 ms | 38 ms |

Stock is linear in depth, about 3.4 ms per million rows skipped, because
upstream's `pruneBlock` decompresses the whole prefix. Linear per slice is
quadratic over an enumeration.

**This reverses an earlier conclusion of mine.** I had written that the patch
was an optimisation and not an enabler. Extrapolated to UniProt's 245 billion
triples, a single resumption at 10⁹ costs about 3.5 s on stock. Passage only
terminates under a progress condition: once merely reaching the resume point
costs more than the quota, the sequence never terminates at all. On WatDiv the
patch buys a factor of two; at UniProt scale it is the difference between
terminating and not. The patch is 22 lines plus a `numRowsIsExact` guard.

## 2026-10-16 — An unknown IRI matched every triple instead of none

`getId` returned the vocabulary's sentinel for a value absent from the
dictionary, and the sentinel makes a scan match **every** triple rather than no
triple — the opposite of what the surrounding comment claimed, which is why it
survived. It now throws `NotFoundException`, the convention the other backends
use, and `anAbsentValueIsReportedAsAbsentRatherThanAsAny` holds it there.
(`qlever-1` a31314c1 corrects the comment.)

## 2026-10-10 — The port: OFFSET patch, block cache, iterator convention

- `firstBlockAfterOffset` in `CompressedRelationReader.cpp`: walk the block
  metadata to the block containing row *k* and decompress only that one.
- A per-thread block cache (`lazy-index-scan-block-cache-size`, off by
  default), keyed on reader + block index + columns. Opening a scan goes from
  0.866 ms to 0.023 ms with 16 blocks cached — worth 38× on a continuation
  sequence, because PAX means reading one row decompresses its whole 31,250-row
  group.
- The iterator convention: `BackendIterator` advances then reads. A regression
  test now pins it, verified by reverting the fix and watching it fail.
- The measurement protocol was redefined around warm backends rather than
  calling the first run a warm-up, which **reversed** the join comparison that
  had been published before it.

---

## Lessons that cost something

Kept because each one was paid for once.

| | what happened |
|---|---|
| Verify the data, not the port | Three measurements were taken against a Docker container still holding port 7019 while my own server logged "address already in use". 2.9 M triples instead of 10,916,457. Nothing is trusted now until the triple count is printed. |
| Read the test's own output | A published Blazegraph scan-open figure of 0.072 ms was measured on a 187-triple access path, because the 3-argument scan passes a null graph. The test printed "(83 subjects, 83 read)" and I had not read it. Figure withdrawn; `timeScans` now prints WITHHELD when it gets fewer subjects than it asked for. |
| One probe is not a measurement | Order selection by a single probe point accepted `(o,s,p)` over `(o,p,s)` and silently lost 28,927 solutions out of 10,916,457. Five probe points now. |
| QLever's vocabulary is ICU-collated | Inferring the scan order by comparing strings in Python failed on every pattern. The order is ICU collation, not bytes. |
| `xsd:decimal` is not `xsd:double` | The result JSON tags 0.5 as decimal while `DATATYPE()` answers double. Picking the datatype class from the JSON gave the wrong class and an infinite continuation loop. |
| Check the edit landed | I reported a cross-reference corrected when the replacement had been a no-op against markup inside the text. Edits are verified on disk now. |
