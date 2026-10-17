# Continuing a triple pattern with FILTER instead of OFFSET

A feasibility probe for an approach that needs **no change to QLever at all**:
instead of resuming a scan with `OFFSET k`, the continuation carries the last
solution emitted and asks for everything after it.

```sparql
# paused after ⟨S, P, O⟩, scanning in (o, p, s) order
SELECT * WHERE { ?s ?p ?o .
  FILTER ( ?o > O || (?o = O && (?p > P || (?p = P && ?s > S))) ) }
```

This is keyset pagination. It works here because QLever already prunes blocks
from `CompressedBlockMetadata` for relational and logical expressions —
`src/engine/sparqlExpressions/PrefilterExpressionIndex.h`, which is **upstream**,
present at the fork point. Nothing in `passage-qlever/qlever` is needed for it.

```sh
~/qlever-1/build/qlever-server -i watdiv10m -p 7731 -j 4 &
python3 -I filter_continuation.py http://localhost:7731 50000
```

## It works

WatDiv 10M, every pattern enumerated to exhaustion, both ways, compared as a
rolling hash of the whole sequence:

| pattern | solutions | slices | by FILTER | by OFFSET | |
|---|---|---|---|---|---|
| `?s a ?c` | 136,215 | 3 | 505 ms | 500 ms | identical |
| `?s friendOf ?o` | 4,491,142 | 90 | 28,542 ms | 17,479 ms | identical |
| `?s ?p ?o` | 10,916,457 | 219 | 66,886 ms | 65,935 ms | identical |

**Identical** means the same solutions in the same order, start to finish — no
gap, no duplicate, nothing re-read.

In the regime Passage actually runs in — many small slices — the overhead is
about **one millisecond per slice**, and it does not grow with depth:

| slice size | slices | by FILTER | by OFFSET | slowest slice |
|---|---|---|---|---|
| 50,000 | 3 | 505 ms | 500 ms | 186 / 184 ms |
| 1,000 | 137 | 749 ms | 562 ms | 12 / 6 ms |
| 200 | 682 | 1,350 ms | 701 ms | 4 / 3 ms |

The slowest slice shrinks as slices shrink, which is the point: **the prefilter
holds all the way down the scan**, so a continuation at depth ten million costs
what one at depth ten costs. An engine that re-read the skipped prefix would
show the opposite.

Note that the OFFSET column is measured on the patched fork. Against stock
QLever the comparison may look different, and that is the next thing to
measure.

## Three things it taught us

**The key must be in the permutation's column order, not the query's.** For
`?s ?p ?o` only `(o, p, s)` resumes in the right place; the other five orders
resume somewhere else entirely, silently. The probe therefore *chooses* the
order empirically — it tries each one against the second OFFSET slice and keeps
the one that lands on the same solution. A real implementation does not need
this: the paused scan knows which permutation it was reading.

**You cannot infer that order by comparing strings.** The first version of this
probe did, and failed on every pattern. QLever sorts its vocabulary by ICU
collation — `"locale": {"language": "en", "country": "US"}` in the index's
`meta-data.json` — so the Id order is not the byte order of the terms. A client
holding only the solutions cannot reconstruct it.

**The comparison must not be wrapped.** `FILTER(?s > <iri>)` is prefiltered and
takes 3 ms; `FILTER(STR(?s) > "…")` is not, and takes 171 ms. The prefilter
only recognises a relational expression applied directly to the variable.

## What this would change

The approach asks less of a backend than the offset approach does. It needs
neither an exact cardinality nor `skip(offset)` — only the last solution
emitted, which the engine necessarily has. That removes the most expensive
operation measured on this backend: an exact cardinality costs 766 µs to 1 ms
per call, against 0.2 µs for the metadata estimate.

The block cache stays useful: it serves scan *opening* inside nested-loop
joins, which is a different problem from resumption.

## What is not settled

- The plan for the continuation query may differ from the plan for the
  original. The key stays correct either way — it selects the same set — but if
  the filtered column is no longer the scan's leading column, the prefilter
  stops biting and the slice falls back to a full scan. Not observed here;
  worth forcing.
- In a quad store the same triple can sit in several graphs and share a key, so
  a strict `>` would skip every copy. The graph has to enter the key.
- QLever gives a total order across datatypes where SPARQL raises a type error:
  `FILTER(?o > "M")` and `FILTER(!(?o > "M"))` partition the 10,916,457
  solutions exactly, with no loss. The continuation is therefore QLever-shaped.
  For this use case that is fine — a continuation is addressed back to the
  server that issued it — but it is not a portable SPARQL query.
