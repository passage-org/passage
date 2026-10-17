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

## The triple pattern is the primitive, not the BGP

Passage builds a BGP continuation as a **union over the pause points**: the
rest of the current inner scan, then the rest of the outer one with fresh
inner scans. So a keyset applied to a whole BGP's result is not a continuation
— it is pagination of a result set, and it answers a different question.

What has to work is the scan: given a triple pattern, the solutions already
emitted and the last of them, produce a query for exactly the rest. If that
holds for a triple pattern, Passage composes it into BGP, OPTIONAL and the
rest the way it already composes offsets.

`filter_tp.py` therefore runs **every shape of triple pattern** — each
combination of bound and free positions — and enumerates each one to
exhaustion, both ways, comparing a rolling hash of the whole sequence.

```sh
~/qlever-1/build/qlever-server -i watdiv10m -p 7731 -j 4 &
python3 -I filter_tp.py http://localhost:7731 20000
```

## It works, on all eight shapes

WatDiv 10M, slices of 20,000:

| shape | solutions | slices | by FILTER | by OFFSET | order | |
|---|---|---|---|---|---|---|
| `? ? ?` | 10,916,457 | 546 | 70,197 ms | 67,553 ms | (o, p, s) | identical |
| `? p ?` | 4,491,142 | 225 | 18,670 ms | 17,736 ms | (s, o) | identical |
| `? ? o` | 58,629 | 3 | 225 ms | 219 ms | (s, p) | identical |
| `s ? ?` | 319 | 1 | 2 ms | 2 ms | (p, o) | identical |
| `s p ?` | 213 | 1 | 0 ms | 0 ms | (o) | identical |
| `s ? o` | 1 | 1 | 0 ms | 0 ms | (p) | identical |
| `? p o` | 0 | 1 | 0 ms | 0 ms | (s) | identical |
| `s p o` | 0 | — | — | — | — | nothing to continue |

**Identical** means the same solutions in the same order, start to finish — no
gap, no duplicate, nothing re-read. The overhead against OFFSET is 1 to 5%,
and the OFFSET column is measured on the *patched* fork, so the honest
comparison against stock QLever is still to do.

The cost per slice does not grow with depth. On `?s a ?c` with slices of 200,
682 of them, the slowest slice is 4 ms — the prefilter holds all the way down,
so a resumption at depth ten million costs what one at depth ten costs. An
engine that re-read the skipped prefix would show the opposite.
`filter_continuation.py` holds that slice-size study.

## The key must name every ordered column

A key one column short silently drops the solutions sharing the boundary's
prefix. Measured at half depth on each shape:

| shape | full key | one column short |
|---|---|---|
| `? ? ?` | exact | `(o, p)` **−54**, `(o)` **−94** |
| `? p ?` | exact | `(s)` **−18** |
| `s ? ?` | exact | `(p)` **−144** |
| `? ? o` | exact | `(s)` exact — here `(o, s)` happens to be unique |

Fifty-four out of five million passes any smoke test, which is what makes it
dangerous. Same failure shape as the iterator-convention bug in this module,
which also lost ninety-four.

Because ⟨s, p, o⟩ is unique in a triple store, the full key resumes at exactly
one place: no ambiguity, no run of equal keys to re-read or skip. In a quad
store that stops being true — see below.

## Why a nest of disjunctions, and not a conjunction of `>`

A conjunction describes a **rectangle**; what is needed is a **suffix** of the
lexicographic order, which is an L. Everything with `s > S` belongs to the
rest *whatever its tag*, including tags smaller than the boundary's. Resuming
`?s a Genre8 . ?s og:tag ?tag` after ⟨SubGenre125, Topic56⟩, where three
solutions should remain:

| | returned |
|---|---|
| `?s > S \|\| (?s = S && ?tag > T)` | **3** — exact |
| `?s >= S && ?tag > T` | 1 — two lost |
| `?s > S && ?tag > T` | 1 — two lost |

The two lost are `SubGenre57/Topic155` and `SubGenre57/Topic188`: their
subject is after the boundary, but their tag sorts *before* `Topic56` because
`'1' < '5'`. Only the last component of the key takes a bare `>`; every
earlier one needs "greater, or equal and keep looking".

## Three things the probe had to learn

**The column order is the permutation's, not the query's.** For `? ? ?` only
`(o, p, s)` is right; the other five resume elsewhere.

**One probe point is not enough to find that order.** An earlier version
validated a candidate order against a single boundary, accepted `(o, s, p)`,
and lost **28,927 solutions out of 10,916,457** — silently, with every slice
looking healthy. It now validates against five boundaries spread through the
scan. A real implementation needs none of this: the paused scan knows which
permutation it was reading.

**You cannot infer the order by comparing strings.** The first version tried,
and failed on every shape. QLever sorts its vocabulary by ICU collation —
`"locale": {"language": "en", "country": "US"}` in the index's
`meta-data.json` — so the Id order is not the byte order of the terms.

And one rule for generating the query: **the comparison must not be wrapped.**
`FILTER(?s > <iri>)` is prefiltered and takes 3 ms; `FILTER(STR(?s) > "…")` is
not, and takes 171 ms.

## What this would change

The approach asks less of a backend than the offset approach does. It needs
neither an exact cardinality nor `skip(offset)` — only the last solution
emitted, which the engine necessarily has. That removes the most expensive
operation measured on this backend: an exact cardinality costs 766 µs to 1 ms
per call, against 0.2 µs for the metadata estimate.

The block cache stays useful: it serves scan *opening* inside nested-loop
joins, which is a different problem from resumption.

## What is not settled

- Composing this into a BGP continuation — the union over pause points — is
  not tested here. Only the primitive is.
- The plan for the continuation query may differ from the plan for the
  original. The key stays correct either way, but if the filtered column is no
  longer the scan's leading column the prefilter stops biting.
- In a quad store the same triple can sit in several graphs and share a key,
  so a strict `>` would skip every copy. The graph has to enter the key.
- QLever gives a total order across datatypes where SPARQL raises a type
  error: `FILTER(?o > "M")` and `FILTER(!(?o > "M"))` partition the 10,916,457
  solutions exactly. The continuation is therefore QLever-shaped — fine for
  this use case, since a continuation goes back to the server that issued it,
  but it is not a portable SPARQL query.
