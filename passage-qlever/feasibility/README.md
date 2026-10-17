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

## Where it breaks: a column that is not one datatype

Everything above holds on WatDiv because every object there is an IRI or a
plain string — 9,595,097 and 1,321,360 of them, no numerics, no dates. Those
all live under one datatype tag, where the Id bits order by string value, so
the storage order and SPARQL's `>` agree.

They are not the same order in general. QLever sorts by the bits of the Id,
which group by datatype tag first:

    Bool < Int < Double < …VocabIndex: strings and IRIs… < Date

`mixed.ttl` and `mixed_datatypes.py` build a 24-triple index that holds all of
them. Its scan order comes out as booleans, then integers, then decimals, then
strings and IRIs, and dates **last** — after the IRIs. Interrupt it after the
sixth row, the integer 42: eighteen rows remain, and `FILTER(?o > 42)` returns
**one**.

Worse, one datatype is not enough either. The integers enumerate as

    0, 3, 17, 42, 1000, −7

because the 60-bit payload is two's complement, so negatives land after the
positives. `FILTER(?o > 42)` keeps `1000` and **loses −7**, which the scan had
not yet emitted.

Run on that index, `filter_tp.py` reports `? ? ?` as **NO ORDER RESUMES
CORRECTLY** — there is no permutation to choose, because no `>` expression
describes a suffix of that scan.

So the approach is sound exactly when the key's leading column is
datatype-homogeneous and free of negative numerics. That covers a great many
real patterns — any scan keyed on subjects, and on objects in a graph like
WatDiv — and it covers none of the rest. A backend using it would have to know
which case it is in, and fall back to offsets otherwise.

## The repair, and why it does not pay

The scan order is the Id order, so say the continuation in that order: what is
left of the boundary's own datatype class, then every class after it.

    (DATATYPE(?o) = xsd:int && (?o > 42 || ?o < 0))
    || DATATYPE(?o) = xsd:double
    || isIRI(?o) || DATATYPE(?o) = xsd:string || LANG(?o) != ""
    || DATATYPE(?o) = xsd:date

`typed_key.py` builds that automatically and it is **correct**: on the mixed
index it enumerates all 24 solutions in nine slices, identical to OFFSET.

It is also **four thousand times slower**. Resuming `?s ?p ?o` at depth five
million on WatDiv:

| filter | time | prefiltered |
|---|---|---|
| `?o > O` | 7 ms | yes |
| `?o > O && isIRI(?o)` | 3 ms | yes |
| `?o > O \|\| ?o < -99999` | 4 ms | yes |
| `?o > O \|\| isIRI(?o)` | 4 ms | yes |
| `isIRI(?o)` alone | 8 ms | yes |
| `?o > O \|\| DATATYPE(?o) = xsd:date` | **5,136 ms** | **no** |
| `DATATYPE(?o) = xsd:date` alone | **11,444 ms** | **no** |
| the full typed key | **15,400 ms** | **no** |

The rule behind it: a conjunction can be pruned by any one of its conjuncts,
so adding a type test to a `>` costs nothing. A disjunction needs *every*
branch to prune, and `DATATYPE(?x) = <t>` prunes nothing — so one such branch
turns the whole scan into a full read. The repair is necessarily a
disjunction, so the repair necessarily pays that.

The `isDatatype` family does prune — `isIRI` and `isLiteral` are fast, and
upstream has been adding more of them, most recently `ql:isGeoPoint`. But
those cannot separate dates from strings, or booleans from numbers, so they
cover only some boundaries.

**So: correct, or fast, and on a datatype-mixed column not both.** On a
homogeneous column — every scan keyed on subjects, and objects in a graph like
WatDiv — plain `>` is both, and that is a large and useful class. Outside it,
the offset is still the only thing that is both.

The constructive path, if this is worth pursuing: a block prefilter for the
datatype tests, proposed upstream the way `ql:isGeoPoint` was. That is a
change to QLever, which is what this approach set out to avoid — but it is a
much smaller and more general change than the OFFSET patch.

## Measured against stock QLever, at last

Every OFFSET figure above was taken on the patched fork, which was not an
honest comparison. Upstream handles an offset by decompressing every block
before it and discarding the rows — `pruneBlock` in
`CompressedRelationReader.cpp` — so its cost grows with depth:

| OFFSET into `?s ?p ?o` | patched | stock |
|---|---|---|
| 1,000 | 3 ms | 5 ms |
| 1,000,000 | 4 ms | 7 ms |
| 5,000,000 | 5 ms | 20 ms |
| 10,000,000 | 4 ms | 38 ms |

Linear per slice means quadratic over a whole enumeration. Running
`?s friendOf ?o` — 4,491,142 solutions — to exhaustion in 4,492 slices of a
thousand:

| | patched fork | **stock upstream** |
|---|---|---|
| by FILTER | 26,630 ms | **26,952 ms** |
| by OFFSET | 21,324 ms | **42,856 ms** |
| slowest slice, by OFFSET | 14 ms | 114 ms |

So on stock QLever the FILTER continuation is **1.6× faster than the OFFSET
one**, and it is the OFFSET that degrades with depth, not the FILTER. The fork
buys another 1.3× on top of FILTER, and lifts the datatype restriction.

Which answers the question this started from. Without touching QLever a
continuation is not merely possible — there are two ways to do it, and the
FILTER is the better one wherever its precondition holds. The patch is an
optimisation and a generalisation, not an enabler.

(The measurement was taken by short-circuiting `firstBlockAfterOffset` behind
an environment variable and rebuilding, so the two runs differ in exactly that
function. The switch is not committed.)

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
