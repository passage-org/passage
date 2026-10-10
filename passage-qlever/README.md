# Passage ⨯ QLever

[QLever](https://github.com/ad-freiburg/qlever) keeps its triples in six
permutations, each a column store cut into compressed blocks, and holds one
metadata record per block in RAM: that block's first and last triple, and its
number of rows.

That metadata is what makes a continuation affordable. Where Blazegraph reaches
an offset through *augmented* balanced trees, QLever reaches it by adding up row
counts it already has in memory and decompressing only the block the offset
falls in. In other terms, it makes efficient:

- [X] `SELECT <projected> WHERE {<triple_pattern>} OFFSET X`, i.e., jumping at
  an integer offset when the query is a triple pattern. On a relation spanning
  640 blocks, a deep offset costs what a shallow one costs, against 32 ms per
  query when the blocks have to be read and discarded;

- [X] `SELECT COUNT(*) WHERE {<triple_pattern>}`, i.e., provide the number of
  elements corresponding to the triple pattern, from the metadata alone unless
  a block has to be read to settle duplicates that differ only in their graph;

- [X] `SELECT DISTINCT ?x WHERE {<triple_pattern>}` on a leading component,
  which a column store answers by walking one column and stepping over equal
  runs, with no sorting and no hash table.

The first of these needs a change that upstream QLever does not carry, so the
`qlever` submodule is pinned at a commit that has it. Its absence changes how
long a scan takes and never what it returns, which means no test here can
detect it; the submodule pointer is the only record of which QLever this was
built against.

Scans cross into the JVM through a JNI bridge, so this module needs a shared
library built from the `qlever` submodule. It is native code for one platform at
a time, and it is QLever's to build rather than ours, so it is not shipped in
this jar and the tests here skip rather than fail when it is absent: the module
has to build, and `mvn test` has to stay green, on a machine that has never
heard of QLever.

## From a clone to passing tests, in one paste

Everything below is one block, meant to be pasted into a terminal as it is.
The rest of this document explains what each part does and how to vary it.

```bash
cd ~/passage                                   # wherever the clone is
export JAVA_HOME=$(brew --prefix openjdk@21)   # Maven needs a 21; see below

# The JNI bridge, built from the pinned QLever. About ten minutes the first
# time, and it leaves ~2.5 GB under passage-qlever/qlever/build.
git submodule update --init passage-qlever/qlever
mvn -pl passage-qlever install -Pnative -DskipTests

# An index to test against: the six triples of Figure 2 of the Passage paper.
# It goes outside the repository, since `mvn clean` would take the whole
# target directory with it. The path is spelled out rather than held in a shell
# variable, so that each command below stands on its own.
mkdir -p $HOME/qlever-fig2
cat > $HOME/qlever-fig2/fig2.ttl <<'TTL'
<http://ex.org/H1> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H2> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H3> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O1> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O2> .
<http://ex.org/H3> <http://ex.org/occupation> <http://ex.org/O3> .
TTL
passage-qlever/build-index.sh $HOME/qlever-fig2/fig2 $HOME/qlever-fig2/fig2.ttl

# The backend's own tests, then the continuation test on the engine side.
mvn -pl passage-qlever,passage-volcano test \
    -Dtest='QLeverBackendTest,QLeverContinuationTest' -DfailIfNoTests=false \
    -Dqlever.test.index=$HOME/qlever-fig2/fig2
```

That ends with `Tests run: 5` and `Tests run: 2`. A `Tests run: 0` means the
tests aborted rather than ran, and the line above it, `SKIPPING <class>: ...`,
says why — most often an index that was not named, since an empty
`-Dqlever.test.index=` counts as naming none.

The library and the index both persist, so coming back to this later needs
only the last command, which is why it carries no shell variable:

```bash
cd ~/passage
mvn -pl passage-qlever,passage-volcano test \
    -Dtest='QLeverBackendTest,QLeverContinuationTest' -DfailIfNoTests=false \
    -Dqlever.test.index=$HOME/qlever-fig2/fig2
```

## Showing continuations from the command line

`passage-cli` recognises a QLever index by its `.index.pso` file, so the
basename goes to `-d` like any other database. `-l` caps the number of scans
per execution and `-r` prints the continuation query that each pause produces:

```bash
java -jar passage-cli/target/passage.jar \
    -d $HOME/qlever-fig2/fig2 \
    -q 'SELECT * WHERE { ?human <http://ex.org/type> <http://ex.org/Human> .
                         ?human <http://ex.org/occupation> ?occupation }' \
    -l 1 -r
```

which stops after one scan and prints

```sparql
SELECT * WHERE {
  { SELECT * WHERE { ?human <http://ex.org/type> <http://ex.org/Human> }
    OFFSET 1 }
  ?human <http://ex.org/occupation> ?occupation }
```

Adding `--loop` feeds each continuation back in until the query is done, which
on this graph takes five pauses for three results, every continuation a `UNION`
between what is already bound and a `SELECT ... OFFSET n` over the rest. That
is Example 3.10 of the Passage paper, over a QLever index.

The jar comes from `mvn -pl passage-cli -am install -DskipTests`, and carries
the native library through its dependency on this module, so nothing has to be
pointed at.

## Against Blazegraph, on WatDiv 10M

`QLeverVsBlazegraphTest` in `passage-volcano` compares a QLever index and a
Blazegraph journal holding the same 10,916,457 triples, as backends of the same
engine, on `SELECT * WHERE { ?s a ?c . ?s ?p ?o }`. Measured on 2026-10-10,
Apple M-series, 16 GB; the Blazegraph journal is the pre-ingested one from
[Zenodo](https://zenodo.org/records/12744414) that the root `README.org` points
at.

They agree. The query restricted to `wsdbm:Genre8`, the least populated class,
has eight solutions, identical answer for answer on both.

They do not perform alike, and the interesting part is that it cuts both ways:

| first 200,000 solutions of | QLever | Blazegraph |
|---|---|---|
| `?s ?p ?o`, one scan | **628,930 /s** | 552,486 /s |
| `?s a ?c`, one scan | **3,322,317 /s** | 1,184,478 /s |
| the join above | 24,000 /s | **732,600 /s** |

Per solution QLever is ahead — 1.1x on an unbound scan and 2.8x on a bound
predicate, which is what a column store cut into compressed blocks should give.
On the join it is 30x behind, and the reason is not per-solution work but what
it costs to *open* a scan. A nested-loop join opens one for every outer
solution, and `theCostOfOpeningAScan` measures that directly, binding a subject
and reading one solution from the result:

| | per scan opened |
|---|---|
| Blazegraph | **0.072 ms** |
| QLever | 0.861 ms |

Twelve times more expensive, which a join multiplies by its number of outer
solutions.

That number is measured, not derived: deriving it from the join's timing gives
3.16 ms, because the first 200,000 join solutions come from the subjects with
the most triples rather than from an average number of them. The direct figure
is also consistent with the 0.416 ms that `SearchThenSkipBench` measures in the
QLever fork on a smaller index.

### It is the first block being decompressed, every time

Opening a scan decompresses a block, and nothing keeps it. Three measurements
place that, two of them negative. It is not the index size: 0.853 ms over a
million triples, 0.861 over eleven million. It is not the prefetch machinery,
which was the likelier guess — sweeping `lazy-index-scan-num-blocks-in-flight`
from 20 to 1 moves it from 0.856 ms to 0.860. And it is not the JVM boundary:
in C++, constructing a cursor costs 0.0004 ms and its first read 0.933.

It is the block. A block holds 250 kB per column, so one triple can cost a
megabyte of Zstd, and asking for the *same* subject two thousand times still
costs 0.919 ms each — that one block decompressed two thousand times over,
because QLever caches query results and vocabulary lookups but not decompressed
blocks.

Which is a design choice rather than an oversight, and the right one for its
own engine: a merge join never returns to a block, so a cache would be pure
overhead. Passage cannot merge-join — a continuation has to stop mid-join and
resume from a query — so it nests loops and lands in the same block thousands
of times running.

### Keeping a few blocks, which this backend does

`lazy-index-scan-block-cache-size` keeps a few decompressed blocks per thread.
It is zero in QLever and `QLeverBackend` sets it to 16, which
`-Dqlever.block.cache` overrides. On the join, first 200,000 solutions, median
of five timed runs after a warm-up:

| blocks kept | solutions/s |
|---|---|
| 0 | 24,384 |
| 1 | 1,081,081 |
| 4 | 1,136,363 |
| **16** | **1,418,439** |

Fifty-eight times faster than without. A single entry already gets most of it,
since a block spans thousands of consecutive subjects and they arrive in order;
more help because a join has more than one scan position live at once.

It changes no answer, which is checked rather than assumed:
`theBlockCacheChangesNoAnswer` runs the join both ways and compares twenty
thousand solutions one by one. A block is cached only when its postprocessing
is a no-op — no located triples and `numRowsIsExact` for the graph filter —
which makes its contents a pure function of its index and the columns read,
since the base index never changes and updates live in the located triples.

### A bug this found, and the convention behind it

Run to completion, the join yielded **10,332,446** solutions through this
backend where Blazegraph and QLever's own engine both say **10,332,352**. The
cause was a convention, and it is worth recording because nothing in the
`Backend` interface states it.

`BlazegraphIterator` and `HDTIterator` both *fetch* in `next()`: the iterator
starts before the first match and a consumer advances before reading.
`QLeverIterator` did the opposite — it started on the first match, and `next()`
left it — which is self-consistent and what its own tests checked. But
`SpliteratorScan` calls `next()` before reading anything, so every scan lost its
first match and, at the end, yielded its last one twice. The totals matched,
which is why no count ever caught it; the join was off by 94 only because the
two affected subjects happened to have 7 and 101 triples.

`current()` was shifted for the same reason. Blazegraph increments its offset
and then fetches, so `current()` is the number of matches consumed — the offset
to resume a continuation from — and `previous()` is the offset of the match in
hand. Ours reported the offset of the match in hand as `current()`. The two
shifts cancelled under our own convention, which is exactly why
`offsetRoundTripsThroughAFreshCursor` passed throughout.

What found it was not a test but a second implementation of the same semantics,
on data large enough for a one-row error to exist.
`WhichSolutionsDiffer` counts solutions per value of a variable and diffs the
two backends; `ColdStart` runs a query to completion in a fresh process, which
is what exposed it, since the capped benchmark measured 2% of the query and the
error cancelled inside that prefix.

Fixed, both backends now return 10,332,352 and no value of `?s` differs. The
tests that encoded the wrong convention are rewritten, and one was added:
a scan must yield exactly as many matches as its cardinality promises.

### Why not `passage-cli -r`

The CLI reports a total execution time, and it cannot be used for this. Its
consumer logs every solution, inside the region it times, so on the join it
reports **147,679 ms** for work that takes about 4,200 — thirty-five times its
own logging. Silencing the log does not help, because the results go through
`log.info` and the report through `log.debug` on the same logger: any level that
shows the report also logs ten million solutions.

Two things to know if you time the CLI from outside instead. Its jar embeds
**logback**, not slf4j-simple, so `-Dorg.slf4j.simpleLogger.defaultLogLevel`
is ignored; it takes `-Dlogback.configurationFile` with a `WARN` root. And
`time` around `java -jar` includes JVM startup and opening the store, which is
a legitimate thing to measure but is not the query: `ColdStart` separates them,
and opening costs about 390 ms for QLever and 490 for Blazegraph.

Silenced and timed from outside, the CLI agrees with `ColdStart`: 4,787 to
4,942 ms for QLever against its 4,836 ms since JVM start.

### The comparison, in a fresh process

Each query run to completion, in a JVM of its own so that neither backend
inherits a compiled pipeline from the other. The page cache is warm — purging
it needs privileges — so this is a cold process against a hot disk, which is
the state a restarted server is in.

| | QLever | Blazegraph | |
|---|---|---|---|
| `?s ?p ?o`, 10,916,457 solutions | **1,510 ms** | 5,355 ms | QLever 3.5x |
| `?s a ?c`, 136,215 | **303 ms** | 419 ms | QLever 1.4x |
| `?s a ?c . ?s ?p ?o`, 10,332,352 | 4,363 ms | **3,198 ms** | Blazegraph 1.4x |

Opening the store costs about 390 ms for QLever and 490 for Blazegraph.

One run of each is not enough, and the last row above is the least firm of the
three. Six fresh processes per backend, alternating, on the join:

```
QLever      4,593  4,318  4,147  3,975  4,230  4,475
Blazegraph  5,550  3,836  3,055  3,118  3,200  3,532
```

QLever spans 1.16x and Blazegraph 1.8x. Since the JVM is new each time, that is
not compilation: it is the page cache filling for a 1.7 GB journal, where
QLever's 283 MB index is resident after one touch and has little left to gain.
So which of the two wins the join depends on how hot the disk is, and two
single measurements of mine landed at opposite ends of Blazegraph's range —
5,523 ms and 3,198 — which is what made an earlier version of this section
claim the opposite result.

A cold disk is not measured here. Evicting 2 GB of page cache needs `purge`
and that needs privileges, so what is above is a cold process against a hot
disk throughout.

So QLever scans faster and Blazegraph joins faster, which is what the two
designs predict: a nested-loop join is a sequence of point lookups, which a
B-tree is for and a block-compressed column store is not. The block cache
below is what brings that from a factor of ninety-five to a factor of 1.4.

### The comparison, at the plateau

"Warm" is two things, and calling them one hid a bad protocol. The JVM compiles
the pipeline as it runs, and a store's working set stays in memory once touched
— Blazegraph's in its heap, QLever's as compressed pages plus whatever its
block cache holds. Both stores fit in this machine's 16 GB, so after a few runs
both are joining in memory. That is the state a running server is in, and it is
not the state a first query sees.

`whatWarmingUpActuallyDoes` prints the rate of each successive run rather than
asserting anything, on `?s a ?c . ?s ?p ?o`, in solutions/s:

```
QLever        405,679  1,226,993  1,142,857  1,388,888  1,324,503  ...  1,418,439
Blazegraph    471,698  2,222,222  2,469,135  2,702,702  2,816,901  ...  2,816,901
```

Two things follow. The plateau does not depend on which store runs first, while
the first run very much does: whichever goes second starts on a JVM that has
already compiled the pipeline, so measured first Blazegraph opens at 268,456
and QLever at 405,679, and measured second they open at 471,698 and 847,457.
And Blazegraph takes four runs to level off where QLever takes two, so throwing
away one run — which this did at first — under-reports Blazegraph. Three are
thrown away now.

| first 200,000 solutions, median of 5 at the plateau | QLever /s | Blazegraph /s | |
|---|---|---|---|
| `?s ?p ?o` | **5,405,405** | 3,636,363 | QLever 1.5x |
| `?s a ?c` | **8,513,437** | 4,864,821 | QLever 1.8x |
| `?s a ?c . ?s ?p ?o` | 1,438,848 | **2,941,176** | Blazegraph 2.0x |

So QLever reads faster and Blazegraph joins faster, and the cache is what makes
the second a factor of two rather than of a hundred and twenty. A nested-loop
join is a sequence of point lookups, which is what a B-tree is for and what a
block-compressed column store is not; the cache removes the decompression but
not the indirection.

On disk QLever holds the same data in 283 MB against 1.7 GB. In memory the
order reverses and by more than expected: over twelve runs of the join the
process grows by about 840 MB with QLever and about 206 MB with Blazegraph.
Both are well under their file sizes, so neither reads everything, but QLever's
resident footprint exceeds its whole index — its vocabulary and the structures
it expands at load account for more than the compressed data does.

### The block size, which the cache settles

Without the cache, smaller blocks are a worthwhile workaround: at 1,000 rows
per block the join reaches 260,000 solutions/s instead of 23,700, for five
percent more disk. With the cache the opposite holds, since one decompressed
block then serves thousands of lookups:

| rows per block | no cache | cache of 16 | index |
|---|---|---|---|
| 31,250 (QLever's default) | 23,700 /s | **1,250,000 /s** | **283 MB** |
| 1,000 | 260,000 /s | 1,040,000 /s | 297 MB |

So `build-index.sh` builds with QLever's default block size, and
`ROWS_PER_BLOCK` is there for anyone who wants the other trade. Blazegraph
holds the same data in 1.7 GB.

To run it:

```bash
mvn -pl passage-volcano test -Dtest=QLeverVsBlazegraphTest \
    -Dbench.qlever=$HOME/bench-watdiv/qlever/watdiv10m \
    -Dbench.blazegraph=$HOME/bench-watdiv/blaze/watdiv10m-blaze/watdiv10M.jnl \
    -Dbench.solutions=200000
```

It skips, with a reason, when either database is missing. Results are counted
and never logged: `passage-cli` logs every solution, which at this scale
measures the logger rather than the engine.

## Building the library

The QLever fork that carries the bridge is a submodule of this repository,
pinned at the commit this backend was built against, so the dependency is
recorded in git rather than in prose. After a fresh clone:

```bash
export JAVA_HOME=$(brew --prefix openjdk@21)   # or wherever a 21 lives
git submodule update --init passage-qlever/qlever
mvn -pl passage-qlever install -Pnative
```

The `native` profile runs `build-native.sh`, which configures and builds the
`qleverjni` target and copies the result to
`target/classes/native/<os>-<arch>/`, so it travels in the jar and any module
that depends on `passage-qlever` gets it with no further setup.

That profile is **not** part of the default build. `mvn install` alone leaves
the submodule untouched and takes seconds; with `-Pnative` it takes about ten
minutes the first time and leaves some 2.5 GB under
`passage-qlever/qlever/build`. Anyone who only wants another backend should
never have to compile a C++ engine, and the tests here skip rather than fail
when the library is absent.

`build-native.sh` carries the platform flags, including the ones QLever's own
macOS CI uses and the `range-v3` guard a recent Apple Clang needs. It refuses
to start when a linked Homebrew `abseil` would shadow the one QLever pins,
which otherwise fails the build with a page of undefined `absl::lts_<date>`
symbols; `brew unlink abseil` is enough, and is reversible.

To point the submodule at a different QLever commit, move it and commit the new
pointer:

```bash
cd passage-qlever/qlever && git checkout <commit> && cd -
git add passage-qlever/qlever && git commit
```

NOTE: the `OFFSET` block skipping that makes deep continuations affordable is a
property of *that commit*, and its absence changes only how long a scan takes,
never what it returns. So none of the tests here can detect a QLever built
without it — verified by neutralising the patch, rebuilding, and watching all
seven still pass. The submodule pointer is what records which QLever this was
built against.

## Pointing this module at it

After a `-Pnative` build nothing needs pointing: `QLeverNative.load()` finds
the library on the classpath and copies it out to a temporary file, since
`System.load` wants a real file and not a resource. The lookup order is

1. the `qlever.jni.library` system property — an absolute path to the library;
2. the `QLEVER_JNI_LIBRARY` environment variable, likewise;
3. `native/<os>-<arch>/libqleverjni.<ext>` on the classpath, which is what the
   `native` profile puts under `target`;
4. `$XDG_CACHE_HOME/passage-qlever/<os>-<arch>/`, defaulting to `~/.cache`,
   where that profile also puts it;
5. `java.library.path` under the plain name `qleverjni`.

so a library built elsewhere still wins over the one in the jar.

The copy under `~/.cache` is not redundant. Anything that empties
`target/classes` — a `mvn clean`, or simply a build that rebuilds the module —
takes the classpath copy with it, and then an ordinary `mvn install` *without*
`-Pnative` reinstalls the jar without a library, silently. Every test here then
skips for want of it, and the cause looks like the tests rather than the build.
The cached copy survives any Maven invocation, so this only costs a second
`-Pnative` run when the library itself has to change.

NOTE: the directory is named after `uname -m`, which says `arm64` where the
JVM's `os.arch` says `aarch64`, and `x86_64` where it says `amd64`; the lookup
tries both spellings.

An index is a set of files sharing a basename, which is what
`QLeverBackendFactory` takes as its `Path`. `build-index.sh` makes one:

```bash
cat > fig2.ttl <<'TTL'
<http://ex.org/H1> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H2> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H3> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O1> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O2> .
<http://ex.org/H3> <http://ex.org/occupation> <http://ex.org/O3> .
TTL
passage-qlever/build-index.sh $PWD/fig2 fig2.ttl
```

`build-index.sh` wraps QLever's `qlever-index`, which the `native` profile
builds alongside the library, with the block size Passage wants rather than the
one QLever defaults to — see [below](#the-block-size-which-the-cache-settles)
for why that matters, and `ROWS_PER_BLOCK=n` to override it. It also pipes a
`.gz` file in, which `qlever-index` does not decompress for itself. The
executable stays in the submodule's build directory rather than being copied
into the jar: it is 60 MB, not something a dependency should carry.

Then run the tests against the two of them:

```bash
JAVA_HOME=$(brew --prefix openjdk@21) \
mvn -pl passage-qlever,passage-volcano test \
    -Dtest='QLeverBackendTest,QLeverContinuationTest' \
    -Dqlever.test.index=$PWD/fig2
```

An empty `-Dqlever.test.index=`, which is what an unset shell variable gives,
counts as naming no index. The tests then abort and print
`SKIPPING <class>: ...` with the reason, because surefire summarises an aborted
`@BeforeAll` as zero tests run rather than as skipped — so without that line a
forgotten path would look exactly like a successful run.

`JAVA_HOME` is on both commands because Maven itself has to run on a 21, the
modules being compiled with release 21. It can be left out once the default JDK
is a 21; a `requireJavaVersion` rule in the root pom refuses at `validate`
either way, so the failure names Java rather than blaming a test class with
`class file version 65.0 ... only recognizes class file versions up to 61.0`.

NOTE for `jenv` users: `jenv local` does not help, because jenv shims the JDK
binaries and not `mvn`. Either `jenv global` a 21, or keep `JAVA_HOME` on the
command.

Only the index has to be named, the library having come from the jar. Without
the index the tests report as skipped, not as passed.

`passage-volcano` is in that list because the backend's own tests cover the
scan interface while the continuation test, which is what the backend exists
for, belongs with the engine; it reaches the native library transitively,
through this jar.

## A crash on exit that is not this module's

This module's own tests do not hit it: `mvn -pl passage-qlever test` with both
properties set runs its assertions and exits green. What does hit it is driving
a query through `passage-volcano` over this backend, which then dies with a
`SIGSEGV` during shutdown, *after* the results and the verdict have been
printed, so the process exits 132 rather than 0:

```
_pthread_exit -> _pthread_tsd_cleanup -> je_tsd_cleanup
  -> tcache_destroy -> tcache_flush_cache -> je_arena_ptr_array_flush
```

QLever's top-level `CMakeLists.txt` calls `link_libraries()` at directory
scope, so jemalloc reaches every target created after it, `qleverjni` included,
and through its static libraries transitively as well. Its per-thread cleanup
then runs as a thread of QLever's own exits inside a JVM that has its own
allocator. Filtering jemalloc off the one target does not help and
`MALLOC_CONF` is not read; the fix belongs in QLever, which should not link an
allocator into a library meant to be loaded into a JVM. Until then, read what a
run prints rather than its exit status.

The trigger is therefore narrower than "a JVM that loaded the bridge", since
the tests here load it and shut down cleanly. It takes a thread of QLever's own
to be created and then to exit, which the scans these tests perform do not
provoke.

Unrelated but visible in the same place: QLever logs to the process's stderr
rather than through Java, so surefire files those lines under `Corrupted
channel by directly writing to native stream` in
`target/surefire-reports/*.dumpstream`. That is where an index's log goes, and
it is not an error.
