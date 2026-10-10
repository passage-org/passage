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
passage-qlever/qlever/build/qlever-index \
    -i $HOME/qlever-fig2/fig2 -f $HOME/qlever-fig2/fig2.ttl -F ttl

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

### It is the block size, and it is a setting

That cost is **decompressing the first block**, and it is set at index build
time rather than inherent. Three things establish it.

It does not come from the data: opening a scan costs 0.853 ms over a million
triples and 0.861 ms over eleven million. It does not come from the prefetch
machinery either, which was the likelier guess — `lazyScan` builds an
`AsyncTransformView` over up to 21 concurrent blocks, and sweeping
`lazy-index-scan-num-blocks-in-flight` from 20 down to 1 moves nothing:

| blocks in flight | 20 | 10 | 5 | 2 | 1 |
|---|---|---|---|---|---|
| ms per scan | 0.856 | 0.858 | 0.851 | 0.853 | 0.860 |

And it does not come from crossing into the JVM. Timed in C++ with no boundary
(`ScanCursor.timeCursorCreationAgainstARealIndex` in the QLever fork),
constructing a cursor costs 0.0004 ms and its first read 0.933 ms.

What does move it is `--index-rows-per-block`. A block holds 250 kB per column,
so reading one triple decompresses up to a megabyte, which at Zstd's throughput
is the millisecond observed. Building the same 10,916,457 triples with smaller
blocks, against Blazegraph's 743,000 solutions/s on the join and 0.072 ms per
scan:

| rows per block | ms per scan | join /s | `?s ?p ?o` /s | index |
|---|---|---|---|---|
| 31,250 (default) | 0.915 | 24,000 | 628,930 | 283 MB |
| 1,000 | **0.048** | 385,356 | 651,465 | 297 MB |
| 100 | **0.016** | 613,496 | 576,368 | 610 MB |

So the thirtyfold gap was a default, not an architecture. At 1,000 rows per
block the join is sixteen times faster for five percent more disk, long scans
are unchanged, and the per-scan cost is already below Blazegraph's. At 100 it is
faster again, but the index doubles and long scans start to give way, which is
the compression working against you: fewer rows per block is less to decompress
per lookup and less context to compress with.

What remains is modest and in both directions. On the join QLever stays 1.9x
behind at 1,000 rows and 1.3x at 100; on a scan with the predicate bound it is
2x ahead; and its index is 283 to 610 MB against Blazegraph's 1.7 GB.

NOTE: the default is tuned for scans that read many rows, which is what QLever's
own engine does. Passage does the opposite — a nested-loop join is a sequence of
point lookups — so an index built for it wants small blocks. That is a
configuration to get right, not a patch to write.

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

An index is made by `qlever-index` and is a set of files sharing a basename,
which is what `QLeverBackendFactory` takes as its `Path`:

```bash
cat > fig2.ttl <<'TTL'
<http://ex.org/H1> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H2> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H3> <http://ex.org/type> <http://ex.org/Human> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O1> .
<http://ex.org/H1> <http://ex.org/occupation> <http://ex.org/O2> .
<http://ex.org/H3> <http://ex.org/occupation> <http://ex.org/O3> .
TTL
passage-qlever/qlever/build/qlever-index -i $PWD/fig2 -f fig2.ttl -F ttl
```

The `native` profile builds that tool alongside the library, since an index is
needed to use this module at all. It stays in the submodule's build directory
rather than being copied into the jar: it is a 60 MB executable, not something
a dependency should carry.

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
