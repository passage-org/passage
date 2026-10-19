# Journal

A running log of the Passage-on-QLever work: what was done, what it measured,
and what turned out to be wrong. Newest first. Dates are the day the work
landed, not the day it started.

The companion documents are `README.md` (how to build, how to measure, what the
storage looks like), `feasibility/README.md` (the FILTER study), and
`REPORT-filter-vs-offset.md` in the student-subject repository next to the
slides. This file records the decisions those documents assume.

Two repositories move together, and not as equals: this one (`passage`, branch
`filter-continuation`) is the work, and the QLever fork (`qlever-1`, branch
`scan-distinct`) is a consequence of it — it exists to give Passage a QLever
backend, and carries only what QLever itself must carry. Entries name the
commit in each where it matters.

---

## 2026-10-19 — The join QLever "loses" is the JNI crossing, not the storage

Asked whether translating across JNI could explain the join difference, and
the answer is yes, for most of it. The published line — QLever 1,438,848
solutions/s against Blazegraph 2,941,176, a factor of two — drives both
backends through the same Java engine, but Blazegraph is a Java store reached
by method calls while every QLever scan crosses into C++. Only one of the two
pays the second cost, and the figure added them together.

Two benchmarks now separate them, running the identical nested loop over
`?s rdf:type ?c . ?s ?p ?o`, fan-out 20:
`ScanCursorTest.timeAJoinAgainstARealIndex` in the fork, in C++, and
`JoinThroughJniBench` here, through `QLeverNative` rather than through
`QLeverBackend` so that the Java iterator is not in the way.

| | solutions/s |
|---|---|
| C++, bulk `fill(1024)`, warm | 3,324,452 / 3,240,329 |
| through JNI, after the fix | 1,953,121 / 1,916,751 / 1,957,075 |
| through JNI, before | 1,518,041 |
| QLever through the whole engine | 1,438,848 |
| Blazegraph through the whole engine | 2,941,176 |

**The crossing costs 1.68x.** The engine costs five percent. So the storage
does this join at something like Blazegraph's rate, and the explanation this
README gave — point lookups suit a B-tree and not a block store — is not
what the measurement says. Corrected in both places it appeared, and on the
deck's `blazegraph` slide.

Most of the crossing was waste. `fill` allocated and value-initialised two
vectors per call, the first sized on the Java array rather than on the matches:
twenty matches zeroed 24 kB, and so did the call returning none that ended each
inner loop. Reusing both per thread gave +29%.

**Three of my own numbers were wrong along the way, and the pattern is the
same each time: I estimated instead of measuring.**

* I told Pascal the boundary was amortised to 0.1 ns per match. It is not:
  there is a per-element conversion loop and, back then, two allocations per
  call. His doubt was better founded than my answer.
* I predicted the fix would give +60 to +80% from memory bandwidth. It gave
  +29%. The zero-filling was 46% of the gap, not three quarters.
* I first reported the C++ side at 2,914,608 /s from a single run of unknown
  warmth. Three runs show the first one cold at 2,436,355 and the warm ones at
  3.3M, which makes the gap *larger* than I had claimed, 1.68 rather than 1.49.

Still open, and both are measurements rather than arguments: the 490 ns per
crossing that remain unexplained against 100 ns for an argument-less call; and
a `BlazegraphJoinBench` symmetric with this one, since Blazegraph has never
been measured bare and "C++ 3.3M against Blazegraph 2.94M" removes QLever's
boundary without removing Blazegraph's engine.

And what it does not license: 1.68x is measured where the boundary hurts most.
On a scan QLever already wins 1.5 to 1.8x *through the same boundary*. The
block cache was worth 58x. The dimension change in this project was
algorithmic, not linguistic.

## 2026-10-18 — Both repositories public, and the QLever one is now a real fork

`github.com/momo54/qlever` is a fork of `ad-freiburg/qlever`, public, with
`scan-distinct` pushed into it; `github.com/momo54/passage` is public too.
Verified: the cross-repository comparison upstream now answers — 40 commits
ahead over 22 files — so a pull request to `ad-freiburg/qlever` is possible,
and the submodule clones with no credential at all.

Why it had to be a fork and not just a visibility switch: GitHub allows a pull
request only between repositories in the same **fork network**, and that
relationship is established when the fork is made, never afterwards. The first
pair were plain repositories created with `gh repo create`, so the comparison
returned 404 while the same comparison against a genuine fork of the same
upstream returned a result. Making them public would have changed nothing.
Nor was the original choice avoidable: a fork of a public repository is
public, visibility being inherited, so a private copy *has* to be a plain
repository. Private and the pull-request path cannot coexist.

The name had to be freed first, `momo54/qlever` being taken by our own private
copy, which is now `momo54/qlever-passage` and redundant.

What went with it, and this is the point: the
`customizations.codespaces.repositories` block, the authorization step in
`README-QLEVER.md` and the 403 row of its failure table, and the red warning
on the student deck's fork slide. All of it existed only because the submodule
was private.

Checked before publishing rather than after: no credentials, no personal
absolute paths, no addresses outside upstream's own author headers. The
occurrences of "token" are prose about the Codespace token.

## 2026-10-18 — It builds and passes on Linux x86_64

Twelve tests green in a Codespace: `QLeverBackendTest` 8,
`QLeverContinuationTest` 4, on Ubuntu 24.04, x86_64, gcc 13.3, OpenJDK 21,
against an index built there by `qlever-index`. Nothing of this had ever run
off macOS/arm64.

So the port is portable, and the loader's two spellings were worth it:
`build-native.sh` names the directory `linux-x86_64` from `uname -m` while the
JVM reports `os.arch=amd64`, and `QLeverNative.platformNames()` tries both
rather than picking a side. That was written before any Linux existed to test
it, and it is the one thing on this road that did not have to be fixed.

What it took, in the order the failures arrived: a submodule pin that pointed
at a QLever without the snapshot natives; a Codespace token that cannot read a
second private repository; `mvn -pl <module>` unable to resolve
`passage-commons` on an empty `~/.m2`; a JNI target silently skipped because
no JAVA_HOME and no `default-java` symlink; and the link failing because
QLever's static archives are not position-independent. Only the last is a real
difference between the platforms. The other four were things this machine
already had, and therefore could not miss — which is the whole argument for
building somewhere that has nothing.

## 2026-10-18 — The first failure that is genuinely about Linux

The C++ build ran to completion and then the link failed:

```
/usr/bin/ld: ../../lib/libindex.a(IndexImpl.cpp.o): relocation R_X86_64_TPOFF32
against symbol `...boost::asio::detail::keyword_tss_ptr<...>::value_'
can not be used when making a shared object; recompile with -fPIC
```

The bridge is a shared library; everything it links is a static archive; and
on Linux x86_64 a static archive built without `-fPIC` cannot go into a `.so`.
The symbol is a `thread_local` inside Boost.Asio's headers, instantiated into
`libindex.a`. macOS cannot show this: arm64 Mach-O code is position-independent
anyway.

QLever turns PIC on only together with `BUILD_SHARED_LIBS`, which defaults to
`OFF` and which its own CI sets to `ON` — so upstream never meets this
either. Taking that route here would scatter the code over a dozen `.so`
files, while `QLeverNative.load()` copies exactly one file out of the jar and
calls `System.load` on it. So `build-native.sh` keeps the archives static and
passes `-DCMAKE_POSITION_INDEPENDENT_CODE=ON`, on Linux only: macOS does not
need it and setting it there would invalidate a working build tree for
nothing.

The cost is a full recompile, since PIC changes the code generated for every
object. There is no way around that.

Worth separating from the four findings below. Those were all things this
machine already had and therefore could not miss. This one is a real
difference between the two platforms, and it is the first.

## 2026-10-18 — What the neutral machine found that this one could not

A Codespace was opened against the GitHub fork, and the run failed twice
before reaching a compiler. Both failures were invisible here by construction.

**A Codespace's token reaches only the repository it was created from.**
Cloning the private `momo54/qlever` submodule returned 403 with
"Write access to repository not granted", which is not about writing.
`customizations.codespaces.repositories` in `devcontainer.json` asks for that
one extra read and GitHub prompts for it at creation time — but only for
Codespaces created after the commit, so an existing one has to be replaced,
or given its own credentials with `gh auth login`. I had verified that the
submodule could be *redirected* to GitHub, never that it could be *read* from
a machine whose credentials are not mine.

**`mvn -pl passage-qlever install` cannot resolve `passage-commons`.** None of
the `fr.gdd` artifacts is published to any Maven repository, and a fresh
`~/.m2` holds none of them, so a single-module build stops with "Could not
find artifact fr.gdd:passage-commons:jar:0.2.0 in central". This machine's
`~/.m2` has held them for weeks, which is exactly why the README's one paste
has always worked here. `build-and-test.sh` now builds the whole reactor,
which is the closure of its own steps anyway: `passage-volcano` needs
`jena-utils` and `raw`, `passage-cli` needs `passage-blazegraph`. Verified
here: eight modules, BUILD SUCCESS, and `passage.jar` shaded, so the separate
`mvn -pl passage-cli` the script used to run was redundant.

**CMake built no `qleverjni` target at all.** It creates it only when it finds
`jni.h`, and it looks in four places: `$JAVA_HOME/include`,
`$JAVA_HOME/Contents/Home/include`, `/opt/homebrew/opt/openjdk/include` and
`/usr/lib/jvm/default-java/include`. On Ubuntu with `openjdk-21-jdk` the JDK
sits in `/usr/lib/jvm/java-21-openjdk-amd64` and there is no `default-java`
symlink, so with no JAVA_HOME in the environment none of the four matches: the
target is quietly skipped and the failure arrives much later as
`gmake: *** No rule to make target 'qleverjni'`. My JAVA_HOME went to
`/etc/profile.d/`, which only login shells read.

`build-native.sh` now derives JAVA_HOME from `javac` when it is unset — and
only when that directory really holds the headers, since `javac` is often a
shim — checks the same four places CMake does, and refuses up front with a
message naming the cause. Tested in four configurations: this Mac with and
without JAVA_HOME, Ubuntu with `openjdk-21-jdk` and no JAVA_HOME, and Ubuntu
with no JDK at all. `install-deps.sh` also installs `default-jdk` instead of
`openjdk-21-jdk`, which on Ubuntu 24.04 pulls the same JDK and adds the
symlink CMake looks for.

Four findings now, from a machine that has compiled nothing yet: a stale
submodule pin, a token that cannot read the submodule, a build that only
resolves on a warm local repository, and a JNI target that is skipped in
silence. **None of them is about Linux.** Every one is something this machine
already had and therefore could not miss.

## 2026-10-18 — Both forks moved to GitHub, and the redirection went with them

GitHub is now the home of this fork and of the QLever one:
`github.com/momo54/passage` and `github.com/momo54/qlever`, both private. The
`gitlab` remote stays configured locally and holds the history up to today,
but nothing is pushed there any more. The student-subject repository stayed on
`gitlab.univ-nantes.fr`; it is institutional and it does not get built.

The reason is the neutral machine. `.gitmodules` recorded an SSH GitLab URL,
so a Codespace could not fetch the submodule without a credential of the
university's, and everything built to work around that is now deleted: the
`insteadOf` rewrite in `install-deps.sh`, the per-submodule URL override in
`build-and-test.sh`. `.gitmodules` names GitHub and
`git submodule update --init` is enough. Two remotes for one repository was
also the shape of the two problems cleaned up earlier today.

Getting there cost three failures, each of them mine and each worth keeping:

* The URL rewrite was the **last** statement of `install-deps.sh`, after all
  the apt work, under `set -e`. Any hiccup and it never ran, and the failure
  surfaced much later as a GitLab credential prompt. Moved to the top — and
  then it broke differently, because it calls `git` and a bare `ubuntu:24.04`
  has none. Installed git first. Caught by running the script in
  `ubuntu:24.04` rather than by reading it.
* `devcontainer.json` asked for `ghcr.io/devcontainers/features/java:1.6.4`,
  a version I invented; the latest is `1.8.3`. A feature that does not resolve
  fails the whole container build, and Codespaces then hands you its recovery
  container: the workspace is mounted, so every script is there, but the image
  is not the one asked for, which is why `apt-get` was missing. There are no
  features now — Ubuntu 24.04 has `openjdk-21-jdk` and `maven` in apt, so
  nothing is left to resolve.
* Debian `bookworm`, which I considered for the Java image, ships Boost 1.74.
  QLever needs 1.81 for `Boost::url`. Ubuntu 24.04 ships 1.83.

The dependency script now reports its own environment on the first line and,
when there is no `apt-get`, says that you are probably in the recovery
container and where the creation log is. Verified on `ubuntu:24.04` (exit 0,
Boost 1.83, cmake 3.28.3, gcc 13.3, Maven 3.8.7) and on `alpine` (exit 1 with
that message).

Still not done: the C++ build on Linux. That is the part no amount of reading
settles.

## 2026-10-18 — A Linux container, and the stale pin it caught before running

Everything here has only ever been built on macOS, on arm64, so `.devcontainer/`
in the repository root builds it on Ubuntu 24.04 instead, in a Codespace. The
dependency list is QLever's own CI rather than a guess, and it is checked in
`ubuntu:24.04`: Boost 1.83, cmake 3.28.3, gcc 13.3, all above what the CMake
configure demands.

Preparing it was already worth it. The submodule pin was `a31314c1`, from
before `snapshotId` and the other two natives existed. Twelve tests pass on
this machine regardless, because the shared library here was built from a
working tree and never from the pin — so nothing local can notice. A clean
build would have produced a library without those symbols and failed
`aGraphThatMovesUnderAContinuationIsNoticed` with an `UnsatisfiedLinkError`.
Moved to `55e9c7a8`, rebuilt from it, twelve tests still green.

The container also leaves out `libjemalloc-dev` deliberately, since QLever
links jemalloc when it finds it and warns and continues when it does not. I
wrote that this would answer, for free, whether jemalloc's per-thread cleanup
is what kills the JVM on exit with 132. **It does not**, and I should have read
my own README before saying so: the crash needs a thread of QLever's own to
start and then exit, and the six triples of Figure 2 never provoke one. Checked
on this machine, which has jemalloc linked: `passage-cli` over that index exits
0. Settling it needs a workload big enough to start a worker thread — the
2,000,000-triple index the fork's demos use, or WatDiv.

Not yet run: the Codespace itself. The two GitHub mirrors exist and the
remotes are configured; pushing to them was refused by the sandbox as possible
exfiltration, so the two `git push` commands are the user's to run.

## 2026-10-18 — Which fork is a consequence of which

Settled as a direction: the QLever fork is **a consequence of this one**. It
exists to give Passage a QLever backend. Passage is the work; the fork is what
that work needed from the store underneath.

That is the general form of the deletion below, and it decides where anything
new goes. The fork carries what QLever itself must carry, which is also what
could be offered upstream to `ad-freiburg/qlever`: the block-skipping `OFFSET`
patch, `ScanCursor`, the JNI bridge, and the standalone demos that exercise
them with no Maven around them. Anything about the backend — its conventions,
its tests, its evaluation — belongs here. Where material could sit on either
side, it sits here and the fork points at it, never a second copy.

Carried out the same day. `PASSAGE-QLEVER-BACKEND.md` in the fork went from
399 lines to 239 and is now titled for what it covers, *The QLever side of a
Passage backend*: the cursor, the bridge, the native build, and the demos.
`Running it` became the documentation of the `PassageOnQlever` demo and points
here for the same thing without Java; `Against Blazegraph` became one sentence
of result and a pointer; the convention section went, since this README told
that story better.

Two things in it existed nowhere else, so they moved here rather than going:
the `null`-versus-`any()` convention that `PassageScan` relies on and the
`getId` overload ambiguity, now under *Two more conventions the interface does
not state*; and the list of what the evaluation does not cover — no workload,
no data beyond memory, basic graph patterns only, no quads, no concurrency —
now *What is not verified*. Both are about this backend, not about QLever.

`PASSAGE-README.md` was not touched. It documents a change to QLever, for
QLever, and it is the part that could go upstream.

`PASSAGE-README.md` stays in the fork whatever happens: it documents a change
to QLever, for QLever, and it is the upstreamable artifact.

Checked while reading: the two `Against Blazegraph, on WatDiv 10M` sections
do **not** disagree. The fork gives absolute times, this README gives
throughputs and the diagnosis that produced the block cache, and both end at
the same place. Two tellings, not two answers — but also two documents with
nothing keeping them in step, which is the point above.

## 2026-10-18 — The QLever fork's copy of these classes is gone

Asked where this journal lived, I went looking for somewhere to point at it
from the QLever fork, and found `src/libqlever/jni/java/fr/gdd/passage/qlever/`
holding a second copy of `QLeverNative`, `QLeverIterator` and `QLeverBackend`,
last touched on 10 October. It had neither fix recorded below: `getId` still
returned the sentinel that matches every triple, and `indexInBatch` still
started at `0`, which reads before it advances. Both are covered by tests here,
and tests here say nothing about a copy over there.

`PASSAGE-QLEVER-BACKEND.md` sent the reader straight to it, with a `javac` line
that compiled it, under a heading stating that the classes "belong in a
`passage-qlever` module of the Passage repository […] move them there as they
are". The move had happened. The originals had never been removed.

Deleted. The fork's two documents now say where the classes live, and compile
`demo/` against this module's jar instead. Nothing in the build read that
directory — `src/libqlever/CMakeLists.txt` compiles only `jni/QleverJni.cpp`,
and the JNI symbol names are written by hand rather than generated from a
`javac -h` header — so the deletion costs nothing. `demo/` stays in the fork:
seven standalone programs that drive the bridge with no Maven and no engine
around them.

Same pass: this README promised `Tests run: 5` and `Tests run: 2`, true when it
was written. Measured again, it is 8 and 4.

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
| An invented version fails into a different container | `devcontainers/features/java:1.6.4` does not exist. The container build failed, Codespaces substituted its recovery container, and the symptom was a missing `apt-get` three steps later. Check that a tag exists before pinning it; prefer no feature at all when apt has the package. |
| A warm `~/.m2` hides a missing dependency | `mvn -pl <one module>` resolved `passage-commons` here for weeks because an earlier build had installed it. On an empty local repository it cannot: nothing under `fr.gdd` is published anywhere. Build the reactor, or test with `-Dmaven.repo.local` pointed somewhere empty. |
| A dev machine cannot check its own pin | The submodule pointer was a week stale and every test passed, because the library on disk was built from a working tree rather than from the pinned commit. Only a build that starts from the pin can tell. |
| A copy is a copy of the bugs too | The QLever fork kept its own copy of the three Java classes, and nothing watched the two for divergence. It sat there for a week with two fixed bugs still in it, and a document telling people to compile it. One copy now, and the other document points at it. |
| Check the edit landed | I reported a cross-reference corrected when the replacement had been a no-op against markup inside the text. Edits are verified on disk now. |
