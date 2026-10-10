# The native side of `passage-qlever`

This module is pure Java, but every scan it serves comes from a QLever index
through a JNI bridge. The bridge is a shared library, `libqleverjni`, built from
a QLever checkout — it is **not** shipped in this jar, because it is native code
for one platform at a time and because it is QLever's to build, not ours.

So `mvn test` here skips its tests unless it is told where that library and an
index are. That is deliberate: the module has to build and its tests have to
pass on a machine that has never heard of QLever.

## Building the library

The QLever fork that carries the bridge is a submodule of this repository,
pinned at the commit this backend was built against, so the dependency is
recorded in git rather than in prose. After a fresh clone:

```bash
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
   `native` profile produces;
4. `java.library.path` under the plain name `qleverjni`.

so a library built elsewhere still wins over the one in the jar. NOTE: the
resource is named after `uname -m`, which says `arm64` where the JVM's
`os.arch` says `aarch64`; the lookup tries both.

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
/path/to/build/qlever-index -i fig2 -f fig2.ttl -F ttl
```

Then run the tests against the two of them:

```bash
mvn -pl passage-qlever,passage-volcano test \
    -Dtest='QLeverBackendTest,QLeverContinuationTest' \
    -Dqlever.test.index=/path/to/fig2
```

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
