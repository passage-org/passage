# The native side of `passage-qlever`

This module is pure Java, but every scan it serves comes from a QLever index
through a JNI bridge. The bridge is a shared library, `libqleverjni`, built from
a QLever checkout — it is **not** shipped in this jar, because it is native code
for one platform at a time and because it is QLever's to build, not ours.

So `mvn test` here skips its tests unless it is told where that library and an
index are. That is deliberate: the module has to build and its tests have to
pass on a machine that has never heard of QLever.

## Building the library

From a checkout of the QLever fork that carries the bridge
(`src/libqlever/jni/QleverJni.cpp` and `src/libqlever/ScanCursor.{h,cpp}`):

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target qleverjni qlever-index -j 8
# -> build/lib/libqleverjni.dylib  (.so on Linux)
```

The target is only created when the JNI headers are found, so an ordinary
QLever build is unaffected. On macOS that `cmake` line needs extra flags; the
fork's `PASSAGE-README.md` lists them, along with what a Homebrew `abseil` and
a recent Apple Clang break.

## Pointing this module at it

`QLeverNative.load()` looks, in order, at the `qlever.jni.library` system
property, the `QLEVER_JNI_LIBRARY` environment variable, and then
`java.library.path` under the plain name `qleverjni`. The first two take an
absolute path to the library file; the third takes the directory holding it.

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
mvn -pl passage-qlever test \
    -Dqlever.jni.library=/path/to/build/lib/libqleverjni.dylib \
    -Dqlever.test.index=/path/to/fig2
```

Without those two properties the tests report as skipped, not as passed.

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
