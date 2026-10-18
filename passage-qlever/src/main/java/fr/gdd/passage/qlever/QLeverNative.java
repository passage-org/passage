package fr.gdd.passage.qlever;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * The native methods of the QLever JNI bridge (see `src/libqlever/jni/QleverJni.cpp`
 * in the QLever repository). Handles are opaque pointers; every `openBackend` and
 * `search` has to be paired with the corresponding `close`.
 */
public final class QLeverNative {

    private QLeverNative() {}

    /** The system property that gives the absolute path of the shared library. */
    public static final String LIBRARY_PROPERTY = "qlever.jni.library";
    /** The environment variable consulted when the property is not set. */
    public static final String LIBRARY_ENV = "QLEVER_JNI_LIBRARY";

    private static boolean loaded = false;

    /** Load the bridge from an explicit path, e.g. `.../lib/libqleverjni.dylib`. */
    public static synchronized void load(String pathToLibrary) {
        if (loaded) { return; }
        System.load(pathToLibrary);
        loaded = true;
    }

    /**
     * Load the bridge without being told where it is, which is what
     * {@link fr.gdd.passage.commons.interfaces.BackendFactory} needs: it hands a
     * backend the path of the index and nothing else. In order, this tries the
     * `qlever.jni.library` system property, the `QLEVER_JNI_LIBRARY` environment
     * variable, and finally `java.library.path` under the plain name `qleverjni`.
     *
     * <p>The library is not shipped in this jar: it is built from a QLever
     * checkout, for one platform at a time. See `NATIVE.md`.
     */
    public static synchronized void load() {
        if (loaded) { return; }
        String explicit = System.getProperty(LIBRARY_PROPERTY);
        if (explicit == null) { explicit = System.getenv(LIBRARY_ENV); }
        if (explicit != null) {
            System.load(explicit);
        } else if (!loadFromClasspath() && !loadFromCache()) {
            try {
                System.loadLibrary("qleverjni");
            } catch (UnsatisfiedLinkError e) {
                throw new UnsatisfiedLinkError(
                        "could not find the QLever JNI bridge. Build the `qleverjni` target"
                        + " in a QLever checkout, then point at the result with"
                        + " -D" + LIBRARY_PROPERTY + "=/path/to/libqleverjni.dylib,"
                        + " with " + LIBRARY_ENV + ", or with -Djava.library.path."
                        + " Original error: " + e.getMessage());
            }
        }
        loaded = true;
    }

    /**
     * Load the library from where the `native` profile also leaves it, outside
     * `target`. A later `mvn install` without that profile rewrites the jar
     * without the library, so the classpath copy is the one that goes missing;
     * this one survives any Maven invocation.
     *
     * @return false if it is not there, so that the caller can go on.
     */
    private static boolean loadFromCache() {
        String cache = System.getenv("XDG_CACHE_HOME");
        if (cache == null || cache.isBlank()) {
            String home = System.getProperty("user.home");
            if (home == null) { return false; }
            cache = home + "/.cache";
        }
        for (String name : platformNames()) {
            Path candidate = Path.of(cache, "passage-qlever", name,
                    "libqleverjni." + extension());
            if (Files.isReadable(candidate)) {
                System.load(candidate.toAbsolutePath().toString());
                return true;
            }
        }
        return false;
    }

    /**
     * `darwin` or `linux` paired with every spelling of the architecture. The
     * directory is named after `uname -m`, which disagrees with the JVM's
     * `os.arch` on both common architectures — `arm64` against `aarch64`, and
     * `x86_64` against `amd64` — so both are tried rather than picking a side.
     * Empty when the platform is neither macOS nor Linux.
     */
    private static List<String> platformNames() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String name = (os.contains("mac") || os.contains("darwin")) ? "darwin"
                : os.contains("linux") ? "linux" : null;
        if (name == null) { return List.of(); }
        String arch = System.getProperty("os.arch", "");
        List<String> spellings = switch (arch) {
            case "aarch64", "arm64" -> List.of("arm64", "aarch64");
            case "amd64", "x86_64" -> List.of("x86_64", "amd64");
            default -> List.of(arch);
        };
        return spellings.stream().map(a -> name + "-" + a).toList();
    }

    /** `dylib` on macOS, `so` on Linux, empty when neither. */
    private static String extension() {
        String os = System.getProperty("os.name", "").toLowerCase();
        return (os.contains("mac") || os.contains("darwin")) ? "dylib" : "so";
    }

    /**
     * Load the library from `native/<os>-<arch>/` on the classpath, which is
     * where the `native` Maven profile puts what it builds from the `qlever`
     * submodule. It has to be copied out before `System.load` can take it,
     * since that needs a real file and not a resource.
     *
     * @return false if there is no such resource, so that the caller can go on
     *         to try `java.library.path`.
     */
    private static boolean loadFromClasspath() {
        for (String candidate : platformNames()) {
            String resource = "native/" + candidate + "/libqleverjni." + extension();
            try (InputStream in = QLeverNative.class.getClassLoader()
                    .getResourceAsStream(resource)) {
                if (in == null) { continue; }
                Path copy = Files.createTempFile("libqleverjni", "." + extension());
                Files.copy(in, copy, StandardCopyOption.REPLACE_EXISTING);
                copy.toFile().deleteOnExit();
                System.load(copy.toAbsolutePath().toString());
                return true;
            } catch (IOException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * How many blocks a lazy scan reads ahead of its consumer, QLever's
     * `lazy-index-scan-num-blocks-in-flight`. It is global, not per scan.
     *
     * <p>An engine that drives the joins itself opens one scan per outer
     * solution and so pays whatever a scan costs to start, and a scan that
     * reads a single block cannot use a prefetch depth of twenty. Lowering this
     * is therefore worth measuring; QLever's own comment warns that it slows
     * down queries with prefiltered joins on large relations, which are the
     * opposite case.
     *
     * @throws RuntimeException below `1`, which a parameter constraint rejects.
     */
    public static native void setBlocksInFlight(long blocks);

    /** @see #setBlocksInFlight(long) */
    public static native long getBlocksInFlight();

    /**
     * How many decompressed blocks a thread keeps, QLever's
     * `lazy-index-scan-block-cache-size`. Zero, the default, keeps none.
     *
     * <p>A nested-loop join opens a scan per outer solution and a block spans
     * thousands of consecutive subjects, so the same block is otherwise
     * decompressed thousands of times. A handful of entries is enough: one per
     * scan position live at once, which is the join depth times the
     * parallelism. QLever defaults it off because a merge join never returns to
     * a block, so for its own engine every lookup would be a miss.
     */
    public static native void setBlockCacheSize(long blocks);

    /** @see #setBlockCacheSize(long) */
    public static native long getBlockCacheSize();

    // Backend.
    public static native long openBackend(String indexBasename);
    public static native void closeBackend(long backend);
    /** The identifier that stands for an unbound component of a triple pattern. */
    /** The snapshot this backend was opened on; every scan reads that one. */
    public static native long snapshotId(long backend);

    /** The snapshot the engine would hand out now. Differs from
     *  {@link #snapshotId} exactly when the graph moved since the open. */
    public static native long currentSnapshotId(long backend);

    /** Inserts one triple, so a test can move the snapshot. Test-only: the
     *  bridge is otherwise read-only. */
    public static native void moveSnapshotForTesting(long backend);

    public static native long any();
    public static native long getId(long backend, String value);
    public static native String getString(long backend, long id);

    // Cursor over a single triple pattern.
    public static native long search(long backend, long s, long p, long o);
    /**
     * As {@link #search}, but enumerating the distinct combinations of the components
     * selected by `distinctMask`: bit 0 for the subject, 1 for the predicate, 2 for
     * the object. A mask of 0 is exactly {@link #search}.
     */
    public static native long searchDistinct(long backend, long s, long p, long o,
                                             int distinctMask);
    public static native void closeCursor(long cursor);
    public static native boolean hasNext(long cursor);
    public static native void next(long cursor);
    /** `component` is 0 for the subject, 1 for the predicate, 2 for the object. */
    public static native long getComponent(long cursor, int component);
    /** Copy up to `maxTuples` matches (3 longs each) into `out`; returns the count. */
    public static native int fill(long cursor, long[] out, int maxTuples);
    public static native void skip(long cursor, long offset);
    public static native long current(long cursor);
    public static native long cardinality(long cursor);
    public static native long cardinalityEstimate(long cursor);
}
