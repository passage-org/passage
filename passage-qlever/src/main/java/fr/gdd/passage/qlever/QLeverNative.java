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
        } else if (!loadFromClasspath()) {
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
     * Load the library from `native/<os>-<arch>/` on the classpath, which is
     * where the `native` Maven profile puts what it builds from the `qlever`
     * submodule. It has to be copied out before `System.load` can take it,
     * since that needs a real file and not a resource.
     *
     * @return false if there is no such resource, so that the caller can go on
     *         to try `java.library.path`.
     */
    private static boolean loadFromClasspath() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String name;
        String extension;
        if (os.contains("mac") || os.contains("darwin")) {
            name = "darwin";
            extension = "dylib";
        } else if (os.contains("linux")) {
            name = "linux";
            extension = "so";
        } else {
            return false;
        }
        // `uname -m` says arm64 where the JVM says aarch64, and the resource is
        // named after the former, so try both rather than pick a side.
        String arch = System.getProperty("os.arch", "");
        List<String> candidates = arch.equals("aarch64")
                ? List.of("arm64", "aarch64")
                : List.of(arch);
        for (String candidate : candidates) {
            String resource = "native/" + name + "-" + candidate
                    + "/libqleverjni." + extension;
            try (InputStream in = QLeverNative.class.getClassLoader()
                    .getResourceAsStream(resource)) {
                if (in == null) { continue; }
                Path copy = Files.createTempFile("libqleverjni", "." + extension);
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

    // Backend.
    public static native long openBackend(String indexBasename);
    public static native void closeBackend(long backend);
    /** The identifier that stands for an unbound component of a triple pattern. */
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
