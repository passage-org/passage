package fr.gdd.passage.qlever;

import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendFactory;

import java.nio.file.Path;

/**
 * Opens a QLever index from its path, so that a QLever backend can be used
 * wherever Passage takes a {@link BackendFactory}, e.g. through
 * `fr.gdd.passage.commons.generics.BackendManager`.
 *
 * <p>NOTE: the path is the *basename* of the index, i.e. the `-i` argument that
 * `qlever-index` was called with, not a single file: an index is a set of files
 * sharing that prefix. The shared library is located by
 * {@link QLeverNative#load()}, since this interface passes the index and
 * nothing else.
 */
public class QLeverBackendFactory implements BackendFactory<Long, String, Long> {

    @Override
    public Backend<Long, String> get(Path path) {
        return new QLeverBackend(path.toString());
    }

}
