package fr.gdd.passage.commons.generics;

import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Multiple endpoint services might need the same dataset. One cannot blindly
 * create a Backend for each path it gets, since the dataset might already be in use.
 * So instead, we must share the Backend object when the path is common.
 */
public class BackendManager implements AutoCloseable {
    // Tested somewhere where at least a backend is implemented…

    final Map<String, Backend<?,?>> path2backend = new HashMap<>();

    public Backend<?,?> addBackend(String path, BackendFactory<?,?,?> backend) {
        Path absolutePath = Path.of(path).toAbsolutePath();

        return path2backend.computeIfAbsent(absolutePath.toString(), k ->
                // only backend.get if the key does not exist.
                backend.get(absolutePath));
    }

    public int size () {
        return path2backend.size();
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<String, Backend<?,?>> entry : path2backend.entrySet()) {
            entry.getValue().close();
        }
        path2backend.clear();
    }
}
