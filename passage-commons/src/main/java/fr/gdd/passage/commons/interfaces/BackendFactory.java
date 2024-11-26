package fr.gdd.passage.commons.interfaces;

import java.io.Serializable;
import java.nio.file.Path;

/**
 * Creates a backend from a Path.
 */
public interface BackendFactory<ID,VALUE,SKIP extends Serializable> {

    Backend<ID,VALUE,SKIP> get(Path path);

}
