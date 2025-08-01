package fr.gdd.passage.commons.io;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.util.Context;

/**
 * Write the output of registered modules.
 */
public interface ModuleOutputWriter {

    void write(IndentedWriter writer, Context context);

}
