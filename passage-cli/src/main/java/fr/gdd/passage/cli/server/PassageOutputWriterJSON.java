package fr.gdd.passage.cli.server;

import fr.gdd.passage.commons.io.ModuleOutputWriter;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassagePaused;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.io.JSWriter;
import org.apache.jena.sparql.util.Context;

import java.util.Objects;

/**
 * Write continuation query of Passage as an output stream, in
 * a `next` field like triple pattern fragment servers do.
 */
public class PassageOutputWriterJSON implements ModuleOutputWriter {

    @Override
    public void write(IndentedWriter writer, Context context) {
        if (context.isUndef(PassageConstants.PAUSED)) {
            return; // nothing to do, nothing to save
        }

        PassagePaused savedString = context.get(PassageConstants.PAUSED);

        if (Objects.isNull(savedString.getPausedQueryAsString())) {
            return; // no next, we are done!
        }

        // otherwise, we add a `next` field to the response.
        writer.print(JSWriter.outputQuotedString("next"));
        writer.println(" : ");
        writer.println(JSWriter.outputQuotedString(savedString.getPausedQueryAsString()));
    }

}
