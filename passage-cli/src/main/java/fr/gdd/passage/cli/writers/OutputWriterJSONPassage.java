package fr.gdd.passage.cli.writers;

import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.pause.PassagePaused;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.io.JSWriter;
import org.apache.jena.sparql.util.Context;

import java.util.Objects;

/**
 * Write a SageOutput to an out-stream.
 */
public class OutputWriterJSONPassage implements ModuleOutputWriter {

    @Override
    public void write(IndentedWriter writer, Context context) {
        if (context.isUndef(PassageConstants.PAUSED)) {
            return; // nothing to do, nothing to save
        }

//        Boolean isPaused = context.isTrue(SagerConstants.PAUSED);
//        if (!isPaused) {
//            throw new RuntimeException("Should be in paused state…");
//        }

        PassagePaused savedString = context.get(PassageConstants.PAUSED);

        if (Objects.isNull(savedString.getPausedQueryAsString())) {
            return; // no next, we are done!
        }
        // otherwise, we add a field to the response.
        writer.print(" ,");
        writer.print(JSWriter.outputQuotedString("metadata"));
        writer.print(" : {");
        writer.print(JSWriter.outputQuotedString("next"));
        writer.println(" : ");
        writer.println(JSWriter.outputQuotedString(savedString.getPausedQueryAsString()));
        writer.print("}"); // end metadata
    }

}
