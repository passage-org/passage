package fr.gdd.passage.volcano.writers;

import fr.gdd.passage.volcano.PassageConstants;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.io.JSWriter;
import org.apache.jena.sparql.util.Context;

import java.util.Objects;

/**
 * Write a SageOutput to an out-stream.
 */
public class OutputWriterJSONSage implements ModuleOutputWriter {

    @Override
    public void write(IndentedWriter writer, Context context) {
        if (context.isUndef(PassageConstants.PAUSED) || context.isUndef(PassageConstants.PAUSED_STATE)) {
            return; // nothing to do, nothing to save
        }

//        Boolean isPaused = context.isTrue(SagerConstants.PAUSED);
//        if (!isPaused) {
//            throw new RuntimeException("Should be in paused state…");
//        }

        PassageSavedState savedString = context.get(PassageConstants.PAUSED_STATE);

        if (Objects.isNull(savedString.getState())) {
            return; // no next, we are done!
        }
        // otherwise, we add a field to the response.
        writer.print(" ,");
        writer.print(JSWriter.outputQuotedString("metadata"));
        writer.print(" : {");
        writer.print(JSWriter.outputQuotedString("next"));
        writer.println(" : ");
        writer.println(JSWriter.outputQuotedString(savedString.getState()));
        writer.print("}"); // end metadata
    }

}
