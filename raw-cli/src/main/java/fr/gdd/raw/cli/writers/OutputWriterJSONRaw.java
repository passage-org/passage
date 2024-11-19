package fr.gdd.raw.cli.writers;

import fr.gdd.passage.cli.writers.ModuleOutputWriter;
import fr.gdd.raw.executor.RawConstants;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.io.JSWriter;
import org.apache.jena.sparql.util.Context;

import java.util.List;

public class OutputWriterJSONRaw implements ModuleOutputWriter {

    @Override
    public void write(IndentedWriter writer, Context context) {

        List<Double> probas = context.get(RawConstants.SCAN_PROBABILITIES);
        Long attempts = RawConstants.getRandomWalkAttempts(context);
        // otherwise, we add a field to the response.
        writer.print(" ,");
        writer.print(JSWriter.outputQuotedString("metadata"));
        writer.println(" : {");

        writer.incIndent();
        writer.print(JSWriter.outputQuotedString("cardinalities"));
        writer.print(" : ");
        writer.println("[");

        writer.incIndent();
        boolean first = true;
        for (Double probability : probas) {
            if(!first) writer.println(",");
            first = false;
            writer.print(JSWriter.outputQuotedString(String.valueOf(probability)));
        }

        writer.decIndent();
        writer.println("],");

        writer.print(JSWriter.outputQuotedString("attempts"));
        writer.print(" : ");
        writer.println(attempts);

        writer.decIndent();
        writer.println("}"); // end metadata
    }

}