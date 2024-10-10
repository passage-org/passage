package fr.gdd.passage.cli.writers;

import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter;

import java.io.OutputStream;

public class SPARQLJSONWriterMetadata extends SPARQLResultsJSONWriter {

    public SPARQLJSONWriterMetadata(OutputStream out) {
        super(out);
    }

}
