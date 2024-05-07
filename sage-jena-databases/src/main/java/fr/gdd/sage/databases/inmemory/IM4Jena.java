package fr.gdd.sage.databases.inmemory;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.tdb2.TDB2Factory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

/**
 * Provides datasets of Apache Jena's TDB2.
 */
public class IM4Jena {

    public static Dataset triple3 () { return buildDataset(InMemoryStatements.triples3); } // simple
    public static Dataset triple6 () { return buildDataset(InMemoryStatements.triples6); } // for optional
    public static Dataset triple9 () { return buildDataset(InMemoryStatements.triples9); } // for random

    public static Dataset buildDataset(List<String> statements) {
        Dataset dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());
        dataset.setDefaultModel(model);
        return dataset;
    }

}
