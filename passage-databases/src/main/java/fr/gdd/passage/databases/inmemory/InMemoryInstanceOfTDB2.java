package fr.gdd.passage.databases.inmemory;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.tdb2.TDB2Factory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Create an in memory database following TDB2 model. Useful for testing.
 **/
public class InMemoryInstanceOfTDB2 {

    Dataset dataset;

    public InMemoryInstanceOfTDB2() {
        dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        // Model containing the 10 first triples of Dataset Watdiv.10M
        // Careful, the order in the DB is not identical to that of the array
        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", InMemoryStatements.cities10).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());

        statementsStream = new ByteArrayInputStream(String.join("\n", InMemoryStatements.cities3).getBytes());
        Model modelA = ModelFactory.createDefaultModel();
        modelA.read(statementsStream, "", Lang.NT.getLabel());

        statementsStream = new ByteArrayInputStream(String.join("\n", InMemoryStatements.city1).getBytes());
        Model modelB = ModelFactory.createDefaultModel();
        modelB.read(statementsStream, "", Lang.NT.getLabel());

        dataset.setDefaultModel(model);
        dataset.addNamedModel("https://graphA.org", modelA);
        dataset.addNamedModel("https://graphB.org", modelB);

    }

    public Dataset getDataset() {
        return dataset;
    }

}