package fr.gdd.passage.databases.inmemory;

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

    public static Dataset triple3 () { return buildDataset(InMemoryStatements.triples3); }
    public static Dataset triple6 () { return buildDataset(InMemoryStatements.triples6); }
    public static Dataset triple9 () { return buildDataset(InMemoryStatements.triples9); }
    public static Dataset triples9PlusLiterals () { return buildDataset(InMemoryStatements.triples9PlusLiterals); }
    public static Dataset stars () { return buildDataset(InMemoryStatements.triples9PlusLiteralsPlusStars); }

    public static Dataset graph3 () { // called `InMemoryInstanceOfTDB2 before.
        Dataset built = buildDataset(InMemoryStatements.cities10);
        addGraphToDataset(built, "https://graphA.org", InMemoryStatements.cities3);
        addGraphToDataset(built, "https://graphB.org", InMemoryStatements.city1);
        return built;
    }

    /**
     * @param statements The NT statements that compose the graph.
     * @return The dataset comprising the triples as default graph.
     */
    public static Dataset buildDataset(List<String> statements) {
        Dataset dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());
        dataset.setDefaultModel(model);
        dataset.commit();
        dataset.end();
        return dataset;
    }

    /**
     * @param dataset The dataset to update with a new graph.
     * @param graphName The graph name.
     * @param statements The statements for the graph.
     */
    public static void addGraphToDataset(Dataset dataset, String graphName, List<String> statements) {
        dataset.begin(ReadWrite.WRITE);

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());

        dataset.addNamedModel(graphName, model);
        dataset.commit();
        dataset.end();
        dataset.close();
    }

}
