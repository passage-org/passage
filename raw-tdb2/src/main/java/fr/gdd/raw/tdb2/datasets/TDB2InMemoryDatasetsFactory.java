package fr.gdd.raw.tdb2.datasets;

import fr.gdd.passage.commons.utils.InMemoryStatements;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.loader.DataLoader;
import org.apache.jena.tdb2.loader.LoaderFactory;
import org.apache.jena.tdb2.loader.base.LoaderOps;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Provides datasets of Apache Jena's TDB2.
 */
public class TDB2InMemoryDatasetsFactory {

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


    /**
     * Ingest the whitelisted files in the Jena database.
     * @param dbPath The path to the database.
     * @param extractedPath The directory location of extracted files.
     * @param whitelist The whitelisted files to ingest.
     */
    static public void ingest(Path dbPath, Path extractedPath, List<String> whitelist) {
        Dataset dataset = TDB2Factory.connectDataset(dbPath.toString());

        for (String whitelisted : whitelist) {
            Path entryExtractPath = extractedPath.resolve(whitelisted);
            // (TODO) model: default or union ?
            DataLoader loader = LoaderFactory.parallelLoader(dataset.asDatasetGraph(), LoaderOps.outputToLog());
            loader.startBulk();
            loader.load(entryExtractPath.toAbsolutePath().toString());
            loader.finishBulk();
        }
    }

}
