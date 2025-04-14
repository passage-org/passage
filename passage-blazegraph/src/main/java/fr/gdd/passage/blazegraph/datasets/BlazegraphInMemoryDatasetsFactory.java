package fr.gdd.passage.blazegraph.datasets;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import fr.gdd.passage.commons.utils.InMemoryStatements;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Provides small datasets for blazegraph that are ideal to create some
 * unit tests.
 */
public class BlazegraphInMemoryDatasetsFactory {

    public static BigdataSail triples3 () { return getDataset(InMemoryStatements.triples3); }
    public static BigdataSail triples6 () { return getDataset(InMemoryStatements.triples6); }
    public static BigdataSail triples9 () { return getDataset(InMemoryStatements.triples9); }
    public static BigdataSail triples9PlusLiterals () { return getDataset(InMemoryStatements.triples9PlusLiterals); }

    public static BigdataSail vendor0 () { return getDataset(InMemoryStatements.vendor0); }
    public static BigdataSail rating_site_41 () { return getDataset(InMemoryStatements.rating_site_41); }

    // for now, stars are not supported by Blazegraph. It needs a workaround.
    // public static BigdataSail stars () { return getDataset(InMemoryStatements.triples9PlusLiteralsPlusStars); }

    public static BigdataSail graph3 () {
        List<String> graphs = new ArrayList<>();
        graphs.addAll(InMemoryStatements.graphA);
        graphs.addAll(InMemoryStatements.graphB);
        graphs.addAll(InMemoryStatements.graphC);
        return getDataset(graphs);
    }

    /**
     * @return Properties that makes sure the dataset is deleted at the end of each test.
     */
    public static Properties getDefaultProps () {
        final Properties props = new Properties();
        props.put(BigdataSail.Options.CREATE_TEMP_FILE, "true");
        props.put(BigdataSail.Options.DELETE_ON_CLOSE, "true");
        props.put(BigdataSail.Options.DELETE_ON_EXIT, "true");
        props.put(BigdataSail.Options.TRUTH_MAINTENANCE, "false"); // not supported with quads mode…
        props.put(BigdataSail.Options.QUADS_MODE, "true");
        return props;
    }

    public static BigdataSail getDataset(List<String> statements) {
        System.setProperty("com.bigdata.Banner.quiet", "true"); // shhhhh banner shhh
        final BigdataSail sail = new BigdataSail(getDefaultProps());
        try {
            sail.initialize();
        } catch (SailException e) {
            throw new RuntimeException(e);
        }
        BigdataSailRepository repository = new BigdataSailRepository(sail);
        try {
            BigdataSailRepositoryConnection connection = repository.getConnection();

            InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());

            if (statements.getFirst().split("\\s+").length > 3) {
                connection.add(statementsStream, "", RDFFormat.NQUADS);
            } else {
                connection.add(statementsStream, "", RDFFormat.NTRIPLES);
            }

            connection.commit();
            connection.close();
        } catch (RepositoryException | IOException | RDFParseException e) {
            throw new RuntimeException(e);
        }
        return sail;
    }
}
