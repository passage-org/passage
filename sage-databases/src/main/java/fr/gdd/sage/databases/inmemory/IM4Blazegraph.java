package fr.gdd.sage.databases.inmemory;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Provides the same datasets for blazegraph.
 */
public class IM4Blazegraph {

    public static BigdataSail triples3 () { return getDataset(InMemoryStatements.triples3); } // simple
    public static BigdataSail triples6 () { return getDataset(InMemoryStatements.triples6); } // for optional
    public static BigdataSail triples9 () { return getDataset(InMemoryStatements.triples9); } // for random

    /**
     * @return Properties that makes sure the dataset is deleted at the end of each test.
     */
    private static Properties getDefaultProps () {
        final Properties props = new Properties();
        props.put(BigdataSail.Options.CREATE_TEMP_FILE, "true");
        props.put(BigdataSail.Options.DELETE_ON_CLOSE, "true");
        props.put(BigdataSail.Options.DELETE_ON_EXIT, "true");
        return props;
    }

    public static BigdataSail getDataset(List<String> statements) {
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

            connection.add(statementsStream, "", RDFFormat.NTRIPLES);

            connection.commit();
        } catch (RepositoryException | IOException | RDFParseException e) {
            throw new RuntimeException(e);
        }
        return sail;
    }
}
