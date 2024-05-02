package fr.gdd.sage.blazegraph;

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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Creates small dataset to run tests on it.
 */
public class SmallDatasetsForTests {

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

    /**
     * @return A dataset populated with some info about pets.
     */
    public static BigdataSail getPetsDataset() {
        final BigdataSail sail = new BigdataSail(getDefaultProps());
        try {
            sail.initialize();
        } catch (SailException e) {
            throw new RuntimeException(e);
        }
        BigdataSailRepository repository = new BigdataSailRepository(sail);
        try {
            BigdataSailRepositoryConnection connection = repository.getConnection();

            List<String> statements = Arrays.asList(
                    "<http://Alice> <http://address> <http://nantes> .",
                    "<http://Bob>   <http://address> <http://paris>  .",
                    "<http://Carol> <http://address> <http://nantes> .",

                    "<http://Alice> <http://own>     <http://cat> .",
                    "<http://Alice> <http://own>     <http://dog> .",
                    "<http://Alice> <http://own>     <http://snake> .",

                    "<http://cat>   <http://species> <http://feline> .",
                    "<http://dog>   <http://species> <http://canine> .",
                    "<http://snake> <http://species> <http://reptile> ."
            );

            InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());

            connection.add(statementsStream, "", RDFFormat.NTRIPLES);

            connection.commit();
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (RDFParseException e) {
            throw new RuntimeException(e);
        }
        return sail;
    }
}
