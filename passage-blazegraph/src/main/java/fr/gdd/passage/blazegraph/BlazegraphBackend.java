package fr.gdd.passage.blazegraph;

import com.bigdata.concurrent.TimeoutException;
import com.bigdata.journal.Options;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.rio.ntriples.BigdataNTriplesParser;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.commons.iterators.BackendLazyIterator;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.turtle.TurtleParser;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.*;

/**
 * Backend for Blazegraph providing easy access to the most important
 * feature: the scan iterator.
 * Using an underlying augmented balanced tree
 * for its indexes, Blazegraph allows us to create flexible iterators over
 * triple patterns: iterators that can skip ranges of triples efficiently, or
 * return random triples uniformly at random.
 */
public class BlazegraphBackend implements Backend<IV, BigdataValue>, AutoCloseable {

    private final static Logger log = LoggerFactory.getLogger(BlazegraphBackend.class);

    final AbstractTripleStore store;
    final BigdataSailRepository repository;
    final BigdataSailRepositoryConnection connection;
    final BigdataSail sail;
    final IV defaultGraph; // TODO better handling of this, ie. comes from journal
    final static IV UNION_OF_GRAPHS = null; // TODO better handling of this, ie. comes from journal

    // solely used to parse a term into a bigdatavalue
    final RDFParser parser = new TurtleParser();
    final GetValueStatementHandler handler = new GetValueStatementHandler();

    /**
     * Creates an empty Blazegraph Backend, only useful for debug purpose
     */
    public BlazegraphBackend() throws SailException, RepositoryException {
        System.setProperty("com.bigdata.Banner.quiet", "true"); // banner is annoying, sorry blazegraph
        final Properties props = BlazegraphInMemoryDatasetsFactory.getDefaultProps();

        this.sail = new BigdataSail(props);
        this.repository = new BigdataSailRepository(sail);
        sail.initialize();
        this.connection = repository.getReadOnlyConnection();
        store = connection.getTripleStore();
        defaultGraph = getDefaultGraph();
        parser.setValueFactory(this.connection.getValueFactory());
        parser.setRDFHandler(handler);

    }

    /**
     * Creates a Blazegraph backend using a path to a journal file or a path to a property file.
     * The former is for a quickstart of default journal file; While the latter defines a more
     * specific dataset (must end with `.properties` to be recognized as such).
     * @param path The path to the file defining the dataset. Relative path(s) are relative to
     *             the actual execution folder.
     */
    public BlazegraphBackend(String path) throws SailException, RepositoryException {
        System.setProperty("com.bigdata.Banner.quiet", "true"); // banner is annoying, sorry blazegraph

        Properties props = new Properties();
        if (path.endsWith(".properties")) {
            try {
                props.load(new FileReader(Path.of(path).toFile()));
            } catch (IOException e) {
                throw new RepositoryException(e);
            }
        } else {
            // resort to default, with `path` being the file path.
            props.put(Options.FILE, path);
        }

        this.sail = new BigdataSail(props);
        this.repository = new BigdataSailRepository(sail);
        sail.initialize();
        this.connection = repository.getReadOnlyConnection();
        store = connection.getTripleStore();
        defaultGraph = getDefaultGraph();
        parser.setValueFactory(this.connection.getValueFactory());
        parser.setRDFHandler(handler);
    }

    /**
     * Creates a Blazegraph backend using an already initialized sail repository.
     * @param sail An already initialized sail (blazegraph) repository.
     */
    public BlazegraphBackend(BigdataSail sail) throws RepositoryException {
        System.setProperty("com.bigdata.Banner.quiet", "true"); // banner is annoying, sorry blazegraph
        this.repository = new BigdataSailRepository(sail);
        // this.connection = repository.getConnection();
        this.connection = repository.getReadOnlyConnection();
        this.store = connection.getTripleStore();
        this.sail = sail;
        defaultGraph = getDefaultGraph();
        parser.setValueFactory(this.connection.getValueFactory());
        parser.setRDFHandler(handler);
    }

    @Override
    public void close() throws RepositoryException, SailException {
        connection.close();
        sail.shutDown();
    }

    @Override
    public BackendIterator<IV, BigdataValue> search(IV s, IV p, IV o) {
        return new BackendLazyIterator<>(this,new BlazegraphIterator(store, s, p, o, UNION_OF_GRAPHS));
    }

    @Override
    public BackendIterator<IV, BigdataValue> search(IV s, IV p, IV o, IV c) {
        return new BackendLazyIterator<>(this,new BlazegraphIterator(store, s, p, o, c));
    }

    @Override
    public BackendIterator<IV, BigdataValue> searchDistinct(IV s, IV p, IV o, Set<Integer> codes) {
        // we assume that this is a triple store and that triples are not replicated in different graphs.
        // However, this is subject to change. If so, we need a more consistent way to characterize the
        // default graph, the union of graphs, and a specific graph.
        Set<Integer> codesWithGraphs = new HashSet<>(codes);
        codesWithGraphs.add(SPOC.GRAPH);
        return BlazegraphDistinctIteratorFactory.get(store, s, p, o, UNION_OF_GRAPHS, codesWithGraphs); // TODO add laziness
    }

    @Override
    public BackendIterator<IV, BigdataValue> searchDistinct(IV s, IV p, IV o, IV c, Set<Integer> codes) {
        if (codes.contains(SPOC.GRAPH)) {
            c = BlazegraphDistinctIteratorFactory.FAKE_BIND;
        }
        return BlazegraphDistinctIteratorFactory.get(store, s, p, o, c, codes); // TODO add laziness
    }

    @Override
    public IV getId(String value, int... type) {
        BigdataValue bdValue = getValue(value, type);
        return getId(bdValue, type);
    }

    @Override
    public IV getId(BigdataValue bigdataValue, int... type) {
        // It uses an inefficient function marked deprecated on purpose:
        // this is meant to be used only once per constant. Not everytime a value is needed.
        IV toReturn = store.getIV(bigdataValue);
        if (Objects.isNull(toReturn)) {
            throw new NotFoundException(bigdataValue.toString());
        } else {
            return toReturn;
        }
    }

    @Override
    public String getString(IV id, int... type) {
        return getString(getValue(id, type));
    }

    public String getString(BigdataValue bigdataValue, int... type) {
        return switch (bigdataValue) {
            case BigdataURI uri ->  "<" + uri + ">";
            default -> bigdataValue.toString();
        };
    }

    @Override
    public BigdataValue getValue(IV iv, int... type) {
        return store.getTerm(iv);
    }

    @Override
    public BigdataValue getValue(String valueAsString, int... type) {
        String fakeNTriple = "<:_> <:_> " + valueAsString + " .";
        try {
            parser.parse(new StringReader(fakeNTriple), "");
            return (BigdataValue) handler.get();
        } catch (Exception e) {
            throw new UnsupportedOperationException(valueAsString);
        }
    }

    @Override
    public IV any() { return null; }

    // comes from : <https://github.com/blazegraph/database/blob/829ce8241ec29fddf7c893f431b57c8cf4221baf/bigdata-core/bigdata-rdf/src/java/com/bigdata/rdf/sparql/ast/eval/AST2BOpUpdateContext.java#L140>
    public IV getDefaultGraph() {
        BigdataURI nullGraph = store.getValueFactory().asValue(BigdataSail.NULL_GRAPH);
        return store.addTerm(nullGraph);
    }

    /* ****************************************************************************** */

    /**
     * For debug purposes, this executes the query using blazegraph's engine.
     * However, be aware that it stores values in a multiset which can be
     * inefficient in terms of execution time and memory usage.
     * @param queryString The SPARQL query to execute as a string.
     */
    public Multiset<BindingSet> executeQuery(String queryString) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult result = tupleQuery.evaluate();
        Multiset<BindingSet> results = HashMultiset.create();
        while (result.hasNext()) {
            results.add(result.next());
        }
        return results;
    }

    public Iterator<BindingSet> executeQueryToIterator(String queryString) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult result = tupleQuery.evaluate();
        return new Iterator<BindingSet>() {
            @Override
            public boolean hasNext() {
                try {
                    return result.hasNext();
                } catch (QueryEvaluationException e) {
                    return false;
                }
            }

            @Override
            public BindingSet next() {
                try {
                    return result.next();
                } catch (QueryEvaluationException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public long countQuery(String queryString) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult result = tupleQuery.evaluate();
        long count = 0L;
        while (result.hasNext()) {
            log.debug(result.next().toString());
            count+=1;
        }
        return count;
    }

    public long countQuery(String queryString, Long timeout) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult result = tupleQuery.evaluate();
        long start = System.currentTimeMillis();
        long count = 0L;
        while (result.hasNext()) {
            log.debug(result.next().toString());
            count+=1;
            if (System.currentTimeMillis() > start + timeout) {
                // /!\ if the timeout happens in hasNext, this does not work
                // before getting a new result.
                throw new TimeoutException(String.valueOf(count));
            }
        }
        return count;
    }

}
