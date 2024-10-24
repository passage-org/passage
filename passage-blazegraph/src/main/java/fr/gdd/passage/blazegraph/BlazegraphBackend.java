package fr.gdd.passage.blazegraph;

import com.bigdata.concurrent.TimeoutException;
import com.bigdata.journal.Options;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.generics.LazyIterator;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Backend for Blazegraph providing easy access to the most important
 * feature: the scan iterator.
 * Using an underlying augmented balanced tree
 * for its indexes, Blazegraph allows us to create flexible iterators over
 * triple patterns: iterators that can skip ranges of triples efficiently, or
 * return random triples uniformly at random.
 */
public class BlazegraphBackend implements Backend<IV, BigdataValue, Long> {

    private final static Logger log = LoggerFactory.getLogger(BlazegraphBackend.class);

    AbstractTripleStore store;
    BigdataSailRepository repository;
    BigdataSailRepositoryConnection connection;

    public BlazegraphBackend() throws SailException, RepositoryException {
        System.setProperty("com.bigdata.Banner.quiet", "true"); // banner is annoying, sorry blazegraph
        final Properties props = new Properties();
        props.put(BigdataSail.Options.CREATE_TEMP_FILE, "true");
        props.put(BigdataSail.Options.DELETE_ON_CLOSE, "true");
        props.put(BigdataSail.Options.DELETE_ON_EXIT, "true");

        final BigdataSail sail = new BigdataSail(props);
        this.repository = new BigdataSailRepository(sail);
        sail.initialize();
        this.connection = repository.getReadOnlyConnection();
        store = connection.getTripleStore();
    }

    public BlazegraphBackend(String path) throws SailException, RepositoryException {
        System.setProperty("com.bigdata.Banner.quiet", "true"); // banner is annoying, sorry blazegraph

        final Properties props = new Properties();
        props.put(Options.FILE, path);

        final BigdataSail sail = new BigdataSail(props);
        this.repository = new BigdataSailRepository(sail);
        sail.initialize();
        this.connection = repository.getReadOnlyConnection();
        store = connection.getTripleStore();
    }

    /**
     * @param sail An already initialized sail (blazegraph) repository.
     */
    public BlazegraphBackend(BigdataSail sail) throws RepositoryException {
        System.setProperty("com.bigdata.Banner.quiet", "true"); // banner is annoying, sorry blazegraph
        this.repository = new BigdataSailRepository(sail);
        this.connection = repository.getReadOnlyConnection();
        store = connection.getTripleStore();
    }

    public void close() throws RepositoryException {
        connection.close();
        store = null;
    }

    @Override
    public BackendIterator<IV, BigdataValue, Long> search(IV s, IV p, IV o) {
        return new LazyIterator<>(this,new BlazegraphIterator(store, s, p, o, null));
    }

    @Override
    public BackendIterator<IV, BigdataValue, Long> search(IV s, IV p, IV o, IV c) {
        return new LazyIterator<>(this,new BlazegraphIterator(store, s, p, o, c));
    }

    public BackendIterator<IV, BigdataValue, Long> searchDistinct(IV s, IV p, IV o, Set<Integer> codes) {
        // TODO add laziness
        return BlazegraphDistinctIteratorFactory.get(store, s, p, o, null, codes);
    }

    public BackendIterator<IV, BigdataValue, Long> searchDistinct(IV s, IV p, IV o, IV c, Set<Integer> codes) {
        // TODO add laziness
        return BlazegraphDistinctIteratorFactory.get(store, s, p, o, c, codes);
    }

    @Override
    public IV getId(String value, int... type) {
        // When the type does not exist, we look for the one.
        // Unfortunately, it's not efficient, so we would like to use the most relevant as often as possible
        if (Objects.isNull(type) || type.length == 0) { // ugly when type is not set
            try {
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    return getId(value, SPOC.OBJECT);
                }
                try { // else not a string
                    return getId(value, SPOC.SUBJECT);
                } catch (Exception e) {
                    try {
                        return getId(value, SPOC.PREDICATE);
                    } catch (Exception f) {
                        try {
                            return getId(value, SPOC.OBJECT); // might still be the object
                        } catch (Exception g) {
                            try {
                                return getId(value, SPOC.CONTEXT);
                            } catch (Exception h) { // otherwise, it throws at runtime
                                throw new NotFoundException(value);
                            }
                        }
                    }
                }
            } catch (Exception i) {
                throw new NotFoundException(value);
            }
        }

        IAccessPath<ISPO> accessPath = switch(type[0]) {
            case SPOC.SUBJECT -> {
                Resource res = (value.startsWith("<") && value.endsWith(">")) ?
                        new URIImpl(value.substring(1, value.length()-1)):
                        new URIImpl(value);
                yield store.getAccessPath(res, null, null);
            }
            case SPOC.PREDICATE -> {
                URIImpl uri = (value.startsWith("<") && value.endsWith(">")) ?
                    new URIImpl(value.substring(1, value.length()-1)):
                    new URIImpl(value);
                yield store.getAccessPath(null, uri, null);
            }
            case SPOC.OBJECT -> {
                if (value.startsWith("<") && value.endsWith(">")) {
                    URIImpl uri = new URIImpl(value.substring(1, value.length()-1));
                    yield store.getAccessPath(null,null, uri);
                }
                Value object = (value.startsWith("\"") && value.endsWith("\"")) ?
                        store.getValueFactory().createLiteral(value.substring(1, value.length()-1)):
                        store.getValueFactory().createLiteral(value);

                // The string might be too long to be inlined in the identifier,
                // therefore, we need to proceed differently.
                // IV possiblyNotInline = store.getVocabulary().get(object);
                IV possiblyNotInline = store.getIV(object);
                if (Objects.isNull(possiblyNotInline)) {   // could not be inlined must do something else…
                    BigdataLiteral blob = store.getValueFactory().createLiteral(value.substring(1, value.length()-1), new URIImpl("http://www.w3.org/2001/XMLSchema#string" ));
                    IV possiblyABlob = store.getIV(blob);
                    BigdataValue bdv = store.getLexiconRelation().getTerm(possiblyABlob);
                    yield store.getAccessPath(null,null, bdv);
                }
                BigdataValue bdv = store.getLexiconRelation().getTerm(possiblyNotInline);
                yield store.getAccessPath(null,null, bdv);
            }
            case SPOC.GRAPH -> {
                Resource res = (value.startsWith("<") && value.endsWith(">")) ?
                        new URIImpl(value.substring(1, value.length()-1)):
                        new URIImpl(value);
                yield store.getAccessPath(null, null, null, res);
            }
            default -> throw new UnsupportedOperationException("Unknown SPOC: " + type[0]);
        };

        IChunkedOrderedIterator<ISPO> it = accessPath.iterator();
        if (!it.hasNext()) throw new NotFoundException(value); // not found
        ISPO spo = it.next();

        IV result = switch(type[0]){
            case SPOC.SUBJECT -> get(spo.getSubject());
            case SPOC.PREDICATE -> get(spo.getPredicate());
            case SPOC.OBJECT -> get(spo.getObject());
            case SPOC.CONTEXT-> get(spo.getContext());
            default -> throw new IllegalStateException("Unexpected value: " + type[0]);
        };
        it.close();
        return result;
    }

    private static IV get(Value sOrPOrO) {
        return switch (sOrPOrO) {
            case TermId t -> t;
            case VocabURIByteIV v -> v;
            default -> TermId.fromString(sOrPOrO.toString());
        };
    }

    @Override
    public IV getId(BigdataValue bigdataValue, int... type) {
        throw new UnsupportedOperationException("TODO"); // TODO
    }

    @Override
    public String getString(IV id, int... type) {
        if (id.isURI()) {
            return "<"+ store.getLexiconRelation().getTerm(id).toString() + ">";
        } else {
            return store.getLexiconRelation().getTerm(id).toString();
        }
    }

    @Override
    public BigdataValue getValue(IV iv, int... type) {
        throw new UnsupportedOperationException("TODO"); // TODO
    }

    @Override
    public BigdataValue getValue(String valueAsString, int... type) {
        if (valueAsString.startsWith("<") && valueAsString.endsWith(">")) {
            return store.getValueFactory().asValue(new URIImpl(valueAsString));
        } else {
            return store.getValueFactory().createLiteral(valueAsString);
        }
    }

    @Override
    public IV any() {
        return null;
    }

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
