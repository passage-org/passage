package fr.gdd.sage.blazegraph;

import com.bigdata.journal.Options;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
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
import fr.gdd.sage.exceptions.NotFoundException;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.util.Objects;
import java.util.Properties;

/**
 * Backend for Blazegraph providing easy access to the most important
 * feature: the scan iterator.
 */
public class BlazegraphBackend implements Backend<IV, BigdataValue, Long> {

    AbstractTripleStore store;
    BigdataSailRepository repository;
    BigdataSailRepositoryConnection connection;

    public BlazegraphBackend(String path) {
        final Properties props = new Properties();
        props.put(Options.FILE, path);

        final BigdataSail sail = new BigdataSail(props);
        this.repository = new BigdataSailRepository(sail);
        try {
            sail.initialize();
        } catch (SailException e) {
            e.printStackTrace();
        }
        try {
            this.connection = repository.getReadOnlyConnection();
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        store = connection.getTripleStore();
    }

    /**
     * @param sail An already initialized sail (blazegraph) repository.
     */
    public BlazegraphBackend(BigdataSail sail) {
        this.repository = new BigdataSailRepository(sail);
        try {
            this.connection = repository.getReadOnlyConnection();
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        store = connection.getTripleStore();
    }

    public void close() {
        try {
            connection.close();
            store = null;
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BackendIterator<IV, BigdataValue, Long> search(IV s, IV p, IV o) {
        return new LazyIterator<>(this,new BlazegraphIterator(store, s, p, o, null));
    }

    @Override
    public BackendIterator<IV, BigdataValue, Long> search(IV s, IV p, IV o, IV c) {
        return new LazyIterator<>(this,new BlazegraphIterator(store, s, p, o, c));
    }

    @Override
    public IV getId(String value, int... type) {
        Resource res = null;
        // TODO not only URIs
        // TODO could use `Node node = NodeFactoryExtra.parseNode(value);`
        if (value.startsWith("<") && value.endsWith(">")) {
            res = new URIImpl(value.substring(1, value.length()-1));
        } else {
            throw new UnsupportedOperationException("parse value to resource");
        }

        if (Objects.isNull(type) || type.length == 0) { // ugly when type is not set
            try {
                return getId(value, SPOC.SUBJECT);
            } catch (NotFoundException e) {
                try {
                    return getId(value, SPOC.PREDICATE);
                } catch (NotFoundException f) {
                    try {
                        return getId(value, SPOC.OBJECT);
                    } catch (NotFoundException g) {
                        return getId(value, SPOC.CONTEXT);
                    }
                }
            }
        }

        IAccessPath<ISPO> accessPath = switch(type[0]) {
            case SPOC.SUBJECT -> store.getAccessPath(res,null, null);
            case SPOC.PREDICATE -> store.getAccessPath(null, (URIImpl) res, null);
            case SPOC.OBJECT -> store.getAccessPath(null,null, res);
            case SPOC.CONTEXT -> store.getAccessPath(null,null, null, res);
            default -> throw new UnsupportedOperationException("Unknown SPOC…");
        };
        IChunkedOrderedIterator<ISPO> it = accessPath.iterator();
        if (!it.hasNext()) throw new NotFoundException("The value "); // not found
        ISPO spo = it.next();
        // Get the Value from the index
        String str = switch(type[0]){
            case SPOC.SUBJECT -> spo.getSubject().toString();
            case SPOC.PREDICATE -> spo.getPredicate().toString();
            case SPOC.OBJECT -> spo.getObject().toString();
            case SPOC.CONTEXT-> spo.getContext().toString();
            default -> throw new IllegalStateException("Unexpected value: " + type[0]);
        };
        it.close();
        return TermId.fromString(str);
    }

    @Override
    public IV getId(BigdataValue bigdataValue, int... type) {
        throw new UnsupportedOperationException("TODO"); // TODO
    }

    @Override
    public String getString(IV value, int... type) {
        if (value.isURI()) {
            return "<"+ store.getLexiconRelation().getTerm(value).toString() + ">";
        } else {
            return store.getLexiconRelation().getTerm(value).toString();
        }
    }

    @Override
    public BigdataValue getValue(IV iv, int... type) {
        throw new UnsupportedOperationException("TODO"); // TODO
    }

    @Override
    public IV any() {
        return null;
    }

    /**
     * For debug purposes, this executes the query using blazegraph's engine.
     * @param queryString The SPARQL query to execute as a string.
     * @throws RepositoryException
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
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

}
