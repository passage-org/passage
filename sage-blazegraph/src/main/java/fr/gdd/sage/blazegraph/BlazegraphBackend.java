package fr.gdd.sage.blazegraph;

import com.bigdata.journal.Options;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;
import fr.gdd.sage.exceptions.NotFoundException;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
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
public class BlazegraphBackend implements Backend<IV, byte[]> {

    AbstractTripleStore store;
    BigdataSailRepository repository;
    BigdataSail.BigdataSailConnection connection;

    public BlazegraphBackend(String path) {
        final Properties props = new Properties();
        props.put(BigdataSail.Options.READ_ONLY, true); // TODO not read only
        props.put(Options.FILE, path);

        final BigdataSail sail = new BigdataSail(props);
        this.repository = new BigdataSailRepository(sail);
        try {
            sail.initialize();
        } catch (SailException e) {
            e.printStackTrace();
        }
        this.connection = sail.getReadOnlyConnection();
        store = connection.getTripleStore();
    }

    public void close() {
        try {
            connection.close();
            store = null;
        } catch (SailException e) {
            e.printStackTrace();
        }
    }

    @Override
    public BackendIterator<IV, byte[]> search(IV s, IV p, IV o, IV... c) {
        return new LazyIterator<>(this,new BlazegraphIterator(store, s, p, o,
                Objects.isNull(c) || c.length == 0 ? null : c[0]));
    }

    @Override
    public IV getId(String value, int... type) {
        // TODO not only URIs
        // TODO could use `Node node = NodeFactoryExtra.parseNode(value);`
        IAccessPath<ISPO> accessPath = switch(type[0]) {
            case SPOC.SUBJECT -> store.getAccessPath(new URIImpl(value),null, null);
            case SPOC.PREDICATE -> store.getAccessPath(null, new URIImpl(value), null);
            case SPOC.OBJECT -> store.getAccessPath(null,null, new URIImpl(value));
            case SPOC.CONTEXT -> store.getAccessPath(null,null, null, new URIImpl(value));
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
        return TermId.fromString(str);
    }

    @Override
    public String getValue(IV value, int... type) {
        return value.getValue().toString();
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
    public void executeQuery(String queryString) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        TupleQuery tupleQuery = this.repository.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult result = tupleQuery.evaluate();
        long start =  System.currentTimeMillis();
        long nbElements = 0;
        while (result.hasNext()) {
            BindingSet bindingSet = result.next();
			/* Value subject = bindingSet.getValue("s");
			/*Value predicate = bindingSet.getValue("p");
			Value object = bindingSet.getValue("o");*/
            // System.out.println(bindingSet);
            ++nbElements;
        }
        long duration = System.currentTimeMillis() - start;
        System.out.println("NbElements = " + nbElements);
        System.out.println("Duration = " + duration + " ms");
    }

}
