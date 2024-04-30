package fr.gdd.sage.blazegraph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

class BlazegraphBackendTest {

    @Disabled
    @Test
    public void creating_a_simple_dataset () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend("./temp/dataset.jnl");

        bb.executeQuery("SELECT * WHERE {?s ?p ?o}");
    }

    @Disabled
    @Test
    public void opening_watdiv10m () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");
        bb.executeQuery("""
                SELECT * WHERE {
                    hint:Query hint:optimizer "None" .
                    ?v0 <http://xmlns.com/foaf/age> <http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup2> .
                    ?v0 <http://schema.org/nationality> ?v8 .
                    ?v2 <http://schema.org/eligibleRegion> ?v8 .
                    ?v2 <http://purl.org/goodrelations/includes> ?v3 .
                    }""");
    }

    @Disabled
    @Test
    public void opening_watdiv2 () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

        long start = System.currentTimeMillis();
        final var any = bb.any();
        final var p_1 = bb.getId("http://xmlns.com/foaf/age", SPOC.PREDICATE);
        final var o_1 = bb.getId("http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup2", SPOC.OBJECT);
        final var p_2 = bb.getId("http://schema.org/nationality", SPOC.PREDICATE);
        final var p_3 = bb.getId("http://schema.org/eligibleRegion", SPOC.PREDICATE);
        final var p_4 = bb.getId("http://purl.org/goodrelations/includes", SPOC.PREDICATE);

        BackendIterator<IV, ?> i_1 = bb.search(any, p_1, o_1);

        long nbElements = 0;

        while (i_1.hasNext()) {
            i_1.next();
            BackendIterator<IV, ?> i_2 = bb.search(i_1.getId(SPOC.SUBJECT), p_2, any);
            while (i_2.hasNext()) {
                i_2.next();
                BackendIterator<IV, ?> i_3 = bb.search(any, p_3, i_2.getId(SPOC.OBJECT));
                while (i_3.hasNext()) {
                    i_3.next();
                    BackendIterator<IV, ?> i_4 = bb.search(i_3.getId(SPOC.SUBJECT), p_4, any);
                    while (i_4.hasNext()) {
                        i_4.next();
                        nbElements += 1;
                    }
                }
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Nb elements = " + nbElements);
        System.out.println("Duration = " + elapsed + " ms");
    }

    @Disabled
    @Test
    public void test_skip() {
        BlazegraphBackend bb = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

        final var any = bb.any();
        final var p_1 = bb.getId("http://xmlns.com/foaf/age", SPOC.PREDICATE);
        BackendIterator<IV, ?> i_1 = bb.search(any, p_1, any);

        BlazegraphIterator.RNG = new Random(1);

        BlazegraphIterator bi = (BlazegraphIterator) ((LazyIterator) i_1).getWrapped();

        System.out.println(bi.cardinality());

        Set<String> results = new HashSet<>();

        for (int i = 0; i < 1_000_000; ++i) {
            ISPO r = bi.random();
            results.add(r.toString(bb.store));
        }
    }

}