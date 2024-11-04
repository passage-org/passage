package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.commons.io.PassageInput;
import fr.gdd.passage.commons.io.PassageOutput;
import fr.gdd.passage.databases.inmemory.IM4Jena;
import fr.gdd.raw.tdb2.JenaBackend;
import fr.gdd.raw.tdb2.SerializableRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testing the pausing/resuming capabilities of {@link PreemptJenaIterator}.
 * The results should always be identical to that of normal iterators.
 **/
public class PreemptJenaIteratorTest {

    static Dataset dataset = null;
    static JenaBackend backend = null;

    static NodeId predicate = null;
    static NodeId any = null;

    @BeforeAll
    public static void initializeDB() {
        dataset = IM4Jena.graph3();

        backend = new JenaBackend(dataset);
        predicate = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        any = backend.any();
    }

    @AfterAll
    public static void closeDB() {
        TDBInternal.expel(dataset.asDatasetGraph());
    }

    @Test
    //  related to issue#7
    public void read_all_using_fully_unbounded_triple_pattern() {
        BackendIterator<NodeId, Node, SerializableRecord> it = backend.search(any, any, any);
        int nbResults = 0;
        while (it.hasNext()) {
            it.next();
            nbResults += 1;
        }
        assertEquals(dataset.getDefaultModel().size(), nbResults);
    }

    @Test
    public void read_the_whole_using_predicate() {
        BackendIterator<NodeId, Node, SerializableRecord> it = backend.search(any, predicate, any);
        int nbResults = 0;
        while (it.hasNext()) {
            it.next();
            nbResults += 1;
        }
        assertEquals(dataset.getDefaultModel().size(), nbResults);
    }

    @Test
    public void read_only_city_2() {
        NodeId city_2 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City102>");
        BackendIterator<NodeId, Node, SerializableRecord> it = backend.search(city_2, any, any);
        int nbResults = 0;
        while (it.hasNext()) {
            it.next();
            nbResults += 1;
        }
        assertEquals(1, nbResults );
        // only one triple so we are sure of its values
        assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/Country17", it.getString(SPOC.OBJECT));
        assertEquals("http://www.geonames.org/ontology#parentCountry", it.getString(SPOC.PREDICATE));
        assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/City102", it.getString(SPOC.SUBJECT));
    }

    @Test
    public void read_first_then_pause_resume() {
        ArrayList<String> subjects = new ArrayList<>();
        ArrayList<String> predicates = new ArrayList<>();
        ArrayList<String> objects = new ArrayList<>();
        fillWithSolutions(subjects, predicates, objects, any, predicate, any);

        BackendIterator<NodeId, Node, SerializableRecord> it = backend.search(any, predicate, any);
        assert(it.hasNext());
        it.next();
        assertTriple(subjects, predicates, objects, 0, it);

        assertNotNull(it.current());
        BackendIterator<NodeId, Node, SerializableRecord> it2 = backend.search(any, predicate, any);
        it2.skip(it.current());
        assert(it2.hasNext());
        it2.next();
        assertTriple(subjects,predicates,objects, 1, it2);

        BackendIterator<NodeId, Node, SerializableRecord> it3 = backend.search(any, predicate, any);
        it3.skip(it2.current());
        assert(it3.hasNext());
        it3.next();
        assertTriple(subjects,predicates,objects, 2, it3);
    }


    @Test
    public void read_half_pause_resume_then_the_rest() {
        ArrayList<String> subjects = new ArrayList<>();
        ArrayList<String> predicates = new ArrayList<>();
        ArrayList<String> objects = new ArrayList<>();
        fillWithSolutions(subjects, predicates, objects, any, predicate, any);

        BackendIterator<NodeId, Node, SerializableRecord> it = backend.search(any, predicate, any);
        int nbResults = 0;
        int stoppingIndex = 5;
        while (nbResults < stoppingIndex && it.hasNext()) { // carefully call hasNext after stopping
            it.next();
            nbResults += 1;
        }

        assertEquals(subjects.get(stoppingIndex - 1), it.getString(SPOC.SUBJECT));
        assertEquals(predicates.get(stoppingIndex - 1), it.getString(SPOC.PREDICATE));
        assertEquals(objects.get(stoppingIndex - 1), it.getString(SPOC.OBJECT));

        SerializableRecord sr = it.current();
        assertEquals(stoppingIndex, sr.getOffset());

        BackendIterator<NodeId, Node, SerializableRecord> it2 = backend.search(any, predicate, any);
        it2.skip(it.current());

        int nbResultsFinish = 0;
        while (it2.hasNext()) {
            it2.next();
            assertEquals(subjects.get(stoppingIndex + nbResultsFinish), it2.getString(SPOC.SUBJECT));
            assertEquals(predicates.get(stoppingIndex + nbResultsFinish), it2.getString(SPOC.PREDICATE));
            assertEquals(objects.get(stoppingIndex + nbResultsFinish), it2.getString(SPOC.OBJECT));
            nbResultsFinish += 1;
        }

        assertEquals(dataset.getDefaultModel().size(), nbResultsFinish + nbResults);

        sr = it2.current();
        assertEquals(dataset.getDefaultModel().size(), sr.getOffset());
    }


    @Test
    public void nested_scans_with_stop_resume_at_first() {
        NodeId city_2 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City102>");
        BackendIterator<NodeId, Node, SerializableRecord> it = backend.search(city_2, any, any);

        PassageOutput<SerializableRecord> output = new PassageOutput<>();

        // #1 first part of the query, we stop at first result
        int nb_results = 0;
        while (it.hasNext()) {
            it.next();
            BackendIterator<NodeId, Node, SerializableRecord> it_2 = backend.search(any, it.getId(SPOC.PREDICATE), any);
            while (it_2.hasNext()) {
                it_2.next();
                nb_results += 1;
                if (nb_results >= 1) { // limit 1
                    assertNull(it.previous());
                    output.save(new ImmutablePair<>(0, it.previous()), new ImmutablePair<>(1, it_2.current()));

                    // Both are null since they both stop at their first iteration
                    SerializableRecord sr = it.previous();
                    SerializableRecord sr_2 = it_2.previous();
                    assertNull(sr);
                    assertNull(sr_2);

                    break;
                }
            }
        }

        assertEquals(1, nb_results);

        // #2 second part of the query, we restart from the first result, then we run till the end.
        BackendIterator<NodeId, Node, SerializableRecord> it_resume = backend.search(city_2, any, any);
        it_resume.skip(output.getState().get(0));
        while (it_resume.hasNext()) {
            it_resume.next();
            BackendIterator<NodeId, Node, SerializableRecord> it_2_resume = backend.search(any, it.getId(SPOC.PREDICATE), any);
            if (output.getState().containsKey(1)) {
                SerializableRecord to = output.getState().remove(1);
                it_2_resume.skip(to);
            }
            while (it_2_resume.hasNext()) {
                it_2_resume.next();
                nb_results += 1;
            }
        }

        assertEquals(10, nb_results);
    }

    @Test
    // related to issue #13
    public void check_singleton_pause_and_resume() {
        NodeId city_2 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City102>");
        NodeId parentCountry = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        NodeId country_17 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country17>");

        // #A first run to check that singleton works well
        long sum = 0;
        BackendIterator<NodeId, Node, SerializableRecord> itSingleton = backend.search(city_2, parentCountry, country_17);
        while (itSingleton.hasNext()) {
            itSingleton.next();
            BackendIterator<NodeId, Node, SerializableRecord> itWithPredicate = backend.search(any, predicate, any);
            while (itWithPredicate.hasNext()) {
                itWithPredicate.next();
                sum += 1;
            }
        }
        assertEquals(10, sum);

        // #B now the same with preemption
        sum = 0;
        itSingleton = backend.search(city_2, parentCountry, country_17);
        PassageOutput<SerializableRecord> output = new PassageOutput<>();
        while (itSingleton.hasNext()) {
            itSingleton.next();
            BackendIterator<NodeId, Node, SerializableRecord> itWithPredicate = backend.search(any, predicate, any);
            while (itWithPredicate.hasNext()) {
                itWithPredicate.next();
                sum += 1;
            }
            // save after loop because we emulate an exceeded timeout
            output.save(new ImmutablePair<>(0, itSingleton.current()));
            break;
        }
        // we stopped yet every result were produced, since we don't know, we must execute till the end.
        assertEquals(10, sum);

        // #C we stopped, so now we resume
        sum = 0;
        itSingleton = backend.search(city_2, parentCountry, country_17);
        if (output.getState().containsKey(0)) {
            itSingleton.skip(output.getState().remove(0));
        }
        while (itSingleton.hasNext()) {
            itSingleton.next();
            BackendIterator<NodeId, Node, SerializableRecord> itWithPredicate = backend.search(any, predicate, any);
            // no key to skip to
            if (output.getState().containsKey(1)) {
                itSingleton.skip(output.getState().remove(1));
            }
            while (itWithPredicate.hasNext()) {
                itWithPredicate.next();
                sum += 1;
            }
        }
        assertEquals(0, sum);
    }

    @Test
    // related to issue #14; but does not seem to be the issue...
    public void check_every_steps_of_preempt_with_singleton_at_the_end() {
        // #A first run to check that singleton works well
        PassageOutput<SerializableRecord> output = execute_vpv_then_spo_till_end(Long.MAX_VALUE);
        assertEquals(10, output.size());

        // #B create a function that will pause/resume every step of the run with sleep and timeout
        output = execute_vpv_then_spo_till_end(1);
        assertEquals(10, output.size());
    }

    private PassageOutput<SerializableRecord> execute_vpv_then_spo_till_end(long timeout) {
        PassageOutput<SerializableRecord> partialOutput = null;
        PassageOutput<SerializableRecord> output = new PassageOutput<>();
        PassageInput<SerializableRecord> input  = new PassageInput<SerializableRecord>().setTimeout(timeout);
        while (Objects.isNull(partialOutput) || (!Objects.isNull(partialOutput.getState()))) {
            partialOutput = execute_vpv_then_spo(input);
            input.setState(partialOutput.getState());
            output.merge(partialOutput);
        }

        return output;
    }

    private PassageOutput<SerializableRecord> execute_vpv_then_spo(PassageInput<SerializableRecord> input) {
        input.setTimeout(input.getTimeout()); // set deadline with CurrentTime

        PassageOutput<SerializableRecord> output = new PassageOutput<>();
        NodeId city_2 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City102>");
        NodeId parentCountry = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        NodeId country_17 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country17>");

        BackendIterator<NodeId, Node, SerializableRecord> itWithPredicate = backend.search(any, predicate, any);
        if (!Objects.isNull(input) && input.getState().containsKey(0)) {
            itWithPredicate.skip(input.getState(0));
        }
        while (itWithPredicate.hasNext()) {
            itWithPredicate.next();

            BackendIterator<NodeId, Node, SerializableRecord> itSingleton = backend.search(city_2, parentCountry, country_17);
            if (!Objects.isNull(input) && input.getState().containsKey(1)) {
                itSingleton.skip(input.getState(1));
            }
            while (itSingleton.hasNext()) {
                itSingleton.next();
                output.add(null); // only +1

                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (System.currentTimeMillis() > input.getDeadline()) {
                    output.save(new ImmutablePair<>(0, itWithPredicate.previous()), new ImmutablePair<>(1, itSingleton.current()));
                    return output;
                }
            }
            if (System.currentTimeMillis() > input.getDeadline()) {
                output.save(new ImmutablePair<>(0, itWithPredicate.current()));
                return output;
            }
        }
        return output;
    }

    /* **************************************************************************************************** */

    /**
     * Convenience function to assert the current values of the iterator compared to the truth.
     */
    static void assertTriple(ArrayList<String> s, ArrayList<String> p, ArrayList<String> o, int index, BackendIterator<?,?,?> it) {
        assertEquals(s.get(index), it.getString(SPOC.SUBJECT));
        assertEquals(p.get(index), it.getString(SPOC.PREDICATE));
        assertEquals(o.get(index), it.getString(SPOC.OBJECT));
    }

    /**
     * Fills the arrays with solutions corresponding to the pattern.
     */
    static void fillWithSolutions(ArrayList<String> subjects, ArrayList<String> predicates, ArrayList<String> objects,
                                  NodeId s, NodeId p, NodeId o) {
        BackendIterator<?,?,?> baseline_it = backend.search(s, p, o);
        while (baseline_it.hasNext()) {
            baseline_it.next();
            subjects.add(baseline_it.getString(SPOC.SUBJECT));
            predicates.add(baseline_it.getString(SPOC.PREDICATE));
            objects.add(baseline_it.getString(SPOC.OBJECT));
        }
    }

}