package org.apache.jena.dboe.trans.bplustree;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.commons.iterators.BackendLazyIterator;
import fr.gdd.raw.tdb2.ArtificialSkewedGraph;
import fr.gdd.raw.tdb2.JenaBackend;
import fr.gdd.raw.tdb2.datasets.TDB2InMemoryDatasetsFactory;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.NodeId;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testing the randomness features of a triple pattern iterator.
 */
public class RandomAccessInBTreeTest {

    static Integer DISTINCT = 100;
    Dataset dataset = new ArtificialSkewedGraph(DISTINCT, 10).getDataset();

    @Test
    public void spo_in_small_dataset() {
        JenaBackend backend = new JenaBackend(TDB2InMemoryDatasetsFactory.triple9());
        BackendIterator<NodeId, Node> it = backend.search(backend.any(), backend.any(), backend.any());

        Multiset<String> expected = HashMultiset.create();
        while (it.hasNext()) {
            it.next();
            expected.add(it.getString(SPOC.SUBJECT) + " " + it.getString(SPOC.PREDICATE) + " " + it.getString(SPOC.OBJECT));
        }

        Multiset<String> results = HashMultiset.create();
        for (int i = 0; i < 1000; ++i) {
            it.random();
            it.next();
            results.add(it.getString(SPOC.SUBJECT) + " " + it.getString(SPOC.PREDICATE) + " " + it.getString(SPOC.OBJECT));
        }
        assertEquals(1000, results.size());
        assertEquals(expected.elementSet().size(), results.elementSet().size());
        expected.elementSet().forEach(e -> assertTrue(results.contains(e)));
        results.elementSet().forEach(e -> assertTrue(expected.contains(e)));
    }

    @Test
    public void predicate_bounded_in_small_dataset() {
        JenaBackend backend = new JenaBackend(TDB2InMemoryDatasetsFactory.triple9());
        NodeId address = backend.getId("<http://address>");
        BackendIterator<NodeId, Node> it = backend.search(backend.any(), address, backend.any());

        Multiset<String> results = HashMultiset.create();
        for (int i = 0; i < 1000; ++i) {
            it.random();
            it.next();
            results.add(it.getString(SPOC.SUBJECT) + " " + it.getString(SPOC.PREDICATE) + " " + it.getString(SPOC.OBJECT));
        }
        assertEquals(1000, results.size());
        assertEquals(3, results.elementSet().size());
    }


    @Test
    public void range_iterator_with_known_number_of_distinct_values() {
        JenaBackend backend = new JenaBackend(dataset);
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((BackendLazyIterator) backend.search(backend.any(), is_a, prof)).getWrapped();

        Set<Record> randomUniformValues = new HashSet<>();
        Set<Record> randomValues = new HashSet<>();

        for (int i = 0; i < 100000; ++i) {
            randomValues.add(iterator.getRandom());
            randomUniformValues.add(iterator.getUniformRandom());
        }
        // with (very) high probability, this works
        assertEquals(DISTINCT, randomValues.size());
        assertEquals(DISTINCT, randomUniformValues.size());
        assertEquals(DISTINCT, (int) iterator.cardinality(Long.MAX_VALUE));
    }

    @Test
    public void range_iterator_that_does_not_exist() {
        JenaBackend backend = new JenaBackend(dataset);
        NodeId prof = backend.getId("<http://Prof>");
        NodeId group = backend.getId("<http://group_1>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((BackendLazyIterator) backend.search(group, backend.any(), prof)).getWrapped();

        Record random = iterator.getRandom();
        Record randomUniform = iterator.getUniformRandom();
        assertNull(randomUniform);
        assertNull(random);
    }

    @Test
    public void range_iterator_where_there_is_only_one_result() {
        Dataset dataset = new ArtificialSkewedGraph(1, 10).getDataset();
        JenaBackend backend = new JenaBackend(dataset);
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((BackendLazyIterator) backend.search(backend.any(), is_a, prof)).getWrapped();

        Record random = iterator.getRandom();
        Record randomUniform = iterator.getUniformRandom();
        assertNotNull(randomUniform);
        assertNotNull(random);
        assertEquals(random, randomUniform);
        assertEquals(1, iterator.count());
        assertEquals(1, iterator.cardinality(Long.MAX_VALUE));
    }

    @Test
    public void a_specific_bounded_triple_that_exists() {
        Dataset dataset = new ArtificialSkewedGraph(1, 10).getDataset();
        JenaBackend backend = new JenaBackend(dataset);
        NodeId prof1 = backend.getId("<http://prof_0>");
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((BackendLazyIterator) backend.search(prof1, is_a, prof)).getWrapped();

        Record random = iterator.getRandom();
        Record randomUniform = iterator.getUniformRandom();
        assertNotNull(randomUniform);
        assertNotNull(random);
        assertEquals(random, randomUniform);
    }

    @Test
    public void a_specific_bounded_triple_that_does_not_exists() {
        Dataset dataset = new ArtificialSkewedGraph(1, 10).getDataset();
        JenaBackend backend = new JenaBackend(dataset);
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((BackendLazyIterator) backend.search(prof, is_a, prof)).getWrapped();

        Record random = iterator.getRandom();
        Record randomUniform = iterator.getUniformRandom();
        assertNull(randomUniform);
        assertNull(random);
    }

    @Test
    public void identifier_appear_in_spo_order_whatever_the_index_used() {
        Dataset dataset = new ArtificialSkewedGraph(1, 10).getDataset();
        JenaBackend backend = new JenaBackend(dataset);
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((BackendLazyIterator) backend.search(backend.any(), is_a, prof)).getWrapped();

        Tuple<NodeId> spo = iterator.getRandomSPO();
        // since we look for ?prof <http://is_a> <http://Prof>, the index used is POS.
        // Using Record, the subject identifier would be last, but using the iterator
        // itself allows reordering so the subject comes first, as expected.
        assertEquals("http://prof_0", backend.getString(spo.get(0)));

        Tuple<NodeId> spoUniform = iterator.getUniformRandomSPO();
        assertEquals("http://prof_0", backend.getString(spoUniform.get(0)));
    }

    @Disabled
    @Test
    public void empty_iterator_was_infinitely_looping_when_empty () {
        JenaBackend backend = new JenaBackend("../../FedUP/temp/fedup-id");
        NodeId predicate = backend.getId("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>");

        NodeId graph = backend.getId("<http://www.vendor88.fr/>");
        BackendLazyIterator iterator =  (BackendLazyIterator) backend.search(backend.any(), predicate, backend.any(), graph);
        ProgressJenaIterator progress = (ProgressJenaIterator) iterator.getWrapped();
        assertNull(progress.getRandomSPOWithProbability().getLeft());
    }

}
