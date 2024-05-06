package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.ArtificallySkewedGraph;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.jena.JenaBackend;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.base.Sys;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.NodeId;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testing the randomness features of a triple pattern iterator.
 */
public class RandomAccessInBTreeTest {

    static Integer DISTINCT = 100;
    Dataset dataset = new ArtificallySkewedGraph(DISTINCT, 10).getDataset();

    @Test
    public void range_iterator_with_known_number_of_distinct_values() {
        JenaBackend backend = new JenaBackend(dataset);
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((LazyIterator) backend.search(backend.any(), is_a, prof)).getWrapped();

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
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((LazyIterator) backend.search(group, backend.any(), prof)).getWrapped();

        Record random = iterator.getRandom();
        Record randomUniform = iterator.getUniformRandom();
        assertNull(randomUniform);
        assertNull(random);
    }

    @Test
    public void range_iterator_where_there_is_only_one_result() {
        Dataset dataset = new ArtificallySkewedGraph(1, 10).getDataset();
        JenaBackend backend = new JenaBackend(dataset);
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((LazyIterator) backend.search(backend.any(), is_a, prof)).getWrapped();

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
        Dataset dataset = new ArtificallySkewedGraph(1, 10).getDataset();
        JenaBackend backend = new JenaBackend(dataset);
        NodeId prof1 = backend.getId("<http://prof_0>");
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((LazyIterator) backend.search(prof1, is_a, prof)).getWrapped();

        Record random = iterator.getRandom();
        Record randomUniform = iterator.getUniformRandom();
        assertNotNull(randomUniform);
        assertNotNull(random);
        assertEquals(random, randomUniform);
    }

    @Test
    public void a_specific_bounded_triple_that_does_not_exists() {
        Dataset dataset = new ArtificallySkewedGraph(1, 10).getDataset();
        JenaBackend backend = new JenaBackend(dataset);
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((LazyIterator) backend.search(prof, is_a, prof)).getWrapped();

        Record random = iterator.getRandom();
        Record randomUniform = iterator.getUniformRandom();
        assertNull(randomUniform);
        assertNull(random);
    }

    @Test
    public void identifier_appear_in_spo_order_whatever_the_index_used() {
        Dataset dataset = new ArtificallySkewedGraph(1, 10).getDataset();
        JenaBackend backend = new JenaBackend(dataset);
        NodeId is_a = backend.getId("<http://is_a>");
        NodeId prof = backend.getId("<http://Prof>");
        ProgressJenaIterator iterator = (ProgressJenaIterator) ((LazyIterator) backend.search(backend.any(), is_a, prof)).getWrapped();

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
        LazyIterator iterator =  (LazyIterator) backend.search(backend.any(), predicate, backend.any(), graph);
        ProgressJenaIterator progress = (ProgressJenaIterator) iterator.getWrapped();
        assertNull(progress.getRandomSPOWithProbability().getLeft());
    }

}
