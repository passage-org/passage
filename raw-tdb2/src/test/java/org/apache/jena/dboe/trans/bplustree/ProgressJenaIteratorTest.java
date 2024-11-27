package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.commons.iterators.BackendLazyIterator;
import fr.gdd.raw.tdb2.ArtificialSkewedGraph;
import fr.gdd.raw.tdb2.JenaBackend;
import fr.gdd.raw.tdb2.datasets.TDB2InMemoryDatasetsFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProgressJenaIteratorTest {

    private static final Logger log = LoggerFactory.getLogger(ProgressJenaIteratorTest.class);

    static Dataset dataset = null;
    static JenaBackend backend = null;

    static NodeId predicate = null;
    static NodeId any = null;

    @BeforeAll
    public static void initializeDB() {
        dataset = TDB2InMemoryDatasetsFactory.graph3();

        backend = new JenaBackend(dataset);
        predicate = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        any = backend.any();
    }

    @AfterAll
    public static void closeDB() {
        TDBInternal.expel(dataset.asDatasetGraph());
    }

    /* ********************************************************************************************* */
    // Since we don't know for sure the error on cardinality, we cannot set appropriate assertions

    @Disabled
    @Test
    public void cardinality_that_seems_not_good_watdiv() {
        // Comes from the observation that with 2k random walks, we
        // converge towards a cardinality that is not good
        // [main] DEBUG fr.gdd.sage.arq.SageOptimizer - triple ?v0 @http://xmlns.com/foaf/familyName ?v1 => 64861 elements
        // [main] DEBUG fr.gdd.sage.arq.SageOptimizer - triple ?v0 @http://xmlns.com/foaf/givenName ?v2 => 68338 elements
        // new Watdiv10M(Optional.of("../target"));
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        NodeId family = backend.getId("<http://xmlns.com/foaf/familyName>");
        NodeId given = backend.getId("<http://xmlns.com/foaf/givenName>");

        PreemptJenaIterator.NB_WALKS = 200000;

        var it = backend.search(backend.any(), given, backend.any());
        ProgressJenaIterator casted = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>)it).iterator;
        log.info("Expected 69970, got {}.", casted.cardinality());
        assertTrue(66000 < casted.cardinality(Integer.MAX_VALUE) && casted.cardinality(Integer.MAX_VALUE) < 74000);

        it = backend.search(backend.any(), family, backend.any());
        casted = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>)it).iterator;
        log.info("Expected 69970, got {}.", casted.cardinality());
        assertTrue(66000 < casted.cardinality(Integer.MAX_VALUE) && casted.cardinality(Integer.MAX_VALUE) < 74000);
    }


    @Disabled
    @Test
    public void cardinality_of_larger_triple_pattern_above_leaf_size_with_watdiv_with_query1000() {
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country21>))"); // expect 2613
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://purl.org/goodrelations/validThrough> ?v3))"); // expect 36346
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://purl.org/goodrelations/includes> ?v1))"); // expect 90000
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v1 <http://schema.org/text> ?v6))"); // expect 7476
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://schema.org/eligibleQuantity> ?v4))"); // expect 90000
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://purl.org/goodrelations/price> ?v2))"); // expect 240000

        NodeId price = backend.getId("<http://purl.org/goodrelations/price>");
        var it = backend.search(backend.any(), price, backend.any());
        ProgressJenaIterator casted = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>) it).iterator;
        assertEquals(240000, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 240000, got {}.", casted.cardinality(200000));

        NodeId eligible = backend.getId("<http://schema.org/eligibleQuantity>");
        it = backend.search(backend.any(), eligible, backend.any());
        casted = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>) it).iterator;
        assertEquals(90000, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 90000, got {}.", casted.cardinality(200000));

        NodeId text = backend.getId("<http://schema.org/text>");
        it = backend.search(backend.any(), text, backend.any());
        casted = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>) it).iterator;
        assertEquals(7476, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 7476, got {}.", casted.cardinality(200000));

        NodeId include = backend.getId("<http://purl.org/goodrelations/includes>");
        it = backend.search(backend.any(), include, backend.any());
        casted = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>) it).iterator;
        assertEquals(90000, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 90000, got {}.", casted.cardinality(200000));

        NodeId valid = backend.getId("<http://purl.org/goodrelations/validThrough>");
        it = backend.search(backend.any(), valid, backend.any());
        casted = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>) it).iterator;
        assertEquals(36346, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 36346, got {}.", casted.cardinality(200000));

        NodeId region = backend.getId("<http://schema.org/eligibleRegion>");
        NodeId country21 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>");
        it = backend.search(backend.any(), region, country21);
        casted = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>) it).iterator;
        assertEquals(2613, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 2613, got {}.", casted.cardinality(200000));
    }

    @Disabled
    @EnabledIfEnvironmentVariable(named = "WATDIV", matches = "true")
    public void count_as_well_but_building_a_tree_in_a_bplustree_way() {
        JenaBackend backend = new JenaBackend("../target/watdiv10M");

        NodeId price = backend.getId("<http://purl.org/goodrelations/price>");
        var it = backend.search(backend.any(), price, backend.any());
        ProgressJenaIterator casted = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>) it).iterator;
        assertEquals(240000, casted.getTreeOfCardinality().sum);

        NodeId eligible = backend.getId("<http://schema.org/eligibleQuantity>");
        it = backend.search(backend.any(), eligible,backend.any());
        casted =(ProgressJenaIterator)((BackendLazyIterator<?,?,?>)it).iterator;
        assertEquals(90000, casted.getTreeOfCardinality().sum);

        NodeId text = backend.getId("<http://schema.org/text>");
        it = backend.search(backend.any(),text,backend.any());
        casted =(ProgressJenaIterator)((BackendLazyIterator<?,?,?>)it).iterator;
        assertEquals(7476, casted.getTreeOfCardinality().sum);

        NodeId include = backend.getId("<http://purl.org/goodrelations/includes>");
        it = backend.search(backend.any(),include,backend.any());
        casted =(ProgressJenaIterator)((BackendLazyIterator<?,?,?>)it).iterator;
        assertEquals(90000, casted.getTreeOfCardinality().sum);

        NodeId valid = backend.getId("<http://purl.org/goodrelations/validThrough>");
        it = backend.search(backend.any(),valid,backend.any());
        casted =(ProgressJenaIterator)((BackendLazyIterator<?,?,?>)it).iterator;
        assertEquals(36346, casted.getTreeOfCardinality().sum);

        NodeId region = backend.getId("<http://schema.org/eligibleRegion>");
        NodeId country21 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>");
        it = backend.search(backend.any(),region,country21);
        casted =(ProgressJenaIterator)((BackendLazyIterator<?,?,?>)it).iterator;
        assertEquals(2613, casted.getTreeOfCardinality().sum);
    }

    /* ********************************************************************** */

    @Disabled
    @Test
    public void getting_the_distribution_of_watdiv_spo_in_balanced_tree_index() {
        // Conclusion of this: the distribution is not uniform in the balanced tree.
        // Some elements have better chance to appear in the sample than others.
        // On the tested dataset it was up to 4x increased chances between the
        // min and the max.
        ProgressJenaIterator.NB_WALKS = 1000;
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        ProgressJenaIterator it = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>)backend.search(backend.any(), backend.any(), backend.any())).iterator;
        HashMap<Tuple<NodeId>, ImmutableTriple<Double, Double, Double>> recordToProba = new HashMap<>();

        log.debug("Start random sampling…");
        for (int i = 0; i < 100_000; ++i) {
            var rWp = it.getRandomSPOWithProbability();
            Tuple<NodeId> ids = rWp.getLeft();
            BackendLazyIterator<?,?,?> s = (BackendLazyIterator<?,?,?>)backend.search(ids.get(0), backend.any(), backend.any());
            ProgressJenaIterator sR = (ProgressJenaIterator) s.iterator;
            BackendLazyIterator<?,?,?> o = (BackendLazyIterator<?,?,?>)backend.search(backend.any(), backend.any(), ids.get(2));
            ProgressJenaIterator oR = (ProgressJenaIterator) o.iterator;
            recordToProba.put(ids, new ImmutableTriple<>(rWp.getRight(), oR.cardinality(), sR.cardinality()));
        }

        var sortedProbas = recordToProba.values().stream().sorted(Comparator.comparing(a -> a.left)).toList();

        log.debug("Start accept/reject to uniformize…");
        List<ImmutableTriple<Double, Double, Double>> accepted = new ArrayList<>();
        for (ImmutableTriple<Double, Double, Double> triple : sortedProbas) {
            double randomNumber= new Random().nextDouble();
            if (randomNumber <= (1./it.cardinality())/triple.getLeft()) { // p = getLeft * 1/|N|/getLeft = 1/|N|
                accepted.add(triple);
            }
        }

        log.info("Size of unique triples = {}", sortedProbas.size());
        log.info("Size of accepted = {}", accepted.size());
        // normalized.forEach(p -> System.out.println(String.format("%s %s %s", p.getLeft(), p.getMiddle(), p.getRight())));

        double sum = 0.;
        for (var triple : accepted) {
            sum += 1. / triple.getRight(); // middle : object ; right : subject
        }
        double estimate = it.cardinality() / accepted.size() * sum;
        log.info("Estimate = {}", estimate);
    }

    @Disabled
    @Test
    public void count_distinct_of_subject_on_watdiv_with_uniformity() {
        ProgressJenaIterator.NB_WALKS = 1000;
        final double DISTINCT_SUBJECT = 521_585.;
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        BackendLazyIterator<?,?,?> spo = (BackendLazyIterator<?,?,?>)backend.search(backend.any(), backend.any(), backend.any());
        ProgressJenaIterator spoR = (ProgressJenaIterator) spo.iterator;

        int sampleSize = 10_000;
        List<Double> results = new ArrayList<>();
        for (int j = 0; j < 10; ++j) {
            double sum = 0.;
            for (int i = 0; i < sampleSize; ++i) {
                var rWp = spoR.getUniformRandomSPO();
                Tuple<NodeId> ids = rWp;
                // LazyIterator o = (LazyIterator) backend.search(backend.any(), backend.any(), ids.get(2));
                BackendLazyIterator<?,?,?> o = (BackendLazyIterator<?,?,?>) backend.search(ids.get(0), backend.any(), backend.any());
                ProgressJenaIterator oR = (ProgressJenaIterator) o.iterator;
                sum += 1. / oR.cardinality(); // since uniform, the formula is simpler
            }
            double estimate = spoR.cardinality() / sampleSize * sum;
            log.info("{}th estimate = {}", j, estimate);
            results.add(estimate);
        }

        double average = results.stream().mapToDouble(v->v).average().orElse(0.);
        log.info("Average estimate = {}", average);
        log.info("Error = {}%", Math.abs(average-DISTINCT_SUBJECT)/DISTINCT_SUBJECT*100);
        //System.out.println("Error = " + Math.abs(average-1_005_832)/1_005_832);

    }

    @Disabled
    @Test
    public void count_distinct_subject_of_spo_on_watdiv_without_uniformity() {
        ProgressJenaIterator.NB_WALKS = 1000;
        final double DISTINCT_SUBJECT = 521585.;
        JenaBackend backend = new JenaBackend("../target/watdiv10M");

        ProgressJenaIterator it = (ProgressJenaIterator) ((BackendLazyIterator<?,?,?>) backend.search(backend.any(), backend.any(), backend.any())).iterator;

        List<ImmutablePair<Double, Double>> sampleWithProbaAndCard = new ArrayList<>();

        log.debug("Start sampling at random…");
        final double SAMPLE_SIZE = 100_000.;
        for (int i = 0; i < SAMPLE_SIZE; ++i) {
            var rWp = it.getRandomSPOWithProbability();
            Tuple<NodeId> ids = rWp.getLeft();
            BackendLazyIterator<?,?,?> s = (BackendLazyIterator<?,?,?>) backend.search(ids.get(0), backend.any(), backend.any());
            ProgressJenaIterator sR = (ProgressJenaIterator) s.iterator; // to get Fi
            sampleWithProbaAndCard.add(new ImmutablePair<>(rWp.getRight(), sR.count()));
        }

        log.debug("Summing all this…"); // (could be done in the sampling directly)
        double sumOfProbas = 0.;
        double sumOfRevisedSample = 0.;
        for (ImmutablePair<Double, Double> result : sampleWithProbaAndCard) {
            sumOfProbas += (1./result.getLeft())/result.getRight();
            sumOfRevisedSample += 1./result.getLeft();
        }

        double estimate = (it.count() / sumOfRevisedSample) * sumOfProbas;
        double relativeError = Math.abs(DISTINCT_SUBJECT - estimate)/ DISTINCT_SUBJECT;
        log.info("Count distinct = {}", estimate);
        log.info("Relative error = {}%", relativeError*100);
    }


    @Disabled
    @Test
    public void check_uniformity_of_triple_pattern() {
        ProgressJenaIterator.NB_WALKS = 1000;

        final int DISTINCT = 15_000;
        ArtificialSkewedGraph graph = new ArtificialSkewedGraph(DISTINCT, 50);
        JenaBackend backend = new JenaBackend(graph.getDataset());
        final int SAMPLE_SIZE = 5_000_000;
        NodeId is_a = backend.getId("<http://is_a>", SPOC.PREDICATE);
        NodeId prof = backend.getId("<http://Prof>", SPOC.OBJECT);

        ProgressJenaIterator pIsAProf = (ProgressJenaIterator)((BackendLazyIterator<?,?,?>) backend.search(backend.any(), is_a, prof)).iterator;
        Map<Record, Integer> record2Count = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; ++i) {
            Record record = pIsAProf.getUniformRandom();
            if (!record2Count.containsKey(record)){
                record2Count.put(record, 0);
            }
            record2Count.put(record, record2Count.get(record) + 1);
        }
        double avg = record2Count.values().stream().mapToDouble(v->v).average().orElse(0.);
        double med = record2Count.values().stream().sorted().toList().get(record2Count.size()/2);
        double max  =record2Count.values().stream().mapToDouble(v->v).max().getAsDouble();
        double min = record2Count.values().stream().mapToDouble(v->v).min().getAsDouble();
        // For uniform sample, (i) the average is close to the median; and (ii) they
        // must be in the middle of min and max. Plotted, this should look like a gaussian
        // centered on the average.
        log.info("Sample occurrences [min (avg, med) max] = [{}, ({}, {}) {}]", min, avg, med, max);
    }

    public static ImmutablePair<Record, Double> getRandomWithProbability(ProgressJenaIterator iterator, boolean uniform) {
        return uniform ?
                new ImmutablePair<>(iterator.getUniformRandomWithProbability().getLeft(), 1./iterator.count()) :
                (ImmutablePair<Record, Double>) iterator.getRandomWithProbability();
    }

    @Disabled
    @Test
    public void count_distinct_in_a_skewed_bgp_query() {
        // SELECT COUNT(DISTINCT ?group) WHERE {
        //     ?teacher <http://is_a> <http://Prof> .
        //     ?teacher <http://teaches> ?student .
        //     ?student <http://belongs_to> ?group .
        // }
        ProgressJenaIterator.NB_WALKS = 1_000;

        final boolean UNIFORM = true;

        int DISTINCT = 10_000;
        ArtificialSkewedGraph graph = new ArtificialSkewedGraph(DISTINCT, 50);
        JenaBackend backend = new JenaBackend(graph.getDataset());
        NodeId is_a = backend.getId("<http://is_a>", SPOC.PREDICATE);
        NodeId prof = backend.getId("<http://Prof>", SPOC.OBJECT);
        NodeId teaches = backend.getId("<http://teaches>", SPOC.PREDICATE);
        NodeId belongs_to = backend.getId("<http://belongs_to>", SPOC.PREDICATE);

        ProgressJenaIterator spo = (ProgressJenaIterator)((BackendLazyIterator<?,?,?>) backend.search(backend.any(), backend.any(), backend.any())).iterator;
        double cardinality = spo.count();
        double estimated = spo.cardinality();
        double relativeErr = Math.abs(cardinality- estimated)/cardinality;
        log.debug("|Ñ| = {}  (Relative error = {}%)", estimated, relativeErr*100);

        log.info("Running the full query to capture exact data…");
        double total = 0.;
        BackendIterator<NodeId, Node, ?> pIsAProfIt = backend.search(backend.any(), is_a, prof);
        Map<NodeId, Double> group2cardinality = new HashMap<>();
        while (pIsAProfIt.hasNext()) {
            pIsAProfIt.next();
            // ?p teaches ?s
            BackendIterator<NodeId, Node, ?> pTeachesSIt = backend.search(pIsAProfIt.getId(SPOC.SUBJECT), teaches, backend.any());
            while (pTeachesSIt.hasNext()) {
                pTeachesSIt.next();
                BackendIterator<NodeId, Node, ?> sBelongsToGIt = backend.search(pTeachesSIt.getId(SPOC.OBJECT), belongs_to, backend.any());
                while (sBelongsToGIt.hasNext()){
                    sBelongsToGIt.next();
                    total += 1; // Counting the total to get the exact |N|
                    // Counting the occurrences of group to get the exact F_i
                    if (!group2cardinality.containsKey(sBelongsToGIt.getId(SPOC.OBJECT))) {
                        group2cardinality.put(sBelongsToGIt.getId(SPOC.OBJECT), 0.);
                    }
                    group2cardinality.put(sBelongsToGIt.getId(SPOC.OBJECT), group2cardinality.get(sBelongsToGIt.getId(SPOC.OBJECT)) + 1);
                }
            }
        }

        /*log.info("Sampling the query…");
        int SAMPLE_SIZE = 1_000_000;
        List<ImmutablePair<Double, Double>> resultsProbaAndCard = new ArrayList<>();
        // ?teacher <http://is_a> <http://Prof>
        ProgressJenaIterator pIsAProf = (ProgressJenaIterator)((LazyIterator<?,?>) backend.search(backend.any(), is_a, prof)).iterator;
        for (int i = 0; i < SAMPLE_SIZE; ++i) {
            var pRP = getRandomWithProbability(pIsAProf, UNIFORM);
            Record pRecord = pRP.getLeft();
            Double firstTripleProba = pRP.getRight();
            NodeId pId = backend.getId(pRecord).get(SPOC.OBJECT); // TODO id of Record reordered depending on used index

            // ?teacher <http://teaches> ?student
            ProgressJenaIterator pTeachesS = (ProgressJenaIterator)((LazyIterator<?,?>) backend.search(pId, teaches, backend.any())).iterator;
            var sRP = getRandomWithProbability(pTeachesS, UNIFORM);
            Record sRecord = sRP.getLeft();
            Double secondTripleProba = sRP.getRight();

            NodeId sId = backend.getId(sRecord).get(SPOC.OBJECT);

            // ?student <http://belongs_to> ?group
            ProgressJenaIterator sBelongsToG = (ProgressJenaIterator)((LazyIterator<?,?>) backend.search(sId, belongs_to, backend.any())).iterator;

            if (sBelongsToG.count() >= 1) { // TODO ugly but needed for this
                var gRP = getRandomWithProbability(sBelongsToG, UNIFORM);
                Record gRecord = gRP.getLeft();
                NodeId gId = backend.getId(gRecord).get(SPOC.OBJECT);

                double thirdTripleProba = 1./1.; // always 1. since there is only one link possible if it exists

                resultsProbaAndCard.add(new ImmutablePair<>(
                        firstTripleProba * secondTripleProba * thirdTripleProba,
                        group2cardinality.get(gId)
                ));
            }
        }

        log.debug("Actual sample size = {}", resultsProbaAndCard.size()); // because some RWs fail to reach the end

        double sumOfProbas = 0.;
        double sumOfCards = 0.;
        for (ImmutablePair<Double, Double> sample: resultsProbaAndCard) {
                sumOfCards += 1./sample.getLeft()/ sample.getRight();
                sumOfProbas += 1./ sample.getLeft();
        }

        double estimate = total / sumOfProbas * sumOfCards;
        double relativeError = Math.abs(DISTINCT - estimate)/DISTINCT;
        log.info("Estimate = {}", estimate);
        log.info("Relative error = {}%", relativeError*100);*/
    }



}