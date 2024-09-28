package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.passage.commons.generics.LazyIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.databases.persistent.Watdiv10M;
import fr.gdd.raw.tdb2.JenaBackend;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.graph.Node;
import org.apache.jena.tdb2.store.NodeId;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

@Disabled
public class CountDistinctTest {

    @Test
    public void rejection_rate_on_sac_spo () {
        Watdiv10M watdiv10M = new Watdiv10M(Optional.empty());
        JenaBackend backend = new JenaBackend(watdiv10M.dbPath_asStr);

        NodeId is_a = backend.getId("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", SPOC.PREDICATE);
        LazyIterator<NodeId, Node, ?> sac = (LazyIterator) backend.search(backend.any(), is_a, backend.any());
        ProgressJenaIterator progress_sac = (ProgressJenaIterator) sac.iterator;
        double t1 = progress_sac.count();

        List<Double> cards_t2 = new ArrayList<>();
        double sumResults = 0;
        while(sac.hasNext()) {
            sac.next();
            NodeId s = sac.getId(SPOC.SUBJECT);
            ProgressJenaIterator progress_spo = (ProgressJenaIterator) ((LazyIterator) backend.search(s, backend.any(), backend.any())).iterator;
            double results = progress_spo.count();
            cards_t2.add(progress_spo.count());
            sumResults += results;
        }
        cards_t2 = cards_t2.stream().sorted().collect(Collectors.toList()).reversed();
        double t2 = cards_t2.stream().mapToDouble(e->e).max().orElse(-1);
        Double rr = 1-sumResults/(t1*t2);
        System.out.println(rr);
    }


    @Test
    public void accept_reject () {
        Integer SAMPLE_SIZE = 100_000;
        Watdiv10M watdiv10M = new Watdiv10M(Optional.empty());
        JenaBackend backend = new JenaBackend(watdiv10M.dbPath_asStr);

        NodeId is_a = backend.getId("<http://schema.org/printPage>", SPOC.PREDICATE);
        LazyIterator<NodeId, Node, ?> sac = (LazyIterator) backend.search(backend.any(), is_a, backend.any());
        ProgressJenaIterator progress_sac = (ProgressJenaIterator) sac.iterator;
        double N = progress_sac.count();

        Map<Tuple<NodeId>, Integer> spo2freq = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; ++i) {
            Tuple<NodeId> tuple = progress_sac.getRandomSPO();
            if (!spo2freq.containsKey(tuple)) {
                spo2freq.put(tuple, 0);
            }
            spo2freq.put(tuple, spo2freq.get(tuple) + 1);
        }
        System.out.println(spo2freq.values().stream().sorted().collect(Collectors.toList()));

        Map<Tuple<NodeId>, Integer> spo2freqUniform = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; ++i) {
            Tuple<NodeId> tuple = progress_sac.getUniformRandomSPO();
            if (!spo2freqUniform.containsKey(tuple)) {
                spo2freqUniform.put(tuple, 0);
            }
            spo2freqUniform.put(tuple, spo2freqUniform.get(tuple) + 1);
        }
        System.out.println("N= " + N + " ; vs " + spo2freqUniform.size());
        System.out.println(spo2freqUniform.values().stream().sorted().collect(Collectors.toList()));

        Map<Tuple<NodeId>, Integer> spo2freqAcceptReject = new HashMap<>();
        Random rn = new Random(12);
        int rejected = 0;
        int spo2freqAcceptRejectSize = 0;
        while (spo2freqAcceptRejectSize < SAMPLE_SIZE) {
            Pair<Tuple<NodeId>, Double> tupleAndProba = progress_sac.getRandomSPOWithProbability();

            double between0and1 = rn.nextDouble();

            // System.out.println(tupleAndProba.getRight()/ (0.5*(1./N)));
            // System.out.println(1./N + " >= " + (tupleAndProba.getRight() * 0.5));
            if (between0and1 <= 1/(tupleAndProba.getRight()/ (0.1* (1./N)))) {

                if (!spo2freqAcceptReject.containsKey(tupleAndProba.getLeft())) {
                    spo2freqAcceptReject.put(tupleAndProba.getLeft(), 0);
                }
                spo2freqAcceptRejectSize += 1;
                spo2freqAcceptReject.put(tupleAndProba.getLeft(), spo2freqAcceptReject.get(tupleAndProba.getLeft()) + 1);
            } else {
                rejected+=1;
            }
        }
        System.out.println("Rejected = " + rejected);
        System.out.println(spo2freqAcceptReject.values().stream().sorted().collect(Collectors.toList()));

    }

    @Test
    public void spo_with_chao () {
        double DISTINCT_OBJECT = 1_005_832.;
        double DISTINCT_SUBJECT = 521_585.;
        Watdiv10M watdiv10M = new Watdiv10M(Optional.empty());
        JenaBackend backend = new JenaBackend(watdiv10M.dbPath_asStr);
        Integer SAMPLE_SIZE = 100_000;


        Set<NodeId> d = new HashSet<>();
        double sumOfNj = 0.;
        double crwdProbaSum = 0.;
        double crwdRightSum = 0.;
        LazyIterator<NodeId, Node, ?> spo = (LazyIterator) backend.search(backend.any(), backend.any(), backend.any());
        ProgressJenaIterator progress_spo = (ProgressJenaIterator) spo.iterator;
        double N = progress_spo.count();
        Integer nbDuplicate = 0;
        for (int i = 0; i < SAMPLE_SIZE; ++i) {
            Pair<Tuple<NodeId>, Double> tupleAndProba = progress_spo.getUniformRandomSPOWithProbability();
            NodeId elementId = tupleAndProba.getLeft().get(SPOC.OBJECT);

            String value = backend.getString(elementId);
            LazyIterator<NodeId, Node, ?> e = (LazyIterator) backend.search(backend.any(), backend.any(), elementId);
            ProgressJenaIterator progress_e = (ProgressJenaIterator) e.iterator;
            double Fi = progress_e.count();

            if (!d.contains(elementId)) {
                sumOfNj += Fi / N;
            } else {
                nbDuplicate += 1;
            }
            d.add(elementId);

            crwdProbaSum += (1./tupleAndProba.getRight());
            crwdRightSum += ((1./tupleAndProba.getRight())/Fi);

            if (i % 1_000 == 0) {
                double chao = d.size() / sumOfNj;
                double eChao = Math.abs(DISTINCT_OBJECT - chao) / DISTINCT_OBJECT;
                double crwd = (N/crwdProbaSum) * crwdRightSum;
                double eCRWD = Math.abs(DISTINCT_OBJECT - crwd) / DISTINCT_OBJECT;
                System.out.println("CHAO " + i + " = " + chao + "     error = " + eChao + "      duplicates = " + nbDuplicate);
                System.out.println("CRWD " + i + " = " + crwd + "     error = " + eCRWD);
            }
        }


    }
}
