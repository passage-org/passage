package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.base.buffer.RecordBuffer;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.trans.bplustree.AccessPath.AccessStep;
import org.apache.jena.graph.Node;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.NodeId;

import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * An iterator that allows measuring the estimated progress of execution, i.e.,
 * the number of explored elements over the estimated number to explore.
 */
public abstract class ProgressJenaIterator extends BackendIterator<NodeId, Node, SerializableRecord> {

    public static Random rng = new Random(12); // random seed is accessible
    private final static Pair<Record, Double> NOTFOUND = new ImmutablePair<>(null, 0.);

    /**
     * Number of walks to approximate how filled bptree's records are.
     */
    public static int NB_WALKS = 2; // (TODO) could be self-adaptive, and depend on the number of possible nodes between bounds

    long offset = 0; // the number of elements explored
    Double cardinality = null; // lazy

    private Record minRecord = null;
    private Record maxRecord = null;

    private BPTreeNode root = null;
    private PreemptTupleIndexRecord ptir = null;
    private CardinalityNode cardinalityNode = null; // lazy

    public ProgressJenaIterator(PreemptTupleIndexRecord ptir, Record minRec, Record maxRec) {
        this.root = ptir.bpt.getNodeManager().getRead(ptir.bpt.getRootId());
        this.minRecord = Objects.isNull(minRec) ? root.minRecord() : minRec;
        this.maxRecord = Objects.isNull(maxRec) ? root.maxRecord() : maxRec;
        this.ptir = ptir;
    }

    /**
     * Empty iterator. Cardinality is zero. More efficient to handle such a specific case.
     */
    public ProgressJenaIterator() {
        this.cardinality = 0.;
    }

    public boolean isNullIterator() {return !Objects.isNull(this.cardinality) && this.cardinality == 0.;}

    /**
     * Singleton iterator. Cardinality is one. More efficient to handle such a specific case.
     */
    public ProgressJenaIterator(PreemptTupleIndexRecord ptir, Record record) {
        this.cardinality = 1.;
        this.minRecord = record;
        this.maxRecord = record;
        this.ptir = ptir;
    }

    public boolean isSingletonIterator() {return !Objects.isNull(this.cardinality) && this.cardinality == 1.;}

    @Override
    public boolean hasNext() {
        return false;
    }

    public void next() {
        this.offset += 1;
    }

    @Override
    public void reset() {
        this.offset = 0;
    }

    public long getOffset() {
        return offset;
    }

    public long currentOffset() {
        return offset;
    }

    public long previousOffset() {
        return offset - 1;
    }

    public void skip(long to) {
        this.offset = to;
    }

    public double getProgress() {
        if (this.cardinality() == 0.) return 1.0; // already finished
        return ((double) this.offset) / cardinality();
    }

    /**
     * Counts the number of elements by iterating over them. Warning: this is costly, but it provides exact
     * cardinality. Mostly useful for debugging purposes.
     * @return The exact number of elements in the iterator.
     */
    public double count() {
        if (isNullIterator() || isSingletonIterator()) return this.cardinality;
        return getTreeOfCardinality().sum; // slightly more efficient + cached
    }

    /**
     * Convenience function that checks the equality of two access paths.
     **/
    private static boolean equalsStep(AccessStep o1, AccessStep o2) {
        return o1.node.getId() == o2.node.getId() &&
                o1.idx == o2.idx &&
                o1.page.getId() == o2.page.getId();
    }

    private static boolean equalsNode(AccessStep o1, AccessStep o2) {
        return o1.node.getId() == o2.node.getId() &&
                o1.idx == o2.idx;
    }

    private static boolean equalsPath(List<AccessStep> p1, List<AccessStep> p2) {
        if (p1.size() != p2.size()) return false;
        for (int i = 0; i < p1.size(); ++i) {
            if (!equalsNode(p1.get(i), p2.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean isParent(AccessPath randomWalk, AccessPath base, AccessPath other) {
        for (int i = 0; i < base.getPath().size(); ++i) {
            if (base.getPath().get(i).node.id == other.getPath().get(i).node.id) {
                continue;
            }
            if (randomWalk.getPath().get(i).node.id == base.getPath().get(i).node.id) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieves a random record in between boundaries along with the probability of getting
     * retrieved, following the wander join formula.
     * @param minPath The smaller boundary.
     * @param maxPath The higher boundary.
     * @return A pair comprising the random record and its probability of getting drawn.
     */
    private Pair<Record, Double> randomWalkWJ(AccessPath minPath, AccessPath maxPath) {
        int idxMin = minPath.getPath().get(0).idx;
        int idxMax = maxPath.getPath().get(0).idx;

        int idxRnd = idxMin + rng.nextInt(idxMax - idxMin + 1);

        BPTreeNode node = minPath.getPath().get(0).node;
        AccessPath.AccessStep lastStep = new AccessStep(node, idxRnd, node.get(idxRnd));
        double proba = 1.0 / (idxMax - idxMin + 1.0);

        while (!lastStep.node.isLeaf()) {
            node = (BPTreeNode) lastStep.page;

            idxMin = node.findSlot(minRecord);
            idxMax = node.findSlot(maxRecord);

            idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;
            idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;

            idxRnd = idxMin + rng.nextInt(idxMax - idxMin + 1);
            
            lastStep = new AccessStep(node, idxRnd, node.get(idxRnd));

            proba *= 1.0 / (idxMax - idxMin + 1.0);
        }

        RecordBuffer recordBuffer = ((BPTreeRecords) lastStep.page).getRecordBuffer();

        idxMin = recordBuffer.find(minRecord);
        idxMax = recordBuffer.find(maxRecord);
        
        idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;
        idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;

        // create an iterator to be sure that there is at least one element
        // this might be overkill but at least it's consistent
        if ((idxMin == idxMax && !this.ptir.bpt.iterator(minRecord,maxRecord,ptir.getRecordMapper()).hasNext()) || isNullIterator()) {
            // Nothing found, becomes a null iterator :)
            this.ptir = null;
            this.cardinality = 0.;
            return NOTFOUND;
        } else if (idxMin == idxMax) {
            // ends up in a boundary leaf (either min or max) that do not have any element
            return this.randomWalkWJ(minPath,maxPath); // we try again.
            // retry.setValue(retry.getRight() * proba); // proba is updated (BUT since it's immutable, we create new one)
            // return new ImmutablePair<>(retry.getLeft(), retry.getRight()*proba);
        }
        // otherwise, the page has element(s), randomize in it.

        idxRnd = idxMin + rng.nextInt(idxMax - idxMin); // no need for -1 in a `RecordBuffer`
        proba *= 1. / (idxMax - idxMin);

        return new ImmutablePair<>(recordBuffer.get(idxRnd), proba);
    }

    /**
     * @return A random record between the set boundaries of the object.
     */
    public Record getRandom() {
        return this.getRandomWithProbability().getLeft();
    }

    /**
     * @return The node identifiers order naturally.
     */
    public Tuple<NodeId> getRandomSPO() {
        Record randomRecord = this.getRandom();
        return Objects.isNull(randomRecord) ? null : getSPO(randomRecord);
    }

    /**
     * @return The node identifiers order naturally along with the probability
     * of getting it.
     */
    public Pair<Tuple<NodeId>, Double> getRandomSPOWithProbability() {
        Pair<Record, Double> rWp = getRandomWithProbability();
        if (rWp.equals(NOTFOUND)) {
            return new ImmutablePair<>(null, 0.);
        }
        return new ImmutablePair<>(getSPO(rWp.getLeft()), rWp.getRight());
    }

    /**
     * @return A random record between the set boundaries of the object along
     * with the probability of being chosen in the balanced tree index.
     */
    public Pair<Record, Double> getRandomWithProbability() {
        if (isNullIterator()) return NOTFOUND;
        if (isSingletonIterator()) return new ImmutablePair<>(minRecord, 1.);

        AccessPath minPath = new AccessPath(null);
        AccessPath maxPath = new AccessPath(null);
        root.internalSearch(minPath, minRecord);
        root.internalSearch(maxPath, maxRecord);
        return randomWalkWJ(minPath, maxPath);
    }


    @Override
    public double cardinality() throws UnsupportedOperationException {
        return cardinality(NB_WALKS);
    }

    /**
     * Estimates the cardinality of a triple/quad pattern knowing that
     * the underlying data structure is a balanced tree.
     * When the number of results is small, more precision is needed.
     * Fortunately, this often means that results are spread among one
     * or two pages, which allows us to precisely count using binary search.
     *
     * @return An estimated cardinality.
     */
    @Override
    public double cardinality(long nbWalks) {
        if (Objects.nonNull(this.cardinality)) {
            return cardinality; // already processed, lazy return. Or singleton or null.
        }

        if (nbWalks == Long.MAX_VALUE) {
            // MAX_VALUE goes for counting since it's the most costly, at least we want exact cardinality
            // return count();
            return getTreeOfCardinality().sum; // slightly more efficient
        }

        AccessPath minPath = new AccessPath(null);
        AccessPath maxPath = new AccessPath(null);

        root.internalSearch(minPath, minRecord);
        root.internalSearch(maxPath, maxRecord);

        AccessPath.AccessStep minStep = minPath.getPath().get(minPath.getPath().size() - 1);
        AccessPath.AccessStep maxStep = maxPath.getPath().get(maxPath.getPath().size() - 1);

        double cardinality = 0.;

        // exact count for the leftmost and rightmost page
        RecordBuffer minRecordBuffer = ((BPTreeRecords) minStep.page).getRecordBuffer();
        RecordBuffer maxRecordBuffer = ((BPTreeRecords) maxStep.page).getRecordBuffer();

        int idxMin = minRecordBuffer.find(minRecord);
        int idxMax =  maxRecordBuffer.find(maxRecord);

        idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;
        idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;

        if (equalsStep(minStep, maxStep)) {
            cardinality += idxMax - idxMin;
        } else {
            cardinality += minRecordBuffer.size() - idxMin;
            cardinality += idxMax;
        }

        // random walks to estimate the number of records between the leftmost and rightmost page
        if (minStep.node.id != maxStep.node.id || maxStep.idx - minStep.idx >= 2) {
            double sum = 0.0;
            int count = 0;

            for (int i = 0; i < nbWalks; i++) {
                Pair<Record, Double> pair = this.randomWalkWJ(minPath, maxPath);
                // records in the leftmost and rightmost page are ignored
                if (pair.getLeft() != null && minRecordBuffer.find(pair.getLeft()) < 0 && maxRecordBuffer.find(pair.getLeft()) < 0) {
                    sum += 1 / pair.getRight();
                }
                count += 1;
            }
            cardinality += sum / (double) count;
        }

        this.cardinality = cardinality; // for laziness

        return this.cardinality;
    }

    /**
     * @return A tree comprising the sum of all downstream elements.
     */
    public CardinalityNode getTreeOfCardinality() {
        if (Objects.nonNull(this.cardinalityNode)) {
            return this.cardinalityNode;
        }

        AccessPath minPath = new AccessPath(null);
        AccessPath maxPath = new AccessPath(null);

        root.internalSearch(minPath, minRecord);
        root.internalSearch(maxPath, maxRecord);

        int idxMin = minPath.getPath().get(0).idx;
        int idxMax = maxPath.getPath().get(0).idx;

        CardinalityNode cardinalityNode = new CardinalityNode();
        for (int idx = idxMin; idx <= idxMax; ++idx) {
            BPTreeNode node = minPath.getPath().get(0).node;
            AccessPath.AccessStep lastStep = new AccessStep(node, idx, node.get(idx));

            cardinalityNode.addChild(_getTreeOfCardinality(lastStep));
        }

        this.cardinalityNode = cardinalityNode;

        return cardinalityNode;
    }

    /*
     * @param step The step in the tree where the count need to be made.
     * @return A cardinality node starting from the current step.
     */
    private CardinalityNode _getTreeOfCardinality(AccessStep step) {
        if (!step.node.isLeaf()) {
            BPTreeNode node = (BPTreeNode) step.page;

            int idxMin = node.findSlot(minRecord);
            int idxMax = node.findSlot(maxRecord);

            idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;
            idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;

            CardinalityNode cardinalityNode = new CardinalityNode();
            for (int idx = idxMin; idx <= idxMax; ++idx) {
                AccessStep lastStep = new AccessStep(node, idx, node.get(idx));
                cardinalityNode.addChild(_getTreeOfCardinality(lastStep));
            }
            return cardinalityNode;
        } else {
            RecordBuffer recordBuffer = ((BPTreeRecords) step.page).getRecordBuffer();
            int idxMin = recordBuffer.find(minRecord);
            int idxMax = recordBuffer.find(maxRecord);

            idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;
            idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;

            if (idxMin == idxMax) {
                return new CardinalityNode();
            } else {
                return new CardinalityNode(idxMax - idxMin);
            }
        }
    }

    /**
     * @return A random record chosen uniformly at random between the bounds.
     * Beware: it may be costly since it needs the cardinality of each node.
     * It performs a weighted random at every successive depth of the balanced tree.
     */
    public Record getUniformRandom() {
        if (isNullIterator()) return NOTFOUND.getKey();
        if (isSingletonIterator()) return minRecord;
        CardinalityNode cardinalityNode = getTreeOfCardinality();
        if (cardinalityNode.sum == 0) return NOTFOUND.getKey(); // Does not exist any element, hence no random element

        AccessPath minPath = new AccessPath(null);
        root.internalSearch(minPath, minRecord);

        AccessPath maxPath = new AccessPath(null);
        root.internalSearch(maxPath, maxRecord);

        int idxMin = minPath.getPath().get(0).idx;
        BPTreeNode node = minPath.getPath().get(0).node;

        int randomIndex = cardinalityNode.getRandomWeightedIndex();
        cardinalityNode = cardinalityNode.children.get(randomIndex);

        AccessStep lastStep = new AccessStep(node, idxMin + randomIndex, node.get(idxMin + randomIndex));

        while (!cardinalityNode.children.isEmpty()) {
            node = (BPTreeNode) lastStep.page;

            randomIndex = cardinalityNode.getRandomWeightedIndex();
            cardinalityNode = cardinalityNode.children.get(randomIndex);

            idxMin = node.findSlot(minRecord);
            idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;

            int idx = idxMin + randomIndex;

            lastStep = new AccessStep(node, idx, node.get(idx));
        }

        RecordBuffer recordBuffer = ((BPTreeRecords) lastStep.page).getRecordBuffer();
        idxMin = recordBuffer.find(minRecord);
        randomIndex = cardinalityNode.getRandomWeightedIndex();

        idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;

        return recordBuffer.get(idxMin+randomIndex);
    }

    /**
     * @return A random record chosen uniformly at random between the bounds.
     * Since it's uniform, its probability is 1 over the number of elements between the bounds.
     * Beware: it may be costly since it requires to compute the cardinality of each node.
     */
    public Pair<Record, Double> getUniformRandomWithProbability() {
        if (isNullIterator()) return NOTFOUND;
        if (isSingletonIterator()) return new ImmutablePair<>(minRecord, 1.);

        CardinalityNode cardinalityNode = getTreeOfCardinality();
        if (cardinalityNode.sum == 0) { // nothing exists
            return NOTFOUND;
        } else {
            return new ImmutablePair<>(getUniformRandom(), 1./cardinalityNode.sum);
        }
    }

    /**
     * @return The node identifiers order naturally.
     */
    public Tuple<NodeId> getUniformRandomSPO() {
        Record randomRecord = this.getUniformRandom();
        return Objects.isNull(randomRecord) ? null : getSPO(randomRecord);
    }

    /**
     * @return The node identifiers order naturally along with the probability
     * of getting it.
     */
    public Pair<Tuple<NodeId>, Double> getUniformRandomSPOWithProbability() {
        Pair<Record, Double> rWp = getUniformRandomWithProbability();
        if (rWp.equals(NOTFOUND)) {
            return new ImmutablePair<>(null, rWp.getRight()); // always proba = 0 ?
        }
        return new ImmutablePair<>(getSPO(rWp.getLeft()), rWp.getRight());
    }

    /**
     * @param record The record to transform and reorder into SPO.
     * @return A SPO pattern only, removing the graph part. Since the graph is put in first
     * position, it forces us to create a new one, skipping the graph slot.
     */
    private Tuple<NodeId> getSPO(Record record) {
        Tuple<NodeId> tuple = TupleLib.tuple(record, this.ptir.tupleMap);
        return tuple.len() > 3 ?
                TupleFactory.create3(tuple.get(SPOC.SUBJECT + 1), tuple.get(SPOC.PREDICATE + 1), tuple.get(SPOC.OBJECT + 1)) :
                tuple;
    }

    @Override
    public Double random() {
        throw new UnsupportedOperationException("TODO");
    }
}