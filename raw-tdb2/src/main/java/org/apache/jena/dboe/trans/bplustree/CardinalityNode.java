package org.apache.jena.dboe.trans.bplustree;

import java.util.ArrayList;
import java.util.List;

/**
 * Cardinality nodes build a tree of the sum of the number of children
 * to perform uniform sampling, using weighted random depending on the
 * number of children in each branch.
 */
public class CardinalityNode {

    Integer sum = 0;

    List<CardinalityNode> children = new ArrayList<>();

    public CardinalityNode() {
    }

    public CardinalityNode(Integer sum) {
        this.sum = sum;
    }

    public CardinalityNode addChild(CardinalityNode child) {
        this.sum += child.sum;
        this.children.add(child);
        return this;
    }

    /**
     * @return A number between 0 and sum.
     */
    public int getRandomWeightedIndex() {
        int random = ProgressJenaIterator.rng.nextInt(this.sum);
        if (children.isEmpty()) { // leaf
            return random;
        }

        int currentSum = 0;
        int i = 0;
        while (currentSum <= random && i < this.children.size()) {
            currentSum += this.children.get(i).sum;
            ++i;
        }
        return i - 1;
    }

}
