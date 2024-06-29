package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.PtrMap;
import fr.gdd.sage.rawer.iterators.RandomScan;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpTriple;

import java.util.Iterator;

/**
 * Process the probability of having retrieved the last random walk.
 * TODO add the possibility to bind some variables to change the probabilities
 */
public class WanderJoinVisitor extends ReturningOpVisitor<Double> {

    public final PtrMap<Op, Iterator<BackendBindings<?,?>>> op2it;

    public WanderJoinVisitor (PtrMap<Op, Iterator<BackendBindings<?,?>>> op2it) {
        this.op2it = op2it;
    }

    @Override
    public Double visit(OpTriple triple) {
        RandomScan<?,?> scan = (RandomScan<?,?>) op2it.get(triple);
        return scan.getProbability();
    }
}
