package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.PtrMap;
import fr.gdd.sage.rawer.iterators.RandomScan;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTriple;

import java.util.Iterator;
import java.util.Objects;

/**
 * Process the probability of having retrieved the last random walk.
 * TODO add the possibility to bind some variables to change the probabilities
 */
public class WanderJoinVisitor<ID, VALUE> extends ReturningOpVisitor<Double> {

    public final PtrMap<Op, Iterator<BackendBindings<ID,VALUE>>> op2it;

    public WanderJoinVisitor (PtrMap<Op, Iterator<BackendBindings<ID,VALUE>>> op2it) {
        this.op2it = op2it;
    }

    @Override
    public Double visit(OpTriple triple) {
        RandomScan<ID,VALUE> scan = (RandomScan<ID,VALUE>) op2it.get(triple);
        return Objects.isNull(scan) ? 0 : scan.getProbability();
    }

    @Override
    public Double visit(OpJoin join) {
        Double leftProba = ReturningOpVisitorRouter.visit(this, join.getLeft());
        Double rightProba = ReturningOpVisitorRouter.visit(this, join.getRight());
        return leftProba * rightProba;
    }
}
