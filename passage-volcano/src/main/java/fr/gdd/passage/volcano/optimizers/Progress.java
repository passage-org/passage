package fr.gdd.passage.volcano.optimizers;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.volcano.iterators.PassageScanFactory;
import fr.gdd.passage.volcano.pause.Pause2SPARQL;
import fr.gdd.passage.volcano.resume.IsSkippable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.Objects;

/**
 * Depending on the physical execution plan, and limited knowledge of
 * iterators' cardinality and current offset, how far am I from finishing
 * the job, approximately? TODO TODO
 */
public class Progress extends ReturningOpVisitor<
        Pair<Double, // progress
                Double>> { // cardinality

    final Pause2SPARQL<?,?> saver;
    private final static double DONE = 1.;

    public Progress(Pause2SPARQL<?,?> saver) {
        this.saver = saver;
    }

    public double get() {
        return ReturningOpVisitorRouter.visit(this, saver.getRoot()).getLeft();
    }

    @Override
    public Pair<Double,Double> visit(OpTriple triple) {
        PassageScanFactory<?,?> scan = (PassageScanFactory) saver.getIterator(triple);
        if (Objects.isNull(scan)) {
            return new ImmutablePair<>(1., DONE); // TODO maybe set as null……………
        }
        if (scan.cardinality() == 0.) {
            return new ImmutablePair<>(1., DONE);
        }

        return new ImmutablePair<>(scan.offset()/scan.cardinality(), scan.cardinality());
    }

    @Override
    public Pair<Double, Double> visit(OpJoin join) {
        var progressAndCardLeft = ReturningOpVisitorRouter.visit(this, join.getLeft());
        var progressAndCardRight = ReturningOpVisitorRouter.visit(this, join.getRight());
        var progress = progressAndCardLeft.getLeft() -
                1./progressAndCardLeft.getRight() + // remove the current step being processed
                progressAndCardRight.getRight()/progressAndCardLeft.getRight(); // to add the actual progress of this step

        return new ImmutablePair<>(progress, progressAndCardRight.getRight());
    }

    @Override
    public Pair<Double, Double> visit(OpSlice slice) {
        IsSkippable isSkippableVisitor = new IsSkippable();
        Boolean isSkippable = ReturningOpVisitorRouter.visit(isSkippableVisitor, slice);
        if (isSkippable) {
            // behaves as if it does not exist since the tp is interpreted as tp with skip.
            // If need be, the tp will add the slice OFFSET itself.
            return ReturningOpVisitorRouter.visit(this, isSkippableVisitor.getOpTriple());
        }
        throw new UnsupportedOperationException("TODO OpSlice cannot be saved right now."); // TODO
    }

    @Override
    public Pair<Double, Double> visit(OpUnion union) {
        var progressAndCardLeft = ReturningOpVisitorRouter.visit(this, union.getLeft());
        var progressAndCardRight = ReturningOpVisitorRouter.visit(this, union.getRight());

        // TODO TODO
        return super.visit(union);
    }
}
