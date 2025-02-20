package fr.gdd.raw.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.raw.iterators.RandomScan;
import org.apache.jena.sparql.algebra.op.*;

import java.util.Objects;

/**
 * Process the probability of having retrieved the last random walk, i.e.,
 * series of successful scans.
 * This suppose that scan iterator are initialized, and the operators
 * are the same than at execution time.
 * TODO add the possibility to bind some variables to change the probabilities
 */
public class WanderJoin<ID, VALUE> extends ReturningOpVisitor<Double> {

    public final BackendSaver<ID,VALUE,?> saver;

    public WanderJoin(BackendSaver<ID,VALUE,?> saver) {
        this.saver = saver;
    }

    @Override
    public Double visit(OpTriple triple) {
        RandomScan<ID,VALUE> scan = (RandomScan<ID,VALUE>) saver.getIterator(triple);
        return Objects.isNull(scan) ? 0 : scan.getProbability();
    }

    @Override
    public Double visit(OpQuad quad) {
        RandomScan<ID, VALUE> scan = (RandomScan<ID, VALUE>) saver.getIterator(quad);
        return Objects.isNull(scan) ? 0 : scan.getProbability();
    }

    @Override
    public Double visit(OpJoin join) {
        Double leftProba = ReturningOpVisitorRouter.visit(this, join.getLeft());
        Double rightProba = ReturningOpVisitorRouter.visit(this, join.getRight());
        return leftProba * rightProba;
    }

    @Override
    public Double visit(OpProject project) { return ReturningOpVisitorRouter.visit(this, project.getSubOp()); }

    @Override
    public Double visit(OpExtend extend) { return ReturningOpVisitorRouter.visit(this, extend.getSubOp()); }

    @Override
    public Double visit(OpTable table) {
        if (table.isJoinIdentity()) {
            return 1.;
        }
        return table.getTable().size() == 0 ? 0.0 : 1.0 / table.getTable().size();
    }

    @Override
    public Double visit(OpFilter filter) {
        // TODO : make sure this is correct?
        // answer : yes it is. For one random walk, it is true that this way look weird, but at the scale of an entire
        // wander join, the increasing number of fail because of the filter will balance the overestimated probability
        return 1. * ReturningOpVisitorRouter.visit(this, filter.getSubOp());
    }

    @Override
    public Double visit(OpLeftJoin leftJoin) {
        Double leftProba = ReturningOpVisitorRouter.visit(this, leftJoin.getLeft());
        try {
            Double rightProba = ReturningOpVisitorRouter.visit(this, leftJoin.getRight());
            return leftProba * rightProba;
        } catch (Exception e) {
            return leftProba;
        }
    }
}
