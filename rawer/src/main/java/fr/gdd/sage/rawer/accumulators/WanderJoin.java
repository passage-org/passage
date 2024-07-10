package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.BackendSaver;
import fr.gdd.sage.generics.PtrMap;
import fr.gdd.sage.rawer.iterators.RandomScan;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.Iterator;
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
        throw new UnsupportedOperationException("Tables are not handled properly yet…");
    }
}
