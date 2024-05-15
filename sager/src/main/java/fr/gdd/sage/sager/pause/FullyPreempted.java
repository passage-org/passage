package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.sage.sager.iterators.SagerScan;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;

import java.util.Objects;

/**
 * Fully preempted operators. If it cannot be preempted, then it returns null.
 */
public class FullyPreempted<ID, VALUE> extends ReturningOpBaseVisitor {

    final Save2SPARQL<ID,VALUE> saver;

    public FullyPreempted(Save2SPARQL<ID,VALUE> saver) {
        this.saver = saver;
    }

    @Override
    public Op visit(OpTriple triple) {
        SagerScan<ID, VALUE> it = (SagerScan<ID, VALUE>) saver.op2it.get(triple);

        if (Objects.isNull(it)) return null;

        return it.asBindAs();
    }

    @Override
    public Op visit(OpExtend extend) {
        return extend;
    }

    @Override
    public Op visit(OpSlice slice) {
        if (slice.getSubOp() instanceof OpTriple triple) {
            return this.visit(triple);
        }
        throw new UnsupportedOperationException("TODO normal slice fully preempted."); // TODO
    }

}
