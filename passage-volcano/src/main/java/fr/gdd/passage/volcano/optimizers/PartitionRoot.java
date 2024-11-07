package fr.gdd.passage.volcano.optimizers;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.iterators.PassageScanFactory;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.List;

/**
 * Find out the root scan and partition it with unions,
 * limit, and offsets. For it to work, the backend must have
 * iterators with `skip` enabled.
 */
public class PartitionRoot<ID,VALUE> extends ReturningOpVisitor<Op> {

    final Backend<ID,VALUE,Long> backend;
    final Integer nbThreads;

    public PartitionRoot(Backend<ID,VALUE,Long> backend, Integer nbThreads) {
        this.backend = backend;
        this.nbThreads = nbThreads;
    }

    /* ************************************************************************* */

    @Override
    public Op visit(OpTriple triple) {
        if (nbThreads == 1) { return triple; } // no changes

        // TODO take into account the original offset of the input opTriple
        PassageExecutionContext<ID,VALUE> context = new PassageExecutionContextBuilder<ID,VALUE>()
                .setBackend(backend)
                .build()
                .setQuery(null);

        PassageScanFactory<ID,VALUE> scan = new PassageScanFactory<>(context, Iter.of(new BackendBindings<>()), triple);
        scan.hasNext(); // TODO remove this `hasNext`
        double cardinality = scan.cardinality();

        if (cardinality <= 1.) { return triple; } // no changes

        int nbPartitions = cardinality/nbThreads > 1 ? nbThreads : (int) Math.floor(cardinality);
        int partitionSize = (int) Math.floor(cardinality/nbPartitions);


        int offset = 0;
        Op left = new OpSlice(new OpTriple(triple.getTriple()), offset, partitionSize);
        for (int i = 0; i < nbPartitions-2; ++i) { // -1 is the first before `for`, -1 is the last that computes the rest
            offset += partitionSize;
            Op right = new OpSlice(new OpTriple(triple.getTriple()), offset, partitionSize);
            left = new OpUnion(left, right);
        }
        offset += partitionSize;
        Op right = new OpSlice(new OpTriple(triple.getTriple()), offset, Long.MIN_VALUE); // the rest
        left = new OpUnion(left, right);

        return left;
    }


    @Override
    public Op visit(OpJoin join) {
        Op leftExplored = ReturningOpVisitorRouter.visit(this, join.getLeft());
        if (!(leftExplored instanceof OpUnion)) {
            return join;
        }
        // otherwise distribute the union
        List<Op> unionElements = FlattenUnflatten.flattenUnion(leftExplored);

        Op left = OpJoin.create(unionElements.get(0), join.getRight());
        for (int i = 1; i < unionElements.size(); ++i) {
            left = new OpUnion(left, OpJoin.create(unionElements.get(i), join.getRight()));
        }

        return left;
    }
}
