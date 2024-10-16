package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageOpExecutor;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

class PassageUnionParallelTest {

    private final static Logger log = LoggerFactory.getLogger(PassageUnionParallel.class);

    @Test
    public void consuming_both_ends_should_be_faster_in_parallel () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv10m-blaze/watdiv10M.jnl");
        String leftAsString = "SELECT * WHERE {?s ?p ?o}";
        String rightAsString = "SELECT * WHERE {?s ?p ?o}";

        Op leftAsOp = Algebra.compile(QueryFactory.create(leftAsString));
        Op rightAsOp = Algebra.compile(QueryFactory.create(rightAsString));

        executeUnionParallel(bb, leftAsOp, rightAsOp);
        executeUnionSingle(bb, leftAsOp, rightAsOp);
        executeUnionParallel(bb, leftAsOp, rightAsOp);
        executeUnionSingle(bb, leftAsOp, rightAsOp);
        executeUnionParallel(bb, leftAsOp, rightAsOp);
        executeUnionSingle(bb, leftAsOp, rightAsOp);
        executeUnionParallel(bb, leftAsOp, rightAsOp);
        executeUnionSingle(bb, leftAsOp, rightAsOp);
    }

    private void executeUnionParallel(BlazegraphBackend backend, Op left, Op right) {
        OpUnion union = new OpUnion(left, right);
        PassageUnionParallel<?,?,?> unionPhysical = new PassageUnionParallel<>(
                new PassageExecutionContext<>(backend),
                new BackendBindings<>(),
                union);

        long count = 0L;
        long start = System.currentTimeMillis();
        while (unionPhysical.hasNext()) {
            unionPhysical.next();
            count += 1; // only count
        }
        long elapsed = System.currentTimeMillis() - start;

        log.debug("Parallel: Counted {} elements in {}ms.", count, elapsed);
    }

    private void executeUnionSingle(BlazegraphBackend backend, Op left, Op right) {
        OpUnion union = new OpUnion(left, right);
        PassageOpExecutor executor = new PassageOpExecutor().setBackend(backend);

        long count = 0L;
        Iterator<BackendBindings> it = executor.execute(union);
        long start = System.currentTimeMillis();
        while (it.hasNext()) {
            it.next();
            count += 1; // only count
        }
        long elapsed = System.currentTimeMillis() - start;

        log.debug("Single: Counted {} elements in {}ms.", count, elapsed);
    }

}