package fr.gdd.passage.volcano.pull;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.benchmarks.WatDivTest;
import fr.gdd.passage.volcano.pull.iterators.PassageUnionParallel;
import fr.gdd.passage.volcano.transforms.BGP2Triples;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

@Disabled(value = "Only a performance quick test. Not a correctness check.")
class PassageUnionParallelTest {

    private final static Logger log = LoggerFactory.getLogger(PassageUnionParallelTest.class);

    @Test
    public void consuming_both_ends_should_be_faster_in_parallel () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(WatDivTest.PATH);
        String leftAsString = "SELECT * WHERE {?s ?p ?o}";
        String rightAsString = "SELECT * WHERE {?s ?p ?o}";

        Op leftAsOp = Algebra.compile(QueryFactory.create(leftAsString));
        Op rightAsOp = Algebra.compile(QueryFactory.create(rightAsString));

        leftAsOp = new BGP2Triples().visit(leftAsOp);
        rightAsOp = new BGP2Triples().visit(rightAsOp);

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
        PassageUnionParallel<?,?> unionPhysical = new PassageUnionParallel<>(
                new PassageExecutionContextBuilder<>().setBackend(backend).build(),
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
        PassagePullExecutor executor = new PassagePullExecutor(new PassageExecutionContextBuilder().setBackend(backend).build());

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