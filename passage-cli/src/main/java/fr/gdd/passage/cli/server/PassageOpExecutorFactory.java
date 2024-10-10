package fr.gdd.passage.cli.server;

import fr.gdd.passage.volcano.PassageOpExecutor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;

/**
 * Apache Jena likes OpExecutor which is not an interface but a concrete
 * implementation. So we wrap this to keep ours clean.
 */
public class PassageOpExecutorFactory implements OpExecutorFactory {

    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new OpExecutorWrapper(execCxt);
    }

    public static class OpExecutorWrapper extends OpExecutor {

        final PassageOpExecutor sager;

        public OpExecutorWrapper(ExecutionContext ec) {
            super(ec);
            sager = new PassageOpExecutor<>(ec);
        }

        @Override
        public QueryIterator executeOp(Op op, QueryIterator input) {
            return new BindingWrapper(sager.execute(op), sager);
        }

        @Override
        protected QueryIterator exec(Op op, QueryIterator input) {
            // for whatever reason, two things with same signature.
            // This one is actually called… not the public one.
            return this.executeOp(op, input);
        }

    }

}
