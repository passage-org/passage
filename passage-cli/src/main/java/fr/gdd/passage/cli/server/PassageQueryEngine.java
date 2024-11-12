package fr.gdd.passage.cli.server;

import fr.gdd.passage.volcano.PassageOpExecutor;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instead of relying on {@link QueryEngineTDB} to
 * call our {@link PassageOpExecutor}, we create and register our engine. We add a necessary
 * counter in top of the execution pipeline.
 */
public class PassageQueryEngine extends QueryEngineBase {

    private static Logger log = LoggerFactory.getLogger(PassageQueryEngine.class);

    protected PassageQueryEngine(Op op, DatasetGraph dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    protected PassageQueryEngine(Query query, DatasetGraph dataset, Binding input, Context cxt) {
        super(query, dataset, input, cxt);
    }

    @Override
    public Plan getPlan() {
        Op op = getOp();
        QueryIterator queryIterator = this.eval(op, dataset, BindingFactory.empty(), context);
        return new PlanOp(getOp(), this, queryIterator);
    }

    static public void register() {
        QueryEngineRegistry.addFactory(PassageQueryEngine.factory);
    }

    static public void unregister() {
        QueryEngineRegistry.removeFactory(PassageQueryEngine.factory);
    }

    @Override
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding input, Context context) {
        // #2 comes from {@link QueryEngineBase}
        ExecutionContext execCxt = new ExecutionContext(context, dsg.getDefaultGraph(), dsg, QC.getFactory(context));

        QueryIterator qIter1 = ( input.isEmpty() ) ?
                QueryIterRoot.create(execCxt) :
                QueryIterRoot.create(input, execCxt);

        return QC.execute(op, qIter1, execCxt);
    }

    // ---- Factory *************************************************************/
    public static QueryEngineFactory factory = new PassageQueryEngineFactory();

    public static class PassageQueryEngineFactory implements QueryEngineFactory {
        @Override
        public boolean accept(Query query, DatasetGraph dataset, Context context) {
            return true;
        }

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding inputBinding, Context context) {
            return new PassageQueryEngine(query, dataset, inputBinding, context).getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            return true;
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding inputBinding, Context context) {
            return new PassageQueryEngine(op, dataset, inputBinding, context).getPlan();
        }
    }
}
