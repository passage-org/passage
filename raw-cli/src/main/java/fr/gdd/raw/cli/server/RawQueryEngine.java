package fr.gdd.raw.cli.server;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.util.Context;

import java.util.logging.Logger;

public class RawQueryEngine extends QueryEngineBase {

    private static Logger log = Logger.getLogger(RawQueryEngine.class.getName());

    protected RawQueryEngine(Query query, DatasetGraph dsg, Binding input, Context cxt) {
        super(query, dsg, input, cxt);
    }

    protected RawQueryEngine(Op op, DatasetGraph dataset, Binding input, Context cxt) {
        super(op, dataset, input, cxt);
    }

    @Override
    public Plan getPlan() {
        Op op = getOp();
        QueryIterator queryIterator = this.eval(op, dataset, BindingFactory.empty(), context);
        return new PlanOp(getOp(), this, queryIterator);
    }

    @Override
    protected QueryIterator eval(Op op, DatasetGraph dsg, Binding input, Context context) {
        // #2 comes from {@link QueryEngineBase}
        ExecutionContext execCxt = new ExecutionContext(context, dsg.getDefaultGraph(), dsg, QC.getFactory(context));

        QueryIterator qIter1 = ( input.isEmpty() ) ?
                QueryIterRoot.create(execCxt) :
                QueryIterRoot.create(input, execCxt);

        return QC.execute(op, qIter1, execCxt);
    }


    // ---- Factory *************************************************************/
    public static QueryEngineFactory factory = new RawQueryEngine.RawerQueryEngineFactory();

    public static class RawerQueryEngineFactory implements QueryEngineFactory {
        @Override
        public boolean accept(Query query, DatasetGraph dataset, Context context) {
            return true;
        }

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding inputBinding, Context context) {
            return new RawQueryEngine(query, dataset, inputBinding, context).getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            return true;
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding inputBinding, Context context) {
            return new RawQueryEngine(op, dataset, inputBinding, context).getPlan();
        }
    }
}
