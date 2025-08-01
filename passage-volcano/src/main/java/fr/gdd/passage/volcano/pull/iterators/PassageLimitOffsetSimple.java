package fr.gdd.passage.volcano.pull.iterators;

import fr.gdd.passage.commons.engines.BackendPullExecutor;
import fr.gdd.passage.commons.factories.BackendNestedLoopJoinFactory;
import fr.gdd.passage.commons.iterators.BackendBind;
import fr.gdd.passage.commons.iterators.BackendProject;
import org.apache.jena.sparql.engine.ExecutionContext;

/**
 * An executor of sub-queries that only knows a very limited number of operators.
 * This limited sub-set of operators allows us to pause/resume the query execution
 * when needed.
 */
public class PassageLimitOffsetSimple<ID,VALUE> extends BackendPullExecutor<ID,VALUE> {

    public PassageLimitOffsetSimple(ExecutionContext context) {
        super(context, BackendProject.factory(),
                PassageScan.triplesFactory(), PassageScan.quadsFactory(),
                new BackendNestedLoopJoinFactory<>(), null, null, BackendBind.factory(),
                null, null, null, null, null, null);
    }
}
