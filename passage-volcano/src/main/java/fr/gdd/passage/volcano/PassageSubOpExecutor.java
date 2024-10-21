package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.factories.BackendJoinFactory;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.iterators.BackendBind;
import fr.gdd.passage.commons.iterators.BackendProject;
import fr.gdd.passage.volcano.iterators.PassageScanFactory;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.io.Serializable;

/**
 * An executor of sub-queries that only knows a very limited number of operators.
 * This limited sub-set of operators allows us to pause/resume the query execution
 * when needed.
 */
public class PassageSubOpExecutor <ID,VALUE> extends BackendOpExecutor<ID,VALUE> {

    public PassageSubOpExecutor(ExecutionContext context) {
        super(context, BackendProject.factory(), PassageScanFactory.factoryLimitOffset(),
                new BackendJoinFactory<>(), null, null, BackendBind.factory(),
                null, null, null, null);
    }
}
