package fr.gdd.passage.volcano.pause;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.resume.Subqueries2LeftOfJoins;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;

/**
 * Generate the SPARQL query of what has been executed.
 */
public class Pause2Previous<ID,VALUE> extends BackendSaver<ID,VALUE,Long> {

    public Pause2Previous(Op root, ExecutionContext context) {
        super(context.getContext().get(PassageConstants.SAVER), root);
    }

    @Override
    public Op save() {
        Op saved = ReturningOpVisitorRouter.visit(this, getRoot());
        saved = Objects.isNull(saved) ? saved : ReturningOpVisitorRouter.visit(new Triples2BGP(), saved);
        saved = Objects.isNull(saved) ? saved : ReturningOpVisitorRouter.visit(new Subqueries2LeftOfJoins(), saved);
        return saved;
    }

    @Override
    public Op visit(OpProject project) {
        return OpCloningUtil.clone(project, ReturningOpVisitorRouter.visit(this, project.getSubOp()));
    }

    @Override
    public Op visit(OpTriple triple) {
        // TODO TODO TODO 
        return super.visit(triple);
    }
}
