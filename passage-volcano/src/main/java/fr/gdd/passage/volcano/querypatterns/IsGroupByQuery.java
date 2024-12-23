package fr.gdd.passage.volcano.querypatterns;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpProject;

import java.util.ArrayList;
import java.util.List;

public class IsGroupByQuery extends ReturningOpVisitor<Boolean> {

    OpProject opProject;
    List<OpExtend> opExtends = new ArrayList<>();
    OpGroup opGroup;

    @Override
    public Boolean visit(Op op) {
        try {
            return super.visit(op);
        } catch (UnsupportedOperationException e) {
            return false;
        }
    }

    @Override
    public Boolean visit(OpProject project) {
        // TODO check the content of the project
        opProject = project;
        return visit(project.getSubOp());
    }

    @Override
    public Boolean visit(OpExtend extend) {
        // TODO check the content of the group by
        opExtends.add(extend);
        return visit(extend.getSubOp());
    }

    @Override
    public Boolean visit(OpGroup groupBy) {
        this.opGroup = groupBy;
        return true;
    }
}
