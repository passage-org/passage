package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.Op0;
import se.liu.ida.hefquin.base.query.SPARQLQuery;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.NullaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;
import se.liu.ida.hefquin.engine.queryplan.physical.NullaryPhysicalOp;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalOperatorForLogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

public class Op0AsNullary extends LogicalOperatorBase implements NullaryLogicalOp, NullaryPhysicalOp, PhysicalOperatorForLogicalOperator, SPARQLQuery {

    final Op0 op;

    public Op0AsNullary(Op0 op) {
        this.op = op;
    }

    public Op0 getOp() {
        return op;
    }

    @Override
    public ExpectedVariables getExpectedVariables(ExpectedVariables... inputVars) {
        return null;
    }

    @Override
    public int getID() {
        return 0;
    }

    @Override
    public void visit(PhysicalPlanVisitor visitor) {

    }

    @Override
    public void visit(LogicalPlanVisitor visitor) {
        // Do nothing
        // throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NullaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
        return null;
    }

    @Override
    public LogicalOperator getLogicalOperator() {
        return this;
    }

    @Override
    public Query asJenaQuery() {
        return OpAsQuery.asQuery(op);
    }
}
