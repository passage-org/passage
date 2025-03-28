package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.Op2;
import se.liu.ida.hefquin.base.query.SPARQLQuery;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.BinaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.logical.BinaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;
import se.liu.ida.hefquin.engine.queryplan.physical.BinaryPhysicalOp;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalOperatorForLogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

public class Op2AsBinary extends LogicalOperatorBase implements BinaryLogicalOp, BinaryPhysicalOp, PhysicalOperatorForLogicalOperator, SPARQLQuery {

    final Op2 op;

    public Op2AsBinary(Op2 op) {
        this.op = op;
    }

    public Op2 getOp() {
        return op;
    }


    @Override
    public ExpectedVariables getExpectedVariables(ExpectedVariables... inputVars) {
        throw new UnsupportedOperationException("Not supported yet.");
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
    public BinaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
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
