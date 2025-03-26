package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.OpAsQuery;
import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.base.query.SPARQLQuery;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.physical.NullaryPhysicalOp;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalOperatorForLogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.physical.impl.BaseForPhysicalOps;

public class PhysicalOpBGP extends BaseForPhysicalOps implements NullaryPhysicalOp, PhysicalOperatorForLogicalOperator, SPARQLQuery, SPARQLRequest {

    final LogicalOpBGP opBGP;

    PhysicalOpBGP(LogicalOpBGP opBGP) {
        this.opBGP = opBGP;
    }

    @Override
    public NullaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ExpectedVariables getExpectedVariables(ExpectedVariables... inputVars) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(PhysicalPlanVisitor visitor) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public LogicalOperator getLogicalOperator() {
        return opBGP;
    }

    @Override
    public Query asJenaQuery() {
        return OpAsQuery.asQuery(opBGP.opBGP);
    }

    @Override
    public SPARQLGraphPattern getQueryPattern() {
        return null;
    }

    @Override
    public ExpectedVariables getExpectedVariables() {
        return null;
    }
}
