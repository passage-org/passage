package fr.gdd.jena.utils;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.expr.ExprList;

public class OpLeftJoinFail extends OpLeftJoin {
    protected OpLeftJoinFail(Op left, Op right, ExprList exprs) {
        super(left, right, exprs);
    }

    @Override
    public String getName(){
        return "leftJoinFail";
    }

    public static OpLeftJoinFail createLeftJoinFail(Op left, Op right, ExprList exprs) {
        return new OpLeftJoinFail(left, right, exprs);
    }
}
