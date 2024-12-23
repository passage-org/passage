package fr.gdd.passage.volcano.transforms;

import fr.gdd.jena.utils.OpCloningUtil;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.optimize.ExprTransformConstantFold;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprTransformSubstitute;
import org.apache.jena.sparql.expr.ExprTransformer;

import java.util.*;
import java.util.stream.Collectors;

public class FactorizeExtends {


    public static OpExtend factorize(OpExtend extend, Op subOp) {
        Set<Var> varsOfInterest = extend.getVarExprList().getExprs().values().stream()
                .flatMap(v->v.getVarsMentioned().stream())
                .collect(Collectors.toSet());

        Map<Var, List<Expr>> varToExpr = new HashMap<>();
        List<OpExtend> extendsToKeep = new ArrayList<>();
        while (subOp instanceof OpExtend subExtend) {
            Var varAssigned = subExtend.getVarExprList().getVars().get(0);
            Expr exprAssign = subExtend.getVarExprList().getExpr(varAssigned);
            if (varsOfInterest.contains(varAssigned)) {
                varToExpr.putIfAbsent(varAssigned, new ArrayList<>());
                varToExpr.get(varAssigned).add(exprAssign);
            } else {
                extendsToKeep.add(subExtend);
            }
            subOp = subExtend.getSubOp();
        }
        Op lastOp = subOp;

        Map<Var, Expr> varToTransformed = new HashMap<>();
        extend.getVarExprList().forEachExpr((assignedVar, initialExpr) -> {
            Expr replaced = initialExpr;
            for (Var var : varsOfInterest) {
                for (Expr expr : varToExpr.get(var)) {
                    replaced = ExprTransformer.transform(new ExprTransformSubstitute(var, expr), replaced);
                }
            }
            varToTransformed.put(assignedVar, replaced);
        } );

        // remove the extends that have been used.
        List<Op> newOps = new ArrayList<>();
        newOps.add(lastOp); // put the last Op in
        // then we link together the remaining ops
        for (int i = extendsToKeep.size() - 1; i >= 0; i-=1) {
            newOps.add(OpCloningUtil.clone(extendsToKeep.get(i), newOps.getLast()));
        }

        VarExprList rewritten  = new VarExprList();
        for (Map.Entry<Var, Expr> entry : varToTransformed.entrySet()) {
            // For example, to factorize expression like ((?v +1) +1) by (?v +2)
            Expr factorizedConstant = ExprTransformer.transform(new ExprTransformConstantFold(), entry.getValue());
            factorizedConstant = ExprUnfolder.unfoldAddition(factorizedConstant); // TODO make it more general, it only works for addition for now
            rewritten.add(entry.getKey(), factorizedConstant);
        }

        return OpExtend.create(newOps.getLast(), rewritten);
    }
}
