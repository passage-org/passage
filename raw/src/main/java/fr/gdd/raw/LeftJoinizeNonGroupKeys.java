package fr.gdd.raw;

import fr.gdd.jena.utils.OpLeftJoinFail;
import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.List;

public class LeftJoinizeNonGroupKeys extends ReturningOpBaseVisitor {

    public Op visit(OpBGP op) {
        List<Triple> triples = op.getPattern().getList();
        if (triples.isEmpty()) return op;

        Op transformed = new OpTriple(triples.remove(0));

        while(!triples.isEmpty()){
            Triple optionalTriple = triples.remove(0);
            OpTriple optionalOpTriple = new OpTriple(optionalTriple);
            // TODO : handle expressions
            transformed = OpLeftJoinFail.createLeftJoinFail(transformed, optionalOpTriple, null);
        }

        return transformed;
    }

    private static List<Var> extractVars(Triple triple){
        List<Var> vars = new ArrayList<>();

        if(triple.getSubject().isVariable()){
            vars.add((Var) triple.getSubject());
        }

        if(triple.getPredicate().isVariable()){
            vars.add((Var) triple.getPredicate());
        }

        if(triple.getObject().isVariable()){
            vars.add((Var) triple.getObject());
        }

        return vars;
    }
}
