package fr.gdd.passage.volcano.transforms;

import org.apache.jena.sparql.expr.E_Add;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

// Generated via ChatGPT4.0
public class ExprUnfolder {
    public static Expr unfoldAddition(Expr expr) {
        List<Expr> terms = new ArrayList<>();
        collectAdditionTerms(expr, terms);

        // Sum up constant terms
        BigDecimal constantSum = BigDecimal.ZERO;
        List<Expr> nonConstantTerms = new ArrayList<>();

        for (Expr term : terms) {
            if (term.isConstant()) {
                constantSum = constantSum.add(term.getConstant().getDecimal());
            } else {
                nonConstantTerms.add(term);
            }
        }

        // Combine constant terms into one
        if (constantSum.compareTo(BigDecimal.ZERO) != 0) {
            nonConstantTerms.add(NodeValue.makeInteger(constantSum.intValue())); // TODO type depending on desired output
        }

        // Reconstruct the unfolded expression
        return reconstructAddition(nonConstantTerms);
    }

    private static void collectAdditionTerms(Expr expr, List<Expr> terms) {
        if (expr instanceof E_Add) {
            // Recursively collect terms from left and right
            collectAdditionTerms(((E_Add) expr).getArg1(), terms);
            collectAdditionTerms(((E_Add) expr).getArg2(), terms);
        } else {
            terms.add(expr); // Add non-addition term
        }
    }

    private static Expr reconstructAddition(List<Expr> terms) {
        if (terms.isEmpty()) {
            return NodeValue.makeInteger(0); // Default: 0
        }

        Expr result = terms.get(0);
        for (int i = 1; i < terms.size(); i++) {
            result = new E_Add(result, terms.get(i));
        }
        return result;
    }
}

