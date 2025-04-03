package fr.gdd.passage.volcano.federation;

import org.apache.jena.graph.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class ToSummaryPattern {

    // it needs to change variable names to work
    HashMap<Var, Var> original2modified = new HashMap<>();

    public Node toSummaryTerm (Node node, int modulo) {
        return switch (node) {
            case Node_URI ignored -> {
                try {
                    URI uri = new URI(node.getURI()); // `URI` provides better methods than `Node_URI`
                    int hashcode = Math.abs(uri.toString().hashCode());
                    if (modulo == 0 || modulo == 1) {
                        yield NodeFactory.createURI(uri.getScheme() + "://" + uri.getAuthority());
                    } else {
                        yield NodeFactory.createURI(uri.getScheme() + "://" + uri.getAuthority() + "/" + (hashcode % modulo));
                    }
                } catch (URISyntaxException e) {
                    yield NodeFactory.createURI("https://donotcare.com/whatever");
                }
            }
            case Node_Blank ignored -> NodeFactory.createBlankNode("_:any");
            case Node_Literal ignored -> NodeFactory.createLiteralString("any");
            case Node_Variable v -> original2modified.computeIfAbsent(Var.alloc(v.getName()), k ->
                    Var.alloc(String.format("__%s%s", k.getVarName(), original2modified.size())));
            default -> throw new UnsupportedOperationException("Unknown kind of node: " + node);
        };
    }

    public Triple toSummaryTriple (Triple triple, int modulo) {
        return Triple.create(
                toSummaryTerm(triple.getSubject(), modulo),
                toSummaryTerm(triple.getPredicate(), modulo),
                toSummaryTerm(triple.getObject(), modulo));
    }

    public Quad toSummaryQuad (Quad quad, int modulo) {
        throw new UnsupportedOperationException("Quads are not supported yet.");
    }

}
