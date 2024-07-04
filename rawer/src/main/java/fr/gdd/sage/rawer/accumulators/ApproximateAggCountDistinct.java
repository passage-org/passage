package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.RawerConstants;
import fr.gdd.sage.rawer.RawerOpExecutor;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.accumulators.SagerAccumulator;
import fr.gdd.sage.sager.optimizers.CardinalityJoinOrdering;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import fr.gdd.sage.sager.pause.Triples2BGP;
import org.apache.jena.atlas.lib.EscapeStr;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.impl.LiteralLabelFactory;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.function.FunctionEnv;
import org.apache.jena.sparql.lang.arq.ARQParser;
import org.apache.jena.sparql.lang.arq.ParseException;
import org.apache.jena.sparql.util.ExprUtils;
import org.apache.jena.sparql.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Expression;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Perform an estimate of the COUNT based on random walks performed on
 * the subQuery.
 */
public class ApproximateAggCountDistinct<ID,VALUE> implements SagerAccumulator<ID, VALUE> {

    private final static Logger log = LoggerFactory.getLogger(ApproximateAggCountDistinct.class);

    final ExecutionContext context;
    final Backend<ID,VALUE,?> backend;
    final OpGroup group;

    final ApproximateAggCount<ID,VALUE> bigN;
    Double sumOfInversedProba = 0.;
    Double sumOfInversedProbaOverFmu = 0.;
    final WanderJoinVisitor<ID,VALUE> wj;

    final Set<Var> vars;
    long sampleSize = 0; // for debug purposes
    long nbZeroFmu = 0; // for debug purposes

    public ApproximateAggCountDistinct(ExprList varsAsExpr, ExecutionContext context, OpGroup group) {
        this.context = context;
        this.backend = context.getContext().get(RawerConstants.BACKEND);
        this.group = group;
        this.bigN = new ApproximateAggCount<>(context, group.getSubOp());
        Save2SPARQL<ID,VALUE> saver = context.getContext().get(SagerConstants.SAVER);
        this.wj = new WanderJoinVisitor<>(saver.op2it);
        this.vars = varsAsExpr.getVarsMentioned();

    }

    @Override
    public void accumulate(BackendBindings<ID, VALUE> binding, FunctionEnv functionEnv) {
        // #1 processing of N
        this.bigN.accumulate(binding, functionEnv);

        if (Objects.isNull(binding)) {return;}
        sampleSize += 1; // only account for those which succeed

        // #2 processing of P_mu
        double proba = ReturningOpVisitorRouter.visit(wj, group.getSubOp());
        double inversedProba = proba == 0. ? 0. : 1./proba;
        sumOfInversedProba += inversedProba;

        // #3 processing of F_mu
        // #A bind the variable with their respective value
        CacheId<ID,VALUE> cache = new CacheId<>(backend); // bound variables are cached as we already know the ID
        Op right = group.getSubOp();

        for (Var v : vars) {
            // Important note: here we have a placeholder because we already have the id, but we
            // don't need the actual value as a string, since we stay in the same engine overall.
            // So this improves (i) performance as we don't need to retrieve the actual value in the database;
            // and (ii) reliability as we get the id from the database, we know for sure it exists as is,
            // while another round of translation would not guarantee it.
            // However, if we happen to output the subquery, it would display placeholders that are meaningless
            // and the query would not return anything. This could be an issue for Sage.
            Node valueAsNode = NodeFactory.createLiteralString("PLACEHOLDER_" + v.toString());
//            String valueAsString = binding.get(v).getString();
//            // valueAsString = EscapeStr.stringEsc(valueAsString);
//            parser.ReInit(new ByteArrayInputStream(valueAsString.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8.toString());
//
//            Node valueAsNode = null;
//            try {
//                valueAsNode = parser.VarOrTerm();
//            } catch (ParseException e){
//                throw new UnsupportedOperationException("Failed to parse the value as node…");
//            }

//            if (valueAsString.startsWith("\"") && valueAsString.endsWith("\"")) {
//                valueAsString = LiteralLabelFactory.createTypedLiteral(valueAsString.substring(1, valueAsString.length()-1)).toString();
//            }

//            NodeValue valueAsNode = ExprUtils.parseNodeValue(valueAsString);
            cache.register(valueAsNode, binding.get(v).getId());
            right = OpJoin.create(OpExtend.extend(OpTable.unit(), v, ExprLib.nodeToExpr(valueAsNode)), right);
        }

        // #B The join ordering of the COUNT subquery using cardinalities will be done by the query executor.
        // However, it needs triple patterns to be organized into BGPs.
        right = ReturningOpVisitorRouter.visit(new Triples2BGP(), right);

        // #C wrap as a COUNT query
        Var countVariable = Var.alloc(RawerConstants.COUNT_VARIABLE);
        // Does not print as a proper COUNT because it does not follow the PROJECT EXTEND GROUP BY
        OpGroup countQuery = new OpGroup(right, new VarExprList(), List.of(new ExprAggregator(countVariable, new AggCount())));


        ExecutionContext newExecutionContext = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        newExecutionContext.getContext().set(RawerConstants.BACKEND, backend);
        RawerOpExecutor<ID,VALUE> fmuExecutor = new RawerOpExecutor<ID,VALUE>(newExecutionContext)
                .setLimit(7_000L) // TODO make this configurable, the number of scans in the subquery
                .setTimeout(10_000L) // TODO make this configurable as well, the allowed execution time for the subquery
                .setCache(cache);

        Iterator<BackendBindings<ID,VALUE>> estimatedFmus = fmuExecutor.execute(countQuery);
        if (!estimatedFmus.hasNext()) {
            // no time to execute maybe ?
            throw new UnsupportedOperationException("TODO need to look at this exception");
        }
        BackendBindings<ID,VALUE> estimatedFmu = estimatedFmus.next();

        long nbScansSubQuery = newExecutionContext.getContext().get(RawerConstants.SCANS);
        context.getContext().set(RawerConstants.SCANS,
                context.getContext().getLong(RawerConstants.SCANS,0L)
                        + nbScansSubQuery);

        // #D TODO ugly but need to be parsed to Double again…
        String fmuAsString = estimatedFmu.get(countVariable).getString();

        // Remove the datatype part and quotes from the string to get the numeric value
        String valueString = fmuAsString.substring(2, fmuAsString.indexOf("\"^^")); // TODO unuglify all this...
        // Create a Literal with the given value and the full xsd:double URI
        Literal literal = ResourceFactory.createTypedLiteral(valueString, TypeMapper.getInstance().getSafeTypeByName("http://www.w3.org/2001/XMLSchema#double"));
        double fmuAsDouble = literal.getDouble();

//        // (for debugging TODO remove this)
//        String countQueryAsString = OpAsQuery.asQuery(countQuery).toString();
//        long actualFMU = -1;
//        try {
//            actualFMU = ((BlazegraphBackend) fmuExecutor.getBackend()).countQuery(countQueryAsString);
//        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
//            throw new RuntimeException(e);
//        }
//
//        if (actualFMU != fmuAsDouble) {
//            System.out.println(binding);
//            System.out.println(actualFMU + "     VS     " + fmuAsDouble);
//        }

        // Math.max because we know for sure that there is at least one result,
        // but we failed to find it in the subquery.
        // TODO bootstrap the count with binding that we found in the count distinct part.
        if (fmuAsDouble == 0.) {
            nbZeroFmu += 1;
            log.warn(binding.toString());
        } // for debug purpose
        sumOfInversedProbaOverFmu += inversedProba / Math.max(1., fmuAsDouble);

    }

    @Override
    public VALUE getValue() {
        log.debug("BigN SampleSize: " + bigN.sampleSize);
        log.debug("CRAWD SampleSize: " + sampleSize);
        log.debug("Nb Total Scans: " + context.getContext().get(RawerConstants.SCANS));
        log.debug("Nb zeros in Fmu: " + nbZeroFmu);
        Backend<ID,VALUE,?> backend = context.getContext().get(RawerConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^%s", getValueAsDouble(), XSDDatatype.XSDdouble.getURI()));
    }

    public double getValueAsDouble () {
        return sumOfInversedProba == 0. ? 0. : (bigN.getValueAsDouble()/sumOfInversedProba) * sumOfInversedProbaOverFmu;
    }

}
