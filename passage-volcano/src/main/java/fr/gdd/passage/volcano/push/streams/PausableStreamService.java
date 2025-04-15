package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.exceptions.PauseException;
import fr.gdd.passage.volcano.federation.LocalServices;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.rowset.rw.rs_json.RowSetBuffered;
import org.apache.jena.riot.rowset.rw.rs_json.RowSetJSONStreamingWithMetadata;
import org.apache.jena.riot.rowset.rw.rs_json.RowSetReaderJSONWithMetadata;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ResultSetStream;
import org.apache.jena.sparql.exec.ResultSetAdapter;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTPBuilder;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Calls a remote service that we assume implements continuation queries as
 * well.
 * *
 * Since the waiting time for queries can be substantial, this iterator also
 * allows pausing query execution, like scan iterators.
 * *
 * It may trigger a pause before receiving the (possibly partial) results of
 * the service query. When this happens. It acts as the query never happened.
 */
public class PausableStreamService<ID,VALUE> implements PausableStream<ID,VALUE> {

    final BackendBindings<ID,VALUE> input;
    final OpService service;
    final PassageExecutionContext<ID,VALUE> context;
    final QueryExecutionHTTPBuilder builder;
    ResultSet wrapped; // not final because of possible throw
    final LocalServices localServices;

    public PausableStreamService(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpService service) {
        this.context = context;
        this.service = service;
        this.input = input;
        this.builder =  QueryExecutionHTTPBuilder
                .service(service.getService().getURI())
                .substitution(input)
                .query(OpAsQuery.asQuery(service.getSubOp()));
        this.localServices = context.localServices;

        // before creating a new call, we check if we should, but then, wrapped can be null
        if (!this.context.paused.isPaused() && context.stoppingConditions.stream().anyMatch(c -> c.test(context))) { throw new PauseException(service); }

        // TODO change this to all handled by the service handler
        try {
            if (this.localServices.contains(service.getService().getURI())) {
                var result = this.localServices.query(service.getService().getURI(), service.getSubOp(), input);
                Set<Var> projected = OpVars.visibleVars(service.getSubOp());
                wrapped = ResultSetStream.create(projected.stream().toList(), result.getLeft().stream().iterator());
            } else {
                // TODO include this in service handler
                RowSetReaderJSONWithMetadata.install(); // making sure that the http handler can read json *WITH* metadata…
                wrapped = builder.select();
            }
        } catch (RuntimeException e) {
            if (service.getSilent()) {
                wrapped = ResultSetStream.create(List.of(), Collections.emptyIterator());
            } else {
                throw e;
            }
        }
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(wrapped, 0), false)
                .map(qs -> new BackendBindings<>(qs, context.backend))
                .onClose(() -> System.out.println("done stream…"));
    }

    @Override
    public Op pause() {
        // initialized but did not execute at all.
        if (Objects.isNull(wrapped)) return this.service;

        // we must get the things that we already retrieved but did not compute yet.
        Table table = new TableN();
        while (wrapped.hasNext()) {
            table.addBinding(new BackendBindings<>(wrapped.next(), context.backend));
        }
        OpTable opTable = table.isEmpty() ? null : OpTable.create(table);
        Op continuationQuery = this.getContinuationOfService();
        OpService newService = Objects.isNull(continuationQuery) ?
                null :
                new OpService(service.getService(), continuationQuery, service.getSilent());

        if (Objects.nonNull(opTable) && Objects.nonNull(newService)) {
            // union between what we already retrieved and the rest
            return OpUnion.create(opTable, newService);
        } else if (Objects.nonNull(opTable)) {
            // what we retrieved is all
            return opTable;
        } else {
            // we processed every result mappings, so only the rest as a service query is left.
            return newService;
        }
    }

    /**
     * @return The continuation query received from the endpoint. Null if no answer yet, or
     *         if the service provided complete result already.
     */
    private Op getContinuationOfService() {
        if (Objects.isNull(wrapped)) return null;

        return ((RowSetJSONStreamingWithMetadata<?>)
                ((RowSetBuffered<?>)
                        ((ResultSetAdapter) wrapped).get()).getDelegate()).continuationQuery;
    }
}
