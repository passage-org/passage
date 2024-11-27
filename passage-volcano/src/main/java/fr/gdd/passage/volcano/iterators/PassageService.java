package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.factories.IBackendServicesFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.iterators.BackendIteratorOverInput;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.pause.PausableIterator;
import fr.gdd.passage.volcano.pause.PauseException;
import org.apache.jena.query.Query;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.rowset.rw.rs_json.RowSetBuffered;
import org.apache.jena.riot.rowset.rw.rs_json.RowSetJSONStreamingWithMetadata;
import org.apache.jena.riot.rowset.rw.rs_json.RowSetReaderJSONWithMetadata;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.exec.ResultSetAdapter;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTPBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Calls a remote service that we assume implements continuation queries as
 * well.
 * *
 * Since the waiting time for queries can be substantial, this iterator also
 * allows pausing query execution, like ScanIterators.
 * *
 * It may trigger a pause before receiving the (possibly partial) results of
 * the service query. When this happens. It acts as the query never happened.
 */
public class PassageService<ID,VALUE> extends PausableIterator<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final private static Logger log = LoggerFactory.getLogger(PassageService.class);

    public static <ID,VALUE> IBackendServicesFactory<ID,VALUE> factory() {
        return (context, input, op) -> new BackendIteratorOverInput<>(context, input, op, PassageService::new);
    }

    final OpService service;
    final BackendBindings<ID,VALUE> input;
    final PassageExecutionContext<ID,VALUE> context;
    final QueryExecutionHTTPBuilder builder;

    public static Function<ExecutionContext, Boolean> stopping = (ec) ->
            System.currentTimeMillis() >= ec.getContext().getLong(PassageConstants.DEADLINE, Long.MAX_VALUE);

    ResultSet wrapped;

    public PassageService(ExecutionContext context, BackendBindings<ID,VALUE> input, OpService service) {
        super((PassageExecutionContext<ID,VALUE>) context, service);
        this.context = (PassageExecutionContext<ID,VALUE>) context;
        this.service = service;
        this.input = input;
        this.builder =  QueryExecutionHTTPBuilder
                .service(service.getService().getURI())
                .substitution(input)
                .query(OpAsQuery.asQuery(service.getSubOp()));

        // before creating a new call, we check if we should, but then, wrapped can be null
        if (!this.context.paused.isPaused() && stopping.apply(this.context)) { throw new PauseException(service); }

        RowSetReaderJSONWithMetadata.install(); // making sure that the http handler can read json *WITH* metadata…
        wrapped = builder.select();
    }

    @Override
    public boolean hasNext() {
        // TODO multiple hasNext calls

        if (!wrapped.hasNext()) {
            wrapped.close();
            Op continuationQuery = getContinuationOfService();
            if (Objects.isNull(continuationQuery)) {
                return false;
            } else {
                // before calling another time the service, we check if we should.
                if (!context.paused.isPaused() && stopping.apply(context)) { throw new PauseException(service); }

                Query query = OpAsQuery.asQuery(continuationQuery);
                log.debug(query.toString());
                wrapped = builder.query(query).select();
            }
        }

        // TODO it's awaiting, so if we want to pause during this execution,
        //      we should create a loop that checks for timeout.
        return wrapped.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        ((AtomicLong) context.getContext().get(PassageConstants.SERVICE_CALLS)).getAndIncrement();
        return new BackendBindings<ID,VALUE>(wrapped.next()).setParent(input);
    }

    @Override
    public Op pause() {
        // initialized but did not execute at all.
        if (Objects.isNull(wrapped)) return this.service;

        // we must get the things that we already retrieved but did not compute yet.
        Table table = new TableN();
        while (wrapped.hasNext()) {
            table.addBinding(this.next());
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
