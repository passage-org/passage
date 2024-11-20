package fr.gdd.passage.volcano.iterators.service;

import fr.gdd.passage.commons.factories.IBackendServicesFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.iterators.BackendIteratorOverInput;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.iterators.PausableIterator;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.exec.http.QueryExecutionHTTPBuilder;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.Symbol;

import java.util.Iterator;

public class PassageService<ID,VALUE> extends PausableIterator<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendServicesFactory<ID,VALUE> factory() {
        return (context, input, op) -> new BackendIteratorOverInput<>(context, input, op, PassageService::new);
    }

    ResultSet wrapped;

    public PassageService(ExecutionContext context, BackendBindings<ID,VALUE> input, OpService op) {
        super((PassageExecutionContext<ID,VALUE>) context, op);

        Context io = Context.create();
        RowSetReaderJSONWithMetadata.install(io);
        QueryExecutionHTTPBuilder builder = QueryExecutionHTTPBuilder.create();

        wrapped = builder.endpoint(op.getService().getURI()) .query(OpAsQuery.asQuery(op.getSubOp())).select();

    }

    @Override
    public boolean hasNext() {
        return wrapped.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return new BackendBindings<>(wrapped.next()); // TODO TODO
    }

    @Override
    public Op pause() {
        return super.pause();
    }
}
