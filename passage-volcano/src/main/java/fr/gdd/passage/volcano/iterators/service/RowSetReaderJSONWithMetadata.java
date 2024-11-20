package fr.gdd.passage.volcano.iterators.service;

import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetReader;
import org.apache.jena.riot.rowset.RowSetReaderFactory;
import org.apache.jena.riot.rowset.RowSetReaderRegistry;
import org.apache.jena.riot.rowset.rw.RowSetReaderJSONStreaming;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.sparql.exec.QueryExecResult;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.resultset.ResultSetException;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.Symbol;

import java.io.InputStream;
import java.io.Reader;
import java.util.Objects;

public class RowSetReaderJSONWithMetadata implements RowSetReader {

    public static void install(Context context) {
        // TODO OFC this should not work since in a global registry for a specific context
        RowSetReaderRegistry.register(ResultSetLang.RS_JSON, factory(context));
    }

    public static RowSetReaderFactory factory(Context context) {
        return (lang) -> {
            if (!Objects.equals(lang, ResultSetLang.RS_JSON))
                throw new ResultSetException("RowSet for JSON asked for a " + lang);
            return new RowSetReaderJSONWithMetadata(context);
        };}

    /* ************************************************************************ */

    final RowSetReader wrapped;
    final Context context;

    public RowSetReaderJSONWithMetadata(Context context) {
        this.wrapped = RowSetReaderJSONStreaming.factory.create(ResultSetLang.RS_JSON);
        this.context = context;
        this.context.set(Symbol.create("meow"), this);
    }

    @Override
    public RowSet read(InputStream in, Context context) {
        RowSet rowset = wrapped.read(in,context);
        return rowset;
    }

    @Override
    public QueryExecResult readAny(InputStream in, Context context) {
        QueryExecResult qer = wrapped.readAny(in, context);
        return qer;
    }

    @Override
    public RowSet read(Reader in, Context context) {
        RowSet rowset = wrapped.read(in, context);
        return rowset;
    }

}
