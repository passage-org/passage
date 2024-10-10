package fr.gdd.passage.cli.writers;

import fr.gdd.passage.cli.server.BindingWrapper;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriter;
import org.apache.jena.riot.rowset.RowSetWriterFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.resultset.ResultSetException;
import org.apache.jena.sparql.util.Context;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;

import java.io.OutputStream;
import java.io.Writer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BlazegraphSerializer implements RowSetWriter {

    public static RowSetWriterFactory factory = lang -> {
        if (!Objects.equals(lang, ResultSetLang.RS_JSON ) )
            throw new ResultSetException("ResultSetWriter for JSON asked for a "+lang);
        return new BlazegraphSerializer();
    };

    @Override
    public void write(OutputStream out, RowSet rowSet, Context context) {
        SPARQLResultsJSONWriter writer = new SPARQLResultsJSONWriter(out);

        org.eclipse.rdf4j.query.TupleQueryResult tqr = new org.eclipse.rdf4j.query.TupleQueryResult () {
            @Override
            public List<String> getBindingNames() throws org.eclipse.rdf4j.query.QueryEvaluationException {
                return rowSet.getResultVars().stream().map(Var::getVarName).collect(Collectors.toList());
            }

            @Override
            public void close() throws org.eclipse.rdf4j.query.QueryEvaluationException {
                // nothing
            }

            @Override
            public boolean hasNext() throws org.eclipse.rdf4j.query.QueryEvaluationException {
                return rowSet.hasNext();
            }

            @Override
            public org.eclipse.rdf4j.query.BindingSet next() throws org.eclipse.rdf4j.query.QueryEvaluationException {
                return ((BindingWrapper) rowSet.next()).getWrapped();
            }

            @Override
            public void remove() throws org.eclipse.rdf4j.query.QueryEvaluationException {
                throw new UnsupportedOperationException("Not implemented");
            }
        };

        try {
            try {
                writer.startQueryResult(tqr.getBindingNames());

                while (tqr.hasNext()) {
                    BindingSet bindingSet = tqr.next();
                    writer.handleSolution(bindingSet);
                }
            }
            finally {
                tqr.close();
            }
            writer.endQueryResult();
        } catch (TupleQueryResultHandlerException | QueryEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Writer out, RowSet rowSet, Context context) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public void write(OutputStream out, boolean result, Context context) {
        throw new UnsupportedOperationException("Not implemented.");
    }
}
