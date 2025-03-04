package fr.gdd.passage.blazegraph;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

/**
 * A dumb handler that allows getting BigdataValue once parsed by
 * a RDFParser. We only get the object, since subject and predicate
 * are placeholder: objects allow all kinds of BigdataValue.
 */
public class GetValueStatementHandler extends RDFHandlerBase {

    Value value = null;

    public Value get() {
        return value;
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        this.value = st.getObject();
    }
}
