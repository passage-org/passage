package fr.gdd.passage.cli.operations;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.apache.jena.sparql.util.Symbol;

import java.util.HashMap;
import java.util.Map;

/**
 * This allows requesters to put additional arguments within the headers of the HTTP request.
 * There are no constraints on the input headers. If they are not handled by the query engine,
 * they will be simply ignored.
 */
public class SPARQL_QueryDatasetWithHeaders extends SPARQL_QueryDataset {

    @Override
    protected void execute(String queryString, HttpAction action) {
        // get the additional query info
        Map<String, String> inputRetrievedFromRequest = getFromBodyOrHeader(action);
        // and put them into the context
        // We cannot override an already set value
        inputRetrievedFromRequest.forEach((key, value) -> action.getContext().setIfUndef(Symbol.create(key), value));

        super.execute(queryString, action);
    }

    // If at some point, the saved state need to be sent in the
    // headers, it could be done by @overriding
    // `sendResults(HttpAction action, SPARQLResult result, Prologue qPrologue)`

    /**
     * @param action The received http action
     * @return The map of key -> value extracted from the header and body of the http request.
     */
    static public Map<String, String> getFromBodyOrHeader(HttpAction action) {
        HashMap<String, String> k2v = new HashMap<>();
        HttpServletRequest req = action.getRequest();
        // With the body of the request
        req.getParameterNames().asIterator().forEachRemaining(k -> k2v.put(k, req.getParameter(k)));
        req.getHeaderNames().asIterator().forEachRemaining(k -> k2v.put(k, req.getHeader(k)));
        return k2v;
    }
}
