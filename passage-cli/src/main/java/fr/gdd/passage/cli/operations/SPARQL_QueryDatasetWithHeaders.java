package fr.gdd.passage.cli.operations;

import fr.gdd.passage.commons.generics.BackendConstants;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.jena.fuseki.servlets.ActionErrorException;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.apache.jena.fuseki.servlets.ServletOps;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.sparql.util.Symbol;

import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.jena.atlas.lib.Lib.uppercase;

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


    // (mostly comes from Apache Jena's `SPARQLQueryProcessor`)
    @Override
    public void validate(HttpAction action) {
        String method = uppercase(action.getRequestMethod());

        if ( HttpNames.METHOD_OPTIONS.equals(method) )
            return;

        if ( !HttpNames.METHOD_POST.equals(method) && !HttpNames.METHOD_GET.equals(method) ) {
            ServletOps.errorMethodNotAllowed("Not a GET or POST request");
            return;
        }

        if ( HttpNames.METHOD_GET.equals(method) && action.getRequestQueryString() == null ) {
            if (action.getContext().isDefined(BackendConstants.DESCRIPTION)) {
                Model description = action.getContext().get(BackendConstants.DESCRIPTION);
                // TODO not only as turtle, try to get the targeted content if possible
                StringWriter sw = new StringWriter();
                description.write(sw, Lang.TURTLE.getLabel());
                action.setResponseContentType(Lang.TURTLE.getLabel());
                ServletOps.writeMessagePlainTextError(action.getResponse(), sw.toString());
            }
            return; // 200
        }

        // Use of the dataset describing parameters is checked later.
        try {
            Collection<String> x = acceptedParams(action);
            validateParams(action, x);
            validateRequest(action);
        } catch (ActionErrorException ex) {
            throw ex;
        }
        // Query not yet parsed.
    }
}
