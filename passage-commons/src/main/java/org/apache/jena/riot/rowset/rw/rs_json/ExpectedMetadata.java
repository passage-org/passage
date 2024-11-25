package org.apache.jena.riot.rowset.rw.rs_json;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.sparql.algebra.Algebra;

import java.io.IOException;
import java.util.Objects;

// Disclaimer: this is part of org.apache…rs_json package
// since some classes are in the visibility of the package and this
// allows us to copy only a few of them.
// In this, we only add a behavior to read `metadata`. Each engine is
// free to register its own fields in the metadata field for third-party
// usage.

/**
 * RowsetJSONStreamingWithMetadata does not properly handle `metadata` by default.
 * So when a foreign keyword is read, it triggers this handler, which itself must
 * dispatch the event to the proper handler.
 */
public class ExpectedMetadata implements RowSetJSONStreamingWithMetadata.UnexpectedJsonEltHandler {

    final ErrorHandler errorHandler;
    final ValidationSettings validationSettings;
    RowSetJSONStreamingWithMetadata<?> parent;

    public ExpectedMetadata(ErrorHandler errorHandler, ValidationSettings validationSettings) {
        this.errorHandler = errorHandler;
        this.validationSettings = validationSettings;
    }

    public void setParent(RowSetJSONStreamingWithMetadata<?> parent) {
        this.parent = parent;
    }

    @Override
    public JsonElement apply(Gson gson, JsonReader reader) throws IOException {
        // TODO make it more generic
        if (reader.getPath().contains("metadata")) { // we read a metadata field
            JsonObject metadataObject = gson.fromJson(reader, JsonObject.class);
            JsonPrimitive nextObject = metadataObject.getAsJsonPrimitive("next");
            if (Objects.isNull(nextObject)) {
                parent.continuationQuery = null;
                return null;
            }
            String continuationQueryAsString = nextObject.getAsString();
            parent.continuationQuery = Algebra.compile(QueryFactory.create(continuationQueryAsString));
            return null;
        }

        // otherwise, normal behavior
        ErrorHandlers.relay(errorHandler, validationSettings.getUnexpectedJsonElementSeverity(), () ->
                new ErrorEvent("Encountered unexpected json element at path " + reader.getPath()));
        reader.skipValue();

        return null;
    }
}
