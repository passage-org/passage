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
