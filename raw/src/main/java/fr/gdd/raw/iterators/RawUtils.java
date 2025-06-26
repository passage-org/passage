package fr.gdd.raw.iterators;

import jakarta.json.*;
import org.apache.jena.sparql.engine.binding.Binding;

public class RawUtils {

    public static JsonObject buildUnion(JsonObject subUnionJson, int chosen){
        // Creating a new union object in the binding

        JsonArray vars = subUnionJson.getJsonArray("vars");

        Double probability = subUnionJson.getJsonNumber("probability").doubleValue();

        JsonObject currentUnionJson = Json.createObjectBuilder()
                .add("type", "union")
                .add("child", chosen)
                .add("sub", subUnionJson)
                .add("vars", vars)
                // As we retrieve a random walk, this probability cannot be known
                // in advance. It is computed based on the number of branches of this union that have been explored by
                // a number of random walks. For now, we keep the probability of the child, as if this union had only one branch,
                // and when computing the estimation later, we apply the observed probability.
                .add("probability", probability)
                .build();

        return currentUnionJson;
    }

    public static JsonObject buildScan(Binding binding, Double probability){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        binding.vars().forEachRemaining(var -> jab.add(Json.createValue(var.toString())));

        JsonArray jsonArray = jab.build();

        JsonObject scanJson = Json.createObjectBuilder()
                .add("type", "scan")
                .add("vars", jsonArray)
                .add("probability", probability)
                .build();

        return scanJson;
    }

    public static JsonObject buildJoin(JsonObject left, JsonObject right){
        // Computing probabilities for current join
        JsonValue probabilityLeftJson = left.get("probability");
        Double probabilityLeft = Double.valueOf(probabilityLeftJson.toString());
        JsonValue probabilityRightJson = right.get("probability");
        Double probabilityRight = Double.valueOf(probabilityRightJson.toString());

        Double probability = probabilityLeft.doubleValue() * probabilityRight.doubleValue();

        JsonArray varsLeft = left.getJsonArray("vars");
        JsonArray varsRight = right.getJsonArray("vars");

        JsonArrayBuilder varsBuilder = Json.createArrayBuilder();
        for(JsonValue var : varsLeft){
            varsBuilder.add(var);
        }
        for(JsonValue var : varsRight){
            varsBuilder.add(var);
        }

        JsonArray vars = varsBuilder.build();

        JsonObject joinJson = Json.createObjectBuilder()
                .add("type", "join")
                .add("left", left)
                .add("right", right)
                .add("probability", probability)
                .add("vars", vars)
                .build();

        return joinJson;
    }

    public static String stringify(JsonObject json){
        // Needed so that a json can be used as a rdf term inside a mapping. Implies that it is "destringified" before being parsed again
        return "\"" + json.toString().replace("\"", "\\\"") + "\"";
    }
}
