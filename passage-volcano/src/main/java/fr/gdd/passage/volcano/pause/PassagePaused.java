package fr.gdd.passage.volcano.pause;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;

import java.util.Objects;

/**
 * It's important that this is a pointer so when the context is saved,
 * even the copy of it done by the JSONWriter will be updated.
 */
public class PassagePaused {

    Boolean paused = false;

    Op pausedQuery;
    String pausedQueryAsString;

    public PassagePaused() {}

    public boolean isPaused() {
        return this.paused;
    }

    public void pause() {
        this.paused = true;
    }

    public void setPausedQuery(Op query) {
        this.pausedQuery = query;
    }

    public Op getPausedQuery() {
        return pausedQuery;
    }

    public String getPausedQueryAsString() {
        if (Objects.isNull(pausedQueryAsString) && Objects.nonNull(pausedQuery)) { // lazy
            pausedQueryAsString = OpAsQuery.asQuery(pausedQuery).toString();
        }
        return pausedQueryAsString;
    }

}
