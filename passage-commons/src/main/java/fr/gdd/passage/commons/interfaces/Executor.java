package fr.gdd.passage.commons.interfaces;

import fr.gdd.passage.commons.io.PassageInput;
import fr.gdd.passage.commons.io.PassageOutput;

import java.io.Serializable;

/**
 * Calling `execute` with the proper `passageInput` to execute the query
 * that may pause itself during execution.

 **/
public interface Executor<SKIP extends Serializable> {

    /**
     * @return The - possibly partial - result of the query along with
     * metadata when resuming is possible.
     */
    PassageOutput<SKIP> execute(PassageInput<SKIP> passageInput);

}
