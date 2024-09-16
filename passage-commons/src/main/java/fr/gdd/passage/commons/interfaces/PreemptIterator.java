package fr.gdd.passage.commons.interfaces;

import java.io.Serializable;

/**
 * Interface that states a scan iterator can pause/resume its
 * execution.
 * @param <SKIP> The serializable type that is provided and used pause/resume.
 */
public interface PreemptIterator<SKIP extends Serializable> {

    /**
     * Goes to the targeted element directly.
     * @param to The cursor location to skip to.
     */
    void skip(final SKIP to);

    /**
     * @return The current offset that allows skipping.
     */
    SKIP current();

    /**
     * @return The previous offset that allows skipping.
     */
    SKIP previous();

}
