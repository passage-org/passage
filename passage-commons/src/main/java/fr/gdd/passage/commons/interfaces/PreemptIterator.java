package fr.gdd.passage.commons.interfaces;

/**
 * Interface that states a scan iterator can pause/resume its
 * execution.
 */
public interface PreemptIterator {

    /**
     * Goes to the targeted element directly.
     * @param to The cursor location to skip to.
     */
    void skip(final long to);

    /**
     * @return The current offset that allows skipping.
     */
    long current();

    /**
     * @return The previous offset that allows skipping.
     */
    long previous();

}
