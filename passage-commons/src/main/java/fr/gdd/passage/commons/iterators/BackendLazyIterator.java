package fr.gdd.passage.commons.iterators;

import fr.gdd.passage.commons.exceptions.UndefinedCode;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;

/**
 * An iterator that enable retrieving values from the dictionary. Once
 * retrieved, the value is cached and only gets erased when the
 * underlying identifier changes.
 */
public class BackendLazyIterator<ID, VALUE> extends BackendIterator<ID, VALUE> {

    public BackendIterator<ID, VALUE> iterator;
    private final Backend<ID, VALUE> backend;

    private boolean subject_has_changed = true;
    private boolean predicate_has_changed = true;
    private boolean object_has_changed = true;
    private boolean context_has_changed = true;

    private ID subject_id = null;
    private ID predicate_id = null;
    private ID object_id = null;
    private ID context_id = null;

    private VALUE subject_value = null;
    private VALUE predicate_value = null;
    private VALUE object_value = null;
    private VALUE context_value = null;

    private String subject = null;
    private String predicate = null;
    private String object = null;
    private String context = null;

    public BackendLazyIterator(Backend<ID, VALUE> backend, BackendIterator<ID, VALUE> wrapped) {
        this.backend = backend;
        this.iterator = wrapped;
    }

    @Override
    public ID getId(final int code) {
        return switch (code) {
            case SPOC.SUBJECT -> this.subject_id;
            case SPOC.PREDICATE -> this.predicate_id;
            case SPOC.OBJECT -> this.object_id;
            case SPOC.CONTEXT -> this.context_id;
            default -> throw new UndefinedCode(code);
        };
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public void next() {
        iterator.next();

        if (iterator.getId(SPOC.SUBJECT) != this.subject_id) {
            this.subject_has_changed = true;
            this.subject_id = iterator.getId(SPOC.SUBJECT);
        }
        if (iterator.getId(SPOC.PREDICATE) != this.predicate_id) {
            this.predicate_has_changed = true;
            this.predicate_id = iterator.getId(SPOC.PREDICATE);
        }
        if (iterator.getId(SPOC.OBJECT) != this.object_id) {            
            this.object_has_changed = true;
            this.object_id = iterator.getId(SPOC.OBJECT);
        }
        if (iterator.getId(SPOC.CONTEXT) != this.context_id) {
            this.context_has_changed = true;
            this.context_id = iterator.getId(SPOC.CONTEXT);
        }
    };

    @Override
    public void reset() {
        iterator.reset();

        this.subject_id = null;
        this.predicate_id  = null;
        this.object_id  = null;
        this.context_id = null;

        this.subject_has_changed   = true;
        this.predicate_has_changed = true;
        this.object_has_changed    = true;
        this.context_has_changed   = true;
    }

    @Override
    public VALUE getValue(final int code) {
        return switch (code) {
            case SPOC.SUBJECT -> {
                if (subject_has_changed) {
                    subject_value = backend.getValue(subject_id, code);
                    subject_has_changed = false;
                }
                yield subject_value;
            }
            case SPOC.PREDICATE -> {
                if (predicate_has_changed) {
                    predicate_value = backend.getValue(predicate_id, code);
                    predicate_has_changed = false;
                }
                yield predicate_value;
            }
            case SPOC.OBJECT -> {
                if (object_has_changed) {
                    object_value = backend.getValue(object_id, code);
                    object_has_changed = false;
                }
                yield object_value;
            }
            case SPOC.CONTEXT -> {
                if (context_has_changed) {
                    context_value = backend.getValue(context_id, code);
                    context_has_changed = false;
                }
                yield context_value;
            }
            default -> throw new UndefinedCode(code);
        };
    }

    @Override
    public String getString(int code) {
        return backend.getString(getId(code));
    }

    @Override
    public long current() {
        return iterator.current();
    }

    @Override
    public long previous() {
        return iterator.previous();
    }

    @Override
    public void skip(long to) {
        iterator.skip(to);
    }

    @Override
    public Double random() {
        return iterator.random();
    }

    @Override
    public double cardinality() {
        return iterator.cardinality();
    }

    @Override
    public double cardinality(long strength) {
        return iterator.cardinality(strength);
    }

    /**
     * @return The wrapped iterator.
     */
    public BackendIterator<ID, VALUE> getWrapped() {
        return this.iterator;
    }
}
