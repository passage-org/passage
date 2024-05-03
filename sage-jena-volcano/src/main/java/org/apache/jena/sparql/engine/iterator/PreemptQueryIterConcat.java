package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.interfaces.PreemptIterator;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.HashMap;
import java.util.Objects;

/**
 * Actual iterator of adjacent unions, i.e., operators of adjacent unions are gathered together
 * in a list and executed sequentially. A preemptive version of {@link QueryIterConcat} must have
 * a unique identifier to save/resume its offset in the list of operators the first time it gets
 * executed.
 */
public class PreemptQueryIterConcat extends QueryIterConcat implements PreemptIterator<Integer> {

    Integer offset;
    Integer previousOffset = null;

    int id;

    public PreemptQueryIterConcat(ExecutionContext context, int id) {
        super(context);
        this.id = id;
        this.offset = 0;

        HashMap<Integer, PreemptIterator> iterators = context.getContext().get(SageConstants.iterators);
        iterators.put(id, this);
    }

    @Override
    protected boolean hasNextBinding() {
        // Copy/pasta of `hasNextBinding` with an offset increment on
        // `iterator.next`.
        if ( isFinished() )
            return false ;

        init();
        if ( currentQIter == null )
            return false ;

        while ( ! currentQIter.hasNext() )
        {
            // End sub iterator
            //currentQIter.close() ;
            currentQIter = null ;
            if ( iterator.hasNext() ) {
                offset += 1;
                currentQIter = iterator.next();
            }
            if ( currentQIter == null )
            {
                // No more.
                //close() ;
                return false ;
            }
        }

        return true;
    }

    @Override
    protected Binding moveToNextBinding() {
        previousOffset = offset;
        return super.moveToNextBinding();
    }

    private void init(Integer... start) {
        if (!initialized) {
            currentQIter = null;
            if (iterator == null) {
                if (Objects.nonNull(start) && start.length > 0) {
                    iteratorList.subList(0, start[0]).forEach(i -> i.close()); // cleaner
                    iteratorList = iteratorList.subList(start[0], iteratorList.size());
                }
                iterator = iteratorList.listIterator();
            }
            if (iterator.hasNext())
                currentQIter = iterator.next();
            initialized = true;
        }
    }

    /* ******************************************************************* */

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public void skip(Integer to) {
        this.offset = to;
        init(to);
    }

    @Override
    public Integer current() {
        return offset;
    }

    @Override
    public Integer previous() {
        return offset;
    }

}
