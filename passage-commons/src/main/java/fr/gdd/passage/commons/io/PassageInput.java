package fr.gdd.passage.commons.io;

import fr.gdd.passage.commons.interfaces.Backend;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * Input given to a Sage query execution for it to execute
 * properly. For instance, Sage needs `timeout` to know when it must
 * pause its execution. 
 **/
public class PassageInput<SKIP extends Serializable> implements Serializable {

    private Map<Integer, SKIP> state = new TreeMap<>();
    private long limit    = Long.MAX_VALUE;
    private long timeout  = Long.MAX_VALUE;
    private long deadline = Long.MAX_VALUE;

    /**
     * We want to execute the query using random walk, eg. to probe
     * it.
     * For now, random walks not big enough to differentiate it from
     * Sage.
     **/
    private boolean randomWalking = false;
    /**
     * (TODO) backjumping enable skipping portions of execution when
     * constraints on variables are not meant. However, it requires a
     * `goto` kind of statement.
     **/
    private boolean backjumping   = false;

    /**
     * Compiled execution may require a shared backend to work.
     **/
    transient Backend<?, ?> backend;

    /* ********************************************************************* */

    public PassageInput() { }

    public PassageInput<SKIP> setLimit(long limit) {
        this.limit = limit;
        return this;
    }

    public PassageInput<SKIP> setBackend(Backend<?, ?> backend) {
        this.backend = backend;
        return this;
    }

    public PassageInput<SKIP> setState(Map<Integer, SKIP> state) {
        this.state = state;
        return this;
    }

    public PassageInput<SKIP> setDeadline(long deadline) {
        this.deadline = deadline;
        return this;
    }

    public PassageInput<SKIP> setTimeout(long timeout) {
        long checkpoint = System.currentTimeMillis();
        if (checkpoint + timeout <= 0) { // overflow
            this.deadline = Long.MAX_VALUE;
        } else {
            this.deadline = checkpoint + timeout;
        }
        this.timeout = timeout;
        return this;
    }

    public PassageInput<SKIP> setBackjumping(boolean backjumping) {
        this.backjumping = backjumping;
        return this;
    }

    public PassageInput<SKIP> setRandomWalking(boolean randomWalking) {
        this.randomWalking = randomWalking;
        return this;
    }

    /* ******************************************************************* */
    
    public Backend<?, ?> getBackend() {
        return backend;
    }

    /**
     * Consumes the value of the state.
     **/
    public SKIP getState(Integer id) {
        return state.remove(id);
    }

    public Map<Integer, SKIP> getState() {
        return state;
    }

    public long getLimit() {
	return limit;
    }

    public long getTimeout() {
        return timeout;
    }

    public long getDeadline() {
        return deadline;
    }

    public boolean isBackjumping() {
        return backjumping;
    }

    public boolean isRandomWalking() {
        return randomWalking;
    }
}
