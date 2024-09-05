package fr.gdd.sage.sager.writers;

/**
 * It's important that this is a pointer so when the context is saved,
 * even the copy of it done by the JSONWriter will be updated.
 */
public class SagerSavedState {

    String state;

    public SagerSavedState() {}

    public void setState (String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }
}
