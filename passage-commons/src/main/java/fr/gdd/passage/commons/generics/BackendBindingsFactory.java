package fr.gdd.passage.commons.generics;

import fr.gdd.passage.commons.interfaces.Backend;

public class BackendBindingsFactory<ID,VALUE> {

    final Backend<ID, VALUE> backend;
    public final BackendCache<ID,VALUE> cache;

    public BackendBindingsFactory(Backend<ID, VALUE> backend) {
        this.backend = backend;
        this.cache = new BackendCache<>(backend);
    }

    public BackendBindings<ID,VALUE> get() {
        return new BackendBindings<>(backend);
    }

}
